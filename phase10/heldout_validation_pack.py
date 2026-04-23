from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from uuid import uuid4

from config.settings import REPO_ROOT
from database.db_manager import apply_schema, get_conn
from phase5 import ConservativePaperTrader, Phase5Repository, build_phase5_person2_report, run_phase5_replay_bundle
from phase10.heldout_family import (
    PHASE10_HELDOUT_OVERALL_END,
    PHASE10_HELDOUT_OVERALL_START,
    PHASE10_HELDOUT_SOURCE_PRICES,
    PHASE10_HELDOUT_SOURCE_TRADES,
    WINDOW_SPECS,
    materialize_phase10_heldout_family,
)


PHASE10_TASK3_CONTRACT_VERSION = "phase10_task3_heldout_validation_pack_v1"
PHASE10_TASK3_VALIDATION_JSON = "reports/phase5/validation/phase10_task3_heldout_validation.json"
PHASE10_TASK3_VALIDATION_MD = "reports/phase5/validation/phase10_task3_heldout_validation.md"
PHASE10_TASK3_BACKTEST_JSON = "reports/phase5/backtests/phase10_task3_conservative_backtest.json"
PHASE10_TASK3_BACKTEST_MD = "reports/phase5/backtests/phase10_task3_conservative_backtest.md"
PHASE10_TASK3_PAPER_TRADES_JSON = "reports/phase5/backtests/phase10_task3_paper_trades.json"
PHASE10_TASK3_SUMMARY_DIR = "reports/phase10/heldout_validation_pack"


def _iso_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _ensure_parent(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def _render_validation_markdown(payload: dict[str, Any]) -> str:
    lines = [
        "# Phase 10 Task 3 - Held-Out Phase 5 Validation Pack",
        "",
        f"- Contract version: `{PHASE10_TASK3_CONTRACT_VERSION}`",
        f"- Held-out family: `{payload['window_family_name']}`",
        f"- Overall window: `{payload['start']}` to `{payload['end']}`",
        f"- Assessment: `{payload['assessment']['status']}`",
        f"- Evaluation rows: `{payload['evaluation_row_count']}`",
        f"- Alert rows: `{payload['alert_row_count']}`",
        f"- Paper trade count: `{payload['paper_trade_count']}`",
        "",
        "## Time-Split Status",
    ]
    for item in payload["time_split_summary"]["candidate"]:
        lines.append(f"- Candidate split `{item['split_key']}` status `{item['status']}` rows `{item['total_rows']}`")
    for item in payload["time_split_summary"]["alert"]:
        lines.append(f"- Alert split `{item['split_key']}` status `{item['status']}` rows `{item['total_rows']}`")
    for item in payload["time_split_summary"]["paper_trade"]:
        lines.append(f"- Paper-trade split `{item['split_key']}` status `{item['status']}` rows `{item['total_rows']}`")
    return "\n".join(lines) + "\n"


def _render_backtest_markdown(payload: dict[str, Any]) -> str:
    metrics = payload["paper_trade_metrics"]
    lines = [
        "# Phase 10 Task 3 - Conservative Held-Out Backtest",
        "",
        f"- Contract version: `{PHASE10_TASK3_CONTRACT_VERSION}`",
        f"- Held-out family: `{payload['window_family_name']}`",
        f"- Replay bundles ready: `{payload['replay_family_ready']}`",
        f"- Paper trades: `{payload['paper_trade_count']}`",
        f"- Filled/resolved trades: `{payload['paper_trade_fills']}`",
        f"- Median bounded PnL: `{metrics['median_bounded_pnl']}`",
        f"- Mean bounded PnL: `{metrics['mean_bounded_pnl']}`",
        f"- Hit rate: `{metrics['hit_rate']}`",
        f"- Loss rate: `{metrics['loss_rate']}`",
    ]
    return "\n".join(lines) + "\n"


def _record_validation_run(*, report: dict[str, Any], replay_bundle_ids: list[str]) -> str:
    validation_run_id = uuid4().hex
    conn = get_conn()
    completed_at = _iso_now()
    try:
        conn.execute(
            """
            INSERT INTO validation_runs (
                validation_run_id,
                replay_run_id,
                validation_type,
                split_name,
                status,
                config_json,
                metrics_json,
                output_path,
                notes,
                created_at,
                completed_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                validation_run_id,
                None,
                "phase10_heldout_validation",
                "time_split_holdout",
                "completed",
                json.dumps(
                    {
                        "contract_version": PHASE10_TASK3_CONTRACT_VERSION,
                        "start": report["start"],
                        "end": report["end"],
                        "replay_bundle_ids": replay_bundle_ids,
                        "window_family_name": report["window_family_name"],
                    },
                    sort_keys=True,
                ),
                json.dumps(report["metrics"], sort_keys=True),
                PHASE10_TASK3_VALIDATION_JSON,
                json.dumps({"phase": "phase10_task3", "assessment_status": report["assessment"]["status"]}, sort_keys=True),
                completed_at,
                completed_at,
            ),
        )
        conn.commit()
    finally:
        conn.close()
    return validation_run_id


def _record_backtest_artifact(*, summary: dict[str, Any], replay_bundle_ids: list[str]) -> str:
    artifact_id = uuid4().hex
    conn = get_conn()
    completed_at = _iso_now()
    try:
        conn.execute(
            """
            INSERT INTO backtest_artifacts (
                backtest_artifact_id,
                replay_run_id,
                artifact_type,
                status,
                config_json,
                summary_json,
                output_path,
                notes,
                created_at,
                completed_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                artifact_id,
                None,
                "phase10_conservative_heldout_backtest",
                "completed",
                json.dumps(
                    {
                        "contract_version": PHASE10_TASK3_CONTRACT_VERSION,
                        "replay_bundle_ids": replay_bundle_ids,
                        "window_family_name": summary["window_family_name"],
                    },
                    sort_keys=True,
                ),
                json.dumps(summary, sort_keys=True),
                PHASE10_TASK3_BACKTEST_JSON,
                json.dumps({"phase": "phase10_task3"}, sort_keys=True),
                completed_at,
                completed_at,
            ),
        )
        conn.commit()
    finally:
        conn.close()
    return artifact_id


def _scored_time_splits(validation_report: dict[str, Any]) -> dict[str, list[dict[str, Any]]]:
    metrics = validation_report["metrics"]
    return {
        "candidate": list(metrics["candidate_regimes"]["time_split_holdout"]),
        "alert": list(metrics["alert_regimes"]["time_split_holdout"]),
        "paper_trade": list(metrics["paper_trade_regimes"]["time_split_holdout"]),
    }


def run_phase10_task3_heldout_validation_pack() -> dict[str, Any]:
    apply_schema()
    seed_summary = materialize_phase10_heldout_family()
    replay_bundles: list[dict[str, Any]] = []
    for window in WINDOW_SPECS:
        replay_bundles.append(
            run_phase5_replay_bundle(
                start=window.start,
                end=window.end,
                source_systems=[PHASE10_HELDOUT_SOURCE_PRICES, PHASE10_HELDOUT_SOURCE_TRADES],
                output_dir=f"reports/phase5/replay_runs/phase10_task3/{window.key}",
                notes=f"phase10_task3 replay bundle for {window.key}",
            ).to_dict()
        )

    validation_report = build_phase5_person2_report(
        start=PHASE10_HELDOUT_OVERALL_START,
        end=PHASE10_HELDOUT_OVERALL_END,
    ).to_dict()
    validation_report["task_contract_version"] = PHASE10_TASK3_CONTRACT_VERSION
    validation_report["window_family_name"] = "phase10_canonical_heldout_family"
    validation_report["window_family"] = [
        {"key": window.key, "role": window.role, "start": window.start, "end": window.end}
        for window in WINDOW_SPECS
    ]
    time_split_summary = _scored_time_splits(validation_report)
    validation_report["time_split_summary"] = time_split_summary

    repository = Phase5Repository()
    rows = repository.load_evaluation_rows(start=PHASE10_HELDOUT_OVERALL_START, end=PHASE10_HELDOUT_OVERALL_END)
    trader = ConservativePaperTrader(repository=repository)
    paper_trades = [trade.to_dict() for trade in trader.simulate(rows)]

    all_candidate_scored = all(item["status"] == "scored" for item in time_split_summary["candidate"])
    all_alert_scored = all(item["status"] == "scored" for item in time_split_summary["alert"])
    all_trade_scored = all(item["status"] == "scored" for item in time_split_summary["paper_trade"])
    if not (all_candidate_scored and all_alert_scored and all_trade_scored):
        raise RuntimeError("Phase 10 Task 3 expected all time-split holdout regimes to be scored.")

    validation_json_path = REPO_ROOT / PHASE10_TASK3_VALIDATION_JSON
    validation_md_path = REPO_ROOT / PHASE10_TASK3_VALIDATION_MD
    backtest_json_path = REPO_ROOT / PHASE10_TASK3_BACKTEST_JSON
    backtest_md_path = REPO_ROOT / PHASE10_TASK3_BACKTEST_MD
    paper_trades_path = REPO_ROOT / PHASE10_TASK3_PAPER_TRADES_JSON
    summary_dir = REPO_ROOT / PHASE10_TASK3_SUMMARY_DIR
    for path in [validation_json_path, validation_md_path, backtest_json_path, backtest_md_path, paper_trades_path]:
        _ensure_parent(path)
    summary_dir.mkdir(parents=True, exist_ok=True)

    validation_json_path.write_text(json.dumps(validation_report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    validation_md_path.write_text(_render_validation_markdown(validation_report), encoding="utf-8")

    backtest_summary = {
        "task_contract_version": PHASE10_TASK3_CONTRACT_VERSION,
        "window_family_name": "phase10_canonical_heldout_family",
        "window_family": validation_report["window_family"],
        "replay_family_ready": all(bundle["overall_status"] == "ready" for bundle in replay_bundles),
        "paper_trade_count": len(paper_trades),
        "paper_trade_fills": sum(1 for trade in paper_trades if trade["status"] in {"filled", "resolved"}),
        "paper_trade_skips": sum(1 for trade in paper_trades if trade["status"] == "skipped"),
        "paper_trade_metrics": validation_report["metrics"]["paper_trade_overall"],
        "failure_metrics": validation_report["metrics"]["failure_overall"],
        "replay_bundles": replay_bundles,
    }
    backtest_json_path.write_text(json.dumps(backtest_summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    backtest_md_path.write_text(_render_backtest_markdown(backtest_summary), encoding="utf-8")
    paper_trades_path.write_text(json.dumps(paper_trades, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    replay_bundle_ids = [bundle["bundle_id"] for bundle in replay_bundles]
    validation_run_id = _record_validation_run(report=validation_report, replay_bundle_ids=replay_bundle_ids)
    backtest_artifact_id = _record_backtest_artifact(summary=backtest_summary, replay_bundle_ids=replay_bundle_ids)

    summary_payload = {
        "task_contract_version": PHASE10_TASK3_CONTRACT_VERSION,
        "task_name": "Phase 10 Task 3 - Held-Out Phase 5 Validation Pack",
        "generated_at": _iso_now(),
        "seed_summary": seed_summary,
        "validation_report": {
            "assessment": validation_report["assessment"],
            "coverage_summary": validation_report["coverage_summary"],
            "evaluation_row_count": validation_report["evaluation_row_count"],
            "alert_row_count": validation_report["alert_row_count"],
            "paper_trade_count": validation_report["paper_trade_count"],
        },
        "time_split_summary": time_split_summary,
        "replay_bundles": replay_bundles,
        "backtest_summary": backtest_summary,
        "artifacts": {
            "validation_json": PHASE10_TASK3_VALIDATION_JSON,
            "validation_markdown": PHASE10_TASK3_VALIDATION_MD,
            "backtest_json": PHASE10_TASK3_BACKTEST_JSON,
            "backtest_markdown": PHASE10_TASK3_BACKTEST_MD,
            "paper_trades_json": PHASE10_TASK3_PAPER_TRADES_JSON,
        },
        "database_rows": {
            "validation_run_id": validation_run_id,
            "backtest_artifact_id": backtest_artifact_id,
        },
    }
    (summary_dir / "phase10_task3_review_packet.json").write_text(
        json.dumps(summary_payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    (summary_dir / "phase10_task3_review_summary.md").write_text(
        "\n".join(
            [
                "# Phase 10 Task 3 - Held-Out Validation Pack",
                "",
                f"- Evaluation rows: `{validation_report['evaluation_row_count']}`",
                f"- Alert rows: `{validation_report['alert_row_count']}`",
                f"- Paper trades: `{validation_report['paper_trade_count']}`",
                f"- Assessment: `{validation_report['assessment']['status']}`",
                f"- Replay bundles ready: `{backtest_summary['replay_family_ready']}`",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    return summary_payload
