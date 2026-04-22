from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from uuid import uuid4

from config.settings import REPO_ROOT
from database.db_manager import apply_schema, get_conn
from phase5 import (
    ConservativePaperTrader,
    Phase5Repository,
    build_phase5_person2_report,
    run_phase5_replay_bundle,
)
from phase9.candidate_alert import (
    PHASE9_TASK2_END,
    PHASE9_TASK2_MARKET_A,
    PHASE9_TASK2_MARKET_B,
    PHASE9_TASK2_SOURCE_PRICES,
    PHASE9_TASK2_SOURCE_TRADES,
    PHASE9_TASK2_START,
)
from utils.event_log import RAW_ARCHIVE_ROOT, archive_raw_event


PHASE9_TASK3_CONTRACT_VERSION = "phase9_task3_phase5_validation_v1"
PHASE9_TASK3_REPLAY_OUTPUT_DIR = "reports/phase5/replay_runs/phase9_task3"
PHASE9_TASK3_VALIDATION_JSON = "reports/phase5/validation/phase9_task3_holdout_validation.json"
PHASE9_TASK3_VALIDATION_MD = "reports/phase5/validation/phase9_task3_holdout_validation.md"
PHASE9_TASK3_BACKTEST_JSON = "reports/phase5/backtests/phase9_task3_conservative_backtest.json"
PHASE9_TASK3_BACKTEST_MD = "reports/phase5/backtests/phase9_task3_conservative_backtest.md"
PHASE9_TASK3_PAPER_TRADES_JSON = "reports/phase5/backtests/phase9_task3_paper_trades.json"
PHASE9_TASK3_SOURCE_SYSTEMS = [PHASE9_TASK2_SOURCE_PRICES, PHASE9_TASK2_SOURCE_TRADES]
PHASE9_TASK3_MARKET_IDS = [PHASE9_TASK2_MARKET_A, PHASE9_TASK2_MARKET_B]


@dataclass(frozen=True, slots=True)
class RawEnvelopeSpec:
    source_system: str
    event_type: str
    captured_at: str
    payload: dict[str, Any]
    metadata: dict[str, Any]


def _iso(value: datetime) -> str:
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).isoformat()


def _parse_iso(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _repo_relative(path: Path) -> str:
    try:
        return str(path.resolve().relative_to(REPO_ROOT.resolve())).replace("\\", "/")
    except ValueError:
        return str(path)


def _raw_hour_path(source_system: str) -> Path:
    return (
        RAW_ARCHIVE_ROOT
        / "year=2026"
        / "month=04"
        / "day=20"
        / "hour=05"
        / f"source_system={source_system}"
        / "events.ndjson"
    )


def _remove_file_if_exists(path: Path) -> None:
    if path.exists():
        path.unlink()
    parent = path.parent
    while parent != RAW_ARCHIVE_ROOT and parent.exists():
        try:
            parent.rmdir()
        except OSError:
            break
        parent = parent.parent


def _raw_envelopes() -> list[RawEnvelopeSpec]:
    return [
        RawEnvelopeSpec(
            source_system=PHASE9_TASK2_SOURCE_PRICES,
            event_type="prices_batch",
            captured_at="2026-04-20T05:00:00+00:00",
            payload={
                "market_snapshots": [
                    {"market_id": PHASE9_TASK2_MARKET_A, "captured_at": "2026-04-20T05:00:00+00:00", "yes_price": 0.40}
                ]
            },
            metadata={"phase": "phase9_task3", "fixture": "reference_prices_a_1"},
        ),
        RawEnvelopeSpec(
            source_system=PHASE9_TASK2_SOURCE_PRICES,
            event_type="prices_batch",
            captured_at="2026-04-20T05:02:00+00:00",
            payload={
                "market_snapshots": [
                    {"market_id": PHASE9_TASK2_MARKET_A, "captured_at": "2026-04-20T05:02:00+00:00", "yes_price": 0.42}
                ]
            },
            metadata={"phase": "phase9_task3", "fixture": "reference_prices_a_2"},
        ),
        RawEnvelopeSpec(
            source_system=PHASE9_TASK2_SOURCE_PRICES,
            event_type="prices_batch",
            captured_at="2026-04-20T05:04:00+00:00",
            payload={
                "market_snapshots": [
                    {"market_id": PHASE9_TASK2_MARKET_A, "captured_at": "2026-04-20T05:04:00+00:00", "yes_price": 0.46}
                ]
            },
            metadata={"phase": "phase9_task3", "fixture": "reference_prices_a_3"},
        ),
        RawEnvelopeSpec(
            source_system=PHASE9_TASK2_SOURCE_PRICES,
            event_type="prices_batch",
            captured_at="2026-04-20T05:05:30+00:00",
            payload={
                "market_snapshots": [
                    {"market_id": PHASE9_TASK2_MARKET_A, "captured_at": "2026-04-20T05:05:30+00:00", "yes_price": 0.52}
                ]
            },
            metadata={"phase": "phase9_task3", "fixture": "reference_prices_a_4"},
        ),
        RawEnvelopeSpec(
            source_system=PHASE9_TASK2_SOURCE_PRICES,
            event_type="prices_batch",
            captured_at="2026-04-20T05:12:00+00:00",
            payload={
                "market_snapshots": [
                    {"market_id": PHASE9_TASK2_MARKET_B, "captured_at": "2026-04-20T05:12:00+00:00", "yes_price": 0.35}
                ]
            },
            metadata={"phase": "phase9_task3", "fixture": "reference_prices_b_1"},
        ),
        RawEnvelopeSpec(
            source_system=PHASE9_TASK2_SOURCE_PRICES,
            event_type="prices_batch",
            captured_at="2026-04-20T05:14:00+00:00",
            payload={
                "market_snapshots": [
                    {"market_id": PHASE9_TASK2_MARKET_B, "captured_at": "2026-04-20T05:14:00+00:00", "yes_price": 0.37}
                ]
            },
            metadata={"phase": "phase9_task3", "fixture": "reference_prices_b_2"},
        ),
        RawEnvelopeSpec(
            source_system=PHASE9_TASK2_SOURCE_PRICES,
            event_type="prices_batch",
            captured_at="2026-04-20T05:16:00+00:00",
            payload={
                "market_snapshots": [
                    {"market_id": PHASE9_TASK2_MARKET_B, "captured_at": "2026-04-20T05:16:00+00:00", "yes_price": 0.44}
                ]
            },
            metadata={"phase": "phase9_task3", "fixture": "reference_prices_b_3"},
        ),
        RawEnvelopeSpec(
            source_system=PHASE9_TASK2_SOURCE_PRICES,
            event_type="prices_batch",
            captured_at="2026-04-20T05:17:30+00:00",
            payload={
                "market_snapshots": [
                    {"market_id": PHASE9_TASK2_MARKET_B, "captured_at": "2026-04-20T05:17:30+00:00", "yes_price": 0.50}
                ]
            },
            metadata={"phase": "phase9_task3", "fixture": "reference_prices_b_4"},
        ),
        RawEnvelopeSpec(
            source_system=PHASE9_TASK2_SOURCE_TRADES,
            event_type="recent_trades_page",
            captured_at="2026-04-20T05:02:00+00:00",
            payload={
                "trades": [
                    {
                        "trade_id": "phase9-a-old-1",
                        "market_id": PHASE9_TASK2_MARKET_A,
                        "trade_time": "2026-04-20T05:02:00+00:00",
                        "proxy_wallet": "0xphase9aold1",
                        "side": "BUY",
                        "outcome_side": "YES",
                        "usdc_notional": 60.0,
                    },
                    {
                        "trade_id": "phase9-a-old-2",
                        "market_id": PHASE9_TASK2_MARKET_A,
                        "trade_time": "2026-04-20T05:02:10+00:00",
                        "proxy_wallet": "0xphase9aold2",
                        "side": "SELL",
                        "outcome_side": "YES",
                        "usdc_notional": 40.0,
                    },
                ]
            },
            metadata={"phase": "phase9_task3", "fixture": "reference_trades_a_1"},
        ),
        RawEnvelopeSpec(
            source_system=PHASE9_TASK2_SOURCE_TRADES,
            event_type="recent_trades_page",
            captured_at="2026-04-20T05:06:00+00:00",
            payload={
                "trades": [
                    {
                        "trade_id": "phase9-a-fresh-1",
                        "market_id": PHASE9_TASK2_MARKET_A,
                        "trade_time": "2026-04-20T05:06:00+00:00",
                        "proxy_wallet": "0xphase9afresh1",
                        "side": "BUY",
                        "outcome_side": "YES",
                        "usdc_notional": 180.0,
                    },
                    {
                        "trade_id": "phase9-a-fresh-2",
                        "market_id": PHASE9_TASK2_MARKET_A,
                        "trade_time": "2026-04-20T05:06:10+00:00",
                        "proxy_wallet": "0xphase9afresh2",
                        "side": "BUY",
                        "outcome_side": "YES",
                        "usdc_notional": 80.0,
                    },
                    {
                        "trade_id": "phase9-a-fresh-3",
                        "market_id": PHASE9_TASK2_MARKET_A,
                        "trade_time": "2026-04-20T05:06:20+00:00",
                        "proxy_wallet": "0xphase9afresh3",
                        "side": "BUY",
                        "outcome_side": "YES",
                        "usdc_notional": 40.0,
                    },
                ]
            },
            metadata={"phase": "phase9_task3", "fixture": "reference_trades_a_2"},
        ),
        RawEnvelopeSpec(
            source_system=PHASE9_TASK2_SOURCE_TRADES,
            event_type="recent_trades_page",
            captured_at="2026-04-20T05:14:00+00:00",
            payload={
                "trades": [
                    {
                        "trade_id": "phase9-b-old-1",
                        "market_id": PHASE9_TASK2_MARKET_B,
                        "trade_time": "2026-04-20T05:14:00+00:00",
                        "proxy_wallet": "0xphase9bold1",
                        "side": "BUY",
                        "outcome_side": "YES",
                        "usdc_notional": 70.0,
                    },
                    {
                        "trade_id": "phase9-b-old-2",
                        "market_id": PHASE9_TASK2_MARKET_B,
                        "trade_time": "2026-04-20T05:14:10+00:00",
                        "proxy_wallet": "0xphase9bold2",
                        "side": "SELL",
                        "outcome_side": "YES",
                        "usdc_notional": 30.0,
                    },
                ]
            },
            metadata={"phase": "phase9_task3", "fixture": "reference_trades_b_1"},
        ),
        RawEnvelopeSpec(
            source_system=PHASE9_TASK2_SOURCE_TRADES,
            event_type="recent_trades_page",
            captured_at="2026-04-20T05:18:00+00:00",
            payload={
                "trades": [
                    {
                        "trade_id": "phase9-b-fresh-1",
                        "market_id": PHASE9_TASK2_MARKET_B,
                        "trade_time": "2026-04-20T05:18:00+00:00",
                        "proxy_wallet": "0xphase9bfresh1",
                        "side": "BUY",
                        "outcome_side": "YES",
                        "usdc_notional": 160.0,
                    },
                    {
                        "trade_id": "phase9-b-fresh-2",
                        "market_id": PHASE9_TASK2_MARKET_B,
                        "trade_time": "2026-04-20T05:18:10+00:00",
                        "proxy_wallet": "0xphase9bfresh2",
                        "side": "BUY",
                        "outcome_side": "YES",
                        "usdc_notional": 90.0,
                    },
                    {
                        "trade_id": "phase9-b-fresh-3",
                        "market_id": PHASE9_TASK2_MARKET_B,
                        "trade_time": "2026-04-20T05:18:20+00:00",
                        "proxy_wallet": "0xphase9bfresh3",
                        "side": "BUY",
                        "outcome_side": "YES",
                        "usdc_notional": 50.0,
                    },
                ]
            },
            metadata={"phase": "phase9_task3", "fixture": "reference_trades_b_2"},
        ),
    ]


def cleanup_phase9_task3_state() -> dict[str, Any]:
    conn = get_conn()
    deleted: dict[str, int] = {}
    try:
        snapshots_deleted = conn.execute(
            f"DELETE FROM snapshots WHERE market_id IN ({', '.join('?' for _ in PHASE9_TASK3_MARKET_IDS)})",
            tuple(PHASE9_TASK3_MARKET_IDS),
        ).rowcount
        resolutions_deleted = conn.execute(
            f"DELETE FROM market_resolutions WHERE market_id IN ({', '.join('?' for _ in PHASE9_TASK3_MARKET_IDS)})",
            tuple(PHASE9_TASK3_MARKET_IDS),
        ).rowcount
        validation_deleted = conn.execute(
            """
            DELETE FROM validation_runs
            WHERE output_path IN (?, ?)
               OR notes LIKE ?
            """,
            (
                PHASE9_TASK3_VALIDATION_JSON,
                PHASE9_TASK3_VALIDATION_MD,
                "%phase9_task3%",
            ),
        ).rowcount
        backtest_deleted = conn.execute(
            """
            DELETE FROM backtest_artifacts
            WHERE output_path IN (?, ?, ?)
               OR notes LIKE ?
            """,
            (
                PHASE9_TASK3_BACKTEST_JSON,
                PHASE9_TASK3_BACKTEST_MD,
                PHASE9_TASK3_PAPER_TRADES_JSON,
                "%phase9_task3%",
            ),
        ).rowcount
        replay_deleted = conn.execute(
            f"""
            DELETE FROM replay_runs
            WHERE source_system IN ({', '.join('?' for _ in PHASE9_TASK3_SOURCE_SYSTEMS)})
              AND notes LIKE ?
            """,
            tuple(PHASE9_TASK3_SOURCE_SYSTEMS + ["%phase9_task3%"]),
        ).rowcount
        raw_manifest_deleted = conn.execute(
            f"DELETE FROM raw_archive_manifests WHERE source_system IN ({', '.join('?' for _ in PHASE9_TASK3_SOURCE_SYSTEMS)})",
            tuple(PHASE9_TASK3_SOURCE_SYSTEMS),
        ).rowcount
        conn.commit()
        deleted = {
            "snapshots_deleted": int(snapshots_deleted or 0),
            "resolutions_deleted": int(resolutions_deleted or 0),
            "validation_runs_deleted": int(validation_deleted or 0),
            "backtest_artifacts_deleted": int(backtest_deleted or 0),
            "replay_runs_deleted": int(replay_deleted or 0),
            "raw_manifests_deleted": int(raw_manifest_deleted or 0),
        }
    finally:
        conn.close()

    for source_system in PHASE9_TASK3_SOURCE_SYSTEMS:
        _remove_file_if_exists(_raw_hour_path(source_system))

    return deleted


def seed_phase9_task3_raw_archive() -> list[dict[str, Any]]:
    written: list[dict[str, Any]] = []
    for spec in _raw_envelopes():
        result = archive_raw_event(
            source_system=spec.source_system,
            event_type=spec.event_type,
            payload=spec.payload,
            captured_at=spec.captured_at,
            metadata=spec.metadata,
        )
        written.append(
            {
                "source_system": spec.source_system,
                "event_type": spec.event_type,
                "captured_at": spec.captured_at,
                "partition_path": result.partition_path,
                "envelope_id": result.envelope_id,
            }
        )
    return written


def align_phase9_task3_historical_timestamps() -> dict[str, Any]:
    conn = get_conn()
    aligned_candidates = 0
    try:
        rows = conn.execute(
            f"""
            SELECT sc.candidate_id, sc.market_id, sc.trigger_time, a.alert_id
            FROM signal_candidates sc
            LEFT JOIN alerts a ON a.candidate_id = sc.candidate_id
            WHERE sc.market_id IN ({', '.join('?' for _ in PHASE9_TASK3_MARKET_IDS)})
            ORDER BY sc.trigger_time ASC
            """,
            tuple(PHASE9_TASK3_MARKET_IDS),
        ).fetchall()

        for index, row in enumerate(rows, start=1):
            candidate_id = str(row["candidate_id"])
            alert_id = row["alert_id"]
            if alert_id is None:
                continue

            trigger_time = _parse_iso(str(row["trigger_time"]))
            evidence_time = trigger_time + timedelta(seconds=30)
            alert_time = trigger_time + timedelta(seconds=45)
            feedback_time = trigger_time + timedelta(minutes=10)

            evidence_iso = _iso(evidence_time)
            alert_iso = _iso(alert_time)
            feedback_iso = _iso(feedback_time)

            conn.execute(
                """
                UPDATE evidence_queries
                SET
                    request_started_at = ?,
                    response_completed_at = ?,
                    created_at = ?
                WHERE candidate_id = ?
                """,
                (
                    _iso(trigger_time + timedelta(seconds=5)),
                    _iso(trigger_time + timedelta(seconds=7)),
                    _iso(trigger_time + timedelta(seconds=7)),
                    candidate_id,
                ),
            )
            conn.execute(
                """
                UPDATE evidence_snapshots
                SET
                    snapshot_time = ?,
                    created_at = ?
                WHERE candidate_id = ?
                """,
                (
                    evidence_iso,
                    evidence_iso,
                    candidate_id,
                ),
            )
            conn.execute(
                """
                UPDATE alerts
                SET
                    created_at = ?,
                    updated_at = ?,
                    first_delivery_at = ?,
                    last_delivery_at = ?
                WHERE alert_id = ?
                """,
                (
                    alert_iso,
                    _iso(alert_time + timedelta(seconds=10)),
                    _iso(alert_time + timedelta(seconds=5)),
                    _iso(alert_time + timedelta(seconds=10)),
                    alert_id,
                ),
            )
            attempts = conn.execute(
                """
                SELECT delivery_attempt_id, attempt_number
                FROM alert_delivery_attempts
                WHERE alert_id = ?
                ORDER BY attempt_number ASC, created_at ASC
                """,
                (alert_id,),
            ).fetchall()
            for attempt_offset, attempt in enumerate(attempts, start=1):
                attempt_time = alert_time + timedelta(seconds=attempt_offset * 5)
                conn.execute(
                    """
                    UPDATE alert_delivery_attempts
                    SET
                        attempted_at = ?,
                        completed_at = ?,
                        created_at = ?
                    WHERE delivery_attempt_id = ?
                    """,
                    (
                        _iso(attempt_time),
                        _iso(attempt_time + timedelta(seconds=1)),
                        _iso(attempt_time),
                        str(attempt["delivery_attempt_id"]),
                    ),
                )
            conn.execute(
                """
                UPDATE analyst_feedback
                SET
                    created_at = ?,
                    follow_up_at = ?
                WHERE alert_id = ?
                """,
                (
                    feedback_iso,
                    _iso(feedback_time + timedelta(minutes=15)),
                    alert_id,
                ),
            )
            aligned_candidates += 1

        conn.commit()
    finally:
        conn.close()

    return {"alerts_backdated": aligned_candidates}


def seed_phase9_task3_market_state() -> dict[str, Any]:
    conn = get_conn()
    try:
        conn.execute(
            """
            UPDATE markets
            SET
                end_date = ?,
                status = 'resolved',
                last_updated_at = ?
            WHERE market_id = ?
            """,
            ("2026-04-20T07:30:00+00:00", "2026-04-20T07:45:00+00:00", PHASE9_TASK2_MARKET_A),
        )
        conn.execute(
            """
            UPDATE markets
            SET
                end_date = ?,
                status = 'resolved',
                last_updated_at = ?
            WHERE market_id = ?
            """,
            ("2026-04-20T08:00:00+00:00", "2026-04-20T08:15:00+00:00", PHASE9_TASK2_MARKET_B),
        )

        conn.execute(
            """
            INSERT INTO market_resolutions (
                market_id,
                condition_id,
                outcome,
                final_price,
                resolved_at,
                source
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            (PHASE9_TASK2_MARKET_A, "phase9-task2-condition-a", "YES", 1.0, "2026-04-20T07:45:00+00:00", "phase9_task3"),
        )
        conn.execute(
            """
            INSERT INTO market_resolutions (
                market_id,
                condition_id,
                outcome,
                final_price,
                resolved_at,
                source
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            (PHASE9_TASK2_MARKET_B, "phase9-task2-condition-b", "NO", 0.0, "2026-04-20T08:15:00+00:00", "phase9_task3"),
        )

        snapshot_rows = [
            (PHASE9_TASK2_MARKET_A, "2026-04-20T05:06:50+00:00", 0.54, 0.46, 0.53, 0.54, 0.01),
            (PHASE9_TASK2_MARKET_A, "2026-04-20T05:30:00+00:00", 0.58, 0.42, 0.57, 0.58, 0.01),
            (PHASE9_TASK2_MARKET_A, "2026-04-20T06:30:00+00:00", 0.71, 0.29, 0.70, 0.71, 0.01),
            (PHASE9_TASK2_MARKET_B, "2026-04-20T05:18:50+00:00", 0.51, 0.49, 0.50, 0.51, 0.01),
            (PHASE9_TASK2_MARKET_B, "2026-04-20T05:45:00+00:00", 0.47, 0.53, 0.46, 0.47, 0.01),
            (PHASE9_TASK2_MARKET_B, "2026-04-20T06:45:00+00:00", 0.35, 0.65, 0.34, 0.35, 0.01),
        ]
        for market_id, captured_at, yes_price, no_price, best_bid, best_ask, spread in snapshot_rows:
            conn.execute(
                """
                INSERT INTO snapshots (
                    market_id,
                    captured_at,
                    yes_price,
                    no_price,
                    best_bid,
                    best_ask,
                    spread,
                    source
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (market_id, captured_at, yes_price, no_price, best_bid, best_ask, spread, "phase9_task3"),
            )
        conn.commit()
    finally:
        conn.close()

    return {
        "market_resolutions_written": 2,
        "snapshots_written": 6,
        "markets_updated": 2,
    }


def _render_validation_markdown(payload: dict[str, Any]) -> str:
    coverage = payload["coverage_summary"]
    metrics = payload["metrics"]
    assessment = payload["assessment"]
    lines = [
        "# Phase 9 Task 3 - Holdout Validation",
        "",
        f"- Contract version: `{PHASE9_TASK3_CONTRACT_VERSION}`",
        f"- Window: `{payload['start']}` to `{payload['end']}`",
        f"- Assessment status: `{assessment['status']}`",
        f"- Evaluation rows: `{payload['evaluation_row_count']}`",
        f"- Alert rows: `{payload['alert_row_count']}`",
        f"- Rows complete: `{coverage['rows_complete']}`",
        f"- Rows partial: `{coverage['rows_partial']}`",
        f"- Rows coverage insufficient: `{coverage['rows_coverage_insufficient']}`",
        f"- Candidate precision: `{metrics['candidate_overall']['candidate_precision']}`",
        f"- Alert usefulness precision: `{metrics['alert_overall']['alert_usefulness_precision']}`",
        f"- Median lead time seconds: `{metrics['lead_time_overall']['median_lead_time_seconds']}`",
        "",
        "## Holdout Regimes",
        f"- Event-family candidate splits: `{len(metrics['candidate_regimes']['event_family_holdout'])}`",
        f"- Category alert splits: `{len(metrics['alert_regimes']['category_holdout'])}`",
        f"- Time-block paper-trade splits: `{len(metrics['paper_trade_regimes']['time_split_holdout'])}`",
    ]
    return "\n".join(lines) + "\n"


def _render_backtest_markdown(summary: dict[str, Any]) -> str:
    lines = [
        "# Phase 9 Task 3 - Conservative Backtest",
        "",
        f"- Contract version: `{PHASE9_TASK3_CONTRACT_VERSION}`",
        f"- Window: `{summary['window']['start']}` to `{summary['window']['end']}`",
        f"- Replay bundle status: `{summary['replay']['overall_status']}`",
        f"- Paper trade count: `{summary['paper_trade_count']}`",
        f"- Filled or resolved trades: `{summary['paper_trade_fills']}`",
        f"- Skipped trades: `{summary['paper_trade_skips']}`",
        f"- Median bounded PnL: `{summary['paper_trade_metrics']['median_bounded_pnl']}`",
        f"- Mean bounded PnL: `{summary['paper_trade_metrics']['mean_bounded_pnl']}`",
        f"- Hit rate: `{summary['paper_trade_metrics']['hit_rate']}`",
        f"- Loss rate: `{summary['paper_trade_metrics']['loss_rate']}`",
        f"- Skip due to data rate: `{summary['failure_metrics']['skip_due_to_data_rate']}`",
    ]
    return "\n".join(lines) + "\n"


def _record_validation_run(*, payload: dict[str, Any], replay_bundle: dict[str, Any]) -> str:
    validation_run_id = uuid4().hex
    completed_at = datetime.now(timezone.utc).isoformat()
    conn = get_conn()
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
                "phase5_holdout_validation",
                "event_family_holdout,category_holdout,time_split_holdout",
                "completed",
                json.dumps(
                    {
                        "contract_version": PHASE9_TASK3_CONTRACT_VERSION,
                        "start": payload["start"],
                        "end": payload["end"],
                        "source_systems": PHASE9_TASK3_SOURCE_SYSTEMS,
                        "replay_bundle_id": replay_bundle["bundle_id"],
                    },
                    sort_keys=True,
                ),
                json.dumps(payload["metrics"], sort_keys=True),
                PHASE9_TASK3_VALIDATION_JSON,
                json.dumps(
                    {
                        "phase": "phase9_task3",
                        "assessment_status": payload["assessment"]["status"],
                        "coverage_summary": payload["coverage_summary"],
                    },
                    sort_keys=True,
                ),
                completed_at,
                completed_at,
            ),
        )
        conn.commit()
    finally:
        conn.close()
    return validation_run_id


def _record_backtest_artifact(*, summary: dict[str, Any], replay_bundle: dict[str, Any]) -> str:
    backtest_artifact_id = uuid4().hex
    completed_at = datetime.now(timezone.utc).isoformat()
    conn = get_conn()
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
                backtest_artifact_id,
                None,
                "conservative_paper_trading",
                "completed",
                json.dumps(
                    {
                        "contract_version": PHASE9_TASK3_CONTRACT_VERSION,
                        "start": summary["window"]["start"],
                        "end": summary["window"]["end"],
                        "source_systems": PHASE9_TASK3_SOURCE_SYSTEMS,
                        "replay_bundle_id": replay_bundle["bundle_id"],
                    },
                    sort_keys=True,
                ),
                json.dumps(summary, sort_keys=True),
                PHASE9_TASK3_BACKTEST_JSON,
                json.dumps(
                    {
                        "phase": "phase9_task3",
                        "artifact_family": "conservative_backtest",
                    },
                    sort_keys=True,
                ),
                completed_at,
                completed_at,
            ),
        )
        conn.commit()
    finally:
        conn.close()
    return backtest_artifact_id


def _table_counts() -> dict[str, int]:
    conn = get_conn()
    try:
        return {
            table: int((conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]) or 0)
            for table in [
                "raw_archive_manifests",
                "detector_input_manifests",
                "market_resolutions",
                "snapshots",
                "replay_runs",
                "validation_runs",
                "backtest_artifacts",
            ]
        }
    finally:
        conn.close()


def run_phase9_task3_phase5(output_dir: str = PHASE9_TASK3_REPLAY_OUTPUT_DIR) -> dict[str, Any]:
    apply_schema()
    cleanup_summary = cleanup_phase9_task3_state()
    raw_seed_summary = seed_phase9_task3_raw_archive()
    timestamp_summary = align_phase9_task3_historical_timestamps()
    market_state_summary = seed_phase9_task3_market_state()

    replay_bundle = run_phase5_replay_bundle(
        start=PHASE9_TASK2_START,
        end=PHASE9_TASK2_END,
        source_systems=PHASE9_TASK3_SOURCE_SYSTEMS,
        output_dir=output_dir,
        notes="phase9_task3 replay bundle for canonical validation window",
    ).to_dict()

    validation_report = build_phase5_person2_report(
        start=PHASE9_TASK2_START,
        end=PHASE9_TASK2_END,
    ).to_dict()

    repository = Phase5Repository()
    rows = repository.load_evaluation_rows(start=PHASE9_TASK2_START, end=PHASE9_TASK2_END)
    trader = ConservativePaperTrader(repository=repository)
    trades = [trade.to_dict() for trade in trader.simulate(rows)]

    validation_json_path = REPO_ROOT / PHASE9_TASK3_VALIDATION_JSON
    validation_md_path = REPO_ROOT / PHASE9_TASK3_VALIDATION_MD
    backtest_json_path = REPO_ROOT / PHASE9_TASK3_BACKTEST_JSON
    backtest_md_path = REPO_ROOT / PHASE9_TASK3_BACKTEST_MD
    paper_trades_json_path = REPO_ROOT / PHASE9_TASK3_PAPER_TRADES_JSON

    for path in [
        validation_json_path,
        validation_md_path,
        backtest_json_path,
        backtest_md_path,
        paper_trades_json_path,
    ]:
        path.parent.mkdir(parents=True, exist_ok=True)

    validation_json_path.write_text(json.dumps(validation_report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    validation_md_path.write_text(_render_validation_markdown(validation_report), encoding="utf-8")

    backtest_summary = {
        "task_contract_version": PHASE9_TASK3_CONTRACT_VERSION,
        "window": {
            "start": PHASE9_TASK2_START,
            "end": PHASE9_TASK2_END,
        },
        "replay": {
            "bundle_id": replay_bundle["bundle_id"],
            "overall_status": replay_bundle["overall_status"],
            "output_path": replay_bundle["output_path"],
        },
        "paper_trade_count": len(trades),
        "paper_trade_fills": sum(1 for trade in trades if trade["status"] in {"filled", "resolved"}),
        "paper_trade_skips": sum(1 for trade in trades if trade["status"] == "skipped"),
        "paper_trade_metrics": validation_report["metrics"]["paper_trade_overall"],
        "failure_metrics": validation_report["metrics"]["failure_overall"],
        "artifacts": {
            "paper_trades_path": PHASE9_TASK3_PAPER_TRADES_JSON,
            "validation_report_path": PHASE9_TASK3_VALIDATION_JSON,
        },
    }
    backtest_json_path.write_text(json.dumps(backtest_summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    backtest_md_path.write_text(_render_backtest_markdown(backtest_summary), encoding="utf-8")
    paper_trades_json_path.write_text(json.dumps(trades, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    validation_run_id = _record_validation_run(payload=validation_report, replay_bundle=replay_bundle)
    backtest_artifact_id = _record_backtest_artifact(summary=backtest_summary, replay_bundle=replay_bundle)

    payload = {
        "task_contract_version": PHASE9_TASK3_CONTRACT_VERSION,
        "task_name": "Phase 9 Task 3 - Replay, Validation, and Conservative Evaluation",
        "window": {
            "start": PHASE9_TASK2_START,
            "end": PHASE9_TASK2_END,
            "source_systems": PHASE9_TASK3_SOURCE_SYSTEMS,
            "reference_alignment": "Uses the same canonical reference hour as Task 1 and the same fixture candidate chain as Task 2.",
        },
        "cleanup_summary": cleanup_summary,
        "raw_archive_seed_summary": raw_seed_summary,
        "historical_timestamp_alignment": timestamp_summary,
        "market_state_summary": market_state_summary,
        "replay_bundle": replay_bundle,
        "validation_report": {
            "assessment": validation_report["assessment"],
            "coverage_summary": validation_report["coverage_summary"],
            "evaluation_row_count": validation_report["evaluation_row_count"],
            "alert_row_count": validation_report["alert_row_count"],
            "paper_trade_count": validation_report["paper_trade_count"],
        },
        "backtest_summary": backtest_summary,
        "artifacts": {
            "validation_json": PHASE9_TASK3_VALIDATION_JSON,
            "validation_markdown": PHASE9_TASK3_VALIDATION_MD,
            "backtest_json": PHASE9_TASK3_BACKTEST_JSON,
            "backtest_markdown": PHASE9_TASK3_BACKTEST_MD,
            "paper_trades_json": PHASE9_TASK3_PAPER_TRADES_JSON,
            "replay_bundle_artifact": replay_bundle["output_path"],
        },
        "database_rows": {
            "validation_run_id": validation_run_id,
            "backtest_artifact_id": backtest_artifact_id,
            "table_counts_after": _table_counts(),
        },
    }
    return payload
