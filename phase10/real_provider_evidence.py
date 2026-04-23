from __future__ import annotations

import asyncio
import json
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from config.settings import PHASE3_DETECTOR_VERSION, PHASE3_FEATURE_SCHEMA_VERSION, REPO_ROOT
from database.db_manager import apply_schema, get_conn
from phase3.detector import Phase3Repository, run_phase3_detector_window
from phase3.state_store import MemoryStateStore
from phase4 import Phase4AlertWorker, Phase4AnalystWorkflow, Phase4EvidenceWorker, Phase4Repository
from phase9.candidate_alert import (
    FIXTURE_MARKETS,
    PHASE9_TASK2_END,
    PHASE9_TASK2_START,
    cleanup_phase9_task2_state,
    seed_phase9_task2_detector_inputs,
    upsert_fixture_metadata,
)
from validation.phase3_candidate_report import build_phase3_candidate_report
from validation.phase4_gate4_report import build_phase4_gate4_report


PHASE10_TASK1_CONTRACT_VERSION = "phase10_task1_real_provider_evidence_v1"
PHASE10_TASK1_OUTPUT_DIR = "reports/phase10/real_provider_evidence_hardening"


def _iso_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _repo_relative(path: Path) -> str:
    try:
        return str(path.resolve().relative_to(REPO_ROOT.resolve())).replace("\\", "/")
    except ValueError:
        return str(path)


def _update_real_provider_fixture_metadata() -> None:
    conn = get_conn()
    now = _iso_now()
    try:
        conn.execute(
            """
            UPDATE events
            SET
                title = ?,
                description = ?,
                slug = ?,
                category = ?,
                last_updated_at = ?
            WHERE event_id = ?
            """,
            (
                "Bitcoin price and crypto market momentum",
                "Phase 10 Task 1 canonical evidence query fixture tied to real-provider-backed news search.",
                "bitcoin-price-and-crypto-market-momentum",
                "phase10_real_provider_fixture",
                now,
                "phase9_task2_event_a",
            ),
        )
        conn.execute(
            """
            UPDATE events
            SET
                title = ?,
                description = ?,
                slug = ?,
                category = ?,
                last_updated_at = ?
            WHERE event_id = ?
            """,
            (
                "Federal Reserve interest rates and inflation outlook",
                "Phase 10 Task 1 canonical evidence query fixture tied to real-provider-backed news search.",
                "federal-reserve-interest-rates-and-inflation-outlook",
                "phase10_real_provider_fixture",
                now,
                "phase9_task2_event_b",
            ),
        )
        conn.execute(
            """
            UPDATE markets
            SET
                question = ?,
                description = ?,
                slug = ?,
                last_updated_at = ?
            WHERE market_id = ?
            """,
            (
                "Will Bitcoin price momentum stay elevated through the replay-linked alert window?",
                "Phase 10 Task 1 market A with a news-searchable real-provider query target.",
                "bitcoin-price-momentum-alert-window",
                now,
                "phase9_task2_market_a",
            ),
        )
        conn.execute(
            """
            UPDATE markets
            SET
                question = ?,
                description = ?,
                slug = ?,
                last_updated_at = ?
            WHERE market_id = ?
            """,
            (
                "Will Federal Reserve rate expectations move through the replay-linked alert window?",
                "Phase 10 Task 1 market B with a news-searchable real-provider query target.",
                "federal-reserve-rate-expectations-alert-window",
                now,
                "phase9_task2_market_b",
            ),
        )
        conn.commit()
    finally:
        conn.close()


def _candidate_rows() -> list[dict[str, Any]]:
    market_ids = [market.market_id for market in FIXTURE_MARKETS]
    conn = get_conn()
    try:
        rows = conn.execute(
            f"""
            SELECT candidate_id, market_id, event_id, trigger_time
            FROM signal_candidates
            WHERE market_id IN ({", ".join("?" for _ in market_ids)})
            ORDER BY trigger_time DESC
            """,
            tuple(market_ids),
        ).fetchall()
    finally:
        conn.close()
    return [
        {
            "candidate_id": row["candidate_id"],
            "market_id": row["market_id"],
            "event_id": row["event_id"],
            "trigger_time": row["trigger_time"],
        }
        for row in rows
    ]


def _latest_alert_id_for_fixture() -> str:
    market_ids = [market.market_id for market in FIXTURE_MARKETS]
    conn = get_conn()
    try:
        row = conn.execute(
            f"""
            SELECT a.alert_id
            FROM alerts a
            JOIN signal_candidates sc ON sc.candidate_id = a.candidate_id
            WHERE sc.market_id IN ({", ".join("?" for _ in market_ids)})
            ORDER BY a.created_at DESC
            LIMIT 1
            """,
            tuple(market_ids),
        ).fetchone()
    finally:
        conn.close()
    if row is None:
        raise RuntimeError("No Phase 10 Task 1 alert was created for the fixture candidates.")
    return str(row["alert_id"])


def _load_evidence_query_rows(candidate_ids: list[str]) -> list[dict[str, Any]]:
    if not candidate_ids:
        return []
    conn = get_conn()
    try:
        rows = conn.execute(
            f"""
            SELECT
                evidence_query_id,
                candidate_id,
                provider_name,
                provider_query_type,
                provider_query_text,
                request_started_at,
                response_completed_at,
                latency_ms,
                result_count,
                query_status,
                timeout_seconds,
                raw_response_metadata,
                error_message,
                created_at
            FROM evidence_queries
            WHERE candidate_id IN ({", ".join("?" for _ in candidate_ids)})
            ORDER BY request_started_at ASC, created_at ASC
            """,
            tuple(candidate_ids),
        ).fetchall()
    finally:
        conn.close()
    payload: list[dict[str, Any]] = []
    for row in rows:
        metadata = json.loads(row["raw_response_metadata"] or "{}")
        payload.append(
            {
                "evidence_query_id": row["evidence_query_id"],
                "candidate_id": row["candidate_id"],
                "provider_name": row["provider_name"],
                "provider_query_type": row["provider_query_type"],
                "provider_query_text": row["provider_query_text"],
                "request_started_at": row["request_started_at"],
                "response_completed_at": row["response_completed_at"],
                "latency_ms": row["latency_ms"],
                "result_count": int(row["result_count"] or 0),
                "query_status": row["query_status"],
                "timeout_seconds": row["timeout_seconds"],
                "raw_response_metadata": metadata,
                "error_message": row["error_message"],
                "created_at": row["created_at"],
            }
        )
    return payload


def _table_count(table_name: str) -> int:
    conn = get_conn()
    try:
        row = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()
    finally:
        conn.close()
    return int((row[0] if row else 0) or 0)


def _summarize_real_provider_queries(query_rows: list[dict[str, Any]]) -> dict[str, Any]:
    real_rows = [row for row in query_rows if not str(row["provider_name"]).startswith("noop_")]
    provider_counts = Counter(row["provider_name"] for row in real_rows)
    status_counts = Counter(row["query_status"] for row in real_rows)
    provider_mode_counts = Counter(
        str((row["raw_response_metadata"] or {}).get("provider_mode") or "unknown")
        for row in real_rows
    )
    cache_hit_count = sum(1 for row in real_rows if bool((row["raw_response_metadata"] or {}).get("cache_hit")))
    live_call_count = sum(
        1
        for row in real_rows
        if str((row["raw_response_metadata"] or {}).get("provider_mode")) == "live"
    )
    budget_rollup: dict[str, dict[str, Any]] = defaultdict(dict)
    for row in real_rows:
        budget = ((row["raw_response_metadata"] or {}).get("budget") or {})
        provider_name = str(row["provider_name"])
        current = budget_rollup[provider_name]
        current["day_query_cap"] = budget.get("day_query_cap")
        current["month_query_cap"] = budget.get("month_query_cap")
        current["day_queries_after"] = max(
            int(current.get("day_queries_after") or 0),
            int(budget.get("day_queries_after") or 0),
        )
        current["month_queries_after"] = max(
            int(current.get("month_queries_after") or 0),
            int(budget.get("month_queries_after") or 0),
        )
        current["day_spend_after_usd"] = max(
            float(current.get("day_spend_after_usd") or 0.0),
            float(budget.get("day_spend_after_usd") or 0.0),
        )
        current["month_spend_after_usd"] = max(
            float(current.get("month_spend_after_usd") or 0.0),
            float(budget.get("month_spend_after_usd") or 0.0),
        )
    return {
        "real_provider_query_count": len(real_rows),
        "live_call_count": live_call_count,
        "cache_hit_count": cache_hit_count,
        "provider_counts": [
            {"provider_name": key, "count": value}
            for key, value in sorted(provider_counts.items())
        ],
        "query_status_counts": [
            {"query_status": key, "count": value}
            for key, value in sorted(status_counts.items())
        ],
        "provider_mode_counts": [
            {"provider_mode": key, "count": value}
            for key, value in sorted(provider_mode_counts.items())
        ],
        "budget_usage": [
            {"provider_name": key, **value}
            for key, value in sorted(budget_rollup.items())
        ],
    }


def render_phase10_task1_markdown(payload: dict[str, Any]) -> str:
    summary = payload["real_provider_summary"]
    latest = (payload["phase4"]["gate4_report"] or {}).get("latest_alert_example") or {}
    lines = [
        "# Phase 10 Task 1 - Real-Provider Evidence Hardening",
        "",
        f"- Contract version: `{payload['task_contract_version']}`",
        f"- Window: `{payload['window']['start']}` to `{payload['window']['end']}`",
        f"- Canonical alignment: `{payload['window']['alignment']}`",
        "",
        "## Evidence Workflow",
        f"- Live provider-backed query rows: `{summary['live_call_count']}`",
        f"- Cached query rows: `{summary['cache_hit_count']}`",
        f"- Real-provider query rows total: `{summary['real_provider_query_count']}`",
        "",
        "## Provider Modes",
    ]
    for row in summary["provider_mode_counts"]:
        lines.append(f"- `{row['provider_mode']}`: `{row['count']}`")
    lines.extend(["", "## Budget Usage"])
    for row in summary["budget_usage"]:
        lines.append(
            f"- `{row['provider_name']}` day `{row.get('day_queries_after')}` / `{row.get('day_query_cap')}`, "
            f"month `{row.get('month_queries_after')}` / `{row.get('month_query_cap')}`, "
            f"month spend `${row.get('month_spend_after_usd', 0.0):.6f}`"
        )
    lines.extend(
        [
            "",
            "## Latest Alert",
            f"- Alert id: `{latest.get('alert_id', 'none')}`",
            f"- Evidence state: `{latest.get('evidence_state', 'none')}`",
            f"- Provider summary present: `{bool(latest.get('provider_summary'))}`",
        ]
    )
    return "\n".join(lines) + "\n"


async def run_phase10_task1_real_provider_evidence(
    output_dir: str = PHASE10_TASK1_OUTPUT_DIR,
) -> dict[str, Any]:
    apply_schema()
    cleanup_summary = cleanup_phase9_task2_state()
    upsert_fixture_metadata()
    _update_real_provider_fixture_metadata()
    seed_summary = seed_phase9_task2_detector_inputs()

    store = MemoryStateStore()
    phase3_repository = Phase3Repository()
    phase3_repository.register_detector_version(
        backend_name=store.backend_name,
        notes="Phase 10 Task 1 real-provider evidence hardening over the canonical replay-linked fixture hour.",
    )
    detector_summary = await run_phase3_detector_window(
        start=PHASE9_TASK2_START,
        end=PHASE9_TASK2_END,
        store=store,
        repository=phase3_repository,
        source_systems=["phase9_seed_prices", "phase9_seed_trades"],
    )
    candidate_report = build_phase3_candidate_report(
        start=PHASE9_TASK2_START,
        end=PHASE9_TASK2_END,
    ).to_dict()

    phase4_repository = Phase4Repository()
    phase4_repository.register_workflow_version(
        notes="Phase 10 Task 1 real-provider evidence hardening over the canonical replay-linked fixture hour."
    )
    evidence_worker = Phase4EvidenceWorker(repository=phase4_repository)
    live_evidence_results = await evidence_worker.process_pending_candidates(limit=10)
    cache_evidence_results = await evidence_worker.process_pending_candidates(limit=10)

    alert_worker = Phase4AlertWorker(repository=phase4_repository)
    alert_results = alert_worker.process_pending_candidates(limit=10)
    analyst_workflow = Phase4AnalystWorkflow(repository=phase4_repository)
    analyst_result = analyst_workflow.record_action(
        alert_id=_latest_alert_id_for_fixture(),
        action_type="mark_useful",
        actor="phase10_task1",
        notes="Phase 10 Task 1 real-provider-backed evidence packet reviewed successfully.",
        follow_up_at="2026-04-20T06:15:00+00:00",
    )

    candidate_rows = _candidate_rows()
    candidate_ids = [str(row["candidate_id"]) for row in candidate_rows]
    query_rows = _load_evidence_query_rows(candidate_ids)
    real_provider_summary = _summarize_real_provider_queries(query_rows)
    if real_provider_summary["live_call_count"] == 0:
        raise RuntimeError(
            "Phase 10 Task 1 did not produce any live real-provider query rows. "
            "Configure network access and rerun the task."
        )
    if real_provider_summary["cache_hit_count"] == 0:
        raise RuntimeError(
            "Phase 10 Task 1 did not produce cached evidence-query rows. "
            "Cache behavior must be materially demonstrated for the canonical path."
        )

    gate4_report = build_phase4_gate4_report().to_dict()
    output_root = REPO_ROOT / output_dir
    output_root.mkdir(parents=True, exist_ok=True)
    review_packet_path = output_root / "phase10_task1_review_packet.json"
    review_summary_path = output_root / "phase10_task1_review_summary.md"

    payload = {
        "task_contract_version": PHASE10_TASK1_CONTRACT_VERSION,
        "task_name": "Phase 10 Task 1 - Real-Provider Evidence Hardening",
        "generated_at": _iso_now(),
        "window": {
            "start": PHASE9_TASK2_START,
            "end": PHASE9_TASK2_END,
            "alignment": "Reuses the canonical replay-linked Phase 9 reference hour and fixture detector-input path.",
        },
        "cleanup_summary": cleanup_summary,
        "seed_summary": seed_summary,
        "phase3": {
            "detector_version": PHASE3_DETECTOR_VERSION,
            "feature_schema_version": PHASE3_FEATURE_SCHEMA_VERSION,
            "detector_summary": detector_summary.to_dict(),
            "candidate_report": candidate_report,
            "candidate_rows": candidate_rows,
        },
        "phase4": {
            "live_evidence_results": live_evidence_results,
            "cached_evidence_results": cache_evidence_results,
            "evidence_summary": evidence_worker.summary.to_dict(),
            "alert_results": alert_results,
            "alert_summary": alert_worker.summary.to_dict(),
            "analyst_result": analyst_result,
            "gate4_report": gate4_report,
        },
        "real_provider_summary": real_provider_summary,
        "evidence_query_rows": query_rows,
        "table_counts_after": {
            "signal_candidates": _table_count("signal_candidates"),
            "evidence_queries": _table_count("evidence_queries"),
            "evidence_snapshots": _table_count("evidence_snapshots"),
            "alerts": _table_count("alerts"),
            "alert_delivery_attempts": _table_count("alert_delivery_attempts"),
            "analyst_feedback": _table_count("analyst_feedback"),
        },
        "review_packet_expectation": {
            "provider_workflow": "At least one real-provider-backed evidence adapter executed live requests and then served immediate cached replays.",
            "cache_behavior": "Evidence query metadata now records cache hit state, cache source linkage, cache TTL, and freshness.",
            "budget_behavior": "Evidence query metadata now records per-provider budget usage before and after each live call.",
            "replay_linkage": "The packet remains tied to the canonical replay-linked Phase 9 detector-input hour.",
        },
    }

    payload["artifacts"] = {
        "review_packet_path": _repo_relative(review_packet_path),
        "review_summary_path": _repo_relative(review_summary_path),
    }
    review_packet_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    review_summary_path.write_text(render_phase10_task1_markdown(payload), encoding="utf-8")
    return payload
