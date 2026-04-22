from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from config.settings import PHASE3_DETECTOR_VERSION, PHASE3_FEATURE_SCHEMA_VERSION, REPO_ROOT
from database.db_manager import apply_schema, get_conn
from phase3.detector import Phase3Repository, run_phase3_detector_window
from phase3.state_store import MemoryStateStore
from phase4 import (
    Phase4AlertWorker,
    Phase4AnalystWorkflow,
    Phase4EvidenceWorker,
    Phase4Repository,
)
from utils.event_log import DETECTOR_INPUT_ROOT, publish_detector_input
from validation.phase3_candidate_report import build_phase3_candidate_report
from validation.phase4_gate4_report import build_phase4_gate4_report


PHASE9_TASK2_CONTRACT_VERSION = "phase9_task2_candidate_to_alert_v1"
PHASE9_TASK2_START = "2026-04-20T05:00:00+00:00"
PHASE9_TASK2_END = "2026-04-20T06:00:00+00:00"

PHASE9_TASK2_EVENT_A = "phase9_task2_event_a"
PHASE9_TASK2_EVENT_B = "phase9_task2_event_b"
PHASE9_TASK2_MARKET_A = "phase9_task2_market_a"
PHASE9_TASK2_MARKET_B = "phase9_task2_market_b"
PHASE9_TASK2_SOURCE_TRADES = "phase9_seed_trades"
PHASE9_TASK2_SOURCE_PRICES = "phase9_seed_prices"


@dataclass(frozen=True, slots=True)
class Phase9FixtureEvent:
    event_id: str
    title: str
    slug: str
    category: str


@dataclass(frozen=True, slots=True)
class Phase9FixtureMarket:
    market_id: str
    event_id: str
    question: str
    condition_id: str
    yes_token_id: str
    no_token_id: str
    description: str


FIXTURE_EVENTS = (
    Phase9FixtureEvent(
        event_id=PHASE9_TASK2_EVENT_A,
        title="Phase 9 Task 2 Reference Event A",
        slug="phase9-task2-reference-event-a",
        category="phase9_fixture",
    ),
    Phase9FixtureEvent(
        event_id=PHASE9_TASK2_EVENT_B,
        title="Phase 9 Task 2 Reference Event B",
        slug="phase9-task2-reference-event-b",
        category="phase9_fixture",
    ),
)

FIXTURE_MARKETS = (
    Phase9FixtureMarket(
        market_id=PHASE9_TASK2_MARKET_A,
        event_id=PHASE9_TASK2_EVENT_A,
        question="Will the seeded Phase 9 candidate-to-alert flow materialize for market A?",
        condition_id="phase9-task2-condition-a",
        yes_token_id="phase9-task2-yes-a",
        no_token_id="phase9-task2-no-a",
        description="Phase 9 Task 2 seeded detector-input market A.",
    ),
    Phase9FixtureMarket(
        market_id=PHASE9_TASK2_MARKET_B,
        event_id=PHASE9_TASK2_EVENT_B,
        question="Will the seeded Phase 9 candidate-to-alert flow materialize for market B?",
        condition_id="phase9-task2-condition-b",
        yes_token_id="phase9-task2-yes-b",
        no_token_id="phase9-task2-no-b",
        description="Phase 9 Task 2 seeded detector-input market B.",
    ),
)


def _iso_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _window_hour_path(source_system: str) -> Path:
    return (
        DETECTOR_INPUT_ROOT
        / "year=2026"
        / "month=04"
        / "day=20"
        / "hour=05"
        / f"source_system={source_system}"
        / "events.ndjson"
    )


def _repo_relative(path: Path) -> str:
    try:
        return str(path.resolve().relative_to(REPO_ROOT.resolve())).replace("\\", "/")
    except ValueError:
        return str(path)


def _remove_file_if_exists(path: Path) -> None:
    if path.exists():
        path.unlink()
    parent = path.parent
    while parent != DETECTOR_INPUT_ROOT and parent.exists():
        try:
            parent.rmdir()
        except OSError:
            break
        parent = parent.parent


def _delete_rows_for_ids(conn, table: str, column: str, values: list[str]) -> None:
    if not values:
        return
    placeholders = ", ".join("?" for _ in values)
    conn.execute(f"DELETE FROM {table} WHERE {column} IN ({placeholders})", tuple(values))


def cleanup_phase9_task2_state() -> dict[str, Any]:
    market_ids = [market.market_id for market in FIXTURE_MARKETS]
    event_ids = [event.event_id for event in FIXTURE_EVENTS]
    sources = [PHASE9_TASK2_SOURCE_TRADES, PHASE9_TASK2_SOURCE_PRICES]

    conn = get_conn()
    deleted_candidate_ids: list[str] = []
    deleted_alert_ids: list[str] = []
    try:
        candidate_rows = conn.execute(
            f"""
            SELECT candidate_id
            FROM signal_candidates
            WHERE market_id IN ({", ".join("?" for _ in market_ids)})
               OR event_id IN ({", ".join("?" for _ in event_ids)})
            """,
            tuple(market_ids + event_ids),
        ).fetchall()
        deleted_candidate_ids = [str(row["candidate_id"]) for row in candidate_rows]

        if deleted_candidate_ids:
            alert_rows = conn.execute(
                f"""
                SELECT alert_id
                FROM alerts
                WHERE candidate_id IN ({", ".join("?" for _ in deleted_candidate_ids)})
                """,
                tuple(deleted_candidate_ids),
            ).fetchall()
            deleted_alert_ids = [str(row["alert_id"]) for row in alert_rows]

        _delete_rows_for_ids(conn, "analyst_feedback", "alert_id", deleted_alert_ids)
        _delete_rows_for_ids(conn, "alert_delivery_attempts", "alert_id", deleted_alert_ids)
        if deleted_candidate_ids:
            placeholders = ", ".join("?" for _ in deleted_candidate_ids)
            conn.execute(
                f"DELETE FROM evidence_queries WHERE candidate_id IN ({placeholders})",
                tuple(deleted_candidate_ids),
            )
            conn.execute(
                f"DELETE FROM evidence_snapshots WHERE candidate_id IN ({placeholders})",
                tuple(deleted_candidate_ids),
            )
            conn.execute(
                f"DELETE FROM alerts WHERE candidate_id IN ({placeholders})",
                tuple(deleted_candidate_ids),
            )
            conn.execute(
                f"DELETE FROM signal_features WHERE candidate_id IN ({placeholders})",
                tuple(deleted_candidate_ids),
            )
            conn.execute(
                f"DELETE FROM signal_candidates WHERE candidate_id IN ({placeholders})",
                tuple(deleted_candidate_ids),
            )

        conn.execute(
            f"""
            DELETE FROM signal_episodes
            WHERE market_id IN ({", ".join("?" for _ in market_ids)})
               OR event_id IN ({", ".join("?" for _ in event_ids)})
            """,
            tuple(market_ids + event_ids),
        )
        conn.execute(
            f"DELETE FROM detector_checkpoints WHERE source_system IN ({', '.join('?' for _ in sources)})",
            tuple(sources),
        )
        conn.execute(
            f"DELETE FROM detector_input_manifests WHERE source_system IN ({', '.join('?' for _ in sources)})",
            tuple(sources),
        )
        conn.commit()
    finally:
        conn.close()

    for source_system in sources:
        _remove_file_if_exists(_window_hour_path(source_system))

    return {
        "deleted_candidate_count": len(deleted_candidate_ids),
        "deleted_alert_count": len(deleted_alert_ids),
        "cleared_sources": sources,
    }


def upsert_fixture_metadata() -> None:
    conn = get_conn()
    now = _iso_now()
    try:
        for event in FIXTURE_EVENTS:
            conn.execute(
                """
                INSERT INTO events (
                    event_id,
                    title,
                    description,
                    slug,
                    category,
                    status,
                    first_seen_at,
                    last_updated_at
                ) VALUES (?, ?, ?, ?, ?, 'active', ?, ?)
                ON CONFLICT(event_id) DO UPDATE SET
                    title = excluded.title,
                    description = excluded.description,
                    slug = excluded.slug,
                    category = excluded.category,
                    status = excluded.status,
                    last_updated_at = excluded.last_updated_at
                """,
                (
                    event.event_id,
                    event.title,
                    f"Fixture event for {event.title}.",
                    event.slug,
                    event.category,
                    now,
                    now,
                ),
            )
        for market in FIXTURE_MARKETS:
            conn.execute(
                """
                INSERT INTO markets (
                    market_id,
                    event_id,
                    question,
                    description,
                    slug,
                    condition_id,
                    yes_token_id,
                    no_token_id,
                    tier,
                    status,
                    accepts_orders,
                    enable_order_book,
                    first_seen_at,
                    last_updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 1, 'active', 1, 1, ?, ?)
                ON CONFLICT(market_id) DO UPDATE SET
                    event_id = excluded.event_id,
                    question = excluded.question,
                    description = excluded.description,
                    slug = excluded.slug,
                    condition_id = excluded.condition_id,
                    yes_token_id = excluded.yes_token_id,
                    no_token_id = excluded.no_token_id,
                    tier = excluded.tier,
                    status = excluded.status,
                    accepts_orders = excluded.accepts_orders,
                    enable_order_book = excluded.enable_order_book,
                    last_updated_at = excluded.last_updated_at
                """,
                (
                    market.market_id,
                    market.event_id,
                    market.question,
                    market.description,
                    market.question.lower().replace(" ", "-").replace("?", ""),
                    market.condition_id,
                    market.yes_token_id,
                    market.no_token_id,
                    now,
                    now,
                ),
            )
        conn.commit()
    finally:
        conn.close()


def _publish_price_envelopes() -> list[dict[str, Any]]:
    written: list[dict[str, Any]] = []
    envelopes = [
        (
            "2026-04-20T05:00:00+00:00",
            "phase9-prices-001",
            [
                {"market_id": PHASE9_TASK2_MARKET_A, "captured_at": "2026-04-20T05:00:00+00:00", "yes_price": 0.40},
            ],
        ),
        (
            "2026-04-20T05:02:00+00:00",
            "phase9-prices-002",
            [
                {"market_id": PHASE9_TASK2_MARKET_A, "captured_at": "2026-04-20T05:02:00+00:00", "yes_price": 0.42},
            ],
        ),
        (
            "2026-04-20T05:04:00+00:00",
            "phase9-prices-003",
            [
                {"market_id": PHASE9_TASK2_MARKET_A, "captured_at": "2026-04-20T05:04:00+00:00", "yes_price": 0.46},
            ],
        ),
        (
            "2026-04-20T05:05:30+00:00",
            "phase9-prices-004",
            [
                {"market_id": PHASE9_TASK2_MARKET_A, "captured_at": "2026-04-20T05:05:30+00:00", "yes_price": 0.52},
            ],
        ),
        (
            "2026-04-20T05:12:00+00:00",
            "phase9-prices-005",
            [
                {"market_id": PHASE9_TASK2_MARKET_B, "captured_at": "2026-04-20T05:12:00+00:00", "yes_price": 0.35},
            ],
        ),
        (
            "2026-04-20T05:14:00+00:00",
            "phase9-prices-006",
            [
                {"market_id": PHASE9_TASK2_MARKET_B, "captured_at": "2026-04-20T05:14:00+00:00", "yes_price": 0.37},
            ],
        ),
        (
            "2026-04-20T05:16:00+00:00",
            "phase9-prices-007",
            [
                {"market_id": PHASE9_TASK2_MARKET_B, "captured_at": "2026-04-20T05:16:00+00:00", "yes_price": 0.44},
            ],
        ),
        (
            "2026-04-20T05:17:30+00:00",
            "phase9-prices-008",
            [
                {"market_id": PHASE9_TASK2_MARKET_B, "captured_at": "2026-04-20T05:17:30+00:00", "yes_price": 0.50},
            ],
        ),
    ]
    for captured_at, ordering_key, market_snapshots in envelopes:
        result = publish_detector_input(
            source_system=PHASE9_TASK2_SOURCE_PRICES,
            entity_type="prices_batch",
            payload={"market_snapshots": market_snapshots},
            captured_at=captured_at,
            ordering_key=ordering_key,
            raw_partition_path="phase9/task2/seeded_prices",
        )
        written.append(
            {
                "captured_at": captured_at,
                "ordering_key": ordering_key,
                "partition_path": result.partition_path,
                "envelope_id": result.envelope_id,
                "market_ids": [item["market_id"] for item in market_snapshots],
            }
        )
    return written


def _publish_trade_envelopes() -> list[dict[str, Any]]:
    written: list[dict[str, Any]] = []
    envelopes = [
        (
            "2026-04-20T05:02:00+00:00",
            "phase9-trades-001",
            [
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
            ],
        ),
        (
            "2026-04-20T05:06:00+00:00",
            "phase9-trades-002",
            [
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
            ],
        ),
        (
            "2026-04-20T05:14:00+00:00",
            "phase9-trades-003",
            [
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
            ],
        ),
        (
            "2026-04-20T05:18:00+00:00",
            "phase9-trades-004",
            [
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
            ],
        ),
    ]
    for captured_at, ordering_key, trades in envelopes:
        result = publish_detector_input(
            source_system=PHASE9_TASK2_SOURCE_TRADES,
            entity_type="recent_trades_page",
            payload={"trades": trades},
            captured_at=captured_at,
            ordering_key=ordering_key,
            raw_partition_path="phase9/task2/seeded_trades",
        )
        written.append(
            {
                "captured_at": captured_at,
                "ordering_key": ordering_key,
                "partition_path": result.partition_path,
                "envelope_id": result.envelope_id,
                "trade_ids": [item["trade_id"] for item in trades],
                "market_ids": sorted({item["market_id"] for item in trades}),
            }
        )
    return written


def seed_phase9_task2_detector_inputs() -> dict[str, Any]:
    return {
        "prices": _publish_price_envelopes(),
        "trades": _publish_trade_envelopes(),
        "detector_input_files": [
            _repo_relative(_window_hour_path(PHASE9_TASK2_SOURCE_PRICES)),
            _repo_relative(_window_hour_path(PHASE9_TASK2_SOURCE_TRADES)),
        ],
    }


def _table_count(table_name: str) -> int:
    conn = get_conn()
    try:
        row = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()
    finally:
        conn.close()
    return int((row[0] if row else 0) or 0)


def _latest_alert_id_for_fixture() -> str:
    conn = get_conn()
    try:
        row = conn.execute(
            f"""
            SELECT a.alert_id
            FROM alerts a
            JOIN signal_candidates sc ON sc.candidate_id = a.candidate_id
            WHERE sc.market_id IN ({", ".join("?" for _ in FIXTURE_MARKETS)})
            ORDER BY a.created_at DESC
            LIMIT 1
            """,
            tuple(market.market_id for market in FIXTURE_MARKETS),
        ).fetchone()
    finally:
        conn.close()
    if row is None:
        raise RuntimeError("No alert was created for the Phase 9 Task 2 fixture markets.")
    return str(row["alert_id"])


def _fixture_table_counts() -> dict[str, int]:
    return {
        "signal_candidates": _table_count("signal_candidates"),
        "signal_episodes": _table_count("signal_episodes"),
        "signal_features": _table_count("signal_features"),
        "evidence_queries": _table_count("evidence_queries"),
        "evidence_snapshots": _table_count("evidence_snapshots"),
        "alerts": _table_count("alerts"),
        "alert_delivery_attempts": _table_count("alert_delivery_attempts"),
        "analyst_feedback": _table_count("analyst_feedback"),
        "detector_input_manifests": _table_count("detector_input_manifests"),
    }


async def materialize_phase9_task2(output_dir: str = "reports/phase9/candidate_to_alert_materialization") -> dict[str, Any]:
    apply_schema()
    cleanup_summary = cleanup_phase9_task2_state()
    upsert_fixture_metadata()
    seed_summary = seed_phase9_task2_detector_inputs()

    store = MemoryStateStore()
    phase3_repository = Phase3Repository()
    phase3_repository.register_detector_version(
        backend_name=store.backend_name,
        notes="Phase 9 Task 2 seeded detector-input materialization using in-memory state.",
    )
    detector_summary = await run_phase3_detector_window(
        start=PHASE9_TASK2_START,
        end=PHASE9_TASK2_END,
        store=store,
        repository=phase3_repository,
        source_systems=[PHASE9_TASK2_SOURCE_PRICES, PHASE9_TASK2_SOURCE_TRADES],
    )

    phase3_candidate_report = build_phase3_candidate_report(
        start=PHASE9_TASK2_START,
        end=PHASE9_TASK2_END,
    ).to_dict()

    phase4_repository = Phase4Repository()
    phase4_repository.register_workflow_version(
        notes="Phase 9 Task 2 candidate-to-alert materialization over seeded detector-input envelopes."
    )
    evidence_worker = Phase4EvidenceWorker(repository=phase4_repository)
    evidence_results = await evidence_worker.process_pending_candidates(limit=10)
    alert_worker = Phase4AlertWorker(repository=phase4_repository)
    alert_results = alert_worker.process_pending_candidates(limit=10)

    analyst_workflow = Phase4AnalystWorkflow(repository=phase4_repository)
    analyst_result = analyst_workflow.record_action(
        alert_id=_latest_alert_id_for_fixture(),
        action_type="mark_useful",
        actor="phase9_task2",
        notes="Seeded Phase 9 Task 2 candidate-to-alert materialization reviewed successfully.",
        follow_up_at="2026-04-20T06:05:00+00:00",
    )

    gate4_report = build_phase4_gate4_report().to_dict()

    task1_gate4_path = REPO_ROOT / "reports" / "phase9" / "reference_window_preparation" / "phase9_task1_gate4_report.json"
    task1_gate4_path.parent.mkdir(parents=True, exist_ok=True)
    task1_gate4_path.write_text(json.dumps(gate4_report, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    output_root = REPO_ROOT / output_dir
    output_root.mkdir(parents=True, exist_ok=True)
    review_packet_path = output_root / "phase9_task2_review_packet.json"
    review_summary_path = output_root / "phase9_task2_review_summary.md"

    payload = {
        "task_contract_version": PHASE9_TASK2_CONTRACT_VERSION,
        "task_name": "Phase 9 Task 2 - Candidate-to-Alert Materialization",
        "window": {
            "start": PHASE9_TASK2_START,
            "end": PHASE9_TASK2_END,
            "reference_alignment": "Uses the same canonical Phase 9 hour selected in Task 1.",
            "detector_input_source_systems": [PHASE9_TASK2_SOURCE_PRICES, PHASE9_TASK2_SOURCE_TRADES],
        },
        "cleanup_summary": cleanup_summary,
        "seed_summary": seed_summary,
        "phase3": {
            "detector_version": PHASE3_DETECTOR_VERSION,
            "feature_schema_version": PHASE3_FEATURE_SCHEMA_VERSION,
            "detector_summary": detector_summary.to_dict(),
            "candidate_report": phase3_candidate_report,
        },
        "phase4": {
            "evidence_results": evidence_results,
            "evidence_summary": evidence_worker.summary.to_dict(),
            "alert_results": alert_results,
            "alert_summary": alert_worker.summary.to_dict(),
            "analyst_result": analyst_result,
            "gate4_report": gate4_report,
            "task1_gate4_report_path": _repo_relative(task1_gate4_path),
        },
        "table_counts_after": _fixture_table_counts(),
        "review_packet_expectation": {
            "what_triggered": "Phase 3 candidate rows seeded through the native detector-input path.",
            "what_evidence_existed": "Phase 4 noop evidence queries and snapshots recorded for each candidate.",
            "what_was_delivered": "Alert delivery attempts were persisted even with disabled outbound channels.",
            "how_analyst_responded": "One analyst feedback row was recorded against the latest fixture alert.",
        },
    }

    review_packet_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    review_summary_path.write_text(render_phase9_task2_markdown(payload), encoding="utf-8")
    payload["artifacts"] = {
        "review_packet_path": _repo_relative(review_packet_path),
        "review_summary_path": _repo_relative(review_summary_path),
    }
    review_packet_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return payload


def render_phase9_task2_markdown(payload: dict[str, Any]) -> str:
    phase3 = payload["phase3"]
    phase4 = payload["phase4"]
    counts = payload["table_counts_after"]
    latest = phase4["gate4_report"].get("latest_alert_example") or {}
    lines = [
        "# Phase 9 Task 2 - Candidate-to-Alert Materialization",
        "",
        f"- Contract version: `{payload['task_contract_version']}`",
        f"- Window: `{payload['window']['start']}` to `{payload['window']['end']}`",
        f"- Detector-input source systems: `{', '.join(payload['window']['detector_input_source_systems'])}`",
        "",
        "## Phase 3 Outcome",
        f"- Processed envelopes: `{phase3['detector_summary']['processed_envelopes']}`",
        f"- Candidates emitted: `{phase3['detector_summary']['candidates_emitted']}`",
        f"- Total persisted candidates in window: `{phase3['candidate_report']['total_candidates']}`",
        "",
        "## Phase 4 Outcome",
        f"- Evidence queries written: `{phase4['evidence_summary']['evidence_queries_written']}`",
        f"- Evidence snapshots written: `{phase4['evidence_summary']['evidence_snapshots_written']}`",
        f"- Alerts created: `{phase4['alert_summary']['alerts_created']}`",
        f"- Delivery attempts written: `{phase4['alert_summary']['delivery_attempts_written']}`",
        f"- Analyst action: `{phase4['analyst_result']['action_type']}` by `{phase4['analyst_result']['actor']}`",
        "",
        "## Table Counts",
        f"- `signal_candidates`: `{counts['signal_candidates']}`",
        f"- `evidence_queries`: `{counts['evidence_queries']}`",
        f"- `evidence_snapshots`: `{counts['evidence_snapshots']}`",
        f"- `alerts`: `{counts['alerts']}`",
        f"- `alert_delivery_attempts`: `{counts['alert_delivery_attempts']}`",
        f"- `analyst_feedback`: `{counts['analyst_feedback']}`",
        "",
        "## Review Packet",
        f"- Latest alert id: `{latest.get('alert_id', 'none')}`",
        f"- Latest alert status: `{latest.get('alert_status', 'none')}`",
        f"- Latest evidence state: `{latest.get('evidence_state', 'none')}`",
    ]
    if latest.get("title"):
        lines.append(f"- Latest alert title: `{latest['title']}`")
    return "\n".join(lines) + "\n"
