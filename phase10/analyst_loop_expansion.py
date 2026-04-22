from __future__ import annotations

import asyncio
import json
from collections import Counter
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
    upsert_fixture_metadata,
)
from utils.event_log import DETECTOR_INPUT_ROOT, publish_detector_input
from validation.phase3_candidate_report import build_phase3_candidate_report
from validation.phase4_gate4_report import build_phase4_gate4_report


PHASE10_TASK2_CONTRACT_VERSION = "phase10_task2_analyst_loop_expansion_v1"
PHASE10_TASK2_OUTPUT_DIR = "reports/phase10/analyst_loop_expansion"
PHASE10_TASK2_SOURCE_TRADES = "phase10_task2_seed_trades"
PHASE10_TASK2_SOURCE_PRICES = "phase10_task2_seed_prices"

PHASE10_TASK2_EVENT_A = "phase9_task2_event_a"
PHASE10_TASK2_EVENT_B = "phase9_task2_event_b"
PHASE10_TASK2_MARKET_A = "phase9_task2_market_a"
PHASE10_TASK2_MARKET_B = "phase9_task2_market_b"


def _iso_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _repo_relative(path: Path) -> str:
    try:
        return str(path.resolve().relative_to(REPO_ROOT.resolve())).replace("\\", "/")
    except ValueError:
        return str(path)


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


def _delete_task2_source_state() -> dict[str, Any]:
    conn = get_conn()
    try:
        conn.execute(
            """
            DELETE FROM detector_checkpoints
            WHERE source_system IN (?, ?)
            """,
            (PHASE10_TASK2_SOURCE_TRADES, PHASE10_TASK2_SOURCE_PRICES),
        )
        conn.execute(
            """
            DELETE FROM detector_input_manifests
            WHERE source_system IN (?, ?)
            """,
            (PHASE10_TASK2_SOURCE_TRADES, PHASE10_TASK2_SOURCE_PRICES),
        )
        conn.commit()
    finally:
        conn.close()

    for source_system in (PHASE10_TASK2_SOURCE_TRADES, PHASE10_TASK2_SOURCE_PRICES):
        _remove_file_if_exists(_window_hour_path(source_system))

    return {
        "cleared_sources": [PHASE10_TASK2_SOURCE_TRADES, PHASE10_TASK2_SOURCE_PRICES],
    }


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
                "Phase 10 canonical analyst-loop fixture tied to real-provider-backed alert episodes.",
                "bitcoin-price-and-crypto-market-momentum",
                "phase10_analyst_loop_fixture",
                now,
                PHASE10_TASK2_EVENT_A,
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
                "Phase 10 canonical analyst-loop fixture tied to real-provider-backed alert episodes.",
                "federal-reserve-interest-rates-and-inflation-outlook",
                "phase10_analyst_loop_fixture",
                now,
                PHASE10_TASK2_EVENT_B,
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
                "Will Bitcoin price momentum stay elevated through the replay-linked alert windows?",
                "Phase 10 Task 2 market A with two replay-linked candidate episodes for suppression review.",
                "bitcoin-price-momentum-alert-windows",
                now,
                PHASE10_TASK2_MARKET_A,
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
                "Phase 10 Task 2 market B with one replay-linked real-provider alert episode.",
                "federal-reserve-rate-expectations-alert-window",
                now,
                PHASE10_TASK2_MARKET_B,
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
            "phase10-task2-prices-001",
            [{"market_id": PHASE10_TASK2_MARKET_A, "captured_at": "2026-04-20T05:00:00+00:00", "yes_price": 0.40}],
        ),
        (
            "2026-04-20T05:02:00+00:00",
            "phase10-task2-prices-002",
            [{"market_id": PHASE10_TASK2_MARKET_A, "captured_at": "2026-04-20T05:02:00+00:00", "yes_price": 0.43}],
        ),
        (
            "2026-04-20T05:04:00+00:00",
            "phase10-task2-prices-003",
            [{"market_id": PHASE10_TASK2_MARKET_A, "captured_at": "2026-04-20T05:04:00+00:00", "yes_price": 0.47}],
        ),
        (
            "2026-04-20T05:05:30+00:00",
            "phase10-task2-prices-004",
            [{"market_id": PHASE10_TASK2_MARKET_A, "captured_at": "2026-04-20T05:05:30+00:00", "yes_price": 0.53}],
        ),
        (
            "2026-04-20T05:12:00+00:00",
            "phase10-task2-prices-005",
            [{"market_id": PHASE10_TASK2_MARKET_B, "captured_at": "2026-04-20T05:12:00+00:00", "yes_price": 0.35}],
        ),
        (
            "2026-04-20T05:14:00+00:00",
            "phase10-task2-prices-006",
            [{"market_id": PHASE10_TASK2_MARKET_B, "captured_at": "2026-04-20T05:14:00+00:00", "yes_price": 0.38}],
        ),
        (
            "2026-04-20T05:16:00+00:00",
            "phase10-task2-prices-007",
            [{"market_id": PHASE10_TASK2_MARKET_B, "captured_at": "2026-04-20T05:16:00+00:00", "yes_price": 0.45}],
        ),
        (
            "2026-04-20T05:17:30+00:00",
            "phase10-task2-prices-008",
            [{"market_id": PHASE10_TASK2_MARKET_B, "captured_at": "2026-04-20T05:17:30+00:00", "yes_price": 0.50}],
        ),
        (
            "2026-04-20T05:18:00+00:00",
            "phase10-task2-prices-009",
            [{"market_id": PHASE10_TASK2_MARKET_A, "captured_at": "2026-04-20T05:18:00+00:00", "yes_price": 0.54}],
        ),
        (
            "2026-04-20T05:20:00+00:00",
            "phase10-task2-prices-010",
            [{"market_id": PHASE10_TASK2_MARKET_A, "captured_at": "2026-04-20T05:20:00+00:00", "yes_price": 0.57}],
        ),
        (
            "2026-04-20T05:21:00+00:00",
            "phase10-task2-prices-011",
            [{"market_id": PHASE10_TASK2_MARKET_A, "captured_at": "2026-04-20T05:21:00+00:00", "yes_price": 0.61}],
        ),
        (
            "2026-04-20T05:22:30+00:00",
            "phase10-task2-prices-012",
            [{"market_id": PHASE10_TASK2_MARKET_A, "captured_at": "2026-04-20T05:22:30+00:00", "yes_price": 0.67}],
        ),
    ]
    for captured_at, ordering_key, market_snapshots in envelopes:
        result = publish_detector_input(
            source_system=PHASE10_TASK2_SOURCE_PRICES,
            entity_type="prices_batch",
            payload={"market_snapshots": market_snapshots},
            captured_at=captured_at,
            ordering_key=ordering_key,
            raw_partition_path="phase10/task2/seeded_prices",
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
            "phase10-task2-trades-001",
            [
                {
                    "trade_id": "phase10-task2-a1-old-1",
                    "market_id": PHASE10_TASK2_MARKET_A,
                    "trade_time": "2026-04-20T05:02:00+00:00",
                    "proxy_wallet": "0xphase10a1old1",
                    "side": "BUY",
                    "outcome_side": "YES",
                    "usdc_notional": 60.0,
                },
                {
                    "trade_id": "phase10-task2-a1-old-2",
                    "market_id": PHASE10_TASK2_MARKET_A,
                    "trade_time": "2026-04-20T05:02:10+00:00",
                    "proxy_wallet": "0xphase10a1old2",
                    "side": "SELL",
                    "outcome_side": "YES",
                    "usdc_notional": 40.0,
                },
            ],
        ),
        (
            "2026-04-20T05:06:00+00:00",
            "phase10-task2-trades-002",
            [
                {
                    "trade_id": "phase10-task2-a1-fresh-1",
                    "market_id": PHASE10_TASK2_MARKET_A,
                    "trade_time": "2026-04-20T05:06:00+00:00",
                    "proxy_wallet": "0xphase10a1fresh1",
                    "side": "BUY",
                    "outcome_side": "YES",
                    "usdc_notional": 180.0,
                },
                {
                    "trade_id": "phase10-task2-a1-fresh-2",
                    "market_id": PHASE10_TASK2_MARKET_A,
                    "trade_time": "2026-04-20T05:06:10+00:00",
                    "proxy_wallet": "0xphase10a1fresh2",
                    "side": "BUY",
                    "outcome_side": "YES",
                    "usdc_notional": 80.0,
                },
                {
                    "trade_id": "phase10-task2-a1-fresh-3",
                    "market_id": PHASE10_TASK2_MARKET_A,
                    "trade_time": "2026-04-20T05:06:20+00:00",
                    "proxy_wallet": "0xphase10a1fresh3",
                    "side": "BUY",
                    "outcome_side": "YES",
                    "usdc_notional": 40.0,
                },
            ],
        ),
        (
            "2026-04-20T05:14:00+00:00",
            "phase10-task2-trades-003",
            [
                {
                    "trade_id": "phase10-task2-b-old-1",
                    "market_id": PHASE10_TASK2_MARKET_B,
                    "trade_time": "2026-04-20T05:14:00+00:00",
                    "proxy_wallet": "0xphase10bold1",
                    "side": "BUY",
                    "outcome_side": "YES",
                    "usdc_notional": 70.0,
                },
                {
                    "trade_id": "phase10-task2-b-old-2",
                    "market_id": PHASE10_TASK2_MARKET_B,
                    "trade_time": "2026-04-20T05:14:10+00:00",
                    "proxy_wallet": "0xphase10bold2",
                    "side": "SELL",
                    "outcome_side": "YES",
                    "usdc_notional": 30.0,
                },
            ],
        ),
        (
            "2026-04-20T05:18:00+00:00",
            "phase10-task2-trades-004",
            [
                {
                    "trade_id": "phase10-task2-b-fresh-1",
                    "market_id": PHASE10_TASK2_MARKET_B,
                    "trade_time": "2026-04-20T05:18:00+00:00",
                    "proxy_wallet": "0xphase10bfresh1",
                    "side": "BUY",
                    "outcome_side": "YES",
                    "usdc_notional": 160.0,
                },
                {
                    "trade_id": "phase10-task2-b-fresh-2",
                    "market_id": PHASE10_TASK2_MARKET_B,
                    "trade_time": "2026-04-20T05:18:10+00:00",
                    "proxy_wallet": "0xphase10bfresh2",
                    "side": "BUY",
                    "outcome_side": "YES",
                    "usdc_notional": 90.0,
                },
                {
                    "trade_id": "phase10-task2-b-fresh-3",
                    "market_id": PHASE10_TASK2_MARKET_B,
                    "trade_time": "2026-04-20T05:18:20+00:00",
                    "proxy_wallet": "0xphase10bfresh3",
                    "side": "BUY",
                    "outcome_side": "YES",
                    "usdc_notional": 50.0,
                },
            ],
        ),
        (
            "2026-04-20T05:18:10+00:00",
            "phase10-task2-trades-005",
            [
                {
                    "trade_id": "phase10-task2-a2-old-1",
                    "market_id": PHASE10_TASK2_MARKET_A,
                    "trade_time": "2026-04-20T05:18:10+00:00",
                    "proxy_wallet": "0xphase10a2old1",
                    "side": "BUY",
                    "outcome_side": "YES",
                    "usdc_notional": 70.0,
                },
                {
                    "trade_id": "phase10-task2-a2-old-2",
                    "market_id": PHASE10_TASK2_MARKET_A,
                    "trade_time": "2026-04-20T05:18:20+00:00",
                    "proxy_wallet": "0xphase10a2old2",
                    "side": "SELL",
                    "outcome_side": "YES",
                    "usdc_notional": 30.0,
                },
            ],
        ),
        (
            "2026-04-20T05:22:00+00:00",
            "phase10-task2-trades-006",
            [
                {
                    "trade_id": "phase10-task2-a2-fresh-1",
                    "market_id": PHASE10_TASK2_MARKET_A,
                    "trade_time": "2026-04-20T05:22:00+00:00",
                    "proxy_wallet": "0xphase10a2fresh1",
                    "side": "BUY",
                    "outcome_side": "YES",
                    "usdc_notional": 170.0,
                },
                {
                    "trade_id": "phase10-task2-a2-fresh-2",
                    "market_id": PHASE10_TASK2_MARKET_A,
                    "trade_time": "2026-04-20T05:22:10+00:00",
                    "proxy_wallet": "0xphase10a2fresh2",
                    "side": "BUY",
                    "outcome_side": "YES",
                    "usdc_notional": 90.0,
                },
                {
                    "trade_id": "phase10-task2-a2-fresh-3",
                    "market_id": PHASE10_TASK2_MARKET_A,
                    "trade_time": "2026-04-20T05:22:20+00:00",
                    "proxy_wallet": "0xphase10a2fresh3",
                    "side": "BUY",
                    "outcome_side": "YES",
                    "usdc_notional": 50.0,
                },
            ],
        ),
    ]
    for captured_at, ordering_key, trades in envelopes:
        result = publish_detector_input(
            source_system=PHASE10_TASK2_SOURCE_TRADES,
            entity_type="recent_trades_page",
            payload={"trades": trades},
            captured_at=captured_at,
            ordering_key=ordering_key,
            raw_partition_path="phase10/task2/seeded_trades",
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


def seed_phase10_task2_detector_inputs() -> dict[str, Any]:
    return {
        "prices": _publish_price_envelopes(),
        "trades": _publish_trade_envelopes(),
        "detector_input_files": [
            _repo_relative(_window_hour_path(PHASE10_TASK2_SOURCE_PRICES)),
            _repo_relative(_window_hour_path(PHASE10_TASK2_SOURCE_TRADES)),
        ],
    }


def _fixture_candidates() -> list[dict[str, Any]]:
    market_ids = [market.market_id for market in FIXTURE_MARKETS]
    conn = get_conn()
    try:
        rows = conn.execute(
            f"""
            SELECT
                sc.candidate_id,
                sc.market_id,
                sc.event_id,
                sc.event_family_id,
                sc.trigger_time,
                sc.detector_version,
                sc.feature_schema_version,
                sc.severity_score,
                sc.triggering_rules,
                sc.feature_snapshot,
                m.question,
                e.title AS event_title,
                e.slug AS event_slug
            FROM signal_candidates sc
            LEFT JOIN markets m ON m.market_id = sc.market_id
            LEFT JOIN events e ON e.event_id = sc.event_id
            WHERE sc.market_id IN ({", ".join("?" for _ in market_ids)})
              AND sc.trigger_time >= ?
              AND sc.trigger_time < ?
            ORDER BY sc.trigger_time ASC, sc.candidate_id ASC
            """,
            tuple(market_ids) + (PHASE9_TASK2_START, PHASE9_TASK2_END),
        ).fetchall()
    finally:
        conn.close()
    return [
        {
            "candidate_id": row["candidate_id"],
            "market_id": row["market_id"],
            "event_id": row["event_id"],
            "event_family_id": row["event_family_id"],
            "trigger_time": row["trigger_time"],
            "detector_version": row["detector_version"],
            "feature_schema_version": row["feature_schema_version"],
            "severity_score": row["severity_score"],
            "triggering_rules": json.loads(row["triggering_rules"] or "[]"),
            "feature_snapshot": json.loads(row["feature_snapshot"] or "{}"),
            "question": row["question"],
            "event_title": row["event_title"],
            "event_slug": row["event_slug"],
        }
        for row in rows
    ]


def _fixture_alert_rows() -> list[dict[str, Any]]:
    market_ids = [market.market_id for market in FIXTURE_MARKETS]
    conn = get_conn()
    try:
        rows = conn.execute(
            f"""
            SELECT
                a.alert_id,
                a.candidate_id,
                a.severity,
                a.alert_status,
                a.title,
                a.suppression_key,
                a.suppression_state,
                a.created_at,
                a.updated_at,
                a.rendered_payload,
                sc.market_id,
                sc.event_id,
                sc.event_family_id,
                sc.trigger_time,
                es.evidence_state
            FROM alerts a
            JOIN signal_candidates sc ON sc.candidate_id = a.candidate_id
            LEFT JOIN evidence_snapshots es ON es.evidence_snapshot_id = a.evidence_snapshot_id
            WHERE sc.market_id IN ({", ".join("?" for _ in market_ids)})
            ORDER BY sc.trigger_time ASC, a.created_at ASC
            """,
            tuple(market_ids),
        ).fetchall()
    finally:
        conn.close()
    payload: list[dict[str, Any]] = []
    for row in rows:
        rendered_payload = json.loads(row["rendered_payload"] or "{}")
        payload.append(
            {
                "alert_id": row["alert_id"],
                "candidate_id": row["candidate_id"],
                "market_id": row["market_id"],
                "event_id": row["event_id"],
                "event_family_id": row["event_family_id"],
                "trigger_time": row["trigger_time"],
                "severity": row["severity"],
                "alert_status": row["alert_status"],
                "title": row["title"],
                "suppression_key": row["suppression_key"],
                "suppression_state": row["suppression_state"],
                "created_at": row["created_at"],
                "updated_at": row["updated_at"],
                "evidence_state": row["evidence_state"],
                "rendered_payload": rendered_payload,
            }
        )
    return payload


def _fixture_delivery_rows() -> list[dict[str, Any]]:
    market_ids = [market.market_id for market in FIXTURE_MARKETS]
    conn = get_conn()
    try:
        rows = conn.execute(
            f"""
            SELECT
                ada.alert_id,
                ada.delivery_channel,
                ada.attempt_number,
                ada.delivery_status,
                ada.attempted_at,
                ada.error_message
            FROM alert_delivery_attempts ada
            JOIN alerts a ON a.alert_id = ada.alert_id
            JOIN signal_candidates sc ON sc.candidate_id = a.candidate_id
            WHERE sc.market_id IN ({", ".join("?" for _ in market_ids)})
            ORDER BY ada.attempted_at ASC, ada.attempt_number ASC
            """,
            tuple(market_ids),
        ).fetchall()
    finally:
        conn.close()
    return [
        {
            "alert_id": row["alert_id"],
            "delivery_channel": row["delivery_channel"],
            "attempt_number": int(row["attempt_number"] or 0),
            "delivery_status": row["delivery_status"],
            "attempted_at": row["attempted_at"],
            "error_message": row["error_message"],
        }
        for row in rows
    ]


def _fixture_feedback_rows() -> list[dict[str, Any]]:
    market_ids = [market.market_id for market in FIXTURE_MARKETS]
    conn = get_conn()
    try:
        rows = conn.execute(
            f"""
            SELECT
                af.feedback_id,
                af.alert_id,
                af.action_type,
                af.actor,
                af.notes,
                af.follow_up_at,
                af.created_at
            FROM analyst_feedback af
            JOIN alerts a ON a.alert_id = af.alert_id
            JOIN signal_candidates sc ON sc.candidate_id = a.candidate_id
            WHERE sc.market_id IN ({", ".join("?" for _ in market_ids)})
            ORDER BY af.created_at ASC
            """,
            tuple(market_ids),
        ).fetchall()
    finally:
        conn.close()
    return [
        {
            "feedback_id": row["feedback_id"],
            "alert_id": row["alert_id"],
            "action_type": row["action_type"],
            "actor": row["actor"],
            "notes": row["notes"],
            "follow_up_at": row["follow_up_at"],
            "created_at": row["created_at"],
        }
        for row in rows
    ]


def _fixture_table_counts() -> dict[str, int]:
    market_ids = [market.market_id for market in FIXTURE_MARKETS]
    conn = get_conn()
    try:
        signal_candidates = conn.execute(
            f"SELECT COUNT(*) FROM signal_candidates WHERE market_id IN ({', '.join('?' for _ in market_ids)})",
            tuple(market_ids),
        ).fetchone()[0]
        evidence_queries = conn.execute(
            f"""
            SELECT COUNT(*)
            FROM evidence_queries
            WHERE candidate_id IN (
                SELECT candidate_id FROM signal_candidates WHERE market_id IN ({", ".join("?" for _ in market_ids)})
            )
            """,
            tuple(market_ids),
        ).fetchone()[0]
        evidence_snapshots = conn.execute(
            f"""
            SELECT COUNT(*)
            FROM evidence_snapshots
            WHERE candidate_id IN (
                SELECT candidate_id FROM signal_candidates WHERE market_id IN ({", ".join("?" for _ in market_ids)})
            )
            """,
            tuple(market_ids),
        ).fetchone()[0]
        alerts = conn.execute(
            f"""
            SELECT COUNT(*)
            FROM alerts
            WHERE candidate_id IN (
                SELECT candidate_id FROM signal_candidates WHERE market_id IN ({", ".join("?" for _ in market_ids)})
            )
            """,
            tuple(market_ids),
        ).fetchone()[0]
        delivery_attempts = conn.execute(
            f"""
            SELECT COUNT(*)
            FROM alert_delivery_attempts
            WHERE alert_id IN (
                SELECT a.alert_id
                FROM alerts a
                JOIN signal_candidates sc ON sc.candidate_id = a.candidate_id
                WHERE sc.market_id IN ({", ".join("?" for _ in market_ids)})
            )
            """,
            tuple(market_ids),
        ).fetchone()[0]
        analyst_feedback = conn.execute(
            f"""
            SELECT COUNT(*)
            FROM analyst_feedback
            WHERE alert_id IN (
                SELECT a.alert_id
                FROM alerts a
                JOIN signal_candidates sc ON sc.candidate_id = a.candidate_id
                WHERE sc.market_id IN ({", ".join("?" for _ in market_ids)})
            )
            """,
            tuple(market_ids),
        ).fetchone()[0]
    finally:
        conn.close()
    return {
        "signal_candidates": int(signal_candidates or 0),
        "evidence_queries": int(evidence_queries or 0),
        "evidence_snapshots": int(evidence_snapshots or 0),
        "alerts": int(alerts or 0),
        "alert_delivery_attempts": int(delivery_attempts or 0),
        "analyst_feedback": int(analyst_feedback or 0),
    }


def _suppression_review(alert_rows: list[dict[str, Any]]) -> dict[str, Any]:
    alerts_by_id = {row["alert_id"]: row for row in alert_rows}
    suppressed_examples: list[dict[str, Any]] = []
    for row in alert_rows:
        suppression_state = str(row.get("suppression_state") or "")
        if not suppression_state.startswith("suppressed_by:"):
            continue
        parent_alert_id = suppression_state.split(":", 1)[1]
        parent = alerts_by_id.get(parent_alert_id)
        suppressed_examples.append(
            {
                "alert_id": row["alert_id"],
                "suppressed_by": parent_alert_id,
                "candidate_id": row["candidate_id"],
                "event_family_id": row["event_family_id"],
                "severity": row["severity"],
                "evidence_state": row["evidence_state"],
                "parent_severity": parent.get("severity") if parent else None,
                "parent_evidence_state": parent.get("evidence_state") if parent else None,
                "same_evidence_state_as_parent": bool(parent)
                and row.get("evidence_state") == parent.get("evidence_state"),
                "same_event_family_as_parent": bool(parent)
                and row.get("event_family_id") == parent.get("event_family_id"),
                "title": row["title"],
            }
        )
    return {
        "suppressed_alert_count": len(suppressed_examples),
        "suppressed_examples": suppressed_examples,
    }


def _evidence_mode_summary(candidate_ids: list[str]) -> dict[str, Any]:
    if not candidate_ids:
        return {
            "provider_counts": [],
            "provider_mode_counts": [],
            "cache_hit_rows": 0,
            "live_provider_rows": 0,
        }
    conn = get_conn()
    try:
        rows = conn.execute(
            f"""
            SELECT provider_name, raw_response_metadata
            FROM evidence_queries
            WHERE candidate_id IN ({", ".join("?" for _ in candidate_ids)})
            ORDER BY created_at ASC
            """,
            tuple(candidate_ids),
        ).fetchall()
    finally:
        conn.close()

    provider_counter: Counter[str] = Counter()
    mode_counter: Counter[str] = Counter()
    cache_hit_rows = 0
    live_provider_rows = 0
    for row in rows:
        provider_name = str(row["provider_name"])
        metadata = json.loads(row["raw_response_metadata"] or "{}")
        provider_counter[provider_name] += 1
        mode = str(metadata.get("provider_mode") or "unknown")
        mode_counter[mode] += 1
        if bool(metadata.get("cache_hit")):
            cache_hit_rows += 1
        if mode == "live":
            live_provider_rows += 1
    return {
        "provider_counts": [
            {"provider_name": key, "count": value}
            for key, value in sorted(provider_counter.items())
        ],
        "provider_mode_counts": [
            {"provider_mode": key, "count": value}
            for key, value in sorted(mode_counter.items())
        ],
        "cache_hit_rows": cache_hit_rows,
        "live_provider_rows": live_provider_rows,
    }


def render_phase10_task2_markdown(payload: dict[str, Any]) -> str:
    counts = payload["fixture_table_counts"]
    suppression = payload["suppression_review"]
    analyst = payload["analyst_review"]
    lines = [
        "# Phase 10 Task 2 - Analyst Loop Expansion and Suppression Review",
        "",
        f"- Contract version: `{payload['task_contract_version']}`",
        f"- Window: `{payload['window']['start']}` to `{payload['window']['end']}`",
        f"- Canonical alignment: `{payload['window']['alignment']}`",
        "",
        "## Alert Episodes",
        f"- Persisted candidates: `{counts['signal_candidates']}`",
        f"- Persisted alerts: `{counts['alerts']}`",
        f"- Delivery attempts: `{counts['alert_delivery_attempts']}`",
        f"- Analyst feedback rows: `{counts['analyst_feedback']}`",
        "",
        "## Suppression Review",
        f"- Suppressed alerts: `{suppression['suppressed_alert_count']}`",
        f"- Created alerts: `{payload['alert_review']['created_alert_count']}`",
        f"- Reviewed alert outcomes: `{', '.join(analyst['action_types'])}`",
        "",
        "## Evidence Modes",
        f"- Live provider rows: `{payload['evidence_mode_summary']['live_provider_rows']}`",
        f"- Cache-hit rows: `{payload['evidence_mode_summary']['cache_hit_rows']}`",
    ]
    for row in suppression["suppressed_examples"]:
        lines.append(
            f"- Suppressed alert `{row['alert_id']}` followed `{row['suppressed_by']}` "
            f"for event family `{row['event_family_id']}` with evidence state `{row['evidence_state']}`."
        )
    return "\n".join(lines) + "\n"


async def run_phase10_task2_analyst_loop_expansion(
    output_dir: str = PHASE10_TASK2_OUTPUT_DIR,
) -> dict[str, Any]:
    apply_schema()
    cleanup_summary = cleanup_phase9_task2_state()
    extra_cleanup_summary = _delete_task2_source_state()
    upsert_fixture_metadata()
    _update_real_provider_fixture_metadata()
    seed_summary = seed_phase10_task2_detector_inputs()

    store = MemoryStateStore()
    phase3_repository = Phase3Repository()
    phase3_repository.register_detector_version(
        backend_name=store.backend_name,
        notes="Phase 10 Task 2 analyst-loop expansion with repeated replay-linked alert episodes.",
    )
    detector_summary = await run_phase3_detector_window(
        start=PHASE9_TASK2_START,
        end=PHASE9_TASK2_END,
        store=store,
        repository=phase3_repository,
        source_systems=[PHASE10_TASK2_SOURCE_PRICES, PHASE10_TASK2_SOURCE_TRADES],
    )
    candidate_report = build_phase3_candidate_report(
        start=PHASE9_TASK2_START,
        end=PHASE9_TASK2_END,
    ).to_dict()

    phase4_repository = Phase4Repository()
    phase4_repository.register_workflow_version(
        notes="Phase 10 Task 2 analyst-loop expansion and suppression review over repeated real-provider alert episodes."
    )

    candidates = _fixture_candidates()
    if len(candidates) < 3:
        raise RuntimeError(
            f"Phase 10 Task 2 expected at least 3 candidates for the canonical fixture, found {len(candidates)}."
        )

    evidence_worker = Phase4EvidenceWorker(repository=phase4_repository)
    evidence_worker.summary.candidates_seen += len(candidates)
    evidence_results = []
    for candidate in candidates:
        evidence_results.append(await evidence_worker.process_candidate(candidate))

    alert_worker = Phase4AlertWorker(repository=phase4_repository)
    alert_worker.summary.candidates_seen += len(candidates)
    alert_results = []
    for candidate in candidates:
        alert_results.append(alert_worker.process_candidate(candidate))

    created_alerts = [row for row in alert_results if row.get("status") != "suppressed"]
    suppressed_alerts = [row for row in alert_results if row.get("status") == "suppressed"]
    if len(created_alerts) < 2:
        raise RuntimeError(
            f"Phase 10 Task 2 expected at least 2 created alerts, found {len(created_alerts)}."
        )
    if not suppressed_alerts:
        raise RuntimeError("Phase 10 Task 2 expected at least 1 suppressed alert example.")

    analyst_workflow = Phase4AnalystWorkflow(repository=phase4_repository)
    analyst_actions = [
        analyst_workflow.record_action(
            alert_id=str(created_alerts[0]["alert_id"]),
            action_type="mark_useful",
            actor="phase10_task2",
            notes="Real-provider-backed alert A looked materially useful after replay-linked review.",
            follow_up_at="2026-04-20T06:12:00+00:00",
        ),
        analyst_workflow.record_action(
            alert_id=str(created_alerts[1]["alert_id"]),
            action_type="mark_false_positive",
            actor="phase10_task2",
            notes="Second alert episode was reviewable but not strong enough after analyst inspection.",
            follow_up_at="2026-04-20T06:18:00+00:00",
        ),
    ]

    alert_rows = _fixture_alert_rows()
    feedback_rows = _fixture_feedback_rows()
    delivery_rows = _fixture_delivery_rows()
    suppression_review = _suppression_review(alert_rows)
    candidate_ids = [str(row["candidate_id"]) for row in candidates]
    evidence_mode_summary = _evidence_mode_summary(candidate_ids)
    if evidence_mode_summary["live_provider_rows"] == 0:
        raise RuntimeError("Phase 10 Task 2 expected live real-provider evidence rows but found none.")
    if evidence_mode_summary["cache_hit_rows"] == 0:
        raise RuntimeError("Phase 10 Task 2 expected at least one cache-hit evidence row but found none.")

    gate4_report = build_phase4_gate4_report().to_dict()
    output_root = REPO_ROOT / output_dir
    output_root.mkdir(parents=True, exist_ok=True)
    review_packet_path = output_root / "phase10_task2_review_packet.json"
    review_summary_path = output_root / "phase10_task2_review_summary.md"

    payload = {
        "task_contract_version": PHASE10_TASK2_CONTRACT_VERSION,
        "task_name": "Phase 10 Task 2 - Analyst Loop Expansion and Suppression Review",
        "generated_at": _iso_now(),
        "window": {
            "start": PHASE9_TASK2_START,
            "end": PHASE9_TASK2_END,
            "alignment": "Uses the canonical replay-linked hour with repeated real-provider alert episodes for one event family.",
            "detector_input_source_systems": [PHASE10_TASK2_SOURCE_PRICES, PHASE10_TASK2_SOURCE_TRADES],
        },
        "cleanup_summary": cleanup_summary,
        "extra_cleanup_summary": extra_cleanup_summary,
        "seed_summary": seed_summary,
        "phase3": {
            "detector_version": PHASE3_DETECTOR_VERSION,
            "feature_schema_version": PHASE3_FEATURE_SCHEMA_VERSION,
            "detector_summary": detector_summary.to_dict(),
            "candidate_report": candidate_report,
            "candidates": candidates,
        },
        "phase4": {
            "evidence_results": evidence_results,
            "evidence_summary": evidence_worker.summary.to_dict(),
            "alert_results": alert_results,
            "alert_summary": alert_worker.summary.to_dict(),
            "analyst_actions": analyst_actions,
            "analyst_summary": analyst_workflow.summary.to_dict(),
            "gate4_report": gate4_report,
        },
        "fixture_table_counts": _fixture_table_counts(),
        "alert_review": {
            "created_alert_count": len(created_alerts),
            "suppressed_alert_count": len(suppressed_alerts),
            "delivery_attempt_count": len(delivery_rows),
            "alerts": alert_rows,
        },
        "analyst_review": {
            "feedback_rows": feedback_rows,
            "feedback_row_count": len(feedback_rows),
            "action_types": [row["action_type"] for row in feedback_rows],
        },
        "suppression_review": suppression_review,
        "delivery_review": {
            "delivery_attempt_rows": delivery_rows,
            "delivery_status_counts": [
                {"delivery_status": key, "count": value}
                for key, value in sorted(Counter(row["delivery_status"] for row in delivery_rows).items())
            ],
        },
        "evidence_mode_summary": evidence_mode_summary,
        "review_packet_expectation": {
            "alerts": "More than one real-provider-backed alert episode reached the canonical alert path.",
            "suppression": "At least one later candidate in the same event family was suppressed by duplicate-alert policy.",
            "analyst_feedback": "More than one persisted analyst action exists, including usefulness and false-positive review outcomes.",
            "delivery": "Delivery attempts exist for multiple created alerts even if outbound channels remain locally disabled.",
        },
    }

    payload["artifacts"] = {
        "review_packet_path": _repo_relative(review_packet_path),
        "review_summary_path": _repo_relative(review_summary_path),
    }
    review_packet_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    review_summary_path.write_text(render_phase10_task2_markdown(payload), encoding="utf-8")
    return payload
