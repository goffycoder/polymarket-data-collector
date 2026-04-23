from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from uuid import uuid4

from config.settings import PHASE3_DETECTOR_VERSION, PHASE3_FEATURE_SCHEMA_VERSION, REPO_ROOT
from database.db_manager import apply_schema, get_conn
from phase3.detector import Phase3Repository
from phase4.alerts import render_alert_payload
from phase4.repository import Phase4Repository
from utils.event_log import DETECTOR_INPUT_ROOT, RAW_ARCHIVE_ROOT, archive_raw_event, publish_detector_input


PHASE10_HELDOUT_PREFIX = "phase10_holdout"
PHASE10_HELDOUT_SOURCE_PRICES = "phase10_holdout_prices"
PHASE10_HELDOUT_SOURCE_TRADES = "phase10_holdout_trades"
PHASE10_HELDOUT_SOURCES = [PHASE10_HELDOUT_SOURCE_PRICES, PHASE10_HELDOUT_SOURCE_TRADES]
PHASE10_HELDOUT_OVERALL_START = "2026-01-15T05:00:00+00:00"
PHASE10_HELDOUT_OVERALL_END = "2026-03-15T06:00:00+00:00"
PHASE10_HELDOUT_CANDIDATES_PER_WINDOW = 32
PHASE10_HELDOUT_TOTAL_WINDOWS = 3
PHASE10_HELDOUT_TOTAL_ROWS = PHASE10_HELDOUT_CANDIDATES_PER_WINDOW * PHASE10_HELDOUT_TOTAL_WINDOWS


@dataclass(frozen=True, slots=True)
class HeldoutWindowSpec:
    key: str
    role: str
    start: str
    end: str


@dataclass(frozen=True, slots=True)
class HeldoutEpisodeSpec:
    window_key: str
    window_role: str
    category: str
    event_id: str
    market_id: str
    question: str
    title: str
    trigger_time: str
    alert_time: str
    evidence_time: str
    resolution_time: str
    market_end_time: str
    direction: str
    resolution_outcome: str
    label_success: bool
    evidence_state: str
    candidate_severity_score: float
    feature_snapshot: dict[str, Any]
    trades_payload: list[dict[str, Any]]
    price_payload: list[dict[str, Any]]


WINDOW_SPECS = [
    HeldoutWindowSpec(
        key="train_window",
        role="train",
        start="2026-01-15T05:00:00+00:00",
        end="2026-01-15T06:00:00+00:00",
    ),
    HeldoutWindowSpec(
        key="validation_window",
        role="validation",
        start="2026-02-15T05:00:00+00:00",
        end="2026-02-15T06:00:00+00:00",
    ),
    HeldoutWindowSpec(
        key="test_window",
        role="test",
        start="2026-03-15T05:00:00+00:00",
        end="2026-03-15T06:00:00+00:00",
    ),
]

CATEGORIES = ("crypto", "macro", "politics", "sports")
EVIDENCE_STATES = ("weakly_public",)
ARCHETYPE_BY_INDEX = ("A", "B", "C", "D")


def _parse_iso(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _iso(value: datetime) -> str:
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).isoformat()


def _repo_relative(path: Path) -> str:
    try:
        return str(path.resolve().relative_to(REPO_ROOT.resolve())).replace("\\", "/")
    except ValueError:
        return str(path)


def _window_hour_path(root: Path, source_system: str, dt: datetime) -> Path:
    return (
        root
        / f"year={dt:%Y}"
        / f"month={dt:%m}"
        / f"day={dt:%d}"
        / f"hour={dt:%H}"
        / f"source_system={source_system}"
        / "events.ndjson"
    )


def _remove_file_if_exists(path: Path, *, root: Path) -> None:
    if path.exists():
        path.unlink()
    parent = path.parent
    while parent != root and parent.exists():
        try:
            parent.rmdir()
        except OSError:
            break
        parent = parent.parent


def _confidence_modifier(evidence_state: str) -> float:
    mapping = {
        "already_public": -0.25,
        "weakly_public": -0.10,
        "not_publicly_explained": 0.15,
    }
    return mapping.get(evidence_state, 0.0)


def _price_path(*, direction: str, label_success: bool) -> tuple[float, float, float]:
    if direction == "YES":
        return (0.55, 0.72, 0.92) if label_success else (0.55, 0.36, 0.12)
    return (0.45, 0.28, 0.08) if label_success else (0.45, 0.64, 0.88)


def _feature_snapshot(
    *,
    trigger_time: datetime,
    archetype: str,
    direction: str,
    market_id: str,
    candidate_severity_score: float,
) -> dict[str, Any]:
    fresh_wallet_count = 5 if archetype in {"A", "B"} else 3
    concentration_ratio = 0.54 if archetype in {"A", "C"} else 0.72
    directional_imbalance = 0.74
    volume_acceleration = 1.62
    fresh_wallet_notional_share = 0.62
    probability_velocity = 0.032 if direction == "YES" else -0.032
    probability_acceleration = 0.012 if direction == "YES" else -0.012
    buy_notional = 248.0 if direction == "YES" else 92.0
    sell_notional = 92.0 if direction == "YES" else 248.0
    current_notional = 340.0
    previous_notional = 215.0
    return {
        "market_id": market_id,
        "fresh_wallet_count": fresh_wallet_count,
        "fresh_wallet_notional_share": round(fresh_wallet_notional_share, 6),
        "directional_imbalance": round(directional_imbalance, 6),
        "concentration_ratio": round(concentration_ratio, 6),
        "probability_velocity": round(probability_velocity, 6),
        "probability_acceleration": round(probability_acceleration, 6),
        "volume_acceleration": round(volume_acceleration, 6),
        "current_window_notional": round(current_notional, 6),
        "previous_window_notional": round(previous_notional, 6),
        "buy_notional": round(buy_notional, 6),
        "sell_notional": round(sell_notional, 6),
        "wallet_count": fresh_wallet_count,
        "trade_count": 3,
        "price_point_count": 3,
        "window_seconds": 300,
        "episode_start_event_time": _iso(trigger_time - timedelta(minutes=4)),
        "episode_end_event_time": _iso(trigger_time),
        "severity_score": round(candidate_severity_score, 6),
    }


def build_phase10_heldout_specs() -> list[HeldoutEpisodeSpec]:
    specs: list[HeldoutEpisodeSpec] = []
    for window_index, window in enumerate(WINDOW_SPECS):
        start_dt = _parse_iso(window.start)
        for local_index in range(PHASE10_HELDOUT_CANDIDATES_PER_WINDOW):
            global_index = (window_index * PHASE10_HELDOUT_CANDIDATES_PER_WINDOW) + local_index + 1
            category = CATEGORIES[local_index % len(CATEGORIES)]
            archetype = ARCHETYPE_BY_INDEX[local_index % len(ARCHETYPE_BY_INDEX)]
            direction = "YES" if (local_index % 2 == 0) else "NO"
            label_success = archetype in {"A", "D"}
            trigger_time = start_dt + timedelta(minutes=2, seconds=(local_index * 90))
            alert_time = trigger_time + timedelta(seconds=45)
            evidence_time = trigger_time + timedelta(seconds=30)
            resolution_time = alert_time + timedelta(minutes=8 + (local_index % 3))
            market_end_time = alert_time + timedelta(minutes=95 + (local_index % 3) * 5)
            resolution_outcome = direction if label_success else ("NO" if direction == "YES" else "YES")
            candidate_severity_score = 0.63 + ((local_index % 5) * 0.01)
            price_entry, price_mid, price_exit = _price_path(direction=direction, label_success=label_success)
            event_id = f"{PHASE10_HELDOUT_PREFIX}_event_{window.role}_{global_index:03d}"
            market_id = f"{PHASE10_HELDOUT_PREFIX}_market_{window.role}_{global_index:03d}"
            title = f"Phase 10 held-out {category} event {window.role} {global_index:03d}"
            question = f"Will the Phase 10 held-out {category} pattern {global_index:03d} resolve as modeled?"
            feature_snapshot = _feature_snapshot(
                trigger_time=trigger_time,
                archetype=archetype,
                direction=direction,
                market_id=market_id,
                candidate_severity_score=candidate_severity_score,
            )
            specs.append(
                HeldoutEpisodeSpec(
                    window_key=window.key,
                    window_role=window.role,
                    category=category,
                    event_id=event_id,
                    market_id=market_id,
                    question=question,
                    title=title,
                    trigger_time=_iso(trigger_time),
                    alert_time=_iso(alert_time),
                    evidence_time=_iso(evidence_time),
                    resolution_time=_iso(resolution_time),
                    market_end_time=_iso(market_end_time),
                    direction=direction,
                    resolution_outcome=resolution_outcome,
                    label_success=label_success,
                    evidence_state=EVIDENCE_STATES[local_index % len(EVIDENCE_STATES)],
                    candidate_severity_score=round(candidate_severity_score, 6),
                    feature_snapshot=feature_snapshot,
                    trades_payload=[
                        {
                            "trade_id": f"{market_id}_trade_1",
                            "market_id": market_id,
                            "trade_time": _iso(trigger_time - timedelta(minutes=4)),
                            "proxy_wallet": f"0x{global_index:040x}"[-42:],
                            "side": "BUY" if direction == "YES" else "SELL",
                            "outcome_side": direction,
                            "usdc_notional": 120.0,
                        },
                        {
                            "trade_id": f"{market_id}_trade_2",
                            "market_id": market_id,
                            "trade_time": _iso(trigger_time - timedelta(minutes=2)),
                            "proxy_wallet": f"0x{(global_index + 1000):040x}"[-42:],
                            "side": "BUY" if direction == "YES" else "SELL",
                            "outcome_side": direction,
                            "usdc_notional": 140.0,
                        },
                        {
                            "trade_id": f"{market_id}_trade_3",
                            "market_id": market_id,
                            "trade_time": _iso(trigger_time),
                            "proxy_wallet": f"0x{(global_index + 2000):040x}"[-42:],
                            "side": "BUY" if direction == "YES" else "SELL",
                            "outcome_side": direction,
                            "usdc_notional": 80.0,
                        },
                    ],
                    price_payload=[
                        {"market_id": market_id, "captured_at": _iso(alert_time + timedelta(minutes=1)), "yes_price": round(price_entry, 6)},
                        {"market_id": market_id, "captured_at": _iso(alert_time + timedelta(minutes=4)), "yes_price": round(price_mid, 6)},
                        {"market_id": market_id, "captured_at": _iso(resolution_time - timedelta(minutes=1)), "yes_price": round(price_exit, 6)},
                    ],
                )
            )
    return specs


def cleanup_phase10_heldout_family_state() -> dict[str, Any]:
    specs = build_phase10_heldout_specs()
    market_ids = [spec.market_id for spec in specs]
    event_ids = [spec.event_id for spec in specs]
    conn = get_conn()
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
        candidate_ids = [str(row["candidate_id"]) for row in candidate_rows]
        alert_ids: list[str] = []
        if candidate_ids:
            alert_rows = conn.execute(
                f"""
                SELECT alert_id
                FROM alerts
                WHERE candidate_id IN ({", ".join("?" for _ in candidate_ids)})
                """,
                tuple(candidate_ids),
            ).fetchall()
            alert_ids = [str(row["alert_id"]) for row in alert_rows]
            conn.execute(
                f"DELETE FROM analyst_feedback WHERE alert_id IN ({', '.join('?' for _ in alert_ids)})",
                tuple(alert_ids),
            ) if alert_ids else None
            conn.execute(
                f"DELETE FROM alert_delivery_attempts WHERE alert_id IN ({', '.join('?' for _ in alert_ids)})",
                tuple(alert_ids),
            ) if alert_ids else None
            conn.execute(
                f"DELETE FROM alerts WHERE candidate_id IN ({', '.join('?' for _ in candidate_ids)})",
                tuple(candidate_ids),
            )
            conn.execute(
                f"DELETE FROM evidence_queries WHERE candidate_id IN ({', '.join('?' for _ in candidate_ids)})",
                tuple(candidate_ids),
            )
            conn.execute(
                f"DELETE FROM evidence_snapshots WHERE candidate_id IN ({', '.join('?' for _ in candidate_ids)})",
                tuple(candidate_ids),
            )
            conn.execute(
                f"DELETE FROM signal_features WHERE candidate_id IN ({', '.join('?' for _ in candidate_ids)})",
                tuple(candidate_ids),
            )
            conn.execute(
                f"DELETE FROM signal_candidates WHERE candidate_id IN ({', '.join('?' for _ in candidate_ids)})",
                tuple(candidate_ids),
            )
        conn.execute(
            f"DELETE FROM signal_episodes WHERE market_id IN ({', '.join('?' for _ in market_ids)})",
            tuple(market_ids),
        )
        conn.execute(
            f"DELETE FROM snapshots WHERE market_id IN ({', '.join('?' for _ in market_ids)})",
            tuple(market_ids),
        )
        conn.execute(
            f"DELETE FROM market_resolutions WHERE market_id IN ({', '.join('?' for _ in market_ids)})",
            tuple(market_ids),
        )
        conn.execute(
            f"DELETE FROM markets WHERE market_id IN ({', '.join('?' for _ in market_ids)})",
            tuple(market_ids),
        )
        conn.execute(
            f"DELETE FROM events WHERE event_id IN ({', '.join('?' for _ in event_ids)})",
            tuple(event_ids),
        )
        conn.execute(
            f"DELETE FROM detector_checkpoints WHERE source_system IN ({', '.join('?' for _ in PHASE10_HELDOUT_SOURCES)})",
            tuple(PHASE10_HELDOUT_SOURCES),
        )
        conn.execute(
            f"DELETE FROM detector_input_manifests WHERE source_system IN ({', '.join('?' for _ in PHASE10_HELDOUT_SOURCES)})",
            tuple(PHASE10_HELDOUT_SOURCES),
        )
        conn.execute(
            f"DELETE FROM raw_archive_manifests WHERE source_system IN ({', '.join('?' for _ in PHASE10_HELDOUT_SOURCES)})",
            tuple(PHASE10_HELDOUT_SOURCES),
        )
        conn.execute(
            """
            DELETE FROM validation_runs
            WHERE output_path LIKE 'reports/phase5/validation/phase10_task3%'
               OR notes LIKE '%phase10_task3%'
            """
        )
        conn.execute(
            """
            DELETE FROM backtest_artifacts
            WHERE output_path LIKE 'reports/phase5/backtests/phase10_task3%'
               OR notes LIKE '%phase10_task3%'
            """
        )
        conn.execute(
            """
            DELETE FROM replay_runs
            WHERE source_system IN (?, ?)
              AND notes LIKE '%phase10_task3%'
            """,
            (PHASE10_HELDOUT_SOURCE_PRICES, PHASE10_HELDOUT_SOURCE_TRADES),
        )
        conn.execute(
            """
            DELETE FROM shadow_model_scores
            WHERE model_version = 'phase10_task4_lightgbm_v1'
            """
        )
        conn.execute(
            """
            DELETE FROM calibration_profiles
            WHERE model_version = 'phase10_task4_lightgbm_v1'
            """
        )
        conn.execute(
            """
            DELETE FROM model_evaluation_runs
            WHERE model_version = 'phase10_task4_lightgbm_v1'
            """
        )
        conn.execute(
            """
            DELETE FROM model_registry
            WHERE model_version = 'phase10_task4_lightgbm_v1'
            """
        )
        conn.commit()
    finally:
        conn.close()

    for window in WINDOW_SPECS:
        dt = _parse_iso(window.start)
        for source_system in PHASE10_HELDOUT_SOURCES:
            _remove_file_if_exists(
                _window_hour_path(RAW_ARCHIVE_ROOT, source_system, dt),
                root=RAW_ARCHIVE_ROOT,
            )
            _remove_file_if_exists(
                _window_hour_path(DETECTOR_INPUT_ROOT, source_system, dt),
                root=DETECTOR_INPUT_ROOT,
            )

    return {
        "deleted_market_count": len(market_ids),
        "deleted_event_count": len(event_ids),
        "cleared_sources": list(PHASE10_HELDOUT_SOURCES),
    }


def _upsert_metadata(specs: list[HeldoutEpisodeSpec]) -> dict[str, int]:
    now = datetime.now(timezone.utc).isoformat()
    conn = get_conn()
    try:
        for spec in specs:
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
                ) VALUES (?, ?, ?, ?, ?, 'resolved', ?, ?)
                ON CONFLICT(event_id) DO UPDATE SET
                    title = excluded.title,
                    description = excluded.description,
                    slug = excluded.slug,
                    category = excluded.category,
                    status = excluded.status,
                    last_updated_at = excluded.last_updated_at
                """,
                (
                    spec.event_id,
                    spec.title,
                    f"Synthetic Phase 10 held-out {spec.window_role} episode for {spec.category} validation.",
                    spec.title.lower().replace(" ", "-"),
                    spec.category,
                    spec.trigger_time,
                    now,
                ),
            )
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
                    end_date,
                    first_seen_at,
                    last_updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 1, 'resolved', 1, 1, ?, ?, ?)
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
                    end_date = excluded.end_date,
                    last_updated_at = excluded.last_updated_at
                """,
                (
                    spec.market_id,
                    spec.event_id,
                    spec.question,
                    f"Synthetic Phase 10 held-out {spec.window_role} market for {spec.category} validation.",
                    spec.question.lower().replace(" ", "-").replace("?", ""),
                    f"{spec.market_id}_condition",
                    f"{spec.market_id}_yes",
                    f"{spec.market_id}_no",
                    spec.market_end_time,
                    spec.trigger_time,
                    now,
                ),
            )
        conn.commit()
    finally:
        conn.close()
    return {"events_upserted": len(specs), "markets_upserted": len(specs)}


def _seed_raw_and_detector_inputs(specs: list[HeldoutEpisodeSpec]) -> dict[str, Any]:
    written_raw: list[dict[str, Any]] = []
    written_detector: list[dict[str, Any]] = []
    by_window: dict[str, list[HeldoutEpisodeSpec]] = {}
    for spec in specs:
        by_window.setdefault(spec.window_key, []).append(spec)

    for window in WINDOW_SPECS:
        episodes = by_window.get(window.key, [])
        if not episodes:
            continue
        captured_dt = _parse_iso(window.start)
        prices_payload = {"market_snapshots": [point for spec in episodes for point in spec.price_payload[:1]]}
        trades_payload = {"trades": [trade for spec in episodes for trade in spec.trades_payload]}
        for source_system, event_type, entity_type, payload in [
            (PHASE10_HELDOUT_SOURCE_PRICES, "prices_batch", "prices_batch", prices_payload),
            (PHASE10_HELDOUT_SOURCE_TRADES, "recent_trades_page", "recent_trades_page", trades_payload),
        ]:
            raw_result = archive_raw_event(
                source_system=source_system,
                event_type=event_type,
                payload=payload,
                captured_at=captured_dt,
                metadata={"phase": "phase10_task3", "window_key": window.key, "window_role": window.role},
            )
            detector_result = publish_detector_input(
                source_system=source_system,
                entity_type=entity_type,
                payload=payload,
                captured_at=captured_dt,
                ordering_key=f"{window.key}:{entity_type}",
                raw_partition_path=raw_result.partition_path,
            )
            written_raw.append(
                {
                    "window_key": window.key,
                    "source_system": source_system,
                    "partition_path": raw_result.partition_path,
                    "envelope_id": raw_result.envelope_id,
                }
            )
            written_detector.append(
                {
                    "window_key": window.key,
                    "source_system": source_system,
                    "partition_path": detector_result.partition_path,
                    "envelope_id": detector_result.envelope_id,
                }
            )
    return {
        "raw_archive_writes": written_raw,
        "detector_input_writes": written_detector,
    }


def _candidate_payload(spec: HeldoutEpisodeSpec) -> dict[str, Any]:
    return {
        "candidate_id": uuid4().hex,
        "episode_id": uuid4().hex,
        "market_id": spec.market_id,
        "event_id": spec.event_id,
        "event_family_id": spec.event_id,
        "trigger_time": spec.trigger_time,
        "episode_start_event_time": spec.feature_snapshot["episode_start_event_time"],
        "episode_end_event_time": spec.feature_snapshot["episode_end_event_time"],
        "feature_schema_version": PHASE3_FEATURE_SCHEMA_VERSION,
        "detector_version": PHASE3_DETECTOR_VERSION,
        "rule_families": [
            "fresh_wallet_burst",
            "concentrated_directional_flow",
            "fast_repricing_with_wallet_support",
        ],
        "primary_rule_family": "fresh_wallet_burst",
        "cooldown_state": {"suppressed": False, "reason": f"phase10_{spec.window_role}_heldout_seed"},
        "severity_score": spec.candidate_severity_score,
        "feature_snapshot": spec.feature_snapshot,
        "episode_metadata": {
            "question": spec.question,
            "event_slug": spec.title.lower().replace(" ", "-"),
            "event_title": spec.title,
        },
    }


def _backdate_alert_rows(*, alert_id: str, evidence_snapshot_id: str, alert_time: str, evidence_time: str) -> None:
    conn = get_conn()
    try:
        conn.execute(
            """
            UPDATE evidence_snapshots
            SET snapshot_time = ?, created_at = ?
            WHERE evidence_snapshot_id = ?
            """,
            (evidence_time, evidence_time, evidence_snapshot_id),
        )
        conn.execute(
            """
            UPDATE alerts
            SET created_at = ?, updated_at = ?, first_delivery_at = ?, last_delivery_at = ?
            WHERE alert_id = ?
            """,
            (
                alert_time,
                alert_time,
                alert_time,
                alert_time,
                alert_id,
            ),
        )
        attempts = conn.execute(
            """
            SELECT delivery_attempt_id, attempt_number
            FROM alert_delivery_attempts
            WHERE alert_id = ?
            ORDER BY attempt_number ASC
            """,
            (alert_id,),
        ).fetchall()
        alert_dt = _parse_iso(alert_time)
        for attempt in attempts:
            attempt_dt = alert_dt + timedelta(seconds=int(attempt["attempt_number"]) * 5)
            conn.execute(
                """
                UPDATE alert_delivery_attempts
                SET attempted_at = ?, completed_at = ?, created_at = ?
                WHERE delivery_attempt_id = ?
                """,
                (_iso(attempt_dt), _iso(attempt_dt + timedelta(seconds=1)), _iso(attempt_dt), str(attempt["delivery_attempt_id"])),
            )
        conn.commit()
    finally:
        conn.close()


def _materialize_candidates_and_alerts(specs: list[HeldoutEpisodeSpec]) -> dict[str, Any]:
    phase3_repository = Phase3Repository()
    phase4_repository = Phase4Repository()
    candidate_rows: list[dict[str, Any]] = []
    evidence_rows: list[dict[str, Any]] = []
    alert_rows: list[dict[str, Any]] = []

    for spec in specs:
        candidate = _candidate_payload(spec)
        phase3_repository.persist_candidate(candidate)
        candidate_rows.append(
            {
                "candidate_id": candidate["candidate_id"],
                "market_id": spec.market_id,
                "event_id": spec.event_id,
                "window_key": spec.window_key,
                "window_role": spec.window_role,
                "label_success": spec.label_success,
                "direction": spec.direction,
                "resolution_outcome": spec.resolution_outcome,
            }
        )
        provider_summary = {
            "providers": [
                {
                    "provider_name": "phase10_heldout_reference",
                    "provider_mode": "replay_seed",
                    "query_type": "heldout_family",
                    "query_status": "ok",
                    "result_count": 1,
                    "cache_hit": False,
                    "budget": {"external_call_made": False, "estimated_cost_usd": 0.0},
                }
            ],
            "real_provider_count": 0,
            "cache_hit_count": 0,
            "total_estimated_cost_usd": 0.0,
        }
        evidence_snapshot_id = phase4_repository.record_evidence_snapshot(
            candidate_id=candidate["candidate_id"],
            snapshot_time=spec.evidence_time,
            evidence_state=spec.evidence_state,
            provider_summary=provider_summary,
            confidence_modifier=_confidence_modifier(spec.evidence_state),
            metadata_json={"window_key": spec.window_key, "window_role": spec.window_role, "category": spec.category},
            cache_key=f"{spec.market_id}:{spec.window_key}",
            freshness_seconds=0,
        )
        evidence_rows.append(
            {
                "candidate_id": candidate["candidate_id"],
                "evidence_snapshot_id": evidence_snapshot_id,
                "evidence_state": spec.evidence_state,
            }
        )
        candidate_for_alert = {
            "candidate_id": candidate["candidate_id"],
            "market_id": spec.market_id,
            "event_id": spec.event_id,
            "event_family_id": spec.event_id,
            "trigger_time": spec.trigger_time,
            "detector_version": candidate["detector_version"],
            "feature_schema_version": candidate["feature_schema_version"],
            "severity_score": candidate["severity_score"],
            "triggering_rules": candidate["rule_families"],
            "feature_snapshot": candidate["feature_snapshot"],
            "question": spec.question,
            "event_title": spec.title,
            "event_slug": spec.title.lower().replace(" ", "-"),
        }
        evidence_snapshot = {
            "evidence_snapshot_id": evidence_snapshot_id,
            "evidence_state": spec.evidence_state,
            "confidence_modifier": _confidence_modifier(spec.evidence_state),
            "provider_summary": provider_summary,
        }
        severity = "ACTIONABLE"
        rendered_payload = render_alert_payload(candidate_for_alert, evidence_snapshot, severity=severity)
        alert_id = phase4_repository.record_alert(
            candidate_id=candidate["candidate_id"],
            severity=severity,
            alert_status="created",
            title=str(rendered_payload["title"]),
            rendered_payload=rendered_payload,
            detector_version=candidate["detector_version"],
            feature_schema_version=candidate["feature_schema_version"],
            evidence_snapshot_id=evidence_snapshot_id,
            suppression_key=spec.event_id,
            suppression_state="new",
        )
        for attempt_number, delivery_channel in enumerate(("telegram", "discord"), start=1):
            phase4_repository.record_delivery_attempt(
                alert_id=alert_id,
                delivery_channel=delivery_channel,
                attempt_number=attempt_number,
                delivery_status="skipped",
                provider_message_id=None,
                request_payload={"alert_id": alert_id, "delivery_channel": delivery_channel},
                response_metadata={"status": "skipped", "reason": f"{delivery_channel}_disabled"},
                error_message=None,
            )
        _backdate_alert_rows(
            alert_id=alert_id,
            evidence_snapshot_id=evidence_snapshot_id,
            alert_time=spec.alert_time,
            evidence_time=spec.evidence_time,
        )
        alert_rows.append(
            {
                "candidate_id": candidate["candidate_id"],
                "alert_id": alert_id,
                "market_id": spec.market_id,
                "window_key": spec.window_key,
                "window_role": spec.window_role,
                "alert_time": spec.alert_time,
            }
        )
    return {
        "candidate_rows": candidate_rows,
        "evidence_rows": evidence_rows,
        "alert_rows": alert_rows,
    }


def _seed_market_state(specs: list[HeldoutEpisodeSpec]) -> dict[str, Any]:
    snapshot_count = 0
    resolution_count = 0
    conn = get_conn()
    try:
        for spec in specs:
            for payload in spec.price_payload:
                yes_price = float(payload["yes_price"])
                no_price = round(1.0 - yes_price, 6)
                best_bid = round(max(0.0, yes_price - 0.01), 6)
                best_ask = round(min(1.0, yes_price + 0.01), 6)
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
                    (
                        spec.market_id,
                        payload["captured_at"],
                        yes_price,
                        no_price,
                        best_bid,
                        best_ask,
                        round(best_ask - best_bid, 6),
                        "phase10_heldout_family",
                    ),
                )
                snapshot_count += 1
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
                (
                    spec.market_id,
                    f"{spec.market_id}_condition",
                    spec.resolution_outcome,
                    1.0 if spec.resolution_outcome == "YES" else 0.0,
                    spec.resolution_time,
                    "phase10_heldout_family",
                ),
            )
            resolution_count += 1
        conn.commit()
    finally:
        conn.close()
    return {"snapshots_written": snapshot_count, "market_resolutions_written": resolution_count}


def materialize_phase10_heldout_family() -> dict[str, Any]:
    apply_schema()
    cleanup_summary = cleanup_phase10_heldout_family_state()
    specs = build_phase10_heldout_specs()
    metadata_summary = _upsert_metadata(specs)
    archive_summary = _seed_raw_and_detector_inputs(specs)
    materialization_summary = _materialize_candidates_and_alerts(specs)
    market_state_summary = _seed_market_state(specs)
    return {
        "cleanup_summary": cleanup_summary,
        "metadata_summary": metadata_summary,
        "archive_summary": archive_summary,
        "materialization_summary": materialization_summary,
        "market_state_summary": market_state_summary,
        "window_family": [
            {"key": window.key, "role": window.role, "start": window.start, "end": window.end}
            for window in WINDOW_SPECS
        ],
        "overall_window": {"start": PHASE10_HELDOUT_OVERALL_START, "end": PHASE10_HELDOUT_OVERALL_END},
        "candidate_count": len(specs),
        "artifact_expectation": {
            "time_splits": ["train_window", "validation_window", "test_window"],
            "rows_per_window": PHASE10_HELDOUT_CANDIDATES_PER_WINDOW,
            "total_rows": len(specs),
        },
    }
