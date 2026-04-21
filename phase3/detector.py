from __future__ import annotations

import json
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable
from uuid import uuid4

from config.settings import (
    PHASE3_COOLDOWN_SECONDS,
    PHASE3_DETECTOR_VERSION,
    PHASE3_FEATURE_SCHEMA_VERSION,
    PHASE3_HISTORY_SECONDS,
    PHASE3_MIN_CONCENTRATION_RATIO,
    PHASE3_MIN_DIRECTIONAL_IMBALANCE,
    PHASE3_MIN_FRESH_WALLET_COUNT,
    PHASE3_MIN_FRESH_WALLET_NOTIONAL_SHARE,
    PHASE3_MIN_PROBABILITY_ACCELERATION,
    PHASE3_MIN_PROBABILITY_VELOCITY,
    PHASE3_MIN_VOLUME_ACCELERATION,
    PHASE3_MIN_WINDOW_NOTIONAL,
    PHASE3_WINDOW_SECONDS,
)
from database.db_manager import get_conn
from phase3.state_store import BaseStateStore
from utils.event_log import DETECTOR_INPUT_ROOT
from utils.logger import get_logger

log = get_logger("phase3_detector")

DEFAULT_PHASE3_SOURCE_SYSTEMS = [
    "clob_ws_market",
    "data_api_trades",
    "data_api_trades_backfill",
    "clob_prices",
    "clob_books",
]


def _parse_iso(value: str | None) -> datetime | None:
    if not value:
        return None
    if isinstance(value, datetime):
        parsed = value
    else:
        text = str(value).strip()
        if not text:
            return None
        try:
            if text.replace(".", "", 1).isdigit():
                raw_value = float(text)
                scale = 1000 if "." in text or abs(raw_value) >= 1_000_000_000_000 else 1
                parsed = datetime.fromtimestamp(raw_value / scale, tz=timezone.utc)
            else:
                parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
        except (TypeError, ValueError, OSError, OverflowError):
            return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _iso(dt: datetime) -> str:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).isoformat()


def _hour_floor(dt: datetime) -> datetime:
    return dt.replace(minute=0, second=0, microsecond=0)


def _iter_hours(start: datetime, end: datetime) -> list[datetime]:
    current = _hour_floor(start)
    hours: list[datetime] = []
    while current < end:
        hours.append(current)
        current += timedelta(hours=1)
    return hours


def _iter_partition_rows(path: Path) -> Iterable[dict[str, Any]]:
    if not path.exists():
        return []
    rows: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            rows.append(json.loads(line))
    return rows


def _partition_file(source_system: str, dt: datetime) -> Path:
    return (
        DETECTOR_INPUT_ROOT
        / f"year={dt:%Y}"
        / f"month={dt:%m}"
        / f"day={dt:%d}"
        / f"hour={dt:%H}"
        / f"source_system={source_system}"
        / "events.ndjson"
    )


@dataclass(slots=True)
class DetectorRunSummary:
    processed_envelopes: int = 0
    processed_trades: int = 0
    processed_snapshots: int = 0
    candidates_emitted: int = 0
    candidates_suppressed: int = 0
    ignored_envelopes: int = 0

    def to_dict(self) -> dict[str, int]:
        return {
            "processed_envelopes": self.processed_envelopes,
            "processed_trades": self.processed_trades,
            "processed_snapshots": self.processed_snapshots,
            "candidates_emitted": self.candidates_emitted,
            "candidates_suppressed": self.candidates_suppressed,
            "ignored_envelopes": self.ignored_envelopes,
        }


class Phase3Repository:
    def __init__(self):
        self._market_cache: dict[str, dict[str, Any]] = {}

    def register_detector_version(self, *, backend_name: str, notes: str) -> None:
        conn = get_conn()
        now = _iso(datetime.now(timezone.utc))
        try:
            conn.execute(
                """
                INSERT INTO detector_versions (
                    detector_version,
                    feature_schema_version,
                    state_backend,
                    notes,
                    created_at,
                    last_used_at
                ) VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(detector_version) DO UPDATE SET
                    feature_schema_version = excluded.feature_schema_version,
                    state_backend = excluded.state_backend,
                    notes = excluded.notes,
                    last_used_at = excluded.last_used_at
                """,
                (
                    PHASE3_DETECTOR_VERSION,
                    PHASE3_FEATURE_SCHEMA_VERSION,
                    backend_name,
                    notes,
                    now,
                    now,
                ),
            )
            conn.commit()
        finally:
            conn.close()

    def market_metadata(self, market_id: str) -> dict[str, Any]:
        cached = self._market_cache.get(market_id)
        if cached is not None:
            return cached

        conn = get_conn()
        try:
            row = conn.execute(
                """
                SELECT
                    m.market_id,
                    m.event_id,
                    m.question,
                    m.condition_id,
                    e.slug AS event_slug,
                    e.title AS event_title
                FROM markets m
                LEFT JOIN events e ON e.event_id = m.event_id
                WHERE m.market_id = ?
                """,
                (market_id,),
            ).fetchone()
        finally:
            conn.close()

        metadata = {
            "market_id": market_id,
            "event_id": row["event_id"] if row else None,
            "event_family_id": row["event_id"] if row else None,
            "condition_id": row["condition_id"] if row else None,
            "question": row["question"] if row else None,
            "event_slug": row["event_slug"] if row else None,
            "event_title": row["event_title"] if row else None,
        }
        self._market_cache[market_id] = metadata
        return metadata

    def persist_candidate(self, candidate: dict[str, Any]) -> None:
        feature_snapshot = candidate["feature_snapshot"]
        feature_items = [
            (
                candidate["candidate_id"],
                candidate["episode_id"],
                candidate["market_id"],
                name,
                value,
                candidate["feature_schema_version"],
                candidate["trigger_time"],
            )
            for name, value in sorted(feature_snapshot.items())
            if isinstance(value, (int, float))
        ]

        conn = get_conn()
        try:
            conn.execute(
                """
                INSERT INTO signal_episodes (
                    episode_id,
                    market_id,
                    event_id,
                    event_family_id,
                    rule_family,
                    episode_start_event_time,
                    episode_end_event_time,
                    feature_schema_version,
                    detector_version,
                    episode_status,
                    metadata_json,
                    created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    candidate["episode_id"],
                    candidate["market_id"],
                    candidate["event_id"],
                    candidate["event_family_id"],
                    candidate["primary_rule_family"],
                    candidate["episode_start_event_time"],
                    candidate["episode_end_event_time"],
                    candidate["feature_schema_version"],
                    candidate["detector_version"],
                    "candidate",
                    json.dumps(candidate["episode_metadata"], sort_keys=True),
                    candidate["trigger_time"],
                ),
            )
            conn.execute(
                """
                INSERT INTO signal_candidates (
                    candidate_id,
                    episode_id,
                    market_id,
                    event_id,
                    event_family_id,
                    trigger_time,
                    episode_start_event_time,
                    episode_end_event_time,
                    feature_schema_version,
                    detector_version,
                    triggering_rules,
                    cooldown_state,
                    feature_snapshot,
                    severity_score,
                    emitted,
                    created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?)
                """,
                (
                    candidate["candidate_id"],
                    candidate["episode_id"],
                    candidate["market_id"],
                    candidate["event_id"],
                    candidate["event_family_id"],
                    candidate["trigger_time"],
                    candidate["episode_start_event_time"],
                    candidate["episode_end_event_time"],
                    candidate["feature_schema_version"],
                    candidate["detector_version"],
                    json.dumps(candidate["rule_families"], sort_keys=True),
                    json.dumps(candidate["cooldown_state"], sort_keys=True),
                    json.dumps(feature_snapshot, sort_keys=True),
                    candidate["severity_score"],
                    candidate["trigger_time"],
                ),
            )
            if feature_items:
                conn.executemany(
                    """
                    INSERT INTO signal_features (
                        candidate_id,
                        episode_id,
                        market_id,
                        feature_name,
                        feature_value,
                        feature_schema_version,
                        observed_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    feature_items,
                )
            conn.commit()
        finally:
            conn.close()

    def get_checkpoint(self, *, source_system: str, partition_path: str) -> dict[str, Any] | None:
        checkpoint_key = f"{PHASE3_DETECTOR_VERSION}:{source_system}:{partition_path}"
        conn = get_conn()
        try:
            row = conn.execute(
                """
                SELECT checkpoint_key, file_offset, last_ordering_key, last_captured_at, updated_at
                FROM detector_checkpoints
                WHERE checkpoint_key = ?
                """,
                (checkpoint_key,),
            ).fetchone()
        finally:
            conn.close()
        if row is None:
            return None
        return {
            "checkpoint_key": row["checkpoint_key"],
            "file_offset": int(row["file_offset"] or 0),
            "last_ordering_key": row["last_ordering_key"],
            "last_captured_at": row["last_captured_at"],
            "updated_at": row["updated_at"],
        }

    def upsert_checkpoint(
        self,
        *,
        source_system: str,
        partition_path: str,
        file_offset: int,
        last_ordering_key: str | None,
        last_captured_at: str | None,
    ) -> None:
        checkpoint_key = f"{PHASE3_DETECTOR_VERSION}:{source_system}:{partition_path}"
        conn = get_conn()
        now = _iso(datetime.now(timezone.utc))
        try:
            conn.execute(
                """
                INSERT INTO detector_checkpoints (
                    checkpoint_key,
                    detector_version,
                    source_system,
                    partition_path,
                    file_offset,
                    last_ordering_key,
                    last_captured_at,
                    updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(checkpoint_key) DO UPDATE SET
                    file_offset = excluded.file_offset,
                    last_ordering_key = excluded.last_ordering_key,
                    last_captured_at = excluded.last_captured_at,
                    updated_at = excluded.updated_at
                """,
                (
                    checkpoint_key,
                    PHASE3_DETECTOR_VERSION,
                    source_system,
                    partition_path,
                    file_offset,
                    last_ordering_key,
                    last_captured_at,
                    now,
                ),
            )
            conn.commit()
        finally:
            conn.close()

    def load_persisted_candidates(self, *, start: str, end: str) -> list[dict[str, Any]]:
        conn = get_conn()
        try:
            rows = conn.execute(
                """
                SELECT
                    candidate_id,
                    episode_id,
                    market_id,
                    event_id,
                    event_family_id,
                    trigger_time,
                    episode_start_event_time,
                    episode_end_event_time,
                    detector_version,
                    feature_schema_version,
                    triggering_rules,
                    cooldown_state,
                    feature_snapshot,
                    severity_score
                FROM signal_candidates
                WHERE detector_version = ?
                  AND trigger_time >= ?
                  AND trigger_time < ?
                ORDER BY trigger_time ASC, candidate_id ASC
                """,
                (PHASE3_DETECTOR_VERSION, start, end),
            ).fetchall()
        finally:
            conn.close()
        return [
            {
                "candidate_id": row["candidate_id"],
                "episode_id": row["episode_id"],
                "market_id": row["market_id"],
                "event_id": row["event_id"],
                "event_family_id": row["event_family_id"],
                "trigger_time": row["trigger_time"],
                "episode_start_event_time": row["episode_start_event_time"],
                "episode_end_event_time": row["episode_end_event_time"],
                "detector_version": row["detector_version"],
                "feature_schema_version": row["feature_schema_version"],
                "triggering_rules": json.loads(row["triggering_rules"]) if row["triggering_rules"] else [],
                "cooldown_state": json.loads(row["cooldown_state"]) if row["cooldown_state"] else {},
                "feature_snapshot": json.loads(row["feature_snapshot"]) if row["feature_snapshot"] else {},
                "severity_score": float(row["severity_score"] or 0.0),
            }
            for row in rows
        ]


class Phase3Detector:
    def __init__(self, *, store: BaseStateStore, repository: Phase3Repository):
        self.store = store
        self.repository = repository
        self.summary = DetectorRunSummary()

    async def handle_envelope(self, envelope: dict[str, Any]) -> None:
        self.summary.processed_envelopes += 1
        entity_type = str(envelope.get("entity_type") or "")
        payload = envelope.get("payload") or {}

        if entity_type in {"recent_trades_page", "historical_trades_page"}:
            trades = payload.get("trades") or []
            for trade in trades:
                if isinstance(trade, dict):
                    await self._process_trade(trade)
                    self.summary.processed_trades += 1
            return

        if entity_type in {"prices_batch", "books_batch"}:
            snapshots = payload.get("market_snapshots") or []
            for snapshot in snapshots:
                if isinstance(snapshot, dict):
                    await self._process_snapshot(snapshot)
                    self.summary.processed_snapshots += 1
            return

        if entity_type.startswith("ws_frame::"):
            for event in payload.get("events") or []:
                if not isinstance(event, dict):
                    continue
                await self._process_ws_event(event)
            return

        self.summary.ignored_envelopes += 1

    async def _process_ws_event(self, event: dict[str, Any]) -> None:
        event_type = str(event.get("event_type") or "")
        if event_type == "last_trade_price":
            trade = event.get("trade")
            if isinstance(trade, dict):
                await self._process_trade(trade)
                self.summary.processed_trades += 1
            return

        if event_type == "price_change":
            for change in event.get("price_changes") or []:
                if not isinstance(change, dict):
                    continue
                snapshot = {
                    "market_id": change.get("market_id"),
                    "captured_at": event.get("captured_at"),
                    "yes_price": change.get("price") if change.get("outcome_side") == "YES" else None,
                    "no_price": change.get("price") if change.get("outcome_side") == "NO" else None,
                    "best_bid": change.get("best_bid") if change.get("outcome_side") == "YES" else None,
                    "best_ask": change.get("best_ask") if change.get("outcome_side") == "YES" else None,
                    "spread": change.get("spread") if change.get("outcome_side") == "YES" else None,
                    "source": "ws",
                }
                await self._process_snapshot(snapshot)
                self.summary.processed_snapshots += 1
            return

        if event_type == "best_bid_ask":
            best_bid_ask = event.get("best_bid_ask") or {}
            snapshot = {
                "market_id": event.get("market_id"),
                "captured_at": event.get("captured_at"),
                "yes_price": best_bid_ask.get("best_bid") if event.get("outcome_side") == "YES" else None,
                "no_price": best_bid_ask.get("best_bid") if event.get("outcome_side") == "NO" else None,
                "best_bid": best_bid_ask.get("best_bid") if event.get("outcome_side") == "YES" else None,
                "best_ask": best_bid_ask.get("best_ask") if event.get("outcome_side") == "YES" else None,
                "spread": best_bid_ask.get("spread") if event.get("outcome_side") == "YES" else None,
                "source": "ws",
            }
            await self._process_snapshot(snapshot)
            self.summary.processed_snapshots += 1
            return

        if event_type == "book":
            book = event.get("book") or {}
            if event.get("market_id"):
                snapshot = {
                    "market_id": event.get("market_id"),
                    "captured_at": event.get("captured_at"),
                    "yes_price": book.get("best_bid") if event.get("outcome_side") == "YES" else None,
                    "no_price": book.get("best_bid") if event.get("outcome_side") == "NO" else None,
                    "best_bid": book.get("best_bid") if event.get("outcome_side") == "YES" else None,
                    "best_ask": book.get("best_ask") if event.get("outcome_side") == "YES" else None,
                    "spread": book.get("spread") if event.get("outcome_side") == "YES" else None,
                    "source": "ws",
                }
                await self._process_snapshot(snapshot)
                self.summary.processed_snapshots += 1
            return

    async def _process_trade(self, trade: dict[str, Any]) -> None:
        market_id = str(trade.get("market_id") or "")
        if not market_id:
            return

        event_dt = _parse_iso(trade.get("trade_time") or trade.get("captured_at"))
        if event_dt is None:
            return

        state = await self.store.get_market_state(market_id)
        trade_points = list(state.get("trade_points") or [])
        wallet = (trade.get("proxy_wallet") or "").lower() or None
        if wallet:
            first_seen = await self.store.get_wallet_first_seen(wallet)
            if first_seen is None:
                await self.store.set_wallet_first_seen(wallet, _iso(event_dt))
                first_seen = _iso(event_dt)
        else:
            first_seen = None

        trade_points.append(
            {
                "ts": _iso(event_dt),
                "wallet": wallet,
                "side": str(trade.get("side") or "").upper() or None,
                "notional": float(trade.get("usdc_notional") or 0),
                "outcome_side": str(trade.get("outcome_side") or "").upper() or None,
                "trade_id": trade.get("trade_id"),
                "wallet_first_seen_at": first_seen,
            }
        )

        state["trade_points"] = trade_points
        pruned_state = self._prune_state(state, now=event_dt)
        await self.store.set_market_state(market_id, pruned_state)
        await self._evaluate_market(market_id, now=event_dt)

    async def _process_snapshot(self, snapshot: dict[str, Any]) -> None:
        market_id = str(snapshot.get("market_id") or "")
        if not market_id:
            return

        event_dt = _parse_iso(snapshot.get("captured_at"))
        if event_dt is None:
            return

        price = snapshot.get("yes_price")
        if price is None:
            price = snapshot.get("mid_price")
        if price is None:
            price = snapshot.get("best_bid")

        state = await self.store.get_market_state(market_id)
        price_points = list(state.get("price_points") or [])
        if price is not None:
            price_points.append({"ts": _iso(event_dt), "price": float(price)})
        state["price_points"] = price_points
        pruned_state = self._prune_state(state, now=event_dt)
        await self.store.set_market_state(market_id, pruned_state)

    def _prune_state(self, state: dict[str, Any], *, now: datetime) -> dict[str, Any]:
        history_floor = now - timedelta(seconds=max(PHASE3_HISTORY_SECONDS, PHASE3_WINDOW_SECONDS * 2))
        pruned = dict(state)
        pruned["trade_points"] = [
            point
            for point in state.get("trade_points", [])
            if (_parse_iso(point.get("ts")) or history_floor) >= history_floor
        ]
        pruned["price_points"] = [
            point
            for point in state.get("price_points", [])
            if (_parse_iso(point.get("ts")) or history_floor) >= history_floor
        ]
        return pruned

    async def _evaluate_market(self, market_id: str, *, now: datetime) -> None:
        state = await self.store.get_market_state(market_id)
        features = await self._compute_features(market_id, state, now=now)
        if features is None:
            return

        rule_families = self._triggered_rule_families(features)
        if not rule_families:
            return

        last_candidate = await self.store.get_last_candidate(market_id)
        suppressed, cooldown_state = self._cooldown_decision(
            features=features,
            rule_families=rule_families,
            now=now,
            last_candidate=last_candidate,
        )
        if suppressed:
            self.summary.candidates_suppressed += 1
            return

        metadata = self.repository.market_metadata(market_id)
        candidate = {
            "candidate_id": uuid4().hex,
            "episode_id": uuid4().hex,
            "market_id": market_id,
            "event_id": metadata.get("event_id"),
            "event_family_id": metadata.get("event_family_id"),
            "trigger_time": _iso(now),
            "episode_start_event_time": features["episode_start_event_time"],
            "episode_end_event_time": features["episode_end_event_time"],
            "feature_schema_version": PHASE3_FEATURE_SCHEMA_VERSION,
            "detector_version": PHASE3_DETECTOR_VERSION,
            "rule_families": rule_families,
            "primary_rule_family": rule_families[0],
            "cooldown_state": cooldown_state,
            "severity_score": features["severity_score"],
            "feature_snapshot": features,
            "episode_metadata": {
                "question": metadata.get("question"),
                "event_slug": metadata.get("event_slug"),
                "event_title": metadata.get("event_title"),
            },
        }
        self.repository.persist_candidate(candidate)
        await self.store.set_last_candidate(
            market_id,
            {
                "ts": candidate["trigger_time"],
                "severity_score": candidate["severity_score"],
                "rule_families": rule_families,
            },
        )
        self.summary.candidates_emitted += 1
        log.info(
            f"Phase 3 candidate emitted market={market_id} rules={','.join(rule_families)} "
            f"severity={candidate['severity_score']:.2f}"
        )

    async def _compute_features(
        self,
        market_id: str,
        state: dict[str, Any],
        *,
        now: datetime,
    ) -> dict[str, Any] | None:
        current_floor = now - timedelta(seconds=PHASE3_WINDOW_SECONDS)
        history_floor = now - timedelta(seconds=max(PHASE3_HISTORY_SECONDS, PHASE3_WINDOW_SECONDS * 2))
        previous_floor = current_floor - timedelta(seconds=PHASE3_WINDOW_SECONDS)

        trade_points = [
            point
            for point in state.get("trade_points", [])
            if (parsed := _parse_iso(point.get("ts"))) and parsed >= history_floor
        ]
        price_points = [
            point
            for point in state.get("price_points", [])
            if (parsed := _parse_iso(point.get("ts"))) and parsed >= history_floor
        ]
        if not trade_points:
            return None

        current_trades = [
            point for point in trade_points if (_parse_iso(point.get("ts")) or current_floor) >= current_floor
        ]
        previous_trades = [
            point
            for point in trade_points
            if previous_floor <= (_parse_iso(point.get("ts")) or previous_floor) < current_floor
        ]
        current_prices = [
            point for point in price_points if (_parse_iso(point.get("ts")) or current_floor) >= current_floor
        ]
        previous_prices = [
            point
            for point in price_points
            if previous_floor <= (_parse_iso(point.get("ts")) or previous_floor) < current_floor
        ]
        if not current_trades:
            return None

        current_notional = sum(float(point.get("notional") or 0) for point in current_trades)
        if current_notional < PHASE3_MIN_WINDOW_NOTIONAL:
            return None
        previous_notional = sum(float(point.get("notional") or 0) for point in previous_trades)

        buy_notional = sum(
            float(point.get("notional") or 0)
            for point in current_trades
            if str(point.get("side") or "").upper() == "BUY"
        )
        sell_notional = sum(
            float(point.get("notional") or 0)
            for point in current_trades
            if str(point.get("side") or "").upper() == "SELL"
        )
        directional_imbalance = abs(buy_notional - sell_notional) / current_notional if current_notional else 0.0

        wallet_totals: dict[str, float] = defaultdict(float)
        fresh_wallets: set[str] = set()
        fresh_notional = 0.0
        for point in current_trades:
            wallet = point.get("wallet")
            notional = float(point.get("notional") or 0)
            if wallet:
                wallet_totals[str(wallet)] += notional
            first_seen = _parse_iso(point.get("wallet_first_seen_at"))
            if wallet and first_seen and first_seen >= current_floor:
                fresh_wallets.add(str(wallet))
                fresh_notional += notional

        concentration_ratio = (
            max(wallet_totals.values()) / current_notional if wallet_totals and current_notional else 0.0
        )
        fresh_wallet_count = len(fresh_wallets)
        fresh_wallet_notional_share = fresh_notional / current_notional if current_notional else 0.0
        volume_acceleration = (
            current_notional / previous_notional
            if previous_notional > 0
            else (2.0 if current_notional > 0 else 0.0)
        )

        probability_velocity = self._price_velocity(current_prices)
        previous_velocity = self._price_velocity(previous_prices)
        probability_acceleration = probability_velocity - previous_velocity

        episode_times = [
            _parse_iso(point.get("ts"))
            for point in current_trades + current_prices
            if _parse_iso(point.get("ts")) is not None
        ]
        episode_start = min(episode_times) if episode_times else now
        episode_end = max(episode_times) if episode_times else now

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
            "wallet_count": len(wallet_totals),
            "trade_count": len(current_trades),
            "price_point_count": len(current_prices),
            "window_seconds": PHASE3_WINDOW_SECONDS,
            "episode_start_event_time": _iso(episode_start),
            "episode_end_event_time": _iso(episode_end),
            "severity_score": round(
                self._severity_score(
                    fresh_wallet_count=fresh_wallet_count,
                    fresh_wallet_notional_share=fresh_wallet_notional_share,
                    directional_imbalance=directional_imbalance,
                    concentration_ratio=concentration_ratio,
                    probability_velocity=probability_velocity,
                    probability_acceleration=probability_acceleration,
                    volume_acceleration=volume_acceleration,
                ),
                6,
            ),
        }

    @staticmethod
    def _price_velocity(points: list[dict[str, Any]]) -> float:
        ordered = sorted(
            (point for point in points if point.get("price") is not None),
            key=lambda point: point.get("ts") or "",
        )
        if len(ordered) < 2:
            return 0.0
        first_dt = _parse_iso(ordered[0].get("ts"))
        last_dt = _parse_iso(ordered[-1].get("ts"))
        if first_dt is None or last_dt is None or last_dt <= first_dt:
            return 0.0
        elapsed_minutes = (last_dt - first_dt).total_seconds() / 60.0
        if elapsed_minutes <= 0:
            return 0.0
        return (float(ordered[-1]["price"]) - float(ordered[0]["price"])) / elapsed_minutes

    @staticmethod
    def _severity_score(
        *,
        fresh_wallet_count: int,
        fresh_wallet_notional_share: float,
        directional_imbalance: float,
        concentration_ratio: float,
        probability_velocity: float,
        probability_acceleration: float,
        volume_acceleration: float,
    ) -> float:
        score = 0.0
        score += fresh_wallet_count / max(PHASE3_MIN_FRESH_WALLET_COUNT, 1)
        score += fresh_wallet_notional_share / max(PHASE3_MIN_FRESH_WALLET_NOTIONAL_SHARE, 0.01)
        score += directional_imbalance / max(PHASE3_MIN_DIRECTIONAL_IMBALANCE, 0.01)
        score += concentration_ratio / max(PHASE3_MIN_CONCENTRATION_RATIO, 0.01)
        score += abs(probability_velocity) / max(PHASE3_MIN_PROBABILITY_VELOCITY, 0.0001)
        score += abs(probability_acceleration) / max(PHASE3_MIN_PROBABILITY_ACCELERATION, 0.0001)
        score += volume_acceleration / max(PHASE3_MIN_VOLUME_ACCELERATION, 0.01)
        return score

    @staticmethod
    def _triggered_rule_families(features: dict[str, Any]) -> list[str]:
        rules: list[str] = []
        if (
            features["fresh_wallet_count"] >= PHASE3_MIN_FRESH_WALLET_COUNT
            and features["fresh_wallet_notional_share"] >= PHASE3_MIN_FRESH_WALLET_NOTIONAL_SHARE
            and abs(features["probability_velocity"]) >= PHASE3_MIN_PROBABILITY_VELOCITY
        ):
            rules.append("fresh_wallet_burst")

        if (
            features["directional_imbalance"] >= PHASE3_MIN_DIRECTIONAL_IMBALANCE
            and features["concentration_ratio"] >= PHASE3_MIN_CONCENTRATION_RATIO
        ):
            rules.append("concentrated_directional_flow")

        if (
            abs(features["probability_velocity"]) >= PHASE3_MIN_PROBABILITY_VELOCITY
            and abs(features["probability_acceleration"]) >= PHASE3_MIN_PROBABILITY_ACCELERATION
            and (
                features["fresh_wallet_count"] >= 1
                or features["concentration_ratio"] >= PHASE3_MIN_CONCENTRATION_RATIO
            )
            and features["volume_acceleration"] >= PHASE3_MIN_VOLUME_ACCELERATION
        ):
            rules.append("fast_repricing_with_wallet_support")

        return rules

    @staticmethod
    def _cooldown_decision(
        *,
        features: dict[str, Any],
        rule_families: list[str],
        now: datetime,
        last_candidate: dict[str, Any] | None,
    ) -> tuple[bool, dict[str, Any]]:
        if not last_candidate:
            return False, {"suppressed": False, "reason": "no_prior_candidate"}

        last_dt = _parse_iso(last_candidate.get("ts"))
        if last_dt is None:
            return False, {"suppressed": False, "reason": "invalid_prior_candidate"}

        elapsed = (now - last_dt).total_seconds()
        if elapsed >= PHASE3_COOLDOWN_SECONDS:
            return False, {"suppressed": False, "reason": "cooldown_expired", "elapsed_seconds": elapsed}

        prior_rules = set(last_candidate.get("rule_families") or [])
        overlapping_rules = sorted(prior_rules.intersection(rule_families))
        prior_severity = float(last_candidate.get("severity_score") or 0.0)
        current_severity = float(features.get("severity_score") or 0.0)
        materially_stronger = current_severity >= (prior_severity * 1.25)
        if overlapping_rules and not materially_stronger:
            return True, {
                "suppressed": True,
                "reason": "cooldown_overlap_without_material_increase",
                "elapsed_seconds": elapsed,
                "prior_severity_score": prior_severity,
                "current_severity_score": current_severity,
                "overlapping_rules": overlapping_rules,
            }

        return False, {
            "suppressed": False,
            "reason": "materially_stronger_during_cooldown" if overlapping_rules else "different_rule_family",
            "elapsed_seconds": elapsed,
            "prior_severity_score": prior_severity,
            "current_severity_score": current_severity,
        }


async def run_phase3_detector_window(
    *,
    start: str,
    end: str,
    store: BaseStateStore,
    repository: Phase3Repository,
    source_systems: list[str] | None = None,
    limit_envelopes: int | None = None,
) -> DetectorRunSummary:
    start_dt = _parse_iso(start)
    end_dt = _parse_iso(end)
    if start_dt is None or end_dt is None or end_dt <= start_dt:
        raise ValueError("A valid start/end window is required and end must be later than start.")

    detector = Phase3Detector(store=store, repository=repository)
    source_list = source_systems or DEFAULT_PHASE3_SOURCE_SYSTEMS

    envelopes: list[dict[str, Any]] = []
    for source_system in source_list:
        for hour in _iter_hours(start_dt, end_dt):
            for row in _iter_partition_rows(_partition_file(source_system, hour)):
                captured_at = _parse_iso(row.get("captured_at"))
                if captured_at is None or not (start_dt <= captured_at < end_dt):
                    continue
                envelopes.append(row)

    envelopes.sort(
        key=lambda row: (
            row.get("captured_at") or "",
            row.get("ordering_key") or "",
            row.get("envelope_id") or "",
        )
    )

    if limit_envelopes is not None:
        envelopes = envelopes[:limit_envelopes]

    for envelope in envelopes:
        await detector.handle_envelope(envelope)

    return detector.summary
