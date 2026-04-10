"""Deterministic semantic helpers for Person 2 Phase 1 validation.

This module contains the first executable versions of the semantic rules
described in the Phase 1 contract:
- archive-derived first_seen_at
- computed fresh-wallet flags
- trade-to-market episode linkage
"""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from validation.phase1_validators import SemanticsRule


@dataclass(slots=True)
class WalletFirstSeenRecord:
    """Represent the archive-derived first_seen_at value for one wallet."""

    wallet_id: str
    first_seen_at: str
    trade_count: int


@dataclass(slots=True)
class FreshWalletRecord:
    """Represent the first-version fresh-wallet classification for one wallet."""

    wallet_id: str
    first_seen_at: str
    latest_trade_at: str
    trade_count: int
    age_days: float
    is_fresh: bool
    confidence: str


@dataclass(slots=True)
class EpisodeLinkageRecord:
    """Represent one derived market episode produced from canonical trades."""

    episode_id: str
    market_id: str
    condition_id: str
    episode_start_at: str
    episode_end_at: str
    trade_count: int
    distinct_wallet_count: int
    yes_trade_count: int
    no_trade_count: int


def derive_wallet_first_seen(
    conn: sqlite3.Connection,
    trade_table_name: str,
    semantic_rule: "SemanticsRule",
) -> list[WalletFirstSeenRecord]:
    """Derive archive-backed first_seen_at values from canonical trades."""

    entity_field = str(semantic_rule.details.get("entity_field", "proxy_wallet"))
    event_time_field = str(semantic_rule.details.get("event_time_field", "trade_time"))
    exclude_blank = bool(semantic_rule.details.get("exclude_blank_entities", True))
    where_clause = ""
    if exclude_blank:
        where_clause = (
            f"WHERE {entity_field} IS NOT NULL AND TRIM(CAST({entity_field} AS TEXT)) != '' "
            f"AND {event_time_field} IS NOT NULL AND TRIM(CAST({event_time_field} AS TEXT)) != ''"
        )

    rows = conn.execute(
        f"""
        SELECT
            {entity_field} AS wallet_id,
            MIN({event_time_field}) AS first_seen_at,
            COUNT(*) AS trade_count
        FROM {trade_table_name}
        {where_clause}
        GROUP BY {entity_field}
        ORDER BY first_seen_at ASC, wallet_id ASC
        """
    ).fetchall()

    return [
        WalletFirstSeenRecord(
            wallet_id=str(row["wallet_id"]),
            first_seen_at=str(row["first_seen_at"]),
            trade_count=int(row["trade_count"] or 0),
        )
        for row in rows
    ]


def derive_fresh_wallet_flags(
    conn: sqlite3.Connection,
    trade_table_name: str,
    first_seen_rule: "SemanticsRule",
    fresh_wallet_rule: "SemanticsRule",
) -> list[FreshWalletRecord]:
    """Compute the first deterministic version of the fresh-wallet flag."""

    entity_field = str(fresh_wallet_rule.details.get("entity_field", "proxy_wallet"))
    event_time_field = str(fresh_wallet_rule.details.get("event_time_field", "trade_time"))
    max_age_days = float(fresh_wallet_rule.details.get("max_age_days", 30))
    max_trade_count = int(fresh_wallet_rule.details.get("max_trade_count", 3))
    profile_age_field = fresh_wallet_rule.details.get("profile_age_field")
    has_profile_age = isinstance(profile_age_field, str) and bool(profile_age_field.strip())

    first_seen_by_wallet = {
        record.wallet_id: record for record in derive_wallet_first_seen(conn, trade_table_name, first_seen_rule)
    }
    if not first_seen_by_wallet:
        return []

    rows = conn.execute(
        f"""
        SELECT
            {entity_field} AS wallet_id,
            MAX({event_time_field}) AS latest_trade_at,
            COUNT(*) AS trade_count,
            COUNT(
                CASE
                    WHEN {event_time_field} IS NULL OR TRIM(CAST({event_time_field} AS TEXT)) = ''
                    THEN 1
                END
            ) AS missing_event_times
        FROM {trade_table_name}
        WHERE {entity_field} IS NOT NULL AND TRIM(CAST({entity_field} AS TEXT)) != ''
        GROUP BY {entity_field}
        ORDER BY latest_trade_at ASC, wallet_id ASC
        """
    ).fetchall()

    records: list[FreshWalletRecord] = []
    for row in rows:
        wallet_id = str(row["wallet_id"])
        first_seen_record = first_seen_by_wallet.get(wallet_id)
        if first_seen_record is None:
            continue

        first_seen_dt = _parse_timestamp(first_seen_record.first_seen_at)
        latest_trade_dt = _parse_timestamp(str(row["latest_trade_at"]))
        age_days = max((latest_trade_dt - first_seen_dt).total_seconds(), 0.0) / 86400.0
        trade_count = int(row["trade_count"] or 0)
        missing_event_times = int(row["missing_event_times"] or 0)

        is_fresh = age_days <= max_age_days and trade_count <= max_trade_count
        if missing_event_times > 0:
            confidence = "low"
        elif has_profile_age:
            confidence = "full"
        else:
            confidence = "limited"

        records.append(
            FreshWalletRecord(
                wallet_id=wallet_id,
                first_seen_at=first_seen_record.first_seen_at,
                latest_trade_at=str(row["latest_trade_at"]),
                trade_count=trade_count,
                age_days=age_days,
                is_fresh=is_fresh,
                confidence=confidence,
            )
        )

    return records


def derive_trade_episode_linkage(
    conn: sqlite3.Connection,
    trade_table_name: str,
    semantic_rule: "SemanticsRule",
) -> tuple[list[EpisodeLinkageRecord], int]:
    """Link trades into deterministic market episodes using an inactivity gap."""

    join_keys = [str(value) for value in semantic_rule.details.get("join_keys", [])]
    if len(join_keys) < 2:
        raise ValueError("Episode linkage requires at least market_id and condition_id join keys")

    market_key, condition_key = join_keys[0], join_keys[1]
    wallet_key = str(semantic_rule.details.get("wallet_key", "proxy_wallet"))
    outcome_side_field = str(semantic_rule.details.get("outcome_side_field", "outcome_side"))
    event_time_field = str(semantic_rule.details.get("event_time_field", "trade_time"))
    gap_minutes = float(semantic_rule.details.get("gap_minutes", 30))
    gap_seconds = gap_minutes * 60.0

    rows = conn.execute(
        f"""
        SELECT
            trade_id,
            {market_key} AS market_id,
            {condition_key} AS condition_id,
            {wallet_key} AS wallet_id,
            {outcome_side_field} AS outcome_side,
            {event_time_field} AS trade_time
        FROM {trade_table_name}
        ORDER BY market_id ASC, condition_id ASC, trade_time ASC, trade_id ASC
        """
    ).fetchall()

    episodes: list[EpisodeLinkageRecord] = []
    unresolved_count = 0

    current_partition: tuple[str, str] | None = None
    current_episode_index = 0
    current_episode: dict[str, object] | None = None
    previous_trade_dt: datetime | None = None

    for row in rows:
        market_id = _normalize_nullable_text(row["market_id"])
        condition_id = _normalize_nullable_text(row["condition_id"])
        trade_time = _normalize_nullable_text(row["trade_time"])

        if not market_id or not condition_id or not trade_time:
            unresolved_count += 1
            continue

        trade_dt = _parse_timestamp(trade_time)
        partition = (market_id, condition_id)
        start_new_episode = False

        if current_partition != partition or current_episode is None:
            start_new_episode = True
        elif previous_trade_dt is None:
            start_new_episode = True
        else:
            gap_seconds_observed = (trade_dt - previous_trade_dt).total_seconds()
            if gap_seconds_observed > gap_seconds:
                start_new_episode = True

        if start_new_episode:
            if current_episode is not None:
                episodes.append(_build_episode_record(current_episode))
            if current_partition != partition:
                current_episode_index = 0
            current_partition = partition
            current_episode_index += 1
            current_episode = {
                "episode_id": f"{market_id}:{condition_id}:{current_episode_index}",
                "market_id": market_id,
                "condition_id": condition_id,
                "episode_start_at": trade_time,
                "episode_end_at": trade_time,
                "trade_count": 0,
                "wallet_ids": set(),
                "yes_trade_count": 0,
                "no_trade_count": 0,
            }

        current_episode["trade_count"] = int(current_episode["trade_count"]) + 1
        current_episode["episode_end_at"] = trade_time

        wallet_id = _normalize_nullable_text(row["wallet_id"])
        if wallet_id:
            wallet_ids = current_episode["wallet_ids"]
            assert isinstance(wallet_ids, set)
            wallet_ids.add(wallet_id)

        outcome_side = _normalize_nullable_text(row["outcome_side"])
        if outcome_side == "YES":
            current_episode["yes_trade_count"] = int(current_episode["yes_trade_count"]) + 1
        elif outcome_side == "NO":
            current_episode["no_trade_count"] = int(current_episode["no_trade_count"]) + 1

        previous_trade_dt = trade_dt

    if current_episode is not None:
        episodes.append(_build_episode_record(current_episode))

    return episodes, unresolved_count


def _build_episode_record(raw_episode: dict[str, object]) -> EpisodeLinkageRecord:
    """Convert the mutable episode accumulator into an immutable record."""

    wallet_ids = raw_episode["wallet_ids"]
    assert isinstance(wallet_ids, set)

    return EpisodeLinkageRecord(
        episode_id=str(raw_episode["episode_id"]),
        market_id=str(raw_episode["market_id"]),
        condition_id=str(raw_episode["condition_id"]),
        episode_start_at=str(raw_episode["episode_start_at"]),
        episode_end_at=str(raw_episode["episode_end_at"]),
        trade_count=int(raw_episode["trade_count"]),
        distinct_wallet_count=len(wallet_ids),
        yes_trade_count=int(raw_episode["yes_trade_count"]),
        no_trade_count=int(raw_episode["no_trade_count"]),
    )


def _normalize_nullable_text(value: object) -> str | None:
    """Normalize SQLite output into a stripped string or None."""

    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _parse_timestamp(value: str) -> datetime:
    """Parse a canonical event timestamp into a timezone-aware datetime."""

    normalized = value.strip().replace("Z", "+00:00")
    dt = datetime.fromisoformat(normalized)
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)
