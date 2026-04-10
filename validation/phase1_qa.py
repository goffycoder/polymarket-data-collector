"""Manual QA sampling helpers for Person 2 Phase 1.

This module turns derived market episodes into reviewer-friendly CSV rows so the
manual QA requirement can be executed consistently and reproducibly.
"""

from __future__ import annotations

import csv
import sqlite3
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any

from validation.phase1_semantics import (
    EpisodeLinkageRecord,
    derive_trade_episode_linkage,
    derive_fresh_wallet_flags,
)
from validation.phase1_validators import _read_yaml, _resolve_approved_market_ids  # reuse contract parsing logic
from validation.phase1_report import ValidationSummary

if TYPE_CHECKING:
    from validation.phase1_validators import Phase1ValidationContract, ValidationRuntime


@dataclass(slots=True)
class Phase1QASampleRow:
    """Represent one manually reviewable Phase 1 episode sample row."""

    episode_id: str
    market_id: str
    condition_id: str
    episode_start_at: str
    episode_end_at: str
    trade_count: int
    distinct_wallet_count: int
    yes_trade_count: int
    no_trade_count: int
    both_side_observed: str
    estimated_fresh_wallet_count: int
    unresolved_trade_count: int
    review_status: str
    failure_taxonomy: str
    reviewer_notes: str


def generate_phase1_qa_samples(
    conn: sqlite3.Connection,
    runtime: "ValidationRuntime",
    summary: ValidationSummary,
    *,
    sample_size: int | None = None,
) -> list[Phase1QASampleRow]:
    """Generate QA sample rows from derived trade episodes.

    The sampling strategy is deterministic so repeated runs against the same
    database and configuration produce the same review sheet ordering.
    """

    config = _read_yaml(runtime.config_path)
    qa_config = config.get("qa", {})
    target_sample_size = int(sample_size or qa_config.get("sample_size", 20))
    include_only_approved_universe = bool(qa_config.get("include_only_approved_universe", True))

    approved_market_ids: set[str] | None = None
    if include_only_approved_universe:
        approved_market_ids = _resolve_approved_market_ids(conn, runtime, summary)
        if not approved_market_ids:
            return []

    episodes, unresolved_trade_count = derive_trade_episode_linkage(
        conn,
        runtime.contract.trade_table_name,
        runtime.contract.semantics["episode_linkage"],
    )

    if approved_market_ids is not None:
        episodes = [episode for episode in episodes if episode.market_id in approved_market_ids]

    if not episodes:
        return []

    fresh_wallet_lookup = _build_episode_fresh_wallet_lookup(conn, runtime, episodes)
    selected_episodes = _select_episode_samples(episodes, target_sample_size)

    return [
        Phase1QASampleRow(
            episode_id=episode.episode_id,
            market_id=episode.market_id,
            condition_id=episode.condition_id,
            episode_start_at=episode.episode_start_at,
            episode_end_at=episode.episode_end_at,
            trade_count=episode.trade_count,
            distinct_wallet_count=episode.distinct_wallet_count,
            yes_trade_count=episode.yes_trade_count,
            no_trade_count=episode.no_trade_count,
            both_side_observed="yes" if episode.yes_trade_count > 0 and episode.no_trade_count > 0 else "no",
            estimated_fresh_wallet_count=fresh_wallet_lookup.get(episode.episode_id, 0),
            unresolved_trade_count=unresolved_trade_count,
            review_status="pending",
            failure_taxonomy="",
            reviewer_notes="",
        )
        for episode in selected_episodes
    ]


def write_phase1_qa_csv(output_path: str | Path, sample_rows: list[Phase1QASampleRow]) -> Path:
    """Write QA sample rows to a CSV file for manual review."""

    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)

    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(asdict(sample_rows[0]).keys()) if sample_rows else list(asdict(_empty_qa_row()).keys()))
        writer.writeheader()
        for row in sample_rows:
            writer.writerow(asdict(row))

    return path


def _select_episode_samples(
    episodes: list[EpisodeLinkageRecord],
    sample_size: int,
) -> list[EpisodeLinkageRecord]:
    """Select a deterministic subset of episodes for manual QA."""

    ranked = sorted(
        episodes,
        key=lambda episode: (
            -episode.trade_count,
            -episode.distinct_wallet_count,
            episode.episode_start_at,
            episode.episode_id,
        ),
    )
    return ranked[:sample_size]


def _build_episode_fresh_wallet_lookup(
    conn: sqlite3.Connection,
    runtime: "ValidationRuntime",
    episodes: list[EpisodeLinkageRecord],
) -> dict[str, int]:
    """Estimate how many fresh wallets appear in each derived episode."""

    episode_map = {episode.episode_id: episode for episode in episodes}
    fresh_wallet_records = derive_fresh_wallet_flags(
        conn,
        runtime.contract.trade_table_name,
        runtime.contract.semantics["first_seen_at"],
        runtime.contract.semantics["fresh_wallet"],
    )
    fresh_wallets = {record.wallet_id for record in fresh_wallet_records if record.is_fresh}
    if not fresh_wallets:
        return {episode_id: 0 for episode_id in episode_map}

    linkage_rule = runtime.contract.semantics["episode_linkage"]
    join_keys = [str(value) for value in linkage_rule.details.get("join_keys", [])]
    market_key = join_keys[0]
    condition_key = join_keys[1]
    wallet_key = str(linkage_rule.details.get("wallet_key", "proxy_wallet"))
    event_time_field = str(linkage_rule.details.get("event_time_field", "trade_time"))

    rows = conn.execute(
        f"""
        SELECT
            {market_key} AS market_id,
            {condition_key} AS condition_id,
            {wallet_key} AS wallet_id,
            {event_time_field} AS trade_time
        FROM {runtime.contract.trade_table_name}
        WHERE {wallet_key} IS NOT NULL AND TRIM(CAST({wallet_key} AS TEXT)) != ''
          AND {event_time_field} IS NOT NULL AND TRIM(CAST({event_time_field} AS TEXT)) != ''
        ORDER BY market_id ASC, condition_id ASC, trade_time ASC
        """
    ).fetchall()

    lookup = {episode_id: set() for episode_id in episode_map}
    for row in rows:
        wallet_id = str(row["wallet_id"])
        if wallet_id not in fresh_wallets:
            continue
        trade_time = str(row["trade_time"])
        market_id = str(row["market_id"])
        condition_id = str(row["condition_id"])
        for episode in episodes:
            if episode.market_id != market_id or episode.condition_id != condition_id:
                continue
            if episode.episode_start_at <= trade_time <= episode.episode_end_at:
                lookup[episode.episode_id].add(wallet_id)
                break

    return {episode_id: len(wallet_ids) for episode_id, wallet_ids in lookup.items()}


def _empty_qa_row() -> Phase1QASampleRow:
    """Return an empty QA row so CSV headers remain stable without data."""

    return Phase1QASampleRow(
        episode_id="",
        market_id="",
        condition_id="",
        episode_start_at="",
        episode_end_at="",
        trade_count=0,
        distinct_wallet_count=0,
        yes_trade_count=0,
        no_trade_count=0,
        both_side_observed="",
        estimated_fresh_wallet_count=0,
        unresolved_trade_count=0,
        review_status="pending",
        failure_taxonomy="",
        reviewer_notes="",
    )
