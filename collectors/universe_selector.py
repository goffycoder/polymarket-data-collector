"""
collectors/universe_selector.py — Local universe policy loader and selector.

Builds the approved high-resolution market universe from synced event/market
metadata and writes excluded-but-interesting candidate review events for manual
inspection.
"""
from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass, field
from pathlib import Path

import yaml


WATCHLISTS_PATH = Path(__file__).resolve().parents[1] / "config" / "watchlists.yaml"


@dataclass(frozen=True)
class RuleSet:
    """Declarative inclusion or exclusion rules."""

    tag_ids: frozenset[str] = field(default_factory=frozenset)
    keywords: tuple[str, ...] = ()
    event_slugs: frozenset[str] = field(default_factory=frozenset)
    market_slugs: frozenset[str] = field(default_factory=frozenset)
    min_event_liquidity: float = 0.0
    min_market_liquidity: float = 0.0


@dataclass(frozen=True)
class CandidateReviewPolicy:
    """Review policy for excluded events that may still matter for finance."""

    min_event_liquidity: float = 0.0
    keywords: tuple[str, ...] = ()
    max_results: int = 100


@dataclass(frozen=True)
class RuntimePolicy:
    """Runtime policy flags for expensive collectors."""

    high_resolution_only_for_approved_universe: bool = True
    candidate_review_enabled: bool = True


@dataclass(frozen=True)
class UniversePolicy:
    """Complete universe selection policy loaded from YAML."""

    include: RuleSet
    exclude: RuleSet
    candidate_review: CandidateReviewPolicy
    runtime: RuntimePolicy


@dataclass(frozen=True)
class MarketDescriptor:
    """Approved market descriptor used by runtime collectors."""

    market_id: str
    event_id: str
    event_slug: str
    event_title: str
    market_slug: str
    question: str
    condition_id: str
    yes_token_id: str
    no_token_id: str
    tier: int
    market_volume: float
    market_liquidity: float

    def tokens(self) -> tuple[str, str]:
        """Return the YES and NO asset IDs for this market."""
        return (self.yes_token_id, self.no_token_id)


@dataclass(frozen=True)
class TokenContext:
    """Metadata required to interpret token-specific market data."""

    market_id: str
    condition_id: str
    outcome_side: str


@dataclass(frozen=True)
class ReviewCandidate:
    """Excluded event that still deserves analyst review."""

    event_id: str
    event_slug: str
    event_title: str
    event_liquidity: float
    event_volume: float
    matched_keywords: tuple[str, ...]
    matched_tag_ids: tuple[str, ...]
    reason: str


@dataclass(frozen=True)
class UniverseSelection:
    """Approved markets plus candidate-review output for a sync cycle."""

    tier0_markets: tuple[MarketDescriptor, ...]
    tier1_markets: tuple[MarketDescriptor, ...]
    tier2_markets: tuple[MarketDescriptor, ...]
    token_context: dict[str, TokenContext]
    review_candidates: tuple[ReviewCandidate, ...]


def load_universe_policy(path: Path | None = None) -> UniversePolicy:
    """Load and normalize the YAML universe policy."""
    config_path = path or WATCHLISTS_PATH
    with open(config_path, "r", encoding="utf-8") as handle:
        raw = yaml.safe_load(handle) or {}

    universe = raw.get("universe", {})
    include = _build_ruleset(universe.get("include", {}))
    exclude = _build_ruleset(universe.get("exclude", {}))
    review_raw = universe.get("candidate_review", {})
    runtime_raw = universe.get("runtime", {})

    return UniversePolicy(
        include=include,
        exclude=exclude,
        candidate_review=CandidateReviewPolicy(
            min_event_liquidity=float(review_raw.get("min_event_liquidity", 0) or 0),
            keywords=_normalize_keywords(review_raw.get("keywords", [])),
            max_results=int(review_raw.get("max_results", 100) or 100),
        ),
        runtime=RuntimePolicy(
            high_resolution_only_for_approved_universe=bool(
                runtime_raw.get("high_resolution_only_for_approved_universe", True)
            ),
            candidate_review_enabled=bool(runtime_raw.get("candidate_review_enabled", True)),
        ),
    )


def select_runtime_universe(
    conn: sqlite3.Connection,
    policy: UniversePolicy,
    max_ws_tokens: int,
) -> UniverseSelection:
    """Select approved runtime markets and persist candidate-review events."""
    rows = conn.execute(
        """
        SELECT
            m.market_id,
            m.event_id,
            m.slug AS market_slug,
            m.question,
            m.description AS market_description,
            m.condition_id,
            m.yes_token_id,
            m.no_token_id,
            m.tier,
            COALESCE(m.volume, 0) AS market_volume,
            COALESCE(m.liquidity, 0) AS market_liquidity,
            e.title AS event_title,
            e.description AS event_description,
            e.slug AS event_slug,
            e.tags AS event_tags,
            e.tag_ids AS event_tag_ids,
            COALESCE(e.volume, 0) AS event_volume,
            COALESCE(e.liquidity, 0) AS event_liquidity
        FROM markets m
        JOIN events e ON m.event_id = e.event_id
        WHERE m.status = 'active'
          AND e.status = 'active'
          AND m.condition_id IS NOT NULL
          AND m.yes_token_id IS NOT NULL
          AND m.no_token_id IS NOT NULL
        ORDER BY m.volume DESC, m.market_id ASC
        """
    ).fetchall()

    approved: list[MarketDescriptor] = []
    candidates_by_event: dict[str, ReviewCandidate] = {}

    for row in rows:
        market = _row_to_market_descriptor(row)
        context = _build_text_context(row)
        tag_ids = _decode_string_list(row["event_tag_ids"])

        if _is_manual_override(market, policy.include):
            approved.append(market)
            continue

        if _is_hard_excluded(market, context, tag_ids, policy.exclude):
            continue

        include_reasons = _include_reasons(market, context, tag_ids, policy.include)
        if include_reasons and _meets_liquidity_thresholds(row, policy.include):
            approved.append(market)
            continue

        if not policy.runtime.candidate_review_enabled:
            continue

        review_candidate = _build_review_candidate(row, context, tag_ids, policy)
        if review_candidate:
            existing = candidates_by_event.get(review_candidate.event_id)
            if existing is None or review_candidate.event_liquidity > existing.event_liquidity:
                candidates_by_event[review_candidate.event_id] = review_candidate

    approved.sort(key=lambda item: (-item.market_volume, item.market_id))
    tier1_markets = tuple(market for market in approved if market.tier == 1)
    tier2_markets = tuple(market for market in approved if market.tier == 2)
    max_tier0_markets = max(1, max_ws_tokens // 2) if max_ws_tokens else 0
    tier0_markets = tier1_markets[:max_tier0_markets]

    review_candidates = tuple(
        sorted(
            candidates_by_event.values(),
            key=lambda candidate: (-candidate.event_liquidity, candidate.event_id),
        )[: policy.candidate_review.max_results]
    )
    persist_review_candidates(conn, review_candidates)

    return UniverseSelection(
        tier0_markets=tier0_markets,
        tier1_markets=tier1_markets,
        tier2_markets=tier2_markets,
        token_context=build_token_context(approved),
        review_candidates=review_candidates,
    )


def build_token_context(markets: list[MarketDescriptor] | tuple[MarketDescriptor, ...]) -> dict[str, TokenContext]:
    """Create the asset lookup used by WS and price collectors."""
    context: dict[str, TokenContext] = {}
    for market in markets:
        context[market.yes_token_id] = TokenContext(
            market_id=market.market_id,
            condition_id=market.condition_id,
            outcome_side="YES",
        )
        context[market.no_token_id] = TokenContext(
            market_id=market.market_id,
            condition_id=market.condition_id,
            outcome_side="NO",
        )
    return context


def persist_review_candidates(
    conn: sqlite3.Connection,
    candidates: tuple[ReviewCandidate, ...],
) -> None:
    """Refresh the candidate-review table for the current sync cycle."""
    conn.execute("DELETE FROM universe_review_candidates")
    if candidates:
        conn.executemany(
            """
            INSERT INTO universe_review_candidates (
                event_id, event_slug, event_title, event_liquidity, event_volume,
                matched_keywords, matched_tag_ids, reason, generated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            """,
            [
                (
                    candidate.event_id,
                    candidate.event_slug,
                    candidate.event_title,
                    candidate.event_liquidity,
                    candidate.event_volume,
                    json.dumps(list(candidate.matched_keywords)),
                    json.dumps(list(candidate.matched_tag_ids)),
                    candidate.reason,
                )
                for candidate in candidates
            ],
        )
    conn.commit()


def _build_ruleset(raw: dict) -> RuleSet:
    """Normalize one ruleset from YAML into typed fields."""
    return RuleSet(
        tag_ids=frozenset(str(tag_id) for tag_id in raw.get("tag_ids", [])),
        keywords=_normalize_keywords(raw.get("keywords", [])),
        event_slugs=frozenset(_normalize_slug_list(raw.get("event_slugs", []))),
        market_slugs=frozenset(_normalize_slug_list(raw.get("market_slugs", []))),
        min_event_liquidity=float(raw.get("min_event_liquidity", 0) or 0),
        min_market_liquidity=float(raw.get("min_market_liquidity", 0) or 0),
    )


def _normalize_keywords(items: list[str]) -> tuple[str, ...]:
    """Normalize keywords for case-insensitive substring matching."""
    return tuple(sorted({str(item).strip().lower() for item in items if str(item).strip()}))


def _normalize_slug_list(items: list[str]) -> tuple[str, ...]:
    """Normalize slug lists for exact matching."""
    return tuple(sorted({str(item).strip().lower() for item in items if str(item).strip()}))


def _decode_string_list(raw: str | None) -> tuple[str, ...]:
    """Parse a JSON string list stored in SQLite."""
    if not raw:
        return ()
    try:
        data = json.loads(raw)
    except (json.JSONDecodeError, TypeError, ValueError):
        return ()
    if not isinstance(data, list):
        return ()
    return tuple(str(item) for item in data if item not in (None, ""))


def _row_to_market_descriptor(row: sqlite3.Row) -> MarketDescriptor:
    """Convert a query row into the runtime market descriptor."""
    return MarketDescriptor(
        market_id=str(row["market_id"]),
        event_id=str(row["event_id"]),
        event_slug=str(row["event_slug"] or ""),
        event_title=str(row["event_title"] or ""),
        market_slug=str(row["market_slug"] or ""),
        question=str(row["question"] or ""),
        condition_id=str(row["condition_id"] or ""),
        yes_token_id=str(row["yes_token_id"] or ""),
        no_token_id=str(row["no_token_id"] or ""),
        tier=int(row["tier"] or 3),
        market_volume=float(row["market_volume"] or 0),
        market_liquidity=float(row["market_liquidity"] or 0),
    )


def _build_text_context(row: sqlite3.Row) -> str:
    """Build a lowercase text blob for keyword matching."""
    parts = [
        row["event_title"],
        row["event_description"],
        row["event_slug"],
        row["market_slug"],
        row["question"],
        row["market_description"],
    ]
    parts.extend(_decode_string_list(row["event_tags"]))
    return " ".join(str(part or "").lower() for part in parts if part)


def _is_manual_override(market: MarketDescriptor, ruleset: RuleSet) -> bool:
    """Check whether manual include slugs force this market into scope."""
    return (
        market.event_slug.lower() in ruleset.event_slugs
        or market.market_slug.lower() in ruleset.market_slugs
    )


def _is_hard_excluded(
    market: MarketDescriptor,
    text_context: str,
    tag_ids: tuple[str, ...],
    ruleset: RuleSet,
) -> bool:
    """Check whether exclusion rules remove this market from approval."""
    if market.event_slug.lower() in ruleset.event_slugs:
        return True
    if market.market_slug.lower() in ruleset.market_slugs:
        return True
    if ruleset.tag_ids and any(tag_id in ruleset.tag_ids for tag_id in tag_ids):
        return True
    return any(keyword in text_context for keyword in ruleset.keywords)


def _include_reasons(
    market: MarketDescriptor,
    text_context: str,
    tag_ids: tuple[str, ...],
    ruleset: RuleSet,
) -> list[str]:
    """Explain why a market belongs in the approved universe."""
    reasons: list[str] = []
    if ruleset.tag_ids:
        matched_tags = sorted(tag_id for tag_id in tag_ids if tag_id in ruleset.tag_ids)
        if matched_tags:
            reasons.append(f"tag_ids={','.join(matched_tags)}")
    matched_keywords = sorted(keyword for keyword in ruleset.keywords if keyword in text_context)
    if matched_keywords:
        reasons.append(f"keywords={','.join(matched_keywords[:5])}")
    if market.event_slug.lower() in ruleset.event_slugs:
        reasons.append("manual_event_slug")
    if market.market_slug.lower() in ruleset.market_slugs:
        reasons.append("manual_market_slug")
    return reasons


def _meets_liquidity_thresholds(row: sqlite3.Row, ruleset: RuleSet) -> bool:
    """Check market and event liquidity thresholds required for approval."""
    event_liquidity = float(row["event_liquidity"] or 0)
    market_liquidity = float(row["market_liquidity"] or 0)
    return (
        event_liquidity >= ruleset.min_event_liquidity
        and market_liquidity >= ruleset.min_market_liquidity
    )


def _build_review_candidate(
    row: sqlite3.Row,
    text_context: str,
    tag_ids: tuple[str, ...],
    policy: UniversePolicy,
) -> ReviewCandidate | None:
    """Create a candidate-review event for excluded but interesting events."""
    matched_keywords = tuple(
        keyword for keyword in policy.candidate_review.keywords if keyword in text_context
    )
    event_liquidity = float(row["event_liquidity"] or 0)

    if not matched_keywords and event_liquidity < policy.candidate_review.min_event_liquidity:
        return None

    matched_tag_ids = tuple(tag_id for tag_id in tag_ids if tag_id in policy.include.tag_ids)
    reasons: list[str] = []
    if matched_keywords:
        reasons.append("secondary_keyword_match")
    if event_liquidity >= policy.candidate_review.min_event_liquidity:
        reasons.append("high_event_liquidity")
    if matched_tag_ids:
        reasons.append("include_tag_but_below_threshold")

    return ReviewCandidate(
        event_id=str(row["event_id"]),
        event_slug=str(row["event_slug"] or ""),
        event_title=str(row["event_title"] or ""),
        event_liquidity=event_liquidity,
        event_volume=float(row["event_volume"] or 0),
        matched_keywords=matched_keywords,
        matched_tag_ids=matched_tag_ids,
        reason=",".join(reasons),
    )
