from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timedelta, timezone

from config.settings import (
    PHASE5_EVENT_FAMILY_FOLDS,
    PHASE5_MIN_ALERT_ROWS,
    PHASE5_MIN_CANDIDATE_ROWS,
    PHASE5_MIN_COVERAGE_RATIO,
    PHASE5_MIN_DISTINCT_CATEGORIES,
    PHASE5_MIN_DISTINCT_EVENT_FAMILIES,
    PHASE5_MIN_PAPER_TRADE_ROWS,
    PHASE5_MIN_RESOLVED_ROWS,
    PHASE5_TIME_BLOCK_DAYS,
)
from phase5.models import EvaluationRow, PaperTradeResult, SplitSummary


_ANCHOR = datetime(1970, 1, 1, tzinfo=timezone.utc)


def _parse_iso(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def event_family_fold_key(row: EvaluationRow) -> str:
    normalized = row.event_family_id or row.event_id or row.market_id
    fold_count = max(1, PHASE5_EVENT_FAMILY_FOLDS)
    fold = sorted({normalized}).index(normalized) % fold_count
    return f"fold_{fold + 1}_of_{fold_count}:{normalized}"


def build_event_family_fold_map(rows: list[EvaluationRow]) -> dict[str, str]:
    fold_count = max(1, PHASE5_EVENT_FAMILY_FOLDS)
    keys = sorted({row.event_family_id or row.event_id or row.market_id for row in rows})
    return {
        key: f"fold_{(idx % fold_count) + 1}_of_{fold_count}"
        for idx, key in enumerate(keys)
    }


def category_holdout_key(row: EvaluationRow) -> str:
    return row.category_key or "unknown"


def time_holdout_key(decision_timestamp: str) -> str:
    parsed = _parse_iso(decision_timestamp)
    if parsed is None:
        return "time_block:unknown"
    block_days = max(1, PHASE5_TIME_BLOCK_DAYS)
    delta = parsed - _ANCHOR
    block_index = int(delta.total_seconds() // timedelta(days=block_days).total_seconds())
    block_start = _ANCHOR + timedelta(days=block_days * block_index)
    block_end = block_start + timedelta(days=block_days)
    return f"time_block:{block_start.date().isoformat()}:{block_end.date().isoformat()}"


def classify_split_status(
    *,
    unit_type: str,
    total_rows: int,
    eligible_rows: int,
    resolved_rows: int,
    distinct_event_families: int,
    distinct_categories: int,
    coverage_ratio: float,
) -> str:
    if total_rows == 0 or coverage_ratio < PHASE5_MIN_COVERAGE_RATIO:
        return "coverage_insufficient"

    if unit_type == "candidate":
        if eligible_rows < PHASE5_MIN_CANDIDATE_ROWS:
            return "descriptive_only"
    elif unit_type == "alert":
        if eligible_rows < PHASE5_MIN_ALERT_ROWS:
            return "descriptive_only"
    elif unit_type == "paper_trade":
        if eligible_rows < PHASE5_MIN_PAPER_TRADE_ROWS:
            return "descriptive_only"

    if resolved_rows < PHASE5_MIN_RESOLVED_ROWS:
        return "descriptive_only"
    if distinct_event_families < PHASE5_MIN_DISTINCT_EVENT_FAMILIES:
        return "descriptive_only"
    if distinct_categories < PHASE5_MIN_DISTINCT_CATEGORIES:
        return "descriptive_only"
    return "scored"


def build_split_summaries_for_rows(
    *,
    unit_type: str,
    rows: list[EvaluationRow],
    regime_name: str,
    key_builder,
    metrics_builder,
) -> list[SplitSummary]:
    groups: dict[str, list[EvaluationRow]] = defaultdict(list)
    for row in rows:
        groups[key_builder(row)].append(row)

    summaries: list[SplitSummary] = []
    for split_key, group_rows in sorted(groups.items(), key=lambda item: item[0]):
        total_rows = len(group_rows)
        eligible_rows = len(group_rows)
        resolved_rows = sum(1 for row in group_rows if row.resolution_outcome and row.resolution_time)
        distinct_event_families = len({row.event_family_id for row in group_rows})
        distinct_categories = len({row.category_key for row in group_rows})
        coverage_good = sum(1 for row in group_rows if row.coverage_status != "coverage_insufficient")
        coverage_ratio = float(coverage_good) / float(total_rows) if total_rows else 0.0
        status = classify_split_status(
            unit_type=unit_type,
            total_rows=total_rows,
            eligible_rows=eligible_rows,
            resolved_rows=resolved_rows,
            distinct_event_families=distinct_event_families,
            distinct_categories=distinct_categories,
            coverage_ratio=coverage_ratio,
        )
        summaries.append(
            SplitSummary(
                split_key=f"{regime_name}:{split_key}",
                total_rows=total_rows,
                eligible_rows=eligible_rows,
                resolved_rows=resolved_rows,
                distinct_event_families=distinct_event_families,
                distinct_categories=distinct_categories,
                coverage_ratio=round(coverage_ratio, 6),
                status=status,
                metrics=metrics_builder(group_rows, status),
            )
        )
    return summaries


def build_split_summaries_for_trades(
    *,
    rows_by_candidate: dict[str, EvaluationRow],
    trades: list[PaperTradeResult],
    regime_name: str,
    key_builder,
    metrics_builder,
) -> list[SplitSummary]:
    groups: dict[str, list[tuple[EvaluationRow, PaperTradeResult]]] = defaultdict(list)
    for trade in trades:
        row = rows_by_candidate.get(trade.candidate_id)
        if row is None:
            continue
        groups[key_builder(row)].append((row, trade))

    summaries: list[SplitSummary] = []
    for split_key, group_items in sorted(groups.items(), key=lambda item: item[0]):
        group_rows = [item[0] for item in group_items]
        group_trades = [item[1] for item in group_items]
        total_rows = len(group_trades)
        eligible_rows = sum(1 for trade in group_trades if trade.status in {"filled", "resolved"})
        resolved_rows = sum(1 for row in group_rows if row.resolution_outcome and row.resolution_time)
        distinct_event_families = len({row.event_family_id for row in group_rows})
        distinct_categories = len({row.category_key for row in group_rows})
        coverage_good = sum(1 for row in group_rows if row.coverage_status != "coverage_insufficient")
        coverage_ratio = float(coverage_good) / float(total_rows) if total_rows else 0.0
        status = classify_split_status(
            unit_type="paper_trade",
            total_rows=total_rows,
            eligible_rows=eligible_rows,
            resolved_rows=resolved_rows,
            distinct_event_families=distinct_event_families,
            distinct_categories=distinct_categories,
            coverage_ratio=coverage_ratio,
        )
        summaries.append(
            SplitSummary(
                split_key=f"{regime_name}:{split_key}",
                total_rows=total_rows,
                eligible_rows=eligible_rows,
                resolved_rows=resolved_rows,
                distinct_event_families=distinct_event_families,
                distinct_categories=distinct_categories,
                coverage_ratio=round(coverage_ratio, 6),
                status=status,
                metrics=metrics_builder(group_trades, status),
            )
        )
    return summaries

