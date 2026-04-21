from __future__ import annotations

from collections import Counter
from statistics import median
from typing import Any

from config.settings import (
    PHASE5_METRICS_VERSION,
    PHASE5_NEAR_RESOLUTION_MINUTES,
)
from phase5.holdouts import (
    build_event_family_fold_map,
    build_split_summaries_for_rows,
    build_split_summaries_for_trades,
    category_holdout_key,
    time_holdout_key,
)
from phase5.models import EvaluationRow, PaperTradeResult, SplitSummary
from phase5.simulator import infer_direction


SEVERITY_RANK = {
    "INFO": 1,
    "WATCH": 2,
    "ACTIONABLE": 3,
    "CRITICAL": 4,
}


def _safe_ratio(numerator: int | float, denominator: int | float) -> float:
    if not denominator:
        return 0.0
    return float(numerator) / float(denominator)


def _directional_label_success(row: EvaluationRow, *, decision_layer: str) -> bool | None:
    if decision_layer == "alert" and row.alert_id is None:
        return None
    direction = infer_direction(row)
    if direction is None or not row.resolution_outcome:
        return None
    return row.resolution_outcome == direction


def _lead_time_seconds(row: EvaluationRow, *, decision_layer: str) -> float | None:
    from phase5.repository import _parse_iso  # local import to avoid cycle

    resolution_time = _parse_iso(row.resolution_time)
    if resolution_time is None:
        return None
    if decision_layer == "alert":
        decision_time = _parse_iso(row.alert_created_at)
    else:
        decision_time = _parse_iso(row.candidate_trigger_time)
    if decision_time is None or resolution_time <= decision_time:
        return None
    return (resolution_time - decision_time).total_seconds()


def _candidate_metrics(rows: list[EvaluationRow], status: str) -> dict[str, Any]:
    labeled = [_directional_label_success(row, decision_layer="candidate") for row in rows]
    labeled_valid = [item for item in labeled if item is not None]
    successes = sum(1 for item in labeled_valid if item)
    severity_scores = [float(row.candidate_severity_score or 0.0) for row in rows]
    rule_counter: Counter[str] = Counter()
    for row in rows:
        rule_counter.update(str(rule) for rule in row.triggering_rules)
    return {
        "status": status,
        "metrics_version": PHASE5_METRICS_VERSION,
        "candidate_count": len(rows),
        "distinct_markets": len({row.market_id for row in rows}),
        "distinct_event_families": len({row.event_family_id for row in rows}),
        "label_coverage": round(_safe_ratio(len(labeled_valid), len(rows)), 6),
        "candidate_precision": round(_safe_ratio(successes, len(labeled_valid)), 6),
        "candidate_false_positive_rate": round(_safe_ratio(len(labeled_valid) - successes, len(labeled_valid)), 6),
        "candidate_resolution_reach_rate": round(
            _safe_ratio(sum(1 for row in rows if row.resolution_outcome), len(rows)),
            6,
        ),
        "median_candidate_severity_score": round(median(severity_scores), 6) if severity_scores else 0.0,
        "rule_family_counts": [
            {"rule_family": key, "count": count}
            for key, count in sorted(rule_counter.items(), key=lambda item: (-item[1], item[0]))
        ],
    }


def _alert_metrics(rows: list[EvaluationRow], status: str) -> dict[str, Any]:
    alert_rows = [row for row in rows if row.alert_id is not None]
    labeled = [_directional_label_success(row, decision_layer="alert") for row in alert_rows]
    labeled_valid = [item for item in labeled if item is not None]
    successes = sum(1 for item in labeled_valid if item)
    severity_counter: Counter[str] = Counter(str(row.alert_severity or "UNKNOWN") for row in alert_rows)
    evidence_counter: Counter[str] = Counter(str(row.evidence_state_at_alert or "missing") for row in alert_rows)
    return {
        "status": status,
        "metrics_version": PHASE5_METRICS_VERSION,
        "alert_count": len(alert_rows),
        "alert_creation_rate_proxy": len(alert_rows),
        "alert_label_coverage": round(_safe_ratio(len(labeled_valid), len(alert_rows)), 6),
        "alert_usefulness_precision": round(_safe_ratio(successes, len(labeled_valid)), 6),
        "alert_non_useful_rate": round(_safe_ratio(len(labeled_valid) - successes, len(labeled_valid)), 6),
        "alert_suppression_rate_proxy": round(_safe_ratio(len(rows) - len(alert_rows), len(rows)), 6),
        "severity_counts": [
            {"severity": key, "count": count}
            for key, count in sorted(severity_counter.items(), key=lambda item: (-SEVERITY_RANK.get(item[0], 0), item[0]))
        ],
        "evidence_state_counts": [
            {"evidence_state": key, "count": count}
            for key, count in sorted(evidence_counter.items(), key=lambda item: (-item[1], item[0]))
        ],
    }


def _lead_time_metrics(rows: list[EvaluationRow], status: str) -> dict[str, Any]:
    successful_alert_rows = [
        row
        for row in rows
        if _directional_label_success(row, decision_layer="alert")
    ]
    lead_times = [
        _lead_time_seconds(row, decision_layer="alert")
        for row in successful_alert_rows
    ]
    lead_times_valid = [value for value in lead_times if value is not None]
    short_lead_share = _safe_ratio(
        sum(1 for value in lead_times_valid if value <= (PHASE5_NEAR_RESOLUTION_MINUTES * 60.0)),
        len(lead_times_valid),
    )
    return {
        "status": status,
        "metrics_version": PHASE5_METRICS_VERSION,
        "successful_alert_count": len(lead_times_valid),
        "median_lead_time_seconds": round(median(lead_times_valid), 6) if lead_times_valid else 0.0,
        "p25_lead_time_seconds": round(sorted(lead_times_valid)[max(0, int(len(lead_times_valid) * 0.25) - 1)], 6)
        if lead_times_valid
        else 0.0,
        "p75_lead_time_seconds": round(sorted(lead_times_valid)[max(0, int(len(lead_times_valid) * 0.75) - 1)], 6)
        if lead_times_valid
        else 0.0,
        "short_lead_alert_share": round(short_lead_share, 6),
    }


def _paper_trade_metrics(trades: list[PaperTradeResult], status: str) -> dict[str, Any]:
    filled = [trade for trade in trades if trade.status in {"filled", "resolved"} and trade.pnl_bounded is not None]
    pnls = [float(trade.pnl_bounded or 0.0) for trade in filled]
    skip_counter: Counter[str] = Counter(trade.skip_reason or "none" for trade in trades if trade.status == "skipped")
    return {
        "status": status,
        "metrics_version": PHASE5_METRICS_VERSION,
        "paper_trade_count": len(trades),
        "fill_rate": round(_safe_ratio(len(filled), len(trades)), 6),
        "trade_eligibility_rate_proxy": round(
            _safe_ratio(sum(1 for trade in trades if trade.skip_reason not in {"skipped_no_directional_mapping", "invalid_replay_coverage"}), len(trades)),
            6,
        ),
        "median_bounded_pnl": round(median(pnls), 6) if pnls else 0.0,
        "mean_bounded_pnl": round(sum(pnls) / len(pnls), 6) if pnls else 0.0,
        "hit_rate": round(_safe_ratio(sum(1 for pnl in pnls if pnl > 0), len(pnls)), 6),
        "loss_rate": round(_safe_ratio(sum(1 for pnl in pnls if pnl <= 0), len(pnls)), 6),
        "worst_decile_pnl": round(sum(sorted(pnls)[: max(1, len(pnls) // 10)]) / max(1, len(pnls) // 10), 6)
        if pnls
        else 0.0,
        "skip_reason_counts": [
            {"skip_reason": key, "count": count}
            for key, count in sorted(skip_counter.items(), key=lambda item: (-item[1], item[0]))
        ],
    }


def _failure_metrics(rows: list[EvaluationRow], trades: list[PaperTradeResult]) -> dict[str, Any]:
    resolved_alerts = [row for row in rows if row.alert_id and row.resolution_outcome]
    grouped_by_category: Counter[str] = Counter(row.category_key for row in resolved_alerts)
    grouped_by_event_family: Counter[str] = Counter(row.event_family_id for row in resolved_alerts)
    skipped_data = sum(
        1
        for trade in trades
        if trade.skip_reason in {
            "skipped_insufficient_market_data",
            "skipped_insufficient_execution_quote",
            "skipped_insufficient_depth",
            "invalid_replay_coverage",
        }
    )
    return {
        "metrics_version": PHASE5_METRICS_VERSION,
        "coverage_insufficient_rows": sum(1 for row in rows if row.coverage_status == "coverage_insufficient"),
        "partial_coverage_rows": sum(1 for row in rows if row.coverage_status == "partial"),
        "top_category_dependency_share": round(
            _safe_ratio(max(grouped_by_category.values()) if grouped_by_category else 0, len(resolved_alerts)),
            6,
        ),
        "top_event_family_dependency_share": round(
            _safe_ratio(max(grouped_by_event_family.values()) if grouped_by_event_family else 0, len(resolved_alerts)),
            6,
        ),
        "skip_due_to_data_rate": round(_safe_ratio(skipped_data, len(trades)), 6),
        "near_resolution_skip_rate": round(
            _safe_ratio(sum(1 for trade in trades if trade.skip_reason == "skipped_near_resolution"), len(trades)),
            6,
        ),
    }


def build_phase5_metrics(
    *,
    rows: list[EvaluationRow],
    trades: list[PaperTradeResult],
) -> dict[str, Any]:
    rows_by_candidate = {row.candidate_id: row for row in rows}
    event_family_folds = build_event_family_fold_map(rows)

    candidate_regimes = {
        "event_family_holdout": build_split_summaries_for_rows(
            unit_type="candidate",
            rows=rows,
            regime_name="event_family_holdout",
            key_builder=lambda row: event_family_folds.get(row.event_family_id, "fold_unknown"),
            metrics_builder=_candidate_metrics,
        ),
        "category_holdout": build_split_summaries_for_rows(
            unit_type="candidate",
            rows=rows,
            regime_name="category_holdout",
            key_builder=category_holdout_key,
            metrics_builder=_candidate_metrics,
        ),
        "time_split_holdout": build_split_summaries_for_rows(
            unit_type="candidate",
            rows=rows,
            regime_name="time_split_holdout",
            key_builder=lambda row: time_holdout_key(row.candidate_trigger_time),
            metrics_builder=_candidate_metrics,
        ),
    }
    alert_rows = [row for row in rows if row.alert_id is not None]
    alert_regimes = {
        "event_family_holdout": build_split_summaries_for_rows(
            unit_type="alert",
            rows=alert_rows,
            regime_name="event_family_holdout",
            key_builder=lambda row: event_family_folds.get(row.event_family_id, "fold_unknown"),
            metrics_builder=_alert_metrics,
        ),
        "category_holdout": build_split_summaries_for_rows(
            unit_type="alert",
            rows=alert_rows,
            regime_name="category_holdout",
            key_builder=category_holdout_key,
            metrics_builder=_alert_metrics,
        ),
        "time_split_holdout": build_split_summaries_for_rows(
            unit_type="alert",
            rows=alert_rows,
            regime_name="time_split_holdout",
            key_builder=lambda row: time_holdout_key(row.alert_created_at or row.candidate_trigger_time),
            metrics_builder=_alert_metrics,
        ),
    }
    lead_time_regimes = {
        "event_family_holdout": build_split_summaries_for_rows(
            unit_type="alert",
            rows=alert_rows,
            regime_name="event_family_holdout",
            key_builder=lambda row: event_family_folds.get(row.event_family_id, "fold_unknown"),
            metrics_builder=_lead_time_metrics,
        ),
        "category_holdout": build_split_summaries_for_rows(
            unit_type="alert",
            rows=alert_rows,
            regime_name="category_holdout",
            key_builder=category_holdout_key,
            metrics_builder=_lead_time_metrics,
        ),
        "time_split_holdout": build_split_summaries_for_rows(
            unit_type="alert",
            rows=alert_rows,
            regime_name="time_split_holdout",
            key_builder=lambda row: time_holdout_key(row.alert_created_at or row.candidate_trigger_time),
            metrics_builder=_lead_time_metrics,
        ),
    }
    trade_regimes = {
        "event_family_holdout": build_split_summaries_for_trades(
            rows_by_candidate=rows_by_candidate,
            trades=trades,
            regime_name="event_family_holdout",
            key_builder=lambda row: event_family_folds.get(row.event_family_id, "fold_unknown"),
            metrics_builder=_paper_trade_metrics,
        ),
        "category_holdout": build_split_summaries_for_trades(
            rows_by_candidate=rows_by_candidate,
            trades=trades,
            regime_name="category_holdout",
            key_builder=category_holdout_key,
            metrics_builder=_paper_trade_metrics,
        ),
        "time_split_holdout": build_split_summaries_for_trades(
            rows_by_candidate=rows_by_candidate,
            trades=trades,
            regime_name="time_split_holdout",
            key_builder=lambda row: time_holdout_key(row.alert_created_at or row.candidate_trigger_time),
            metrics_builder=_paper_trade_metrics,
        ),
    }

    return {
        "metrics_version": PHASE5_METRICS_VERSION,
        "candidate_overall": _candidate_metrics(rows, status="overall"),
        "alert_overall": _alert_metrics(alert_rows, status="overall"),
        "lead_time_overall": _lead_time_metrics(alert_rows, status="overall"),
        "paper_trade_overall": _paper_trade_metrics(trades, status="overall"),
        "failure_overall": _failure_metrics(rows, trades),
        "candidate_regimes": {
            key: [summary.to_dict() for summary in value]
            for key, value in candidate_regimes.items()
        },
        "alert_regimes": {
            key: [summary.to_dict() for summary in value]
            for key, value in alert_regimes.items()
        },
        "lead_time_regimes": {
            key: [summary.to_dict() for summary in value]
            for key, value in lead_time_regimes.items()
        },
        "paper_trade_regimes": {
            key: [summary.to_dict() for summary in value]
            for key, value in trade_regimes.items()
        },
    }

