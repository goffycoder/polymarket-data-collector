from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any

from config.settings import (
    PHASE5_METRICS_VERSION,
    PHASE5_REPORT_VERSION,
    PHASE5_SIMULATOR_VERSION,
)
from phase5.metrics import build_phase5_metrics
from phase5.models import EvaluationRow, PaperTradeResult
from phase5.repository import Phase5Repository
from phase5.simulator import ConservativePaperTrader


@dataclass(slots=True)
class Phase5Person2Report:
    start: str
    end: str
    report_version: str
    simulator_version: str
    metrics_version: str
    evaluation_row_count: int
    alert_row_count: int
    paper_trade_count: int
    coverage_summary: dict[str, Any]
    metrics: dict[str, Any]
    strongest_windows: list[dict[str, Any]]
    weakest_windows: list[dict[str, Any]]
    assessment: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def _coverage_summary(rows: list[EvaluationRow], trades: list[PaperTradeResult]) -> dict[str, Any]:
    return {
        "total_rows": len(rows),
        "alert_rows": sum(1 for row in rows if row.alert_id is not None),
        "rows_complete": sum(1 for row in rows if row.coverage_status == "complete"),
        "rows_partial": sum(1 for row in rows if row.coverage_status == "partial"),
        "rows_coverage_insufficient": sum(1 for row in rows if row.coverage_status == "coverage_insufficient"),
        "paper_trade_skips": sum(1 for trade in trades if trade.status == "skipped"),
        "paper_trade_fills": sum(1 for trade in trades if trade.status in {"filled", "resolved"}),
    }


def _window_candidates(rows: list[EvaluationRow], *, limit: int, reverse: bool) -> list[dict[str, Any]]:
    counts: dict[str, dict[str, Any]] = {}
    for row in rows:
        key = row.category_key
        bucket = counts.setdefault(
            key,
            {
                "window_key": key,
                "row_count": 0,
                "alert_count": 0,
                "coverage_insufficient_count": 0,
            },
        )
        bucket["row_count"] += 1
        if row.alert_id is not None:
            bucket["alert_count"] += 1
        if row.coverage_status == "coverage_insufficient":
            bucket["coverage_insufficient_count"] += 1
    ordered = sorted(
        counts.values(),
        key=lambda item: (
            item["coverage_insufficient_count"],
            item["row_count"],
            item["window_key"],
        ),
        reverse=reverse,
    )
    return ordered[:limit]


def _assessment(report_metrics: dict[str, Any], coverage: dict[str, Any]) -> dict[str, Any]:
    trade_metrics = report_metrics["paper_trade_overall"]
    alert_metrics = report_metrics["alert_overall"]
    failure_metrics = report_metrics["failure_overall"]

    if coverage["total_rows"] == 0:
        status = "no_rows_in_window"
    elif coverage["rows_coverage_insufficient"] > 0 and coverage["paper_trade_fills"] == 0:
        status = "coverage_limited"
    elif (
        trade_metrics["median_bounded_pnl"] > 0
        and alert_metrics["alert_usefulness_precision"] >= 0.5
        and failure_metrics["top_category_dependency_share"] < 0.75
        and failure_metrics["top_event_family_dependency_share"] < 0.75
    ):
        status = "promising"
    elif trade_metrics["median_bounded_pnl"] > 0 or alert_metrics["alert_usefulness_precision"] >= 0.5:
        status = "mixed"
    else:
        status = "not_yet_defendable"

    return {
        "status": status,
        "median_bounded_pnl": trade_metrics["median_bounded_pnl"],
        "alert_usefulness_precision": alert_metrics["alert_usefulness_precision"],
        "top_category_dependency_share": failure_metrics["top_category_dependency_share"],
        "top_event_family_dependency_share": failure_metrics["top_event_family_dependency_share"],
    }


def build_phase5_person2_report(*, start: str, end: str) -> Phase5Person2Report:
    repository = Phase5Repository()
    rows = repository.load_evaluation_rows(start=start, end=end)
    trader = ConservativePaperTrader(repository=repository)
    trades = trader.simulate(rows)
    metrics = build_phase5_metrics(rows=rows, trades=trades)
    coverage = _coverage_summary(rows, trades)

    return Phase5Person2Report(
        start=start,
        end=end,
        report_version=PHASE5_REPORT_VERSION,
        simulator_version=PHASE5_SIMULATOR_VERSION,
        metrics_version=PHASE5_METRICS_VERSION,
        evaluation_row_count=len(rows),
        alert_row_count=sum(1 for row in rows if row.alert_id is not None),
        paper_trade_count=len(trades),
        coverage_summary=coverage,
        metrics=metrics,
        strongest_windows=_window_candidates(rows, limit=3, reverse=False),
        weakest_windows=_window_candidates(rows, limit=3, reverse=True),
        assessment=_assessment(metrics, coverage),
    )

