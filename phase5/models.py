from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any


@dataclass(slots=True)
class EvaluationRow:
    evaluation_row_id: str
    candidate_id: str
    alert_id: str | None
    market_id: str
    event_id: str | None
    event_family_id: str
    category_key: str
    candidate_trigger_time: str
    alert_created_at: str | None
    decision_timestamp: str
    detector_version: str | None
    feature_schema_version: str | None
    workflow_version: str | None
    candidate_severity_score: float
    alert_severity: str | None
    alert_status: str | None
    evidence_state_at_alert: str | None
    triggering_rules: list[str] = field(default_factory=list)
    feature_snapshot: dict[str, Any] = field(default_factory=dict)
    resolution_outcome: str | None = None
    resolution_time: str | None = None
    market_end_date: str | None = None
    market_status: str | None = None
    coverage_status: str = "complete"
    coverage_notes: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class PaperTradeResult:
    paper_trade_id: str
    alert_id: str | None
    candidate_id: str
    market_id: str
    event_family_id: str
    decision_timestamp: str
    direction: str | None
    status: str
    skip_reason: str | None
    entry_time: str | None = None
    exit_time: str | None = None
    entry_price: float | None = None
    exit_price: float | None = None
    fee_paid: float = 0.0
    pnl_bounded: float | None = None
    holding_seconds: float | None = None
    notes: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class SplitSummary:
    split_key: str
    total_rows: int
    eligible_rows: int
    resolved_rows: int
    distinct_event_families: int
    distinct_categories: int
    coverage_ratio: float
    status: str
    metrics: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

