from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from functools import lru_cache
from typing import Any

from database.db_manager import get_conn
from phase5.models import EvaluationRow


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


def _iso(value: datetime) -> str:
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).isoformat()


def _parse_json(value: str | None, default: Any) -> Any:
    if not value:
        return default
    try:
        return json.loads(value)
    except (json.JSONDecodeError, TypeError, ValueError):
        return default


def _normalize_category(row: Any) -> str:
    raw_category = str(row.get("category") or "").strip().lower()
    if raw_category:
        return raw_category

    tags = _parse_json(row.get("tags"), [])
    if isinstance(tags, list):
        for tag in tags:
            text = str(tag).strip().lower()
            if text:
                return text
    return "unknown"


@dataclass(slots=True)
class SnapshotPoint:
    captured_at: str
    yes_price: float | None
    no_price: float | None
    best_bid: float | None
    best_ask: float | None
    spread: float | None


class Phase5Repository:
    def load_evaluation_rows(self, *, start: str, end: str) -> list[EvaluationRow]:
        conn = get_conn()
        try:
            rows = conn.execute(
                """
                SELECT
                    sc.candidate_id,
                    sc.market_id,
                    sc.event_id,
                    sc.event_family_id,
                    sc.trigger_time,
                    sc.detector_version,
                    sc.feature_schema_version,
                    sc.triggering_rules,
                    sc.feature_snapshot,
                    sc.severity_score,
                    m.end_date AS market_end_date,
                    m.status AS market_status,
                    e.category,
                    e.tags,
                    a.alert_id,
                    a.created_at AS alert_created_at,
                    a.workflow_version,
                    a.severity AS alert_severity,
                    a.alert_status
                FROM signal_candidates sc
                LEFT JOIN markets m ON m.market_id = sc.market_id
                LEFT JOIN events e ON e.event_id = sc.event_id
                LEFT JOIN alerts a ON a.candidate_id = sc.candidate_id
                WHERE sc.trigger_time >= ?
                  AND sc.trigger_time < ?
                ORDER BY sc.trigger_time ASC, sc.candidate_id ASC
                """,
                (start, end),
            ).fetchall()
        finally:
            conn.close()

        evidence_map = self._load_alert_evidence_snapshot_map()
        resolution_map = self._load_market_resolution_map()

        evaluation_rows: list[EvaluationRow] = []
        for row in rows:
            candidate_id = str(row["candidate_id"])
            market_id = str(row["market_id"])
            event_family_id = str(
                row["event_family_id"]
                or row["event_id"]
                or row["market_id"]
            )
            feature_snapshot = _parse_json(row["feature_snapshot"], {})
            if not isinstance(feature_snapshot, dict):
                feature_snapshot = {}
            triggering_rules = _parse_json(row["triggering_rules"], [])
            if not isinstance(triggering_rules, list):
                triggering_rules = []

            coverage_notes: list[str] = []
            coverage_status = "complete"
            if row["alert_id"] is None:
                coverage_status = "partial"
                coverage_notes.append("missing_alert")
            if resolution_map.get(market_id) is None:
                coverage_status = "partial"
                coverage_notes.append("missing_resolution")
            if row["market_end_date"] is None:
                coverage_notes.append("missing_market_end_date")
                if coverage_status == "complete":
                    coverage_status = "partial"

            evidence_state = None
            if row["alert_id"] is not None:
                evidence_state = evidence_map.get(candidate_id)
                if evidence_state is None:
                    coverage_notes.append("missing_evidence_snapshot_at_alert")
                    if coverage_status == "complete":
                        coverage_status = "partial"

            resolution = resolution_map.get(market_id)
            alert_created_at = row["alert_created_at"]
            decision_timestamp = alert_created_at or row["trigger_time"]
            if decision_timestamp is None:
                coverage_status = "coverage_insufficient"
                coverage_notes.append("missing_decision_timestamp")
                continue

            evaluation_rows.append(
                EvaluationRow(
                    evaluation_row_id=f"candidate:{candidate_id}",
                    candidate_id=candidate_id,
                    alert_id=row["alert_id"],
                    market_id=market_id,
                    event_id=row["event_id"],
                    event_family_id=event_family_id,
                    category_key=_normalize_category(row),
                    candidate_trigger_time=row["trigger_time"],
                    alert_created_at=alert_created_at,
                    decision_timestamp=decision_timestamp,
                    detector_version=row["detector_version"],
                    feature_schema_version=row["feature_schema_version"],
                    workflow_version=row["workflow_version"],
                    candidate_severity_score=float(row["severity_score"] or 0.0),
                    alert_severity=row["alert_severity"],
                    alert_status=row["alert_status"],
                    evidence_state_at_alert=evidence_state,
                    triggering_rules=[str(item) for item in triggering_rules],
                    feature_snapshot=feature_snapshot,
                    resolution_outcome=(resolution or {}).get("outcome"),
                    resolution_time=(resolution or {}).get("resolved_at"),
                    market_end_date=row["market_end_date"],
                    market_status=row["market_status"],
                    coverage_status=coverage_status,
                    coverage_notes=coverage_notes,
                )
            )

        return evaluation_rows

    def _load_alert_evidence_snapshot_map(self) -> dict[str, str]:
        conn = get_conn()
        try:
            rows = conn.execute(
                """
                SELECT
                    a.candidate_id,
                    (
                        SELECT es.evidence_state
                        FROM evidence_snapshots es
                        WHERE es.candidate_id = a.candidate_id
                          AND es.snapshot_time <= a.created_at
                        ORDER BY es.snapshot_time DESC, es.created_at DESC
                        LIMIT 1
                    ) AS evidence_state_at_alert
                FROM alerts a
                """
            ).fetchall()
        finally:
            conn.close()
        return {
            str(row["candidate_id"]): str(row["evidence_state_at_alert"])
            for row in rows
            if row["candidate_id"] is not None and row["evidence_state_at_alert"] is not None
        }

    def _load_market_resolution_map(self) -> dict[str, dict[str, str]]:
        conn = get_conn()
        try:
            rows = conn.execute(
                """
                SELECT market_id, outcome, resolved_at
                FROM market_resolutions
                ORDER BY resolved_at DESC
                """
            ).fetchall()
        finally:
            conn.close()

        resolutions: dict[str, dict[str, str]] = {}
        for row in rows:
            market_id = str(row["market_id"])
            if market_id in resolutions:
                continue
            resolutions[market_id] = {
                "outcome": row["outcome"],
                "resolved_at": row["resolved_at"],
            }
        return resolutions

    @lru_cache(maxsize=512)
    def load_snapshot_series(
        self,
        market_id: str,
        start: str,
        end: str,
    ) -> tuple[SnapshotPoint, ...]:
        conn = get_conn()
        try:
            rows = conn.execute(
                """
                SELECT captured_at, yes_price, no_price, best_bid, best_ask, spread
                FROM snapshots
                WHERE market_id = ?
                  AND captured_at >= ?
                  AND captured_at <= ?
                ORDER BY captured_at ASC
                """,
                (market_id, start, end),
            ).fetchall()
        finally:
            conn.close()

        return tuple(
            SnapshotPoint(
                captured_at=str(row["captured_at"]),
                yes_price=float(row["yes_price"]) if row["yes_price"] is not None else None,
                no_price=float(row["no_price"]) if row["no_price"] is not None else None,
                best_bid=float(row["best_bid"]) if row["best_bid"] is not None else None,
                best_ask=float(row["best_ask"]) if row["best_ask"] is not None else None,
                spread=float(row["spread"]) if row["spread"] is not None else None,
            )
            for row in rows
        )

    def compute_window_bounds(self, rows: list[EvaluationRow]) -> tuple[str | None, str | None]:
        if not rows:
            return None, None
        timestamps = [_parse_iso(row.decision_timestamp) for row in rows]
        parsed = [item for item in timestamps if item is not None]
        if not parsed:
            return None, None
        return _iso(min(parsed)), _iso(max(parsed))

