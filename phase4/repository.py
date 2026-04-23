from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any
from uuid import uuid4

from config.settings import (
    PHASE4_ALERT_CHANNELS,
    PHASE4_ALERT_SCHEMA_VERSION,
    PHASE4_WORKFLOW_VERSION,
    PHASE4_EVIDENCE_SCHEMA_VERSION,
)
from database.db_manager import get_conn


def _iso(dt: datetime) -> str:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).isoformat()


def _parse_iso(value: str | None) -> datetime | None:
    if not value:
        return None
    parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


@dataclass(slots=True)
class Phase4BootstrapSummary:
    workflow_version: str
    pending_candidates: int
    existing_alerts: int
    evidence_queries: int
    evidence_snapshots: int
    analyst_feedback_rows: int

    def to_dict(self) -> dict[str, int | str]:
        return {
            "workflow_version": self.workflow_version,
            "pending_candidates": self.pending_candidates,
            "existing_alerts": self.existing_alerts,
            "evidence_queries": self.evidence_queries,
            "evidence_snapshots": self.evidence_snapshots,
            "analyst_feedback_rows": self.analyst_feedback_rows,
        }


class Phase4Repository:
    def register_workflow_version(self, *, notes: str) -> None:
        conn = get_conn()
        now = _iso(datetime.now(timezone.utc))
        try:
            conn.execute(
                """
                INSERT INTO alert_workflow_versions (
                    workflow_version,
                    evidence_schema_version,
                    alert_schema_version,
                    delivery_channels,
                    notes,
                    created_at,
                    last_used_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(workflow_version) DO UPDATE SET
                    evidence_schema_version = excluded.evidence_schema_version,
                    alert_schema_version = excluded.alert_schema_version,
                    delivery_channels = excluded.delivery_channels,
                    notes = excluded.notes,
                    last_used_at = excluded.last_used_at
                """,
                (
                    PHASE4_WORKFLOW_VERSION,
                    PHASE4_EVIDENCE_SCHEMA_VERSION,
                    PHASE4_ALERT_SCHEMA_VERSION,
                    json.dumps(list(PHASE4_ALERT_CHANNELS)),
                    notes,
                    now,
                    now,
                ),
            )
            conn.commit()
        finally:
            conn.close()

    def bootstrap_summary(self) -> Phase4BootstrapSummary:
        conn = get_conn()
        try:
            pending_candidates = conn.execute(
                """
                SELECT COUNT(*)
                FROM signal_candidates sc
                LEFT JOIN alerts a ON a.candidate_id = sc.candidate_id
                WHERE a.alert_id IS NULL
                """
            ).fetchone()[0]
            existing_alerts = conn.execute("SELECT COUNT(*) FROM alerts").fetchone()[0]
            evidence_queries = conn.execute("SELECT COUNT(*) FROM evidence_queries").fetchone()[0]
            evidence_snapshots = conn.execute("SELECT COUNT(*) FROM evidence_snapshots").fetchone()[0]
            analyst_feedback_rows = conn.execute("SELECT COUNT(*) FROM analyst_feedback").fetchone()[0]
        finally:
            conn.close()

        return Phase4BootstrapSummary(
            workflow_version=PHASE4_WORKFLOW_VERSION,
            pending_candidates=int(pending_candidates or 0),
            existing_alerts=int(existing_alerts or 0),
            evidence_queries=int(evidence_queries or 0),
            evidence_snapshots=int(evidence_snapshots or 0),
            analyst_feedback_rows=int(analyst_feedback_rows or 0),
        )

    def pending_candidates(
        self,
        *,
        limit: int = 10,
        include_existing_alerts: bool = False,
    ) -> list[dict[str, str | float | None]]:
        conn = get_conn()
        try:
            where_clause = ""
            if not include_existing_alerts:
                where_clause = "WHERE a.alert_id IS NULL"
            rows = conn.execute(
                f"""
                SELECT
                    sc.candidate_id,
                    sc.market_id,
                    sc.event_id,
                    sc.event_family_id,
                    sc.trigger_time,
                    sc.detector_version,
                    sc.feature_schema_version,
                    sc.severity_score,
                    sc.triggering_rules,
                    sc.feature_snapshot,
                    m.question,
                    e.title AS event_title,
                    e.slug AS event_slug
                FROM signal_candidates sc
                LEFT JOIN markets m ON m.market_id = sc.market_id
                LEFT JOIN events e ON e.event_id = sc.event_id
                LEFT JOIN alerts a ON a.candidate_id = sc.candidate_id
                {where_clause}
                ORDER BY sc.trigger_time DESC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
        finally:
            conn.close()

        return [
            {
                "candidate_id": row["candidate_id"],
                "market_id": row["market_id"],
                "event_id": row["event_id"],
                "event_family_id": row["event_family_id"],
                "trigger_time": row["trigger_time"],
                "detector_version": row["detector_version"],
                "feature_schema_version": row["feature_schema_version"],
                "severity_score": row["severity_score"],
                "triggering_rules": json.loads(row["triggering_rules"] or "[]"),
                "feature_snapshot": json.loads(row["feature_snapshot"] or "{}"),
                "question": row["question"],
                "event_title": row["event_title"],
                "event_slug": row["event_slug"],
            }
            for row in rows
        ]

    def record_evidence_query(
        self,
        *,
        candidate_id: str,
        provider_name: str,
        provider_query_type: str,
        provider_query_text: str,
        request_started_at: str,
        response_completed_at: str | None,
        latency_ms: float | None,
        result_count: int,
        query_status: str,
        timeout_seconds: int | None,
        raw_response_metadata: dict[str, Any] | None,
        error_message: str | None = None,
        alert_id: str | None = None,
    ) -> str:
        evidence_query_id = uuid4().hex
        conn = get_conn()
        try:
            conn.execute(
                """
                INSERT INTO evidence_queries (
                    evidence_query_id,
                    candidate_id,
                    alert_id,
                    provider_name,
                    provider_query_type,
                    provider_query_text,
                    request_started_at,
                    response_completed_at,
                    latency_ms,
                    result_count,
                    query_status,
                    timeout_seconds,
                    raw_response_metadata,
                    error_message
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    evidence_query_id,
                    candidate_id,
                    alert_id,
                    provider_name,
                    provider_query_type,
                    provider_query_text,
                    request_started_at,
                    response_completed_at,
                    latency_ms,
                    result_count,
                    query_status,
                    timeout_seconds,
                    json.dumps(raw_response_metadata or {}, sort_keys=True),
                    error_message,
                ),
            )
            conn.commit()
        finally:
            conn.close()
        return evidence_query_id

    def record_evidence_snapshot(
        self,
        *,
        candidate_id: str,
        snapshot_time: str,
        evidence_state: str,
        provider_summary: dict[str, Any],
        confidence_modifier: float | None,
        metadata_json: dict[str, Any] | None,
        alert_id: str | None = None,
        cache_key: str | None = None,
        freshness_seconds: int | None = None,
    ) -> str:
        evidence_snapshot_id = uuid4().hex
        conn = get_conn()
        try:
            conn.execute(
                """
                INSERT INTO evidence_snapshots (
                    evidence_snapshot_id,
                    candidate_id,
                    alert_id,
                    snapshot_time,
                    evidence_state,
                    provider_summary,
                    confidence_modifier,
                    cache_key,
                    freshness_seconds,
                    metadata_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    evidence_snapshot_id,
                    candidate_id,
                    alert_id,
                    snapshot_time,
                    evidence_state,
                    json.dumps(provider_summary, sort_keys=True),
                    confidence_modifier,
                    cache_key,
                    freshness_seconds,
                    json.dumps(metadata_json or {}, sort_keys=True),
                ),
            )
            conn.commit()
        finally:
            conn.close()
        return evidence_snapshot_id

    def latest_evidence_query_for_cache(
        self,
        *,
        provider_name: str,
        provider_query_type: str,
        provider_query_text: str,
        max_age_seconds: int,
    ) -> dict[str, Any] | None:
        conn = get_conn()
        try:
            row = conn.execute(
                """
                SELECT
                    evidence_query_id,
                    candidate_id,
                    provider_name,
                    provider_query_type,
                    provider_query_text,
                    request_started_at,
                    response_completed_at,
                    latency_ms,
                    result_count,
                    query_status,
                    timeout_seconds,
                    raw_response_metadata,
                    error_message,
                    created_at
                FROM evidence_queries
                WHERE provider_name = ?
                  AND provider_query_type = ?
                  AND provider_query_text = ?
                  AND query_status IN ('ok', 'no_results')
                ORDER BY request_started_at DESC, created_at DESC
                LIMIT 1
                """,
                (provider_name, provider_query_type, provider_query_text),
            ).fetchone()
        finally:
            conn.close()

        if row is None:
            return None

        response_time = _parse_iso(row["response_completed_at"]) or _parse_iso(row["request_started_at"])
        if response_time is None:
            return None
        freshness_seconds = max(
            0,
            int((datetime.now(timezone.utc) - response_time).total_seconds()),
        )
        if freshness_seconds > max_age_seconds:
            return None

        raw_response_metadata = json.loads(row["raw_response_metadata"] or "{}")
        return {
            "evidence_query_id": row["evidence_query_id"],
            "candidate_id": row["candidate_id"],
            "provider_name": row["provider_name"],
            "provider_query_type": row["provider_query_type"],
            "provider_query_text": row["provider_query_text"],
            "request_started_at": row["request_started_at"],
            "response_completed_at": row["response_completed_at"],
            "latency_ms": row["latency_ms"],
            "result_count": int(row["result_count"] or 0),
            "query_status": row["query_status"],
            "timeout_seconds": row["timeout_seconds"],
            "raw_response_metadata": raw_response_metadata,
            "error_message": row["error_message"],
            "created_at": row["created_at"],
            "freshness_seconds": freshness_seconds,
        }

    def provider_budget_usage(
        self,
        *,
        provider_name: str,
        day_start: str,
        month_start: str,
    ) -> dict[str, float | int]:
        conn = get_conn()
        try:
            rows = conn.execute(
                """
                SELECT request_started_at, raw_response_metadata
                FROM evidence_queries
                WHERE provider_name = ?
                  AND request_started_at >= ?
                ORDER BY request_started_at ASC
                """,
                (provider_name, month_start),
            ).fetchall()
        finally:
            conn.close()

        day_start_dt = _parse_iso(day_start)
        month_queries_used = 0
        day_queries_used = 0
        month_spend_usd = 0.0
        day_spend_usd = 0.0

        for row in rows:
            metadata = json.loads(row["raw_response_metadata"] or "{}")
            budget = metadata.get("budget") or {}
            external_call_made = bool(budget.get("external_call_made"))
            if not external_call_made:
                continue
            request_started_at = _parse_iso(row["request_started_at"])
            month_queries_used += 1
            spend = _safe_float(budget.get("estimated_cost_usd"))
            month_spend_usd += spend
            if day_start_dt is not None and request_started_at is not None and request_started_at >= day_start_dt:
                day_queries_used += 1
                day_spend_usd += spend

        return {
            "day_queries_used": day_queries_used,
            "month_queries_used": month_queries_used,
            "day_spend_usd": round(day_spend_usd, 6),
            "month_spend_usd": round(month_spend_usd, 6),
        }

    def latest_evidence_snapshot_for_candidate(self, candidate_id: str) -> dict[str, Any] | None:
        conn = get_conn()
        try:
            row = conn.execute(
                """
                SELECT
                    evidence_snapshot_id,
                    snapshot_time,
                    evidence_state,
                    provider_summary,
                    confidence_modifier,
                    cache_key,
                    freshness_seconds,
                    metadata_json
                FROM evidence_snapshots
                WHERE candidate_id = ?
                ORDER BY snapshot_time DESC, created_at DESC
                LIMIT 1
                """,
                (candidate_id,),
            ).fetchone()
        finally:
            conn.close()

        if row is None:
            return None

        return {
            "evidence_snapshot_id": row["evidence_snapshot_id"],
            "snapshot_time": row["snapshot_time"],
            "evidence_state": row["evidence_state"],
            "provider_summary": json.loads(row["provider_summary"] or "{}"),
            "confidence_modifier": row["confidence_modifier"],
            "cache_key": row["cache_key"],
            "freshness_seconds": row["freshness_seconds"],
            "metadata_json": json.loads(row["metadata_json"] or "{}"),
        }

    def alert_for_candidate(self, candidate_id: str) -> dict[str, Any] | None:
        conn = get_conn()
        try:
            row = conn.execute(
                """
                SELECT
                    alert_id,
                    severity,
                    alert_status,
                    title,
                    rendered_payload,
                    workflow_version,
                    detector_version,
                    feature_schema_version,
                    evidence_snapshot_id,
                    suppression_key,
                    suppression_state,
                    first_delivery_at,
                    last_delivery_at,
                    created_at,
                    updated_at
                FROM alerts
                WHERE candidate_id = ?
                ORDER BY created_at DESC
                LIMIT 1
                """,
                (candidate_id,),
            ).fetchone()
        finally:
            conn.close()

        if row is None:
            return None

        return {
            "alert_id": row["alert_id"],
            "severity": row["severity"],
            "alert_status": row["alert_status"],
            "title": row["title"],
            "rendered_payload": json.loads(row["rendered_payload"] or "{}"),
            "workflow_version": row["workflow_version"],
            "detector_version": row["detector_version"],
            "feature_schema_version": row["feature_schema_version"],
            "evidence_snapshot_id": row["evidence_snapshot_id"],
            "suppression_key": row["suppression_key"],
            "suppression_state": row["suppression_state"],
            "first_delivery_at": row["first_delivery_at"],
            "last_delivery_at": row["last_delivery_at"],
            "created_at": row["created_at"],
            "updated_at": row["updated_at"],
        }

    def record_alert(
        self,
        *,
        candidate_id: str,
        severity: str,
        alert_status: str,
        title: str,
        rendered_payload: dict[str, Any],
        detector_version: str | None,
        feature_schema_version: str | None,
        evidence_snapshot_id: str | None,
        suppression_key: str | None,
        suppression_state: str | None,
    ) -> str:
        alert_id = uuid4().hex
        now = _iso(datetime.now(timezone.utc))
        conn = get_conn()
        try:
            conn.execute(
                """
                INSERT INTO alerts (
                    alert_id,
                    candidate_id,
                    severity,
                    alert_status,
                    title,
                    rendered_payload,
                    workflow_version,
                    detector_version,
                    feature_schema_version,
                    evidence_snapshot_id,
                    suppression_key,
                    suppression_state,
                    first_delivery_at,
                    last_delivery_at,
                    created_at,
                    updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    alert_id,
                    candidate_id,
                    severity,
                    alert_status,
                    title,
                    json.dumps(rendered_payload, sort_keys=True),
                    PHASE4_WORKFLOW_VERSION,
                    detector_version,
                    feature_schema_version,
                    evidence_snapshot_id,
                    suppression_key,
                    suppression_state,
                    None,
                    None,
                    now,
                    now,
                ),
            )
            conn.commit()
        finally:
            conn.close()
        return alert_id

    def record_delivery_attempt(
        self,
        *,
        alert_id: str,
        delivery_channel: str,
        attempt_number: int,
        delivery_status: str,
        request_payload: dict[str, Any] | None,
        response_metadata: dict[str, Any] | None,
        provider_message_id: str | None = None,
        error_message: str | None = None,
    ) -> str:
        delivery_attempt_id = uuid4().hex
        attempted_at = _iso(datetime.now(timezone.utc))
        completed_at = _iso(datetime.now(timezone.utc))
        conn = get_conn()
        try:
            conn.execute(
                """
                INSERT INTO alert_delivery_attempts (
                    delivery_attempt_id,
                    alert_id,
                    delivery_channel,
                    attempt_number,
                    delivery_status,
                    provider_message_id,
                    request_payload,
                    response_metadata,
                    attempted_at,
                    completed_at,
                    error_message
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    delivery_attempt_id,
                    alert_id,
                    delivery_channel,
                    attempt_number,
                    delivery_status,
                    provider_message_id,
                    json.dumps(request_payload or {}, sort_keys=True),
                    json.dumps(response_metadata or {}, sort_keys=True),
                    attempted_at,
                    completed_at,
                    error_message,
                ),
            )
            conn.execute(
                """
                UPDATE alerts
                SET
                    alert_status = ?,
                    first_delivery_at = COALESCE(first_delivery_at, ?),
                    last_delivery_at = ?,
                    updated_at = ?
                WHERE alert_id = ?
                """,
                (
                    "delivered" if delivery_status == "sent" else "delivery_attempted",
                    attempted_at,
                    completed_at,
                    completed_at,
                    alert_id,
                ),
            )
            conn.commit()
        finally:
            conn.close()
        return delivery_attempt_id

    def delivery_attempt_count(self, alert_id: str) -> int:
        conn = get_conn()
        try:
            row = conn.execute(
                "SELECT COUNT(*) FROM alert_delivery_attempts WHERE alert_id = ?",
                (alert_id,),
            ).fetchone()
        finally:
            conn.close()
        return int((row[0] if row else 0) or 0)

    def recent_alert_for_suppression(
        self,
        *,
        suppression_key: str,
        since_time: str,
    ) -> dict[str, Any] | None:
        conn = get_conn()
        try:
            row = conn.execute(
                """
                SELECT
                    alert_id,
                    candidate_id,
                    severity,
                    alert_status,
                    title,
                    rendered_payload,
                    workflow_version,
                    detector_version,
                    feature_schema_version,
                    evidence_snapshot_id,
                    suppression_key,
                    suppression_state,
                    first_delivery_at,
                    last_delivery_at,
                    created_at,
                    updated_at
                FROM alerts
                WHERE suppression_key = ?
                  AND created_at >= ?
                ORDER BY created_at DESC
                LIMIT 1
                """,
                (suppression_key, since_time),
            ).fetchone()
        finally:
            conn.close()

        if row is None:
            return None

        payload = json.loads(row["rendered_payload"] or "{}")
        return {
            "alert_id": row["alert_id"],
            "candidate_id": row["candidate_id"],
            "severity": row["severity"],
            "alert_status": row["alert_status"],
            "title": row["title"],
            "rendered_payload": payload,
            "workflow_version": row["workflow_version"],
            "detector_version": row["detector_version"],
            "feature_schema_version": row["feature_schema_version"],
            "evidence_snapshot_id": row["evidence_snapshot_id"],
            "suppression_key": row["suppression_key"],
            "suppression_state": row["suppression_state"],
            "first_delivery_at": row["first_delivery_at"],
            "last_delivery_at": row["last_delivery_at"],
            "created_at": row["created_at"],
            "updated_at": row["updated_at"],
            "public_evidence_state": payload.get("public_evidence_state"),
        }

    def update_alert_status(
        self,
        *,
        alert_id: str,
        alert_status: str,
        title: str | None = None,
        rendered_payload: dict[str, Any] | None = None,
        suppression_state: str | None = None,
        severity: str | None = None,
        evidence_snapshot_id: str | None = None,
    ) -> None:
        assignments = ["alert_status = ?", "updated_at = ?"]
        params: list[Any] = [alert_status, _iso(datetime.now(timezone.utc))]

        if title is not None:
            assignments.append("title = ?")
            params.append(title)
        if rendered_payload is not None:
            assignments.append("rendered_payload = ?")
            params.append(json.dumps(rendered_payload, sort_keys=True))
        if suppression_state is not None:
            assignments.append("suppression_state = ?")
            params.append(suppression_state)
        if severity is not None:
            assignments.append("severity = ?")
            params.append(severity)
        if evidence_snapshot_id is not None:
            assignments.append("evidence_snapshot_id = ?")
            params.append(evidence_snapshot_id)

        params.append(alert_id)
        conn = get_conn()
        try:
            conn.execute(
                f"""
                UPDATE alerts
                SET {", ".join(assignments)}
                WHERE alert_id = ?
                """,
                tuple(params),
            )
            conn.commit()
        finally:
            conn.close()

    def record_analyst_feedback(
        self,
        *,
        alert_id: str,
        action_type: str,
        actor: str | None,
        notes: str | None,
        follow_up_at: str | None = None,
    ) -> str:
        feedback_id = uuid4().hex
        created_at = _iso(datetime.now(timezone.utc))
        conn = get_conn()
        try:
            conn.execute(
                """
                INSERT INTO analyst_feedback (
                    feedback_id,
                    alert_id,
                    action_type,
                    actor,
                    notes,
                    follow_up_at,
                    created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    feedback_id,
                    alert_id,
                    action_type,
                    actor,
                    notes,
                    follow_up_at,
                    created_at,
                ),
            )
            conn.commit()
        finally:
            conn.close()
        return feedback_id

    def recent_feedback_for_alert(self, alert_id: str, *, limit: int = 20) -> list[dict[str, Any]]:
        conn = get_conn()
        try:
            rows = conn.execute(
                """
                SELECT
                    feedback_id,
                    action_type,
                    actor,
                    notes,
                    follow_up_at,
                    created_at
                FROM analyst_feedback
                WHERE alert_id = ?
                ORDER BY created_at DESC
                LIMIT ?
                """,
                (alert_id, limit),
            ).fetchall()
        finally:
            conn.close()

        return [
            {
                "feedback_id": row["feedback_id"],
                "action_type": row["action_type"],
                "actor": row["actor"],
                "notes": row["notes"],
                "follow_up_at": row["follow_up_at"],
                "created_at": row["created_at"],
            }
            for row in rows
        ]
