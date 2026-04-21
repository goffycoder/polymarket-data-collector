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

    def pending_candidates(self, *, limit: int = 10) -> list[dict[str, str | float | None]]:
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
                WHERE a.alert_id IS NULL
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
                "triggering_rules": row["triggering_rules"],
                "feature_snapshot": row["feature_snapshot"],
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
