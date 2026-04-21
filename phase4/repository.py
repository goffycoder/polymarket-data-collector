from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone

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
                    sc.trigger_time,
                    sc.detector_version,
                    sc.feature_schema_version,
                    sc.severity_score
                FROM signal_candidates sc
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
                "trigger_time": row["trigger_time"],
                "detector_version": row["detector_version"],
                "feature_schema_version": row["feature_schema_version"],
                "severity_score": row["severity_score"],
            }
            for row in rows
        ]
