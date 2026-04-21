from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from typing import Any

from config.settings import PHASE4_ALERT_CHANNELS, PHASE4_WORKFLOW_VERSION
from database.db_manager import get_conn
from phase4.timefmt import format_eastern


@dataclass(slots=True)
class Phase4Gate4Report:
    workflow_version: str
    workflow_registration: dict[str, Any] | None
    alert_summary: dict[str, Any]
    delivery_summary: dict[str, Any]
    analyst_summary: dict[str, Any]
    latest_alert_example: dict[str, Any] | None
    assessment: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def _load_workflow_registration() -> dict[str, Any] | None:
    conn = get_conn()
    try:
        row = conn.execute(
            """
            SELECT workflow_version, evidence_schema_version, alert_schema_version, delivery_channels, notes, created_at, last_used_at
            FROM alert_workflow_versions
            WHERE workflow_version = ?
            """,
            (PHASE4_WORKFLOW_VERSION,),
        ).fetchone()
    finally:
        conn.close()

    if row is None:
        return None
    return {
        "workflow_version": row["workflow_version"],
        "evidence_schema_version": row["evidence_schema_version"],
        "alert_schema_version": row["alert_schema_version"],
        "delivery_channels": json.loads(row["delivery_channels"] or "[]"),
        "notes": row["notes"],
        "created_at": row["created_at"],
        "created_at_display": format_eastern(row["created_at"]),
        "last_used_at": row["last_used_at"],
        "last_used_at_display": format_eastern(row["last_used_at"]),
    }


def _alert_summary() -> dict[str, Any]:
    conn = get_conn()
    try:
        total_alerts = conn.execute("SELECT COUNT(*) FROM alerts").fetchone()[0]
        suppressed_alerts = conn.execute(
            "SELECT COUNT(*) FROM alerts WHERE alert_status = 'suppressed'"
        ).fetchone()[0]
        delivered_alerts = conn.execute(
            "SELECT COUNT(*) FROM alerts WHERE alert_status IN ('delivered', 'acknowledged', 'snoozed', 'dismissed', 'useful', 'false_positive', 'annotated')"
        ).fetchone()[0]
        severity_rows = conn.execute(
            """
            SELECT severity, COUNT(*) AS count
            FROM alerts
            GROUP BY severity
            ORDER BY count DESC, severity ASC
            """
        ).fetchall()
    finally:
        conn.close()

    return {
        "total_alerts": int(total_alerts or 0),
        "suppressed_alerts": int(suppressed_alerts or 0),
        "delivered_or_reviewed_alerts": int(delivered_alerts or 0),
        "severity_counts": [
            {"severity": row["severity"], "count": int(row["count"] or 0)}
            for row in severity_rows
        ],
    }


def _delivery_summary() -> dict[str, Any]:
    conn = get_conn()
    try:
        total_attempts = conn.execute("SELECT COUNT(*) FROM alert_delivery_attempts").fetchone()[0]
        sent_attempts = conn.execute(
            "SELECT COUNT(*) FROM alert_delivery_attempts WHERE delivery_status = 'sent'"
        ).fetchone()[0]
        skipped_attempts = conn.execute(
            "SELECT COUNT(*) FROM alert_delivery_attempts WHERE delivery_status = 'skipped'"
        ).fetchone()[0]
        error_attempts = conn.execute(
            "SELECT COUNT(*) FROM alert_delivery_attempts WHERE delivery_status = 'error'"
        ).fetchone()[0]
        channel_rows = conn.execute(
            """
            SELECT delivery_channel, COUNT(*) AS count
            FROM alert_delivery_attempts
            GROUP BY delivery_channel
            ORDER BY count DESC, delivery_channel ASC
            """
        ).fetchall()
    finally:
        conn.close()

    return {
        "total_attempts": int(total_attempts or 0),
        "sent_attempts": int(sent_attempts or 0),
        "skipped_attempts": int(skipped_attempts or 0),
        "error_attempts": int(error_attempts or 0),
        "channel_counts": [
            {"delivery_channel": row["delivery_channel"], "count": int(row["count"] or 0)}
            for row in channel_rows
        ],
        "configured_channels": list(PHASE4_ALERT_CHANNELS),
    }


def _analyst_summary() -> dict[str, Any]:
    conn = get_conn()
    try:
        total_feedback = conn.execute("SELECT COUNT(*) FROM analyst_feedback").fetchone()[0]
        action_rows = conn.execute(
            """
            SELECT action_type, COUNT(*) AS count
            FROM analyst_feedback
            GROUP BY action_type
            ORDER BY count DESC, action_type ASC
            """
        ).fetchall()
    finally:
        conn.close()

    return {
        "total_feedback_rows": int(total_feedback or 0),
        "action_counts": [
            {"action_type": row["action_type"], "count": int(row["count"] or 0)}
            for row in action_rows
        ],
    }


def _latest_alert_example() -> dict[str, Any] | None:
    conn = get_conn()
    try:
        row = conn.execute(
            """
            SELECT
                a.alert_id,
                a.candidate_id,
                a.severity,
                a.alert_status,
                a.title,
                a.rendered_payload,
                a.suppression_state,
                a.created_at,
                es.evidence_state,
                es.provider_summary
            FROM alerts a
            LEFT JOIN evidence_snapshots es ON es.evidence_snapshot_id = a.evidence_snapshot_id
            ORDER BY a.created_at DESC
            LIMIT 1
            """
        ).fetchone()
        if row is None:
            return None

        delivery_rows = conn.execute(
            """
            SELECT delivery_channel, delivery_status, attempted_at, error_message
            FROM alert_delivery_attempts
            WHERE alert_id = ?
            ORDER BY attempted_at ASC
            """,
            (row["alert_id"],),
        ).fetchall()
        feedback_rows = conn.execute(
            """
            SELECT action_type, actor, notes, created_at
            FROM analyst_feedback
            WHERE alert_id = ?
            ORDER BY created_at ASC
            """,
            (row["alert_id"],),
        ).fetchall()
    finally:
        conn.close()

    return {
        "alert_id": row["alert_id"],
        "candidate_id": row["candidate_id"],
        "severity": row["severity"],
        "alert_status": row["alert_status"],
        "title": row["title"],
        "suppression_state": row["suppression_state"],
        "created_at": row["created_at"],
        "created_at_display": format_eastern(row["created_at"]),
        "payload": json.loads(row["rendered_payload"] or "{}"),
        "evidence_state": row["evidence_state"],
        "provider_summary": json.loads(row["provider_summary"] or "{}") if row["provider_summary"] else {},
        "delivery_attempts": [
            {
                "delivery_channel": delivery_row["delivery_channel"],
                "delivery_status": delivery_row["delivery_status"],
                "attempted_at": delivery_row["attempted_at"],
                "attempted_at_display": format_eastern(delivery_row["attempted_at"]),
                "error_message": delivery_row["error_message"],
            }
            for delivery_row in delivery_rows
        ],
        "analyst_feedback": [
            {
                "action_type": feedback_row["action_type"],
                "actor": feedback_row["actor"],
                "notes": feedback_row["notes"],
                "created_at": feedback_row["created_at"],
                "created_at_display": format_eastern(feedback_row["created_at"]),
            }
            for feedback_row in feedback_rows
        ],
    }


def _assessment(
    alert_summary: dict[str, Any],
    delivery_summary: dict[str, Any],
    analyst_summary: dict[str, Any],
) -> dict[str, Any]:
    total_alerts = alert_summary["total_alerts"]
    if total_alerts == 0:
        status = "no_alerts_yet"
    elif delivery_summary["error_attempts"] > 0:
        status = "delivery_errors_present"
    elif analyst_summary["total_feedback_rows"] == 0:
        status = "alerts_live_without_feedback"
    else:
        status = "end_to_end_live"

    return {
        "status": status,
        "total_alerts": total_alerts,
        "suppressed_alerts": alert_summary["suppressed_alerts"],
        "total_delivery_attempts": delivery_summary["total_attempts"],
        "analyst_feedback_rows": analyst_summary["total_feedback_rows"],
    }


def build_phase4_gate4_report() -> Phase4Gate4Report:
    workflow_registration = _load_workflow_registration()
    alert_summary = _alert_summary()
    delivery_summary = _delivery_summary()
    analyst_summary = _analyst_summary()
    latest_alert_example = _latest_alert_example()

    return Phase4Gate4Report(
        workflow_version=PHASE4_WORKFLOW_VERSION,
        workflow_registration=workflow_registration,
        alert_summary=alert_summary,
        delivery_summary=delivery_summary,
        analyst_summary=analyst_summary,
        latest_alert_example=latest_alert_example,
        assessment=_assessment(alert_summary, delivery_summary, analyst_summary),
    )
