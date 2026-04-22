from __future__ import annotations

import json
from typing import Any

from database.db_manager import get_conn


def _load_latest_row(table_name: str, id_column: str) -> dict[str, Any] | None:
    conn = get_conn()
    try:
        row = conn.execute(
            f"""
            SELECT *
            FROM {table_name}
            ORDER BY created_at DESC, {id_column} DESC
            LIMIT 1
            """
        ).fetchone()
    finally:
        conn.close()
    if row is None:
        return None
    payload = {key: row[key] for key in row.keys()}
    if "summary_json" in payload and payload["summary_json"]:
        try:
            payload["summary"] = json.loads(payload["summary_json"])
        except json.JSONDecodeError:
            payload["summary"] = None
    return payload


def build_phase7_person1_report() -> dict[str, Any]:
    latest_audit = _load_latest_row("storage_audit_runs", "storage_audit_run_id")
    latest_compaction = _load_latest_row("compaction_plan_runs", "compaction_plan_run_id")
    latest_restore = _load_latest_row("restore_plan_runs", "restore_plan_run_id")
    latest_integrity = _load_latest_row("integrity_summary_runs", "integrity_summary_run_id")
    latest_action_batch = _load_latest_row("archive_action_runs", "archive_action_run_id")
    latest_service_profile = _load_latest_row("service_profile_runs", "service_profile_run_id")

    blockers: list[str] = []
    if latest_audit is None:
        blockers.append("no_storage_audit")
    elif int(latest_audit.get("missing_file_count") or 0) > 0:
        blockers.append("missing_archive_files")

    if latest_integrity is None:
        blockers.append("no_integrity_summary")
    elif str(latest_integrity.get("status")) != "ready":
        blockers.append(f"integrity_status={latest_integrity.get('status')}")

    if latest_service_profile is None:
        blockers.append("no_service_profile")
    elif int(latest_service_profile.get("failure_risk_count") or 0) > 0:
        blockers.append("service_failure_risks_detected")

    assessment_status = "phase7_person1_ready" if not blockers else "phase7_person1_followup_required"
    return {
        "latest_storage_audit": latest_audit,
        "latest_compaction_plan": latest_compaction,
        "latest_restore_plan": latest_restore,
        "latest_integrity_summary": latest_integrity,
        "latest_archive_action_batch": latest_action_batch,
        "latest_service_profile": latest_service_profile,
        "assessment": {
            "status": assessment_status,
            "blockers": blockers,
            "has_storage_audit": latest_audit is not None,
            "has_integrity_summary": latest_integrity is not None,
            "has_service_profile": latest_service_profile is not None,
        },
    }
