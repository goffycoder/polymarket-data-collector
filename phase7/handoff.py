from __future__ import annotations

from typing import Any

from validation.phase7_person1_report import build_phase7_person1_report


def build_phase7_person2_handoff() -> dict[str, Any]:
    report = build_phase7_person1_report()
    latest_audit = report.get("latest_storage_audit") or {}
    latest_restore = report.get("latest_restore_plan") or {}
    latest_integrity = report.get("latest_integrity_summary") or {}
    latest_profile = report.get("latest_service_profile") or {}
    latest_action_batch = report.get("latest_archive_action_batch") or {}

    storage_summary = (latest_audit.get("summary") or {}) if isinstance(latest_audit, dict) else {}
    integrity_summary = (latest_integrity.get("summary") or {}) if isinstance(latest_integrity, dict) else {}
    profile_summary = (latest_profile.get("summary") or {}) if isinstance(latest_profile, dict) else {}

    return {
        "handoff_scope": "phase7_person1_to_person2",
        "assessment": report.get("assessment", {}),
        "retained_dataset_guarantees": {
            "latest_storage_audit_run_id": latest_audit.get("storage_audit_run_id"),
            "total_partitions": latest_audit.get("total_partitions"),
            "missing_file_count": latest_audit.get("missing_file_count"),
            "compact_candidate_count": latest_audit.get("compact_candidate_count"),
            "cold_candidate_count": latest_audit.get("cold_candidate_count"),
        },
        "restore_guarantees": {
            "latest_restore_plan_run_id": latest_restore.get("restore_plan_run_id"),
            "restore_scope": latest_restore.get("restore_scope"),
            "total_items": latest_restore.get("total_items"),
            "missing_item_count": latest_restore.get("missing_item_count"),
            "status": latest_restore.get("status"),
        },
        "archive_tiering_rules": {
            "latest_integrity_summary_run_id": latest_integrity.get("integrity_summary_run_id"),
            "summary_scope": latest_integrity.get("summary_scope"),
            "status": latest_integrity.get("status"),
            "source_rollups": integrity_summary.get("source_rollups", []),
        },
        "operator_dashboard_views": {
            "dashboard_top_sources": storage_summary.get("top_sources_by_bytes", []),
            "largest_partitions": storage_summary.get("largest_partitions", []),
            "runtime_rollups": profile_summary.get("runtime_rollups", {}),
        },
        "scale_constraints": {
            "latest_service_profile_run_id": latest_profile.get("service_profile_run_id"),
            "bottlenecks": profile_summary.get("prioritized_bottlenecks", []),
            "failure_risks": profile_summary.get("prioritized_failure_risks", []),
            "latest_action_batch_status": latest_action_batch.get("status"),
        },
    }
