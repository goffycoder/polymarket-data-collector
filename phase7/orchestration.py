from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from typing import Any
from uuid import uuid4

from config.settings import PHASE7_POLICY_BATCH_SIZE
from database.db_manager import get_conn
from phase7.reporting import _load_latest_audit, _load_tiering_decisions
from phase7.storage import _iso_now


ELIGIBLE_ACTIONS = {
    "compact_candidate",
    "cold_archive_candidate",
    "compact_and_cold_archive",
    "investigate_missing_partition",
}


@dataclass(slots=True)
class Phase7PolicyEnforcementSummary:
    archive_action_run_id: str
    storage_audit_run_id: str | None
    execution_mode: str
    total_items: int
    compact_item_count: int
    cold_archive_item_count: int
    investigate_item_count: int
    output_path: str | None
    status: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def build_policy_enforcement_plan(
    *,
    execution_mode: str = "dry_run",
    max_items: int | None = None,
    storage_audit_run_id: str | None = None,
    output_path: str | None = None,
) -> tuple[Phase7PolicyEnforcementSummary, dict[str, Any]]:
    latest = _load_latest_audit()
    if latest is None and storage_audit_run_id is None:
        raise ValueError("No storage audit exists yet. Run run_phase7_storage_audit.py first.")

    audit_id = storage_audit_run_id or str((latest or {})["storage_audit_run_id"])
    batch_limit = max_items if max_items is not None and max_items > 0 else int(PHASE7_POLICY_BATCH_SIZE)
    decisions = [
        item
        for item in _load_tiering_decisions(audit_id)
        if item["recommended_action"] in ELIGIBLE_ACTIONS
    ]
    decisions = decisions[:batch_limit]

    planned_items: list[dict[str, Any]] = []
    compact_count = 0
    cold_count = 0
    investigate_count = 0
    for item in decisions:
        recommended_action = str(item["recommended_action"])
        if recommended_action == "compact_and_cold_archive":
            enforcement_action = "compact_then_stage_cold_archive"
            compact_count += 1
            cold_count += 1
        elif recommended_action == "compact_candidate":
            enforcement_action = "compact_partition"
            compact_count += 1
        elif recommended_action == "cold_archive_candidate":
            enforcement_action = "stage_cold_archive"
            cold_count += 1
        else:
            enforcement_action = "investigate_partition_gap"
            investigate_count += 1
        planned_items.append(
            {
                "archive_action_item_id": uuid4().hex,
                "partition_path": item["partition_path"],
                "source_system": item["source_system"],
                "storage_class": item["storage_class"],
                "recommended_action": recommended_action,
                "enforcement_action": enforcement_action,
                "recommended_tier": item["recommended_tier"],
                "byte_count": int(item["byte_count"] or 0),
                "age_days": item["age_days"],
                "file_exists": bool(item["file_exists"]),
                "status": "planned" if execution_mode == "dry_run" else "queued",
                "metadata_json": {
                    "tiering_decision_id": item["tiering_decision_id"],
                    "created_from_mode": execution_mode,
                },
            }
        )

    status = "planned" if execution_mode == "dry_run" else "queued"
    payload = {
        "generated_at": _iso_now(),
        "storage_audit_run_id": audit_id,
        "execution_mode": execution_mode,
        "batch_limit": batch_limit,
        "status": status,
        "total_items": len(planned_items),
        "compact_item_count": compact_count,
        "cold_archive_item_count": cold_count,
        "investigate_item_count": investigate_count,
        "planned_items": planned_items,
    }

    archive_action_run_id = uuid4().hex
    conn = get_conn()
    try:
        conn.execute(
            """
            INSERT INTO archive_action_runs (
                archive_action_run_id,
                storage_audit_run_id,
                execution_mode,
                status,
                total_items,
                compact_item_count,
                cold_archive_item_count,
                investigate_item_count,
                output_path,
                summary_json,
                completed_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                archive_action_run_id,
                audit_id,
                execution_mode,
                status,
                len(planned_items),
                compact_count,
                cold_count,
                investigate_count,
                output_path,
                json.dumps(payload, sort_keys=True),
                _iso_now(),
            ),
        )
        conn.executemany(
            """
            INSERT INTO archive_action_items (
                archive_action_item_id,
                archive_action_run_id,
                partition_path,
                source_system,
                storage_class,
                recommended_action,
                enforcement_action,
                status,
                byte_count,
                age_days,
                file_exists,
                metadata_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    item["archive_action_item_id"],
                    archive_action_run_id,
                    item["partition_path"],
                    item["source_system"],
                    item["storage_class"],
                    item["recommended_action"],
                    item["enforcement_action"],
                    item["status"],
                    item["byte_count"],
                    item["age_days"],
                    1 if item["file_exists"] else 0,
                    json.dumps(item["metadata_json"], sort_keys=True),
                )
                for item in planned_items
            ],
        )
        conn.commit()
    finally:
        conn.close()

    return (
        Phase7PolicyEnforcementSummary(
            archive_action_run_id=archive_action_run_id,
            storage_audit_run_id=audit_id,
            execution_mode=execution_mode,
            total_items=len(planned_items),
            compact_item_count=compact_count,
            cold_archive_item_count=cold_count,
            investigate_item_count=investigate_count,
            output_path=output_path,
            status=status,
        ),
        payload,
    )
