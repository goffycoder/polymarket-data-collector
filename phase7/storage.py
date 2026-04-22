from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from uuid import uuid4

from config.settings import (
    PHASE7_COLD_RETENTION_DAYS,
    PHASE7_COMPACTION_MIN_BYTES,
    PHASE7_HOT_RETENTION_DAYS,
    PHASE7_WARM_RETENTION_DAYS,
)
from database.db_manager import get_conn


REPO_ROOT = Path(__file__).resolve().parent.parent


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


def _iso_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _storage_tier(age_days: float | None) -> str:
    if age_days is None:
        return "unknown"
    if age_days <= float(PHASE7_HOT_RETENTION_DAYS):
        return "hot"
    if age_days <= float(PHASE7_WARM_RETENTION_DAYS):
        return "warm"
    if age_days <= float(PHASE7_COLD_RETENTION_DAYS):
        return "cold"
    return "archive_only"


def _recommended_action(*, age_days: float | None, byte_count: int, file_exists: bool) -> str:
    if not file_exists:
        return "investigate_missing_partition"
    if age_days is None:
        return "review_unknown_age"
    if age_days <= float(PHASE7_HOT_RETENTION_DAYS):
        return "retain_hot"
    if byte_count >= int(PHASE7_COMPACTION_MIN_BYTES):
        if age_days > float(PHASE7_WARM_RETENTION_DAYS):
            return "compact_and_cold_archive"
        return "compact_candidate"
    if age_days > float(PHASE7_WARM_RETENTION_DAYS):
        return "cold_archive_candidate"
    return "retain_warm"


@dataclass(slots=True)
class Phase7StorageAuditSummary:
    storage_audit_run_id: str
    audit_scope: str
    total_partitions: int
    total_bytes: int
    missing_file_count: int
    compact_candidate_count: int
    cold_candidate_count: int
    output_path: str | None
    status: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def _load_manifests() -> list[dict[str, Any]]:
    conn = get_conn()
    try:
        raw_rows = conn.execute(
            """
            SELECT
                partition_path,
                source_system,
                event_type AS entity_kind,
                schema_version,
                row_count,
                byte_count,
                first_captured_at,
                last_captured_at,
                'raw' AS storage_class
            FROM raw_archive_manifests
            """
        ).fetchall()
        detector_rows = conn.execute(
            """
            SELECT
                partition_path,
                source_system,
                entity_type AS entity_kind,
                schema_version,
                row_count,
                byte_count,
                first_captured_at,
                last_captured_at,
                'detector_input' AS storage_class
            FROM detector_input_manifests
            """
        ).fetchall()
    finally:
        conn.close()

    manifests: list[dict[str, Any]] = []
    for row in list(raw_rows) + list(detector_rows):
        manifests.append(
            {
                "partition_path": row["partition_path"],
                "source_system": row["source_system"],
                "entity_kind": row["entity_kind"],
                "schema_version": row["schema_version"],
                "row_count": int(row["row_count"] or 0),
                "byte_count": int(row["byte_count"] or 0),
                "first_captured_at": row["first_captured_at"],
                "last_captured_at": row["last_captured_at"],
                "storage_class": row["storage_class"],
            }
        )
    return manifests


def _materialize_decisions(manifests: list[dict[str, Any]]) -> list[dict[str, Any]]:
    now = datetime.now(timezone.utc)
    decisions: list[dict[str, Any]] = []
    for item in manifests:
        partition_path = str(item["partition_path"])
        full_path = REPO_ROOT / partition_path
        last_captured = _parse_iso(item["last_captured_at"])
        age_days = ((now - last_captured).total_seconds() / 86400.0) if last_captured is not None else None
        file_exists = full_path.exists()
        actual_bytes = full_path.stat().st_size if file_exists else None
        recommended_tier = _storage_tier(age_days)
        recommended_action = _recommended_action(
            age_days=age_days,
            byte_count=int(item["byte_count"] or 0),
            file_exists=file_exists,
        )
        decisions.append(
            {
                "partition_path": partition_path,
                "source_system": item["source_system"],
                "entity_kind": item["entity_kind"],
                "schema_version": item["schema_version"],
                "storage_class": item["storage_class"],
                "row_count": item["row_count"],
                "byte_count": item["byte_count"],
                "actual_bytes": actual_bytes,
                "first_captured_at": item["first_captured_at"],
                "last_captured_at": item["last_captured_at"],
                "age_days": round(age_days, 3) if age_days is not None else None,
                "file_exists": file_exists,
                "recommended_tier": recommended_tier,
                "recommended_action": recommended_action,
            }
        )
    return sorted(
        decisions,
        key=lambda item: (
            item["recommended_tier"],
            -(item["byte_count"] or 0),
            item["partition_path"],
        ),
    )


def _persist_audit(
    *,
    audit_scope: str,
    output_path: str | None,
    summary_json: dict[str, Any],
    decisions: list[dict[str, Any]],
) -> Phase7StorageAuditSummary:
    storage_audit_run_id = uuid4().hex
    missing_file_count = sum(1 for item in decisions if not item["file_exists"])
    compact_candidate_count = sum(
        1 for item in decisions if item["recommended_action"] in {"compact_candidate", "compact_and_cold_archive"}
    )
    cold_candidate_count = sum(
        1 for item in decisions if item["recommended_action"] in {"cold_archive_candidate", "compact_and_cold_archive"}
    )
    total_partitions = len(decisions)
    total_bytes = sum(int(item["byte_count"] or 0) for item in decisions)

    conn = get_conn()
    try:
        conn.execute(
            """
            INSERT INTO storage_audit_runs (
                storage_audit_run_id,
                audit_scope,
                status,
                total_partitions,
                total_bytes,
                missing_file_count,
                compact_candidate_count,
                cold_candidate_count,
                output_path,
                summary_json,
                notes,
                completed_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                storage_audit_run_id,
                audit_scope,
                "completed",
                total_partitions,
                total_bytes,
                missing_file_count,
                compact_candidate_count,
                cold_candidate_count,
                output_path,
                json.dumps(summary_json, sort_keys=True),
                "Phase 7 Person 1 storage audit foundation run.",
                _iso_now(),
            ),
        )
        for item in decisions:
            conn.execute(
                """
                INSERT INTO archive_tiering_decisions (
                    tiering_decision_id,
                    storage_audit_run_id,
                    partition_path,
                    source_system,
                    storage_class,
                    recommended_tier,
                    recommended_action,
                    byte_count,
                    age_days,
                    file_exists,
                    metadata_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    uuid4().hex,
                    storage_audit_run_id,
                    item["partition_path"],
                    item["source_system"],
                    item["storage_class"],
                    item["recommended_tier"],
                    item["recommended_action"],
                    int(item["byte_count"] or 0),
                    item["age_days"],
                    1 if item["file_exists"] else 0,
                    json.dumps(
                        {
                            "entity_kind": item["entity_kind"],
                            "schema_version": item["schema_version"],
                            "row_count": item["row_count"],
                            "actual_bytes": item["actual_bytes"],
                            "first_captured_at": item["first_captured_at"],
                            "last_captured_at": item["last_captured_at"],
                        },
                        sort_keys=True,
                    ),
                ),
            )
        conn.commit()
    finally:
        conn.close()

    return Phase7StorageAuditSummary(
        storage_audit_run_id=storage_audit_run_id,
        audit_scope=audit_scope,
        total_partitions=total_partitions,
        total_bytes=total_bytes,
        missing_file_count=missing_file_count,
        compact_candidate_count=compact_candidate_count,
        cold_candidate_count=cold_candidate_count,
        output_path=output_path,
        status="completed",
    )


def build_storage_audit(*, audit_scope: str = "full_repo", output_path: str | None = None) -> tuple[Phase7StorageAuditSummary, dict[str, Any]]:
    manifests = _load_manifests()
    decisions = _materialize_decisions(manifests)
    source_rollups: dict[str, dict[str, Any]] = {}
    for item in decisions:
        rollup = source_rollups.setdefault(
            str(item["source_system"]),
            {
                "source_system": item["source_system"],
                "partition_count": 0,
                "total_bytes": 0,
                "missing_file_count": 0,
                "storage_classes": set(),
                "recommended_tiers": {},
            },
        )
        rollup["partition_count"] += 1
        rollup["total_bytes"] += int(item["byte_count"] or 0)
        rollup["missing_file_count"] += 0 if item["file_exists"] else 1
        rollup["storage_classes"].add(item["storage_class"])
        rollup["recommended_tiers"][item["recommended_tier"]] = rollup["recommended_tiers"].get(item["recommended_tier"], 0) + 1

    summary_payload = {
        "audit_scope": audit_scope,
        "generated_at": _iso_now(),
        "totals": {
            "partition_count": len(decisions),
            "total_bytes": sum(int(item["byte_count"] or 0) for item in decisions),
            "missing_file_count": sum(1 for item in decisions if not item["file_exists"]),
        },
        "source_rollups": [
            {
                **value,
                "storage_classes": sorted(value["storage_classes"]),
            }
            for value in sorted(source_rollups.values(), key=lambda item: (-item["total_bytes"], item["source_system"]))
        ],
        "largest_partitions": sorted(decisions, key=lambda item: (-int(item["byte_count"] or 0), item["partition_path"]))[:25],
        "tiering_decisions": decisions[:250],
    }
    summary = _persist_audit(
        audit_scope=audit_scope,
        output_path=output_path,
        summary_json=summary_payload,
        decisions=decisions,
    )
    return summary, summary_payload
