from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from uuid import uuid4

from config.settings import (
    DETECTOR_INPUT_ROOT,
    EXTERNAL_ARCHIVE_ROOTS,
    PHASE7_COLD_RETENTION_DAYS,
    PHASE7_COMPACTION_MIN_BYTES,
    PHASE7_HOT_RETENTION_DAYS,
    PHASE7_WARM_RETENTION_DAYS,
    RAW_ARCHIVE_ROOT,
)
from database.db_manager import get_conn


REPO_ROOT = Path(__file__).resolve().parent.parent
RAW_PREFIX = "data/raw/"
DETECTOR_PREFIX = "data/detector_input/"


def resolve_manifest_path(partition_path: str) -> Path:
    """Resolve a manifest partition path from either repo-relative or absolute storage."""

    raw_text = str(partition_path)
    path = Path(raw_text)
    if path.is_absolute():
        return path
    if raw_text.startswith(RAW_PREFIX):
        return Path(str(RAW_ARCHIVE_ROOT)) / raw_text[len(RAW_PREFIX):]
    if raw_text.startswith(DETECTOR_PREFIX):
        return Path(str(DETECTOR_INPUT_ROOT)) / raw_text[len(DETECTOR_PREFIX):]
    return REPO_ROOT / path


def _discovered_external_archive_roots() -> list[Path]:
    volumes_root = Path("/Volumes")
    if not volumes_root.exists():
        return []

    roots: list[Path] = []
    for volume in volumes_root.iterdir():
        if volume.name == "Macintosh HD" or not volume.is_dir():
            continue
        candidates = [
            volume / "polymarket_phase11_data_archive_2026-05-15",
            volume / "polymarket_archive",
            volume / "polymarket_arbitrage",
        ]
        for candidate in candidates:
            if (candidate / "data").exists():
                roots.append(candidate)
        for candidate in volume.glob("polymarket*_archive*"):
            if candidate.is_dir() and (candidate / "data").exists():
                roots.append(candidate)
    return roots


def external_archive_roots() -> list[Path]:
    roots: list[Path] = []
    seen: set[str] = set()
    for root in [*EXTERNAL_ARCHIVE_ROOTS, *_discovered_external_archive_roots()]:
        resolved = root.expanduser()
        key = str(resolved)
        if key in seen:
            continue
        seen.add(key)
        roots.append(resolved)
    return roots


def resolve_manifest_candidates(partition_path: str) -> list[dict[str, Any]]:
    raw_text = str(partition_path)
    primary = resolve_manifest_path(raw_text)
    candidates: list[tuple[str, Path]] = [("configured", primary)]

    if not Path(raw_text).is_absolute() and (raw_text.startswith(RAW_PREFIX) or raw_text.startswith(DETECTOR_PREFIX)):
        candidates.append(("repo_local", REPO_ROOT / raw_text))
        for root in external_archive_roots():
            candidates.append(("external_archive", root / raw_text))

    deduped: list[dict[str, Any]] = []
    seen: set[str] = set()
    for kind, path in candidates:
        key = str(path)
        if key in seen:
            continue
        seen.add(key)
        exists = path.exists()
        deduped.append(
            {
                "kind": kind,
                "path": str(path),
                "exists": exists,
                "size_bytes": int(path.stat().st_size) if exists and path.is_file() else None,
            }
        )
    return deduped


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


def _recommended_action(
    *,
    age_days: float | None,
    byte_count: int,
    file_exists: bool,
    availability: str = "local",
) -> str:
    if not file_exists:
        return "investigate_missing_partition"
    if availability == "external_archive":
        return "restore_from_external_archive_if_needed"
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
        path_candidates = resolve_manifest_candidates(partition_path)
        configured_candidate = path_candidates[0]
        available_candidate = next((candidate for candidate in path_candidates if candidate["exists"]), None)
        full_path = Path(str((available_candidate or configured_candidate)["path"]))
        last_captured = _parse_iso(item["last_captured_at"])
        age_days = ((now - last_captured).total_seconds() / 86400.0) if last_captured is not None else None
        file_exists = available_candidate is not None
        actual_bytes = None if available_candidate is None else available_candidate["size_bytes"]
        availability = "missing"
        if available_candidate is not None:
            availability = "external_archive" if available_candidate["kind"] == "external_archive" else "local"
        recommended_tier = _storage_tier(age_days)
        recommended_action = _recommended_action(
            age_days=age_days,
            byte_count=int(item["byte_count"] or 0),
            file_exists=file_exists,
            availability=availability,
        )
        decisions.append(
            {
                "partition_path": partition_path,
                "resolved_path": str(full_path),
                "configured_path": str(configured_candidate["path"]),
                "path_candidates": path_candidates,
                "availability": availability,
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
    archive_only_count = sum(1 for item in decisions if item.get("availability") == "external_archive")
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
                            "resolved_path": item.get("resolved_path"),
                            "configured_path": item.get("configured_path"),
                            "availability": item.get("availability"),
                            "path_candidates": item.get("path_candidates"),
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
                "archive_only_count": 0,
                "storage_classes": set(),
                "recommended_tiers": {},
            },
        )
        rollup["partition_count"] += 1
        rollup["total_bytes"] += int(item["byte_count"] or 0)
        rollup["missing_file_count"] += 0 if item["file_exists"] else 1
        rollup["archive_only_count"] += 1 if item.get("availability") == "external_archive" else 0
        rollup["storage_classes"].add(item["storage_class"])
        rollup["recommended_tiers"][item["recommended_tier"]] = rollup["recommended_tiers"].get(item["recommended_tier"], 0) + 1

    archive_only_count = sum(1 for item in decisions if item.get("availability") == "external_archive")
    summary_payload = {
        "audit_scope": audit_scope,
        "generated_at": _iso_now(),
        "external_archive_roots": [str(root) for root in external_archive_roots()],
        "totals": {
            "partition_count": len(decisions),
            "total_bytes": sum(int(item["byte_count"] or 0) for item in decisions),
            "missing_file_count": sum(1 for item in decisions if not item["file_exists"]),
            "archive_only_count": archive_only_count,
            "local_or_archive_available_count": sum(1 for item in decisions if item["file_exists"]),
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
