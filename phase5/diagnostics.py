from __future__ import annotations

import json
import subprocess
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from uuid import uuid4

from database.db_manager import get_conn
from validation.phase2_replay import build_replay_window_report


REPO_ROOT = Path(__file__).resolve().parent.parent
PHASE5_WINDOW_HEALTH_VERSION = "phase5_window_health_v1"


def _iso_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _git_head() -> str:
    try:
        completed = subprocess.run(
            ["git", "rev-parse", "--short", "HEAD"],
            cwd=REPO_ROOT,
            capture_output=True,
            text=True,
            check=True,
        )
    except Exception:
        return "unknown"
    return completed.stdout.strip() or "unknown"


def _count_manifest_mismatches(partitions: list[dict[str, Any]]) -> int:
    mismatches = 0
    for partition in partitions:
        manifest_rows = partition.get("manifest_row_count")
        if partition.get("exists") and manifest_rows is not None and int(manifest_rows) != int(partition.get("total_rows") or 0):
            mismatches += 1
    return mismatches


def _missing_partition_paths(partitions: list[dict[str, Any]]) -> list[str]:
    return [str(item["partition_path"]) for item in partitions if not item["exists"]]


def _manifest_mismatch_paths(partitions: list[dict[str, Any]]) -> list[str]:
    paths: list[str] = []
    for partition in partitions:
        manifest_rows = partition.get("manifest_row_count")
        if partition.get("exists") and manifest_rows is not None and int(manifest_rows) != int(partition.get("total_rows") or 0):
            paths.append(str(partition["partition_path"]))
    return paths


@dataclass(slots=True)
class Phase5WindowHealthItem:
    source_system: str
    integrity_status: str
    raw_rows_in_window: int
    detector_rows_in_window: int
    raw_partitions_found: int
    detector_partitions_found: int
    raw_missing_partitions: list[str]
    detector_missing_partitions: list[str]
    raw_manifest_mismatches: list[str]
    detector_manifest_mismatches: list[str]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class Phase5WindowHealthSummary:
    health_check_id: str
    artifact_version: str
    git_commit: str
    start: str
    end: str
    source_systems: list[str]
    overall_status: str
    output_path: str | None
    health_items: list[dict[str, Any]]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class Phase5BackfillRequestSummary:
    backfill_request_id: str
    source_system: str
    start: str
    end: str
    request_status: str
    priority: str
    requested_by: str | None
    reason: str | None
    output_path: str | None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def _health_item_from_report(*, source_system: str, payload: dict[str, Any]) -> Phase5WindowHealthItem:
    raw_missing = _missing_partition_paths(payload["raw_partitions"])
    detector_missing = _missing_partition_paths(payload["detector_partitions"])
    raw_mismatches = _manifest_mismatch_paths(payload["raw_partitions"])
    detector_mismatches = _manifest_mismatch_paths(payload["detector_partitions"])
    summary = payload["summary"]
    total_rows = int(summary["raw_rows_in_window"]) + int(summary["detector_rows_in_window"])

    if total_rows == 0:
        integrity_status = "empty_window"
    elif raw_missing or detector_missing or raw_mismatches or detector_mismatches:
        integrity_status = "degraded"
    else:
        integrity_status = "ready"

    return Phase5WindowHealthItem(
        source_system=source_system,
        integrity_status=integrity_status,
        raw_rows_in_window=int(summary["raw_rows_in_window"]),
        detector_rows_in_window=int(summary["detector_rows_in_window"]),
        raw_partitions_found=int(summary["raw_partitions_found"]),
        detector_partitions_found=int(summary["detector_partitions_found"]),
        raw_missing_partitions=raw_missing,
        detector_missing_partitions=detector_missing,
        raw_manifest_mismatches=raw_mismatches,
        detector_manifest_mismatches=detector_mismatches,
    )


def inspect_phase5_window_health(
    *,
    start: str,
    end: str,
    source_systems: list[str],
    output_dir: str = "reports/phase5/window_health",
    notes: str | None = None,
) -> Phase5WindowHealthSummary:
    unique_source_systems = list(dict.fromkeys(source_systems))
    if not unique_source_systems:
        raise ValueError("At least one source system is required.")

    git_commit = _git_head()
    health_check_id = uuid4().hex
    health_items: list[Phase5WindowHealthItem] = []

    for source_system in unique_source_systems:
        report = build_replay_window_report(start=start, end=end, source_system=source_system)
        health_items.append(_health_item_from_report(source_system=source_system, payload=report.to_dict()))

    overall_status = "ready"
    if any(item.integrity_status == "degraded" for item in health_items):
        overall_status = "degraded"
    elif all(item.integrity_status == "empty_window" for item in health_items):
        overall_status = "empty_window"
    elif any(item.integrity_status == "empty_window" for item in health_items):
        overall_status = "partial"

    artifact_payload = {
        "health_check_id": health_check_id,
        "artifact_version": PHASE5_WINDOW_HEALTH_VERSION,
        "git_commit": git_commit,
        "start": start,
        "end": end,
        "source_systems": unique_source_systems,
        "overall_status": overall_status,
        "notes": notes,
        "health_items": [item.to_dict() for item in health_items],
    }
    artifact_dir = REPO_ROOT / output_dir
    artifact_dir.mkdir(parents=True, exist_ok=True)
    artifact_path = artifact_dir / f"{health_check_id}.json"
    artifact_path.write_text(json.dumps(artifact_payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    return Phase5WindowHealthSummary(
        health_check_id=health_check_id,
        artifact_version=PHASE5_WINDOW_HEALTH_VERSION,
        git_commit=git_commit,
        start=start,
        end=end,
        source_systems=unique_source_systems,
        overall_status=overall_status,
        output_path=str(artifact_path.relative_to(REPO_ROOT)),
        health_items=[item.to_dict() for item in health_items],
    )


def record_phase5_backfill_requests(
    *,
    start: str,
    end: str,
    source_systems: list[str],
    requested_by: str | None,
    reason: str,
    priority: str = "normal",
    health_check_id: str | None = None,
    output_dir: str = "reports/phase5/backfill_requests",
    notes: str | None = None,
) -> list[Phase5BackfillRequestSummary]:
    unique_source_systems = list(dict.fromkeys(source_systems))
    if not unique_source_systems:
        raise ValueError("At least one source system is required.")
    if not reason.strip():
        raise ValueError("A non-empty reason is required for backfill requests.")

    conn = get_conn()
    results: list[Phase5BackfillRequestSummary] = []
    artifact_dir = REPO_ROOT / output_dir
    artifact_dir.mkdir(parents=True, exist_ok=True)
    try:
        for source_system in unique_source_systems:
            backfill_request_id = uuid4().hex
            payload = {
                "backfill_request_id": backfill_request_id,
                "source_system": source_system,
                "start": start,
                "end": end,
                "requested_by": requested_by,
                "reason": reason,
                "priority": priority,
                "health_check_id": health_check_id,
                "notes": notes,
                "requested_at": _iso_now(),
            }
            artifact_path = artifact_dir / f"{backfill_request_id}.json"
            artifact_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
            conn.execute(
                """
                INSERT INTO backfill_requests (
                    backfill_request_id,
                    source_system,
                    start_time,
                    end_time,
                    request_status,
                    priority,
                    requested_by,
                    reason,
                    request_payload,
                    output_path,
                    notes
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    backfill_request_id,
                    source_system,
                    start,
                    end,
                    "requested",
                    priority,
                    requested_by,
                    reason,
                    json.dumps(payload, sort_keys=True),
                    str(artifact_path.relative_to(REPO_ROOT)),
                    notes,
                ),
            )
            results.append(
                Phase5BackfillRequestSummary(
                    backfill_request_id=backfill_request_id,
                    source_system=source_system,
                    start=start,
                    end=end,
                    request_status="requested",
                    priority=priority,
                    requested_by=requested_by,
                    reason=reason,
                    output_path=str(artifact_path.relative_to(REPO_ROOT)),
                )
            )
        conn.commit()
    finally:
        conn.close()

    return results
