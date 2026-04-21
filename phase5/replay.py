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
PHASE5_REPLAY_ARTIFACT_VERSION = "phase5_replay_v1"


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


@dataclass(slots=True)
class Phase5ReplayRunSummary:
    replay_run_id: str
    artifact_version: str
    source_system: str
    start: str
    end: str
    status: str
    git_commit: str
    integrity_status: str
    raw_missing_partitions: int
    detector_missing_partitions: int
    raw_manifest_mismatches: int
    detector_manifest_mismatches: int
    raw_partitions_touched: int
    raw_rows_scanned: int
    detector_rows_observed: int
    output_path: str | None
    notes: str | None

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass(slots=True)
class Phase5ReplayBundleSummary:
    bundle_id: str
    artifact_version: str
    git_commit: str
    start: str
    end: str
    source_systems: list[str]
    overall_status: str
    output_path: str | None
    total_raw_rows_scanned: int
    total_detector_rows_observed: int
    replay_runs: list[dict[str, Any]]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def _insert_run_start(*, replay_run_id: str, source_system: str, start: str, end: str, notes: str | None) -> None:
    conn = get_conn()
    try:
        conn.execute(
            """
            INSERT INTO replay_runs (
                replay_run_id,
                source_system,
                start_time,
                end_time,
                status,
                notes
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                replay_run_id,
                source_system,
                start,
                end,
                "running",
                notes,
            ),
        )
        conn.commit()
    finally:
        conn.close()


def _update_run(
    *,
    replay_run_id: str,
    status: str,
    raw_partitions_touched: int,
    raw_rows_scanned: int,
    rows_republished: int,
    output_path: str | None,
    notes: str | None,
) -> None:
    conn = get_conn()
    try:
        conn.execute(
            """
            UPDATE replay_runs
            SET
                status = ?,
                raw_partitions_touched = ?,
                raw_rows_scanned = ?,
                rows_republished = ?,
                output_path = ?,
                notes = ?,
                completed_at = ?
            WHERE replay_run_id = ?
            """,
            (
                status,
                raw_partitions_touched,
                raw_rows_scanned,
                rows_republished,
                output_path,
                notes,
                _iso_now(),
                replay_run_id,
            ),
        )
        conn.commit()
    finally:
        conn.close()


def _count_manifest_mismatches(partitions: list[dict[str, Any]]) -> int:
    mismatches = 0
    for partition in partitions:
        manifest_rows = partition.get("manifest_row_count")
        if partition.get("exists") and manifest_rows is not None and int(manifest_rows) != int(partition.get("total_rows") or 0):
            mismatches += 1
    return mismatches


def _integrity_from_payload(payload: dict[str, Any]) -> dict[str, int | str]:
    raw_partitions = payload["raw_partitions"]
    detector_partitions = payload["detector_partitions"]

    raw_missing = sum(1 for item in raw_partitions if not item["exists"])
    detector_missing = sum(1 for item in detector_partitions if not item["exists"])
    raw_mismatches = _count_manifest_mismatches(raw_partitions)
    detector_mismatches = _count_manifest_mismatches(detector_partitions)
    total_rows = int(payload["summary"]["raw_rows_in_window"]) + int(payload["summary"]["detector_rows_in_window"])

    if total_rows == 0:
        integrity_status = "empty_window"
    elif raw_missing or detector_missing or raw_mismatches or detector_mismatches:
        integrity_status = "degraded"
    else:
        integrity_status = "ready"

    return {
        "integrity_status": integrity_status,
        "raw_missing_partitions": raw_missing,
        "detector_missing_partitions": detector_missing,
        "raw_manifest_mismatches": raw_mismatches,
        "detector_manifest_mismatches": detector_mismatches,
    }


def run_phase5_replay_window(
    *,
    start: str,
    end: str,
    source_system: str,
    output_dir: str = "reports/phase5/replay_runs",
    notes: str | None = None,
) -> Phase5ReplayRunSummary:
    replay_run_id = uuid4().hex
    git_commit = _git_head()
    base_notes = notes or "Phase 5 Person 1 replay foundation run."
    _insert_run_start(
        replay_run_id=replay_run_id,
        source_system=source_system,
        start=start,
        end=end,
        notes=base_notes,
    )

    try:
        report = build_replay_window_report(start=start, end=end, source_system=source_system)
        payload = report.to_dict()
        summary = payload["summary"]
        integrity = _integrity_from_payload(payload)
        artifact_payload = {
            "replay_run_id": replay_run_id,
            "artifact_version": PHASE5_REPLAY_ARTIFACT_VERSION,
            "git_commit": git_commit,
            "config": {
                "start": start,
                "end": end,
                "source_system": source_system,
            },
            "integrity": integrity,
            "report": payload,
        }

        artifact_dir = REPO_ROOT / output_dir
        artifact_dir.mkdir(parents=True, exist_ok=True)
        artifact_path = artifact_dir / f"{replay_run_id}.json"
        artifact_path.write_text(json.dumps(artifact_payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")

        run_notes = json.dumps(
            {
                "artifact_version": PHASE5_REPLAY_ARTIFACT_VERSION,
                "git_commit": git_commit,
                "detector_partitions_found": summary["detector_partitions_found"],
                "integrity": integrity,
                "mode": "coverage_replay_foundation",
                "notes": base_notes,
            },
            sort_keys=True,
        )
        _update_run(
            replay_run_id=replay_run_id,
            status="completed" if integrity["integrity_status"] == "ready" else "completed_with_gaps",
            raw_partitions_touched=int(summary["raw_partitions_found"]),
            raw_rows_scanned=int(summary["raw_rows_in_window"]),
            rows_republished=int(summary["detector_rows_in_window"]),
            output_path=str(artifact_path.relative_to(REPO_ROOT)),
            notes=run_notes,
        )

        return Phase5ReplayRunSummary(
            replay_run_id=replay_run_id,
            artifact_version=PHASE5_REPLAY_ARTIFACT_VERSION,
            source_system=source_system,
            start=payload["start"],
            end=payload["end"],
            status="completed" if integrity["integrity_status"] == "ready" else "completed_with_gaps",
            git_commit=git_commit,
            integrity_status=str(integrity["integrity_status"]),
            raw_missing_partitions=int(integrity["raw_missing_partitions"]),
            detector_missing_partitions=int(integrity["detector_missing_partitions"]),
            raw_manifest_mismatches=int(integrity["raw_manifest_mismatches"]),
            detector_manifest_mismatches=int(integrity["detector_manifest_mismatches"]),
            raw_partitions_touched=int(summary["raw_partitions_found"]),
            raw_rows_scanned=int(summary["raw_rows_in_window"]),
            detector_rows_observed=int(summary["detector_rows_in_window"]),
            output_path=str(artifact_path.relative_to(REPO_ROOT)),
            notes=base_notes,
        )
    except Exception as exc:
        error_notes = json.dumps(
            {
                "artifact_version": PHASE5_REPLAY_ARTIFACT_VERSION,
                "git_commit": git_commit,
                "mode": "coverage_replay_foundation",
                "error": str(exc),
                "notes": base_notes,
            },
            sort_keys=True,
        )
        _update_run(
            replay_run_id=replay_run_id,
            status="failed",
            raw_partitions_touched=0,
            raw_rows_scanned=0,
            rows_republished=0,
            output_path=None,
            notes=error_notes,
        )
        raise


def run_phase5_replay_bundle(
    *,
    start: str,
    end: str,
    source_systems: list[str],
    output_dir: str = "reports/phase5/replay_runs",
    notes: str | None = None,
) -> Phase5ReplayBundleSummary:
    if not source_systems:
        raise ValueError("At least one source system is required.")

    git_commit = _git_head()
    bundle_id = uuid4().hex
    unique_source_systems = list(dict.fromkeys(source_systems))
    results: list[Phase5ReplayRunSummary] = []
    for source_system in unique_source_systems:
        results.append(
            run_phase5_replay_window(
                start=start,
                end=end,
                source_system=source_system,
                output_dir=output_dir,
                notes=notes,
            )
        )

    overall_status = "ready"
    if any(result.integrity_status == "degraded" for result in results):
        overall_status = "degraded"
    elif all(result.integrity_status == "empty_window" for result in results):
        overall_status = "empty_window"
    elif any(result.integrity_status == "empty_window" for result in results):
        overall_status = "partial"

    artifact_dir = REPO_ROOT / output_dir / "bundles"
    artifact_dir.mkdir(parents=True, exist_ok=True)
    artifact_path = artifact_dir / f"{bundle_id}.json"
    artifact_payload = {
        "bundle_id": bundle_id,
        "artifact_version": PHASE5_REPLAY_ARTIFACT_VERSION,
        "git_commit": git_commit,
        "start": start,
        "end": end,
        "source_systems": unique_source_systems,
        "overall_status": overall_status,
        "notes": notes,
        "replay_runs": [result.to_dict() for result in results],
    }
    artifact_path.write_text(json.dumps(artifact_payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    return Phase5ReplayBundleSummary(
        bundle_id=bundle_id,
        artifact_version=PHASE5_REPLAY_ARTIFACT_VERSION,
        git_commit=git_commit,
        start=start,
        end=end,
        source_systems=unique_source_systems,
        overall_status=overall_status,
        output_path=str(artifact_path.relative_to(REPO_ROOT)),
        total_raw_rows_scanned=sum(result.raw_rows_scanned for result in results),
        total_detector_rows_observed=sum(result.detector_rows_observed for result in results),
        replay_runs=[result.to_dict() for result in results],
    )
