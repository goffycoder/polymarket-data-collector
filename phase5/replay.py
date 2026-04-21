from __future__ import annotations

import json
import subprocess
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
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
    raw_partitions_touched: int
    raw_rows_scanned: int
    detector_rows_observed: int
    output_path: str | None
    notes: str | None

    def to_dict(self) -> dict:
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
        artifact_payload = {
            "replay_run_id": replay_run_id,
            "artifact_version": PHASE5_REPLAY_ARTIFACT_VERSION,
            "git_commit": git_commit,
            "config": {
                "start": start,
                "end": end,
                "source_system": source_system,
            },
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
                "mode": "coverage_replay_foundation",
                "notes": base_notes,
            },
            sort_keys=True,
        )
        _update_run(
            replay_run_id=replay_run_id,
            status="completed",
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
            status="completed",
            git_commit=git_commit,
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
