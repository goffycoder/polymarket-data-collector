"""Phase 2 replay republisher.

This module turns archived raw envelopes back into a deterministic replay input
artifact for one requested historical window. The output is a local NDJSON log
that later detector workers can consume in-order.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from uuid import uuid4

from database.db_manager import get_conn
from utils.event_log import RAW_ARCHIVE_ROOT


REPO_ROOT = Path(__file__).resolve().parent.parent
REPLAY_RUNS_ROOT = REPO_ROOT / "data" / "replay_runs"


def _parse_iso(value: str) -> datetime:
    """Parse a datetime-like string into UTC."""

    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _hour_floor(value: datetime) -> datetime:
    """Round a datetime down to the hour."""

    return value.replace(minute=0, second=0, microsecond=0)


def _iter_hours(start: datetime, end: datetime) -> list[datetime]:
    """Return hourly buckets overlapping the requested window."""

    buckets: list[datetime] = []
    current = _hour_floor(start)
    while current < end:
        buckets.append(current)
        current += timedelta(hours=1)
    return buckets


def _partition_file(source_system: str, dt: datetime) -> Path:
    """Build the canonical raw archive partition file path."""

    return (
        RAW_ARCHIVE_ROOT
        / f"year={dt:%Y}"
        / f"month={dt:%m}"
        / f"day={dt:%d}"
        / f"hour={dt:%H}"
        / f"source_system={source_system}"
        / "events.ndjson"
    )


def _write_json_line(path: Path, payload: dict[str, Any]) -> int:
    """Append one JSON line and return the bytes written."""

    path.parent.mkdir(parents=True, exist_ok=True)
    encoded = json.dumps(payload, sort_keys=True).encode("utf-8") + b"\n"
    with path.open("ab") as handle:
        handle.write(encoded)
    return len(encoded)


def _iter_partition_rows(path: Path) -> list[dict[str, Any]]:
    """Load all JSON lines from one archive partition."""

    rows: list[dict[str, Any]] = []
    if not path.exists():
        return rows

    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            rows.append(json.loads(line))
    return rows


@dataclass(slots=True)
class ReplayRepublishResult:
    """Summarize one completed replay republish run."""

    replay_run_id: str
    source_system: str
    start: str
    end: str
    raw_partitions_touched: int
    raw_rows_scanned: int
    rows_republished: int
    output_path: str

    def to_dict(self) -> dict[str, Any]:
        """Convert the result to a JSON-serializable payload."""

        return {
            "replay_run_id": self.replay_run_id,
            "source_system": self.source_system,
            "start": self.start,
            "end": self.end,
            "raw_partitions_touched": self.raw_partitions_touched,
            "raw_rows_scanned": self.raw_rows_scanned,
            "rows_republished": self.rows_republished,
            "output_path": self.output_path,
        }


def _insert_replay_run(replay_run_id: str, source_system: str, start: str, end: str) -> None:
    """Create the replay run row before work begins."""

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
                created_at
            ) VALUES (?, ?, ?, ?, 'running', ?)
            """,
            (replay_run_id, source_system, start, end, datetime.now(timezone.utc).isoformat()),
        )
        conn.commit()
    finally:
        conn.close()


def _complete_replay_run(
    *,
    replay_run_id: str,
    raw_partitions_touched: int,
    raw_rows_scanned: int,
    rows_republished: int,
    output_path: str,
    notes: str | None = None,
) -> None:
    """Mark the replay run as complete with summary stats."""

    conn = get_conn()
    try:
        conn.execute(
            """
            UPDATE replay_runs
            SET status = 'completed',
                raw_partitions_touched = ?,
                raw_rows_scanned = ?,
                rows_republished = ?,
                output_path = ?,
                notes = ?,
                completed_at = ?
            WHERE replay_run_id = ?
            """,
            (
                raw_partitions_touched,
                raw_rows_scanned,
                rows_republished,
                output_path,
                notes,
                datetime.now(timezone.utc).isoformat(),
                replay_run_id,
            ),
        )
        conn.commit()
    finally:
        conn.close()


def republish_raw_window(*, start: str, end: str, source_system: str) -> ReplayRepublishResult:
    """Republish archived raw envelopes for one source/window into a replay input log."""

    start_dt = _parse_iso(start)
    end_dt = _parse_iso(end)
    if end_dt <= start_dt:
        raise ValueError("end must be later than start")

    replay_run_id = uuid4().hex[:12]
    _insert_replay_run(replay_run_id, source_system, start_dt.isoformat(), end_dt.isoformat())

    output_path = (
        REPLAY_RUNS_ROOT
        / replay_run_id
        / f"source_system={source_system}"
        / "detector_input.ndjson"
    )
    republished_at = datetime.now(timezone.utc).isoformat()

    raw_partitions_touched = 0
    raw_rows_scanned = 0
    rows_republished = 0

    for hour in _iter_hours(start_dt, end_dt):
        partition = _partition_file(source_system, hour)
        rows = _iter_partition_rows(partition)
        if not rows:
            continue

        raw_partitions_touched += 1
        raw_rows_scanned += len(rows)
        relative_partition = partition.relative_to(REPO_ROOT).as_posix()

        for row in rows:
            captured_at_raw = row.get("captured_at")
            if not captured_at_raw:
                continue
            captured_at = _parse_iso(str(captured_at_raw))
            if not (start_dt <= captured_at < end_dt):
                continue

            replay_envelope = {
                "replay_run_id": replay_run_id,
                "captured_at": captured_at.isoformat(),
                "replayed_at": republished_at,
                "source_system": source_system,
                "entity_type": "replay_raw_event",
                "ordering_key": f"{captured_at.isoformat()}::{row.get('envelope_id', '')}",
                "raw_partition_path": relative_partition,
                "original_envelope_id": row.get("envelope_id"),
                "original_event_type": row.get("event_type"),
                "original_schema_version": row.get("schema_version"),
                "payload": row,
            }
            _write_json_line(output_path, replay_envelope)
            rows_republished += 1

    relative_output = output_path.relative_to(REPO_ROOT).as_posix()
    _complete_replay_run(
        replay_run_id=replay_run_id,
        raw_partitions_touched=raw_partitions_touched,
        raw_rows_scanned=raw_rows_scanned,
        rows_republished=rows_republished,
        output_path=relative_output,
        notes="Republished archived raw envelopes into a deterministic replay detector-input log.",
    )

    return ReplayRepublishResult(
        replay_run_id=replay_run_id,
        source_system=source_system,
        start=start_dt.isoformat(),
        end=end_dt.isoformat(),
        raw_partitions_touched=raw_partitions_touched,
        raw_rows_scanned=raw_rows_scanned,
        rows_republished=rows_republished,
        output_path=relative_output,
    )
