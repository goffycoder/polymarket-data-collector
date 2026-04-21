"""Phase 2 replay-validation helpers.

This module provides a first practical replay-validation surface for the
local durable data plane:
- locate raw and detector-input partitions overlapping a time window
- count envelopes in-window from the local append-only logs
- compare those counts against manifest coverage

It does not yet republish events into the detector. That comes in the next
replay iteration once the archive and detector-input contracts are stable.
"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from database.db_manager import get_conn
from utils.event_log import DETECTOR_INPUT_ROOT, RAW_ARCHIVE_ROOT


REPO_ROOT = Path(__file__).resolve().parent.parent


def _parse_iso(value: str) -> datetime:
    """Parse a datetime-like string into a UTC datetime."""

    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _hour_floor(value: datetime) -> datetime:
    """Round a datetime down to the start of the hour."""

    return value.replace(minute=0, second=0, microsecond=0)


def _iter_hours(start: datetime, end: datetime) -> list[datetime]:
    """Return every hourly bucket touched by the requested window."""

    hours: list[datetime] = []
    current = _hour_floor(start)
    while current < end:
        hours.append(current)
        current += timedelta(hours=1)
    return hours


def _partition_file(root: Path, source_system: str, dt: datetime) -> Path:
    """Construct the canonical partition file path for a source/hour pair."""

    return (
        root
        / f"year={dt:%Y}"
        / f"month={dt:%m}"
        / f"day={dt:%d}"
        / f"hour={dt:%H}"
        / f"source_system={source_system}"
        / "events.ndjson"
    )


def _read_ndjson_rows(path: Path) -> list[dict[str, Any]]:
    """Read one NDJSON partition into parsed JSON rows."""

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


def _count_rows_in_window(rows: list[dict[str, Any]], start: datetime, end: datetime) -> int:
    """Count rows whose captured_at timestamp falls inside the requested window."""

    count = 0
    for row in rows:
        captured_at = row.get("captured_at")
        if not captured_at:
            continue
        captured_dt = _parse_iso(str(captured_at))
        if start <= captured_dt < end:
            count += 1
    return count


@dataclass(slots=True)
class PartitionReplayStats:
    """Summarize one replay-relevant partition file."""

    partition_path: str
    exists: bool
    total_rows: int
    rows_in_window: int
    manifest_row_count: int | None
    manifest_byte_count: int | None


@dataclass(slots=True)
class ReplayWindowReport:
    """Summarize replay-validation status for one requested historical window."""

    start: str
    end: str
    source_system: str
    raw_partitions: list[PartitionReplayStats]
    detector_partitions: list[PartitionReplayStats]

    def to_dict(self) -> dict[str, Any]:
        """Convert the report into a JSON-serializable structure."""

        return {
            "start": self.start,
            "end": self.end,
            "source_system": self.source_system,
            "raw_partitions": [asdict(item) for item in self.raw_partitions],
            "detector_partitions": [asdict(item) for item in self.detector_partitions],
            "summary": {
                "raw_rows_in_window": sum(item.rows_in_window for item in self.raw_partitions),
                "detector_rows_in_window": sum(item.rows_in_window for item in self.detector_partitions),
                "raw_partitions_found": sum(1 for item in self.raw_partitions if item.exists),
                "detector_partitions_found": sum(1 for item in self.detector_partitions if item.exists),
            },
        }


def _load_manifest_map(table_name: str, source_system: str) -> dict[str, dict[str, Any]]:
    """Load manifest rows keyed by partition path for one source system."""

    conn = get_conn()
    try:
        rows = conn.execute(
            f"""
            SELECT partition_path, row_count, byte_count
            FROM {table_name}
            WHERE source_system = ?
            """,
            (source_system,),
        ).fetchall()
        return {
            str(row["partition_path"]): {
                "row_count": int(row["row_count"]) if row["row_count"] is not None else None,
                "byte_count": int(row["byte_count"]) if row["byte_count"] is not None else None,
            }
            for row in rows
        }
    finally:
        conn.close()


def _build_partition_stats(
    *,
    root: Path,
    manifest_table: str,
    source_system: str,
    start: datetime,
    end: datetime,
) -> list[PartitionReplayStats]:
    """Collect file-level replay stats for one log root and manifest table."""

    manifest_map = _load_manifest_map(manifest_table, source_system)
    stats: list[PartitionReplayStats] = []

    for hour in _iter_hours(start, end):
        partition_file = _partition_file(root, source_system, hour)
        relative_path = partition_file.relative_to(REPO_ROOT).as_posix()
        rows = _read_ndjson_rows(partition_file)
        manifest = manifest_map.get(relative_path, {})
        stats.append(
            PartitionReplayStats(
                partition_path=relative_path,
                exists=partition_file.exists(),
                total_rows=len(rows),
                rows_in_window=_count_rows_in_window(rows, start, end),
                manifest_row_count=manifest.get("row_count"),
                manifest_byte_count=manifest.get("byte_count"),
            )
        )

    return stats


def build_replay_window_report(*, start: str, end: str, source_system: str) -> ReplayWindowReport:
    """Build a replay-validation report for one source system and time window."""

    start_dt = _parse_iso(start)
    end_dt = _parse_iso(end)
    if end_dt <= start_dt:
        raise ValueError("end must be later than start")

    raw_stats = _build_partition_stats(
        root=RAW_ARCHIVE_ROOT,
        manifest_table="raw_archive_manifests",
        source_system=source_system,
        start=start_dt,
        end=end_dt,
    )
    detector_stats = _build_partition_stats(
        root=DETECTOR_INPUT_ROOT,
        manifest_table="detector_input_manifests",
        source_system=source_system,
        start=start_dt,
        end=end_dt,
    )

    return ReplayWindowReport(
        start=start_dt.isoformat(),
        end=end_dt.isoformat(),
        source_system=source_system,
        raw_partitions=raw_stats,
        detector_partitions=detector_stats,
    )
