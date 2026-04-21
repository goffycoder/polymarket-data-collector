"""Local Phase 2 durable event-log helpers.

This module provides the first shared implementation for:
- raw event envelopes written to deterministic local partitions
- manifest tracking for those partitions
- a local detector-input log for normalized envelopes

The Phase 2 rollout starts with lower-frequency REST sync paths first.
Higher-frequency paths such as WebSocket bursts can adopt the same helpers
once their batching and performance semantics are defined.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from uuid import uuid4

from database.db_manager import get_conn
from utils.logger import get_logger

log = get_logger("event_log")

REPO_ROOT = Path(__file__).resolve().parent.parent
RAW_ARCHIVE_ROOT = REPO_ROOT / "data" / "raw"
DETECTOR_INPUT_ROOT = REPO_ROOT / "data" / "detector_input"

RAW_ENVELOPE_SCHEMA_VERSION = "raw_event_envelope.v1"
NORMALIZED_ENVELOPE_SCHEMA_VERSION = "normalized_envelope.v1"


@dataclass(frozen=True, slots=True)
class PartitionWriteResult:
    """Describe one append to a durable local log partition."""

    partition_path: str
    bytes_written: int
    captured_at: str
    envelope_id: str
    schema_version: str


def _normalize_timestamp(captured_at: str | datetime | None) -> datetime:
    """Convert optional captured_at input into a timezone-aware UTC datetime."""

    if captured_at is None:
        return datetime.now(timezone.utc)
    if isinstance(captured_at, datetime):
        if captured_at.tzinfo is None:
            return captured_at.replace(tzinfo=timezone.utc)
        return captured_at.astimezone(timezone.utc)
    parsed = datetime.fromisoformat(str(captured_at).replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _sanitize_source_system(source_system: str) -> str:
    """Keep partition paths filesystem-friendly and deterministic."""

    cleaned = source_system.strip().lower().replace("/", "_").replace(" ", "_")
    return cleaned or "unknown"


def _partition_path(root: Path, source_system: str, captured_dt: datetime) -> Path:
    """Build the deterministic hourly partition path for one source system."""

    safe_source = _sanitize_source_system(source_system)
    return (
        root
        / f"year={captured_dt:%Y}"
        / f"month={captured_dt:%m}"
        / f"day={captured_dt:%d}"
        / f"hour={captured_dt:%H}"
        / f"source_system={safe_source}"
        / "events.ndjson"
    )


def _json_default(value: Any) -> str:
    """Serialize datetime-like values in JSON payloads."""

    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc).isoformat()
    raise TypeError(f"Object of type {type(value).__name__} is not JSON serializable")


def _append_json_line(path: Path, payload: dict[str, Any]) -> int:
    """Append one JSON line to the given partition file and return bytes written."""

    path.parent.mkdir(parents=True, exist_ok=True)
    encoded = json.dumps(payload, sort_keys=True, default=_json_default).encode("utf-8") + b"\n"
    with path.open("ab") as handle:
        handle.write(encoded)
    return len(encoded)


def _register_schema_version(component: str, schema_version: str, notes: str) -> None:
    """Track the latest schema version used by a Phase 2 component."""

    conn = get_conn()
    try:
        conn.execute(
            """
            INSERT INTO schema_versions (component, schema_version, notes, updated_at)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(component) DO UPDATE SET
                schema_version = excluded.schema_version,
                notes = excluded.notes,
                updated_at = excluded.updated_at
            """,
            (component, schema_version, notes, datetime.now(timezone.utc).isoformat()),
        )
        conn.commit()
    finally:
        conn.close()


def _update_raw_manifest(
    partition_path: str,
    source_system: str,
    event_type: str,
    schema_version: str,
    captured_at: str,
    bytes_written: int,
    envelope_id: str,
) -> None:
    """Record raw archive partition activity in the manifest table."""

    conn = get_conn()
    try:
        conn.execute(
            """
            INSERT INTO raw_archive_manifests (
                partition_path,
                source_system,
                event_type,
                schema_version,
                row_count,
                byte_count,
                first_captured_at,
                last_captured_at,
                last_envelope_id,
                last_updated_at
            ) VALUES (?, ?, ?, ?, 1, ?, ?, ?, ?, ?)
            ON CONFLICT(partition_path) DO UPDATE SET
                row_count = raw_archive_manifests.row_count + 1,
                byte_count = raw_archive_manifests.byte_count + excluded.byte_count,
                last_captured_at = excluded.last_captured_at,
                last_envelope_id = excluded.last_envelope_id,
                last_updated_at = excluded.last_updated_at
            """,
            (
                partition_path,
                source_system,
                event_type,
                schema_version,
                bytes_written,
                captured_at,
                captured_at,
                envelope_id,
                datetime.now(timezone.utc).isoformat(),
            ),
        )
        conn.commit()
    finally:
        conn.close()


def _update_detector_manifest(
    partition_path: str,
    source_system: str,
    entity_type: str,
    schema_version: str,
    captured_at: str,
    bytes_written: int,
    ordering_key: str | None,
) -> None:
    """Record local normalized detector-input partition activity."""

    conn = get_conn()
    try:
        conn.execute(
            """
            INSERT INTO detector_input_manifests (
                partition_path,
                source_system,
                entity_type,
                schema_version,
                row_count,
                byte_count,
                first_captured_at,
                last_captured_at,
                last_ordering_key,
                last_updated_at
            ) VALUES (?, ?, ?, ?, 1, ?, ?, ?, ?, ?)
            ON CONFLICT(partition_path) DO UPDATE SET
                row_count = detector_input_manifests.row_count + 1,
                byte_count = detector_input_manifests.byte_count + excluded.byte_count,
                last_captured_at = excluded.last_captured_at,
                last_ordering_key = excluded.last_ordering_key,
                last_updated_at = excluded.last_updated_at
            """,
            (
                partition_path,
                source_system,
                entity_type,
                schema_version,
                bytes_written,
                captured_at,
                captured_at,
                ordering_key,
                datetime.now(timezone.utc).isoformat(),
            ),
        )
        conn.commit()
    finally:
        conn.close()


def archive_raw_event(
    *,
    source_system: str,
    event_type: str,
    payload: dict[str, Any] | list[Any],
    captured_at: str | datetime | None = None,
    metadata: dict[str, Any] | None = None,
    schema_version: str = RAW_ENVELOPE_SCHEMA_VERSION,
) -> PartitionWriteResult:
    """Persist one raw inbound payload to the local append-only archive."""

    captured_dt = _normalize_timestamp(captured_at)
    captured_iso = captured_dt.isoformat()
    envelope_id = uuid4().hex
    partition = _partition_path(RAW_ARCHIVE_ROOT, source_system, captured_dt)
    relative_partition = partition.relative_to(REPO_ROOT).as_posix()
    envelope = {
        "envelope_id": envelope_id,
        "schema_version": schema_version,
        "source_system": source_system,
        "event_type": event_type,
        "captured_at": captured_iso,
        "metadata": metadata or {},
        "payload": payload,
    }
    bytes_written = _append_json_line(partition, envelope)
    _update_raw_manifest(
        partition_path=relative_partition,
        source_system=source_system,
        event_type=event_type,
        schema_version=schema_version,
        captured_at=captured_iso,
        bytes_written=bytes_written,
        envelope_id=envelope_id,
    )
    _register_schema_version(
        component="raw_event_envelope",
        schema_version=schema_version,
        notes="Local Phase 2 raw envelope archive format.",
    )
    return PartitionWriteResult(
        partition_path=relative_partition,
        bytes_written=bytes_written,
        captured_at=captured_iso,
        envelope_id=envelope_id,
        schema_version=schema_version,
    )


def publish_detector_input(
    *,
    source_system: str,
    entity_type: str,
    payload: dict[str, Any],
    captured_at: str | datetime | None = None,
    ordering_key: str | None = None,
    raw_partition_path: str | None = None,
    schema_version: str = NORMALIZED_ENVELOPE_SCHEMA_VERSION,
) -> PartitionWriteResult:
    """Persist one normalized detector-input envelope to the local log."""

    captured_dt = _normalize_timestamp(captured_at)
    captured_iso = captured_dt.isoformat()
    envelope_id = uuid4().hex
    partition = _partition_path(DETECTOR_INPUT_ROOT, source_system, captured_dt)
    relative_partition = partition.relative_to(REPO_ROOT).as_posix()
    envelope = {
        "envelope_id": envelope_id,
        "schema_version": schema_version,
        "source_system": source_system,
        "entity_type": entity_type,
        "captured_at": captured_iso,
        "ordering_key": ordering_key,
        "raw_partition_path": raw_partition_path,
        "payload": payload,
    }
    bytes_written = _append_json_line(partition, envelope)
    _update_detector_manifest(
        partition_path=relative_partition,
        source_system=source_system,
        entity_type=entity_type,
        schema_version=schema_version,
        captured_at=captured_iso,
        bytes_written=bytes_written,
        ordering_key=ordering_key,
    )
    _register_schema_version(
        component="normalized_envelope",
        schema_version=schema_version,
        notes="Local Phase 2 detector-input envelope format.",
    )
    return PartitionWriteResult(
        partition_path=relative_partition,
        bytes_written=bytes_written,
        captured_at=captured_iso,
        envelope_id=envelope_id,
        schema_version=schema_version,
    )

