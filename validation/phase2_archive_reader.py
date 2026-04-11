"""Streaming archive reader for Person 2 Phase 2 replay validation.

This module intentionally stays below normalization and replay logic. Its job
is to define the archive input surface and to stream raw archived envelopes for
one replay window in a deterministic, rejection-aware way.

Expected archive layout
=======================
Archive files are addressed through manifest entries and are expected to live in
deterministic partitions such as::

    <archive_root>/
      year=YYYY/month=MM/day=DD/hour=HH/
        source_system=<source_system>/
          source_endpoint=<normalized_source_endpoint>/
            part-0001.ndjson
            part-0002.ndjson.gz

Expected manifest schema
========================
Manifest files are newline-delimited JSON (NDJSON) or JSON arrays. Each entry
must include:

    {
      "manifest_id": "manifest-2026-04-10T14-data_api-trades-0001",
      "archive_uri": "year=2026/month=04/day=10/hour=14/source_system=data_api/source_endpoint=trades/part-0001.ndjson.gz",
      "file_format": "ndjson",
      "compression": "gzip",
      "source_system": "data_api",
      "source_endpoint": "/trades",
      "collector_source": "trades_collector",
      "collector_version": "git:4f3c2ab",
      "raw_schema_version": "raw-envelope/v1",
      "window_start": "2026-04-10T14:00:00.000000Z",
      "window_end": "2026-04-10T15:00:00.000000Z",
      "row_count": 500,
      "checksum": "sha256:..."
    }

Supported archive file formats
==============================
Only NDJSON and gzipped NDJSON are supported in this phase. Files are always
streamed line by line; the reader never loads an entire archive file into
memory.
"""

from __future__ import annotations

import gzip
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator


@dataclass(slots=True)
class ArchiveManifestEntry:
    """Represent one manifest row that points to a deterministic archive file."""

    manifest_id: str
    archive_uri: str
    file_format: str
    compression: str
    source_system: str
    source_endpoint: str
    collector_source: str | None
    collector_version: str | None
    raw_schema_version: str | None
    window_start: str
    window_end: str
    row_count: int | None = None
    checksum: str | None = None


@dataclass(slots=True)
class RawArchiveRecord:
    """Represent one raw archived record with manifest-attached metadata."""

    raw_event_uuid: str
    payload_json: Any
    event_time: str
    ingest_time: str
    event_time_source: str
    source_system: str
    source_endpoint: str
    manifest_id: str
    archive_uri: str
    payload_hash: str
    collector_source: str | None
    collector_version: str | None
    raw_schema_version: str | None
    file_offset: int
    line_number: int
    source_event_id: str | None = None
    market_id: str | None = None
    condition_id: str | None = None
    asset_id: str | None = None
    wallet_id: str | None = None


@dataclass(slots=True)
class ArchiveRecordRejection:
    """Represent one explicit archive parsing or validation rejection."""

    reason_code: str
    message: str
    manifest_id: str | None
    archive_uri: str
    file_offset: int
    line_number: int
    raw_line: str | None = None
    exception_type: str | None = None


def _load_archive_window(
    archive_root: str | Path,
    manifest_path: str | Path,
    *,
    start_time: str | datetime,
    end_time: str | datetime,
    source_system: str | None = None,
    source_endpoint: str | None = None,
) -> tuple[Iterator[RawArchiveRecord], list[ArchiveRecordRejection]]:
    """Return a deterministic iterator of raw archive records for one window.

    The returned iterator streams matching archive files in this order:
    1. ``archive_uri`` ascending
    2. ``file_offset`` ascending within each file

    The returned rejection list is populated as the iterator is consumed.
    Rejections are explicit and never silently dropped.
    """

    archive_root_path = Path(archive_root)
    manifest_path_obj = Path(manifest_path)
    start_dt = _coerce_timestamp(start_time)
    end_dt = _coerce_timestamp(end_time)
    if end_dt <= start_dt:
        raise ValueError("Archive window end_time must be greater than start_time")

    rejected_records: list[ArchiveRecordRejection] = []
    manifest_entries = _load_manifest_entries(
        manifest_path_obj,
        start_time=start_dt,
        end_time=end_dt,
        source_system=source_system,
        source_endpoint=source_endpoint,
        rejected_records=rejected_records,
    )

    def _record_iterator() -> Iterator[RawArchiveRecord]:
        for manifest_entry in manifest_entries:
            archive_path = _resolve_archive_path(archive_root_path, manifest_entry.archive_uri)
            yield from _read_archive_file(
                archive_path,
                manifest_entry,
                start_time=start_dt,
                end_time=end_dt,
                rejected_records=rejected_records,
            )

    return _record_iterator(), rejected_records


def _read_archive_file(
    archive_path: str | Path,
    manifest_entry: ArchiveManifestEntry,
    *,
    start_time: str | datetime,
    end_time: str | datetime,
    rejected_records: list[ArchiveRecordRejection] | None = None,
) -> Iterator[RawArchiveRecord]:
    """Stream one NDJSON archive file and yield matching raw archive records.

    Records are filtered by the half-open interval
    ``start_time <= event_time < end_time`` after event-time fallback rules are
    applied by :func:`_parse_raw_envelope`.
    """

    archive_path_obj = Path(archive_path)
    start_dt = _coerce_timestamp(start_time)
    end_dt = _coerce_timestamp(end_time)
    rejections = rejected_records if rejected_records is not None else []

    if not archive_path_obj.exists():
        rejections.append(
            ArchiveRecordRejection(
                reason_code="archive_file_missing",
                message=f"Archive file not found: {archive_path_obj}",
                manifest_id=manifest_entry.manifest_id,
                archive_uri=manifest_entry.archive_uri,
                file_offset=0,
                line_number=0,
            )
        )
        return

    for line_number, file_offset, raw_line in _iter_archive_lines(archive_path_obj):
        try:
            record = _parse_raw_envelope(
                raw_line,
                manifest_entry,
                file_offset=file_offset,
                line_number=line_number,
            )
        except ValueError as exc:
            rejections.append(
                ArchiveRecordRejection(
                    reason_code=_reason_code_from_error(exc),
                    message=str(exc),
                    manifest_id=manifest_entry.manifest_id,
                    archive_uri=manifest_entry.archive_uri,
                    file_offset=file_offset,
                    line_number=line_number,
                    raw_line=raw_line.rstrip("\n"),
                    exception_type=type(exc).__name__,
                )
            )
            continue

        event_dt = _coerce_timestamp(record.event_time)
        if start_dt <= event_dt < end_dt:
            yield record


def _parse_raw_envelope(
    raw_line: str,
    manifest_entry: ArchiveManifestEntry,
    *,
    file_offset: int,
    line_number: int,
) -> RawArchiveRecord:
    """Parse one raw archive line into a typed raw record.

    This function does not normalize business payloads. It only validates and
    attaches archive/manifest metadata needed by later replay steps.
    """

    try:
        payload = json.loads(raw_line)
    except json.JSONDecodeError as exc:
        raise ValueError(f"invalid_json_line: {exc.msg}") from exc

    if not isinstance(payload, dict):
        raise ValueError("invalid_raw_envelope_shape: expected object at top level")

    raw_event_uuid = _required_text(payload, ("event_uuid", "raw_event_uuid"))
    payload_json = _required_json(payload, ("payload_json", "payload"))
    ingest_time = _canonical_timestamp(
        _required_value(payload, ("ingest_time",)),
        field_name="ingest_time",
    )
    raw_event_time = payload.get("event_time")
    event_time = ingest_time
    event_time_source = "collector_observed"
    if raw_event_time not in {None, ""}:
        event_time = _canonical_timestamp(raw_event_time, field_name="event_time")
        event_time_source = "provider"

    source_system = _optional_text(payload, ("source_system",)) or manifest_entry.source_system
    if not source_system:
        raise ValueError("missing_required_field: source_system")

    source_endpoint = _optional_text(payload, ("source_endpoint",)) or manifest_entry.source_endpoint
    if not source_endpoint:
        raise ValueError("missing_required_field: source_endpoint")

    payload_hash = _required_text(payload, ("payload_hash",))
    collector_source = _optional_text(payload, ("collector_source",)) or manifest_entry.collector_source
    collector_version = _optional_text(payload, ("collector_version",)) or manifest_entry.collector_version
    raw_schema_version = _optional_text(payload, ("schema_version", "raw_schema_version"))
    if not raw_schema_version:
        raw_schema_version = manifest_entry.raw_schema_version

    return RawArchiveRecord(
        raw_event_uuid=raw_event_uuid,
        payload_json=payload_json,
        event_time=event_time,
        ingest_time=ingest_time,
        event_time_source=event_time_source,
        source_system=source_system,
        source_endpoint=source_endpoint,
        manifest_id=manifest_entry.manifest_id,
        archive_uri=manifest_entry.archive_uri,
        payload_hash=payload_hash,
        collector_source=collector_source,
        collector_version=collector_version,
        raw_schema_version=raw_schema_version,
        file_offset=file_offset,
        line_number=line_number,
        source_event_id=_optional_text(payload, ("source_event_id",)),
        market_id=_optional_text(payload, ("market_id",)),
        condition_id=_optional_text(payload, ("condition_id",)),
        asset_id=_optional_text(payload, ("asset_id",)),
        wallet_id=_optional_text(payload, ("wallet_id", "proxy_wallet")),
    )


def _load_manifest_entries(
    manifest_path: Path,
    *,
    start_time: datetime,
    end_time: datetime,
    source_system: str | None,
    source_endpoint: str | None,
    rejected_records: list[ArchiveRecordRejection],
) -> list[ArchiveManifestEntry]:
    """Load and filter manifest entries for the requested archive window."""

    if not manifest_path.exists():
        raise FileNotFoundError(f"Manifest file not found: {manifest_path}")

    entries: list[ArchiveManifestEntry] = []
    for line_number, file_offset, raw_line in _iter_archive_lines(manifest_path):
        if not raw_line.strip():
            continue

        try:
            raw_entry = json.loads(raw_line)
        except json.JSONDecodeError as exc:
            rejected_records.append(
                ArchiveRecordRejection(
                    reason_code="invalid_manifest_json",
                    message=f"Manifest JSON parse error: {exc.msg}",
                    manifest_id=None,
                    archive_uri=str(manifest_path),
                    file_offset=file_offset,
                    line_number=line_number,
                    raw_line=raw_line.rstrip("\n"),
                    exception_type=type(exc).__name__,
                )
            )
            continue

        if isinstance(raw_entry, list):
            for array_index, item in enumerate(raw_entry):
                _append_manifest_entry(
                    item,
                    manifest_path,
                    file_offset,
                    line_number,
                    array_index,
                    entries,
                    rejected_records,
                )
            continue

        _append_manifest_entry(
            raw_entry,
            manifest_path,
            file_offset,
            line_number,
            None,
            entries,
            rejected_records,
        )

    filtered_entries = [
        entry
        for entry in entries
        if _manifest_overlaps_window(entry, start_time, end_time)
        and (source_system is None or entry.source_system == source_system)
        and (source_endpoint is None or entry.source_endpoint == source_endpoint)
    ]
    filtered_entries.sort(key=lambda entry: entry.archive_uri)
    return filtered_entries


def _append_manifest_entry(
    raw_entry: Any,
    manifest_path: Path,
    file_offset: int,
    line_number: int,
    array_index: int | None,
    entries: list[ArchiveManifestEntry],
    rejected_records: list[ArchiveRecordRejection],
) -> None:
    """Parse one manifest object and append it or emit a structured rejection."""

    if not isinstance(raw_entry, dict):
        rejected_records.append(
            ArchiveRecordRejection(
                reason_code="invalid_manifest_shape",
                message="Manifest entry must be an object",
                manifest_id=None,
                archive_uri=str(manifest_path),
                file_offset=file_offset,
                line_number=line_number,
                raw_line=json.dumps(raw_entry),
            )
        )
        return

    try:
        entry = ArchiveManifestEntry(
            manifest_id=_required_text(raw_entry, ("manifest_id",)),
            archive_uri=_required_text(raw_entry, ("archive_uri",)),
            file_format=_required_text(raw_entry, ("file_format",)).lower(),
            compression=(_optional_text(raw_entry, ("compression",)) or _infer_compression(raw_entry)).lower(),
            source_system=_required_text(raw_entry, ("source_system",)),
            source_endpoint=_required_text(raw_entry, ("source_endpoint",)),
            collector_source=_optional_text(raw_entry, ("collector_source",)),
            collector_version=_optional_text(raw_entry, ("collector_version",)),
            raw_schema_version=_optional_text(raw_entry, ("raw_schema_version",)),
            window_start=_canonical_timestamp(
                _required_value(raw_entry, ("window_start",)),
                field_name="window_start",
            ),
            window_end=_canonical_timestamp(
                _required_value(raw_entry, ("window_end",)),
                field_name="window_end",
            ),
            row_count=_optional_int(raw_entry.get("row_count")),
            checksum=_optional_text(raw_entry, ("checksum",)),
        )
    except ValueError as exc:
        suffix = "" if array_index is None else f" (array_index={array_index})"
        rejected_records.append(
            ArchiveRecordRejection(
                reason_code="invalid_manifest_entry",
                message=f"{exc}{suffix}",
                manifest_id=raw_entry.get("manifest_id") if isinstance(raw_entry.get("manifest_id"), str) else None,
                archive_uri=str(raw_entry.get("archive_uri") or manifest_path),
                file_offset=file_offset,
                line_number=line_number,
                raw_line=json.dumps(raw_entry),
                exception_type=type(exc).__name__,
            )
        )
        return

    if entry.file_format != "ndjson":
        rejected_records.append(
            ArchiveRecordRejection(
                reason_code="unsupported_manifest_file_format",
                message=f"Unsupported archive file_format '{entry.file_format}'",
                manifest_id=entry.manifest_id,
                archive_uri=entry.archive_uri,
                file_offset=file_offset,
                line_number=line_number,
                raw_line=json.dumps(raw_entry),
            )
        )
        return

    if entry.compression not in {"none", "gzip"}:
        rejected_records.append(
            ArchiveRecordRejection(
                reason_code="unsupported_manifest_compression",
                message=f"Unsupported archive compression '{entry.compression}'",
                manifest_id=entry.manifest_id,
                archive_uri=entry.archive_uri,
                file_offset=file_offset,
                line_number=line_number,
                raw_line=json.dumps(raw_entry),
            )
        )
        return

    if _coerce_timestamp(entry.window_end) <= _coerce_timestamp(entry.window_start):
        rejected_records.append(
            ArchiveRecordRejection(
                reason_code="invalid_manifest_window",
                message="Manifest window_end must be greater than window_start",
                manifest_id=entry.manifest_id,
                archive_uri=entry.archive_uri,
                file_offset=file_offset,
                line_number=line_number,
                raw_line=json.dumps(raw_entry),
            )
        )
        return

    entries.append(entry)


def _iter_archive_lines(path: Path) -> Iterator[tuple[int, int, str]]:
    """Yield ``(line_number, file_offset, raw_line)`` from one archive file.

    Offsets are tracked against the decompressed byte stream. This keeps
    ordering deterministic for both plain NDJSON and gzipped NDJSON inputs.
    """

    if path.suffix.lower() == ".gz":
        handle = gzip.open(path, mode="rb")
    else:
        handle = path.open(mode="rb")

    with handle:
        file_offset = 0
        for line_number, raw_bytes in enumerate(handle, start=1):
            decoded_line = raw_bytes.decode("utf-8")
            yield line_number, file_offset, decoded_line
            file_offset += len(raw_bytes)


def _resolve_archive_path(archive_root: Path, archive_uri: str) -> Path:
    """Resolve a manifest archive URI against the configured archive root."""

    candidate = Path(archive_uri)
    if candidate.is_absolute():
        return candidate
    return archive_root / archive_uri


def _manifest_overlaps_window(
    entry: ArchiveManifestEntry,
    start_time: datetime,
    end_time: datetime,
) -> bool:
    """Return whether one manifest entry overlaps the requested time window."""

    entry_start = _coerce_timestamp(entry.window_start)
    entry_end = _coerce_timestamp(entry.window_end)
    return entry_end > start_time and entry_start < end_time


def _canonical_timestamp(value: Any, *, field_name: str) -> str:
    """Normalize one timestamp value to canonical RFC3339 UTC text."""

    try:
        return _coerce_timestamp(value).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    except Exception as exc:  # pragma: no cover - defensive normalization
        raise ValueError(f"invalid_timestamp: {field_name}") from exc


def _coerce_timestamp(value: str | datetime) -> datetime:
    """Parse a timestamp value into a timezone-aware UTC datetime."""

    if isinstance(value, datetime):
        dt = value
    else:
        normalized = str(value).strip()
        if not normalized:
            raise ValueError("Timestamp cannot be blank")
        normalized = normalized.replace("Z", "+00:00")
        dt = datetime.fromisoformat(normalized)

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _required_value(payload: dict[str, Any], field_names: tuple[str, ...]) -> Any:
    """Return the first present required value from a list of field names."""

    for field_name in field_names:
        if field_name in payload:
            value = payload[field_name]
            if value is None:
                continue
            if isinstance(value, str) and value == "":
                continue
            if isinstance(value, (list, dict, int, float, bool)) or value:
                return value
    raise ValueError(f"missing_required_field: {field_names[0]}")


def _required_text(payload: dict[str, Any], field_names: tuple[str, ...]) -> str:
    """Return one required non-blank text field from the provided payload."""

    value = _required_value(payload, field_names)
    text = str(value).strip()
    if not text:
        raise ValueError(f"missing_required_field: {field_names[0]}")
    return text


def _optional_text(payload: dict[str, Any], field_names: tuple[str, ...]) -> str | None:
    """Return one optional text field, normalized to stripped text or ``None``."""

    for field_name in field_names:
        if field_name not in payload:
            continue
        value = payload[field_name]
        if value is None:
            return None
        text = str(value).strip()
        return text or None
    return None


def _required_json(payload: dict[str, Any], field_names: tuple[str, ...]) -> Any:
    """Return one required JSON-compatible payload field."""

    value = _required_value(payload, field_names)
    if isinstance(value, str):
        try:
            return json.loads(value)
        except json.JSONDecodeError:
            return value
    return value


def _optional_int(value: Any) -> int | None:
    """Return an integer when possible, else ``None``."""

    if value in {None, ""}:
        return None
    try:
        return int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError("invalid_manifest_row_count") from exc


def _infer_compression(raw_entry: dict[str, Any]) -> str:
    """Infer manifest compression from archive URI when not provided."""

    archive_uri = str(raw_entry.get("archive_uri") or "").lower()
    if archive_uri.endswith(".gz"):
        return "gzip"
    return "none"


def _reason_code_from_error(exc: ValueError) -> str:
    """Map one parsing error message to a stable rejection reason code."""

    message = str(exc)
    if message.startswith("invalid_json_line:"):
        return "invalid_raw_json"
    if message.startswith("invalid_raw_envelope_shape:"):
        return "invalid_raw_envelope_shape"
    if message.startswith("missing_required_field:"):
        return "missing_required_field"
    if message.startswith("invalid_timestamp:"):
        return "invalid_timestamp"
    return "raw_record_parse_error"
