"""Deterministic replay ordering engine for Person 2 Phase 2.

This module operates strictly on already reconstructed normalized envelopes.
Its responsibilities are limited to:
- applying the contract-defined replay ordering tuple,
- preserving all records without mutation,
- validating the resulting order,
- reporting duplicate envelope identifiers for idempotency preparation.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Iterable


@dataclass(slots=True)
class ReplayMetadata:
    """Represent the replay-ordering metadata for one replay run."""

    total_records: int
    duplicate_envelope_ids: list[str]
    ordering_validation_passed: bool


def _replay_envelopes(envelopes: Iterable[dict[str, Any]]) -> tuple[list[dict[str, Any]], ReplayMetadata]:
    """Return envelopes in deterministic replay order plus replay metadata."""

    input_envelopes = list(envelopes)
    ordered_envelopes = _apply_replay_ordering(input_envelopes)
    duplicate_envelope_ids = _find_duplicate_envelope_ids(ordered_envelopes)
    ordering_validation_passed = _validate_replay_order(
        ordered_envelopes,
        original_count=len(input_envelopes),
        duplicate_envelope_ids=duplicate_envelope_ids,
    )

    metadata = ReplayMetadata(
        total_records=len(ordered_envelopes),
        duplicate_envelope_ids=duplicate_envelope_ids,
        ordering_validation_passed=ordering_validation_passed,
    )
    return ordered_envelopes, metadata


def _sort_envelopes_deterministically(envelopes: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    """Sort envelopes by the exact contract-defined ordering tuple."""

    return sorted(envelopes, key=_ordering_key)


def _apply_replay_ordering(envelopes: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    """Apply deterministic replay ordering without mutating envelope content."""

    return _sort_envelopes_deterministically(envelopes)


def _validate_replay_order(
    ordered_envelopes: list[dict[str, Any]],
    *,
    original_count: int | None = None,
    duplicate_envelope_ids: list[str] | None = None,
) -> bool:
    """Validate ordering correctness, record preservation, and duplicate status."""

    if original_count is not None and len(ordered_envelopes) != original_count:
        return False

    for index in range(1, len(ordered_envelopes)):
        previous_key = _ordering_key(ordered_envelopes[index - 1])
        current_key = _ordering_key(ordered_envelopes[index])
        if previous_key > current_key:
            return False

    if duplicate_envelope_ids:
        return False

    return True


def _ordering_key(envelope: dict[str, Any]) -> tuple[Any, ...]:
    """Return the exact replay ordering tuple defined by the contract."""

    return (
        _parse_timestamp(_required_field(envelope, "event_time")),
        _parse_timestamp(_required_field(envelope, "ingest_time")),
        _required_int(envelope, "source_priority"),
        _required_text(envelope, "source_endpoint"),
        _required_text(envelope, "raw_event_uuid"),
        _required_int(envelope, "record_index"),
    )


def _find_duplicate_envelope_ids(envelopes: Iterable[dict[str, Any]]) -> list[str]:
    """Return duplicate envelope IDs in first-duplicate encounter order."""

    seen: set[str] = set()
    duplicates: list[str] = []
    duplicate_set: set[str] = set()
    for envelope in envelopes:
        envelope_id = _required_text(envelope, "envelope_id")
        if envelope_id in seen and envelope_id not in duplicate_set:
            duplicates.append(envelope_id)
            duplicate_set.add(envelope_id)
            continue
        seen.add(envelope_id)
    return duplicates


def _required_field(envelope: dict[str, Any], field_name: str) -> Any:
    """Return one required envelope field or raise a replay-ordering error."""

    if field_name not in envelope or envelope[field_name] is None:
        raise ValueError(f"missing_required_field: {field_name}")
    return envelope[field_name]


def _required_text(envelope: dict[str, Any], field_name: str) -> str:
    """Return one required non-blank text envelope field."""

    value = str(_required_field(envelope, field_name)).strip()
    if not value:
        raise ValueError(f"missing_required_field: {field_name}")
    return value


def _required_int(envelope: dict[str, Any], field_name: str) -> int:
    """Return one required integer envelope field."""

    value = _required_field(envelope, field_name)
    try:
        return int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"invalid_integer_field: {field_name}") from exc


def _parse_timestamp(value: Any) -> datetime:
    """Parse one canonical RFC3339 UTC timestamp for replay ordering."""

    normalized = str(value).strip()
    if not normalized:
        raise ValueError("invalid_timestamp: blank")
    normalized = normalized.replace("Z", "+00:00")
    dt = datetime.fromisoformat(normalized)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)
