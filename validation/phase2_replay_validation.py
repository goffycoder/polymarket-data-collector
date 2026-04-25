"""Replay-versus-original validation for Person 2 Phase 2.

This module compares replayed normalized envelopes against the raw archive
records and archive manifest metadata. It only detects and reports issues; it
does not modify replay data or attempt to fix discrepancies automatically.
"""

from __future__ import annotations

from collections import Counter
from typing import Any, Iterable

from validation.phase2_archive_reader import ArchiveManifestEntry, RawArchiveRecord
from validation.phase2_envelope_reconstruction import ReconstructionRejection
from validation.phase2_replay_engine import ReplayMetadata, _validate_replay_order


def _validate_replay_output(
    replayed_envelopes: Iterable[dict[str, Any]],
    raw_records: Iterable[RawArchiveRecord],
    manifest: Iterable[ArchiveManifestEntry],
    *,
    reconstruction_rejections: Iterable[ReconstructionRejection] | None = None,
    replay_metadata: ReplayMetadata | None = None,
) -> dict[str, Any]:
    """Validate replay output against raw-record and manifest expectations."""

    replayed_envelope_list = list(replayed_envelopes)
    raw_record_list = list(raw_records)
    manifest_entries = list(manifest)
    reconstruction_rejection_list = list(reconstruction_rejections or [])

    manifest_result = _compare_with_manifest(raw_record_list, manifest_entries)
    metrics = _compute_replay_metrics(
        replayed_envelope_list,
        raw_record_list,
        manifest_entries,
        reconstruction_rejections=reconstruction_rejection_list,
        replay_metadata=replay_metadata,
        manifest_result=manifest_result,
    )
    discrepancies = _detect_replay_discrepancies(
        replayed_envelope_list,
        raw_record_list,
        manifest_entries,
        reconstruction_rejections=reconstruction_rejection_list,
        replay_metadata=replay_metadata,
        manifest_result=manifest_result,
        metrics=metrics,
    )
    status = _determine_status(metrics, discrepancies, manifest_result)

    return {
        "status": status,
        "metrics": metrics,
        "discrepancies": discrepancies,
        "summary": {
            "total_manifest_files": len(manifest_entries),
            "manifest_validation_passed": not manifest_result["has_mismatch"],
            "ordering_validation_passed": metrics["ordering_validation_passed"],
            "discrepancy_reason_counts": dict(Counter(item["reason_code"] for item in discrepancies)),
        },
    }


def _compute_replay_metrics(
    replayed_envelopes: Iterable[dict[str, Any]],
    raw_records: Iterable[RawArchiveRecord],
    manifest: Iterable[ArchiveManifestEntry],
    *,
    reconstruction_rejections: Iterable[ReconstructionRejection] | None = None,
    replay_metadata: ReplayMetadata | None = None,
    manifest_result: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Compute replay-validation counts and metadata-backed metrics."""

    replayed_envelope_list = list(replayed_envelopes)
    raw_record_list = list(raw_records)
    manifest_entries = list(manifest)
    reconstruction_rejection_list = list(reconstruction_rejections or [])
    manifest_comparison = manifest_result or _compare_with_manifest(raw_record_list, manifest_entries)

    raw_event_uuids = {record.raw_event_uuid for record in raw_record_list}
    replayed_raw_event_uuids = {str(envelope.get("raw_event_uuid")) for envelope in replayed_envelope_list}
    rejection_raw_event_uuids = {rejection.raw_event_uuid for rejection in reconstruction_rejection_list}

    missing_envelope_raw_event_uuids = sorted(
        raw_event_uuid
        for raw_event_uuid in raw_event_uuids
        if raw_event_uuid not in replayed_raw_event_uuids and raw_event_uuid not in rejection_raw_event_uuids
    )
    extra_envelopes = [
        envelope
        for envelope in replayed_envelope_list
        if str(envelope.get("raw_event_uuid")) not in raw_event_uuids
    ]

    duplicate_envelope_ids = (
        list(replay_metadata.duplicate_envelope_ids)
        if replay_metadata is not None
        else _duplicate_envelope_ids(replayed_envelope_list)
    )
    ordering_validation_passed = (
        replay_metadata.ordering_validation_passed
        if replay_metadata is not None
        else _validate_replay_order(
            replayed_envelope_list,
            original_count=len(replayed_envelope_list),
            duplicate_envelope_ids=duplicate_envelope_ids,
        )
    )

    return {
        "total_raw_records": len(raw_record_list),
        "total_replayed_envelopes": len(replayed_envelope_list),
        "reconstruction_success_count": len(replayed_raw_event_uuids.intersection(raw_event_uuids)),
        "reconstruction_failure_count": len(reconstruction_rejection_list),
        "replay_duplicate_envelope_ids": duplicate_envelope_ids,
        "missing_envelopes_count": len(missing_envelope_raw_event_uuids),
        "extra_envelopes_count": len(extra_envelopes),
        "ordering_validation_passed": ordering_validation_passed,
        "manifest_expected_total_records": manifest_comparison["expected_total_records"],
        "manifest_actual_total_records": manifest_comparison["actual_total_records"],
        "manifest_missing_files_count": len(manifest_comparison["missing_files"]),
        "manifest_incomplete_partitions_count": len(manifest_comparison["incomplete_partitions"]),
        "missing_envelope_raw_event_uuids": missing_envelope_raw_event_uuids,
    }


def _compare_with_manifest(
    raw_records: Iterable[RawArchiveRecord],
    manifest: Iterable[ArchiveManifestEntry],
) -> dict[str, Any]:
    """Compare raw-record presence and counts against manifest expectations."""

    raw_record_list = list(raw_records)
    manifest_entries = list(manifest)

    actual_counts = Counter(record.archive_uri for record in raw_record_list)
    expected_counts = {
        entry.archive_uri: (entry.row_count if entry.row_count is not None else actual_counts.get(entry.archive_uri, 0))
        for entry in manifest_entries
    }

    missing_files: list[dict[str, Any]] = []
    incomplete_partitions: list[dict[str, Any]] = []
    unexpected_files: list[dict[str, Any]] = []

    manifest_uris = set(expected_counts)
    for entry in manifest_entries:
        actual_count = actual_counts.get(entry.archive_uri, 0)
        expected_count = expected_counts[entry.archive_uri]
        if actual_count == 0:
            missing_files.append(
                {
                    "manifest_id": entry.manifest_id,
                    "archive_uri": entry.archive_uri,
                    "expected_count": expected_count,
                    "actual_count": actual_count,
                }
            )
        elif actual_count != expected_count:
            incomplete_partitions.append(
                {
                    "manifest_id": entry.manifest_id,
                    "archive_uri": entry.archive_uri,
                    "expected_count": expected_count,
                    "actual_count": actual_count,
                }
            )

    for archive_uri, actual_count in actual_counts.items():
        if archive_uri not in manifest_uris:
            unexpected_files.append(
                {
                    "archive_uri": archive_uri,
                    "expected_count": 0,
                    "actual_count": actual_count,
                }
            )

    expected_total_records = sum(expected_counts.values())
    actual_total_records = len(raw_record_list)
    has_mismatch = bool(
        missing_files
        or incomplete_partitions
        or unexpected_files
        or expected_total_records != actual_total_records
    )

    return {
        "expected_counts_by_archive": expected_counts,
        "actual_counts_by_archive": dict(actual_counts),
        "expected_total_records": expected_total_records,
        "actual_total_records": actual_total_records,
        "missing_files": missing_files,
        "incomplete_partitions": incomplete_partitions,
        "unexpected_files": unexpected_files,
        "has_mismatch": has_mismatch,
    }


def _detect_replay_discrepancies(
    replayed_envelopes: Iterable[dict[str, Any]],
    raw_records: Iterable[RawArchiveRecord],
    manifest: Iterable[ArchiveManifestEntry],
    *,
    reconstruction_rejections: Iterable[ReconstructionRejection] | None = None,
    replay_metadata: ReplayMetadata | None = None,
    manifest_result: dict[str, Any] | None = None,
    metrics: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    """Detect and classify replay discrepancies with stable reason codes."""

    replayed_envelope_list = list(replayed_envelopes)
    raw_record_list = list(raw_records)
    manifest_entries = list(manifest)
    reconstruction_rejection_list = list(reconstruction_rejections or [])
    manifest_comparison = manifest_result or _compare_with_manifest(raw_record_list, manifest_entries)
    computed_metrics = metrics or _compute_replay_metrics(
        replayed_envelope_list,
        raw_record_list,
        manifest_entries,
        reconstruction_rejections=reconstruction_rejection_list,
        replay_metadata=replay_metadata,
        manifest_result=manifest_comparison,
    )

    discrepancies: list[dict[str, Any]] = []

    for rejection in reconstruction_rejection_list:
        discrepancies.append(
            {
                "reason_code": "reconstruction_failure",
                "severity": "warn",
                "raw_event_uuid": rejection.raw_event_uuid,
                "manifest_id": rejection.manifest_id,
                "archive_uri": rejection.archive_uri,
                "record_index": rejection.record_index,
                "message": rejection.message,
            }
        )

    for raw_event_uuid in computed_metrics["missing_envelope_raw_event_uuids"]:
        discrepancies.append(
            {
                "reason_code": "missing_envelope",
                "severity": "error",
                "raw_event_uuid": raw_event_uuid,
                "message": "Raw record did not produce an envelope or reconstruction rejection.",
            }
        )

    raw_event_uuids = {record.raw_event_uuid for record in raw_record_list}
    for envelope in replayed_envelope_list:
        raw_event_uuid = str(envelope.get("raw_event_uuid"))
        if raw_event_uuid not in raw_event_uuids:
            discrepancies.append(
                {
                    "reason_code": "extra_envelope",
                    "severity": "warn",
                    "envelope_id": envelope.get("envelope_id"),
                    "raw_event_uuid": raw_event_uuid,
                    "message": "Replayed envelope has no matching raw archive record.",
                }
            )

    duplicate_envelope_ids = (
        list(replay_metadata.duplicate_envelope_ids)
        if replay_metadata is not None
        else computed_metrics["replay_duplicate_envelope_ids"]
    )
    for envelope_id in duplicate_envelope_ids:
        discrepancies.append(
            {
                "reason_code": "duplicate_envelope_id",
                "severity": "error",
                "envelope_id": envelope_id,
                "message": "Duplicate envelope_id observed during replay validation.",
            }
        )

    for mismatch in manifest_comparison["missing_files"]:
        discrepancies.append(
            {
                "reason_code": "manifest_mismatch",
                "severity": "error",
                "archive_uri": mismatch["archive_uri"],
                "manifest_id": mismatch["manifest_id"],
                "expected_count": mismatch["expected_count"],
                "actual_count": mismatch["actual_count"],
                "message": "Manifest file is missing from the raw replay input.",
            }
        )

    for mismatch in manifest_comparison["incomplete_partitions"]:
        discrepancies.append(
            {
                "reason_code": "manifest_mismatch",
                "severity": "error",
                "archive_uri": mismatch["archive_uri"],
                "manifest_id": mismatch["manifest_id"],
                "expected_count": mismatch["expected_count"],
                "actual_count": mismatch["actual_count"],
                "message": "Manifest partition count does not match the loaded raw-record count.",
            }
        )

    for mismatch in manifest_comparison["unexpected_files"]:
        discrepancies.append(
            {
                "reason_code": "manifest_mismatch",
                "severity": "error",
                "archive_uri": mismatch["archive_uri"],
                "expected_count": mismatch["expected_count"],
                "actual_count": mismatch["actual_count"],
                "message": "Loaded raw records from an archive file not present in the manifest.",
            }
        )

    if manifest_comparison["expected_total_records"] != manifest_comparison["actual_total_records"]:
        discrepancies.append(
            {
                "reason_code": "manifest_mismatch",
                "severity": "error",
                "expected_total_records": manifest_comparison["expected_total_records"],
                "actual_total_records": manifest_comparison["actual_total_records"],
                "message": "Manifest total row count does not match the loaded raw-record total.",
            }
        )

    return discrepancies


def _determine_status(
    metrics: dict[str, Any],
    discrepancies: list[dict[str, Any]],
    manifest_result: dict[str, Any],
) -> str:
    """Resolve replay validation status using the requested pass/warn/fail rules."""

    has_manifest_mismatch = manifest_result["has_mismatch"] or any(
        item["reason_code"] == "manifest_mismatch" for item in discrepancies
    )
    has_duplicate = bool(metrics["replay_duplicate_envelope_ids"])
    has_missing = metrics["missing_envelopes_count"] > 0
    has_ordering_failure = not metrics["ordering_validation_passed"]

    if has_missing or has_manifest_mismatch or has_duplicate or has_ordering_failure:
        return "fail"
    if metrics["reconstruction_failure_count"] > 0 or metrics["extra_envelopes_count"] > 0:
        return "warn"
    return "pass"


def _duplicate_envelope_ids(replayed_envelopes: Iterable[dict[str, Any]]) -> list[str]:
    """Return duplicate envelope IDs in first-duplicate encounter order."""

    seen: set[str] = set()
    duplicates: list[str] = []
    duplicate_set: set[str] = set()
    for envelope in replayed_envelopes:
        envelope_id = str(envelope.get("envelope_id") or "").strip()
        if not envelope_id:
            continue
        if envelope_id in seen and envelope_id not in duplicate_set:
            duplicates.append(envelope_id)
            duplicate_set.add(envelope_id)
            continue
        seen.add(envelope_id)
    return duplicates
