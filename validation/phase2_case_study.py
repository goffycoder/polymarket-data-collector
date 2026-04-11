"""Gate 2 replay case-study runner for Person 2 Phase 2."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any

from validation.phase2_archive_reader import _coerce_timestamp, _load_archive_window, _load_manifest_entries
from validation.phase2_envelope_reconstruction import _reconstruct_envelopes
from validation.phase2_replay_engine import _replay_envelopes
from validation.phase2_replay_validation import _validate_replay_output


DEFAULT_CASE_STUDY_START = "2026-04-10T14:00:00.000000Z"
DEFAULT_CASE_STUDY_END = "2026-04-10T15:00:00.000000Z"


def run_phase2_replay_case_study(
    archive_root: str | Path,
    manifest_path: str | Path,
    *,
    start_time: str = DEFAULT_CASE_STUDY_START,
    end_time: str = DEFAULT_CASE_STUDY_END,
    source_system: str | None = None,
    source_endpoint: str | None = None,
    sample_size: int = 5,
    save_output: bool = False,
    output_path: str | Path | None = None,
) -> dict[str, Any]:
    """Run the full Phase 2 replay case study and return an evidence pack."""

    start_dt = _coerce_timestamp(start_time)
    end_dt = _coerce_timestamp(end_time)
    if end_dt <= start_dt:
        raise ValueError("Case study end_time must be greater than start_time")

    raw_records_iter, archive_rejections = _load_archive_window(
        archive_root,
        manifest_path,
        start_time=start_dt,
        end_time=end_dt,
        source_system=source_system,
        source_endpoint=source_endpoint,
    )
    raw_records = list(raw_records_iter)

    manifest_entries = _load_manifest_entries(
        Path(manifest_path),
        start_time=start_dt,
        end_time=end_dt,
        source_system=source_system,
        source_endpoint=source_endpoint,
        rejected_records=[],
    )

    reconstructed_envelopes_iter, reconstruction_rejections = _reconstruct_envelopes(raw_records)
    reconstructed_envelopes = list(reconstructed_envelopes_iter)

    replayed_envelopes, replay_metadata = _replay_envelopes(reconstructed_envelopes)
    validation_report = _validate_replay_output(
        replayed_envelopes,
        raw_records,
        manifest_entries,
        reconstruction_rejections=reconstruction_rejections,
        replay_metadata=replay_metadata,
    )

    evidence_pack = _serialize_case_study_payload(
        {
            "case_study_window": {
                "start_time": start_dt.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
                "end_time": end_dt.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
                "duration_hours": (end_dt - start_dt).total_seconds() / 3600.0,
                "source_system": source_system,
                "source_endpoint": source_endpoint,
            },
            "input_summary": {
                "manifest_path": str(manifest_path),
                "archive_root": str(archive_root),
                "manifest_entries_count": len(manifest_entries),
                "archive_rejections_count": len(archive_rejections),
                "total_raw_records": len(raw_records),
            },
            "replay_summary": {
                "total_reconstructed_envelopes": len(reconstructed_envelopes),
                "total_replayed_envelopes": len(replayed_envelopes),
                "replay_metadata": {
                    "total_records": replay_metadata.total_records,
                    "duplicate_envelope_ids": replay_metadata.duplicate_envelope_ids,
                    "ordering_validation_passed": replay_metadata.ordering_validation_passed,
                },
            },
            "validation_report": validation_report,
            "sample_envelopes": replayed_envelopes[:sample_size],
            "sample_discrepancies": validation_report["discrepancies"][:sample_size],
        }
    )

    if save_output:
        target_path = _resolve_output_path(output_path)
        target_path.parent.mkdir(parents=True, exist_ok=True)
        target_path.write_text(
            json.dumps(evidence_pack, indent=2, sort_keys=True),
            encoding="utf-8",
        )

    return evidence_pack


def _serialize_case_study_payload(value: Any) -> Any:
    """Convert the case-study payload into a stable JSON-safe structure."""

    if isinstance(value, Decimal):
        return format(value, "f")
    if isinstance(value, dict):
        return {str(key): _serialize_case_study_payload(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_serialize_case_study_payload(item) for item in value]
    if isinstance(value, tuple):
        return [_serialize_case_study_payload(item) for item in value]
    return value


def _resolve_output_path(output_path: str | Path | None) -> Path:
    """Resolve the optional case-study output path."""

    if output_path is not None:
        return Path(output_path)

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return Path("outputs") / f"phase2_case_study_{timestamp}.json"
