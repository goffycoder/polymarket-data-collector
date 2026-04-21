from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any

from database.db_manager import get_conn
from phase3.detector import PHASE3_DETECTOR_VERSION, PHASE3_FEATURE_SCHEMA_VERSION
from validation.phase3_candidate_report import Phase3CandidateReport, build_phase3_candidate_report
from validation.phase3_reconciliation import (
    Phase3ReconciliationReport,
    build_phase3_reconciliation_report,
)


@dataclass(slots=True)
class Phase3Gate3Report:
    start: str
    end: str
    detector_version: str
    feature_schema_version: str
    detector_registration: dict[str, Any] | None
    candidate_report: dict[str, Any]
    reconciliation_report: dict[str, Any]
    assessment: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def _load_detector_registration() -> dict[str, Any] | None:
    conn = get_conn()
    try:
        row = conn.execute(
            """
            SELECT detector_version, feature_schema_version, state_backend, notes, created_at, last_used_at
            FROM detector_versions
            WHERE detector_version = ?
            """,
            (PHASE3_DETECTOR_VERSION,),
        ).fetchone()
    finally:
        conn.close()
    if row is None:
        return None
    return {
        "detector_version": row["detector_version"],
        "feature_schema_version": row["feature_schema_version"],
        "state_backend": row["state_backend"],
        "notes": row["notes"],
        "created_at": row["created_at"],
        "last_used_at": row["last_used_at"],
    }


def _assessment(
    candidate_report: Phase3CandidateReport,
    reconciliation_report: Phase3ReconciliationReport,
) -> dict[str, Any]:
    persisted = candidate_report.total_candidates
    replayed = reconciliation_report.replay_candidate_count
    mismatches = len(reconciliation_report.missing_from_replay) + len(reconciliation_report.extra_in_replay)

    if persisted == 0 and replayed == 0:
        status = "no_candidates_in_window"
    elif mismatches == 0:
        status = "aligned"
    else:
        status = "mismatch_detected"

    return {
        "status": status,
        "persisted_candidate_count": persisted,
        "replay_candidate_count": replayed,
        "signature_mismatch_count": mismatches,
        "top_rule_family": (
            candidate_report.rule_family_counts[0]["rule_family"]
            if candidate_report.rule_family_counts
            else None
        ),
    }


async def build_phase3_gate3_report(*, start: str, end: str) -> Phase3Gate3Report:
    candidate_report = build_phase3_candidate_report(start=start, end=end)
    reconciliation_report = await build_phase3_reconciliation_report(start=start, end=end)
    detector_registration = _load_detector_registration()

    return Phase3Gate3Report(
        start=start,
        end=end,
        detector_version=PHASE3_DETECTOR_VERSION,
        feature_schema_version=PHASE3_FEATURE_SCHEMA_VERSION,
        detector_registration=detector_registration,
        candidate_report=candidate_report.to_dict(),
        reconciliation_report=reconciliation_report.to_dict(),
        assessment=_assessment(candidate_report, reconciliation_report),
    )
