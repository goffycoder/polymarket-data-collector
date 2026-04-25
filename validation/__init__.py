"""Validation package for Person 2 Phase 1 data-quality work.

This package is intentionally isolated from the collector modules so validation
logic can evolve without coupling QA workflows to ingestion code.
"""

from validation.phase1_report import ValidationFinding, ValidationSummary
from validation.phase1_qa import Phase1QASampleRow, generate_phase1_qa_samples, write_phase1_qa_csv
from validation.phase1_semantics import (
    EpisodeLinkageRecord,
    FreshWalletRecord,
    WalletFirstSeenRecord,
)
from validation.phase1_validators import (
    FieldRule,
    Phase1ValidationContract,
    load_phase1_validation_contract,
    run_phase1_validation,
)
from validation.phase2_archive_reader import (
    ArchiveManifestEntry,
    ArchiveRecordRejection,
    RawArchiveRecord,
)
from validation.phase2_envelope_reconstruction import ReconstructionRejection
from validation.phase2_replay_engine import ReplayMetadata
from validation.phase2_replay_validation import (
    _compare_with_manifest,
    _compute_replay_metrics,
    _detect_replay_discrepancies,
    _validate_replay_output,
)
from validation.phase2_case_study import run_phase2_replay_case_study

__all__ = [
    "FieldRule",
    "Phase1ValidationContract",
    "WalletFirstSeenRecord",
    "FreshWalletRecord",
    "EpisodeLinkageRecord",
    "Phase1QASampleRow",
    "ArchiveManifestEntry",
    "ArchiveRecordRejection",
    "RawArchiveRecord",
    "ReconstructionRejection",
    "ReplayMetadata",
    "_compare_with_manifest",
    "_compute_replay_metrics",
    "_detect_replay_discrepancies",
    "_validate_replay_output",
    "run_phase2_replay_case_study",
    "ValidationFinding",
    "ValidationSummary",
    "generate_phase1_qa_samples",
    "load_phase1_validation_contract",
    "run_phase1_validation",
    "write_phase1_qa_csv",
]
