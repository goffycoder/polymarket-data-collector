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

__all__ = [
    "FieldRule",
    "Phase1ValidationContract",
    "WalletFirstSeenRecord",
    "FreshWalletRecord",
    "EpisodeLinkageRecord",
    "Phase1QASampleRow",
    "ValidationFinding",
    "ValidationSummary",
    "generate_phase1_qa_samples",
    "load_phase1_validation_contract",
    "run_phase1_validation",
    "write_phase1_qa_csv",
]
