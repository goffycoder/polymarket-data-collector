"""CLI entrypoint for generating Person 2 Phase 1 QA review sheets."""

from __future__ import annotations

import argparse
import sqlite3
from pathlib import Path

from validation.phase1_qa import generate_phase1_qa_samples, write_phase1_qa_csv
from validation.phase1_report import ValidationSummary
from validation.phase1_validators import _read_yaml
from validation.phase1_validators import ValidationRuntime, load_phase1_validation_contract


def build_parser() -> argparse.ArgumentParser:
    """Create the command-line interface for QA sample generation."""

    parser = argparse.ArgumentParser(description="Generate a Phase 1 QA episode review sheet.")
    parser.add_argument(
        "--db",
        default="database/polymarket_state.db",
        help="Path to the SQLite database to sample from.",
    )
    parser.add_argument(
        "--config",
        default="config/phase1_validation.yaml",
        help="Path to the Phase 1 validation contract file.",
    )
    parser.add_argument(
        "--output",
        default=None,
        help="Optional CSV output path override.",
    )
    parser.add_argument(
        "--sample-size",
        type=int,
        default=None,
        help="Optional QA sample size override.",
    )
    return parser


def main() -> int:
    """Generate a manual QA review sheet and print a compact status summary."""

    parser = build_parser()
    args = parser.parse_args()
    contract = load_phase1_validation_contract(args.config)
    raw_config = _read_yaml(Path(args.config))
    runtime = ValidationRuntime(
        contract=contract,
        db_path=Path(args.db),
        config_path=Path(args.config),
    )
    summary = ValidationSummary(run_label="phase1_qa_generation")

    if not runtime.db_path.exists():
        summary.add(
            "qa_generation",
            "fail",
            "error",
            f"Database file not found: {runtime.db_path}",
            reason_code="missing_database",
        )
        print(summary.render_text())
        return 1

    conn = sqlite3.connect(runtime.db_path)
    conn.row_factory = sqlite3.Row
    try:
        sample_rows = generate_phase1_qa_samples(
            conn,
            runtime,
            summary,
            sample_size=args.sample_size,
        )
    finally:
        conn.close()

    if not sample_rows:
        summary.add(
            "qa_generation",
            "warn",
            "warn",
            "No QA samples were generated from the current database and configuration.",
            reason_code="no_qa_samples",
        )
        print(summary.render_text())
        return 0

    output_path = args.output or raw_config.get("qa", {}).get(
        "output_csv",
        "Documentation/person2Phases/phase1_qa_sample_review.csv",
    )
    written_path = write_phase1_qa_csv(output_path, sample_rows)
    summary.add(
        "qa_generation",
        "pass",
        "info",
        f"Generated Phase 1 QA review sheet: {written_path}",
        metrics={"sample_count": len(sample_rows)},
    )
    print(summary.render_text())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
