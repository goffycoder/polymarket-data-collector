"""CLI entrypoint for Person 2 Phase 1 validation.

Usage:
    python -m validation.run_phase1_validation
    python -m validation.run_phase1_validation --db path/to/db.sqlite
"""

from __future__ import annotations

import argparse
import json

from validation.phase1_validators import run_phase1_validation


def build_parser() -> argparse.ArgumentParser:
    """Create the command-line interface for the Phase 1 validator."""

    parser = argparse.ArgumentParser(description="Run Person 2 Phase 1 validation checks.")
    parser.add_argument(
        "--db",
        default="database/polymarket_state.db",
        help="Path to the SQLite database to validate.",
    )
    parser.add_argument(
        "--config",
        default="config/phase1_validation.yaml",
        help="Path to the Phase 1 validation contract file.",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Emit findings as JSON instead of text.",
    )
    return parser


def main() -> int:
    """Run the configured Phase 1 validation checks and print the results."""

    parser = build_parser()
    args = parser.parse_args()
    summary = run_phase1_validation(db_path=args.db, config_path=args.config)

    if args.json:
        payload = {
            "run_label": summary.run_label,
            "overall_status": summary.overall_status(),
            "status_counts": summary.counts_by_status(),
            "aggregate_report": summary.aggregate_report,
            "findings": [
                {
                    "check_name": finding.check_name,
                    "status": finding.status,
                    "severity": finding.severity,
                    "message": finding.message,
                    "reason_code": finding.reason_code,
                    "sample_identifier": finding.sample_identifier,
                    "metrics": finding.metrics,
                }
                for finding in summary.findings
            ],
        }
        print(json.dumps(payload, indent=2, sort_keys=True))
    else:
        print(summary.render_text())

    return 0 if summary.overall_status() != "fail" else 1


if __name__ == "__main__":
    raise SystemExit(main())
