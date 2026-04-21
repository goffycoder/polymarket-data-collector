from __future__ import annotations

import argparse
import json

from database.db_manager import apply_schema
from validation.phase5_person2_report import build_phase5_person2_report


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Build the Phase 5 Person 2 historical validation and paper-trading report."
    )
    parser.add_argument("--start", required=True, help="Inclusive ISO timestamp for the report window.")
    parser.add_argument("--end", required=True, help="Exclusive ISO timestamp for the report window.")
    parser.add_argument("--json", action="store_true", help="Emit machine-readable JSON.")
    return parser


def _render_text(payload: dict) -> str:
    assessment = payload["assessment"]
    coverage = payload["coverage_summary"]
    metrics = payload["metrics"]
    return "\n".join(
        [
            f"Report version: {payload['report_version']}",
            f"Assessment status: {assessment['status']}",
            f"Window: {payload['start']} -> {payload['end']}",
            f"Evaluation rows: {payload['evaluation_row_count']}",
            f"Alert rows: {payload['alert_row_count']}",
            f"Paper trades: {payload['paper_trade_count']}",
            f"Coverage partial rows: {coverage['rows_partial']}",
            f"Coverage insufficient rows: {coverage['rows_coverage_insufficient']}",
            f"Candidate precision: {metrics['candidate_overall']['candidate_precision']}",
            f"Alert usefulness precision: {metrics['alert_overall']['alert_usefulness_precision']}",
            f"Median lead time seconds: {metrics['lead_time_overall']['median_lead_time_seconds']}",
            f"Median bounded PnL: {metrics['paper_trade_overall']['median_bounded_pnl']}",
            f"Skip due to data rate: {metrics['failure_overall']['skip_due_to_data_rate']}",
        ]
    )


def main() -> int:
    args = build_parser().parse_args()
    apply_schema()
    payload = build_phase5_person2_report(start=args.start, end=args.end).to_dict()
    if args.json:
        print(json.dumps(payload, indent=2, sort_keys=True, default=str))
    else:
        print(_render_text(payload))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

