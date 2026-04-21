from __future__ import annotations

import argparse
import json

from database.db_manager import apply_schema
from validation.phase4_gate4_report import build_phase4_gate4_report


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Build one combined Gate 4 evidence report for the Phase 4 alert loop."
    )
    parser.add_argument("--json", action="store_true", help="Emit machine-readable JSON.")
    return parser


def _render_text(payload: dict) -> str:
    assessment = payload["assessment"]
    alert_summary = payload["alert_summary"]
    delivery_summary = payload["delivery_summary"]
    analyst_summary = payload["analyst_summary"]
    return "\n".join(
        [
            f"Workflow version: {payload['workflow_version']}",
            f"Assessment status: {assessment['status']}",
            f"Total alerts: {alert_summary['total_alerts']}",
            f"Suppressed alerts: {alert_summary['suppressed_alerts']}",
            f"Delivery attempts: {delivery_summary['total_attempts']}",
            f"Sent attempts: {delivery_summary['sent_attempts']}",
            f"Skipped attempts: {delivery_summary['skipped_attempts']}",
            f"Error attempts: {delivery_summary['error_attempts']}",
            f"Analyst feedback rows: {analyst_summary['total_feedback_rows']}",
        ]
    )


def main() -> int:
    args = build_parser().parse_args()
    apply_schema()
    payload = build_phase4_gate4_report().to_dict()
    if args.json:
        print(json.dumps(payload, indent=2, sort_keys=True))
    else:
        print(_render_text(payload))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
