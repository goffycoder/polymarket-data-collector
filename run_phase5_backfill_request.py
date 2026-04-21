from __future__ import annotations

import argparse
import json

from database.db_manager import apply_schema
from phase5.diagnostics import record_phase5_backfill_requests


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Create one or more Phase 5 Person 1 backfill requests for degraded historical windows."
    )
    parser.add_argument("--start", required=True, help="Window start timestamp (ISO 8601).")
    parser.add_argument("--end", required=True, help="Window end timestamp (ISO 8601).")
    parser.add_argument(
        "--source-system",
        action="append",
        required=True,
        help="One source system name. Repeat this flag to create requests for multiple sources.",
    )
    parser.add_argument("--requested-by", default="", help="Operator or teammate requesting the backfill.")
    parser.add_argument("--reason", required=True, help="Reason the backfill is needed.")
    parser.add_argument("--priority", default="normal", help="Priority label, e.g. low, normal, high.")
    parser.add_argument("--health-check-id", default="", help="Optional linked window-health artifact id.")
    parser.add_argument(
        "--output-dir",
        default="reports/phase5/backfill_requests",
        help="Relative output directory for request artifacts.",
    )
    parser.add_argument("--notes", default="", help="Optional operator notes.")
    parser.add_argument("--json", action="store_true", help="Emit machine-readable JSON.")
    return parser


def _render_text(payload: list[dict]) -> str:
    lines = ["Backfill requests created:"]
    for item in payload:
        lines.append(
            f"  - {item['source_system']}: request_id={item['backfill_request_id']} "
            f"status={item['request_status']} priority={item['priority']} output={item['output_path']}"
        )
    return "\n".join(lines)


def main() -> int:
    args = build_parser().parse_args()
    apply_schema()
    requests = record_phase5_backfill_requests(
        start=args.start,
        end=args.end,
        source_systems=args.source_system,
        requested_by=args.requested_by or None,
        reason=args.reason,
        priority=args.priority,
        health_check_id=args.health_check_id or None,
        output_dir=args.output_dir,
        notes=args.notes or None,
    )
    payload = [item.to_dict() for item in requests]
    if args.json:
        print(json.dumps(payload, indent=2, sort_keys=True))
    else:
        print(_render_text(payload))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
