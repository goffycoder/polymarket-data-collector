from __future__ import annotations

import argparse
import json

from database.db_manager import apply_schema
from phase5.orchestration import dispatch_phase5_backfill_requests


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Dispatch stored Phase 5 Person 1 backfill requests using the current support matrix."
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=10,
        help="Maximum number of pending/planned backfill requests to inspect.",
    )
    parser.add_argument(
        "--execute-supported",
        action="store_true",
        help="Actually execute supported requests. Without this flag, the command only plans and updates statuses.",
    )
    parser.add_argument("--json", action="store_true", help="Emit machine-readable JSON.")
    return parser


def _render_text(payload: dict) -> str:
    lines = [
        f"Requests processed: {payload['request_count']}",
        f"Execute supported: {payload['execute_supported']}",
        "",
        "Dispatch items:",
    ]
    for item in payload["dispatch_items"]:
        lines.extend(
            [
                f"  - {item['source_system']}: status={item['request_status']} strategy={item['strategy']}",
                f"    executed={item['executed']} output={item['output_path']}",
                f"    command={item['command_preview'] or 'n/a'}",
            ]
        )
    return "\n".join(lines)


def main() -> int:
    args = build_parser().parse_args()
    apply_schema()
    summary = dispatch_phase5_backfill_requests(
        limit=max(1, args.limit),
        execute_supported=args.execute_supported,
    )
    payload = summary.to_dict()
    if args.json:
        print(json.dumps(payload, indent=2, sort_keys=True))
    else:
        print(_render_text(payload))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
