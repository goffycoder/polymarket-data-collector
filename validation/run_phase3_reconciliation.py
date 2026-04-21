from __future__ import annotations

import argparse
import asyncio
import json

from database.db_manager import apply_schema
from validation.phase3_reconciliation import build_phase3_reconciliation_report


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Compare persisted Phase 3 candidates against replayed detector output for the same window."
    )
    parser.add_argument("--start", required=True, help="Window start timestamp (ISO 8601).")
    parser.add_argument("--end", required=True, help="Window end timestamp (ISO 8601).")
    parser.add_argument("--json", action="store_true", help="Emit machine-readable JSON.")
    return parser


def _render_text(payload: dict) -> str:
    lines = [
        f"Window: {payload['start']} -> {payload['end']}",
        f"Persisted candidates: {payload['persisted_candidate_count']}",
        f"Replay candidates: {payload['replay_candidate_count']}",
        f"Missing from replay: {len(payload['missing_from_replay'])}",
        f"Extra in replay: {len(payload['extra_in_replay'])}",
        f"Replay summary: {payload['replay_summary']}",
    ]
    if payload["missing_from_replay"]:
        lines.append("Missing signatures:")
        lines.extend(f"  - {item}" for item in payload["missing_from_replay"])
    if payload["extra_in_replay"]:
        lines.append("Extra signatures:")
        lines.extend(f"  - {item}" for item in payload["extra_in_replay"])
    return "\n".join(lines)


async def _main() -> int:
    args = build_parser().parse_args()
    apply_schema()
    report = await build_phase3_reconciliation_report(start=args.start, end=args.end)
    payload = report.to_dict()

    if args.json:
        print(json.dumps(payload, indent=2, sort_keys=True))
    else:
        print(_render_text(payload))
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(_main()))
