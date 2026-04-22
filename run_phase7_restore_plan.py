from __future__ import annotations

import argparse
import json
from pathlib import Path

from database.db_manager import apply_schema
from phase7.reporting import build_restore_plan


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Build the Phase 7 Person 1 restore plan for one historical window.")
    parser.add_argument("--start", required=True, help="UTC ISO8601 inclusive start timestamp.")
    parser.add_argument("--end", required=True, help="UTC ISO8601 exclusive end timestamp.")
    parser.add_argument(
        "--restore-scope",
        default="historical_window",
        help="Short label for this restore plan.",
    )
    parser.add_argument(
        "--storage-audit-run-id",
        default="",
        help="Optional explicit storage audit run id.",
    )
    parser.add_argument(
        "--output",
        default="reports/phase7/restore_plan.json",
        help="JSON output path.",
    )
    parser.add_argument("--json", action="store_true", help="Emit machine-readable JSON.")
    return parser


def main() -> int:
    args = build_parser().parse_args()
    apply_schema()
    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    summary, payload = build_restore_plan(
        start=args.start,
        end=args.end,
        restore_scope=args.restore_scope,
        storage_audit_run_id=args.storage_audit_run_id or None,
        output_path=str(output_path),
    )
    output_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    result = {
        "summary": summary.to_dict(),
        "output_path": str(output_path),
    }
    if args.json:
        print(json.dumps(result, indent=2, sort_keys=True))
    else:
        print(f"Restore plan: {summary.restore_plan_run_id}")
        print(f"Items: {summary.total_items}")
        print(f"Missing: {summary.missing_item_count}")
        print(f"Report: {output_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
