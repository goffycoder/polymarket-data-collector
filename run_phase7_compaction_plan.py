from __future__ import annotations

import argparse
import json
from pathlib import Path

from database.db_manager import apply_schema
from phase7.reporting import build_compaction_plan


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Build the Phase 7 Person 1 compaction/cold-archive plan from the latest storage audit.")
    parser.add_argument(
        "--storage-audit-run-id",
        default="",
        help="Optional explicit storage audit run id.",
    )
    parser.add_argument(
        "--plan-scope",
        default="default",
        help="Short label for this compaction plan.",
    )
    parser.add_argument(
        "--output",
        default="reports/phase7/compaction_plan.json",
        help="JSON output path.",
    )
    parser.add_argument("--json", action="store_true", help="Emit machine-readable JSON.")
    return parser


def main() -> int:
    args = build_parser().parse_args()
    apply_schema()
    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    summary, payload = build_compaction_plan(
        storage_audit_run_id=args.storage_audit_run_id or None,
        plan_scope=args.plan_scope,
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
        print(f"Compaction plan: {summary.compaction_plan_run_id}")
        print(f"Items: {summary.total_items}")
        print(f"Report: {output_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
