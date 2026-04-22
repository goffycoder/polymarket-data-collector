from __future__ import annotations

import argparse
import json
from pathlib import Path

from phase7.orchestration import build_policy_enforcement_plan


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Build a Phase 7 policy-enforcement dry-run batch.")
    parser.add_argument(
        "--execution-mode",
        default="dry_run",
        help="Execution mode label. Defaults to dry_run.",
    )
    parser.add_argument(
        "--max-items",
        type=int,
        default=0,
        help="Maximum number of planned items to include. Uses configured batch size when omitted or zero.",
    )
    parser.add_argument(
        "--output",
        default="reports/phase7/policy_enforcement.json",
        help="Path to write the policy-enforcement JSON payload.",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Print the policy-enforcement JSON payload to stdout.",
    )
    return parser


def main() -> int:
    args = _build_parser().parse_args()
    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    summary, payload = build_policy_enforcement_plan(
        execution_mode=args.execution_mode,
        max_items=args.max_items,
        output_path=str(output_path),
    )
    output_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    if args.json:
        print(json.dumps(payload, indent=2, sort_keys=True))
    else:
        print(json.dumps(summary.to_dict(), indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
