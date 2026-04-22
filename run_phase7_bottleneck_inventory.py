from __future__ import annotations

import argparse
import json
from pathlib import Path

from phase7.profiling import build_bottleneck_inventory


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Build a Phase 7 bottleneck and failure inventory.")
    parser.add_argument(
        "--profile-scope",
        default="default",
        help="Profile scope label for this inventory run.",
    )
    parser.add_argument(
        "--output",
        default="reports/phase7/bottleneck_inventory.json",
        help="Path to write the bottleneck inventory JSON payload.",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Print the bottleneck inventory JSON payload to stdout.",
    )
    return parser


def main() -> int:
    args = _build_parser().parse_args()
    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    summary, payload = build_bottleneck_inventory(
        profile_scope=args.profile_scope,
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
