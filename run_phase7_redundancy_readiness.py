from __future__ import annotations

import argparse
import json
from pathlib import Path

from phase7 import build_redundancy_readiness_report


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Build a Phase 7 redundancy-readiness report.")
    parser.add_argument(
        "--output",
        default="reports/phase7/redundancy_readiness.json",
        help="Path to write the redundancy-readiness JSON payload.",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Print the redundancy-readiness JSON payload to stdout.",
    )
    return parser


def main() -> int:
    args = _build_parser().parse_args()
    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    payload = build_redundancy_readiness_report(output_path=str(output_path))
    output_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    if args.json:
        print(json.dumps(payload, indent=2, sort_keys=True))
    else:
        print(json.dumps({"status": payload["status"], "blockers": payload["blockers"]}, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
