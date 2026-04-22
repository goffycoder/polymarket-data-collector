from __future__ import annotations

import argparse
import json
from pathlib import Path

from phase7.handoff import build_phase7_person2_handoff


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Build the Phase 7 Person 1 handoff artifact for Person 2.")
    parser.add_argument(
        "--output",
        default="reports/phase7/person2_handoff.json",
        help="Path to write the Person 2 handoff JSON payload.",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Print the handoff JSON payload to stdout.",
    )
    return parser


def main() -> int:
    args = _build_parser().parse_args()
    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    payload = build_phase7_person2_handoff()
    output_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    if args.json:
        print(json.dumps(payload, indent=2, sort_keys=True))
    else:
        print(json.dumps(payload["assessment"], indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
