from __future__ import annotations

import argparse
import json

from database.db_manager import apply_schema
from validation.phase6_person2_report import build_phase6_person2_report


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Render the Phase 6 Person 2 evaluation and calibration report.")
    parser.add_argument("--limit", type=int, default=10, help="Recent rows to include.")
    parser.add_argument("--json", action="store_true", help="Emit machine-readable JSON.")
    return parser


def main() -> int:
    args = build_parser().parse_args()
    apply_schema()
    payload = build_phase6_person2_report(limit=max(1, args.limit))
    if args.json:
        print(json.dumps(payload, indent=2, sort_keys=True))
    else:
        print(json.dumps(payload["assessment"], indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
