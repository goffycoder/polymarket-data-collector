from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from database.db_manager import apply_schema
from validation.phase7_gate7_report import build_phase7_gate7_report


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Render the Phase 7 Gate 7 report.")
    parser.add_argument("--json", action="store_true", help="Emit machine-readable JSON.")
    return parser


def main() -> int:
    args = build_parser().parse_args()
    apply_schema()
    payload = build_phase7_gate7_report()
    if args.json:
        print(json.dumps(payload, indent=2, sort_keys=True))
    else:
        print(json.dumps(payload["gate7_assessment"], indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
