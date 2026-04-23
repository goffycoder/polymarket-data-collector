from __future__ import annotations

import argparse
import json

from phase10 import run_phase10_task3_heldout_validation_pack


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run Phase 10 Task 3: held-out Phase 5 validation pack.")
    parser.add_argument("--json", action="store_true", help="Emit the full task payload to stdout.")
    return parser


def main() -> int:
    args = _build_parser().parse_args()
    payload = run_phase10_task3_heldout_validation_pack()
    if args.json:
        print(json.dumps(payload, indent=2, sort_keys=True))
    else:
        print(f"Evaluation rows: {payload['validation_report']['evaluation_row_count']}")
        print(f"Alert rows: {payload['validation_report']['alert_row_count']}")
        print(f"Paper trades: {payload['validation_report']['paper_trade_count']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
