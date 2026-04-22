from __future__ import annotations

import argparse
import json

from phase10 import run_phase10_task5_ops_governance_closeout


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run Phase 10 Task 5: ops, security, governance, and final closeout.")
    parser.add_argument("--json", action="store_true", help="Emit the full task payload to stdout.")
    return parser


def main() -> int:
    args = _build_parser().parse_args()
    payload = run_phase10_task5_ops_governance_closeout()
    if args.json:
        print(json.dumps(payload, indent=2, sort_keys=True))
    else:
        print(f"SRS v1 complete: {payload['completion_payload']['srs_v1_complete']}")
        print(f"Overall status: {payload['completion_payload']['overall_status']}")
        print(f"Completion memo: {payload['artifacts']['completion_memo_path']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
