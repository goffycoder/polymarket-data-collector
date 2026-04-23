from __future__ import annotations

import argparse
import json

from phase10 import run_phase10_task4_heldout_model_completion


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run Phase 10 Task 4: held-out Phase 6 boosted-tree completion.")
    parser.add_argument("--json", action="store_true", help="Emit the full task payload to stdout.")
    return parser


def main() -> int:
    args = _build_parser().parse_args()
    payload = run_phase10_task4_heldout_model_completion()
    if args.json:
        print(json.dumps(payload, indent=2, sort_keys=True))
    else:
        print(f"Dataset rows: {payload['dataset_summary']['row_count']}")
        print(f"Baseline assessment: {(payload['required_baseline_report'].get('assessment') or {}).get('status')}")
        print(f"Shadow scores: {payload['shadow_score_count']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
