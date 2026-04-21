from __future__ import annotations

import argparse
import json
import time

from config.settings import ENABLE_PHASE6_SHADOW_MODE, PHASE6_LIVE_LOOKBACK_MINUTES, PHASE6_LIVE_POLL_SECONDS
from database.db_manager import apply_schema
from phase6.live_shadow import run_live_shadow_window


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run Phase 6 Person 1 live shadow scoring over rolling replay windows.")
    parser.add_argument("--model-version", default="", help="Optional registry model override.")
    parser.add_argument("--lookback-minutes", type=int, default=PHASE6_LIVE_LOOKBACK_MINUTES, help="Rolling replay lookback.")
    parser.add_argument("--poll-seconds", type=int, default=PHASE6_LIVE_POLL_SECONDS, help="Loop polling interval.")
    parser.add_argument("--iterations", type=int, default=1, help="Number of polling iterations; 0 means forever.")
    parser.add_argument("--json", action="store_true", help="Emit machine-readable JSON.")
    return parser


def main() -> int:
    args = build_parser().parse_args()
    apply_schema()
    if not ENABLE_PHASE6_SHADOW_MODE:
        raise SystemExit("Phase 6 shadow mode is disabled. Set POLYMARKET_ENABLE_PHASE6_SHADOW_MODE=true.")

    iteration = 0
    summaries = []
    while True:
        iteration += 1
        summary = run_live_shadow_window(
            lookback_minutes=max(1, args.lookback_minutes),
            model_version=args.model_version or None,
        )
        summaries.append(summary.to_dict())
        if args.iterations and iteration >= args.iterations:
            break
        time.sleep(max(1, args.poll_seconds))

    payload = {
        "iteration_count": len(summaries),
        "summaries": summaries,
    }
    if args.json:
        print(json.dumps(payload, indent=2, sort_keys=True))
    else:
        latest = summaries[-1] if summaries else {}
        print(f"Iterations: {len(summaries)}")
        print(f"Latest model: {latest.get('model_version', 'none')}")
        print(f"Latest score count: {latest.get('score_count', 0)}")
        print(f"Latest artifact: {latest.get('output_path', '')}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
