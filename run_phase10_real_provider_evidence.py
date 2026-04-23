from __future__ import annotations

import argparse
import asyncio
import json

from phase10 import run_phase10_task1_real_provider_evidence


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run Phase 10 Task 1: real-provider evidence hardening on the canonical replay-linked alert path."
    )
    parser.add_argument("--json", action="store_true", help="Emit the full task payload to stdout.")
    return parser


def _render_text(payload: dict[str, object]) -> str:
    summary = payload["real_provider_summary"]
    artifacts = payload["artifacts"]
    return "\n".join(
        [
            f"Live provider-backed query rows: {summary['live_call_count']}",
            f"Cached query rows: {summary['cache_hit_count']}",
            f"Review packet: {artifacts['review_packet_path']}",
            f"Review summary: {artifacts['review_summary_path']}",
        ]
    )


def main() -> int:
    args = _build_parser().parse_args()
    payload = asyncio.run(run_phase10_task1_real_provider_evidence())
    if args.json:
        print(json.dumps(payload, indent=2, sort_keys=True))
    else:
        print(_render_text(payload))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
