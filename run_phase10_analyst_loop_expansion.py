from __future__ import annotations

import argparse
import asyncio
import json

from phase10 import run_phase10_task2_analyst_loop_expansion


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run Phase 10 Task 2: analyst-loop expansion and suppression review on repeated real-provider alert episodes."
    )
    parser.add_argument("--json", action="store_true", help="Emit the full task payload to stdout.")
    return parser


def _render_text(payload: dict[str, object]) -> str:
    review = payload["alert_review"]
    artifacts = payload["artifacts"]
    return "\n".join(
        [
            f"Created alerts: {review['created_alert_count']}",
            f"Suppressed alerts: {review['suppressed_alert_count']}",
            f"Delivery attempts: {review['delivery_attempt_count']}",
            f"Review packet: {artifacts['review_packet_path']}",
            f"Review summary: {artifacts['review_summary_path']}",
        ]
    )


def main() -> int:
    args = _build_parser().parse_args()
    payload = asyncio.run(run_phase10_task2_analyst_loop_expansion())
    if args.json:
        print(json.dumps(payload, indent=2, sort_keys=True))
    else:
        print(_render_text(payload))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
