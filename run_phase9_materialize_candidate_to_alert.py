from __future__ import annotations

import argparse
import asyncio
import json

from phase9 import materialize_phase9_task2


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Materialize the Phase 9 Task 2 candidate-to-alert path for the canonical reference hour."
    )
    parser.add_argument(
        "--output-dir",
        default="reports/phase9/candidate_to_alert_materialization",
        help="Directory for the generated Task 2 review packet and summary.",
    )
    parser.add_argument("--json", action="store_true", help="Emit the full Task 2 payload to stdout.")
    return parser


def _render_text(payload: dict[str, object]) -> str:
    phase3 = payload["phase3"]
    phase4 = payload["phase4"]
    return "\n".join(
        [
            f"Window: {payload['window']['start']} -> {payload['window']['end']}",
            f"Candidates emitted: {phase3['detector_summary']['candidates_emitted']}",
            f"Alerts created: {phase4['alert_summary']['alerts_created']}",
            f"Evidence queries written: {phase4['evidence_summary']['evidence_queries_written']}",
            f"Analyst action: {phase4['analyst_result']['action_type']}",
            f"Review packet: {payload['artifacts']['review_packet_path']}",
            f"Review summary: {payload['artifacts']['review_summary_path']}",
        ]
    )


async def _main() -> int:
    args = _build_parser().parse_args()
    payload = await materialize_phase9_task2(output_dir=args.output_dir)
    if args.json:
        print(json.dumps(payload, indent=2, sort_keys=True))
    else:
        print(_render_text(payload))
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(_main()))
