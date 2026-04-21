from __future__ import annotations

import argparse
import asyncio
import json

from database.db_manager import apply_schema
from validation.phase3_gate3_report import build_phase3_gate3_report


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Build one combined Gate 3 evidence report for a Phase 3 detector window."
    )
    parser.add_argument("--start", required=True, help="Window start timestamp (ISO 8601).")
    parser.add_argument("--end", required=True, help="Window end timestamp (ISO 8601).")
    parser.add_argument("--json", action="store_true", help="Emit machine-readable JSON.")
    return parser


def _render_text(payload: dict) -> str:
    assessment = payload["assessment"]
    registration = payload.get("detector_registration") or {}
    candidate_report = payload["candidate_report"]
    reconciliation_report = payload["reconciliation_report"]
    return "\n".join(
        [
            f"Window: {payload['start']} -> {payload['end']}",
            f"Detector version: {payload['detector_version']}",
            f"Feature schema version: {payload['feature_schema_version']}",
            f"State backend: {registration.get('state_backend')}",
            f"Assessment status: {assessment['status']}",
            f"Persisted candidates: {candidate_report['total_candidates']}",
            f"Replay candidates: {reconciliation_report['replay_candidate_count']}",
            f"Signature mismatch count: {assessment['signature_mismatch_count']}",
            f"Top rule family: {assessment['top_rule_family']}",
        ]
    )


async def _main() -> int:
    args = build_parser().parse_args()
    apply_schema()
    payload = (await build_phase3_gate3_report(start=args.start, end=args.end)).to_dict()
    if args.json:
        print(json.dumps(payload, indent=2, sort_keys=True))
    else:
        print(_render_text(payload))
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(_main()))
