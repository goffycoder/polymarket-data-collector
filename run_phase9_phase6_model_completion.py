from __future__ import annotations

import argparse
import json

from phase9.phase6_model_completion import run_phase9_task4_model_completion


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run Phase 9 Task 4 Phase 6 model completion on the canonical reference window."
    )
    parser.add_argument("--json", action="store_true", help="Emit machine-readable JSON.")
    return parser


def _render_text(payload: dict) -> str:
    assessment = payload["required_baseline_report"]["assessment"]
    return "\n".join(
        [
            f"Contract version: {payload['task_contract_version']}",
            f"Model version: {payload['active_shadow_model']['model_version']}",
            f"Window: {payload['window']['start']} -> {payload['window']['end']}",
            f"Baseline assessment: {assessment['status']}",
            f"Preferred split: {payload['required_baseline_report']['preferred_split']}",
            f"Shadow scores written: {payload['shadow_score_count']}",
            f"Model artifact: {payload['artifacts']['model_path']}",
            f"Model card: {payload['artifacts']['model_card_path']}",
        ]
    )


def main() -> int:
    args = build_parser().parse_args()
    payload = run_phase9_task4_model_completion()
    if args.json:
        print(json.dumps(payload, indent=2, sort_keys=True))
    else:
        print(_render_text(payload))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
