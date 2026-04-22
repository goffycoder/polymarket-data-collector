from __future__ import annotations

import argparse
import json

from phase9.phase5_evaluation import PHASE9_TASK3_REPLAY_OUTPUT_DIR, run_phase9_task3_phase5


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Materialize Phase 9 Task 3 replay, holdout validation, and conservative backtest artifacts."
    )
    parser.add_argument(
        "--output-dir",
        default=PHASE9_TASK3_REPLAY_OUTPUT_DIR,
        help="Relative output directory for replay bundle artifacts.",
    )
    parser.add_argument("--json", action="store_true", help="Emit machine-readable JSON.")
    return parser


def _render_text(payload: dict) -> str:
    validation = payload["validation_report"]
    backtest = payload["backtest_summary"]
    replay = payload["replay_bundle"]
    return "\n".join(
        [
            f"Contract version: {payload['task_contract_version']}",
            f"Window: {payload['window']['start']} -> {payload['window']['end']}",
            f"Replay bundle status: {replay['overall_status']}",
            f"Replay artifact: {replay['output_path']}",
            f"Evaluation rows: {validation['evaluation_row_count']}",
            f"Alert rows: {validation['alert_row_count']}",
            f"Paper trades: {validation['paper_trade_count']}",
            f"Validation assessment: {validation['assessment']['status']}",
            f"Median bounded PnL: {backtest['paper_trade_metrics']['median_bounded_pnl']}",
            f"Validation row id: {payload['database_rows']['validation_run_id']}",
            f"Backtest artifact id: {payload['database_rows']['backtest_artifact_id']}",
        ]
    )


def main() -> int:
    args = build_parser().parse_args()
    payload = run_phase9_task3_phase5(output_dir=args.output_dir)
    if args.json:
        print(json.dumps(payload, indent=2, sort_keys=True))
    else:
        print(_render_text(payload))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
