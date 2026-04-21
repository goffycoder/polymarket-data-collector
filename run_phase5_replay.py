from __future__ import annotations

import argparse
import json

from database.db_manager import apply_schema
from phase5 import run_phase5_replay_window


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run the Phase 5 Person 1 replay foundation for one historical window."
    )
    parser.add_argument("--start", required=True, help="Window start timestamp (ISO 8601).")
    parser.add_argument("--end", required=True, help="Window end timestamp (ISO 8601).")
    parser.add_argument(
        "--source-system",
        required=True,
        help="One source system name, e.g. gamma_events, clob_ws_market, data_api_trades.",
    )
    parser.add_argument(
        "--output-dir",
        default="reports/phase5/replay_runs",
        help="Relative output directory for replay artifacts.",
    )
    parser.add_argument(
        "--notes",
        default="",
        help="Optional operator notes to store with the replay run.",
    )
    parser.add_argument("--json", action="store_true", help="Emit machine-readable JSON.")
    return parser


def _render_text(payload: dict) -> str:
    return "\n".join(
        [
            f"Replay run id: {payload['replay_run_id']}",
            f"Status: {payload['status']}",
            f"Source system: {payload['source_system']}",
            f"Window: {payload['start']} -> {payload['end']}",
            f"Git commit: {payload['git_commit']}",
            f"Raw partitions touched: {payload['raw_partitions_touched']}",
            f"Raw rows scanned: {payload['raw_rows_scanned']}",
            f"Detector rows observed: {payload['detector_rows_observed']}",
            f"Artifact path: {payload['output_path']}",
        ]
    )


def main() -> int:
    args = build_parser().parse_args()
    apply_schema()
    summary = run_phase5_replay_window(
        start=args.start,
        end=args.end,
        source_system=args.source_system,
        output_dir=args.output_dir,
        notes=args.notes or None,
    )
    payload = summary.to_dict()
    if args.json:
        print(json.dumps(payload, indent=2, sort_keys=True))
    else:
        print(_render_text(payload))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
