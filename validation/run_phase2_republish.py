"""CLI entrypoint for Phase 2 replay republishing."""

from __future__ import annotations

import argparse
import json

from database.db_manager import apply_schema
from validation.phase2_republish import republish_raw_window


def build_parser() -> argparse.ArgumentParser:
    """Create the CLI parser."""

    parser = argparse.ArgumentParser(description="Republish raw archived envelopes for a historical window.")
    parser.add_argument("--start", required=True, help="Window start timestamp (ISO 8601).")
    parser.add_argument("--end", required=True, help="Window end timestamp (ISO 8601).")
    parser.add_argument(
        "--source-system",
        required=True,
        help="One source system name, e.g. gamma_events, clob_ws_market, data_api_trades.",
    )
    parser.add_argument("--json", action="store_true", help="Emit machine-readable JSON.")
    return parser


def _render_text(payload: dict) -> str:
    """Render a compact text summary for one replay republish run."""

    return "\n".join(
        [
            f"Replay run: {payload['replay_run_id']}",
            f"Source system: {payload['source_system']}",
            f"Window: {payload['start']} -> {payload['end']}",
            f"Raw partitions touched: {payload['raw_partitions_touched']}",
            f"Raw rows scanned: {payload['raw_rows_scanned']}",
            f"Rows republished: {payload['rows_republished']}",
            f"Output path: {payload['output_path']}",
        ]
    )


def main() -> int:
    """Run the CLI."""

    args = build_parser().parse_args()
    apply_schema()
    result = republish_raw_window(
        start=args.start,
        end=args.end,
        source_system=args.source_system,
    )
    payload = result.to_dict()
    if args.json:
        print(json.dumps(payload, indent=2, sort_keys=True))
    else:
        print(_render_text(payload))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
