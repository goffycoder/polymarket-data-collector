"""CLI entrypoint for Phase 2 replay validation."""

from __future__ import annotations

import argparse
import json

from database.db_manager import apply_schema
from validation.phase2_replay import build_replay_window_report


def build_parser() -> argparse.ArgumentParser:
    """Create the CLI parser for replay validation."""

    parser = argparse.ArgumentParser(description="Inspect Phase 2 archive coverage for a historical window.")
    parser.add_argument("--start", required=True, help="Window start timestamp (ISO 8601).")
    parser.add_argument("--end", required=True, help="Window end timestamp (ISO 8601).")
    parser.add_argument(
        "--source-system",
        required=True,
        help="One source system name, e.g. gamma_events, gamma_markets, clob_ws_market, data_api_trades.",
    )
    parser.add_argument("--json", action="store_true", help="Emit machine-readable JSON.")
    return parser


def _render_text(payload: dict) -> str:
    """Render a compact human-readable replay report."""

    summary = payload["summary"]
    lines = [
        f"Replay window: {payload['start']} -> {payload['end']}",
        f"Source system: {payload['source_system']}",
        f"Raw rows in window: {summary['raw_rows_in_window']}",
        f"Detector rows in window: {summary['detector_rows_in_window']}",
        f"Raw partitions found: {summary['raw_partitions_found']}",
        f"Detector partitions found: {summary['detector_partitions_found']}",
        "",
        "Raw partitions:",
    ]
    for item in payload["raw_partitions"]:
        lines.append(
            f"  - {item['partition_path']}: exists={item['exists']} total_rows={item['total_rows']} "
            f"rows_in_window={item['rows_in_window']} manifest_rows={item['manifest_row_count']}"
        )
    lines.append("")
    lines.append("Detector partitions:")
    for item in payload["detector_partitions"]:
        lines.append(
            f"  - {item['partition_path']}: exists={item['exists']} total_rows={item['total_rows']} "
            f"rows_in_window={item['rows_in_window']} manifest_rows={item['manifest_row_count']}"
        )
    return "\n".join(lines)


def main() -> int:
    """Run the replay-validation CLI."""

    args = build_parser().parse_args()
    apply_schema()
    report = build_replay_window_report(
        start=args.start,
        end=args.end,
        source_system=args.source_system,
    )
    payload = report.to_dict()

    if args.json:
        print(json.dumps(payload, indent=2, sort_keys=True))
    else:
        print(_render_text(payload))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
