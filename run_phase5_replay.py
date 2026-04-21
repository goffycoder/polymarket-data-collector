from __future__ import annotations

import argparse
import json

from database.db_manager import apply_schema
from phase5 import run_phase5_replay_bundle


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run the Phase 5 Person 1 replay foundation for one or more historical sources."
    )
    parser.add_argument("--start", required=True, help="Window start timestamp (ISO 8601).")
    parser.add_argument("--end", required=True, help="Window end timestamp (ISO 8601).")
    parser.add_argument(
        "--source-system",
        action="append",
        required=True,
        help="One source system name. Repeat this flag for multi-source replay, e.g. --source-system clob_ws_market --source-system clob_books.",
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
    lines = [
        f"Replay bundle id: {payload['bundle_id']}",
        f"Overall status: {payload['overall_status']}",
        f"Window: {payload['start']} -> {payload['end']}",
        f"Git commit: {payload['git_commit']}",
        f"Source systems: {', '.join(payload['source_systems'])}",
        f"Total raw rows scanned: {payload['total_raw_rows_scanned']}",
        f"Total detector rows observed: {payload['total_detector_rows_observed']}",
        f"Artifact path: {payload['output_path']}",
        "",
        "Replay runs:",
    ]
    for item in payload["replay_runs"]:
        lines.extend(
            [
                f"  - {item['source_system']}: status={item['status']} integrity={item['integrity_status']}",
                f"    raw_missing={item['raw_missing_partitions']} detector_missing={item['detector_missing_partitions']}",
                f"    raw_mismatches={item['raw_manifest_mismatches']} detector_mismatches={item['detector_manifest_mismatches']}",
                f"    artifact={item['output_path']}",
            ]
        )
    return "\n".join(lines)


def main() -> int:
    args = build_parser().parse_args()
    apply_schema()
    summary = run_phase5_replay_bundle(
        start=args.start,
        end=args.end,
        source_systems=args.source_system,
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
