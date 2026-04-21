from __future__ import annotations

import argparse
import json

from database.db_manager import apply_schema
from phase5.diagnostics import inspect_phase5_window_health


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Inspect historical window health for one or more Phase 5 replay source systems."
    )
    parser.add_argument("--start", required=True, help="Window start timestamp (ISO 8601).")
    parser.add_argument("--end", required=True, help="Window end timestamp (ISO 8601).")
    parser.add_argument(
        "--source-system",
        action="append",
        required=True,
        help="One source system name. Repeat this flag for multi-source health checks.",
    )
    parser.add_argument(
        "--output-dir",
        default="reports/phase5/window_health",
        help="Relative output directory for health artifacts.",
    )
    parser.add_argument("--notes", default="", help="Optional operator notes.")
    parser.add_argument("--json", action="store_true", help="Emit machine-readable JSON.")
    return parser


def _render_text(payload: dict) -> str:
    lines = [
        f"Window health id: {payload['health_check_id']}",
        f"Overall status: {payload['overall_status']}",
        f"Window: {payload['start']} -> {payload['end']}",
        f"Artifact path: {payload['output_path']}",
        "",
        "Per-source status:",
    ]
    for item in payload["health_items"]:
        lines.extend(
            [
                f"  - {item['source_system']}: integrity={item['integrity_status']}",
                f"    raw_missing={len(item['raw_missing_partitions'])} detector_missing={len(item['detector_missing_partitions'])}",
                f"    raw_mismatches={len(item['raw_manifest_mismatches'])} detector_mismatches={len(item['detector_manifest_mismatches'])}",
            ]
        )
    return "\n".join(lines)


def main() -> int:
    args = build_parser().parse_args()
    apply_schema()
    summary = inspect_phase5_window_health(
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
