from __future__ import annotations

import argparse
import json

from database.db_manager import apply_schema
from validation.phase3_candidate_report import build_phase3_candidate_report


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Summarize Phase 3 candidate volume by hour, rule family, and market."
    )
    parser.add_argument("--start", required=True, help="Window start timestamp (ISO 8601).")
    parser.add_argument("--end", required=True, help="Window end timestamp (ISO 8601).")
    parser.add_argument("--json", action="store_true", help="Emit machine-readable JSON.")
    return parser


def _render_text(payload: dict) -> str:
    lines = [
        f"Window: {payload['start']} -> {payload['end']}",
        f"Detector version: {payload['detector_version']}",
        f"Total candidates: {payload['total_candidates']}",
        f"Unique markets: {payload['unique_markets']}",
        "",
        "Hourly counts:",
    ]
    if payload["hourly_counts"]:
        lines.extend(
            f"  - {item['hour_bucket']}: {item['candidate_count']}"
            for item in payload["hourly_counts"]
        )
    else:
        lines.append("  - none")

    lines.append("")
    lines.append("Rule family counts:")
    if payload["rule_family_counts"]:
        lines.extend(
            f"  - {item['rule_family']}: {item['candidate_count']}"
            for item in payload["rule_family_counts"]
        )
    else:
        lines.append("  - none")

    lines.append("")
    lines.append("Top markets:")
    if payload["top_markets"]:
        lines.extend(
            f"  - {item['market_id']}: count={item['candidate_count']} max_severity={item['max_severity_score']:.2f}"
            for item in payload["top_markets"]
        )
    else:
        lines.append("  - none")
    return "\n".join(lines)


def main() -> int:
    args = build_parser().parse_args()
    apply_schema()
    payload = build_phase3_candidate_report(start=args.start, end=args.end).to_dict()
    if args.json:
        print(json.dumps(payload, indent=2, sort_keys=True))
    else:
        print(_render_text(payload))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
