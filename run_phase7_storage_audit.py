from __future__ import annotations

import argparse
import json
from pathlib import Path

from database.db_manager import apply_schema
from phase7 import build_storage_audit


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run the Phase 7 Person 1 storage and archive tiering audit.")
    parser.add_argument(
        "--audit-scope",
        default="full_repo",
        help="Short label for the audit run.",
    )
    parser.add_argument(
        "--output",
        default="reports/phase7/storage_audit.json",
        help="JSON output path.",
    )
    parser.add_argument("--json", action="store_true", help="Emit machine-readable JSON to stdout.")
    return parser


def main() -> int:
    args = build_parser().parse_args()
    apply_schema()
    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    summary, payload = build_storage_audit(
        audit_scope=args.audit_scope,
        output_path=str(output_path),
    )
    output_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    result = {
        "summary": summary.to_dict(),
        "output_path": str(output_path),
    }
    if args.json:
        print(json.dumps(result, indent=2, sort_keys=True))
    else:
        print(f"Storage audit run: {summary.storage_audit_run_id}")
        print(f"Partitions: {summary.total_partitions}")
        print(f"Bytes: {summary.total_bytes}")
        print(f"Missing files: {summary.missing_file_count}")
        print(f"Report: {output_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
