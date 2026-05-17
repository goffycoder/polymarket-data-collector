from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
from typing import Any

from config.runtime_env import load_runtime_env


DEFAULT_OUTPUT = "reports/phase12/archive_reconciliation.json"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Verify Phase 12 archive roots and refresh storage status against visible local/SSD archives."
    )
    parser.add_argument("--env-file", default=".env.runtime", help="Runtime env file to load before reconciliation.")
    parser.add_argument(
        "--external-root",
        action="append",
        default=[],
        help="Explicit external archive root. Repeat for multiple roots. Root should contain data/raw or data/detector_input.",
    )
    parser.add_argument("--output", default=DEFAULT_OUTPUT, help="JSON output path for the reconciliation report.")
    parser.add_argument("--json", action="store_true", help="Emit the JSON report to stdout.")
    return parser


def _visible_volume_dirs() -> list[str]:
    root = Path("/Volumes")
    if not root.exists():
        return []
    return sorted(str(path) for path in root.iterdir() if path.is_dir())


def _root_status(root: Path) -> dict[str, Any]:
    raw_root = root / "data" / "raw"
    detector_root = root / "data" / "detector_input"
    return {
        "root": str(root),
        "exists": root.exists(),
        "has_data_dir": (root / "data").exists(),
        "has_raw_archive": raw_root.exists(),
        "has_detector_input": detector_root.exists(),
        "accepted": root.exists() and (raw_root.exists() or detector_root.exists()),
    }


def _candidate_roots(explicit_roots: list[str]) -> list[Path]:
    roots = [Path(root).expanduser() for root in explicit_roots if str(root).strip()]
    for volume in _visible_volume_dirs():
        volume_path = Path(volume)
        if volume_path.name == "Macintosh HD":
            continue
        roots.extend(
            [
                volume_path / "polymarket_phase11_data_archive_2026-05-15",
                volume_path / "polymarket_archive",
                volume_path / "polymarket_arbitrage",
            ]
        )
        roots.extend(path for path in volume_path.glob("polymarket*_archive*") if path.is_dir())

    deduped: list[Path] = []
    seen: set[str] = set()
    for root in roots:
        key = str(root)
        if key in seen:
            continue
        seen.add(key)
        deduped.append(root)
    return deduped


def main() -> int:
    args = build_parser().parse_args()
    env_result = load_runtime_env(args.env_file or None, override=True)

    root_checks = [_root_status(root) for root in _candidate_roots(args.external_root)]
    accepted_roots = [item["root"] for item in root_checks if item["accepted"]]
    if accepted_roots:
        os.environ["POLYMARKET_EXTERNAL_ARCHIVE_ROOTS"] = ",".join(accepted_roots)

    from database.db_manager import apply_schema
    from phase7.reporting import build_compaction_plan
    from phase7.runtime_storage import build_runtime_storage_status
    from phase7.storage import build_storage_audit, external_archive_roots

    apply_schema()
    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    audit_output_path = output_path.parent / "storage_audit_archive_reconciliation.json"
    compaction_output_path = output_path.parent / "compaction_plan_archive_reconciliation.json"
    status_output_path = output_path.parent / "runtime_storage_status_archive_reconciliation.json"

    audit_summary, audit_payload = build_storage_audit(
        audit_scope="phase12_archive_reconciliation",
        output_path=str(audit_output_path),
    )
    audit_output_path.write_text(json.dumps(audit_payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    compaction_summary, compaction_payload = build_compaction_plan(
        storage_audit_run_id=audit_summary.storage_audit_run_id,
        plan_scope="phase12_archive_reconciliation",
        output_path=str(compaction_output_path),
    )
    compaction_output_path.write_text(
        json.dumps(compaction_payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )

    storage_summary, storage_payload = build_runtime_storage_status(output_path=str(status_output_path))
    status_report = {
        "storage_status_summary": storage_summary.to_dict(),
        "storage_status": storage_payload,
    }
    status_output_path.write_text(json.dumps(status_report, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    missing_file_count = int(audit_summary.missing_file_count)
    archive_only_count = int(audit_payload["totals"]["archive_only_count"])
    if accepted_roots and missing_file_count == 0:
        status = "archive_reconciled"
        next_action = "Run bounded archived replay and then the long organic proof."
    elif accepted_roots:
        status = "archive_partially_reconciled"
        next_action = "Inspect missing partitions; the mounted archive does not cover every manifest path."
    else:
        status = "external_archive_not_visible"
        next_action = "Mount the SSD so it appears under /Volumes, or rerun with --external-root /absolute/path/to/archive."

    report = {
        "status": status,
        "next_action": next_action,
        "env_file": None if env_result.env_file is None else str(env_result.env_file),
        "visible_volume_dirs": _visible_volume_dirs(),
        "checked_roots": root_checks,
        "accepted_roots": accepted_roots,
        "effective_external_archive_roots": [str(root) for root in external_archive_roots()],
        "audit_summary": audit_summary.to_dict(),
        "compaction_summary": compaction_summary.to_dict(),
        "storage_status_summary": storage_summary.to_dict(),
        "artifacts": {
            "storage_audit": str(audit_output_path),
            "compaction_plan": str(compaction_output_path),
            "runtime_storage_status": str(status_output_path),
        },
    }
    output_path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    if args.json:
        print(json.dumps(report, indent=2, sort_keys=True))
    else:
        print(f"Archive reconciliation: {status}")
        print(f"Missing partitions: {missing_file_count}")
        print(f"Archive-backed partitions: {archive_only_count}")
        print(f"Report: {output_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
