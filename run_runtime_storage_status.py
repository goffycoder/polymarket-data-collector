from __future__ import annotations

import argparse
import json
from pathlib import Path

from config.runtime_env import load_runtime_env


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Inspect Phase 11 runtime storage safety, retention posture, and safe prune candidates."
    )
    parser.add_argument(
        "--env-file",
        default="",
        help="Optional runtime env file to load before evaluating storage safety.",
    )
    parser.add_argument(
        "--refresh-storage-audit",
        action="store_true",
        help="Refresh the storage audit and compaction plan before building the Phase 11 storage status.",
    )
    parser.add_argument(
        "--apply-prune",
        action="store_true",
        help="Delete safe prune candidates such as old logs and manual replay artifacts.",
    )
    parser.add_argument(
        "--output",
        default="reports/phase11/runtime_storage_status.json",
        help="JSON output path for the consolidated storage status payload.",
    )
    parser.add_argument("--json", action="store_true", help="Emit the JSON payload to stdout.")
    return parser


def main() -> int:
    args = build_parser().parse_args()
    load_runtime_env(args.env_file or None, override=True)

    from database.db_manager import apply_schema
    from phase7.reporting import build_compaction_plan
    from phase7.runtime_storage import build_runtime_storage_status, prune_runtime_artifacts
    from phase7.storage import build_storage_audit

    apply_schema()
    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    refreshed_artifacts: dict[str, object] = {}
    if args.refresh_storage_audit:
        audit_output_path = output_path.parent / "storage_audit.json"
        compaction_output_path = output_path.parent / "compaction_plan.json"
        audit_summary, audit_payload = build_storage_audit(
            audit_scope="phase11_runtime_storage_status",
            output_path=str(audit_output_path),
        )
        audit_output_path.write_text(json.dumps(audit_payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        compaction_summary, compaction_payload = build_compaction_plan(
            storage_audit_run_id=audit_summary.storage_audit_run_id,
            plan_scope="phase11_runtime_storage_status",
            output_path=str(compaction_output_path),
        )
        compaction_output_path.write_text(
            json.dumps(compaction_payload, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        refreshed_artifacts = {
            "storage_audit": {
                "summary": audit_summary.to_dict(),
                "output_path": str(audit_output_path),
            },
            "compaction_plan": {
                "summary": compaction_summary.to_dict(),
                "output_path": str(compaction_output_path),
            },
        }

    storage_summary, storage_payload = build_runtime_storage_status(output_path=str(output_path))
    prune_summary = None
    prune_payload = None
    if args.apply_prune:
        prune_summary, prune_payload = prune_runtime_artifacts(apply=True)

    payload = {
        "storage_status_summary": storage_summary.to_dict(),
        "storage_status": storage_payload,
        "refreshed_artifacts": refreshed_artifacts,
        "prune_summary": None if prune_summary is None else prune_summary.to_dict(),
        "prune_payload": prune_payload,
    }
    output_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    if args.json:
        print(json.dumps(payload, indent=2, sort_keys=True))
    else:
        print(f"Storage status: {storage_summary.status} ({storage_summary.reason})")
        print(f"Free headroom: {storage_summary.free_gb} GB / {storage_summary.free_percent}%")
        print(f"Managed repo storage: {storage_summary.managed_gb} GB")
        print(f"Safe prune candidates: {storage_summary.prune_candidate_count}")
        print(f"Report: {output_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
