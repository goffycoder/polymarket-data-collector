from __future__ import annotations

import argparse
import asyncio
import json
from pathlib import Path

from config.runtime_env import load_runtime_env


async def _main() -> int:
    parser = argparse.ArgumentParser(
        description="Replay one archived detector-input window through Phase 3 with explicit restore and gap reporting."
    )
    parser.add_argument("--start", required=True, help="UTC ISO8601 inclusive start timestamp.")
    parser.add_argument("--end", required=True, help="UTC ISO8601 exclusive end timestamp.")
    parser.add_argument(
        "--env-file",
        default="",
        help="Optional runtime env file to load before replay.",
    )
    parser.add_argument(
        "--source-system",
        action="append",
        default=None,
        help="Detector-input source system to replay. Repeatable.",
    )
    parser.add_argument(
        "--phase5-source-system",
        action="append",
        default=None,
        help="Optional raw/detector coverage source system for a matching Phase 5 replay bundle.",
    )
    parser.add_argument(
        "--storage-audit-run-id",
        default="",
        help="Optional explicit storage audit run id for restore planning.",
    )
    parser.add_argument(
        "--allow-missing-partitions",
        action="store_true",
        help="Allow Phase 3 archived replay to continue even when the restore plan reports missing partitions.",
    )
    parser.add_argument(
        "--request-backfill-on-missing",
        action="store_true",
        help="Create Phase 5 backfill-request artifacts for missing source systems before exiting.",
    )
    parser.add_argument(
        "--backfill-requested-by",
        default="phase11_runtime_replay",
        help="Operator name recorded on automatic backfill requests.",
    )
    parser.add_argument(
        "--backfill-reason",
        default="Archived replay window missing required partitions during Phase 11 runtime recovery.",
        help="Reason to store on automatic backfill requests.",
    )
    parser.add_argument(
        "--use-redis-state",
        action="store_true",
        help="Use the canonical Redis-backed Phase 3 state path instead of an isolated in-memory replay store.",
    )
    parser.add_argument(
        "--limit-envelopes",
        type=int,
        default=0,
        help="Optional envelope cap for smoke tests.",
    )
    parser.add_argument(
        "--output",
        default="reports/phase11/runtime_replay_window.json",
        help="JSON output path.",
    )
    parser.add_argument("--json", action="store_true", help="Emit machine-readable JSON.")
    args = parser.parse_args()

    if args.env_file:
        load_runtime_env(args.env_file, override=True)

    from database.db_manager import apply_schema
    from phase3.detector import DEFAULT_PHASE3_SOURCE_SYSTEMS, Phase3Repository, run_phase3_detector_window
    from phase3.state_store import MemoryStateStore, create_state_store
    from phase5 import record_phase5_backfill_requests, run_phase5_replay_bundle
    from phase7.reporting import build_restore_plan

    apply_schema()
    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    restore_output_path = output_path.parent / "runtime_replay_restore_plan.json"

    restore_summary, restore_payload = build_restore_plan(
        start=args.start,
        end=args.end,
        restore_scope="phase11_runtime_replay_window",
        storage_audit_run_id=args.storage_audit_run_id or None,
        output_path=str(restore_output_path),
    )
    restore_output_path.write_text(json.dumps(restore_payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    missing_source_systems = sorted(
        {
            str(item["source_system"])
            for item in restore_payload["restore_items"]
            if not bool(item["file_exists"])
        }
    )

    backfill_requests = []
    if missing_source_systems and args.request_backfill_on_missing:
        backfill_requests = [
            item.to_dict()
            for item in record_phase5_backfill_requests(
                start=args.start,
                end=args.end,
                source_systems=missing_source_systems,
                requested_by=args.backfill_requested_by,
                reason=args.backfill_reason,
                priority="high",
                output_dir="reports/phase5/backfill_requests/runtime_replay",
                notes="Generated automatically by run_runtime_replay_window.py",
            )
        ]

    repository = Phase3Repository()
    checkpoint_before = repository.live_runtime_status()
    candidates_before = repository.load_persisted_candidates(start=args.start, end=args.end)

    if missing_source_systems and not args.allow_missing_partitions:
        payload = {
            "status": "missing_partitions_detected",
            "window": {"start": args.start, "end": args.end},
            "restore_summary": restore_summary.to_dict(),
            "restore_payload": restore_payload,
            "missing_source_systems": missing_source_systems,
            "backfill_requests": backfill_requests,
            "checkpoint_behavior": "archived_window_replay_does_not_advance_live_detector_checkpoints",
            "checkpoint_before": checkpoint_before,
            "checkpoint_after": checkpoint_before,
            "phase3_replay": None,
            "candidate_rows_before": len(candidates_before),
            "candidate_rows_after": len(candidates_before),
            "candidate_rows_created": 0,
            "candidate_ids_created": [],
            "phase5_replay_bundle": None,
        }
        output_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        if args.json:
            print(json.dumps(payload, indent=2, sort_keys=True))
        else:
            print("Archived replay blocked: restore plan reported missing partitions.")
            print(f"Missing source systems: {', '.join(missing_source_systems)}")
            print(f"Report: {output_path}")
        return 2

    if args.use_redis_state:
        state_context = await create_state_store(require_backend="redis", allow_fallback=False)
        state_store = state_context.store
        state_backend = state_context.backend_name
        state_notes = state_context.notes
    else:
        state_store = MemoryStateStore()
        state_backend = "memory"
        state_notes = "Archived-window replay defaults to isolated in-memory detector state."

    try:
        phase3_summary = await run_phase3_detector_window(
            start=args.start,
            end=args.end,
            store=state_store,
            repository=repository,
            source_systems=args.source_system or list(DEFAULT_PHASE3_SOURCE_SYSTEMS),
            limit_envelopes=args.limit_envelopes or None,
        )
    finally:
        await state_store.aclose()

    candidates_after = repository.load_persisted_candidates(start=args.start, end=args.end)
    before_ids = {str(item["candidate_id"]) for item in candidates_before}
    created_rows = [item for item in candidates_after if str(item["candidate_id"]) not in before_ids]
    checkpoint_after = repository.live_runtime_status()

    phase5_bundle = None
    if args.phase5_source_system:
        phase5_bundle = run_phase5_replay_bundle(
            start=args.start,
            end=args.end,
            source_systems=list(dict.fromkeys(args.phase5_source_system)),
            output_dir="reports/phase5/replay_runs/runtime_replay",
            notes="Generated from the Phase 11 archived-window replay path.",
        ).to_dict()

    payload = {
        "status": "completed_with_gaps" if missing_source_systems else "completed",
        "window": {"start": args.start, "end": args.end},
        "restore_summary": restore_summary.to_dict(),
        "restore_payload": restore_payload,
        "missing_source_systems": missing_source_systems,
        "backfill_requests": backfill_requests,
        "phase3_replay": {
            "source_systems": args.source_system or list(DEFAULT_PHASE3_SOURCE_SYSTEMS),
            "state_backend": state_backend,
            "state_notes": state_notes,
            "summary": phase3_summary.to_dict(),
        },
        "checkpoint_behavior": "archived_window_replay_does_not_advance_live_detector_checkpoints",
        "checkpoint_before": checkpoint_before,
        "checkpoint_after": checkpoint_after,
        "candidate_rows_before": len(candidates_before),
        "candidate_rows_after": len(candidates_after),
        "candidate_rows_created": len(created_rows),
        "candidate_ids_created": [item["candidate_id"] for item in created_rows[:50]],
        "phase5_replay_bundle": phase5_bundle,
    }
    output_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    if args.json:
        print(json.dumps(payload, indent=2, sort_keys=True))
    else:
        print(f"Archived replay status: {payload['status']}")
        print(f"Candidate rows created: {payload['candidate_rows_created']}")
        print(f"Missing source systems: {', '.join(missing_source_systems) if missing_source_systems else 'none'}")
        print(f"Report: {output_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(_main()))
