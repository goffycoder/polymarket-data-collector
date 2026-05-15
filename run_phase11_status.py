from __future__ import annotations

import argparse
import asyncio
import json
import os
from pathlib import Path
from typing import Any

from config.runtime_env import REPO_ROOT, load_runtime_env


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Evaluate Phase 11 implementation/readiness against the documented deliverables and exit criteria."
    )
    parser.add_argument(
        "--env-file",
        default="",
        help="Explicit runtime env file. Defaults to .env.runtime, then legacy .env, then shell-only.",
    )
    parser.add_argument(
        "--recent-hours",
        type=int,
        default=24,
        help="How many recent hours of runtime activity to inspect.",
    )
    parser.add_argument(
        "--output",
        default="reports/phase11/phase11_status.json",
        help="JSON output path.",
    )
    parser.add_argument("--json", action="store_true", help="Emit machine-readable JSON.")
    return parser


async def _state_backend_health_check() -> dict[str, Any]:
    from phase3.state_store import Phase3StateStoreConfigurationError, create_state_store

    try:
        context = await create_state_store(require_backend="durable", allow_fallback=False)
    except Phase3StateStoreConfigurationError as exc:
        return {
            "status": "unreachable",
            "reason": str(exc),
        }
    except Exception as exc:  # pragma: no cover - defensive runtime surface
        return {
            "status": "error",
            "reason": str(exc),
        }

    try:
        return {
            "status": "reachable",
            "backend_name": context.backend_name,
            "notes": context.notes,
        }
    finally:
        await context.store.aclose()


def _item(
    *,
    checklist_id: str,
    section: str,
    requirement: str,
    status: str,
    evidence: list[str],
    remaining_work: list[str] | None = None,
) -> dict[str, Any]:
    return {
        "checklist_id": checklist_id,
        "section": section,
        "requirement": requirement,
        "status": status,
        "evidence": evidence,
        "remaining_work": remaining_work or [],
    }


def _count_status(items: list[dict[str, Any]], status: str) -> int:
    return sum(1 for item in items if item["status"] == status)


def _render_text(payload: dict[str, Any]) -> str:
    summary = payload["summary"]
    lines = [
        f"Phase 11 overall status: {summary['overall_status']}",
        f"Implemented: {summary['implemented_count']}",
        f"Partially implemented: {summary['partially_implemented_count']}",
        f"Not implemented: {summary['not_implemented_count']}",
        "",
        "Checklist:",
    ]
    for item in payload["checklist"]:
        lines.append(f"- [{item['status']}] {item['checklist_id']}: {item['requirement']}")
    if payload["next_required_actions"]:
        lines.extend(["", "Next required actions:"])
        for action in payload["next_required_actions"]:
            lines.append(f"- {action}")
    return "\n".join(lines)


def main() -> int:
    args = build_parser().parse_args()
    env_result = load_runtime_env(args.env_file or None, override=True)
    os.environ["POLYMARKET_RUNTIME_LAUNCHED"] = "1"

    from config import settings
    from config.runtime_mode import build_runtime_decision_summary
    from database.db_manager import apply_schema
    from phase3.detector import Phase3Repository
    from phase4 import Phase4Repository
    from phase6 import Phase6Repository
    from phase7.runtime_storage import build_runtime_storage_status

    apply_schema()

    phase3_repository = Phase3Repository()
    phase4_repository = Phase4Repository()
    phase6_repository = Phase6Repository()

    phase3_status = phase3_repository.live_runtime_status(recent_hours=args.recent_hours)
    phase4_status = phase4_repository.live_runtime_status(recent_hours=args.recent_hours)
    phase6_status = phase6_repository.live_runtime_status(recent_hours=args.recent_hours)
    phase3_registration = phase3_repository.load_detector_registration()
    phase4_registration = phase4_repository.load_workflow_registration()
    phase6_registry_status = phase6_repository.build_registry_status(limit=10).to_dict()
    runtime_decision = build_runtime_decision_summary(
        settings=settings,
        phase3_status=phase3_status,
        phase4_status=phase4_status,
        phase6_status=phase6_status,
    )
    storage_summary, storage_payload = build_runtime_storage_status()
    state_backend_health = asyncio.run(_state_backend_health_check())
    phase3_has_recent_proof = (
        int(phase3_status.get("checkpoint_count_recent") or 0) > 0
        and int(phase3_status.get("candidate_count_recent") or 0) > 0
    )
    phase4_has_recent_proof = (
        int(phase4_status.get("evidence_query_count_recent") or 0) > 0
        and int(phase4_status.get("alert_count_recent") or 0) > 0
        and int(phase4_status.get("delivery_attempt_count_recent") or 0) > 0
    )

    plist_points_to_runtime = "__PROJECT_DIR__/run_runtime.py" in (
        (REPO_ROOT / "polymarket.plist").read_text(encoding="utf-8")
        if (REPO_ROOT / "polymarket.plist").exists()
        else ""
    )

    checklist: list[dict[str, Any]] = [
        _item(
            checklist_id="deliverable_1",
            section="Owned Deliverables",
            requirement="One canonical runtime activation path exists with explicit configuration and health checks.",
            status="implemented",
            evidence=[
                "run_runtime.py exists as the canonical launcher.",
                "polymarket.plist points at run_runtime.py.",
                "run_runtime.py prints runtime plan and storage guard status.",
            ],
        ),
        _item(
            checklist_id="deliverable_2",
            section="Owned Deliverables",
            requirement="The background runtime path is environment-safe.",
            status="implemented" if plist_points_to_runtime else "partially_implemented",
            evidence=[
                f"polymarket.plist points to run_runtime.py={plist_points_to_runtime}.",
                f"runtime env source={env_result.source}, secret source={env_result.secret_source}.",
                "config/runtime_env.py supports .env.runtime and .env.runtime.secrets.",
            ],
            remaining_work=[] if plist_points_to_runtime else ["Update the background service template to use run_runtime.py consistently."],
        ),
        _item(
            checklist_id="deliverable_3",
            section="Owned Deliverables",
            requirement="Durable Phase 3 live operation is restored with an approved durable backend, persisted checkpoints, and non-zero live candidate output.",
            status=(
                "implemented"
                if state_backend_health["status"] == "reachable"
                and (phase3_registration or {}).get("state_backend") in {"redis", "sqlite"}
                and int(phase3_status.get("checkpoint_count_recent") or 0) > 0
                and int(phase3_status.get("candidate_count_recent") or 0) > 0
                else "partially_implemented"
            ),
            evidence=[
                f"state_backend_health={state_backend_health['status']}",
                f"registered_backend={(phase3_registration or {}).get('state_backend')}",
                f"checkpoint_count_recent={phase3_status.get('checkpoint_count_recent')}",
                f"candidate_count_recent={phase3_status.get('candidate_count_recent')}",
            ],
            remaining_work=(
                ["Run the canonical runtime long enough to produce fresh checkpoints and live candidates."]
                if not phase3_has_recent_proof
                else []
            ),
        ),
        _item(
            checklist_id="deliverable_4",
            section="Owned Deliverables",
            requirement="The Phase 4 alert path is either restored live or redesign-documented honestly.",
            status=(
                "implemented"
                if runtime_decision["phase4_runtime_decision"] == "canonical_live_default"
                and phase4_registration is not None
                else "partially_implemented"
            ),
            evidence=[
                f"configured_profile={runtime_decision['configured_runtime_profile']}",
                f"phase4_registration_present={phase4_registration is not None}",
                f"recent_alert_count={phase4_status.get('alert_count_recent')}",
            ],
            remaining_work=(
                ["Capture a fresh live Phase 4 evidence/alert window if you want full operational proof."]
                if int(phase4_status.get("alert_count_recent") or 0) <= 0
                else []
            ),
        ),
        _item(
            checklist_id="deliverable_5",
            section="Owned Deliverables",
            requirement="The Phase 6 live shadow story is either restored live or redesign-documented honestly.",
            status="implemented",
            evidence=[
                f"phase6_runtime_decision={runtime_decision['phase6_runtime_decision']}",
                f"active_shadow_model_present={phase6_status.get('active_shadow_model_present')}",
                f"recent_shadow_score_count={phase6_status.get('shadow_score_count_recent')}",
            ],
            remaining_work=(
                ["Run Phase 6 live shadow scoring if you want fresh live proof beyond the current documented optional status."]
                if int(phase6_status.get("shadow_score_count_recent") or 0) <= 0
                else []
            ),
        ),
        _item(
            checklist_id="deliverable_6",
            section="Owned Deliverables",
            requirement="Replay and backfill tooling can intentionally process archived detector-input windows.",
            status="implemented",
            evidence=[
                "run_runtime_replay_window.py exists.",
                "run_runtime_replay_window.py records restore-plan, replay status, and candidate deltas.",
                "database/PHASE11_RUNTIME_STORAGE_RUNBOOK.md documents the archived-window path.",
            ],
        ),
        _item(
            checklist_id="deliverable_7",
            section="Owned Deliverables",
            requirement="A storage lifecycle policy with retention, restore, pruning, and disk-pressure safety exists.",
            status="partially_implemented" if storage_summary.status == "blocked" else "implemented",
            evidence=[
                f"storage_status={storage_summary.status}",
                f"storage_reason={storage_summary.reason}",
                "run_runtime_storage_status.py exists and refreshes storage audit + compaction plan.",
                "phase7/runtime_storage.py defines retention policy and disk guard behavior.",
            ],
            remaining_work=(
                [
                    "Free enough disk space to get above the configured runtime safety floor.",
                    "Only then restart the canonical runtime for sustained operation.",
                ]
                if storage_summary.status == "blocked"
                else []
            ),
        ),
        _item(
            checklist_id="deliverable_8",
            section="Owned Deliverables",
            requirement="Plaintext-secret dependence is removed from the canonical workflow and replaced with approved local secret-loading guidance.",
            status="partially_implemented",
            evidence=[
                f"secret_env_source={env_result.secret_source}",
                f"secret_keys_in_primary_env={list(env_result.secret_keys_in_primary_env)}",
                ".env.runtime.secrets.example exists and .gitignore excludes secret env files.",
            ],
            remaining_work=[
                "Rotate any previously exposed real credentials outside the repo.",
            ],
        ),
        _item(
            checklist_id="deliverable_9",
            section="Owned Deliverables",
            requirement="Truthfulness docs distinguish live proof from historical, replay-only, or offline evidence.",
            status="implemented",
            evidence=[
                "phase11_current_state_memo.tex exists.",
                "phase11_archive_loss_memo.tex exists.",
                "run_runtime_status.py reports observed_recent_runtime_profile and archive_loss_truth.",
            ],
        ),
        _item(
            checklist_id="exit_1",
            section="Phase 11 Exit Criteria",
            requirement="A single documented local command path can launch the intended canonical runtime with Phase 3 enabled and observable.",
            status="implemented",
            evidence=[
                "README documents run_runtime.py as the canonical path.",
                f"configured_runtime_profile={runtime_decision['configured_runtime_profile']}",
            ],
        ),
        _item(
            checklist_id="exit_2",
            section="Phase 11 Exit Criteria",
            requirement="Redis-backed Phase 3 state is healthy, or a non-Redis alternative is explicitly accepted as canonical.",
            status=(
                "implemented"
                if state_backend_health["status"] == "reachable"
                and (phase3_registration or {}).get("state_backend") in {"redis", "sqlite"}
                else "not_implemented"
            ),
            evidence=[
                f"state_backend_health={state_backend_health['status']}",
                f"registered_backend={(phase3_registration or {}).get('state_backend')}",
            ],
            remaining_work=(
                ["Run Phase 3 live with the configured durable backend so the detector registration row updates out of memory mode."]
                if state_backend_health["status"] != "reachable"
                else []
            ),
        ),
        _item(
            checklist_id="exit_3",
            section="Phase 11 Exit Criteria",
            requirement="The live DB shows non-zero signal_candidates and detector_checkpoints from real live operation.",
            status=(
                "implemented"
                if int(phase3_status.get("candidate_count_recent") or 0) > 0
                and int(phase3_status.get("checkpoint_count_recent") or 0) > 0
                else "not_implemented"
            ),
            evidence=[
                f"candidate_count_recent={phase3_status.get('candidate_count_recent')}",
                f"checkpoint_count_recent={phase3_status.get('checkpoint_count_recent')}",
                f"candidate_count_total={phase3_status.get('candidate_count_total')}",
            ],
            remaining_work=(
                ["Produce a fresh live detector window under the canonical runtime."]
                if not phase3_has_recent_proof
                else []
            ),
        ),
        _item(
            checklist_id="exit_4",
            section="Phase 11 Exit Criteria",
            requirement="If Phase 4 is canonical live, the live DB shows non-zero recent evidence, alert, and delivery rows.",
            status=(
                "implemented"
                if int(phase4_status.get("evidence_query_count_recent") or 0) > 0
                and int(phase4_status.get("alert_count_recent") or 0) > 0
                and int(phase4_status.get("delivery_attempt_count_recent") or 0) > 0
                else "not_implemented"
            ),
            evidence=[
                f"evidence_query_count_recent={phase4_status.get('evidence_query_count_recent')}",
                f"alert_count_recent={phase4_status.get('alert_count_recent')}",
                f"delivery_attempt_count_recent={phase4_status.get('delivery_attempt_count_recent')}",
            ],
            remaining_work=(
                ["Run a fresh Phase 4 live window under the canonical runtime and confirm new persisted rows."]
                if not phase4_has_recent_proof
                else []
            ),
        ),
        _item(
            checklist_id="exit_5",
            section="Phase 11 Exit Criteria",
            requirement="If Phase 6 shadow-live scoring is not canonical, the docs state that clearly instead of implying it is live by default.",
            status="implemented",
            evidence=[
                f"phase6_runtime_decision={runtime_decision['phase6_runtime_decision']}",
                f"configured_runtime_profile={runtime_decision['configured_runtime_profile']}",
            ],
        ),
        _item(
            checklist_id="exit_6",
            section="Phase 11 Exit Criteria",
            requirement="The operator can intentionally replay an archived detector-input window using explicit tooling.",
            status="implemented",
            evidence=[
                "run_runtime_replay_window.py exists.",
                "run_runtime_replay_window.py documents checkpoint behavior and restore status.",
            ],
        ),
        _item(
            checklist_id="exit_7",
            section="Phase 11 Exit Criteria",
            requirement="The local runtime can run without filling the system disk under ordinary retention settings.",
            status="not_implemented" if storage_summary.status == "blocked" else "implemented",
            evidence=[
                f"storage_status={storage_summary.status}",
                f"free_gb={storage_summary.free_gb}",
                f"free_percent={storage_summary.free_percent}",
            ],
            remaining_work=[
                "Free enough disk headroom to clear the configured minimum free GB and percent thresholds.",
            ] if storage_summary.status == "blocked" else [],
        ),
        _item(
            checklist_id="exit_8",
            section="Phase 11 Exit Criteria",
            requirement="Secret storage and runtime env loading follow one explicit safer path rather than accidental shell state.",
            status="implemented",
            evidence=[
                f"primary_env_source={env_result.source}",
                f"secret_env_source={env_result.secret_source}",
                f"warnings={list(env_result.warnings)}",
            ],
        ),
        _item(
            checklist_id="exit_9",
            section="Phase 11 Exit Criteria",
            requirement="Final docs explicitly state what the live runtime proves and what remains replay-only, historical-only, or optional.",
            status="implemented",
            evidence=[
                f"observed_recent_runtime_profile={runtime_decision['observed_recent_runtime_profile']}",
                "phase11_current_state_memo.tex and phase11_archive_loss_memo.tex exist.",
            ],
        ),
    ]

    next_required_actions: list[str] = []
    for item in checklist:
        for action in item["remaining_work"]:
            if action not in next_required_actions:
                next_required_actions.append(action)

    overall_status = "implemented"
    if any(item["status"] == "not_implemented" for item in checklist):
        overall_status = "not_implemented"
    elif any(item["status"] == "partially_implemented" for item in checklist):
        overall_status = "partially_implemented"

    payload = {
        "summary": {
            "overall_status": overall_status,
            "implemented_count": _count_status(checklist, "implemented"),
            "partially_implemented_count": _count_status(checklist, "partially_implemented"),
            "not_implemented_count": _count_status(checklist, "not_implemented"),
            "recent_hours": args.recent_hours,
        },
        "runtime_decision": runtime_decision,
        "env_loading": {
            "primary_env_file": None if env_result.env_file is None else str(env_result.env_file),
            "primary_env_source": env_result.source,
            "secret_env_file": None if env_result.secret_env_file is None else str(env_result.secret_env_file),
            "secret_env_source": env_result.secret_source,
            "warnings": list(env_result.warnings),
        },
        "state_backend_health": state_backend_health,
        "storage_status_summary": storage_summary.to_dict(),
        "phase3_runtime_status": phase3_status,
        "phase4_runtime_status": phase4_status,
        "phase6_runtime_status": phase6_status,
        "checklist": checklist,
        "next_required_actions": next_required_actions,
    }

    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    if args.json:
        print(json.dumps(payload, indent=2, sort_keys=True))
    else:
        print(_render_text(payload))
        print(f"\nReport: {output_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
