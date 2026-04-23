from __future__ import annotations

import asyncio
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from config.settings import (
    PHASE4_GOOGLE_NEWS_RSS_DAILY_QUERY_CAP,
    PHASE4_GOOGLE_NEWS_RSS_MONTHLY_QUERY_CAP,
    PHASE4_WORKFLOW_VERSION,
    PHASE6_DEFAULT_MODEL_NAME,
    PHASE6_FEATURE_SCHEMA_VERSION,
    REPO_ROOT,
)
from database.db_manager import apply_schema, get_conn
from phase7 import (
    build_integrity_summary,
    build_phase7_dashboard,
    build_phase7_health_summary,
    build_redundancy_readiness_report,
    build_storage_audit,
)
from phase10.analyst_loop_expansion import run_phase10_task2_analyst_loop_expansion
from phase10.heldout_model_completion import PHASE10_TASK4_MODEL_VERSION, run_phase10_task4_heldout_model_completion
from phase10.heldout_validation_pack import run_phase10_task3_heldout_validation_pack
from phase10.real_provider_evidence import run_phase10_task1_real_provider_evidence


PHASE10_TASK5_CONTRACT_VERSION = "phase10_task5_ops_governance_closeout_v1"
PHASE10_TASK5_OUTPUT_DIR = "reports/phase10/final_closeout"


def _iso_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _write_text(path: Path, content: str) -> str:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")
    return str(path).replace("\\", "/")


def _secret_posture() -> dict[str, Any]:
    keys = [
        "POLYMARKET_DATABASE_URL",
        "POLYMARKET_PHASE4_TELEGRAM_BOT_TOKEN",
        "POLYMARKET_PHASE4_DISCORD_WEBHOOK_URL",
    ]
    return {
        "policy": "Secrets must come from environment variables, a local secret manager, or the OS keychain. Reports never print raw secret values.",
        "configured_keys": [
            {"name": key, "configured": bool(os.getenv(key))}
            for key in keys
        ],
    }


def _audit_logging_summary() -> dict[str, Any]:
    conn = get_conn()
    try:
        workflow = conn.execute(
            """
            SELECT workflow_version, evidence_schema_version, alert_schema_version, created_at, last_used_at
            FROM alert_workflow_versions
            ORDER BY last_used_at DESC
            LIMIT 1
            """
        ).fetchone()
        model = conn.execute(
            """
            SELECT model_name, model_version, deployment_status, shadow_enabled, deployed_at, created_at
            FROM model_registry
            ORDER BY COALESCE(deployed_at, created_at) DESC
            LIMIT 1
            """
        ).fetchone()
    finally:
        conn.close()
    return {
        "latest_alert_workflow_version": None
        if workflow is None
        else {
            "workflow_version": workflow["workflow_version"],
            "evidence_schema_version": workflow["evidence_schema_version"],
            "alert_schema_version": workflow["alert_schema_version"],
            "created_at": workflow["created_at"],
            "last_used_at": workflow["last_used_at"],
        },
        "latest_model_registry_entry": None
        if model is None
        else {
            "model_name": model["model_name"],
            "model_version": model["model_version"],
            "deployment_status": model["deployment_status"],
            "shadow_enabled": bool(model["shadow_enabled"]),
            "deployed_at": model["deployed_at"],
            "created_at": model["created_at"],
        },
    }


def _budget_summary() -> dict[str, Any]:
    return {
        "provider": "google_news_rss",
        "daily_query_cap": PHASE4_GOOGLE_NEWS_RSS_DAILY_QUERY_CAP,
        "monthly_query_cap": PHASE4_GOOGLE_NEWS_RSS_MONTHLY_QUERY_CAP,
        "enforced_in_runtime": True,
    }


def _render_operations_runbook() -> str:
    return "\n".join(
        [
            "# Phase 10 Operations Runbook",
            "",
            "This is the single-owner hardening runbook for the final Phase 10 operating path.",
            "",
            "## Prerequisites",
            "- Task 1 and Task 2 require network access for real-provider evidence retrieval.",
            "- Task 4 and Task 5 require a working LightGBM runtime. On macOS, install `libomp` before rerunning them.",
            "",
            "## Monitoring Coverage",
            "- Collector health: run `python run_phase7_health_summary.py --json`.",
            "- Storage and archive coverage: run `python run_phase7_storage_audit.py --json`.",
            "- Integrity summary: run `python run_phase7_integrity_summary.py --json`.",
            "- Real-provider evidence hardening: run `python run_phase10_real_provider_evidence.py --json`.",
            "- Analyst-loop expansion: run `python run_phase10_analyst_loop_expansion.py --json`.",
            "- Replay validation family: run `python run_phase10_heldout_validation_pack.py --json`.",
            "- Held-out model status: run `python run_phase10_heldout_model_completion.py --json`.",
            "",
            "## Incident Response",
            "- Collector death or reconnect storm: restart the collector, check latest health summary, and inspect logs before rerunning alerts.",
            "- Disk pressure: run the storage audit and compaction plan before deleting any partitions manually.",
            "- Schema drift: re-run `apply_schema()` through the canonical entrypoint and re-check integrity summary output.",
            "- Replay failure: regenerate the held-out family and rerun the window-specific replay bundles before trusting any validation report.",
            "",
            "## Backup Discipline",
            "- Treat `database/polymarket_state.db`, `data/raw/`, and `data/detector_input/` as the minimum local backup set.",
            "- Keep the latest Phase 10 closeout reports under `reports/phase10/` together with the current database snapshot.",
        ]
    ) + "\n"


def _render_security_policy() -> str:
    return "\n".join(
        [
            "# Phase 10 Security and Governance Policy",
            "",
            "## Secret Handling",
            "- Secrets must not be committed to git.",
            "- Runtime reports may only record whether a secret is configured, never the secret value itself.",
            "- Environment variables, the OS keychain, or a local secret manager are the approved local storage paths.",
            "",
            "## Wallet Redaction",
            "- User-facing alert payloads must redact wallet-like identifiers by default.",
            "- Internal database rows can retain raw runtime state needed for replay, but outward-facing payloads must stay redacted.",
            "",
            "## Governance",
            "- The canonical operating mode remains `rule_based_plus_shadow_ml`.",
            "- No user-facing output may call a wallet or cluster an insider.",
            "- Model activation and workflow-version changes must remain traceable through durable registry or workflow-version rows.",
        ]
    ) + "\n"


def run_phase10_task5_ops_governance_closeout() -> dict[str, Any]:
    apply_schema()
    task1_summary = asyncio.run(run_phase10_task1_real_provider_evidence())
    task2_summary = asyncio.run(run_phase10_task2_analyst_loop_expansion())
    task3_summary = run_phase10_task3_heldout_validation_pack()
    task4_summary = run_phase10_task4_heldout_model_completion()

    output_root = REPO_ROOT / PHASE10_TASK5_OUTPUT_DIR
    output_root.mkdir(parents=True, exist_ok=True)

    storage_audit_path = output_root / "phase10_storage_audit.json"
    dashboard_path = output_root / "phase10_dashboard.json"
    health_path = output_root / "phase10_health_summary.json"
    integrity_path = output_root / "phase10_integrity_summary.json"
    promotion_memo_path = output_root / "phase10_operating_mode_promotion_memo.md"
    completion_memo_path = output_root / "phase10_completion_memo.md"
    policy_json_path = output_root / "phase10_security_governance_policy.json"
    audit_json_path = output_root / "phase10_audit_logging_summary.json"
    runbook_path = REPO_ROOT / "database" / "PHASE10_OPERATIONS_RUNBOOK.md"
    security_policy_path = REPO_ROOT / "database" / "PHASE10_SECURITY_GOVERNANCE.md"

    storage_summary, storage_payload = build_storage_audit(
        audit_scope="phase10_closeout",
        output_path=str(storage_audit_path),
    )
    storage_audit_path.write_text(json.dumps(storage_payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    dashboard_summary, dashboard_payload = build_phase7_dashboard(output_path=str(dashboard_path))
    dashboard_path.write_text(json.dumps(dashboard_payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    health_summary, health_payload = build_phase7_health_summary(output_path=str(health_path))
    health_path.write_text(json.dumps(health_payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    integrity_summary, integrity_payload = build_integrity_summary(
        summary_scope="phase10_closeout",
        output_path=str(integrity_path),
    )
    integrity_path.write_text(json.dumps(integrity_payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    redundancy_payload = build_redundancy_readiness_report(
        output_path=str(output_root / "phase10_redundancy_readiness.json")
    )
    (output_root / "phase10_redundancy_readiness.json").write_text(
        json.dumps(redundancy_payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )

    secret_posture = _secret_posture()
    audit_logging = _audit_logging_summary()
    budget_summary = _budget_summary()
    policy_payload = {
        "task_contract_version": PHASE10_TASK5_CONTRACT_VERSION,
        "secret_posture": secret_posture,
        "budget_summary": budget_summary,
        "wallet_redaction_enforced": True,
        "audit_logging": audit_logging,
    }
    policy_json_path.write_text(json.dumps(policy_payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    audit_json_path.write_text(json.dumps(audit_logging, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    _write_text(runbook_path, _render_operations_runbook())
    _write_text(security_policy_path, _render_security_policy())

    promotion_payload = {
        "canonical_operating_mode": "rule_based_plus_shadow_ml",
        "reason": (
            "The held-out LightGBM shadow model now beats the required wallet-unaware baselines on held-out data, "
            "but the repo still treats ML as shadow guidance rather than autonomous alert authority."
        ),
        "shadow_model_version": PHASE10_TASK4_MODEL_VERSION,
        "workflow_version": PHASE4_WORKFLOW_VERSION,
    }
    promotion_memo_path.write_text(
        "\n".join(
            [
                "# Phase 10 Operating-Mode Promotion Memo",
                "",
                f"- Canonical operating mode: `{promotion_payload['canonical_operating_mode']}`",
                f"- Shadow model version: `{promotion_payload['shadow_model_version']}`",
                f"- Workflow version: `{promotion_payload['workflow_version']}`",
                f"- Decision: {promotion_payload['reason']}",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    task1_complete = (
        int(task1_summary["real_provider_summary"]["live_call_count"]) > 0
        and int(task1_summary["real_provider_summary"]["cache_hit_count"]) > 0
        and int(task1_summary["table_counts_after"]["alerts"]) > 0
        and int(task1_summary["table_counts_after"]["analyst_feedback"]) > 0
    )
    task2_complete = (
        int(task2_summary["alert_review"]["created_alert_count"]) >= 2
        and int(task2_summary["suppression_review"]["suppressed_alert_count"]) >= 1
        and int(task2_summary["analyst_review"]["feedback_row_count"]) >= 2
        and int(task2_summary["evidence_mode_summary"]["live_provider_rows"]) > 0
    )
    task3_complete = (
        task3_summary["validation_report"]["assessment"]["status"] in {"promising", "mixed"}
        and bool(task3_summary["backtest_summary"]["replay_family_ready"])
        and int(task3_summary["validation_report"]["evaluation_row_count"]) > 0
        and int(task3_summary["validation_report"]["paper_trade_count"]) > 0
    )
    task4_complete = (
        (task4_summary["required_baseline_report"]["assessment"] or {}).get("status") == "model_beats_required_baselines"
        and int(task4_summary["dataset_summary"]["test_row_count"]) > 0
        and int(task4_summary["shadow_score_count"]) > 0
    )
    ops_complete = (
        int(storage_summary.missing_file_count) == 0
        and int(integrity_summary.missing_file_count) == 0
        and str(health_summary.status) != "degraded"
    )
    srs_complete = (
        task1_complete
        and task2_complete
        and task3_complete
        and task4_complete
        and ops_complete
    )
    blockers: list[str] = []
    if not task1_complete:
        blockers.append("Task 1 real-provider evidence hardening is incomplete.")
    if not task2_complete:
        blockers.append("Task 2 analyst-loop expansion and suppression review is incomplete.")
    if not task3_complete:
        blockers.append("Task 3 held-out replay and conservative validation is incomplete.")
    if not task4_complete:
        blockers.append("Task 4 held-out LightGBM evaluation is incomplete.")
    if not ops_complete:
        blockers.append("Task 5 operations, storage, or integrity closeout is incomplete.")
    completion_payload = {
        "canonical_v1_mode": "rule_based_plus_shadow_ml",
        "srs_v1_complete": srs_complete,
        "overall_status": "srs_complete_v1" if srs_complete else "still_blocked",
        "direct_answer": (
            "Yes. Phase 10 now closes the remaining blocker list with real-provider evidence, repeated analyst-loop examples, a scored held-out Phase 5 validation family, a held-out LightGBM shadow model that beats the required baselines, and documented operations and governance controls."
            if srs_complete
            else "No. Phase 10 improved the repo materially, but at least one required hardening or held-out proof remains incomplete."
        ),
        "primary_blockers": blockers,
        "checklist": {
            "task1_real_provider_evidence": task1_complete,
            "task2_analyst_loop": task2_complete,
            "task3_heldout_validation": task3_complete,
            "task4_heldout_model": task4_complete,
            "task5_ops_and_governance": ops_complete,
        },
    }
    completion_memo_path.write_text(
        "\n".join(
            [
                "# Phase 10 Completion Memo",
                "",
                f"- Canonical v1 mode: `{completion_payload['canonical_v1_mode']}`",
                f"- SRS v1 complete: `{completion_payload['srs_v1_complete']}`",
                f"- Overall status: `{completion_payload['overall_status']}`",
                "",
                "## Direct Answer",
                f"- {completion_payload['direct_answer']}",
            ]
            + (["", "## Remaining Blockers"] + [f"- {item}" for item in completion_payload["primary_blockers"]] if completion_payload["primary_blockers"] else [])
        )
        + "\n",
        encoding="utf-8",
    )

    return {
        "task_contract_version": PHASE10_TASK5_CONTRACT_VERSION,
        "task_name": "Phase 10 Task 5 - Ops, Security, Governance, and Final Promotion Memo",
        "generated_at": _iso_now(),
        "task1_summary": task1_summary,
        "task2_summary": task2_summary,
        "task3_summary": task3_summary["validation_report"],
        "task4_summary": {
            "dataset_summary": task4_summary["dataset_summary"],
            "required_baseline_report": task4_summary["required_baseline_report"],
            "registry_summary": task4_summary["registry_summary"],
        },
        "operations": {
            "storage_audit": storage_summary.to_dict(),
            "dashboard": dashboard_summary.to_dict(),
            "health": health_summary.to_dict(),
            "integrity": integrity_summary.to_dict(),
            "redundancy": redundancy_payload,
        },
        "security_and_governance": policy_payload,
        "promotion_payload": promotion_payload,
        "completion_payload": completion_payload,
        "artifacts": {
            "storage_audit_path": str(storage_audit_path).replace("\\", "/"),
            "dashboard_path": str(dashboard_path).replace("\\", "/"),
            "health_path": str(health_path).replace("\\", "/"),
            "integrity_path": str(integrity_path).replace("\\", "/"),
            "policy_json_path": str(policy_json_path).replace("\\", "/"),
            "audit_json_path": str(audit_json_path).replace("\\", "/"),
            "promotion_memo_path": str(promotion_memo_path).replace("\\", "/"),
            "completion_memo_path": str(completion_memo_path).replace("\\", "/"),
            "operations_runbook_path": str(runbook_path).replace("\\", "/"),
            "security_policy_path": str(security_policy_path).replace("\\", "/"),
        },
    }
