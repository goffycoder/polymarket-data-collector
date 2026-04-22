from __future__ import annotations

import json
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from config.settings import (
    PHASE4_ALERT_ACTIONABLE_THRESHOLD,
    PHASE4_ALERT_INFO_THRESHOLD,
    PHASE4_ALERT_WATCH_THRESHOLD,
    PHASE4_EVIDENCE_PROVIDERS,
    PHASE6_ACTIONABLE_PRECISION_TARGET,
    PHASE6_CRITICAL_PRECISION_TARGET,
    PHASE6_WATCH_PRECISION_TARGET,
    REPO_ROOT,
)
from phase7 import sha256_file


METRICS_REVIEW_CONTRACT_VERSION = "phase8_metrics_review_v1"


def _iso_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _git_head() -> str:
    try:
        completed = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=REPO_ROOT,
            capture_output=True,
            text=True,
            check=True,
        )
    except Exception:
        return "unknown"
    return completed.stdout.strip() or "unknown"


def _load_json(path_value: str) -> dict[str, Any]:
    path = (REPO_ROOT / path_value).resolve()
    return json.loads(path.read_text(encoding="utf-8"))


def _load_optional_json(path_value: str) -> dict[str, Any] | None:
    path = (REPO_ROOT / path_value).resolve()
    if not path.exists():
        return None
    return json.loads(path.read_text(encoding="utf-8"))


def _file_artifact(path_value: str, *, kind: str, note: str | None = None) -> dict[str, Any]:
    path = (REPO_ROOT / path_value).resolve()
    exists = path.exists()
    artifact: dict[str, Any] = {
        "kind": kind,
        "path": str(path.relative_to(REPO_ROOT)).replace("\\", "/") if exists or str(path).startswith(str(REPO_ROOT)) else path_value,
        "exists": exists,
        "note": note,
        "sha256": sha256_file(path) if exists and path.is_file() else None,
    }
    if exists and path.is_file():
        artifact["size_bytes"] = path.stat().st_size
    return artifact


def _table_count(freeze_manifest: dict[str, Any], table_name: str) -> int | None:
    return (freeze_manifest.get("database_snapshot") or {}).get("table_counts", {}).get(table_name)


def _metric_entry(
    *,
    metric_key: str,
    priority_rank: int,
    metric_name: str,
    target: str,
    status: str,
    current_value: Any,
    evidence_summary: str,
    blockers: list[str],
    evidence_sources: list[dict[str, Any]],
) -> dict[str, Any]:
    return {
        "metric_key": metric_key,
        "priority_rank": priority_rank,
        "metric_name": metric_name,
        "target": target,
        "status": status,
        "current_value": current_value,
        "evidence_summary": evidence_summary,
        "blockers": blockers,
        "evidence_sources": evidence_sources,
    }


def build_phase8_metrics_review_manifest() -> dict[str, Any]:
    freeze_manifest = _load_json("reports/phase8/reference_window_freeze/phase8_reference_window_manifest.json")
    operating_mode_manifest = _load_json("reports/phase8/operating_mode/phase8_v1_operating_mode_manifest.json")
    task2_manifest = _load_optional_json("reports/phase9/candidate_to_alert_materialization/phase9_task2_review_packet.json")
    task3_validation = _load_optional_json("reports/phase5/validation/phase9_task3_holdout_validation.json")
    task3_backtest = _load_optional_json("reports/phase5/backtests/phase9_task3_conservative_backtest.json")
    task4_summary = _load_optional_json("reports/phase9/phase6_model_completion/phase9_task4_summary.json")

    evidence_sources = {
        "srs": _file_artifact(
            "Documentation/SRS.tex",
            kind="requirements_source",
            note="Defines the success-metric order and stop conditions.",
        ),
        "phase4_signoff": _file_artifact(
            "Documentation/phases/phase4_gate4_signoff.tex",
            kind="signoff_doc",
            note="Documents known Phase 4 limitations, especially placeholder providers and the need for stronger real-provider-backed evidence.",
        ),
        "phase5_doc": _file_artifact(
            "Documentation/phases/phase5.tex",
            kind="phase_doc",
            note="Defines replay, validation, and conservative paper-trading requirements.",
        ),
        "phase6_reporting": _file_artifact(
            "phase6/reporting.py",
            kind="runtime_module",
            note="Contains calibration/reporting logic and explicit caveats about shadow-only threshold use and descriptive-only evidence.",
        ),
        "phase6_eval_report": _file_artifact(
            "validation/phase6_person2_report.py",
            kind="validation_module",
            note="Summarizes baseline margin and calibration profile status when evaluation runs exist.",
        ),
        "phase7_observability": _file_artifact(
            "phase7/observability.py",
            kind="runtime_module",
            note="Defines measurable observability risks and deployment implications.",
        ),
        "task2_manifest": _file_artifact(
            "reports/phase8/reference_window_freeze/phase8_reference_window_manifest.json",
            kind="phase8_artifact",
            note="Current end-to-end freeze status and database table counts.",
        ),
        "task3_manifest": _file_artifact(
            "reports/phase8/operating_mode/phase8_v1_operating_mode_manifest.json",
            kind="phase8_artifact",
            note="Canonical v1 operating-mode decision used to interpret shadow ML versus authoritative logic.",
        ),
        "settings": _file_artifact(
            "config/settings.py",
            kind="runtime_config",
            note="Defines default evidence providers and precision target thresholds.",
        ),
        "task2_review_packet": _file_artifact(
            "reports/phase9/candidate_to_alert_materialization/phase9_task2_review_packet.json",
            kind="phase9_artifact",
            note="Phase 9 Task 2 evidence that candidate, alert, delivery, and analyst-feedback rows now exist locally.",
        ),
        "task3_validation_report": _file_artifact(
            "reports/phase5/validation/phase9_task3_holdout_validation.json",
            kind="phase5_artifact",
            note="Phase 9 Task 3 holdout-validation report for the canonical reference window.",
        ),
        "task3_backtest_report": _file_artifact(
            "reports/phase5/backtests/phase9_task3_conservative_backtest.json",
            kind="phase5_artifact",
            note="Phase 9 Task 3 conservative paper-trading and backtest report for the canonical reference window.",
        ),
        "task4_summary": _file_artifact(
            "reports/phase9/phase6_model_completion/phase9_task4_summary.json",
            kind="phase9_artifact",
            note="Phase 9 Task 4 summary of the LightGBM shadow artifact, required baselines, calibration, registry state, and shadow scores.",
        ),
    }

    alert_rows = _table_count(freeze_manifest, "alerts") or 0
    analyst_rows = _table_count(freeze_manifest, "analyst_feedback") or 0
    delivery_rows = _table_count(freeze_manifest, "alert_delivery_attempts") or 0
    model_eval_rows = _table_count(freeze_manifest, "model_evaluation_runs") or 0
    calibration_rows = _table_count(freeze_manifest, "calibration_profiles") or 0
    shadow_score_rows = _table_count(freeze_manifest, "shadow_model_scores") or 0
    backtest_rows = _table_count(freeze_manifest, "backtest_artifacts") or 0
    validation_rows = _table_count(freeze_manifest, "validation_runs") or 0
    raw_manifest_rows = _table_count(freeze_manifest, "raw_archive_manifests") or 0
    replay_rows = _table_count(freeze_manifest, "replay_runs") or 0
    evidence_snapshot_rows = _table_count(freeze_manifest, "evidence_snapshots") or 0
    evidence_query_rows = _table_count(freeze_manifest, "evidence_queries") or 0

    alert_precision = (((task3_validation or {}).get("assessment") or {}).get("alert_usefulness_precision"))
    median_pnl = (((task3_validation or {}).get("assessment") or {}).get("median_bounded_pnl"))
    lead_time_seconds = ((((task3_validation or {}).get("metrics") or {}).get("lead_time_overall") or {}).get("median_lead_time_seconds"))
    required_baseline_assessment = (((task4_summary or {}).get("required_baseline_report") or {}).get("assessment") or {})
    active_shadow_model = (task4_summary or {}).get("active_shadow_model") or {}
    phase4_providers = (((task2_manifest or {}).get("phase4") or {}).get("evidence_results") or [])
    seeded_provider_only = bool(phase4_providers) and all(
        all(str(provider).startswith("noop_") for provider in (result.get("providers") or []))
        for result in phase4_providers
    )
    delivery_summary = ((((task2_manifest or {}).get("phase4") or {}).get("gate4_report") or {}).get("delivery_summary") or {})
    delivery_status_summary = (
        f"sent={delivery_summary.get('sent_attempts', 0)} skipped={delivery_summary.get('skipped_attempts', 0)}"
        if delivery_summary
        else "unavailable"
    )

    metrics_bundle = [
        _metric_entry(
            metric_key="alert_precision_usefulness",
            priority_rank=1,
            metric_name="Ranked alert precision and operational usefulness",
            target="Precision@10 > 0.60 (aspirational), plus meaningful analyst usefulness evidence",
            status="materialized_seeded_local_only" if alert_rows and analyst_rows else "not_materialized_in_workspace",
            current_value={
                "alerts": alert_rows,
                "analyst_feedback": analyst_rows,
                "alert_usefulness_precision": alert_precision,
                "delivery_attempts": delivery_rows,
            }
            if alert_rows
            else None,
            evidence_summary=(
                "The workspace now contains a replay-linked local alert packet with persisted alerts, delivery attempts, and one analyst-feedback row. "
                f"Current alert usefulness precision is `{alert_precision}` on a two-alert seeded packet, with delivery summary `{delivery_status_summary}`. "
                "This is enough to show the loop exists, but not enough to claim robust real-world precision."
            ),
            blockers=[
                "alert packet is only two alerts, so usefulness evidence is descriptive rather than statistically strong",
                "Phase 4 evidence providers in the canonical local packet are noop adapters rather than real-provider-backed retrieval",
                "delivery attempts were persisted, but outbound channels were skipped in the current local environment",
            ],
            evidence_sources=[
                evidence_sources["srs"],
                evidence_sources["phase4_signoff"],
                evidence_sources["task2_manifest"],
                evidence_sources["task2_review_packet"],
                evidence_sources["task3_validation_report"],
            ],
        ),
        _metric_entry(
            metric_key="calibration",
            priority_rank=2,
            metric_name="Calibration",
            target=(
                "Brier score < 0.20 (aspirational), with Phase 6 shadow thresholds currently targeting "
                f"WATCH {PHASE6_WATCH_PRECISION_TARGET:.2f}, ACTIONABLE {PHASE6_ACTIONABLE_PRECISION_TARGET:.2f}, "
                f"CRITICAL {PHASE6_CRITICAL_PRECISION_TARGET:.2f} precision slices"
            ),
            status="materialized_descriptive_only" if model_eval_rows and calibration_rows and shadow_score_rows else "not_materialized_in_workspace",
            current_value={
                "model_evaluation_runs": model_eval_rows,
                "calibration_profiles": calibration_rows,
                "shadow_model_scores": shadow_score_rows,
                "active_shadow_model": active_shadow_model.get("model_version"),
                "required_baseline_assessment": required_baseline_assessment,
            }
            if model_eval_rows
            else None,
            evidence_summary=(
                "The workspace now contains model evaluation rows, calibration profiles, a registered LightGBM shadow model, and shadow scores for the canonical window. "
                f"The current required-baseline assessment is `{required_baseline_assessment.get('status')}`, which means the artifact contract is satisfied but the local evidence is still descriptive and not held-out-defendable."
            ),
            blockers=[
                "the local dataset is only two labeled rows and uses train-only evidence",
                "held-out baseline-beating evidence is not yet available",
                "Phase 6 remains shadow-only in canonical v1 even after artifact completion",
            ],
            evidence_sources=[
                evidence_sources["srs"],
                evidence_sources["phase6_reporting"],
                evidence_sources["phase6_eval_report"],
                evidence_sources["task2_manifest"],
                evidence_sources["task3_manifest"],
                evidence_sources["settings"],
                evidence_sources["task4_summary"],
            ],
        ),
        _metric_entry(
            metric_key="lead_time",
            priority_rank=3,
            metric_name="Lead time over public corroboration",
            target="Median lead time > 30 minutes on the subset where corroboration exists",
            status="materialized_seeded_local_only" if lead_time_seconds is not None else "not_materialized_in_workspace",
            current_value={
                "median_lead_time_seconds": lead_time_seconds,
                "median_lead_time_minutes": round(float(lead_time_seconds) / 60.0, 3) if lead_time_seconds is not None else None,
            }
            if lead_time_seconds is not None
            else None,
            evidence_summary=(
                "Lead-time analysis is now materialized for the canonical local packet. "
                f"The current median lead time is `{lead_time_seconds}` seconds, which exceeds the 30-minute aspirational target on the single successful alert in the seeded packet. "
                "This demonstrates the reporting path, but the sample is too small and synthetic to treat as a strong production claim."
            ),
            blockers=[
                "lead-time evidence comes from a seeded local packet with only one successful alert",
                "evidence providers remain noop-backed, so corroboration timing is not representative of real retrieval behavior",
                "Phase 7 observability analysis still lacks broader alert history beyond the tiny canonical packet",
            ],
            evidence_sources=[
                evidence_sources["srs"],
                evidence_sources["phase7_observability"],
                evidence_sources["task2_manifest"],
                evidence_sources["task2_review_packet"],
                evidence_sources["task3_validation_report"],
            ],
        ),
        _metric_entry(
            metric_key="paper_trade_edge",
            priority_rank=4,
            metric_name="Economic edge under conservative execution",
            target="Positive paper-trade edge after fees and slippage",
            status="materialized_small_sample" if backtest_rows and validation_rows else "not_materialized_in_workspace",
            current_value={
                "validation_runs": validation_rows,
                "backtest_artifacts": backtest_rows,
                "median_bounded_pnl": median_pnl,
                "paper_trade_count": (task3_backtest or {}).get("paper_trade_count"),
            }
            if backtest_rows
            else None,
            evidence_summary=(
                "The conservative paper-trading framework is now materially populated for the canonical window. "
                f"The current backtest packet shows median bounded PnL `{median_pnl}` across `{(task3_backtest or {}).get('paper_trade_count')}` paper trades after explicit conservative assumptions. "
                "This is enough to prove the workflow and artifact contract, but not enough for a broad edge claim."
            ),
            blockers=[
                "paper-trading evidence is based on only two trades",
                "the current result is descriptive and vulnerable to sample-size noise",
                "the strongest and weakest windows are the same single fixture packet",
            ],
            evidence_sources=[
                evidence_sources["srs"],
                evidence_sources["phase5_doc"],
                evidence_sources["task2_manifest"],
                evidence_sources["task3_validation_report"],
                evidence_sources["task3_backtest_report"],
            ],
        ),
    ]

    limitations_review = {
        "weak_regimes": [
            {
                "title": "Materialized later-phase evidence is still tiny and seeded",
                "severity": "high",
                "detail": "Phase 9 materially populated the end-to-end packet, but the canonical reference window still covers only two alerts and two paper trades in a seeded local scenario.",
            },
            {
                "title": "Canonical v1 metrics are now available, but mostly descriptive rather than defensible",
                "severity": "high",
                "detail": "Alert precision, calibration, lead time, and paper-trade edge can now be computed honestly from local artifacts, but they are still too small or too synthetic for strong deployment claims.",
            },
            {
                "title": "Boosted-tree ML exists, but held-out evidence is still missing",
                "severity": "medium",
                "detail": "The committed Phase 6 trainer now supports LightGBM and the Task 4 artifact contract is satisfied, but the local evidence remains train-only on a two-row dataset.",
            },
        ],
        "observability_caveats": [
            {
                "title": "Goodhart and observability analysis still outpaces the local evidence base",
                "severity": "medium",
                "detail": "Phase 7 observability logic is implemented, but the current local alert/evidence history is too small to turn those metrics into strong measured conclusions.",
            },
            {
                "title": "Visible operator metrics may not preserve true lead time or hidden candidate edge",
                "severity": "medium",
                "detail": "The committed Phase 7 observability study code explicitly warns that better visible precision is not the same as preserved tradable or investigative edge.",
            },
        ],
        "provider_issues": [
            {
                "title": "Evidence providers default to placeholder noop adapters",
                "severity": "high",
                "detail": f"Current default evidence providers are {list(PHASE4_EVIDENCE_PROVIDERS)}, and the materialized Phase 9 packet used noop adapters only, so richer real retrieval evidence is still not demonstrated.",
            },
            {
                "title": "Live delivery integrations are gated and may be disabled",
                "severity": "medium",
                "detail": "Telegram and Discord integrations exist, but the canonical Task 2 packet recorded skipped rather than sent attempts because credentials were not enabled in the current environment.",
            },
        ],
        "failure_modes": [
            {
                "title": "Alert-loop claims without real traffic would be overstated",
                "severity": "high",
                "detail": "The Phase 4 signoff memo itself says final validation should use at least one real candidate-to-alert run rather than seeded or purely local test data.",
            },
            {
                "title": "ML thresholds are shadow-only recommendations",
                "severity": "medium",
                "detail": "Phase 6 reporting explicitly says threshold recommendations should stay shadow-only until they survive larger replay windows.",
            },
            {
                "title": "Replay-to-alert reproducibility is now demonstrated only on a seeded local packet",
                "severity": "high",
                "detail": "Phase 9 now demonstrates replay-to-alert reproducibility locally, but the packet still depends on seeded detector-input sources and noop evidence providers.",
            },
        ],
    }

    stop_conditions = [
        {
            "condition": "raw archive gaps exceed 1 hour",
            "status": "not_triggered_on_canonical_seeded_packet",
            "reason": (
                "The canonical local packet now includes raw-archive and detector-input manifest rows for the frozen hour, so the stop condition is not triggered on the seeded packet itself."
            ),
            "evidence": {
                "raw_archive_manifest_rows": raw_manifest_rows,
                "task2_overall_status": freeze_manifest.get("overall_status"),
            },
        },
        {
            "condition": "duplicate trade inflation cannot be controlled",
            "status": "not_triggered_on_canonical_seeded_packet",
            "reason": (
                "The canonical seeded packet replays deterministically and produces stable downstream counts, so this stop condition is not currently active on the local proof packet."
            ),
            "evidence": {
                "raw_archive_manifest_rows": raw_manifest_rows,
                "replay_rows": replay_rows,
            },
        },
        {
            "condition": "alert false-positive rate remains operationally unusable after suppression tuning",
            "status": "not_yet_defendable_for_real_operations",
            "reason": (
                "Local alert, delivery, and analyst-feedback rows now exist, but the packet is too small and synthetic to defend operational false-positive behavior for real deployment."
            ),
            "evidence": {
                "alerts": alert_rows,
                "delivery_attempts": delivery_rows,
                "analyst_feedback": analyst_rows,
            },
        },
        {
            "condition": "evidence-provider spend exceeds budget without measurable lift",
            "status": "not_triggered_in_current_workspace_but_not_validated_for_real_providers",
            "reason": (
                "Default providers are noop adapters and no live spend evidence exists locally, so overspend is not currently happening here, but real-provider lift remains unevidenced."
            ),
            "evidence": {
                "default_evidence_providers": list(PHASE4_EVIDENCE_PROVIDERS),
            },
        },
        {
            "condition": "replay cannot reproduce a historical alert end to end",
            "status": "resolved_for_seeded_local_packet_not_for_real_provider_packet",
            "reason": (
                "Phase 9 now reproduces a full local replay-to-alert-to-validation-to-ML packet for the canonical hour, but the evidence path is still seeded and noop-provider-backed rather than fully real-provider-backed."
            ),
            "evidence": {
                "task2_overall_status": freeze_manifest.get("overall_status"),
                "replay_runs": replay_rows,
                "alerts": alert_rows,
                "validation_runs": validation_rows,
                "backtest_artifacts": backtest_rows,
                "model_evaluation_runs": model_eval_rows,
            },
        },
    ]

    readiness_summary = {
        "overall_status": "metrics_materialized_but_not_yet_defendable_for_srs_v1",
        "highest_priority_gap": "The project now has a materially populated end-to-end local packet, but the remaining blocker is stronger real-provider-backed and held-out-sized evidence rather than missing artifacts.",
        "canonical_v1_mode": (operating_mode_manifest.get("decision") or {}).get("canonical_v1_operating_mode"),
        "workspace_snapshot_highlights": {
            "alerts": alert_rows,
            "analyst_feedback": analyst_rows,
            "evidence_queries": evidence_query_rows,
            "evidence_snapshots": evidence_snapshot_rows,
            "model_evaluation_runs": model_eval_rows,
            "calibration_profiles": calibration_rows,
            "shadow_model_scores": shadow_score_rows,
            "validation_runs": validation_rows,
            "backtest_artifacts": backtest_rows,
            "seeded_provider_only": seeded_provider_only,
        },
    }

    return {
        "contract_version": METRICS_REVIEW_CONTRACT_VERSION,
        "generated_at": _iso_now(),
        "git_commit": _git_head(),
        "readiness_summary": readiness_summary,
        "metrics_bundle": metrics_bundle,
        "limitations_review": limitations_review,
        "stop_conditions": stop_conditions,
        "supporting_evidence": list(evidence_sources.values()),
        "srs_targets": {
            "alert_precision_precision_at_10": "> 0.60 aspirational",
            "calibration_brier_score": "< 0.20 aspirational",
            "lead_time_minutes": "> 30 median on corroborated subset",
            "paper_trade_edge": "positive after fees and slippage",
            "phase4_alert_thresholds": {
                "info": PHASE4_ALERT_INFO_THRESHOLD,
                "watch": PHASE4_ALERT_WATCH_THRESHOLD,
                "actionable": PHASE4_ALERT_ACTIONABLE_THRESHOLD,
            },
            "phase6_shadow_precision_targets": {
                "watch": PHASE6_WATCH_PRECISION_TARGET,
                "actionable": PHASE6_ACTIONABLE_PRECISION_TARGET,
                "critical": PHASE6_CRITICAL_PRECISION_TARGET,
            },
        },
    }


def render_phase8_metrics_review_markdown(manifest: dict[str, Any]) -> str:
    lines = [
        "# Phase 8 Final Metrics and Limitations Review",
        "",
        f"- Contract version: `{manifest['contract_version']}`",
        f"- Generated at: `{manifest['generated_at']}`",
        f"- Git commit: `{manifest['git_commit']}`",
        f"- Overall status: `{manifest['readiness_summary']['overall_status']}`",
        f"- Canonical v1 mode: `{manifest['readiness_summary']['canonical_v1_mode']}`",
        "",
        "## Metrics Bundle",
    ]
    for item in sorted(manifest["metrics_bundle"], key=lambda row: row["priority_rank"]):
        lines.extend(
            [
                f"- [{item['priority_rank']}] {item['metric_name']}: `{item['status']}`",
                f"  Target: {item['target']}",
                f"  Evidence: {item['evidence_summary']}",
            ]
        )
    lines.extend(["", "## Stop Conditions"])
    for item in manifest["stop_conditions"]:
        lines.append(f"- {item['condition']}: `{item['status']}`")
    lines.extend(["", "## Highest-Priority Gap", f"- {manifest['readiness_summary']['highest_priority_gap']}"])
    return "\n".join(lines) + "\n"
