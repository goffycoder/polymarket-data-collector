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

    evidence_sources = {
        "srs": _file_artifact(
            "Documentation/SRS.tex",
            kind="requirements_source",
            note="Defines the success-metric order and stop conditions.",
        ),
        "phase4_signoff": _file_artifact(
            "Documentation/phases/phase4_gate4_signoff.tex",
            kind="signoff_doc",
            note="Documents known Phase 4 limitations, especially placeholder providers and missing real traffic evidence.",
        ),
        "phase5_doc": _file_artifact(
            "Documentation/phases/phase5.tex",
            kind="phase_doc",
            note="Defines replay, validation, and conservative paper-trading requirements.",
        ),
        "phase6_reporting": _file_artifact(
            "phase6/reporting.py",
            kind="runtime_module",
            note="Contains calibration/reporting logic and explicit caveats about the starter ranker.",
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

    metrics_bundle = [
        _metric_entry(
            metric_key="alert_precision_usefulness",
            priority_rank=1,
            metric_name="Ranked alert precision and operational usefulness",
            target="Precision@10 > 0.60 (aspirational), plus meaningful analyst usefulness evidence",
            status="not_materialized_in_workspace",
            current_value=None,
            evidence_summary=(
                "No persisted alerts, delivery attempts, or analyst feedback rows are present in the current workspace snapshot, "
                "so alert precision/usefulness cannot be computed honestly."
            ),
            blockers=[
                "alerts table row count is 0",
                "alert_delivery_attempts table row count is 0",
                "analyst_feedback table row count is 0",
                "Phase 4 signoff itself says final evidence should come from a real live candidate-to-alert run",
            ],
            evidence_sources=[
                evidence_sources["srs"],
                evidence_sources["phase4_signoff"],
                evidence_sources["task2_manifest"],
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
            status="not_materialized_in_workspace",
            current_value=None,
            evidence_summary=(
                "No model evaluation runs, calibration profiles, or shadow-score rows are present locally. "
                "The code supports calibration, but no committed local evidence proves current calibration quality."
            ),
            blockers=[
                "model_evaluation_runs table row count is 0",
                "calibration_profiles table row count is 0",
                "shadow_model_scores table row count is 0",
                "Task 3 keeps ML in shadow mode only for canonical v1",
            ],
            evidence_sources=[
                evidence_sources["srs"],
                evidence_sources["phase6_reporting"],
                evidence_sources["phase6_eval_report"],
                evidence_sources["task2_manifest"],
                evidence_sources["task3_manifest"],
                evidence_sources["settings"],
            ],
        ),
        _metric_entry(
            metric_key="lead_time",
            priority_rank=3,
            metric_name="Lead time over public corroboration",
            target="Median lead time > 30 minutes on the subset where corroboration exists",
            status="not_materialized_in_workspace",
            current_value=None,
            evidence_summary=(
                "Lead-time analysis requires real alerts, evidence states, and often shadow-score or review data. "
                "Those artifacts are absent in the current workspace snapshot."
            ),
            blockers=[
                "alerts table row count is 0",
                "evidence_snapshots table row count is 0",
                "evidence_queries table row count is 0",
                "Phase 7 observability study cannot be meaningfully populated without alert/evidence history",
            ],
            evidence_sources=[
                evidence_sources["srs"],
                evidence_sources["phase7_observability"],
                evidence_sources["task2_manifest"],
            ],
        ),
        _metric_entry(
            metric_key="paper_trade_edge",
            priority_rank=4,
            metric_name="Economic edge under conservative execution",
            target="Positive paper-trade edge after fees and slippage",
            status="not_materialized_in_workspace",
            current_value=None,
            evidence_summary=(
                "The conservative paper-trading framework exists by design, but this workspace contains no backtest artifacts or validation runs that would justify a PnL or edge claim."
            ),
            blockers=[
                "backtest_artifacts table row count is 0",
                "validation_runs table row count is 0",
                "reports/phase5 output root is not materialized",
            ],
            evidence_sources=[
                evidence_sources["srs"],
                evidence_sources["phase5_doc"],
                evidence_sources["task2_manifest"],
            ],
        ),
    ]

    limitations_review = {
        "weak_regimes": [
            {
                "title": "No materialized later-phase runtime evidence in the current workspace",
                "severity": "high",
                "detail": "Phase 2 through Phase 7 tables are present structurally but populated counts are zero in the committed local SQLite snapshot.",
            },
            {
                "title": "Canonical v1 metrics are mostly unavailable rather than merely weak",
                "severity": "high",
                "detail": "Alert precision, calibration, lead time, and paper-trade edge cannot be computed honestly from the current workspace because the required runtime artifacts are absent.",
            },
            {
                "title": "Current ML implementation is still a starter baseline",
                "severity": "medium",
                "detail": "The committed Phase 6 trainer fits a linear starter ranker, and the reporting layer explicitly says it is not yet the final boosted-tree model promised by the full Phase 6 scope.",
            },
        ],
        "observability_caveats": [
            {
                "title": "Goodhart and observability analysis exists as code and policy, not as current measured evidence",
                "severity": "medium",
                "detail": "Phase 7 observability logic is implemented, but no local alert/evidence history exists to populate those metrics in this workspace snapshot.",
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
                "detail": f"Current default evidence providers are {list(PHASE4_EVIDENCE_PROVIDERS)}, which means the persistence path exists but richer real retrieval evidence is not the default runtime state.",
            },
            {
                "title": "Live delivery integrations are gated and may be disabled",
                "severity": "medium",
                "detail": "Telegram and Discord integrations exist, but the settings default to disabled unless credentials are configured.",
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
                "title": "Replay-to-alert reproducibility is not yet materially demonstrated in this workspace",
                "severity": "high",
                "detail": "Task 2 froze the path definition but marked the overall reference chain as missing runtime outputs.",
            },
        ],
    }

    stop_conditions = [
        {
            "condition": "raw archive gaps exceed 1 hour",
            "status": "unresolved_in_current_workspace",
            "reason": (
                "No raw archive partitions or manifest rows are present locally, so current archive-gap risk cannot be assessed from this workspace snapshot."
            ),
            "evidence": {
                "raw_archive_manifest_rows": raw_manifest_rows,
                "task2_overall_status": freeze_manifest.get("overall_status"),
            },
        },
        {
            "condition": "duplicate trade inflation cannot be controlled",
            "status": "unresolved_in_current_workspace",
            "reason": (
                "The repo contains the Phase 1 validation framework, but no current populated trade data or recent validation outputs are present locally to prove duplicate inflation is bounded."
            ),
            "evidence": {
                "raw_archive_manifest_rows": raw_manifest_rows,
                "replay_rows": replay_rows,
            },
        },
        {
            "condition": "alert false-positive rate remains operationally unusable after suppression tuning",
            "status": "unresolved_in_current_workspace",
            "reason": (
                "There are no persisted alerts, delivery attempts, or analyst feedback rows in the workspace, so operational false-positive behavior cannot be measured yet."
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
            "status": "active_concern",
            "reason": (
                "Task 2's frozen reference path concluded with missing runtime outputs across replay, candidate, alert, validation, and ML/research stages, so a full historical-alert reproduction cannot currently be demonstrated from this workspace."
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
        "overall_status": "metrics_bundle_defined_but_not_materialized",
        "highest_priority_gap": "No real alert/evaluation/backtest evidence exists locally, so none of the SRS priority metrics can be defended numerically in this workspace.",
        "canonical_v1_mode": (operating_mode_manifest.get("decision") or {}).get("canonical_v1_operating_mode"),
        "workspace_snapshot_highlights": {
            "alerts": alert_rows,
            "analyst_feedback": analyst_rows,
            "model_evaluation_runs": model_eval_rows,
            "calibration_profiles": calibration_rows,
            "shadow_model_scores": shadow_score_rows,
            "validation_runs": validation_rows,
            "backtest_artifacts": backtest_rows,
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
