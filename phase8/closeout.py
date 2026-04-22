from __future__ import annotations

import json
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from config.settings import REPO_ROOT
from phase7 import sha256_file


FINAL_CLOSEOUT_CONTRACT_VERSION = "phase8_final_closeout_v1"


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


def _repo_relative(path: Path) -> str:
    try:
        return str(path.resolve().relative_to(REPO_ROOT.resolve())).replace("\\", "/")
    except ValueError:
        return str(path)


def _path_artifact(path_value: str, *, kind: str, note: str | None = None) -> dict[str, Any]:
    path = (REPO_ROOT / path_value).resolve()
    exists = path.exists()
    artifact: dict[str, Any] = {
        "kind": kind,
        "path": _repo_relative(path) if exists or str(path).startswith(str(REPO_ROOT)) else path_value,
        "exists": exists,
        "note": note,
        "sha256": sha256_file(path) if exists and path.is_file() else None,
    }
    if exists and path.is_file():
        artifact["size_bytes"] = path.stat().st_size
    elif exists and path.is_dir():
        artifact["child_count"] = sum(1 for _ in path.iterdir())
    return artifact


def _load_json(path_value: str) -> dict[str, Any]:
    path = (REPO_ROOT / path_value).resolve()
    return json.loads(path.read_text(encoding="utf-8"))


def _runbook_step(
    *,
    order: int,
    label: str,
    path_value: str,
    classification: str,
    purpose: str,
    note: str | None = None,
) -> dict[str, Any]:
    artifact = _path_artifact(path_value, kind="runbook", note=note)
    return {
        "order": order,
        "label": label,
        "classification": classification,
        "purpose": purpose,
        "artifact": artifact,
    }


def _doc_step(
    *,
    order: int,
    label: str,
    path_value: str,
    role: str,
    note: str | None = None,
) -> dict[str, Any]:
    artifact = _path_artifact(path_value, kind="document", note=note)
    return {
        "order": order,
        "label": label,
        "role": role,
        "artifact": artifact,
    }


def build_phase8_final_closeout_manifest() -> dict[str, Any]:
    freeze_manifest = _load_json("reports/phase8/reference_window_freeze/phase8_reference_window_manifest.json")
    operating_mode_manifest = _load_json("reports/phase8/operating_mode/phase8_v1_operating_mode_manifest.json")
    metrics_manifest = _load_json("reports/phase8/metrics_review/phase8_metrics_review_manifest.json")

    canonical_read_order = [
        _doc_step(
            order=1,
            label="Top-level orientation",
            path_value="README.md",
            role="fastest repo entry point for a reviewer or handoff receiver",
        ),
        _doc_step(
            order=2,
            label="Documentation index",
            path_value="Documentation/INDEX.tex",
            role="canonical navigation map and ownership guidance",
        ),
        _doc_step(
            order=3,
            label="Requirements source",
            path_value="Documentation/SRS.tex",
            role="formal source of truth for v1 completeness, metrics, and scope",
        ),
        _doc_step(
            order=4,
            label="Artifact inventory",
            path_value="Documentation/phases/phase8_canonical_inventory.tex",
            role="canonical versus historical classification across Phase 1 through Phase 7 outputs",
        ),
        _doc_step(
            order=5,
            label="Frozen reference path",
            path_value="Documentation/phases/phase8_reference_path.tex",
            role="exact end-to-end reproducibility chain and evidence-gap record",
        ),
        _doc_step(
            order=6,
            label="Canonical v1 mode",
            path_value="Documentation/phases/phase8_v1_operating_mode.tex",
            role="formal operating-mode and promotion decision",
        ),
        _doc_step(
            order=7,
            label="Metrics and stop conditions",
            path_value="Documentation/phases/phase8_metrics_review.tex",
            role="final metrics bundle, limitations review, and stop-condition ledger",
        ),
        _doc_step(
            order=8,
            label="Final closeout memo",
            path_value="Documentation/phases/phase8_final_closeout.tex",
            role="single-owner final answer for demo, thesis, and defense handoff",
        ),
        _doc_step(
            order=9,
            label="Phase 9 remediation plan",
            path_value="Documentation/phases/phase9.tex",
            role="single-owner remediation plan that closes the exact Phase 8 evidence gaps.",
        ),
        _doc_step(
            order=10,
            label="Phase 9 final refresh task",
            path_value="Documentation/phases/phase9_task5_closeout_refresh.tex",
            role="documents the canonical closeout-refresh contract and regeneration commands.",
        ),
    ]

    operator_runbooks = [
        _runbook_step(
            order=1,
            label="Collector and archive bootstrap",
            path_value="database/POSTGRES_LOCAL_RUNBOOK.md",
            classification="canonical",
            purpose="Start the local PostgreSQL-backed collector and preserve the raw archive and replay foundation.",
        ),
        _runbook_step(
            order=2,
            label="Phase 3 live detector",
            path_value="database/PHASE3_LOCAL_RUNBOOK.md",
            classification="canonical",
            purpose="Produce real candidate windows from the detector-input stream.",
        ),
        _runbook_step(
            order=3,
            label="Phase 4 alert loop",
            path_value="database/PHASE4_LOCAL_RUNBOOK.md",
            classification="supporting_active",
            purpose="Exercise evidence gathering, alert creation, delivery, and analyst feedback capture.",
            note="The filename is active, but the runbook still carries historical local machine paths that should be normalized before a fresh live run.",
        ),
        _runbook_step(
            order=4,
            label="Phase 5 replay and validation",
            path_value="database/PHASE5_SINGLE_OWNER_RUNBOOK.md",
            classification="canonical",
            purpose="Replay historical windows, inspect health, and generate validation or backfill artifacts.",
            note="Single-owner Phase 5 runbook created in Phase 9 Task 5 and now replaces the historical person-labeled primary path.",
        ),
        _runbook_step(
            order=5,
            label="Phase 6 shadow-model operations",
            path_value="database/PHASE6_SINGLE_OWNER_RUNBOOK.md",
            classification="canonical",
            purpose="Materialize features, train and evaluate the LightGBM shadow model, register or activate models, and run shadow scoring.",
            note="Single-owner Phase 6 runbook created in Phase 9 Task 5 and now replaces the historical person-labeled primary path.",
        ),
        _runbook_step(
            order=6,
            label="Historical Phase 5 support path",
            path_value="database/PHASE5_PERSON1_RUNBOOK.md",
            classification="supporting_historical",
            purpose="Retained for historical traceability only; no longer the primary single-owner handoff path.",
        ),
        _runbook_step(
            order=7,
            label="Historical Phase 6 support path",
            path_value="database/PHASE6_PERSON1_RUNBOOK.md",
            classification="supporting_historical",
            purpose="Retained for historical traceability only; no longer the primary single-owner handoff path.",
        ),
        _runbook_step(
            order=8,
            label="Historical Phase 6 evaluation support path",
            path_value="database/PHASE6_PERSON2_RUNBOOK.md",
            classification="supporting_historical",
            purpose="Retained for historical traceability only; no longer the primary single-owner handoff path.",
        ),
    ]

    architecture_layers = [
        {
            "layer_key": "ingestion_and_archive",
            "title": "Ingestion and archive plane",
            "canonical_for_v1": True,
            "summary": "The collector, database schema, and raw-archive helpers form the local-first system of record for market ingestion and replayable storage.",
            "artifacts": [
                _path_artifact("run_collector.py", kind="runtime_entrypoint"),
                _path_artifact("database/schema.sql", kind="schema"),
                _path_artifact("database/POSTGRES_LOCAL_RUNBOOK.md", kind="runbook"),
                _path_artifact("utils/event_log.py", kind="runtime_module"),
            ],
        },
        {
            "layer_key": "candidate_generation",
            "title": "Deterministic candidate plane",
            "canonical_for_v1": True,
            "summary": "Phase 3 state tracking and detector logic convert replayable inputs into deterministic candidate episodes.",
            "artifacts": [
                _path_artifact("phase3/state_store.py", kind="runtime_module"),
                _path_artifact("phase3/detector.py", kind="runtime_module"),
                _path_artifact("phase3/live_runner.py", kind="runtime_module"),
                _path_artifact("database/PHASE3_LOCAL_RUNBOOK.md", kind="runbook"),
            ],
        },
        {
            "layer_key": "evidence_and_alerting",
            "title": "Evidence and alert plane",
            "canonical_for_v1": True,
            "summary": "Phase 4 gathers evidence, renders alerts, applies suppression, and captures analyst feedback.",
            "artifacts": [
                _path_artifact("phase4/evidence.py", kind="runtime_module"),
                _path_artifact("phase4/alerts.py", kind="runtime_module"),
                _path_artifact("phase4/analyst.py", kind="runtime_module"),
                _path_artifact("database/PHASE4_LOCAL_RUNBOOK.md", kind="runbook"),
            ],
        },
        {
            "layer_key": "validation_and_backtest",
            "title": "Validation and paper-trading plane",
            "canonical_for_v1": True,
            "summary": "Phase 5 is the historical replay, validation, and conservative paper-trading layer that should support final v1 claims.",
            "artifacts": [
                _path_artifact("phase5/replay.py", kind="runtime_module"),
                _path_artifact("phase5/simulator.py", kind="runtime_module"),
                _path_artifact("phase5/reporting.py", kind="runtime_module"),
                _path_artifact("database/PHASE5_SINGLE_OWNER_RUNBOOK.md", kind="runbook"),
            ],
        },
        {
            "layer_key": "shadow_ml",
            "title": "Shadow ML plane",
            "canonical_for_v1": True,
            "summary": "Phase 6 remains part of the canonical v1 story as shadow-only scoring, registry control, calibration, and evaluation infrastructure.",
            "artifacts": [
                _path_artifact("phase6/training.py", kind="runtime_module"),
                _path_artifact("phase6/reporting.py", kind="runtime_module"),
                _path_artifact("database/PHASE6_SINGLE_OWNER_RUNBOOK.md", kind="runbook"),
                _path_artifact("reports/phase9/phase6_model_completion/phase9_task4_summary.json", kind="phase9_artifact"),
            ],
        },
        {
            "layer_key": "advanced_research",
            "title": "Advanced research plane",
            "canonical_for_v1": False,
            "summary": "Phase 7 contributes research, observability, packaging, and thesis-grade methodology, but not live decision authority for canonical v1.",
            "artifacts": [
                _path_artifact("phase7/graph_features.py", kind="runtime_module"),
                _path_artifact("phase7/observability.py", kind="runtime_module"),
                _path_artifact("phase7/packaging.py", kind="runtime_module"),
                _path_artifact("reports/phase7", kind="report_root"),
            ],
        },
    ]

    methodology_summary = {
        "evaluation_sequence": [
            "Collect and archive raw market activity locally with deterministic storage contracts.",
            "Replay exact historical windows into detector-input form.",
            "Regenerate deterministic candidate episodes from replayed data.",
            "Enrich candidates with evidence and render operator-facing alerts.",
            "Validate historical behavior with replay summaries and conservative paper-trading assumptions.",
            "Compare shadow ML against wallet-unaware baselines without giving ML decision authority in canonical v1.",
            "Treat Phase 7 outputs as thesis-grade research and governance guidance unless a later promotion decision says otherwise.",
        ],
        "operating_principles": [
            "local-first reproducibility beats undocumented convenience",
            "rule-based alert authority with shadow ML observability",
            "conservative evaluation before stronger deployment claims",
            "exact paths, versions, and hashes for all defense-critical artifacts",
        ],
        "methodology_sources": [
            _path_artifact("Documentation/SRS.tex", kind="requirements_source"),
            _path_artifact("Documentation/phases/phase8_reference_path.tex", kind="phase_doc"),
            _path_artifact("Documentation/phases/phase8_v1_operating_mode.tex", kind="phase_doc"),
            _path_artifact("Documentation/phases/phase8_metrics_review.tex", kind="phase_doc"),
            _path_artifact("Documentation/phases/phase9.tex", kind="phase_doc"),
        ],
    }

    key_figures_and_tables = [
        {
            "key": "frozen_reference_chain_table",
            "title": "Frozen end-to-end reference chain",
            "type": "table",
            "status": "available_in_doc",
            "intended_use": "Show the exact raw archive to research-package path used for reproducibility defense.",
            "primary_reference": _path_artifact("Documentation/phases/phase8_reference_path.tex", kind="phase_doc"),
            "supporting_reference": _path_artifact(
                "reports/phase8/reference_window_freeze/phase8_reference_window_summary.md",
                kind="phase8_artifact",
            ),
        },
        {
            "key": "operating_mode_decision_table",
            "title": "Canonical v1 operating-mode decision",
            "type": "table",
            "status": "available_in_doc",
            "intended_use": "Explain why v1 is rule-based plus shadow ML rather than ML-authoritative.",
            "primary_reference": _path_artifact("Documentation/phases/phase8_v1_operating_mode.tex", kind="phase_doc"),
            "supporting_reference": _path_artifact(
                "reports/phase8/operating_mode/phase8_v1_operating_mode_summary.md",
                kind="phase8_artifact",
            ),
        },
        {
            "key": "metrics_bundle_table",
            "title": "Final metrics bundle in SRS priority order",
            "type": "table",
            "status": "available_in_doc",
            "intended_use": "Show the final success-metric ordering and current evidence state.",
            "primary_reference": _path_artifact("Documentation/phases/phase8_metrics_review.tex", kind="phase_doc"),
            "supporting_reference": _path_artifact(
                "reports/phase8/metrics_review/phase8_metrics_review_summary.md",
                kind="phase8_artifact",
            ),
        },
        {
            "key": "stop_condition_ledger",
            "title": "Stop-condition ledger",
            "type": "table",
            "status": "available_in_doc",
            "intended_use": "Make the SRS stop conditions explicit during defense or handoff.",
            "primary_reference": _path_artifact("Documentation/phases/phase8_metrics_review.tex", kind="phase_doc"),
            "supporting_reference": _path_artifact(
                "reports/phase8/metrics_review/phase8_metrics_review_manifest.json",
                kind="phase8_artifact",
            ),
        },
        {
            "key": "architecture_and_methodology_story",
            "title": "Architecture and methodology summary",
            "type": "memo",
            "status": "available_in_doc",
            "intended_use": "Give a reviewer one compact system story without forcing them back into all prior phase docs.",
            "primary_reference": _path_artifact("Documentation/phases/phase8_final_closeout.tex", kind="phase_doc"),
            "supporting_reference": _path_artifact("README.md", kind="document"),
        },
        {
            "key": "phase9_runtime_evidence_packet",
            "title": "Phase 9 runtime evidence packet",
            "type": "artifact_family",
            "status": "materialized_in_workspace",
            "intended_use": "Show the concrete Task 2 through Task 4 artifacts that now back the refreshed closeout package.",
            "primary_reference": _path_artifact(
                "reports/phase9/candidate_to_alert_materialization/phase9_task2_review_packet.json",
                kind="phase9_artifact",
            ),
            "supporting_reference": _path_artifact(
                "reports/phase9/phase6_model_completion/phase9_task4_summary.json",
                kind="phase9_artifact",
            ),
        },
        {
            "key": "phase7_thesis_figures",
            "title": "Phase 7 thesis-grade figures and ablation tables",
            "type": "figure_and_table_family",
            "status": "not_materialized_in_workspace",
            "intended_use": "Optional thesis-depth appendix if research artifacts are generated later.",
            "primary_reference": _path_artifact("reports/phase7", kind="report_root"),
            "supporting_reference": _path_artifact("Documentation/person2Phases/phase7_person2.tex", kind="supporting_doc"),
        },
    ]

    reproducibility_references = {
        "reference_window": freeze_manifest.get("reference_window"),
        "freeze_status": freeze_manifest.get("overall_status"),
        "database_snapshot": freeze_manifest.get("database_snapshot"),
        "phase_versions": freeze_manifest.get("versions"),
        "rebuild_commands": [
            {
                "label": "Freeze the reference path",
                "command": "python run_phase8_freeze_reference_path.py",
                "artifact": _path_artifact(
                    "reports/phase8/reference_window_freeze/phase8_reference_window_manifest.json",
                    kind="phase8_artifact",
                ),
            },
            {
                "label": "Rebuild the operating-mode decision",
                "command": "python run_phase8_decide_operating_mode.py",
                "artifact": _path_artifact(
                    "reports/phase8/operating_mode/phase8_v1_operating_mode_manifest.json",
                    kind="phase8_artifact",
                ),
            },
            {
                "label": "Rebuild the metrics review",
                "command": "python run_phase8_metrics_review.py",
                "artifact": _path_artifact(
                    "reports/phase8/metrics_review/phase8_metrics_review_manifest.json",
                    kind="phase8_artifact",
                ),
            },
            {
                "label": "Rebuild the final closeout package",
                "command": "python run_phase8_build_closeout_package.py",
                "artifact": _path_artifact(
                    "reports/phase8/final_closeout/phase8_final_closeout_manifest.json",
                    kind="phase8_artifact",
                ),
            },
            {
                "label": "Refresh the full Phase 9 closeout packet",
                "command": "python run_phase9_closeout_refresh.py",
                "artifact": _path_artifact(
                    "reports/phase9/closeout_refresh/phase9_task5_closeout_refresh_summary.json",
                    kind="phase9_artifact",
                ),
            },
        ],
    }

    final_closeout_memo = {
        "srs_v1_complete": False,
        "overall_status": "materially_populated_but_not_srs_complete_v1",
        "canonical_v1_mode": (operating_mode_manifest.get("decision") or {}).get("canonical_v1_operating_mode"),
        "direct_answer": (
            "No. Phase 9 materially improved the repo and now provides a replay-linked local evidence packet through Phase 6, "
            "but the project is still not SRS-complete v1 because the remaining blocker is evidence quality rather than missing artifacts: "
            "the current Phase 4 path is still noop-provider-backed and the current Phase 6 LightGBM evidence is still too small and train-only to defend a held-out claim."
        ),
        "srs_checklist": [
            {
                "criterion": "the collector runs stably across the intended market universe",
                "status": "historically_supported_not_reverified_in_phase8",
                "assessment": "The repo and earlier docs support the collector story, but Task 5 does not add a fresh live-run proof packet.",
                "evidence": [
                    _path_artifact("README.md", kind="document"),
                    _path_artifact("Documentation/phases/phase2.tex", kind="phase_doc"),
                ],
            },
            {
                "criterion": "the raw archive and replay path are working",
                "status": "materialized_for_canonical_local_packet",
                "assessment": "The canonical local packet now has raw-archive and detector-input manifest rows plus replay artifacts for the frozen hour, though the path is still a seeded local proof rather than a broad operational packet.",
                "evidence": [
                    _path_artifact("Documentation/phases/phase2.tex", kind="phase_doc"),
                    _path_artifact(
                        "reports/phase8/reference_window_freeze/phase8_reference_window_manifest.json",
                        kind="phase8_artifact",
                    ),
                    _path_artifact(
                        "reports/phase5/replay_runs/phase9_task3",
                        kind="phase5_artifact",
                    ),
                ],
            },
            {
                "criterion": "deterministic candidate episodes can be regenerated from replay",
                "status": "materialized_for_canonical_local_packet",
                "assessment": "Phase 9 Task 2 regenerated deterministic candidate rows for the canonical window through the native detector path.",
                "evidence": [
                    _path_artifact("Documentation/phases/phase3.tex", kind="phase_doc"),
                    _path_artifact(
                        "reports/phase9/candidate_to_alert_materialization/phase9_task2_review_packet.json",
                        kind="phase9_artifact",
                    ),
                ],
            },
            {
                "criterion": "evidence-backed alerts are live with analyst feedback capture",
                "status": "materialized_but_not_real_provider_backed",
                "assessment": "The canonical packet now includes persisted alerts, delivery attempts, and analyst feedback, but the evidence providers are noop adapters and outbound delivery was skipped locally, so this is not yet a strong real-world Phase 4 proof.",
                "evidence": [
                    _path_artifact("Documentation/phases/phase4_gate4_signoff.tex", kind="signoff_doc"),
                    _path_artifact(
                        "reports/phase9/candidate_to_alert_materialization/phase9_task2_review_packet.json",
                        kind="phase9_artifact",
                    ),
                ],
            },
            {
                "criterion": "one conservative backtest and one paper-trading evaluation exist",
                "status": "satisfied_for_canonical_local_packet",
                "assessment": "Phase 9 Task 3 now provides replay, holdout validation, and conservative paper-trading artifacts under reports/phase5/, with honest caveats that the sample is tiny.",
                "evidence": [
                    _path_artifact("Documentation/phases/phase5.tex", kind="phase_doc"),
                    _path_artifact(
                        "reports/phase5/validation/phase9_task3_holdout_validation.json",
                        kind="phase5_artifact",
                    ),
                    _path_artifact(
                        "reports/phase5/backtests/phase9_task3_conservative_backtest.json",
                        kind="phase5_artifact",
                    ),
                ],
            },
            {
                "criterion": "one LightGBM or CatBoost ranker is evaluated against the wallet-unaware baselines",
                "status": "artifact_contract_satisfied_but_not_strong_enough_for_v1_claim",
                "assessment": "Phase 9 Task 4 now evaluates a LightGBM ranker against the required wallet-unaware baselines and persists calibration, threshold, registry, and shadow-score artifacts, but the evidence is still train-only on a tiny dataset.",
                "evidence": [
                    _path_artifact("Documentation/phases/phase9_task4_phase6_model_completion.tex", kind="phase_doc"),
                    _path_artifact(
                        "reports/phase9/phase6_model_completion/phase9_task4_summary.json",
                        kind="phase9_artifact",
                    ),
                ],
            },
        ],
        "primary_blockers": [
            metrics_manifest["readiness_summary"]["highest_priority_gap"],
            "The canonical Phase 4 evidence path is still seeded local replay with noop providers rather than a real-provider-backed alert-evidence packet.",
            "The canonical Phase 6 LightGBM evaluation is still train-only on a tiny dataset, so it does not yet justify a held-out-strength SRS completion claim.",
        ],
        "strongest_defensible_claims": [
            "The repo now has one explicit canonical documentation path for single-owner handoff.",
            "One exact reference window is frozen with paths, versions, hashes, and stage-by-stage evidence-gap status.",
            "Canonical v1 is formally defined as rule-based plus shadow ML, with clear rollback semantics.",
            "The final metrics order, limitations review, and stop-condition ledger are now explicit and machine-readable.",
            "Phase 9 now provides a materially populated local evidence packet spanning candidate generation, alerting, validation, conservative paper trading, and LightGBM shadow evaluation.",
        ],
        "intentionally_out_of_scope": [
            "live capital deployment or autonomous execution",
            "cloud-first migration, HA redesign, or redundancy before the single-instance path is fully proven",
            "promotion of Phase 7 research models into canonical alert authority",
            "treating thesis-grade ablation figures as if they were required canonical v1 operating artifacts",
        ],
        "next_required_actions_before_v1_complete": [
            "materialize one real-provider-backed end-to-end replay-to-alert evidence packet for the frozen or equivalent reference window",
            "expand Phase 5 evaluation beyond the tiny seeded packet so conservative edge and lead-time claims survive a less fragile sample",
            "re-run the LightGBM or CatBoost path on a held-out-sized dataset and preserve evidence that comparisons remain strong off-train",
            "treat the new single-owner runbooks as the default operator path and retire person-labeled instructions from primary use",
        ],
    }

    return {
        "contract_version": FINAL_CLOSEOUT_CONTRACT_VERSION,
        "generated_at": _iso_now(),
        "git_commit": _git_head(),
        "authoritative_inputs": [
            _path_artifact("README.md", kind="document"),
            _path_artifact("Documentation/INDEX.tex", kind="document"),
            _path_artifact("Documentation/SRS.tex", kind="requirements_source"),
            _path_artifact("Documentation/phases/phase8_canonical_inventory.tex", kind="phase_doc"),
            _path_artifact("Documentation/phases/phase8_reference_path.tex", kind="phase_doc"),
            _path_artifact("Documentation/phases/phase8_v1_operating_mode.tex", kind="phase_doc"),
            _path_artifact("Documentation/phases/phase8_metrics_review.tex", kind="phase_doc"),
            _path_artifact("Documentation/phases/phase9.tex", kind="phase_doc"),
            _path_artifact("Documentation/phases/phase9_task5_closeout_refresh.tex", kind="phase_doc"),
        ],
        "consolidated_handoff_path": {
            "canonical_read_order": canonical_read_order,
            "operator_runbooks": operator_runbooks,
            "note": "Phase 9 Task 5 creates canonical single-owner Phase 5 and Phase 6 runbooks; historical person-labeled runbooks remain only as supporting traceability artifacts.",
        },
        "architecture_summary": {
            "canonical_v1_mode": (operating_mode_manifest.get("decision") or {}).get("canonical_v1_operating_mode"),
            "architecture_layers": architecture_layers,
            "narrative": "Canonical v1 remains a local-first, replayable, rule-based alerting system with shadow ML evaluation and a separate post-v1 research plane.",
        },
        "methodology_summary": methodology_summary,
        "key_figures_and_tables": key_figures_and_tables,
        "reproducibility_references": reproducibility_references,
        "final_closeout_memo": final_closeout_memo,
    }


def render_phase8_final_closeout_markdown(manifest: dict[str, Any]) -> str:
    memo = manifest["final_closeout_memo"]
    handoff = manifest["consolidated_handoff_path"]

    lines = [
        "# Phase 8 Final Closeout Package",
        "",
        f"- Contract version: `{manifest['contract_version']}`",
        f"- Generated at: `{manifest['generated_at']}`",
        f"- Git commit: `{manifest['git_commit']}`",
        f"- Canonical v1 mode: `{memo['canonical_v1_mode']}`",
        f"- SRS v1 complete: `{memo['srs_v1_complete']}`",
        f"- Overall status: `{memo['overall_status']}`",
        "",
        "## Direct Answer",
        f"- {memo['direct_answer']}",
        "",
        "## Handoff Read Order",
    ]
    for step in handoff["canonical_read_order"]:
        lines.append(f"- [{step['order']}] `{step['artifact']['path']}`: {step['role']}")
    lines.extend(["", "## Operator Runbook Path"])
    for step in handoff["operator_runbooks"]:
        lines.append(
            f"- [{step['order']}] `{step['artifact']['path']}` ({step['classification']}): {step['purpose']}"
        )
    lines.extend(["", "## Primary Blockers"])
    for item in memo["primary_blockers"]:
        lines.append(f"- {item}")
    lines.extend(["", "## Intentionally Out of Scope"])
    for item in memo["intentionally_out_of_scope"]:
        lines.append(f"- {item}")
    return "\n".join(lines) + "\n"
