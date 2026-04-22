from __future__ import annotations

import json
import sqlite3
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from config.settings import (
    DB_PATH,
    PHASE3_DETECTOR_VERSION,
    PHASE3_FEATURE_SCHEMA_VERSION,
    PHASE4_ALERT_SCHEMA_VERSION,
    PHASE4_EVIDENCE_SCHEMA_VERSION,
    PHASE4_WORKFLOW_VERSION,
    PHASE5_METRICS_VERSION,
    PHASE5_REPORT_VERSION,
    PHASE5_SIMULATOR_VERSION,
    PHASE6_CALIBRATION_VERSION,
    PHASE6_EVALUATION_VERSION,
    PHASE6_FEATURE_SCHEMA_VERSION,
    PHASE6_MODEL_REGISTRY_VERSION,
    PHASE7_ADVANCED_EXPERIMENT_VERSION,
    PHASE7_CONFIG_VERSION,
    PHASE7_DATASET_INDEX_VERSION,
    PHASE7_EXPERIMENT_LEDGER_VERSION,
    PHASE7_GRAPH_FEATURE_SCHEMA_VERSION,
    PHASE7_LABEL_SCHEMA_VERSION,
    PHASE7_RESEARCH_PACKAGE_VERSION,
    PHASE7_SCOPE_INDEX_VERSION,
    REPO_ROOT,
)
from phase7 import sha256_file


DEFAULT_REFERENCE_WINDOW_START = "2026-04-20T05:00:00+00:00"
DEFAULT_REFERENCE_WINDOW_END = "2026-04-20T06:00:00+00:00"
FREEZE_CONTRACT_VERSION = "phase8_reference_freeze_v1"


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


def _file_artifact(path_value: str, *, kind: str, note: str | None = None) -> dict[str, Any]:
    path = (REPO_ROOT / path_value).resolve()
    exists = path.exists()
    artifact: dict[str, Any] = {
        "kind": kind,
        "path": _repo_relative(path),
        "exists": exists,
        "note": note,
    }
    if path.is_file():
        artifact["sha256"] = sha256_file(path)
        artifact["size_bytes"] = path.stat().st_size
    elif path.is_dir():
        artifact["sha256"] = None
        artifact["child_count"] = sum(1 for _ in path.iterdir())
    else:
        artifact["sha256"] = None
    return artifact


def _logical_artifact(path_value: str, *, kind: str, exists: bool, note: str | None = None, **extra: Any) -> dict[str, Any]:
    payload = {
        "kind": kind,
        "path": path_value,
        "exists": exists,
        "sha256": None,
        "note": note,
    }
    payload.update(extra)
    return payload


def _load_table_counts(db_path: Path, table_names: list[str]) -> dict[str, int | None]:
    if not db_path.exists():
        return {name: None for name in table_names}
    conn = sqlite3.connect(db_path)
    try:
        counts: dict[str, int | None] = {}
        for table_name in table_names:
            try:
                row = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()
            except sqlite3.Error:
                counts[table_name] = None
                continue
            counts[table_name] = int(row[0]) if row else 0
        return counts
    finally:
        conn.close()


def _stage_status(*, runtime_artifacts: list[dict[str, Any]], table_counts: dict[str, int | None]) -> str:
    runtime_exists = False
    for item in runtime_artifacts:
        if not bool(item.get("exists")):
            continue
        if item.get("kind") == "report_root":
            runtime_exists = int(item.get("child_count") or 0) > 0
        elif item.get("kind") == "sqlite_table":
            runtime_exists = int(item.get("row_count") or 0) > 0
        else:
            runtime_exists = True
        if runtime_exists:
            break
    populated_tables = any((count or 0) > 0 for count in table_counts.values() if count is not None)
    if runtime_exists or populated_tables:
        return "partially_materialized"
    return "frozen_code_and_docs_only"


def _versions_payload() -> dict[str, str]:
    return {
        "phase3_feature_schema_version": PHASE3_FEATURE_SCHEMA_VERSION,
        "phase3_detector_version": PHASE3_DETECTOR_VERSION,
        "phase4_workflow_version": PHASE4_WORKFLOW_VERSION,
        "phase4_evidence_schema_version": PHASE4_EVIDENCE_SCHEMA_VERSION,
        "phase4_alert_schema_version": PHASE4_ALERT_SCHEMA_VERSION,
        "phase5_simulator_version": PHASE5_SIMULATOR_VERSION,
        "phase5_metrics_version": PHASE5_METRICS_VERSION,
        "phase5_report_version": PHASE5_REPORT_VERSION,
        "phase6_feature_schema_version": PHASE6_FEATURE_SCHEMA_VERSION,
        "phase6_model_registry_version": PHASE6_MODEL_REGISTRY_VERSION,
        "phase6_evaluation_version": PHASE6_EVALUATION_VERSION,
        "phase6_calibration_version": PHASE6_CALIBRATION_VERSION,
        "phase7_dataset_index_version": PHASE7_DATASET_INDEX_VERSION,
        "phase7_scope_index_version": PHASE7_SCOPE_INDEX_VERSION,
        "phase7_experiment_ledger_version": PHASE7_EXPERIMENT_LEDGER_VERSION,
        "phase7_label_schema_version": PHASE7_LABEL_SCHEMA_VERSION,
        "phase7_config_version": PHASE7_CONFIG_VERSION,
        "phase7_graph_feature_schema_version": PHASE7_GRAPH_FEATURE_SCHEMA_VERSION,
        "phase7_advanced_experiment_version": PHASE7_ADVANCED_EXPERIMENT_VERSION,
        "phase7_research_package_version": PHASE7_RESEARCH_PACKAGE_VERSION,
    }


def build_reference_freeze_manifest(
    *,
    start: str = DEFAULT_REFERENCE_WINDOW_START,
    end: str = DEFAULT_REFERENCE_WINDOW_END,
    sqlite_path: str | None = None,
) -> dict[str, Any]:
    db_path = Path(sqlite_path or DB_PATH)
    db_file = _file_artifact(_repo_relative(db_path.resolve()), kind="sqlite_database")

    stage_table_groups = {
        "raw_archive": ["raw_archive_manifests", "detector_input_manifests", "replay_runs"],
        "candidate_generation": ["signal_candidates", "signal_episodes", "signal_features", "detector_checkpoints"],
        "alert_and_evidence": ["evidence_queries", "evidence_snapshots", "alerts", "alert_delivery_attempts", "analyst_feedback"],
        "validation_and_backtest": ["validation_runs", "backtest_artifacts", "backfill_requests"],
        "ml_and_research": [
            "feature_materialization_runs",
            "model_registry",
            "shadow_model_scores",
            "model_evaluation_runs",
            "calibration_profiles",
            "phase7_research_datasets",
            "phase7_research_windows",
            "phase7_research_scopes",
            "phase7_experiment_ledger",
        ],
    }
    all_tables: list[str] = []
    for names in stage_table_groups.values():
        all_tables.extend(names)
    table_counts = _load_table_counts(db_path, all_tables)

    raw_partition = f"data/raw/year=2026/month=04/day=20/hour=05/source_system=gamma_events/events.ndjson"
    detector_partition = f"data/detector_input/year=2026/month=04/day=20/hour=05/source_system=gamma_events/events.ndjson"
    replay_partition = "data/replay_runs/12bc07e8ae50/source_system=gamma_events/detector_input.ndjson"

    stages: list[dict[str, Any]] = []

    raw_runtime = [
        _file_artifact(raw_partition, kind="raw_partition", note="Canonical Phase 2 reference raw archive partition for the frozen one-hour window."),
        _file_artifact(detector_partition, kind="detector_input_partition", note="Expected normalized detector-input partition for the same reference hour."),
        _file_artifact(replay_partition, kind="replay_republish_partition", note="Exact replay republish output path cited in Documentation/phases/phase2.tex."),
    ]
    raw_stage_tables = {name: table_counts.get(name) for name in stage_table_groups["raw_archive"]}
    stages.append(
        {
            "stage_key": "raw_archive_and_replay",
            "status": _stage_status(runtime_artifacts=raw_runtime, table_counts=raw_stage_tables),
            "stage_window": {"start": start, "end": end, "source_system": "gamma_events"},
            "committed_inputs": [
                _file_artifact("Documentation/phases/phase2.tex", kind="phase_doc"),
                _file_artifact("Documentation/phases/phase2_gate2_signoff.tex", kind="signoff_doc"),
                _file_artifact("utils/event_log.py", kind="runtime_module"),
                _file_artifact("validation/phase2_replay.py", kind="validation_module"),
                _file_artifact("validation/phase2_republish.py", kind="validation_module"),
                _file_artifact("database/POSTGRES_LOCAL_RUNBOOK.md", kind="runbook"),
            ],
            "runtime_artifacts": raw_runtime,
            "database_tables": raw_stage_tables,
            "note": "This is the anchor stage because Phase 2 is the only committed canonical doc that names an exact one-hour raw and replay proof window.",
        }
    )

    candidate_runtime = [
        _logical_artifact(
            "database/polymarket_state.db::signal_candidates",
            kind="sqlite_table",
            exists=(raw_stage_tables.get("replay_runs") or 0) > 0 or (table_counts.get("signal_candidates") or 0) > 0,
            row_count=table_counts.get("signal_candidates"),
            note="Phase 3 candidate outputs are persisted in SQLite/PostgreSQL tables rather than a committed file artifact.",
        ),
        _logical_artifact(
            "database/polymarket_state.db::signal_features",
            kind="sqlite_table",
            exists=(table_counts.get("signal_features") or 0) > 0,
            row_count=table_counts.get("signal_features"),
            note="Feature snapshots for emitted windows.",
        ),
    ]
    candidate_stage_tables = {name: table_counts.get(name) for name in stage_table_groups["candidate_generation"]}
    stages.append(
        {
            "stage_key": "candidate_generation",
            "status": _stage_status(runtime_artifacts=candidate_runtime, table_counts=candidate_stage_tables),
            "stage_window": {"start": start, "end": end},
            "committed_inputs": [
                _file_artifact("Documentation/phases/phase3.tex", kind="phase_doc"),
                _file_artifact("database/PHASE3_LOCAL_RUNBOOK.md", kind="runbook"),
                _file_artifact("phase3/detector.py", kind="runtime_module"),
                _file_artifact("phase3/live_runner.py", kind="runtime_module"),
                _file_artifact("run_phase3_live.py", kind="runner"),
                _file_artifact("validation/phase3_gate3_report.py", kind="validation_module"),
            ],
            "runtime_artifacts": candidate_runtime,
            "database_tables": candidate_stage_tables,
            "note": "The frozen path expects Phase 3 to consume detector-input envelopes for the same reference window and emit candidates into persisted tables.",
        }
    )

    alert_runtime = [
        _logical_artifact(
            "database/polymarket_state.db::evidence_snapshots",
            kind="sqlite_table",
            exists=(table_counts.get("evidence_snapshots") or 0) > 0,
            row_count=table_counts.get("evidence_snapshots"),
            note="Point-in-time evidence snapshots for candidate-linked alerts.",
        ),
        _logical_artifact(
            "database/polymarket_state.db::alerts",
            kind="sqlite_table",
            exists=(table_counts.get("alerts") or 0) > 0,
            row_count=table_counts.get("alerts"),
            note="Rendered alert records for the Phase 4 workflow.",
        ),
        _logical_artifact(
            "database/polymarket_state.db::alert_delivery_attempts",
            kind="sqlite_table",
            exists=(table_counts.get("alert_delivery_attempts") or 0) > 0,
            row_count=table_counts.get("alert_delivery_attempts"),
            note="Delivery logging for Telegram/Discord or equivalent local channels.",
        ),
    ]
    alert_stage_tables = {name: table_counts.get(name) for name in stage_table_groups["alert_and_evidence"]}
    stages.append(
        {
            "stage_key": "alert_and_evidence",
            "status": _stage_status(runtime_artifacts=alert_runtime, table_counts=alert_stage_tables),
            "stage_window": {"start": start, "end": end},
            "committed_inputs": [
                _file_artifact("Documentation/phases/phase4.tex", kind="phase_doc"),
                _file_artifact("Documentation/phases/phase4_gate4_signoff.tex", kind="signoff_doc"),
                _file_artifact("database/PHASE4_LOCAL_RUNBOOK.md", kind="runbook"),
                _file_artifact("phase4/evidence.py", kind="runtime_module"),
                _file_artifact("phase4/alerts.py", kind="runtime_module"),
                _file_artifact("run_phase4_pipeline.py", kind="runner"),
                _file_artifact("validation/phase4_gate4_report.py", kind="validation_module"),
            ],
            "runtime_artifacts": alert_runtime,
            "database_tables": alert_stage_tables,
            "note": "The alert stage is frozen as persisted evidence and alert workflow outputs over the same historical slice, even though this workspace currently contains no materialized rows.",
        }
    )

    validation_runtime = [
        _file_artifact("reports/phase5", kind="report_root", note="Expected Phase 5 replay, validation, and backtest artifact root."),
        _logical_artifact(
            "database/polymarket_state.db::validation_runs",
            kind="sqlite_table",
            exists=(table_counts.get("validation_runs") or 0) > 0,
            row_count=table_counts.get("validation_runs"),
            note="Stored validation-run summaries for replayed windows.",
        ),
        _logical_artifact(
            "database/polymarket_state.db::backtest_artifacts",
            kind="sqlite_table",
            exists=(table_counts.get("backtest_artifacts") or 0) > 0,
            row_count=table_counts.get("backtest_artifacts"),
            note="Stored paper-trading and backtest artifacts.",
        ),
    ]
    validation_stage_tables = {name: table_counts.get(name) for name in stage_table_groups["validation_and_backtest"]}
    stages.append(
        {
            "stage_key": "validation_and_backtest",
            "status": _stage_status(runtime_artifacts=validation_runtime, table_counts=validation_stage_tables),
            "stage_window": {"start": start, "end": end},
            "committed_inputs": [
                _file_artifact("Documentation/phases/phase5.tex", kind="phase_doc"),
                _file_artifact("database/PHASE5_PERSON1_RUNBOOK.md", kind="runbook"),
                _file_artifact("phase5/replay.py", kind="runtime_module"),
                _file_artifact("phase5/simulator.py", kind="runtime_module"),
                _file_artifact("phase5/reporting.py", kind="runtime_module"),
                _file_artifact("run_phase5_replay.py", kind="runner"),
                _file_artifact("validation/phase5_person2_report.py", kind="validation_module"),
            ],
            "runtime_artifacts": validation_runtime,
            "database_tables": validation_stage_tables,
            "note": "This stage ties the frozen window to replay validation and conservative paper-trading outputs.",
        }
    )

    ml_runtime = [
        _file_artifact("reports/phase7", kind="report_root", note="Expected advanced research package root."),
        _logical_artifact(
            "database/polymarket_state.db::model_evaluation_runs",
            kind="sqlite_table",
            exists=(table_counts.get("model_evaluation_runs") or 0) > 0,
            row_count=table_counts.get("model_evaluation_runs"),
            note="Phase 6 model evaluation lineage tied to replay-derived datasets.",
        ),
        _logical_artifact(
            "database/polymarket_state.db::phase7_experiment_ledger",
            kind="sqlite_table",
            exists=(table_counts.get("phase7_experiment_ledger") or 0) > 0,
            row_count=table_counts.get("phase7_experiment_ledger"),
            note="Phase 7 reproducibility ledger for advanced experiments and research packages.",
        ),
    ]
    ml_stage_tables = {name: table_counts.get(name) for name in stage_table_groups["ml_and_research"]}
    stages.append(
        {
            "stage_key": "ml_and_research",
            "status": _stage_status(runtime_artifacts=ml_runtime, table_counts=ml_stage_tables),
            "stage_window": {"start": start, "end": end},
            "committed_inputs": [
                _file_artifact("Documentation/person2Phases/phase7_graph_feature_contract.md", kind="supporting_doc"),
                _file_artifact("Documentation/person2Phases/phase7_person2.tex", kind="supporting_doc"),
                _file_artifact("database/PHASE6_PERSON1_RUNBOOK.md", kind="runbook"),
                _file_artifact("database/PHASE6_PERSON2_RUNBOOK.md", kind="runbook"),
                _file_artifact("phase6/training.py", kind="runtime_module"),
                _file_artifact("run_phase6_train_ranker.py", kind="runner"),
                _file_artifact("phase7/graph_features.py", kind="runtime_module"),
                _file_artifact("phase7/packaging.py", kind="runtime_module"),
                _file_artifact("run_phase7_train_graph_ranker.py", kind="runner"),
                _file_artifact("run_phase7_build_research_package.py", kind="runner"),
            ],
            "runtime_artifacts": ml_runtime,
            "database_tables": ml_stage_tables,
            "note": "The terminal stage is frozen as either a Phase 6 ML artifact or a Phase 7 research package, with the research-package runner chosen as the preferred end-state for Phase 8 closeout.",
        }
    )

    overall_status = "fully_materialized" if all(stage["status"] == "partially_materialized" for stage in stages) else "frozen_definition_with_missing_runtime_outputs"
    return {
        "freeze_contract_version": FREEZE_CONTRACT_VERSION,
        "generated_at": _iso_now(),
        "git_commit": _git_head(),
        "reference_window": {
            "start": start,
            "end": end,
            "source_system": "gamma_events",
            "selection_reason": "Chosen from the committed Phase 2 delivery doc because it is the only canonical artifact in git that names an exact one-hour raw/replay proof window.",
            "selected_from": "Documentation/phases/phase2.tex",
        },
        "versions": _versions_payload(),
        "database_snapshot": {
            "database_file": db_file,
            "table_counts": table_counts,
        },
        "stages": stages,
        "overall_status": overall_status,
        "closeout_note": "This manifest freezes the exact committed code and document provenance for the end-to-end chain, and records which runtime outputs are currently missing in the local workspace.",
    }


def render_reference_freeze_markdown(manifest: dict[str, Any]) -> str:
    def _materialized_count(items: list[dict[str, Any]]) -> int:
        count = 0
        for item in items:
            if item.get("kind") == "report_root":
                if int(item.get("child_count") or 0) > 0:
                    count += 1
            elif item.get("kind") == "sqlite_table":
                if int(item.get("row_count") or 0) > 0:
                    count += 1
            elif bool(item.get("exists")):
                count += 1
        return count

    lines = [
        "# Phase 8 Reference Window Freeze",
        "",
        f"- Freeze contract version: `{manifest['freeze_contract_version']}`",
        f"- Generated at: `{manifest['generated_at']}`",
        f"- Git commit: `{manifest['git_commit']}`",
        f"- Reference window: `{manifest['reference_window']['start']}` to `{manifest['reference_window']['end']}`",
        f"- Overall status: `{manifest['overall_status']}`",
        "",
        "## Stage Status",
    ]
    for stage in manifest["stages"]:
        lines.extend(
            [
                f"- `{stage['stage_key']}`: `{stage['status']}`",
                f"  Materialized runtime artifacts: `{_materialized_count(stage['runtime_artifacts'])}` of `{len(stage['runtime_artifacts'])}`",
            ]
        )
    lines.extend(
        [
            "",
            "## Database Snapshot",
            f"- Database file: `{manifest['database_snapshot']['database_file']['path']}`",
            f"- Database file hash: `{manifest['database_snapshot']['database_file']['sha256']}`",
        ]
    )
    return "\n".join(lines) + "\n"
