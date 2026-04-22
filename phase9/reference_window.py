from __future__ import annotations

import sqlite3
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from config.settings import DB_PATH, REPO_ROOT
from phase7 import sha256_file


DEFAULT_REFERENCE_WINDOW_START = "2026-04-20T05:00:00+00:00"
DEFAULT_REFERENCE_WINDOW_END = "2026-04-20T06:00:00+00:00"
PHASE9_TASK1_CONTRACT_VERSION = "phase9_task1_reference_window_v1"


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
    payload: dict[str, Any] = {
        "kind": kind,
        "path": _repo_relative(path),
        "exists": exists,
        "note": note,
    }
    if path.is_file():
        payload["sha256"] = sha256_file(path)
        payload["size_bytes"] = path.stat().st_size
    elif path.is_dir():
        payload["sha256"] = None
        payload["child_count"] = sum(1 for _ in path.iterdir())
    else:
        payload["sha256"] = None
    return payload


def _logical_artifact(
    path_value: str,
    *,
    kind: str,
    exists: bool,
    note: str | None = None,
    **extra: Any,
) -> dict[str, Any]:
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


def _command_spec(
    *,
    step_key: str,
    purpose: str,
    command: str,
    expected_outputs: list[str],
    readiness: str,
) -> dict[str, Any]:
    return {
        "step_key": step_key,
        "purpose": purpose,
        "command": command,
        "expected_outputs": expected_outputs,
        "readiness": readiness,
    }


def build_phase9_task1_manifest(
    *,
    start: str = DEFAULT_REFERENCE_WINDOW_START,
    end: str = DEFAULT_REFERENCE_WINDOW_END,
    sqlite_path: str | None = None,
) -> dict[str, Any]:
    db_path = Path(sqlite_path or DB_PATH)
    raw_partition = "data/raw/year=2026/month=04/day=20/hour=05/source_system=gamma_events/events.ndjson"
    detector_partition = "data/detector_input/year=2026/month=04/day=20/hour=05/source_system=gamma_events/events.ndjson"
    replay_partition = "data/replay_runs/12bc07e8ae50/source_system=gamma_events/detector_input.ndjson"

    tracked_tables = [
        "raw_archive_manifests",
        "detector_input_manifests",
        "replay_runs",
        "signal_candidates",
        "signal_episodes",
        "signal_features",
        "evidence_queries",
        "evidence_snapshots",
        "alerts",
        "alert_delivery_attempts",
        "analyst_feedback",
        "validation_runs",
        "backtest_artifacts",
        "model_evaluation_runs",
        "calibration_profiles",
        "shadow_model_scores",
    ]
    table_counts = _load_table_counts(db_path, tracked_tables)

    db_file_artifact = _path_artifact(_repo_relative(db_path.resolve()), kind="sqlite_database")
    phase8_manifest = _path_artifact(
        "reports/phase8/reference_window_freeze/phase8_reference_window_manifest.json",
        kind="phase8_manifest",
        note="Phase 8 freeze manifest that this Phase 9 Task 1 contract intentionally reuses and tightens.",
    )
    raw_artifact = _path_artifact(
        raw_partition,
        kind="raw_partition",
        note="Canonical raw archive partition for the selected one-hour reference window.",
    )
    detector_artifact = _path_artifact(
        detector_partition,
        kind="detector_input_partition",
        note="Canonical normalized detector-input partition for the same reference hour.",
    )
    replay_artifact = _path_artifact(
        replay_partition,
        kind="replay_republish_partition",
        note="Exact replay-republish path cited in the committed Phase 2 delivery document.",
    )
    phase5_root = _path_artifact(
        "reports/phase5",
        kind="report_root",
        note="Expected Phase 5 replay, validation, and backtest report root.",
    )
    phase6_root = _path_artifact(
        "reports/phase6",
        kind="report_root",
        note="Expected Phase 6 features, model artifacts, and shadow-score report root.",
    )
    phase7_root = _path_artifact(
        "reports/phase7",
        kind="report_root",
        note="Advanced research root kept here only for context; Phase 9 Task 1 does not promote it.",
    )

    later_phase_tables = [
        "signal_candidates",
        "signal_episodes",
        "signal_features",
        "evidence_queries",
        "evidence_snapshots",
        "alerts",
        "alert_delivery_attempts",
        "analyst_feedback",
        "validation_runs",
        "backtest_artifacts",
        "model_evaluation_runs",
        "calibration_profiles",
        "shadow_model_scores",
    ]
    non_zero_later_phase_tables = [
        name for name in later_phase_tables if (table_counts.get(name) or 0) > 0
    ]

    availability_checks = {
        "data_root_exists": (REPO_ROOT / "data").exists(),
        "selected_raw_partition_exists": raw_artifact["exists"],
        "selected_detector_partition_exists": detector_artifact["exists"],
        "selected_replay_partition_exists": replay_artifact["exists"],
        "sqlite_database_exists": db_file_artifact["exists"],
        "phase5_report_root_exists": phase5_root["exists"],
        "phase6_report_root_exists": phase6_root["exists"],
        "phase7_report_root_exists": phase7_root["exists"],
        "phase7_report_root_child_count": int(phase7_root.get("child_count") or 0),
        "non_zero_later_phase_tables": non_zero_later_phase_tables,
    }

    if raw_artifact["exists"] and detector_artifact["exists"]:
        readiness = "reference_window_available_for_materialization"
    else:
        readiness = "reference_window_frozen_but_not_locally_materialized"

    frozen_commands = [
        _command_spec(
            step_key="phase2_replay_validate",
            purpose="Verify raw-archive and detector-input coverage for the exact selected window.",
            command=(
                "python validation/run_phase2_replay.py "
                f"--start {start} --end {end} --source-system gamma_events --json"
            ),
            expected_outputs=[
                "stdout JSON replay coverage report",
                "raw and detector partition counts for the selected hour",
            ],
            readiness="ready_when_raw_and_detector_partitions_exist",
        ),
        _command_spec(
            step_key="phase2_republish",
            purpose="Republish the frozen raw window into detector-input form for downstream replay-safe processing.",
            command=(
                "python validation/run_phase2_republish.py "
                f"--start {start} --end {end} --source-system gamma_events --json"
            ),
            expected_outputs=[
                "one replay_run_id in database::replay_runs",
                "one republished detector-input artifact under data/replay_runs/<replay_run_id>/source_system=gamma_events/detector_input.ndjson",
            ],
            readiness="blocked_until_raw_archive_exists",
        ),
        _command_spec(
            step_key="phase4_gate4_capture",
            purpose="Freeze the later alert/evidence capture output path that Task 2 will populate for this same reference story.",
            command=(
                "python run_phase4_gate4_capture.py "
                "--limit 10 "
                "--latest-alert-limit 5 "
                "--output reports/phase9/reference_window_preparation/phase9_task1_gate4_report.json"
            ),
            expected_outputs=[
                "reports/phase9/reference_window_preparation/phase9_task1_gate4_report.json",
                "non-zero alert/evidence tables once later materialization succeeds",
            ],
            readiness="blocked_until_phase3_candidates_exist",
        ),
        _command_spec(
            step_key="phase5_replay_bundle",
            purpose="Freeze the canonical Phase 5 replay bundle command and artifact root for the selected window.",
            command=(
                "python run_phase5_replay.py "
                f"--start {start} --end {end} "
                "--source-system gamma_events "
                "--output-dir reports/phase9/reference_window_preparation/phase5_replay_bundle "
                "--json"
            ),
            expected_outputs=[
                "reports/phase9/reference_window_preparation/phase5_replay_bundle/",
                "replay summary rows in database-backed Phase 5 tables",
            ],
            readiness="blocked_until_raw_archive_and_phase4_outputs_exist",
        ),
        _command_spec(
            step_key="phase6_training_dataset",
            purpose="Freeze the replay-derived Phase 6 training-dataset output root tied to the same window.",
            command=(
                "python run_phase6_build_training_dataset.py "
                f"--start {start} --end {end} "
                "--output-dir reports/phase9/reference_window_preparation/phase6_training_datasets "
                "--json"
            ),
            expected_outputs=[
                "reports/phase9/reference_window_preparation/phase6_training_datasets/<dataset_hash>.csv",
                "reports/phase9/reference_window_preparation/phase6_training_datasets/<dataset_hash>.json",
            ],
            readiness="blocked_until_phase5_evaluation_rows_exist",
        ),
        _command_spec(
            step_key="phase6_feature_materialization",
            purpose="Freeze the Phase 6 feature-materialization command and output root for later shadow evaluation.",
            command=(
                "python run_phase6_materialize_features.py "
                f"--start {start} --end {end} "
                "--mode inference "
                "--output-dir reports/phase9/reference_window_preparation/phase6_features "
                "--json"
            ),
            expected_outputs=[
                "reports/phase9/reference_window_preparation/phase6_features/<dataset_hash>.jsonl",
                "one materialization summary row in Phase 6 repository state",
            ],
            readiness="blocked_until_phase5_evaluation_rows_exist",
        ),
    ]

    return {
        "task_contract_version": PHASE9_TASK1_CONTRACT_VERSION,
        "generated_at": _iso_now(),
        "git_commit": _git_head(),
        "task_name": "Phase 9 Task 1 - Reference Window Preparation",
        "task_goal": "Pick the canonical historical window, verify local availability, and freeze the exact command and artifact contract that Phase 9 will use.",
        "reference_window": {
            "start": start,
            "end": end,
            "source_system": "gamma_events",
            "selected_from": "Documentation/phases/phase2.tex and reports/phase8/reference_window_freeze/phase8_reference_window_manifest.json",
            "selection_reason": (
                "The 2026-04-20T05:00:00+00:00 to 2026-04-20T06:00:00+00:00 gamma_events window remains the strongest canonical choice because it is the only exact one-hour window named in a committed canonical Phase 2 delivery document and it was already frozen formally by Phase 8."
            ),
            "selection_status": "selected_as_phase9_canonical_reference_window",
        },
        "workspace_availability": {
            "overall_readiness": readiness,
            "checks": availability_checks,
            "key_artifacts": [
                phase8_manifest,
                db_file_artifact,
                raw_artifact,
                detector_artifact,
                replay_artifact,
                phase5_root,
                phase6_root,
                phase7_root,
                _logical_artifact(
                    "database/polymarket_state.db::later_phase_tables",
                    kind="sqlite_table_group",
                    exists=bool(non_zero_later_phase_tables),
                    note="Summary of later-phase runtime tables that would need non-zero rows for a materially populated reference path.",
                    populated_tables=non_zero_later_phase_tables,
                ),
            ],
            "table_counts": table_counts,
        },
        "frozen_command_path": frozen_commands,
        "canonical_output_roots": [
            "reports/phase9/reference_window_preparation/",
            "reports/phase9/reference_window_preparation/phase9_task1_gate4_report.json",
            "reports/phase9/reference_window_preparation/phase5_replay_bundle/",
            "reports/phase9/reference_window_preparation/phase6_training_datasets/",
            "reports/phase9/reference_window_preparation/phase6_features/",
        ],
        "closeout_note": (
            "Task 1 is complete when the reference window choice, local availability truth, and command/output contract are frozen. It does not require the later stages to be populated yet, but it must say plainly whether the chosen window is locally materialized or still missing."
        ),
    }


def render_phase9_task1_markdown(manifest: dict[str, Any]) -> str:
    checks = manifest["workspace_availability"]["checks"]
    lines = [
        "# Phase 9 Task 1 - Reference Window Preparation",
        "",
        f"- Contract version: `{manifest['task_contract_version']}`",
        f"- Generated at: `{manifest['generated_at']}`",
        f"- Git commit: `{manifest['git_commit']}`",
        f"- Selected window: `{manifest['reference_window']['start']}` to `{manifest['reference_window']['end']}`",
        f"- Source system: `{manifest['reference_window']['source_system']}`",
        f"- Overall readiness: `{manifest['workspace_availability']['overall_readiness']}`",
        "",
        "## Local Availability Checks",
        f"- `data/` exists: `{checks['data_root_exists']}`",
        f"- Raw partition exists: `{checks['selected_raw_partition_exists']}`",
        f"- Detector-input partition exists: `{checks['selected_detector_partition_exists']}`",
        f"- Replay republish partition exists: `{checks['selected_replay_partition_exists']}`",
        f"- SQLite database exists: `{checks['sqlite_database_exists']}`",
        f"- Non-zero later-phase tables: `{', '.join(checks['non_zero_later_phase_tables']) or 'none'}`",
        "",
        "## Frozen Command Path",
    ]
    for item in manifest["frozen_command_path"]:
        lines.extend(
            [
                f"- `{item['step_key']}`",
                f"  Purpose: {item['purpose']}",
                f"  Command: `{item['command']}`",
                f"  Readiness: `{item['readiness']}`",
            ]
        )
    return "\n".join(lines) + "\n"
