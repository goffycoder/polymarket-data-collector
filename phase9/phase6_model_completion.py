from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from config.settings import PHASE6_DEFAULT_MODEL_NAME, PHASE6_FEATURE_SCHEMA_VERSION, REPO_ROOT
from database.db_manager import apply_schema, get_conn
from phase5.repository import Phase5Repository
from phase6 import (
    Phase6Repository,
    build_calibration_profiles,
    build_model_card_markdown,
    build_required_baseline_comparison,
    build_score_report,
    build_shadow_scores,
    build_training_frame,
    fit_lightgbm_ranker,
    score_training_frame,
)
from phase9.candidate_alert import PHASE9_TASK2_END, PHASE9_TASK2_START


PHASE9_TASK4_CONTRACT_VERSION = "phase9_task4_phase6_model_completion_v1"
PHASE9_TASK4_MODEL_VERSION = "phase9_task4_lightgbm_v1"
PHASE9_TASK4_MODEL_NAME = PHASE6_DEFAULT_MODEL_NAME
PHASE9_TASK4_DATASET_DIR = "reports/phase6/training_datasets/phase9_task4"
PHASE9_TASK4_MODEL_DIR = "reports/phase6/model_artifacts/phase9_task4"
PHASE9_TASK4_BASELINE_DIR = "reports/phase6/baseline_comparisons"
PHASE9_TASK4_CALIBRATION_DIR = "reports/phase6/calibration"
PHASE9_TASK4_SHADOW_DIR = "reports/phase6/shadow_scores"
PHASE9_TASK4_SUMMARY_DIR = "reports/phase9/phase6_model_completion"


def _iso_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _repo_relative(path: Path) -> str:
    try:
        return str(path.resolve().relative_to(REPO_ROOT.resolve())).replace("\\", "/")
    except ValueError:
        return str(path)


def _ensure_parent(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def cleanup_phase9_task4_state() -> dict[str, int]:
    conn = get_conn()
    deleted: dict[str, int] = {}
    try:
        deleted["shadow_model_scores"] = int(
            conn.execute(
                "DELETE FROM shadow_model_scores WHERE model_version = ?",
                (PHASE9_TASK4_MODEL_VERSION,),
            ).rowcount
            or 0
        )
        deleted["calibration_profiles"] = int(
            conn.execute(
                "DELETE FROM calibration_profiles WHERE model_version = ?",
                (PHASE9_TASK4_MODEL_VERSION,),
            ).rowcount
            or 0
        )
        deleted["model_evaluation_runs"] = int(
            conn.execute(
                "DELETE FROM model_evaluation_runs WHERE model_version = ?",
                (PHASE9_TASK4_MODEL_VERSION,),
            ).rowcount
            or 0
        )
        deleted["model_registry"] = int(
            conn.execute(
                "DELETE FROM model_registry WHERE model_version = ?",
                (PHASE9_TASK4_MODEL_VERSION,),
            ).rowcount
            or 0
        )
        conn.commit()
    finally:
        conn.close()
    return deleted


def _thresholds_from_profiles(profiles: list[Any]) -> dict[str, float | None]:
    for profile in profiles:
        if profile.profile_scope == "global" and profile.profile_key == "global":
            return {
                "watch": profile.watch_threshold,
                "actionable": profile.actionable_threshold,
                "critical": profile.critical_threshold,
            }
    return {"watch": None, "actionable": None, "critical": None}


def _render_baseline_markdown(report: dict[str, Any]) -> str:
    lines = [
        "# Phase 9 Task 4 - Required Baseline Comparison",
        "",
        f"- Contract version: `{PHASE9_TASK4_CONTRACT_VERSION}`",
        f"- Preferred split: `{report.get('preferred_split')}`",
        f"- Assessment: `{(report.get('assessment') or {}).get('status')}`",
        f"- Held-out evidence available: `{(report.get('assessment') or {}).get('heldout_evidence_available')}`",
        "",
        "## Required Baselines",
    ]
    for item in report.get("required_baselines", []):
        lines.extend(
            [
                f"- `{item['baseline_key']}`",
                f"  AUC margin vs model: `{item.get('auc_margin_vs_model')}`",
                f"  Precision@10 margin vs model: `{item.get('precision_at_10_margin_vs_model')}`",
                f"  Status: `{item.get('status')}`",
            ]
        )
    return "\n".join(lines) + "\n"


def _render_calibration_markdown(calibration_profiles: list[Any], thresholds: dict[str, Any]) -> str:
    lines = [
        "# Phase 9 Task 4 - Calibration Report",
        "",
        f"- Contract version: `{PHASE9_TASK4_CONTRACT_VERSION}`",
        f"- Profiles written: `{len(calibration_profiles)}`",
        f"- WATCH threshold: `{thresholds.get('watch')}`",
        f"- ACTIONABLE threshold: `{thresholds.get('actionable')}`",
        f"- CRITICAL threshold: `{thresholds.get('critical')}`",
        "",
        "## Scope Summary",
    ]
    for profile in calibration_profiles:
        lines.append(
            f"- `{profile.profile_scope}:{profile.profile_key}` sample_count=`{profile.sample_count}` positive_rate=`{profile.positive_rate}`"
        )
    return "\n".join(lines) + "\n"


def _table_counts() -> dict[str, int]:
    conn = get_conn()
    try:
        return {
            table: int((conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]) or 0)
            for table in [
                "model_evaluation_runs",
                "calibration_profiles",
                "model_registry",
                "shadow_model_scores",
            ]
        }
    finally:
        conn.close()


def run_phase9_task4_model_completion() -> dict[str, Any]:
    apply_schema()
    cleanup_summary = cleanup_phase9_task4_state()

    phase5_repository = Phase5Repository()
    evaluation_rows = phase5_repository.load_evaluation_rows(start=PHASE9_TASK2_START, end=PHASE9_TASK2_END)
    training_frame, dataset_summary = build_training_frame(evaluation_rows, repository=phase5_repository)
    model_spec, fit_summary = fit_lightgbm_ranker(
        training_frame,
        model_version=PHASE9_TASK4_MODEL_VERSION,
        dataset_hash=dataset_summary.dataset_hash,
    )
    scored = score_training_frame(training_frame, model_spec=model_spec)
    score_report = build_score_report(scored)
    required_baseline_report = build_required_baseline_comparison(score_report)
    calibration_profiles = build_calibration_profiles(scored)
    thresholds = _thresholds_from_profiles(calibration_profiles)
    model_spec["thresholds"] = thresholds

    dataset_dir = REPO_ROOT / PHASE9_TASK4_DATASET_DIR
    model_dir = REPO_ROOT / PHASE9_TASK4_MODEL_DIR
    baseline_dir = REPO_ROOT / PHASE9_TASK4_BASELINE_DIR
    calibration_dir = REPO_ROOT / PHASE9_TASK4_CALIBRATION_DIR
    shadow_dir = REPO_ROOT / PHASE9_TASK4_SHADOW_DIR
    summary_dir = REPO_ROOT / PHASE9_TASK4_SUMMARY_DIR
    for path in [dataset_dir, model_dir, baseline_dir, calibration_dir, shadow_dir, summary_dir]:
        path.mkdir(parents=True, exist_ok=True)

    dataset_csv_path = dataset_dir / f"{dataset_summary.dataset_hash}.csv"
    dataset_meta_path = dataset_dir / f"{dataset_summary.dataset_hash}.json"
    model_path = model_dir / f"{PHASE9_TASK4_MODEL_VERSION}.json"
    report_path = model_dir / f"{PHASE9_TASK4_MODEL_VERSION}_report.json"
    model_card_path = model_dir / f"{PHASE9_TASK4_MODEL_VERSION}_model_card.md"
    scored_csv_path = model_dir / f"{PHASE9_TASK4_MODEL_VERSION}_scored.csv"
    baseline_json_path = baseline_dir / f"{PHASE9_TASK4_MODEL_VERSION}_required_baselines.json"
    baseline_md_path = baseline_dir / f"{PHASE9_TASK4_MODEL_VERSION}_required_baselines.md"
    calibration_json_path = calibration_dir / f"{PHASE9_TASK4_MODEL_VERSION}_calibration_report.json"
    calibration_md_path = calibration_dir / f"{PHASE9_TASK4_MODEL_VERSION}_calibration_report.md"
    shadow_json_path = shadow_dir / f"{PHASE9_TASK4_MODEL_VERSION}_reference_window.json"
    summary_json_path = summary_dir / "phase9_task4_summary.json"
    summary_md_path = summary_dir / "phase9_task4_summary.md"

    training_frame.to_csv(dataset_csv_path, index=False)
    dataset_meta_path.write_text(
        json.dumps(
            {
                "contract_version": PHASE9_TASK4_CONTRACT_VERSION,
                "summary": dataset_summary.to_dict(),
                "columns": list(training_frame.columns),
                "window": {"start": PHASE9_TASK2_START, "end": PHASE9_TASK2_END},
            },
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )
    model_path.write_text(json.dumps(model_spec, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    scored.to_csv(scored_csv_path, index=False)

    score_payload = {
        "contract_version": PHASE9_TASK4_CONTRACT_VERSION,
        "dataset_summary": dataset_summary.to_dict(),
        "fit_summary": fit_summary.to_dict(),
        "score_report": score_report,
        "required_baseline_report": required_baseline_report,
        "calibration_profiles": [profile.to_dict() for profile in calibration_profiles],
        "thresholds": thresholds,
        "artifacts": {
            "model_path": _repo_relative(model_path),
            "dataset_csv_path": _repo_relative(dataset_csv_path),
            "scored_csv_path": _repo_relative(scored_csv_path),
        },
    }
    report_path.write_text(json.dumps(score_payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    baseline_json_path.write_text(
        json.dumps(required_baseline_report, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    baseline_md_path.write_text(_render_baseline_markdown(required_baseline_report), encoding="utf-8")
    calibration_json_path.write_text(
        json.dumps(
            {
                "contract_version": PHASE9_TASK4_CONTRACT_VERSION,
                "thresholds": thresholds,
                "profiles": [profile.to_dict() for profile in calibration_profiles],
            },
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )
    calibration_md_path.write_text(
        _render_calibration_markdown(calibration_profiles, thresholds),
        encoding="utf-8",
    )
    model_card_path.write_text(
        build_model_card_markdown(
            model_version=PHASE9_TASK4_MODEL_VERSION,
            dataset_hash=dataset_summary.dataset_hash,
            score_report=score_report,
            calibration_profiles=calibration_profiles,
            model_kind=str(model_spec.get("kind")),
            required_baseline_report=required_baseline_report,
        ),
        encoding="utf-8",
    )

    repo = Phase6Repository()
    evaluation_summary = repo.record_evaluation_run(
        model_version=PHASE9_TASK4_MODEL_VERSION,
        feature_schema_version=PHASE6_FEATURE_SCHEMA_VERSION,
        dataset_hash=dataset_summary.dataset_hash,
        start=PHASE9_TASK2_START,
        end=PHASE9_TASK2_END,
        train_row_count=dataset_summary.train_row_count,
        validation_row_count=dataset_summary.validation_row_count,
        test_row_count=dataset_summary.test_row_count,
        labeled_row_count=dataset_summary.labeled_row_count,
        output_path=_repo_relative(report_path),
        summary_json=score_payload,
    )
    calibration_summaries = [
        repo.record_calibration_profile(
            model_version=PHASE9_TASK4_MODEL_VERSION,
            profile_scope=profile.profile_scope,
            profile_key=profile.profile_key,
            sample_count=profile.sample_count,
            positive_rate=profile.positive_rate,
            watch_threshold=profile.watch_threshold,
            actionable_threshold=profile.actionable_threshold,
            critical_threshold=profile.critical_threshold,
            metadata_json=profile.metadata,
        ).to_dict()
        for profile in calibration_profiles
    ]
    registry_summary = repo.register_model(
        model_name=PHASE9_TASK4_MODEL_NAME,
        model_version=PHASE9_TASK4_MODEL_VERSION,
        artifact_path=_repo_relative(model_path),
        feature_schema_version=PHASE6_FEATURE_SCHEMA_VERSION,
        training_dataset_hash=dataset_summary.dataset_hash,
        calibration_metadata={
            "thresholds": thresholds,
            "required_baseline_assessment": required_baseline_report.get("assessment"),
            "preferred_split": required_baseline_report.get("preferred_split"),
        },
        deployment_status="shadow",
        shadow_enabled=True,
        notes="Phase 9 Task 4 LightGBM shadow model over canonical reference window.",
    ).to_dict()
    active_summary = repo.activate_shadow_model(
        model_version=PHASE9_TASK4_MODEL_VERSION,
        retire_previous=True,
        notes="Activated by Phase 9 Task 4 canonical model-completion run.",
    )

    shadow_rows = build_shadow_scores(scored, model_spec=model_spec)
    shadow_summaries = [
        repo.log_shadow_score(
            model_version=PHASE9_TASK4_MODEL_VERSION,
            feature_schema_version=PHASE6_FEATURE_SCHEMA_VERSION,
            candidate_id=str(row["candidate_id"]),
            alert_id=row.get("alert_id"),
            market_id=str(row["market_id"]),
            score_value=float(row["score_value"]),
            score_label=row.get("score_label"),
            score_metadata=row.get("score_metadata"),
            scored_at=str(row["decision_timestamp"]),
        ).to_dict()
        for row in shadow_rows
    ]
    shadow_json_path.write_text(
        json.dumps(
            {
                "contract_version": PHASE9_TASK4_CONTRACT_VERSION,
                "model_version": PHASE9_TASK4_MODEL_VERSION,
                "feature_schema_version": PHASE6_FEATURE_SCHEMA_VERSION,
                "window": {"start": PHASE9_TASK2_START, "end": PHASE9_TASK2_END},
                "score_rows": shadow_rows,
            },
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )

    payload = {
        "task_contract_version": PHASE9_TASK4_CONTRACT_VERSION,
        "task_name": "Phase 9 Task 4 - SRS-Compliant Phase 6 Model Completion",
        "window": {
            "start": PHASE9_TASK2_START,
            "end": PHASE9_TASK2_END,
            "alignment": "Uses the same canonical Phase 9 reference hour already materialized through Task 3.",
        },
        "cleanup_summary": cleanup_summary,
        "dataset_summary": dataset_summary.to_dict(),
        "fit_summary": fit_summary.to_dict(),
        "required_baseline_report": required_baseline_report,
        "thresholds": thresholds,
        "evaluation_summary": evaluation_summary.to_dict(),
        "calibration_profiles_written": calibration_summaries,
        "registry_summary": registry_summary,
        "active_shadow_model": active_summary,
        "shadow_score_count": len(shadow_summaries),
        "artifacts": {
            "dataset_csv_path": _repo_relative(dataset_csv_path),
            "dataset_meta_path": _repo_relative(dataset_meta_path),
            "model_path": _repo_relative(model_path),
            "report_path": _repo_relative(report_path),
            "model_card_path": _repo_relative(model_card_path),
            "scored_csv_path": _repo_relative(scored_csv_path),
            "baseline_json_path": _repo_relative(baseline_json_path),
            "baseline_markdown_path": _repo_relative(baseline_md_path),
            "calibration_json_path": _repo_relative(calibration_json_path),
            "calibration_markdown_path": _repo_relative(calibration_md_path),
            "shadow_json_path": _repo_relative(shadow_json_path),
        },
        "table_counts_after": _table_counts(),
        "generated_at": _iso_now(),
    }
    summary_json_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    summary_md_path.write_text(
        "\n".join(
            [
                "# Phase 9 Task 4 - Phase 6 Model Completion",
                "",
                f"- Model version: `{PHASE9_TASK4_MODEL_VERSION}`",
                f"- Model kind: `{model_spec.get('kind')}`",
                f"- Preferred baseline split: `{required_baseline_report.get('preferred_split')}`",
                f"- Baseline assessment: `{(required_baseline_report.get('assessment') or {}).get('status')}`",
                f"- Thresholds: `WATCH={thresholds.get('watch')}`, `ACTIONABLE={thresholds.get('actionable')}`, `CRITICAL={thresholds.get('critical')}`",
                f"- Shadow scores written: `{len(shadow_summaries)}`",
                f"- Evaluation rows in DB: `{payload['table_counts_after']['model_evaluation_runs']}`",
                f"- Calibration rows in DB: `{payload['table_counts_after']['calibration_profiles']}`",
                f"- Registry rows in DB: `{payload['table_counts_after']['model_registry']}`",
                f"- Shadow-score rows in DB: `{payload['table_counts_after']['shadow_model_scores']}`",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    return payload
