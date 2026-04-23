from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from config.settings import PHASE6_DEFAULT_MODEL_NAME, PHASE6_FEATURE_SCHEMA_VERSION, REPO_ROOT
from database.db_manager import apply_schema
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
from phase10.heldout_family import PHASE10_HELDOUT_OVERALL_END, PHASE10_HELDOUT_OVERALL_START, materialize_phase10_heldout_family


PHASE10_TASK4_CONTRACT_VERSION = "phase10_task4_heldout_model_completion_v1"
PHASE10_TASK4_MODEL_VERSION = "phase10_task4_lightgbm_v1"
PHASE10_TASK4_MODEL_NAME = PHASE6_DEFAULT_MODEL_NAME
PHASE10_TASK4_DATASET_DIR = "reports/phase6/training_datasets/phase10_task4"
PHASE10_TASK4_MODEL_DIR = "reports/phase6/model_artifacts/phase10_task4"
PHASE10_TASK4_BASELINE_DIR = "reports/phase6/baseline_comparisons"
PHASE10_TASK4_CALIBRATION_DIR = "reports/phase6/calibration"
PHASE10_TASK4_SHADOW_DIR = "reports/phase6/shadow_scores"
PHASE10_TASK4_SUMMARY_DIR = "reports/phase10/heldout_model_completion"


def _iso_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _repo_relative(path: Path) -> str:
    try:
        return str(path.resolve().relative_to(REPO_ROOT.resolve())).replace("\\", "/")
    except ValueError:
        return str(path)


def _thresholds_from_profiles(profiles: list[Any]) -> dict[str, float | None]:
    for profile in profiles:
        if profile.profile_scope == "global" and profile.profile_key == "global":
            return {
                "watch": profile.watch_threshold,
                "actionable": profile.actionable_threshold,
                "critical": profile.critical_threshold,
            }
    return {"watch": None, "actionable": None, "critical": None}


def run_phase10_task4_heldout_model_completion() -> dict[str, Any]:
    apply_schema()
    seed_summary = materialize_phase10_heldout_family()
    phase5_repository = Phase5Repository()
    evaluation_rows = phase5_repository.load_evaluation_rows(
        start=PHASE10_HELDOUT_OVERALL_START,
        end=PHASE10_HELDOUT_OVERALL_END,
    )
    training_frame, dataset_summary = build_training_frame(evaluation_rows, repository=phase5_repository)
    model_spec, fit_summary = fit_lightgbm_ranker(
        training_frame,
        model_version=PHASE10_TASK4_MODEL_VERSION,
        dataset_hash=dataset_summary.dataset_hash,
    )
    scored = score_training_frame(training_frame, model_spec=model_spec)
    score_report = build_score_report(scored)
    required_baseline_report = build_required_baseline_comparison(score_report)
    if (required_baseline_report.get("assessment") or {}).get("status") != "model_beats_required_baselines":
        raise RuntimeError(
            "Phase 10 Task 4 expected the held-out LightGBM model to beat the required baselines on held-out evidence."
        )
    calibration_profiles = build_calibration_profiles(scored)
    thresholds = _thresholds_from_profiles(calibration_profiles)
    model_spec["thresholds"] = thresholds

    dataset_dir = REPO_ROOT / PHASE10_TASK4_DATASET_DIR
    model_dir = REPO_ROOT / PHASE10_TASK4_MODEL_DIR
    baseline_dir = REPO_ROOT / PHASE10_TASK4_BASELINE_DIR
    calibration_dir = REPO_ROOT / PHASE10_TASK4_CALIBRATION_DIR
    shadow_dir = REPO_ROOT / PHASE10_TASK4_SHADOW_DIR
    summary_dir = REPO_ROOT / PHASE10_TASK4_SUMMARY_DIR
    for path in [dataset_dir, model_dir, baseline_dir, calibration_dir, shadow_dir, summary_dir]:
        path.mkdir(parents=True, exist_ok=True)

    dataset_csv_path = dataset_dir / f"{dataset_summary.dataset_hash}.csv"
    dataset_meta_path = dataset_dir / f"{dataset_summary.dataset_hash}.json"
    model_path = model_dir / f"{PHASE10_TASK4_MODEL_VERSION}.json"
    report_path = model_dir / f"{PHASE10_TASK4_MODEL_VERSION}_report.json"
    model_card_path = model_dir / f"{PHASE10_TASK4_MODEL_VERSION}_model_card.md"
    scored_csv_path = model_dir / f"{PHASE10_TASK4_MODEL_VERSION}_scored.csv"
    baseline_json_path = baseline_dir / f"{PHASE10_TASK4_MODEL_VERSION}_required_baselines.json"
    baseline_md_path = baseline_dir / f"{PHASE10_TASK4_MODEL_VERSION}_required_baselines.md"
    calibration_json_path = calibration_dir / f"{PHASE10_TASK4_MODEL_VERSION}_calibration_report.json"
    calibration_md_path = calibration_dir / f"{PHASE10_TASK4_MODEL_VERSION}_calibration_report.md"
    shadow_json_path = shadow_dir / f"{PHASE10_TASK4_MODEL_VERSION}_heldout_family.json"

    training_frame.to_csv(dataset_csv_path, index=False)
    dataset_meta_path.write_text(
        json.dumps(
            {
                "contract_version": PHASE10_TASK4_CONTRACT_VERSION,
                "summary": dataset_summary.to_dict(),
                "columns": list(training_frame.columns),
                "window": {"start": PHASE10_HELDOUT_OVERALL_START, "end": PHASE10_HELDOUT_OVERALL_END},
            },
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )
    model_path.write_text(json.dumps(model_spec, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    scored.to_csv(scored_csv_path, index=False)

    report_payload = {
        "contract_version": PHASE10_TASK4_CONTRACT_VERSION,
        "dataset_summary": dataset_summary.to_dict(),
        "fit_summary": fit_summary.to_dict(),
        "score_report": score_report,
        "required_baseline_report": required_baseline_report,
        "calibration_profiles": [profile.to_dict() for profile in calibration_profiles],
        "thresholds": thresholds,
    }
    report_path.write_text(json.dumps(report_payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    baseline_json_path.write_text(
        json.dumps(required_baseline_report, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    baseline_md_path.write_text(
        "\n".join(
            [
                "# Phase 10 Task 4 - Required Baseline Comparison",
                "",
                f"- Preferred split: `{required_baseline_report.get('preferred_split')}`",
                f"- Assessment: `{(required_baseline_report.get('assessment') or {}).get('status')}`",
            ]
            + [
                f"- `{item['baseline_key']}` auc_margin=`{item.get('auc_margin_vs_model')}` precision_at_10_margin=`{item.get('precision_at_10_margin_vs_model')}`"
                for item in required_baseline_report.get("required_baselines", [])
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    calibration_json_path.write_text(
        json.dumps(
            {
                "contract_version": PHASE10_TASK4_CONTRACT_VERSION,
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
        "\n".join(
            [
                "# Phase 10 Task 4 - Calibration Report",
                "",
                f"- Profiles written: `{len(calibration_profiles)}`",
                f"- WATCH threshold: `{thresholds.get('watch')}`",
                f"- ACTIONABLE threshold: `{thresholds.get('actionable')}`",
                f"- CRITICAL threshold: `{thresholds.get('critical')}`",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    model_card_path.write_text(
        build_model_card_markdown(
            model_version=PHASE10_TASK4_MODEL_VERSION,
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
        model_version=PHASE10_TASK4_MODEL_VERSION,
        feature_schema_version=PHASE6_FEATURE_SCHEMA_VERSION,
        dataset_hash=dataset_summary.dataset_hash,
        start=PHASE10_HELDOUT_OVERALL_START,
        end=PHASE10_HELDOUT_OVERALL_END,
        train_row_count=dataset_summary.train_row_count,
        validation_row_count=dataset_summary.validation_row_count,
        test_row_count=dataset_summary.test_row_count,
        labeled_row_count=dataset_summary.labeled_row_count,
        output_path=_repo_relative(report_path),
        summary_json=report_payload,
    )
    calibration_summaries = [
        repo.record_calibration_profile(
            model_version=PHASE10_TASK4_MODEL_VERSION,
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
        model_name=PHASE10_TASK4_MODEL_NAME,
        model_version=PHASE10_TASK4_MODEL_VERSION,
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
        notes="Phase 10 Task 4 held-out LightGBM shadow model over the canonical held-out window family.",
    ).to_dict()
    active_summary = repo.activate_shadow_model(
        model_version=PHASE10_TASK4_MODEL_VERSION,
        retire_previous=True,
        notes="Activated by Phase 10 Task 4 held-out model completion.",
    )
    shadow_rows = build_shadow_scores(scored, model_spec=model_spec)
    shadow_summaries = [
        repo.log_shadow_score(
            model_version=PHASE10_TASK4_MODEL_VERSION,
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
                "contract_version": PHASE10_TASK4_CONTRACT_VERSION,
                "model_version": PHASE10_TASK4_MODEL_VERSION,
                "feature_schema_version": PHASE6_FEATURE_SCHEMA_VERSION,
                "window": {"start": PHASE10_HELDOUT_OVERALL_START, "end": PHASE10_HELDOUT_OVERALL_END},
                "score_rows": shadow_rows,
            },
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )

    summary_payload = {
        "task_contract_version": PHASE10_TASK4_CONTRACT_VERSION,
        "task_name": "Phase 10 Task 4 - Held-Out Phase 6 Boosted-Tree Completion",
        "generated_at": _iso_now(),
        "seed_summary": seed_summary,
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
    }
    (summary_dir / "phase10_task4_summary.json").write_text(
        json.dumps(summary_payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    (summary_dir / "phase10_task4_summary.md").write_text(
        "\n".join(
            [
                "# Phase 10 Task 4 - Held-Out Model Completion",
                "",
                f"- Dataset rows: `{dataset_summary.row_count}`",
                f"- Train/validation/test: `{dataset_summary.train_row_count}/{dataset_summary.validation_row_count}/{dataset_summary.test_row_count}`",
                f"- Baseline assessment: `{(required_baseline_report.get('assessment') or {}).get('status')}`",
                f"- Shadow scores written: `{len(shadow_summaries)}`",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    return summary_payload
