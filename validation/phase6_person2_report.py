from __future__ import annotations

from collections import defaultdict

from phase6 import Phase6Repository


def _best_baseline_auc(test_metrics: dict) -> float | None:
    candidates = []
    for key in ("baseline_severity", "baseline_wallet", "baseline_velocity"):
        auc_value = ((test_metrics or {}).get(key) or {}).get("auc")
        if auc_value is not None:
            candidates.append(float(auc_value))
    return max(candidates) if candidates else None


def build_phase6_person2_report(*, limit: int = 10) -> dict:
    repo = Phase6Repository()
    evaluations = repo.list_recent_evaluation_runs(limit=max(1, limit))
    latest = evaluations[0] if evaluations else None
    calibration_profiles = repo.list_calibration_profiles(
        model_version=(latest or {}).get("model_version"),
        limit=200,
    )

    grouped_profiles: dict[str, list[dict]] = defaultdict(list)
    for profile in calibration_profiles:
        grouped_profiles[str(profile["profile_scope"])].append(profile)

    assessment_status = "no_evaluations_yet"
    baseline_margin = None
    if latest:
        splits = ((latest.get("summary_json") or {}).get("score_report") or {}).get("splits") or {}
        test_metrics = splits.get("test") or {}
        model_auc = ((test_metrics.get("model") or {}).get("auc"))
        baseline_auc = _best_baseline_auc(test_metrics)
        if model_auc is not None and baseline_auc is not None:
            baseline_margin = round(float(model_auc) - float(baseline_auc), 6)
            assessment_status = "model_beats_baseline" if baseline_margin >= 0 else "baseline_still_stronger"
        else:
            assessment_status = "descriptive_only"

    return {
        "latest_evaluation_run": latest,
        "recent_evaluation_runs": evaluations,
        "calibration_profiles": dict(grouped_profiles),
        "assessment": {
            "status": assessment_status,
            "baseline_margin_auc": baseline_margin,
            "latest_model_version": (latest or {}).get("model_version"),
            "calibration_profile_count": len(calibration_profiles),
        },
    }
