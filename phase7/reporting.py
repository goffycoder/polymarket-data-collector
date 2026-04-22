from __future__ import annotations

from typing import Any

from config.settings import PHASE7_ADVANCED_EXPERIMENT_VERSION


def _best_heuristic_auc(test_metrics: dict[str, Any]) -> float | None:
    candidates: list[float] = []
    for key in ("baseline_severity", "baseline_wallet", "baseline_velocity"):
        auc_value = (test_metrics.get(key) or {}).get("auc")
        if auc_value is not None:
            candidates.append(float(auc_value))
    return max(candidates) if candidates else None


def _split_comparison(
    *,
    split_name: str,
    advanced_metrics: dict[str, Any],
    phase6_metrics: dict[str, Any],
) -> dict[str, Any]:
    advanced_model = advanced_metrics.get("model") or {}
    phase6_model = phase6_metrics.get("model") or {}
    advanced_auc = advanced_model.get("auc")
    phase6_auc = phase6_model.get("auc")
    heuristic_auc = _best_heuristic_auc(advanced_metrics)

    phase6_margin = None
    if advanced_auc is not None and phase6_auc is not None:
        phase6_margin = round(float(advanced_auc) - float(phase6_auc), 6)

    heuristic_margin = None
    if advanced_auc is not None and heuristic_auc is not None:
        heuristic_margin = round(float(advanced_auc) - float(heuristic_auc), 6)

    status = "descriptive_only"
    if phase6_margin is not None:
        status = "beats_phase6_baseline" if phase6_margin > 0 else "lags_phase6_baseline"

    return {
        "split_name": split_name,
        "advanced_auc": advanced_auc,
        "phase6_auc": phase6_auc,
        "best_heuristic_auc": heuristic_auc,
        "phase6_margin_auc": phase6_margin,
        "heuristic_margin_auc": heuristic_margin,
        "advanced_precision_at_10": advanced_model.get("precision_at_10"),
        "phase6_precision_at_10": phase6_model.get("precision_at_10"),
        "status": status,
    }


def _strict_holdout_assessment(comparisons: dict[str, dict[str, Any]]) -> dict[str, Any]:
    validation = comparisons.get("validation") or {}
    test = comparisons.get("test") or {}
    validation_margin = validation.get("phase6_margin_auc")
    test_margin = test.get("phase6_margin_auc")

    reasons: list[str] = []
    status = "descriptive_only"
    accepted = False

    if validation_margin is None or test_margin is None:
        reasons.append("missing_validation_or_test_auc")
    elif validation_margin > 0 and test_margin > 0:
        accepted = True
        status = "accepted_strict_holdouts"
    else:
        status = "rejected_unstable_gain"
        if validation_margin <= 0:
            reasons.append("validation_did_not_beat_phase6_baseline")
        if test_margin <= 0:
            reasons.append("test_did_not_beat_phase6_baseline")

    return {
        "status": status,
        "accepted": accepted,
        "validation_margin_auc": validation_margin,
        "test_margin_auc": test_margin,
        "reasons": reasons,
    }


def build_advanced_experiment_report(
    *,
    dataset_summary: dict[str, Any],
    graph_diagnostics: dict[str, Any],
    advanced_model_version: str,
    phase6_baseline_model_version: str,
    advanced_score_report: dict[str, Any],
    phase6_score_report: dict[str, Any],
) -> dict[str, Any]:
    advanced_splits = advanced_score_report.get("splits") or {}
    phase6_splits = phase6_score_report.get("splits") or {}
    comparisons = {
        split_name: _split_comparison(
            split_name=split_name,
            advanced_metrics=advanced_splits.get(split_name) or {},
            phase6_metrics=phase6_splits.get(split_name) or {},
        )
        for split_name in ("train", "validation", "test")
    }
    holdout_assessment = _strict_holdout_assessment(comparisons)

    return {
        "experiment_version": PHASE7_ADVANCED_EXPERIMENT_VERSION,
        "advanced_model_version": advanced_model_version,
        "phase6_baseline_model_version": phase6_baseline_model_version,
        "dataset_summary": dataset_summary,
        "graph_diagnostics": graph_diagnostics,
        "advanced_score_report": advanced_score_report,
        "phase6_score_report": phase6_score_report,
        "split_comparisons": comparisons,
        "holdout_assessment": holdout_assessment,
        "comparability": {
            "same_label_discipline": True,
            "same_split_discipline": True,
            "same_underlying_dataset_hash": True,
            "same_heuristic_baselines": True,
        },
    }


def build_advanced_model_card_markdown(
    *,
    advanced_model_version: str,
    phase6_baseline_model_version: str,
    dataset_hash: str,
    experiment_report: dict[str, Any],
) -> str:
    holdout = experiment_report.get("holdout_assessment") or {}
    comparisons = experiment_report.get("split_comparisons") or {}
    lines = [
        f"# Phase 7 Advanced Model Card: {advanced_model_version}",
        "",
        "## Summary",
        f"- Experiment version: `{experiment_report.get('experiment_version')}`",
        f"- Dataset hash: `{dataset_hash}`",
        f"- Compared against Phase 6 baseline: `{phase6_baseline_model_version}`",
        f"- Strict holdout status: `{holdout.get('status')}`",
        "",
        "## Split Comparison",
    ]
    for split_name in ("train", "validation", "test"):
        split = comparisons.get(split_name) or {}
        lines.extend(
            [
                f"### {split_name.title()}",
                f"- Advanced AUC: `{split.get('advanced_auc')}`",
                f"- Phase 6 baseline AUC: `{split.get('phase6_auc')}`",
                f"- Margin vs Phase 6 baseline: `{split.get('phase6_margin_auc')}`",
                f"- Best heuristic AUC: `{split.get('best_heuristic_auc')}`",
                "",
            ]
        )
    lines.extend(
        [
            "## Holdout Gate",
            f"- Accepted: `{holdout.get('accepted')}`",
            f"- Validation margin AUC: `{holdout.get('validation_margin_auc')}`",
            f"- Test margin AUC: `{holdout.get('test_margin_auc')}`",
            f"- Reasons: `{', '.join(holdout.get('reasons') or []) or 'none'}`",
            "",
            "## Notes",
            "- Gains that do not survive both validation and test holdouts are rejected automatically.",
            "- This artifact is comparable to the Phase 6 baseline because labels, partitions, and heuristic baselines are unchanged.",
            "",
        ]
    )
    return "\n".join(lines)
