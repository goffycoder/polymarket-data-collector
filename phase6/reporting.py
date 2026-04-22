from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any

import pandas as pd

from config.settings import (
    PHASE6_ACTIONABLE_PRECISION_TARGET,
    PHASE6_CALIBRATION_VERSION,
    PHASE6_CRITICAL_PRECISION_TARGET,
    PHASE6_EVALUATION_VERSION,
    PHASE6_WATCH_PRECISION_TARGET,
)

REQUIRED_BASELINE_COLUMNS = {
    "baseline_probability_momentum": "baseline_probability_momentum_score",
    "baseline_order_imbalance": "baseline_order_imbalance_score",
    "baseline_microstructure": "baseline_microstructure_score",
    "baseline_external_evidence": "baseline_external_evidence_score",
    "baseline_fresh_wallet": "baseline_fresh_wallet_score",
}


@dataclass(slots=True)
class CalibrationProfile:
    profile_scope: str
    profile_key: str
    sample_count: int
    positive_rate: float | None
    watch_threshold: float | None
    actionable_threshold: float | None
    critical_threshold: float | None
    metadata: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def _safe_ratio(numerator: float, denominator: float) -> float:
    if not denominator:
        return 0.0
    return float(numerator) / float(denominator)


def _auc(labels: list[int], scores: list[float]) -> float | None:
    positives = [(score, label) for score, label in zip(scores, labels) if label == 1]
    negatives = [(score, label) for score, label in zip(scores, labels) if label == 0]
    if not positives or not negatives:
        return None
    wins = 0.0
    ties = 0.0
    for pos_score, _ in positives:
        for neg_score, _ in negatives:
            if pos_score > neg_score:
                wins += 1.0
            elif pos_score == neg_score:
                ties += 1.0
    return (wins + 0.5 * ties) / (len(positives) * len(negatives))


def _precision_at_k(labels: list[int], scores: list[float], k: int) -> float | None:
    if not labels or k <= 0:
        return None
    ranked = sorted(zip(scores, labels), key=lambda item: item[0], reverse=True)[:k]
    if not ranked:
        return None
    return _safe_ratio(sum(label for _, label in ranked), len(ranked))


def _score_metrics(frame: pd.DataFrame, *, score_column: str) -> dict[str, Any]:
    labeled = frame[frame["label_available"]].copy()
    if labeled.empty:
        return {
            "row_count": 0,
            "auc": None,
            "precision_at_10": None,
            "precision_at_25": None,
            "mean_score": None,
            "positive_rate": None,
        }
    labels = [int(value) for value in labeled["label_success"].tolist()]
    scores = [float(value) for value in labeled[score_column].fillna(0.0).tolist()]
    auc_value = _auc(labels, scores)
    precision_at_10 = _precision_at_k(labels, scores, min(10, len(labels)))
    precision_at_25 = _precision_at_k(labels, scores, min(25, len(labels)))
    return {
        "row_count": int(len(labeled)),
        "auc": round(auc_value, 6) if auc_value is not None else None,
        "precision_at_10": round(precision_at_10, 6) if precision_at_10 is not None else None,
        "precision_at_25": round(precision_at_25, 6) if precision_at_25 is not None else None,
        "mean_score": round(sum(scores) / len(scores), 6) if scores else None,
        "positive_rate": round(sum(labels) / len(labels), 6) if labels else None,
    }


def build_score_report(scored_frame: pd.DataFrame) -> dict[str, Any]:
    report: dict[str, Any] = {
        "evaluation_version": PHASE6_EVALUATION_VERSION,
        "splits": {},
    }
    for split_name in ("train", "validation", "test"):
        split_frame = scored_frame[scored_frame["dataset_partition"] == split_name]
        split_report = {
            "model": _score_metrics(split_frame, score_column="model_score"),
            "baseline_severity": _score_metrics(split_frame, score_column="baseline_severity_score"),
            "baseline_wallet": _score_metrics(split_frame, score_column="baseline_wallet_score"),
            "baseline_velocity": _score_metrics(split_frame, score_column="baseline_velocity_score"),
        }
        for baseline_key, column_name in REQUIRED_BASELINE_COLUMNS.items():
            split_report[baseline_key] = _score_metrics(split_frame, score_column=column_name)
        report["splits"][split_name] = split_report
    return report


def build_required_baseline_comparison(score_report: dict[str, Any]) -> dict[str, Any]:
    splits = (score_report.get("splits") or {})
    preferred_split = None
    for split_name in ("test", "validation", "train"):
        model_metrics = ((splits.get(split_name) or {}).get("model") or {})
        if int(model_metrics.get("row_count") or 0) > 0:
            preferred_split = split_name
            break

    if preferred_split is None:
        return {
            "preferred_split": None,
            "assessment": {
                "status": "no_labeled_rows",
                "heldout_evidence_available": False,
                "model_beats_all_required_baselines": None,
            },
            "required_baselines": [],
        }

    split_payload = splits.get(preferred_split) or {}
    model_metrics = split_payload.get("model") or {}
    baseline_rows = []
    auc_margins: list[bool] = []
    p10_margins: list[bool] = []
    for baseline_key in REQUIRED_BASELINE_COLUMNS:
        baseline_metrics = split_payload.get(baseline_key) or {}
        auc_margin = None
        if model_metrics.get("auc") is not None and baseline_metrics.get("auc") is not None:
            auc_margin = round(float(model_metrics["auc"]) - float(baseline_metrics["auc"]), 6)
            auc_margins.append(auc_margin >= 0)
        precision_margin = None
        if model_metrics.get("precision_at_10") is not None and baseline_metrics.get("precision_at_10") is not None:
            precision_margin = round(
                float(model_metrics["precision_at_10"]) - float(baseline_metrics["precision_at_10"]),
                6,
            )
            p10_margins.append(precision_margin >= 0)
        baseline_rows.append(
            {
                "baseline_key": baseline_key,
                "auc_margin_vs_model": auc_margin,
                "precision_at_10_margin_vs_model": precision_margin,
                "baseline_metrics": baseline_metrics,
                "status": (
                    "model_stronger"
                    if (auc_margin is not None and auc_margin >= 0 and precision_margin is not None and precision_margin >= 0)
                    else "inconclusive" if auc_margin is None and precision_margin is None
                    else "baseline_stronger_or_mixed"
                ),
            }
        )

    heldout = preferred_split in {"validation", "test"}
    if not heldout:
        status = "descriptive_only_train_split"
    elif all(auc_margins or [False]) and all(p10_margins or [False]):
        status = "model_beats_required_baselines"
    else:
        status = "required_baselines_still_competitive"
    return {
        "preferred_split": preferred_split,
        "assessment": {
            "status": status,
            "heldout_evidence_available": heldout,
            "model_beats_all_required_baselines": (
                bool(all(auc_margins or [False]) and all(p10_margins or [False])) if heldout else None
            ),
        },
        "required_baselines": baseline_rows,
    }


def _threshold_for_precision(frame: pd.DataFrame, *, score_column: str, target_precision: float) -> float | None:
    labeled = frame[frame["label_available"]].sort_values(score_column, ascending=False)
    if labeled.empty:
        return None
    running_hits = 0
    for idx, (_, row) in enumerate(labeled.iterrows(), start=1):
        running_hits += int(row["label_success"])
        precision = running_hits / idx
        if precision >= target_precision:
            return float(row[score_column])
    return float(labeled[score_column].quantile(max(0.0, min(1.0, 1.0 - target_precision))))


def build_calibration_profiles(scored_frame: pd.DataFrame) -> list[CalibrationProfile]:
    validation = scored_frame[
        (scored_frame["dataset_partition"] == "validation") & (scored_frame["label_available"])
    ]
    if validation.empty:
        validation = scored_frame[scored_frame["label_available"]]

    profiles: list[CalibrationProfile] = []
    scopes = {
        "global": [("global", validation)],
        "liquidity_bucket": list(validation.groupby("liquidity_bucket", dropna=False)),
        "category": list(validation.groupby("category_key", dropna=False)),
    }

    for scope_name, groups in scopes.items():
        for profile_key, group_frame in groups:
            labeled = group_frame[group_frame["label_available"]]
            if labeled.empty:
                continue
            positive_rate = float(labeled["label_success"].mean())
            watch_threshold = _threshold_for_precision(
                labeled,
                score_column="model_score",
                target_precision=PHASE6_WATCH_PRECISION_TARGET,
            )
            actionable_threshold = _threshold_for_precision(
                labeled,
                score_column="model_score",
                target_precision=PHASE6_ACTIONABLE_PRECISION_TARGET,
            )
            critical_threshold = _threshold_for_precision(
                labeled,
                score_column="model_score",
                target_precision=PHASE6_CRITICAL_PRECISION_TARGET,
            )
            profiles.append(
                CalibrationProfile(
                    profile_scope=scope_name,
                    profile_key=str(profile_key),
                    sample_count=int(len(labeled)),
                    positive_rate=round(positive_rate, 6),
                    watch_threshold=round(watch_threshold, 6) if watch_threshold is not None else None,
                    actionable_threshold=round(actionable_threshold, 6) if actionable_threshold is not None else None,
                    critical_threshold=round(critical_threshold, 6) if critical_threshold is not None else None,
                    metadata={
                        "calibration_version": PHASE6_CALIBRATION_VERSION,
                        "mean_model_score": round(float(labeled["model_score"].mean()), 6),
                    },
                )
            )
    return profiles


def build_model_card_markdown(
    *,
    model_version: str,
    dataset_hash: str,
    score_report: dict[str, Any],
    calibration_profiles: list[CalibrationProfile],
    model_kind: str | None = None,
    required_baseline_report: dict[str, Any] | None = None,
) -> str:
    lines = [
        f"# Phase 6 Model Card: {model_version}",
        "",
        "## Summary",
        f"- Evaluation version: `{PHASE6_EVALUATION_VERSION}`",
        f"- Calibration version: `{PHASE6_CALIBRATION_VERSION}`",
        f"- Dataset hash: `{dataset_hash}`",
        f"- Model kind: `{model_kind or 'phase6_linear_ranker_v1'}`",
        "",
        "## Split Metrics",
    ]
    for split_name, payload in score_report.get("splits", {}).items():
        model_metrics = payload.get("model", {})
        lines.extend(
            [
                f"### {split_name.title()}",
                f"- Model AUC: `{model_metrics.get('auc')}`",
                f"- Model Precision@10: `{model_metrics.get('precision_at_10')}`",
                f"- Model Precision@25: `{model_metrics.get('precision_at_25')}`",
                f"- Positive rate: `{model_metrics.get('positive_rate')}`",
                "",
            ]
        )
    if required_baseline_report is not None:
        lines.extend(
            [
                "## Required Baselines",
                f"- Preferred comparison split: `{required_baseline_report.get('preferred_split')}`",
                f"- Assessment: `{(required_baseline_report.get('assessment') or {}).get('status')}`",
            ]
        )
        for item in required_baseline_report.get("required_baselines", []):
            lines.append(
                f"- `{item['baseline_key']}`: auc_margin=`{item.get('auc_margin_vs_model')}`, precision_at_10_margin=`{item.get('precision_at_10_margin_vs_model')}`"
            )
        lines.append("")
    lines.extend(
        [
            "## Calibration Coverage",
            f"- Profiles written: `{len(calibration_profiles)}`",
            "- Liquidity and category slices are advisory until larger labeled windows exist.",
            "",
            "## Known Failure Modes",
            "- Direction labels inherit Phase 5 directional inference and can fail when velocity is weak or stale.",
            "- Sparse or missing market resolution rows reduce label coverage and can bias holdout metrics.",
            "- Liquidity bucketing currently relies on early post-decision spread snapshots, so thin coverage can blur calibration quality.",
            (
                "- This artifact is a LightGBM boosted-tree shadow model, but the current local dataset is still tiny enough that its ranking quality remains only weakly evidenced."
                if str(model_kind or "").startswith("phase6_lightgbm")
                else "- This starter ranker is a linear baseline foundation, not the final boosted-tree model promised in the full Phase 6 scope."
            ),
            "",
            "## Deployment Guidance",
            "- Treat thresholds as shadow-only recommendations until they survive larger replay windows.",
            "- Refit and recalibrate whenever the feature schema or label contract changes.",
            "",
        ]
    )
    return "\n".join(lines)
