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
        report["splits"][split_name] = {
            "model": _score_metrics(split_frame, score_column="model_score"),
            "baseline_severity": _score_metrics(split_frame, score_column="baseline_severity_score"),
            "baseline_wallet": _score_metrics(split_frame, score_column="baseline_wallet_score"),
            "baseline_velocity": _score_metrics(split_frame, score_column="baseline_velocity_score"),
        }
    return report


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
) -> str:
    lines = [
        f"# Phase 6 Model Card: {model_version}",
        "",
        "## Summary",
        f"- Evaluation version: `{PHASE6_EVALUATION_VERSION}`",
        f"- Calibration version: `{PHASE6_CALIBRATION_VERSION}`",
        f"- Dataset hash: `{dataset_hash}`",
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
            "- This starter ranker is a linear baseline foundation, not the final boosted-tree model promised in the full Phase 6 scope.",
            "",
            "## Deployment Guidance",
            "- Treat thresholds as shadow-only recommendations until they survive larger replay windows.",
            "- Refit and recalibrate whenever the feature schema or label contract changes.",
            "",
        ]
    )
    return "\n".join(lines)
