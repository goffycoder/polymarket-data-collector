from __future__ import annotations

import math
from dataclasses import asdict, dataclass
from typing import Any

import pandas as pd

from phase6.training import NUMERIC_MODEL_FEATURES
from phase7.graph_features import GRAPH_FEATURE_COLUMNS


ADVANCED_GRAPH_MODEL_FEATURES = [
    *NUMERIC_MODEL_FEATURES,
    *GRAPH_FEATURE_COLUMNS,
]


@dataclass(slots=True)
class Phase7AdvancedModelFitSummary:
    model_version: str
    dataset_hash: str
    labeled_row_count: int
    train_row_count: int
    feature_count: int
    baseline_feature_count: int
    graph_feature_count: int
    base_rate: float
    status: str
    notes: list[str]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _fit_linear_spec(
    frame: pd.DataFrame,
    *,
    model_version: str,
    dataset_hash: str,
    feature_order: list[str],
    model_kind: str,
) -> tuple[dict[str, Any] | None, Phase7AdvancedModelFitSummary]:
    train = frame[(frame["dataset_partition"] == "train") & (frame["label_available"])]
    if train.empty:
        return None, Phase7AdvancedModelFitSummary(
            model_version=model_version,
            dataset_hash=dataset_hash,
            labeled_row_count=int(frame["label_available"].sum()) if "label_available" in frame.columns else 0,
            train_row_count=0,
            feature_count=len(feature_order),
            baseline_feature_count=len(NUMERIC_MODEL_FEATURES),
            graph_feature_count=len(GRAPH_FEATURE_COLUMNS),
            base_rate=0.0,
            status="insufficient_training_data",
            notes=["no_labeled_train_rows"],
        )

    labels = train["label_success"].astype(float)
    positives = train[labels == 1.0]
    negatives = train[labels == 0.0]
    if positives.empty or negatives.empty:
        missing = "no_positive_labels" if positives.empty else "no_negative_labels"
        return None, Phase7AdvancedModelFitSummary(
            model_version=model_version,
            dataset_hash=dataset_hash,
            labeled_row_count=int(frame["label_available"].sum()),
            train_row_count=int(len(train)),
            feature_count=len(feature_order),
            baseline_feature_count=len(NUMERIC_MODEL_FEATURES),
            graph_feature_count=len(GRAPH_FEATURE_COLUMNS),
            base_rate=round(float(labels.mean()), 6) if len(labels) else 0.0,
            status="insufficient_training_data",
            notes=[missing],
        )

    feature_stats: dict[str, dict[str, float]] = {}
    weights: dict[str, float] = {}
    for column in feature_order:
        series = train[column].fillna(0.0).astype(float) if column in train.columns else pd.Series(0.0, index=train.index)
        mean_value = float(series.mean())
        std_raw = float(series.std())
        std_value = std_raw if std_raw > 1e-9 else 1.0
        pos_mean = float(positives[column].fillna(0.0).astype(float).mean()) if column in positives.columns else 0.0
        neg_mean = float(negatives[column].fillna(0.0).astype(float).mean()) if column in negatives.columns else 0.0
        feature_stats[column] = {
            "mean": mean_value,
            "std": std_value,
        }
        weights[column] = (pos_mean - neg_mean) / std_value

    total_abs_weight = sum(abs(value) for value in weights.values()) or 1.0
    normalized_weights = {
        key: round(value / total_abs_weight, 8)
        for key, value in weights.items()
    }
    base_rate = float(labels.mean())
    base_rate = min(0.999999, max(0.000001, base_rate))
    intercept = math.log(base_rate / (1.0 - base_rate))

    model_spec = {
        "kind": model_kind,
        "model_version": model_version,
        "dataset_hash": dataset_hash,
        "feature_schema_version": str(frame["feature_schema_version"].iloc[0]),
        "feature_order": feature_order,
        "feature_stats": feature_stats,
        "weights": normalized_weights,
        "intercept": round(intercept, 8),
    }
    summary = Phase7AdvancedModelFitSummary(
        model_version=model_version,
        dataset_hash=dataset_hash,
        labeled_row_count=int(frame["label_available"].sum()),
        train_row_count=int(len(train)),
        feature_count=len(feature_order),
        baseline_feature_count=len(NUMERIC_MODEL_FEATURES),
        graph_feature_count=len(GRAPH_FEATURE_COLUMNS),
        base_rate=round(base_rate, 6),
        status="trained",
        notes=[],
    )
    return model_spec, summary


def fit_graph_aware_ranker(
    frame: pd.DataFrame,
    *,
    model_version: str,
    dataset_hash: str,
) -> tuple[dict[str, Any] | None, Phase7AdvancedModelFitSummary]:
    return _fit_linear_spec(
        frame,
        model_version=model_version,
        dataset_hash=dataset_hash,
        feature_order=ADVANCED_GRAPH_MODEL_FEATURES,
        model_kind="phase7_graph_aware_ranker_v1",
    )


def score_with_model_spec(frame: pd.DataFrame, *, model_spec: dict[str, Any]) -> pd.DataFrame:
    scored = frame.copy()
    weights = model_spec.get("weights", {})
    feature_stats = model_spec.get("feature_stats", {})
    linear = pd.Series(float(model_spec.get("intercept", 0.0)), index=scored.index, dtype="float64")

    for column in model_spec.get("feature_order", []):
        stats = feature_stats.get(column, {})
        mean_value = _safe_float(stats.get("mean"), 0.0)
        std_value = max(1e-9, _safe_float(stats.get("std"), 1.0))
        source = scored[column].fillna(0.0).astype(float) if column in scored.columns else pd.Series(0.0, index=scored.index)
        normalized = (source - mean_value) / std_value
        linear = linear + (normalized * _safe_float(weights.get(column), 0.0))

    scored["model_linear_score"] = linear
    scored["model_score"] = linear.map(
        lambda value: 1.0 / (1.0 + math.exp(max(-50.0, min(50.0, -float(value)))))
    )
    scored["baseline_severity_score"] = scored["candidate_severity_score"].fillna(0.0).astype(float)
    scored["baseline_wallet_score"] = (
        scored["fresh_wallet_count"].fillna(0.0).astype(float)
        + scored["fresh_wallet_notional_share"].fillna(0.0).astype(float)
    )
    scored["baseline_velocity_score"] = scored["probability_velocity_abs"].fillna(0.0).astype(float)
    return scored
