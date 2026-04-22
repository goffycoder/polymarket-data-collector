from __future__ import annotations

import json
import math
from dataclasses import asdict, dataclass
from datetime import timedelta
from hashlib import sha256
from typing import Any

import pandas as pd
try:
    import lightgbm as lgb
except ImportError:  # pragma: no cover - optional dependency is validated at runtime.
    lgb = None

from config.settings import (
    PHASE6_FEATURE_SCHEMA_VERSION,
    PHASE6_LIQUIDITY_SPREAD_TIGHT,
    PHASE6_LIQUIDITY_SPREAD_WIDE,
)
from ml_pipeline.feature_builder import build_features
from phase5.holdouts import build_event_family_fold_map, category_holdout_key, time_holdout_key
from phase5.models import EvaluationRow
from phase5.repository import Phase5Repository, _iso, _parse_iso
from phase5.simulator import infer_direction


@dataclass(slots=True)
class Phase6DatasetBuildSummary:
    feature_schema_version: str
    dataset_hash: str
    row_count: int
    labeled_row_count: int
    train_row_count: int
    validation_row_count: int
    test_row_count: int

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class Phase6ModelFitSummary:
    model_version: str
    dataset_hash: str
    labeled_row_count: int
    train_row_count: int
    feature_count: int
    base_rate: float
    artifact_path: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


NUMERIC_MODEL_FEATURES = [
    "candidate_severity_score",
    "alert_severity_rank",
    "has_alert",
    "rule_count",
    "fresh_wallet_count",
    "fresh_wallet_notional_share",
    "directional_imbalance",
    "concentration_ratio",
    "probability_velocity_abs",
    "probability_acceleration_abs",
    "volume_acceleration",
]

EVIDENCE_STATE_SCORES = {
    "confirmed_public_reporting": 1.0,
    "plausible_open_source_signal": 0.8,
    "not_publicly_explained": 0.5,
    "pending_evidence": 0.25,
    "contradicted_or_explained": 0.05,
}


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def prepare_model_input_frame(
    frame: pd.DataFrame,
    *,
    feature_order: list[str] | None = None,
) -> pd.DataFrame:
    prepared = frame.copy()
    if "probability_velocity_abs" not in prepared.columns and "probability_velocity" in prepared.columns:
        prepared["probability_velocity_abs"] = prepared["probability_velocity"].fillna(0.0).astype(float).abs()
    if "probability_acceleration_abs" not in prepared.columns and "probability_acceleration" in prepared.columns:
        prepared["probability_acceleration_abs"] = (
            prepared["probability_acceleration"].fillna(0.0).astype(float).abs()
        )
    if feature_order:
        for column in feature_order:
            if column not in prepared.columns:
                prepared[column] = 0.0
    return prepared


def _append_required_baseline_scores(frame: pd.DataFrame) -> pd.DataFrame:
    enriched = prepare_model_input_frame(frame)
    enriched["baseline_severity_score"] = enriched["candidate_severity_score"].fillna(0.0).astype(float)
    enriched["baseline_probability_momentum_score"] = (
        enriched["probability_velocity_abs"].fillna(0.0).astype(float)
    )
    enriched["baseline_order_imbalance_score"] = (
        enriched["directional_imbalance"].fillna(0.0).astype(float).abs()
    )
    enriched["baseline_microstructure_score"] = (
        enriched["baseline_probability_momentum_score"] * 0.35
        + enriched["baseline_order_imbalance_score"] * 0.25
        + enriched["probability_acceleration_abs"].fillna(0.0).astype(float) * 0.20
        + enriched["concentration_ratio"].fillna(0.0).astype(float) * 0.10
        + enriched["volume_acceleration"].fillna(0.0).astype(float) * 0.10
    )
    enriched["baseline_external_evidence_score"] = (
        enriched.get("evidence_state", pd.Series(["pending_evidence"] * len(enriched), index=enriched.index))
        .astype(str)
        .map(lambda value: EVIDENCE_STATE_SCORES.get(value, EVIDENCE_STATE_SCORES["pending_evidence"]))
        .astype(float)
    )
    enriched["baseline_fresh_wallet_score"] = (
        (enriched["fresh_wallet_count"].fillna(0.0).astype(float) >= 3.0).astype(float)
        + (enriched["fresh_wallet_notional_share"].fillna(0.0).astype(float) >= 0.5).astype(float) * 0.5
    )
    enriched["baseline_wallet_score"] = enriched["baseline_fresh_wallet_score"]
    enriched["baseline_velocity_score"] = enriched["baseline_probability_momentum_score"]
    return enriched


def _positive_label(row: EvaluationRow) -> int | None:
    direction = infer_direction(row)
    if direction is None or not row.resolution_outcome:
        return None
    return 1 if row.resolution_outcome == direction else 0


def _liquidity_bucket(repository: Phase5Repository, row: EvaluationRow) -> str:
    decision_time = _parse_iso(row.decision_timestamp)
    if decision_time is None:
        return "unknown"
    window_end = decision_time + timedelta(minutes=30)
    series = repository.load_snapshot_series(row.market_id, _iso(decision_time), _iso(window_end))
    if not series:
        return "unknown"
    point = series[0]
    spread = _safe_float(point.spread, default=-1.0)
    if spread < 0:
        return "unknown"
    if spread <= PHASE6_LIQUIDITY_SPREAD_TIGHT:
        return "tight"
    if spread <= PHASE6_LIQUIDITY_SPREAD_WIDE:
        return "mid"
    return "wide"


def _dataset_partition(frame: pd.DataFrame) -> pd.Series:
    time_blocks = sorted(str(item) for item in frame["time_holdout"].dropna().unique())
    if not time_blocks:
        return pd.Series(["train"] * len(frame), index=frame.index)
    if len(time_blocks) == 1:
        return pd.Series(["train"] * len(frame), index=frame.index)
    if len(time_blocks) == 2:
        train_block, test_block = time_blocks
        return frame["time_holdout"].astype(str).map(
            lambda value: "test" if value == test_block else "train"
        )
    test_block = time_blocks[-1]
    validation_block = time_blocks[-2]

    def _classify(value: str) -> str:
        if value == test_block:
            return "test"
        if value == validation_block:
            return "validation"
        return "train"

    return frame["time_holdout"].astype(str).map(_classify)


def _dataset_hash(frame: pd.DataFrame) -> str:
    if frame.empty or "decision_timestamp" not in frame.columns or "candidate_id" not in frame.columns:
        return sha256(b"[]").hexdigest()
    records = frame.sort_values(["decision_timestamp", "candidate_id"]).to_dict(orient="records")
    payload = json.dumps(records, sort_keys=True, default=str)
    return sha256(payload.encode("utf-8")).hexdigest()


def build_training_frame(
    rows: list[EvaluationRow],
    *,
    repository: Phase5Repository | None = None,
    feature_schema_version: str = PHASE6_FEATURE_SCHEMA_VERSION,
) -> tuple[pd.DataFrame, Phase6DatasetBuildSummary]:
    repository = repository or Phase5Repository()
    feature_frame = build_features(rows, feature_schema_version=feature_schema_version)
    if feature_frame.empty:
        empty = feature_frame.copy()
        empty["label_success"] = pd.Series(dtype="float64")
        empty["label_available"] = pd.Series(dtype="bool")
        empty["direction"] = pd.Series(dtype="string")
        empty["liquidity_bucket"] = pd.Series(dtype="string")
        empty["event_family_holdout"] = pd.Series(dtype="string")
        empty["category_holdout"] = pd.Series(dtype="string")
        empty["time_holdout"] = pd.Series(dtype="string")
        empty["dataset_partition"] = pd.Series(dtype="string")
        empty["probability_velocity_abs"] = pd.Series(dtype="float64")
        empty["probability_acceleration_abs"] = pd.Series(dtype="float64")
        summary = Phase6DatasetBuildSummary(
            feature_schema_version=feature_schema_version,
            dataset_hash=_dataset_hash(empty),
            row_count=0,
            labeled_row_count=0,
            train_row_count=0,
            validation_row_count=0,
            test_row_count=0,
        )
        return empty, summary

    fold_map = build_event_family_fold_map(rows)
    extras: list[dict[str, Any]] = []
    for row in rows:
        label = _positive_label(row)
        extras.append(
            {
                "evaluation_row_id": row.evaluation_row_id,
                "direction": infer_direction(row) or "unknown",
                "label_success": label,
                "label_available": label is not None,
                "liquidity_bucket": _liquidity_bucket(repository, row),
                "event_family_holdout": fold_map.get(row.event_family_id, "fold_unknown"),
                "category_holdout": category_holdout_key(row),
                "time_holdout": time_holdout_key(row.decision_timestamp),
            }
        )

    extras_frame = pd.DataFrame.from_records(extras)
    frame = feature_frame.merge(extras_frame, on="evaluation_row_id", how="left")
    frame["probability_velocity_abs"] = frame["probability_velocity"].abs()
    frame["probability_acceleration_abs"] = frame["probability_acceleration"].abs()
    frame["dataset_partition"] = _dataset_partition(frame)
    frame["label_success"] = frame["label_success"].astype("float64")
    frame["label_available"] = frame["label_available"].fillna(False).astype(bool)
    frame["event_family_holdout"] = frame["event_family_holdout"].fillna("fold_unknown")
    frame["category_holdout"] = frame["category_holdout"].fillna("unknown")
    frame["time_holdout"] = frame["time_holdout"].fillna("time_block:unknown")
    frame["liquidity_bucket"] = frame["liquidity_bucket"].fillna("unknown")

    dataset_hash = _dataset_hash(frame)
    summary = Phase6DatasetBuildSummary(
        feature_schema_version=feature_schema_version,
        dataset_hash=dataset_hash,
        row_count=int(len(frame)),
        labeled_row_count=int(frame["label_available"].sum()),
        train_row_count=int((frame["dataset_partition"] == "train").sum()),
        validation_row_count=int((frame["dataset_partition"] == "validation").sum()),
        test_row_count=int((frame["dataset_partition"] == "test").sum()),
    )
    return frame, summary


def fit_linear_ranker(
    frame: pd.DataFrame,
    *,
    model_version: str,
    dataset_hash: str,
) -> tuple[dict[str, Any], Phase6ModelFitSummary]:
    train = frame[(frame["dataset_partition"] == "train") & (frame["label_available"])]
    if train.empty:
        raise ValueError("No labeled training rows available for Phase 6 Person 2 model fitting.")

    labels = train["label_success"].astype(float)
    positives = train[labels == 1.0]
    negatives = train[labels == 0.0]
    if positives.empty or negatives.empty:
        raise ValueError("Training rows must include both positive and negative labels.")

    feature_stats: dict[str, dict[str, float]] = {}
    weights: dict[str, float] = {}
    for column in NUMERIC_MODEL_FEATURES:
        series = train[column].fillna(0.0).astype(float)
        mean_value = float(series.mean())
        std_value = float(series.std()) if float(series.std()) > 1e-9 else 1.0
        pos_mean = float(positives[column].fillna(0.0).astype(float).mean())
        neg_mean = float(negatives[column].fillna(0.0).astype(float).mean())
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
        "kind": "phase6_linear_ranker_v1",
        "model_version": model_version,
        "dataset_hash": dataset_hash,
        "feature_schema_version": str(frame["feature_schema_version"].iloc[0]),
        "feature_order": NUMERIC_MODEL_FEATURES,
        "feature_stats": feature_stats,
        "weights": normalized_weights,
        "intercept": round(intercept, 8),
    }
    summary = Phase6ModelFitSummary(
        model_version=model_version,
        dataset_hash=dataset_hash,
        labeled_row_count=int(frame["label_available"].sum()),
        train_row_count=int(len(train)),
        feature_count=len(NUMERIC_MODEL_FEATURES),
        base_rate=round(base_rate, 6),
    )
    return model_spec, summary


def fit_lightgbm_ranker(
    frame: pd.DataFrame,
    *,
    model_version: str,
    dataset_hash: str,
    feature_order: list[str] | None = None,
) -> tuple[dict[str, Any], Phase6ModelFitSummary]:
    if lgb is None:
        raise ImportError("lightgbm is required for the boosted Phase 6 ranker.")

    feature_order = list(feature_order or NUMERIC_MODEL_FEATURES)
    prepared = prepare_model_input_frame(frame, feature_order=feature_order)
    train = prepared[(prepared["dataset_partition"] == "train") & (prepared["label_available"])]
    if train.empty:
        train = prepared[prepared["label_available"]]
    if train.empty:
        raise ValueError("No labeled training rows available for Phase 6 boosted model fitting.")

    labels = train["label_success"].astype(int)
    positives = int((labels == 1).sum())
    negatives = int((labels == 0).sum())
    if positives == 0 or negatives == 0:
        raise ValueError("Training rows must include both positive and negative labels.")

    feature_matrix = train[feature_order].fillna(0.0).astype(float)
    train_dataset = lgb.Dataset(feature_matrix, label=labels, feature_name=feature_order, free_raw_data=False)
    booster = lgb.train(
        params={
            "objective": "binary",
            "metric": ["binary_logloss"],
            "learning_rate": 0.15,
            "num_leaves": 4,
            "min_data_in_leaf": 1,
            "min_sum_hessian_in_leaf": 0.0,
            "feature_pre_filter": False,
            "verbosity": -1,
            "seed": 42,
            "deterministic": True,
            "force_col_wise": True,
        },
        train_set=train_dataset,
        num_boost_round=max(8, min(32, len(train.index) * 8)),
    )

    base_rate = float(labels.mean())
    model_spec = {
        "kind": "phase6_lightgbm_ranker_v1",
        "model_version": model_version,
        "dataset_hash": dataset_hash,
        "feature_schema_version": str(prepared["feature_schema_version"].iloc[0]),
        "feature_order": feature_order,
        "library": "lightgbm",
        "library_version": getattr(lgb, "__version__", "unknown"),
        "booster_model_str": booster.model_to_string(num_iteration=booster.current_iteration()),
        "feature_importance_gain": {
            name: round(float(value), 8)
            for name, value in zip(feature_order, booster.feature_importance(importance_type="gain"))
        },
        "training_summary": {
            "positive_rows": positives,
            "negative_rows": negatives,
            "num_iterations": int(booster.current_iteration() or 0),
        },
    }
    summary = Phase6ModelFitSummary(
        model_version=model_version,
        dataset_hash=dataset_hash,
        labeled_row_count=int(prepared["label_available"].sum()),
        train_row_count=int(len(train)),
        feature_count=len(feature_order),
        base_rate=round(base_rate, 6),
    )
    return model_spec, summary


def score_training_frame(frame: pd.DataFrame, *, model_spec: dict[str, Any]) -> pd.DataFrame:
    feature_order = [str(item) for item in model_spec.get("feature_order", [])]
    scored = prepare_model_input_frame(frame, feature_order=feature_order)
    kind = str(model_spec.get("kind") or "phase6_linear_ranker_v1")

    if kind == "phase6_lightgbm_ranker_v1":
        if lgb is None:
            raise ImportError("lightgbm is required for scoring the boosted Phase 6 ranker.")
        booster = lgb.Booster(model_str=str(model_spec.get("booster_model_str") or ""))
        feature_matrix = scored[feature_order].fillna(0.0).astype(float)
        scored["model_linear_score"] = pd.Series([None] * len(scored.index), index=scored.index, dtype="object")
        scored["model_score"] = booster.predict(feature_matrix)
    else:
        weights = model_spec.get("weights", {})
        feature_stats = model_spec.get("feature_stats", {})
        linear = pd.Series(float(model_spec.get("intercept", 0.0)), index=scored.index, dtype="float64")
        for column in feature_order:
            stats = feature_stats.get(column, {})
            mean_value = _safe_float(stats.get("mean"), 0.0)
            std_value = max(1e-9, _safe_float(stats.get("std"), 1.0))
            normalized = (scored[column].fillna(0.0).astype(float) - mean_value) / std_value
            linear = linear + (normalized * _safe_float(weights.get(column), 0.0))
        scored["model_linear_score"] = linear
        scored["model_score"] = linear.map(
            lambda value: 1.0 / (1.0 + math.exp(max(-50.0, min(50.0, -float(value)))))
        )

    scored["model_score"] = scored["model_score"].fillna(0.0).astype(float)
    return _append_required_baseline_scores(scored)
