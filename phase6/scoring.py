from __future__ import annotations

import json
import math
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

import pandas as pd
try:
    import lightgbm as lgb
except ImportError:  # pragma: no cover - dependency is checked by callers.
    lgb = None

from phase6.training import prepare_model_input_frame


DEFAULT_LINEAR_WEIGHTS = {
    "candidate_severity_score": 0.45,
    "fresh_wallet_count": 0.10,
    "fresh_wallet_notional_share": 0.15,
    "directional_imbalance": 0.10,
    "concentration_ratio": 0.10,
    "probability_velocity": 0.05,
    "probability_acceleration": 0.025,
    "volume_acceleration": 0.025,
}


@dataclass(slots=True)
class Phase6ShadowRunSummary:
    model_version: str
    feature_schema_version: str
    score_count: int
    new_score_count: int
    output_path: str | None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def _clamp(value: float, lower: float = 0.0, upper: float = 1.0) -> float:
    return max(lower, min(upper, value))


def _sigmoid(value: float) -> float:
    return 1.0 / (1.0 + math.exp(-value))


def load_model_spec(artifact_path: str) -> dict[str, Any]:
    path = Path(artifact_path)
    if not path.is_absolute():
        path = Path.cwd() / path
    raw = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise ValueError("Model artifact must be a JSON object.")
    return raw


def _score_label(score_value: float, thresholds: dict[str, float]) -> str:
    if score_value >= float(thresholds.get("critical", 0.9)):
        return "CRITICAL"
    if score_value >= float(thresholds.get("actionable", 0.75)):
        return "ACTIONABLE"
    if score_value >= float(thresholds.get("watch", 0.5)):
        return "WATCH"
    return "INFO"


def build_shadow_scores(
    frame: pd.DataFrame,
    *,
    model_spec: dict[str, Any],
) -> list[dict[str, Any]]:
    kind = str(model_spec.get("kind") or "phase6_linear_ranker_v1")
    feature_order = [str(item) for item in model_spec.get("feature_order", [])]
    prepared = prepare_model_input_frame(frame, feature_order=feature_order)
    thresholds = model_spec.get("thresholds")
    if not isinstance(thresholds, dict):
        thresholds = {"watch": 0.5, "actionable": 0.75, "critical": 0.9}

    score_values: list[float] = []
    if kind == "phase6_lightgbm_ranker_v1":
        if lgb is None:
            raise ImportError("lightgbm is required for scoring the boosted Phase 6 model.")
        booster = lgb.Booster(model_str=str(model_spec.get("booster_model_str") or ""))
        feature_matrix = prepared[feature_order].fillna(0.0).astype(float)
        score_values = [round(float(value), 6) for value in booster.predict(feature_matrix)]
        records = prepared.to_dict(orient="records")
    else:
        weights = model_spec.get("weights")
        if not isinstance(weights, dict) or not weights:
            weights = DEFAULT_LINEAR_WEIGHTS
        records = prepared.to_dict(orient="records")
        for record in records:
            linear_value = 0.0
            for feature_name, weight in weights.items():
                feature_value = float(record.get(feature_name) or 0.0)
                linear_value += float(weight) * feature_value
            score_values.append(round(_clamp(_sigmoid(linear_value)), 6))

    score_rows: list[dict[str, Any]] = []
    for record, score_value in zip(records, score_values):
        score_rows.append(
            {
                "candidate_id": record["candidate_id"],
                "alert_id": record.get("alert_id"),
                "market_id": record["market_id"],
                "event_id": record.get("event_id"),
                "event_family_id": record.get("event_family_id"),
                "decision_timestamp": record["decision_timestamp"],
                "score_value": score_value,
                "score_label": _score_label(score_value, thresholds),
                "score_metadata": {
                    "model_kind": kind,
                    "feature_order": feature_order,
                    "thresholds": thresholds,
                },
            }
        )
    return score_rows
