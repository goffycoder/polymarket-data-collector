from __future__ import annotations

import json
import math
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

import pandas as pd


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
    weights = model_spec.get("weights")
    if not isinstance(weights, dict) or not weights:
        weights = DEFAULT_LINEAR_WEIGHTS
    thresholds = model_spec.get("thresholds")
    if not isinstance(thresholds, dict):
        thresholds = {"watch": 0.5, "actionable": 0.75, "critical": 0.9}

    score_rows: list[dict[str, Any]] = []
    for record in frame.to_dict(orient="records"):
        linear_value = 0.0
        contributions: dict[str, float] = {}
        for feature_name, weight in weights.items():
            feature_value = float(record.get(feature_name) or 0.0)
            contrib = float(weight) * feature_value
            linear_value += contrib
            contributions[feature_name] = round(contrib, 8)
        score_value = round(_clamp(_sigmoid(linear_value)), 6)
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
                    "model_kind": str(model_spec.get("kind") or "linear_ranker"),
                    "weights": weights,
                    "thresholds": thresholds,
                    "linear_value": round(linear_value, 8),
                    "contributions": contributions,
                },
            }
        )
    return score_rows
