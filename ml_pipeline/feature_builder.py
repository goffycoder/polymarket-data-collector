from __future__ import annotations

import hashlib
import json
from typing import Any

import pandas as pd

from phase5.models import EvaluationRow


def _parse_feature_snapshot(snapshot: dict[str, Any] | None) -> dict[str, Any]:
    if isinstance(snapshot, dict):
        return snapshot
    return {}


def _severity_rank(value: str | None) -> int:
    mapping = {
        "INFO": 1,
        "WATCH": 2,
        "ACTIONABLE": 3,
        "CRITICAL": 4,
    }
    return mapping.get(str(value or "").upper(), 0)


def evaluation_row_to_feature_row(
    row: EvaluationRow,
    *,
    feature_schema_version: str,
) -> dict[str, Any]:
    snapshot = _parse_feature_snapshot(row.feature_snapshot)
    evidence_state = str(row.evidence_state_at_alert or "pending_evidence")
    return {
        "evaluation_row_id": row.evaluation_row_id,
        "candidate_id": row.candidate_id,
        "alert_id": row.alert_id,
        "market_id": row.market_id,
        "event_id": row.event_id,
        "event_family_id": row.event_family_id,
        "category_key": row.category_key,
        "decision_timestamp": row.decision_timestamp,
        "feature_schema_version": feature_schema_version,
        "candidate_severity_score": float(row.candidate_severity_score or 0.0),
        "alert_severity_rank": _severity_rank(row.alert_severity),
        "has_alert": 1 if row.alert_id else 0,
        "has_resolution": 1 if row.resolution_outcome else 0,
        "rule_count": len(row.triggering_rules),
        "fresh_wallet_count": int(snapshot.get("fresh_wallet_count") or 0),
        "fresh_wallet_notional_share": float(snapshot.get("fresh_wallet_notional_share") or 0.0),
        "directional_imbalance": float(snapshot.get("directional_imbalance") or 0.0),
        "concentration_ratio": float(snapshot.get("concentration_ratio") or 0.0),
        "probability_velocity": float(snapshot.get("probability_velocity") or 0.0),
        "probability_acceleration": float(snapshot.get("probability_acceleration") or 0.0),
        "volume_acceleration": float(snapshot.get("volume_acceleration") or 0.0),
        "market_status_closed": 1 if str(row.market_status or "").lower() == "closed" else 0,
        "evidence_state": evidence_state,
        "coverage_status": row.coverage_status,
    }


def build_features(evaluation_rows: list[EvaluationRow], *, feature_schema_version: str) -> pd.DataFrame:
    rows = [
        evaluation_row_to_feature_row(item, feature_schema_version=feature_schema_version)
        for item in evaluation_rows
    ]
    return pd.DataFrame(rows)


def dataset_hash_from_frame(frame: pd.DataFrame) -> str:
    if frame.empty:
        return hashlib.sha256(b"[]").hexdigest()
    records = frame.sort_values(by=["decision_timestamp", "candidate_id"]).to_dict(orient="records")
    encoded = json.dumps(records, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()
