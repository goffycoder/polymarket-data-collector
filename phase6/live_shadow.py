from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from config.settings import PHASE6_FEATURE_SCHEMA_VERSION
from ml_pipeline.feature_builder import build_features
from phase5.repository import Phase5Repository
from phase6 import Phase6Repository, build_shadow_scores, load_model_spec


@dataclass(slots=True)
class Phase6LiveShadowSummary:
    model_version: str
    feature_schema_version: str
    window_start: str
    window_end: str
    candidate_count: int
    score_count: int
    output_path: str | None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _iso(value: datetime) -> str:
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).replace(microsecond=0).isoformat()


def run_live_shadow_window(
    *,
    lookback_minutes: int,
    model_version: str | None = None,
    output_dir: str = "reports/phase6/live_shadow",
) -> Phase6LiveShadowSummary:
    repo = Phase6Repository()
    model_entry = (
        repo.load_model_registry_entry(model_version=model_version)
        if model_version
        else repo.load_active_shadow_model()
    )
    if model_entry is None:
        raise ValueError("No registered shadow model found.")

    window_end_dt = _utc_now()
    window_start_dt = window_end_dt - timedelta(minutes=max(1, lookback_minutes))
    start = _iso(window_start_dt)
    end = _iso(window_end_dt)

    rows = Phase5Repository().load_evaluation_rows(start=start, end=end)
    frame = build_features(rows, feature_schema_version=PHASE6_FEATURE_SCHEMA_VERSION)
    model_spec = load_model_spec(str(model_entry["artifact_path"]))
    score_rows = build_shadow_scores(frame, model_spec=model_spec)

    artifact_dir = Path(output_dir)
    artifact_dir.mkdir(parents=True, exist_ok=True)
    artifact_path = artifact_dir / f"{model_entry['model_version']}_{start}_{end}".replace(":", "-")
    artifact_path = artifact_path.with_suffix(".json")
    artifact_payload = {
        "model_version": model_entry["model_version"],
        "feature_schema_version": PHASE6_FEATURE_SCHEMA_VERSION,
        "window": {"start": start, "end": end},
        "score_rows": score_rows,
    }
    artifact_path.write_text(json.dumps(artifact_payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    for row in score_rows:
        repo.log_shadow_score(
            model_version=str(model_entry["model_version"]),
            feature_schema_version=PHASE6_FEATURE_SCHEMA_VERSION,
            candidate_id=str(row["candidate_id"]),
            alert_id=row.get("alert_id"),
            market_id=str(row["market_id"]),
            score_value=float(row["score_value"]),
            score_label=row.get("score_label"),
            score_metadata=row.get("score_metadata"),
            scored_at=str(row["decision_timestamp"]),
        )

    return Phase6LiveShadowSummary(
        model_version=str(model_entry["model_version"]),
        feature_schema_version=PHASE6_FEATURE_SCHEMA_VERSION,
        window_start=start,
        window_end=end,
        candidate_count=len(rows),
        score_count=len(score_rows),
        output_path=str(artifact_path),
    )
