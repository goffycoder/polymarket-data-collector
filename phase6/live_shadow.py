from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from config.settings import PHASE6_FEATURE_SCHEMA_VERSION
from database.db_manager import get_conn
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


def _candidate_ids_for_live_shadow_window(*, start: str, end: str) -> tuple[set[str], str]:
    conn = get_conn()
    try:
        rows = conn.execute(
            """
            SELECT DISTINCT
                sc.candidate_id,
                sc.trigger_time
            FROM signal_candidates sc
            LEFT JOIN alerts a ON a.candidate_id = sc.candidate_id
            WHERE (sc.trigger_time >= ? AND sc.trigger_time < ?)
               OR (a.updated_at >= ? AND a.updated_at < ?)
            """,
            (start, end, start, end),
        ).fetchall()
    finally:
        conn.close()

    candidate_ids = {
        str(row["candidate_id"])
        for row in rows
        if row["candidate_id"] is not None
    }
    trigger_times = [
        str(row["trigger_time"])
        for row in rows
        if row["trigger_time"] is not None
    ]
    return candidate_ids, min(trigger_times) if trigger_times else start


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

    candidate_ids, query_start = _candidate_ids_for_live_shadow_window(start=start, end=end)
    rows = Phase5Repository().load_evaluation_rows(start=query_start, end=end)
    if candidate_ids:
        rows = [row for row in rows if row.candidate_id in candidate_ids]
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
        "candidate_selection": {
            "source": "candidate_trigger_time_or_alert_updated_at",
            "candidate_count": len(candidate_ids),
            "query_start": query_start,
        },
        "score_rows": score_rows,
    }
    artifact_path.write_text(
        json.dumps(artifact_payload, allow_nan=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )

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
