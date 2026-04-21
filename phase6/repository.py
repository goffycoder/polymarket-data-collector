from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Any
from uuid import uuid4

from config.settings import PHASE6_MODEL_REGISTRY_VERSION
from database.db_manager import get_conn


def _iso_now() -> str:
    return datetime.now(timezone.utc).isoformat()


@dataclass(slots=True)
class Phase6FeatureMaterializationSummary:
    materialization_run_id: str
    feature_schema_version: str
    materialization_mode: str
    start: str
    end: str
    source_row_count: int
    feature_row_count: int
    dataset_hash: str
    output_path: str | None
    status: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class Phase6ModelRegistrySummary:
    model_version: str
    model_name: str
    artifact_path: str
    feature_schema_version: str
    training_dataset_hash: str
    deployment_status: str
    shadow_enabled: bool

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class Phase6ShadowScoreSummary:
    shadow_score_id: str
    model_version: str
    candidate_id: str
    market_id: str
    score_value: float
    score_label: str | None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


class Phase6Repository:
    def record_materialization_run(
        self,
        *,
        feature_schema_version: str,
        materialization_mode: str,
        start: str,
        end: str,
        source_row_count: int,
        feature_row_count: int,
        dataset_hash: str,
        output_path: str | None,
        status: str,
        notes: str | None = None,
    ) -> Phase6FeatureMaterializationSummary:
        materialization_run_id = uuid4().hex
        conn = get_conn()
        try:
            conn.execute(
                """
                INSERT INTO feature_materialization_runs (
                    materialization_run_id,
                    feature_schema_version,
                    materialization_mode,
                    start_time,
                    end_time,
                    source_row_count,
                    feature_row_count,
                    dataset_hash,
                    output_path,
                    status,
                    notes,
                    completed_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    materialization_run_id,
                    feature_schema_version,
                    materialization_mode,
                    start,
                    end,
                    source_row_count,
                    feature_row_count,
                    dataset_hash,
                    output_path,
                    status,
                    notes,
                    _iso_now(),
                ),
            )
            conn.commit()
        finally:
            conn.close()
        return Phase6FeatureMaterializationSummary(
            materialization_run_id=materialization_run_id,
            feature_schema_version=feature_schema_version,
            materialization_mode=materialization_mode,
            start=start,
            end=end,
            source_row_count=source_row_count,
            feature_row_count=feature_row_count,
            dataset_hash=dataset_hash,
            output_path=output_path,
            status=status,
        )

    def register_model(
        self,
        *,
        model_name: str,
        model_version: str,
        artifact_path: str,
        feature_schema_version: str,
        training_dataset_hash: str,
        calibration_metadata: dict[str, Any] | None,
        deployment_status: str,
        shadow_enabled: bool,
        notes: str | None = None,
    ) -> Phase6ModelRegistrySummary:
        conn = get_conn()
        try:
            conn.execute(
                """
                INSERT INTO model_registry (
                    model_version,
                    model_name,
                    registry_version,
                    artifact_path,
                    feature_schema_version,
                    training_dataset_hash,
                    calibration_metadata,
                    deployment_status,
                    shadow_enabled,
                    deployed_at,
                    notes
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(model_version) DO UPDATE SET
                    model_name = excluded.model_name,
                    registry_version = excluded.registry_version,
                    artifact_path = excluded.artifact_path,
                    feature_schema_version = excluded.feature_schema_version,
                    training_dataset_hash = excluded.training_dataset_hash,
                    calibration_metadata = excluded.calibration_metadata,
                    deployment_status = excluded.deployment_status,
                    shadow_enabled = excluded.shadow_enabled,
                    deployed_at = excluded.deployed_at,
                    notes = excluded.notes
                """,
                (
                    model_version,
                    model_name,
                    PHASE6_MODEL_REGISTRY_VERSION,
                    artifact_path,
                    feature_schema_version,
                    training_dataset_hash,
                    json.dumps(calibration_metadata or {}, sort_keys=True),
                    deployment_status,
                    1 if shadow_enabled else 0,
                    _iso_now() if deployment_status in {"shadow", "deployed"} else None,
                    notes,
                ),
            )
            conn.commit()
        finally:
            conn.close()
        return Phase6ModelRegistrySummary(
            model_version=model_version,
            model_name=model_name,
            artifact_path=artifact_path,
            feature_schema_version=feature_schema_version,
            training_dataset_hash=training_dataset_hash,
            deployment_status=deployment_status,
            shadow_enabled=shadow_enabled,
        )

    def log_shadow_score(
        self,
        *,
        model_version: str,
        feature_schema_version: str,
        candidate_id: str,
        alert_id: str | None,
        market_id: str,
        score_value: float,
        score_label: str | None,
        score_metadata: dict[str, Any] | None,
        scored_at: str,
    ) -> Phase6ShadowScoreSummary:
        shadow_score_id = uuid4().hex
        conn = get_conn()
        try:
            conn.execute(
                """
                INSERT INTO shadow_model_scores (
                    shadow_score_id,
                    model_version,
                    feature_schema_version,
                    candidate_id,
                    alert_id,
                    market_id,
                    score_value,
                    score_label,
                    score_metadata,
                    scored_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    shadow_score_id,
                    model_version,
                    feature_schema_version,
                    candidate_id,
                    alert_id,
                    market_id,
                    score_value,
                    score_label,
                    json.dumps(score_metadata or {}, sort_keys=True),
                    scored_at,
                ),
            )
            conn.commit()
        finally:
            conn.close()
        return Phase6ShadowScoreSummary(
            shadow_score_id=shadow_score_id,
            model_version=model_version,
            candidate_id=candidate_id,
            market_id=market_id,
            score_value=score_value,
            score_label=score_label,
        )
