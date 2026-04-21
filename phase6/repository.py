from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Any
from uuid import uuid4

from config.settings import PHASE6_CALIBRATION_VERSION, PHASE6_EVALUATION_VERSION, PHASE6_MODEL_REGISTRY_VERSION
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


@dataclass(slots=True)
class Phase6EvaluationRunSummary:
    evaluation_run_id: str
    model_version: str
    evaluation_version: str
    feature_schema_version: str
    dataset_hash: str
    train_row_count: int
    validation_row_count: int
    test_row_count: int
    labeled_row_count: int
    output_path: str | None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class Phase6CalibrationProfileSummary:
    calibration_profile_id: str
    model_version: str
    profile_scope: str
    profile_key: str
    sample_count: int
    positive_rate: float | None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


class Phase6Repository:
    def list_recent_evaluation_runs(self, *, limit: int = 20) -> list[dict[str, Any]]:
        conn = get_conn()
        try:
            rows = conn.execute(
                """
                SELECT
                    evaluation_run_id,
                    model_version,
                    evaluation_version,
                    feature_schema_version,
                    dataset_hash,
                    start_time,
                    end_time,
                    train_row_count,
                    validation_row_count,
                    test_row_count,
                    labeled_row_count,
                    output_path,
                    summary_json,
                    created_at
                FROM model_evaluation_runs
                ORDER BY created_at DESC, evaluation_run_id DESC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
        finally:
            conn.close()
        return [
            {
                "evaluation_run_id": row["evaluation_run_id"],
                "model_version": row["model_version"],
                "evaluation_version": row["evaluation_version"],
                "feature_schema_version": row["feature_schema_version"],
                "dataset_hash": row["dataset_hash"],
                "start_time": row["start_time"],
                "end_time": row["end_time"],
                "train_row_count": int(row["train_row_count"] or 0),
                "validation_row_count": int(row["validation_row_count"] or 0),
                "test_row_count": int(row["test_row_count"] or 0),
                "labeled_row_count": int(row["labeled_row_count"] or 0),
                "output_path": row["output_path"],
                "summary_json": json.loads(row["summary_json"] or "{}"),
                "created_at": row["created_at"],
            }
            for row in rows
        ]

    def list_calibration_profiles(
        self,
        *,
        model_version: str | None = None,
        limit: int = 200,
    ) -> list[dict[str, Any]]:
        conn = get_conn()
        try:
            if model_version:
                rows = conn.execute(
                    """
                    SELECT
                        calibration_profile_id,
                        model_version,
                        calibration_version,
                        profile_scope,
                        profile_key,
                        sample_count,
                        positive_rate,
                        watch_threshold,
                        actionable_threshold,
                        critical_threshold,
                        metadata_json,
                        created_at
                    FROM calibration_profiles
                    WHERE model_version = ?
                    ORDER BY created_at DESC, calibration_profile_id DESC
                    LIMIT ?
                    """,
                    (model_version, limit),
                ).fetchall()
            else:
                rows = conn.execute(
                    """
                    SELECT
                        calibration_profile_id,
                        model_version,
                        calibration_version,
                        profile_scope,
                        profile_key,
                        sample_count,
                        positive_rate,
                        watch_threshold,
                        actionable_threshold,
                        critical_threshold,
                        metadata_json,
                        created_at
                    FROM calibration_profiles
                    ORDER BY created_at DESC, calibration_profile_id DESC
                    LIMIT ?
                    """,
                    (limit,),
                ).fetchall()
        finally:
            conn.close()
        return [
            {
                "calibration_profile_id": row["calibration_profile_id"],
                "model_version": row["model_version"],
                "calibration_version": row["calibration_version"],
                "profile_scope": row["profile_scope"],
                "profile_key": row["profile_key"],
                "sample_count": int(row["sample_count"] or 0),
                "positive_rate": row["positive_rate"],
                "watch_threshold": row["watch_threshold"],
                "actionable_threshold": row["actionable_threshold"],
                "critical_threshold": row["critical_threshold"],
                "metadata_json": json.loads(row["metadata_json"] or "{}"),
                "created_at": row["created_at"],
            }
            for row in rows
        ]

    def load_model_registry_entry(self, *, model_version: str) -> dict[str, Any] | None:
        conn = get_conn()
        try:
            row = conn.execute(
                """
                SELECT
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
                    created_at,
                    notes
                FROM model_registry
                WHERE model_version = ?
                """,
                (model_version,),
            ).fetchone()
        finally:
            conn.close()
        if row is None:
            return None
        return {
            "model_version": row["model_version"],
            "model_name": row["model_name"],
            "registry_version": row["registry_version"],
            "artifact_path": row["artifact_path"],
            "feature_schema_version": row["feature_schema_version"],
            "training_dataset_hash": row["training_dataset_hash"],
            "calibration_metadata": json.loads(row["calibration_metadata"] or "{}"),
            "deployment_status": row["deployment_status"],
            "shadow_enabled": bool(row["shadow_enabled"]),
            "deployed_at": row["deployed_at"],
            "created_at": row["created_at"],
            "notes": row["notes"],
        }

    def load_active_shadow_model(self) -> dict[str, Any] | None:
        conn = get_conn()
        try:
            row = conn.execute(
                """
                SELECT
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
                    created_at,
                    notes
                FROM model_registry
                WHERE shadow_enabled = 1
                  AND deployment_status IN ('shadow', 'deployed')
                ORDER BY
                    CASE deployment_status WHEN 'deployed' THEN 0 ELSE 1 END,
                    COALESCE(deployed_at, created_at) DESC
                LIMIT 1
                """
            ).fetchone()
        finally:
            conn.close()
        if row is None:
            return None
        return {
            "model_version": row["model_version"],
            "model_name": row["model_name"],
            "registry_version": row["registry_version"],
            "artifact_path": row["artifact_path"],
            "feature_schema_version": row["feature_schema_version"],
            "training_dataset_hash": row["training_dataset_hash"],
            "calibration_metadata": json.loads(row["calibration_metadata"] or "{}"),
            "deployment_status": row["deployment_status"],
            "shadow_enabled": bool(row["shadow_enabled"]),
            "deployed_at": row["deployed_at"],
            "created_at": row["created_at"],
            "notes": row["notes"],
        }

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

    def record_evaluation_run(
        self,
        *,
        model_version: str,
        feature_schema_version: str,
        dataset_hash: str,
        start: str,
        end: str,
        train_row_count: int,
        validation_row_count: int,
        test_row_count: int,
        labeled_row_count: int,
        output_path: str | None,
        summary_json: dict[str, Any],
    ) -> Phase6EvaluationRunSummary:
        evaluation_run_id = uuid4().hex
        conn = get_conn()
        try:
            conn.execute(
                """
                INSERT INTO model_evaluation_runs (
                    evaluation_run_id,
                    model_version,
                    evaluation_version,
                    feature_schema_version,
                    dataset_hash,
                    start_time,
                    end_time,
                    train_row_count,
                    validation_row_count,
                    test_row_count,
                    labeled_row_count,
                    output_path,
                    summary_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    evaluation_run_id,
                    model_version,
                    PHASE6_EVALUATION_VERSION,
                    feature_schema_version,
                    dataset_hash,
                    start,
                    end,
                    train_row_count,
                    validation_row_count,
                    test_row_count,
                    labeled_row_count,
                    output_path,
                    json.dumps(summary_json, sort_keys=True),
                ),
            )
            conn.commit()
        finally:
            conn.close()
        return Phase6EvaluationRunSummary(
            evaluation_run_id=evaluation_run_id,
            model_version=model_version,
            evaluation_version=PHASE6_EVALUATION_VERSION,
            feature_schema_version=feature_schema_version,
            dataset_hash=dataset_hash,
            train_row_count=train_row_count,
            validation_row_count=validation_row_count,
            test_row_count=test_row_count,
            labeled_row_count=labeled_row_count,
            output_path=output_path,
        )

    def record_calibration_profile(
        self,
        *,
        model_version: str,
        profile_scope: str,
        profile_key: str,
        sample_count: int,
        positive_rate: float | None,
        watch_threshold: float | None,
        actionable_threshold: float | None,
        critical_threshold: float | None,
        metadata_json: dict[str, Any] | None,
    ) -> Phase6CalibrationProfileSummary:
        calibration_profile_id = uuid4().hex
        conn = get_conn()
        try:
            conn.execute(
                """
                INSERT INTO calibration_profiles (
                    calibration_profile_id,
                    model_version,
                    calibration_version,
                    profile_scope,
                    profile_key,
                    sample_count,
                    positive_rate,
                    watch_threshold,
                    actionable_threshold,
                    critical_threshold,
                    metadata_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    calibration_profile_id,
                    model_version,
                    PHASE6_CALIBRATION_VERSION,
                    profile_scope,
                    profile_key,
                    sample_count,
                    positive_rate,
                    watch_threshold,
                    actionable_threshold,
                    critical_threshold,
                    json.dumps(metadata_json or {}, sort_keys=True),
                ),
            )
            conn.commit()
        finally:
            conn.close()
        return Phase6CalibrationProfileSummary(
            calibration_profile_id=calibration_profile_id,
            model_version=model_version,
            profile_scope=profile_scope,
            profile_key=profile_key,
            sample_count=sample_count,
            positive_rate=positive_rate,
        )
