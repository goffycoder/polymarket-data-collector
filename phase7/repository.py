from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Any
from uuid import uuid4

from config.settings import (
    PHASE6_FEATURE_SCHEMA_VERSION,
    PHASE7_CONFIG_VERSION,
    PHASE7_DATASET_INDEX_VERSION,
    PHASE7_DEFAULT_DATASET_ROLE,
    PHASE7_DEFAULT_EXPERIMENT_FAMILY,
    PHASE7_EXPERIMENT_LEDGER_VERSION,
    PHASE7_LABEL_SCHEMA_VERSION,
    PHASE7_SCOPE_INDEX_VERSION,
)
from database.db_manager import get_conn


def _iso_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _stable_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


def _json_hash(value: Any) -> str:
    return hashlib.sha256(_stable_json(value).encode("utf-8")).hexdigest()


def _normalize_string_list(values: list[str] | tuple[str, ...] | None) -> list[str]:
    seen: set[str] = set()
    items: list[str] = []
    for raw in values or ():
        value = str(raw).strip()
        if not value or value in seen:
            continue
        seen.add(value)
        items.append(value)
    return items


def _normalize_scope(scope: dict[str, Any]) -> dict[str, Any]:
    scope_type = str(scope.get("scope_type") or "").strip()
    scope_key = str(scope.get("scope_key") or "").strip()
    if not scope_type or not scope_key:
        raise ValueError("Each research scope requires non-empty scope_type and scope_key.")
    return {
        "scope_type": scope_type,
        "scope_key": scope_key,
        "scope_label": str(scope.get("scope_label") or scope_key).strip(),
        "scope_definition": scope.get("scope_definition") or {},
    }


def _normalize_window(window: dict[str, Any]) -> dict[str, Any]:
    window_key = str(window.get("window_key") or "").strip()
    start_time = str(window.get("start_time") or "").strip()
    end_time = str(window.get("end_time") or "").strip()
    evaluation_scope = str(window.get("evaluation_scope") or "").strip()
    if not window_key or not start_time or not end_time or not evaluation_scope:
        raise ValueError(
            "Each research window requires non-empty window_key, start_time, end_time, and evaluation_scope."
        )
    return {
        "window_key": window_key,
        "window_label": str(window.get("window_label") or window_key).strip(),
        "start_time": start_time,
        "end_time": end_time,
        "category": (str(window.get("category")).strip() if window.get("category") is not None else None),
        "evaluation_scope": evaluation_scope,
        "split_key": (str(window.get("split_key")).strip() if window.get("split_key") is not None else None),
        "window_metadata": window.get("window_metadata") or {},
        "notes": (str(window.get("notes")).strip() if window.get("notes") is not None else None),
    }


@dataclass(slots=True)
class Phase7ResearchDatasetSummary:
    dataset_key: str
    dataset_role: str
    dataset_hash: str
    manifest_hash: str
    feature_schema_version: str
    label_schema_version: str
    restore_guarantee_status: str
    baseline_model_versions: list[str]
    window_count: int
    scope_count: int

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class Phase7ExperimentRunSummary:
    experiment_run_id: str
    experiment_name: str
    experiment_family: str
    experiment_version: str
    dataset_key: str
    dataset_hash: str
    model_version: str
    config_hash: str
    code_version: str
    status: str
    output_path: str | None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class Phase7ResearchStatusSummary:
    latest_dataset: dict[str, Any] | None
    recent_datasets: list[dict[str, Any]]
    latest_dataset_windows: list[dict[str, Any]]
    latest_dataset_scopes: list[dict[str, Any]]
    recent_experiments: list[dict[str, Any]]
    traceable_experiment_count: int

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


class Phase7Repository:
    def register_research_setup(
        self,
        *,
        dataset_key: str,
        dataset_hash: str,
        windows: list[dict[str, Any]],
        scopes: list[dict[str, Any]],
        dataset_role: str = PHASE7_DEFAULT_DATASET_ROLE,
        feature_schema_version: str = PHASE6_FEATURE_SCHEMA_VERSION,
        label_schema_version: str = PHASE7_LABEL_SCHEMA_VERSION,
        dataset_path: str | None = None,
        dataset_created_at: str | None = None,
        handoff_source: str | None = None,
        handoff_artifact_path: str | None = None,
        restore_guarantee: dict[str, Any] | None = None,
        baseline_model_versions: list[str] | None = None,
        notes: str | None = None,
    ) -> Phase7ResearchDatasetSummary:
        dataset_key = dataset_key.strip()
        dataset_hash = dataset_hash.strip()
        if not dataset_key or not dataset_hash:
            raise ValueError("dataset_key and dataset_hash are required.")

        normalized_windows = [_normalize_window(window) for window in windows]
        normalized_scopes = [_normalize_scope(scope) for scope in scopes]
        if not normalized_windows:
            raise ValueError("At least one research window is required.")
        if not normalized_scopes:
            raise ValueError("At least one research scope is required.")

        restore_payload = restore_guarantee or {}
        restore_status = str(restore_payload.get("status") or "unverified").strip()
        baseline_versions = _normalize_string_list(baseline_model_versions)

        manifest_payload = {
            "dataset_key": dataset_key,
            "dataset_role": dataset_role,
            "dataset_hash": dataset_hash,
            "feature_schema_version": feature_schema_version,
            "label_schema_version": label_schema_version,
            "dataset_path": dataset_path,
            "dataset_created_at": dataset_created_at,
            "handoff_source": handoff_source,
            "handoff_artifact_path": handoff_artifact_path,
            "restore_guarantee": restore_payload,
            "baseline_model_versions": baseline_versions,
            "windows": normalized_windows,
            "scopes": normalized_scopes,
        }
        manifest_hash = _json_hash(manifest_payload)

        conn = get_conn()
        try:
            conn.execute(
                """
                INSERT INTO phase7_research_datasets (
                    dataset_key,
                    dataset_role,
                    dataset_hash,
                    manifest_hash,
                    dataset_index_version,
                    feature_schema_version,
                    label_schema_version,
                    dataset_path,
                    dataset_created_at,
                    handoff_source,
                    handoff_artifact_path,
                    restore_guarantee_status,
                    restore_guarantee_json,
                    baseline_model_versions,
                    notes,
                    updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(dataset_key) DO UPDATE SET
                    dataset_role = excluded.dataset_role,
                    dataset_hash = excluded.dataset_hash,
                    manifest_hash = excluded.manifest_hash,
                    dataset_index_version = excluded.dataset_index_version,
                    feature_schema_version = excluded.feature_schema_version,
                    label_schema_version = excluded.label_schema_version,
                    dataset_path = excluded.dataset_path,
                    dataset_created_at = excluded.dataset_created_at,
                    handoff_source = excluded.handoff_source,
                    handoff_artifact_path = excluded.handoff_artifact_path,
                    restore_guarantee_status = excluded.restore_guarantee_status,
                    restore_guarantee_json = excluded.restore_guarantee_json,
                    baseline_model_versions = excluded.baseline_model_versions,
                    notes = excluded.notes,
                    updated_at = excluded.updated_at
                """,
                (
                    dataset_key,
                    dataset_role,
                    dataset_hash,
                    manifest_hash,
                    PHASE7_DATASET_INDEX_VERSION,
                    feature_schema_version,
                    label_schema_version,
                    dataset_path,
                    dataset_created_at,
                    handoff_source,
                    handoff_artifact_path,
                    restore_status,
                    _stable_json(restore_payload),
                    _stable_json(baseline_versions),
                    notes,
                    _iso_now(),
                ),
            )
            conn.execute(
                "DELETE FROM phase7_research_windows WHERE dataset_key = ?",
                (dataset_key,),
            )
            conn.execute(
                "DELETE FROM phase7_research_scopes WHERE dataset_key = ?",
                (dataset_key,),
            )
            conn.executemany(
                """
                INSERT INTO phase7_research_windows (
                    window_key,
                    dataset_key,
                    window_label,
                    start_time,
                    end_time,
                    category,
                    evaluation_scope,
                    split_key,
                    window_metadata_json,
                    notes
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        window["window_key"],
                        dataset_key,
                        window["window_label"],
                        window["start_time"],
                        window["end_time"],
                        window["category"],
                        window["evaluation_scope"],
                        window["split_key"],
                        _stable_json(window["window_metadata"]),
                        window["notes"],
                    )
                    for window in normalized_windows
                ],
            )
            conn.executemany(
                """
                INSERT INTO phase7_research_scopes (
                    dataset_key,
                    scope_type,
                    scope_key,
                    scope_label,
                    scope_definition_json,
                    scope_index_version
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        dataset_key,
                        scope["scope_type"],
                        scope["scope_key"],
                        scope["scope_label"],
                        _stable_json(scope["scope_definition"]),
                        PHASE7_SCOPE_INDEX_VERSION,
                    )
                    for scope in normalized_scopes
                ],
            )
            conn.commit()
        finally:
            conn.close()

        return Phase7ResearchDatasetSummary(
            dataset_key=dataset_key,
            dataset_role=dataset_role,
            dataset_hash=dataset_hash,
            manifest_hash=manifest_hash,
            feature_schema_version=feature_schema_version,
            label_schema_version=label_schema_version,
            restore_guarantee_status=restore_status,
            baseline_model_versions=baseline_versions,
            window_count=len(normalized_windows),
            scope_count=len(normalized_scopes),
        )

    def load_research_dataset(self, *, dataset_key: str) -> dict[str, Any] | None:
        conn = get_conn()
        try:
            row = conn.execute(
                """
                SELECT
                    dataset_key,
                    dataset_role,
                    dataset_hash,
                    manifest_hash,
                    dataset_index_version,
                    feature_schema_version,
                    label_schema_version,
                    dataset_path,
                    dataset_created_at,
                    handoff_source,
                    handoff_artifact_path,
                    restore_guarantee_status,
                    restore_guarantee_json,
                    baseline_model_versions,
                    notes,
                    created_at,
                    updated_at
                FROM phase7_research_datasets
                WHERE dataset_key = ?
                """,
                (dataset_key,),
            ).fetchone()
        finally:
            conn.close()
        if row is None:
            return None
        return {
            "dataset_key": row["dataset_key"],
            "dataset_role": row["dataset_role"],
            "dataset_hash": row["dataset_hash"],
            "manifest_hash": row["manifest_hash"],
            "dataset_index_version": row["dataset_index_version"],
            "feature_schema_version": row["feature_schema_version"],
            "label_schema_version": row["label_schema_version"],
            "dataset_path": row["dataset_path"],
            "dataset_created_at": row["dataset_created_at"],
            "handoff_source": row["handoff_source"],
            "handoff_artifact_path": row["handoff_artifact_path"],
            "restore_guarantee_status": row["restore_guarantee_status"],
            "restore_guarantee_json": json.loads(row["restore_guarantee_json"] or "{}"),
            "baseline_model_versions": json.loads(row["baseline_model_versions"] or "[]"),
            "notes": row["notes"],
            "created_at": row["created_at"],
            "updated_at": row["updated_at"],
        }

    def list_recent_datasets(self, *, limit: int = 20) -> list[dict[str, Any]]:
        conn = get_conn()
        try:
            rows = conn.execute(
                """
                SELECT
                    dataset_key,
                    dataset_role,
                    dataset_hash,
                    manifest_hash,
                    dataset_index_version,
                    feature_schema_version,
                    label_schema_version,
                    dataset_path,
                    dataset_created_at,
                    handoff_source,
                    handoff_artifact_path,
                    restore_guarantee_status,
                    restore_guarantee_json,
                    baseline_model_versions,
                    notes,
                    created_at,
                    updated_at
                FROM phase7_research_datasets
                ORDER BY updated_at DESC, dataset_key DESC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
        finally:
            conn.close()
        return [
            {
                "dataset_key": row["dataset_key"],
                "dataset_role": row["dataset_role"],
                "dataset_hash": row["dataset_hash"],
                "manifest_hash": row["manifest_hash"],
                "dataset_index_version": row["dataset_index_version"],
                "feature_schema_version": row["feature_schema_version"],
                "label_schema_version": row["label_schema_version"],
                "dataset_path": row["dataset_path"],
                "dataset_created_at": row["dataset_created_at"],
                "handoff_source": row["handoff_source"],
                "handoff_artifact_path": row["handoff_artifact_path"],
                "restore_guarantee_status": row["restore_guarantee_status"],
                "restore_guarantee_json": json.loads(row["restore_guarantee_json"] or "{}"),
                "baseline_model_versions": json.loads(row["baseline_model_versions"] or "[]"),
                "notes": row["notes"],
                "created_at": row["created_at"],
                "updated_at": row["updated_at"],
            }
            for row in rows
        ]

    def list_research_windows(
        self,
        *,
        dataset_key: str | None = None,
        limit: int = 200,
    ) -> list[dict[str, Any]]:
        conn = get_conn()
        try:
            if dataset_key:
                rows = conn.execute(
                    """
                    SELECT
                        window_key,
                        dataset_key,
                        window_label,
                        start_time,
                        end_time,
                        category,
                        evaluation_scope,
                        split_key,
                        window_metadata_json,
                        notes,
                        created_at
                    FROM phase7_research_windows
                    WHERE dataset_key = ?
                    ORDER BY start_time ASC, window_key ASC
                    LIMIT ?
                    """,
                    (dataset_key, limit),
                ).fetchall()
            else:
                rows = conn.execute(
                    """
                    SELECT
                        window_key,
                        dataset_key,
                        window_label,
                        start_time,
                        end_time,
                        category,
                        evaluation_scope,
                        split_key,
                        window_metadata_json,
                        notes,
                        created_at
                    FROM phase7_research_windows
                    ORDER BY start_time ASC, window_key ASC
                    LIMIT ?
                    """,
                    (limit,),
                ).fetchall()
        finally:
            conn.close()
        return [
            {
                "window_key": row["window_key"],
                "dataset_key": row["dataset_key"],
                "window_label": row["window_label"],
                "start_time": row["start_time"],
                "end_time": row["end_time"],
                "category": row["category"],
                "evaluation_scope": row["evaluation_scope"],
                "split_key": row["split_key"],
                "window_metadata": json.loads(row["window_metadata_json"] or "{}"),
                "notes": row["notes"],
                "created_at": row["created_at"],
            }
            for row in rows
        ]

    def list_research_scopes(
        self,
        *,
        dataset_key: str | None = None,
        limit: int = 200,
    ) -> list[dict[str, Any]]:
        conn = get_conn()
        try:
            if dataset_key:
                rows = conn.execute(
                    """
                    SELECT
                        scope_id,
                        dataset_key,
                        scope_type,
                        scope_key,
                        scope_label,
                        scope_definition_json,
                        scope_index_version,
                        created_at
                    FROM phase7_research_scopes
                    WHERE dataset_key = ?
                    ORDER BY scope_type ASC, scope_key ASC
                    LIMIT ?
                    """,
                    (dataset_key, limit),
                ).fetchall()
            else:
                rows = conn.execute(
                    """
                    SELECT
                        scope_id,
                        dataset_key,
                        scope_type,
                        scope_key,
                        scope_label,
                        scope_definition_json,
                        scope_index_version,
                        created_at
                    FROM phase7_research_scopes
                    ORDER BY created_at DESC, scope_id DESC
                    LIMIT ?
                    """,
                    (limit,),
                ).fetchall()
        finally:
            conn.close()
        return [
            {
                "scope_id": row["scope_id"],
                "dataset_key": row["dataset_key"],
                "scope_type": row["scope_type"],
                "scope_key": row["scope_key"],
                "scope_label": row["scope_label"],
                "scope_definition": json.loads(row["scope_definition_json"] or "{}"),
                "scope_index_version": row["scope_index_version"],
                "created_at": row["created_at"],
            }
            for row in rows
        ]

    def _load_model_registry_snapshots(self, *, model_versions: list[str]) -> list[dict[str, Any]]:
        versions = _normalize_string_list(model_versions)
        if not versions:
            return []
        conn = get_conn()
        try:
            placeholders = ", ".join("?" for _ in versions)
            rows = conn.execute(
                f"""
                SELECT
                    model_version,
                    model_name,
                    registry_version,
                    artifact_path,
                    feature_schema_version,
                    training_dataset_hash,
                    deployment_status,
                    shadow_enabled,
                    deployed_at,
                    created_at
                FROM model_registry
                WHERE model_version IN ({placeholders})
                ORDER BY created_at DESC, model_version DESC
                """,
                tuple(versions),
            ).fetchall()
        finally:
            conn.close()

        snapshots = {str(row["model_version"]): {
            "model_version": row["model_version"],
            "model_name": row["model_name"],
            "registry_version": row["registry_version"],
            "artifact_path": row["artifact_path"],
            "feature_schema_version": row["feature_schema_version"],
            "training_dataset_hash": row["training_dataset_hash"],
            "deployment_status": row["deployment_status"],
            "shadow_enabled": bool(row["shadow_enabled"]),
            "deployed_at": row["deployed_at"],
            "created_at": row["created_at"],
        } for row in rows}

        result: list[dict[str, Any]] = []
        for version in versions:
            result.append(
                snapshots.get(
                    version,
                    {
                        "model_version": version,
                        "registry_status": "missing",
                    },
                )
            )
        return result

    def record_experiment_run(
        self,
        *,
        dataset_key: str,
        experiment_name: str,
        experiment_version: str,
        model_version: str,
        config_json: dict[str, Any],
        experiment_family: str = PHASE7_DEFAULT_EXPERIMENT_FAMILY,
        config_version: str = PHASE7_CONFIG_VERSION,
        baseline_model_versions: list[str] | None = None,
        code_version: str,
        random_seed: int | None,
        status: str,
        output_path: str | None = None,
        notes: str | None = None,
        started_at: str | None = None,
        completed_at: str | None = None,
    ) -> Phase7ExperimentRunSummary:
        dataset = self.load_research_dataset(dataset_key=dataset_key)
        if dataset is None:
            raise ValueError(f"Research dataset not found: {dataset_key}")

        baseline_versions = _normalize_string_list(
            baseline_model_versions or dataset["baseline_model_versions"]
        )
        baseline_registry = self._load_model_registry_snapshots(model_versions=baseline_versions)
        config_hash = _json_hash({"config_version": config_version, "config_json": config_json})
        input_fingerprint = _json_hash(
            {
                "dataset_key": dataset_key,
                "dataset_hash": dataset["dataset_hash"],
                "manifest_hash": dataset["manifest_hash"],
                "model_version": model_version,
                "baseline_model_versions": baseline_versions,
                "config_version": config_version,
                "config_hash": config_hash,
                "code_version": code_version,
                "random_seed": random_seed,
            }
        )
        experiment_run_id = uuid4().hex

        conn = get_conn()
        try:
            conn.execute(
                """
                INSERT INTO phase7_experiment_ledger (
                    experiment_run_id,
                    experiment_name,
                    experiment_family,
                    experiment_version,
                    ledger_version,
                    dataset_key,
                    dataset_hash,
                    manifest_hash,
                    model_version,
                    baseline_model_versions,
                    baseline_registry_json,
                    feature_schema_version,
                    label_schema_version,
                    config_version,
                    config_hash,
                    config_json,
                    code_version,
                    random_seed,
                    restore_guarantee_status,
                    input_fingerprint,
                    output_path,
                    status,
                    notes,
                    started_at,
                    completed_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    experiment_run_id,
                    experiment_name,
                    experiment_family,
                    experiment_version,
                    PHASE7_EXPERIMENT_LEDGER_VERSION,
                    dataset_key,
                    dataset["dataset_hash"],
                    dataset["manifest_hash"],
                    model_version,
                    _stable_json(baseline_versions),
                    _stable_json(baseline_registry),
                    dataset["feature_schema_version"],
                    dataset["label_schema_version"],
                    config_version,
                    config_hash,
                    _stable_json(config_json),
                    code_version,
                    random_seed,
                    dataset["restore_guarantee_status"],
                    input_fingerprint,
                    output_path,
                    status,
                    notes,
                    started_at,
                    completed_at,
                ),
            )
            conn.commit()
        finally:
            conn.close()

        return Phase7ExperimentRunSummary(
            experiment_run_id=experiment_run_id,
            experiment_name=experiment_name,
            experiment_family=experiment_family,
            experiment_version=experiment_version,
            dataset_key=dataset_key,
            dataset_hash=str(dataset["dataset_hash"]),
            model_version=model_version,
            config_hash=config_hash,
            code_version=code_version,
            status=status,
            output_path=output_path,
        )

    def update_experiment_output_path(
        self,
        *,
        experiment_run_id: str,
        output_path: str,
    ) -> None:
        conn = get_conn()
        try:
            conn.execute(
                """
                UPDATE phase7_experiment_ledger
                SET output_path = ?
                WHERE experiment_run_id = ?
                """,
                (output_path, experiment_run_id),
            )
            conn.commit()
        finally:
            conn.close()

    def list_experiment_runs(
        self,
        *,
        dataset_key: str | None = None,
        limit: int = 50,
    ) -> list[dict[str, Any]]:
        conn = get_conn()
        try:
            if dataset_key:
                rows = conn.execute(
                    """
                    SELECT
                        experiment_run_id,
                        experiment_name,
                        experiment_family,
                        experiment_version,
                        ledger_version,
                        dataset_key,
                        dataset_hash,
                        manifest_hash,
                        model_version,
                        baseline_model_versions,
                        baseline_registry_json,
                        feature_schema_version,
                        label_schema_version,
                        config_version,
                        config_hash,
                        config_json,
                        code_version,
                        random_seed,
                        restore_guarantee_status,
                        input_fingerprint,
                        output_path,
                        status,
                        notes,
                        started_at,
                        completed_at,
                        created_at
                    FROM phase7_experiment_ledger
                    WHERE dataset_key = ?
                    ORDER BY created_at DESC, experiment_run_id DESC
                    LIMIT ?
                    """,
                    (dataset_key, limit),
                ).fetchall()
            else:
                rows = conn.execute(
                    """
                    SELECT
                        experiment_run_id,
                        experiment_name,
                        experiment_family,
                        experiment_version,
                        ledger_version,
                        dataset_key,
                        dataset_hash,
                        manifest_hash,
                        model_version,
                        baseline_model_versions,
                        baseline_registry_json,
                        feature_schema_version,
                        label_schema_version,
                        config_version,
                        config_hash,
                        config_json,
                        code_version,
                        random_seed,
                        restore_guarantee_status,
                        input_fingerprint,
                        output_path,
                        status,
                        notes,
                        started_at,
                        completed_at,
                        created_at
                    FROM phase7_experiment_ledger
                    ORDER BY created_at DESC, experiment_run_id DESC
                    LIMIT ?
                    """,
                    (limit,),
                ).fetchall()
        finally:
            conn.close()
        return [
            {
                "experiment_run_id": row["experiment_run_id"],
                "experiment_name": row["experiment_name"],
                "experiment_family": row["experiment_family"],
                "experiment_version": row["experiment_version"],
                "ledger_version": row["ledger_version"],
                "dataset_key": row["dataset_key"],
                "dataset_hash": row["dataset_hash"],
                "manifest_hash": row["manifest_hash"],
                "model_version": row["model_version"],
                "baseline_model_versions": json.loads(row["baseline_model_versions"] or "[]"),
                "baseline_registry": json.loads(row["baseline_registry_json"] or "[]"),
                "feature_schema_version": row["feature_schema_version"],
                "label_schema_version": row["label_schema_version"],
                "config_version": row["config_version"],
                "config_hash": row["config_hash"],
                "config_json": json.loads(row["config_json"] or "{}"),
                "code_version": row["code_version"],
                "random_seed": row["random_seed"],
                "restore_guarantee_status": row["restore_guarantee_status"],
                "input_fingerprint": row["input_fingerprint"],
                "output_path": row["output_path"],
                "status": row["status"],
                "notes": row["notes"],
                "started_at": row["started_at"],
                "completed_at": row["completed_at"],
                "created_at": row["created_at"],
            }
            for row in rows
        ]

    def build_reproducibility_status(self, *, limit: int = 20) -> Phase7ResearchStatusSummary:
        recent_datasets = self.list_recent_datasets(limit=limit)
        latest_dataset = recent_datasets[0] if recent_datasets else None
        latest_dataset_key = (latest_dataset or {}).get("dataset_key")
        latest_windows = (
            self.list_research_windows(dataset_key=str(latest_dataset_key), limit=500)
            if latest_dataset_key
            else []
        )
        latest_scopes = (
            self.list_research_scopes(dataset_key=str(latest_dataset_key), limit=500)
            if latest_dataset_key
            else []
        )
        recent_experiments = self.list_experiment_runs(limit=limit)
        traceable_experiment_count = sum(
            1
            for item in recent_experiments
            if item.get("dataset_hash")
            and item.get("manifest_hash")
            and item.get("model_version")
            and item.get("config_hash")
            and item.get("code_version")
        )
        return Phase7ResearchStatusSummary(
            latest_dataset=latest_dataset,
            recent_datasets=recent_datasets,
            latest_dataset_windows=latest_windows,
            latest_dataset_scopes=latest_scopes,
            recent_experiments=recent_experiments,
            traceable_experiment_count=traceable_experiment_count,
        )
