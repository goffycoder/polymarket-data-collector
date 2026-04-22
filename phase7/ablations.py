from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any


EXPECTED_FAMILY_ORDER: tuple[str, ...] = (
    "severity_heuristic",
    "wallet_heuristic",
    "velocity_heuristic",
    "wallet_aware_phase6",
    "graph_aware",
    "temporal",
    "other_advanced",
)

FAMILY_DISPLAY_NAMES: dict[str, str] = {
    "severity_heuristic": "Severity Heuristic",
    "wallet_heuristic": "Wallet Heuristic",
    "velocity_heuristic": "Velocity Heuristic",
    "wallet_aware_phase6": "Wallet-Aware Phase 6",
    "graph_aware": "Graph-Aware Advanced",
    "temporal": "Temporal Advanced",
    "other_advanced": "Other Advanced",
}


@dataclass(slots=True)
class Phase7AblationSummary:
    family_rows: list[dict[str, Any]]
    split_rows: list[dict[str, Any]]
    available_families: list[str]
    missing_families: list[str]
    duplicate_families: list[str]
    dataset_hashes: list[str]
    status: str
    notes: list[str]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def canonical_family_key(value: str | None) -> str:
    key = str(value or "").strip().lower()
    if key in {"graph", "graph_aware", "graph_aware_model", "graph_model", "cluster_graph"}:
        return "graph_aware"
    if key in {"temporal", "temporal_model", "hawkes", "marked_hawkes", "tcn", "sequence"}:
        return "temporal"
    if key in {"other", "other_advanced", "other_advanced_model", "advanced_research"}:
        return "other_advanced"
    if key in EXPECTED_FAMILY_ORDER:
        return key
    return "other_advanced"


def family_display_name(family_key: str) -> str:
    return FAMILY_DISPLAY_NAMES.get(family_key, family_key.replace("_", " ").title())


def _safe_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _stable_split_metrics(score_report: dict[str, Any], *, score_key: str) -> dict[str, dict[str, Any]]:
    splits = (score_report or {}).get("splits") or {}
    result: dict[str, dict[str, Any]] = {}
    for split_name in ("train", "validation", "test"):
        metrics = ((splits.get(split_name) or {}).get(score_key) or {}).copy()
        result[split_name] = metrics
    return result


def _family_flags(family_key: str) -> dict[str, bool]:
    return {
        "uses_wallet_features": family_key in {"wallet_heuristic", "wallet_aware_phase6", "graph_aware", "temporal", "other_advanced"},
        "uses_graph_features": family_key == "graph_aware",
        "uses_temporal_model": family_key == "temporal",
        "uses_advanced_modeling": family_key in {"graph_aware", "temporal", "other_advanced"},
    }


def _build_family_row(
    *,
    family_key: str,
    dataset_hash: str | None,
    artifact_path: str | None,
    model_version: str | None,
    experiment_version: str | None,
    split_metrics: dict[str, dict[str, Any]],
    strict_holdout_status: str,
    strict_holdout_accepted: bool,
    test_margin_vs_phase6_auc: float | None,
    validation_margin_vs_phase6_auc: float | None,
    test_margin_vs_best_heuristic_auc: float | None,
    notes: list[str] | None = None,
    availability_status: str = "available",
) -> dict[str, Any]:
    validation = split_metrics.get("validation") or {}
    test = split_metrics.get("test") or {}
    train = split_metrics.get("train") or {}
    return {
        "family_key": family_key,
        "display_name": family_display_name(family_key),
        "availability_status": availability_status,
        "dataset_hash": dataset_hash,
        "artifact_path": artifact_path,
        "model_version": model_version,
        "experiment_version": experiment_version,
        "train_auc": _safe_float(train.get("auc")),
        "validation_auc": _safe_float(validation.get("auc")),
        "test_auc": _safe_float(test.get("auc")),
        "train_precision_at_10": _safe_float(train.get("precision_at_10")),
        "validation_precision_at_10": _safe_float(validation.get("precision_at_10")),
        "test_precision_at_10": _safe_float(test.get("precision_at_10")),
        "train_row_count": int(train.get("row_count") or 0),
        "validation_row_count": int(validation.get("row_count") or 0),
        "test_row_count": int(test.get("row_count") or 0),
        "strict_holdout_status": strict_holdout_status,
        "strict_holdout_accepted": bool(strict_holdout_accepted),
        "validation_margin_vs_phase6_auc": validation_margin_vs_phase6_auc,
        "test_margin_vs_phase6_auc": test_margin_vs_phase6_auc,
        "test_margin_vs_best_heuristic_auc": test_margin_vs_best_heuristic_auc,
        "notes": list(notes or []),
        **_family_flags(family_key),
    }


def _placeholder_family_row(family_key: str, *, note: str) -> dict[str, Any]:
    return _build_family_row(
        family_key=family_key,
        dataset_hash=None,
        artifact_path=None,
        model_version=None,
        experiment_version=None,
        split_metrics={split_name: {} for split_name in ("train", "validation", "test")},
        strict_holdout_status="missing_artifact",
        strict_holdout_accepted=False,
        test_margin_vs_phase6_auc=None,
        validation_margin_vs_phase6_auc=None,
        test_margin_vs_best_heuristic_auc=None,
        notes=[note],
        availability_status="missing",
    )


def _append_split_rows(
    split_rows: list[dict[str, Any]],
    *,
    family_key: str,
    model_version: str | None,
    dataset_hash: str | None,
    artifact_path: str | None,
    split_metrics: dict[str, dict[str, Any]],
) -> None:
    for split_name in ("train", "validation", "test"):
        metrics = split_metrics.get(split_name) or {}
        split_rows.append(
            {
                "family_key": family_key,
                "display_name": family_display_name(family_key),
                "split_name": split_name,
                "dataset_hash": dataset_hash,
                "artifact_path": artifact_path,
                "model_version": model_version,
                "auc": _safe_float(metrics.get("auc")),
                "precision_at_10": _safe_float(metrics.get("precision_at_10")),
                "precision_at_25": _safe_float(metrics.get("precision_at_25")),
                "positive_rate": _safe_float(metrics.get("positive_rate")),
                "row_count": int(metrics.get("row_count") or 0),
                "mean_score": _safe_float(metrics.get("mean_score")),
            }
        )


def _best_heuristic_test_auc(score_report: dict[str, Any]) -> float | None:
    best_value = None
    for key in ("baseline_severity", "baseline_wallet", "baseline_velocity"):
        value = _safe_float((((score_report or {}).get("splits") or {}).get("test") or {}).get(key, {}).get("auc"))
        if value is not None and (best_value is None or value > best_value):
            best_value = value
    return best_value


def _normalize_advanced_artifact(
    *,
    payload: dict[str, Any],
    family_key: str,
    artifact_path: str,
) -> tuple[dict[str, Any], dict[str, dict[str, Any]], dict[str, dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    experiment_report = payload.get("experiment_report") or payload
    advanced_score_report = (experiment_report.get("advanced_score_report") or payload.get("advanced_score_report") or {})
    phase6_score_report = (experiment_report.get("phase6_score_report") or payload.get("phase6_score_report") or {})
    split_comparisons = experiment_report.get("split_comparisons") or {}
    holdout_assessment = experiment_report.get("holdout_assessment") or {}
    dataset_hash = str(payload.get("dataset_hash") or ((payload.get("dataset_summary") or {}).get("dataset_hash") or ""))
    model_version = str(payload.get("advanced_model_version") or payload.get("model_version") or family_key)
    experiment_version = str(payload.get("experiment_version") or experiment_report.get("experiment_version") or "")

    advanced_splits = _stable_split_metrics(advanced_score_report, score_key="model")
    phase6_splits = _stable_split_metrics(phase6_score_report, score_key="model")

    family_row = _build_family_row(
        family_key=family_key,
        dataset_hash=dataset_hash or None,
        artifact_path=artifact_path,
        model_version=model_version,
        experiment_version=experiment_version or None,
        split_metrics=advanced_splits,
        strict_holdout_status=str(holdout_assessment.get("status") or "descriptive_only"),
        strict_holdout_accepted=bool(holdout_assessment.get("accepted")),
        validation_margin_vs_phase6_auc=_safe_float((split_comparisons.get("validation") or {}).get("phase6_margin_auc")),
        test_margin_vs_phase6_auc=_safe_float((split_comparisons.get("test") or {}).get("phase6_margin_auc")),
        test_margin_vs_best_heuristic_auc=_safe_float((split_comparisons.get("test") or {}).get("heuristic_margin_auc")),
        notes=list(holdout_assessment.get("reasons") or []),
    )

    phase6_row = _build_family_row(
        family_key="wallet_aware_phase6",
        dataset_hash=dataset_hash or None,
        artifact_path=artifact_path,
        model_version=str(payload.get("phase6_baseline_model_version") or "phase6_baseline"),
        experiment_version=experiment_version or None,
        split_metrics=phase6_splits,
        strict_holdout_status="baseline_reference",
        strict_holdout_accepted=True,
        validation_margin_vs_phase6_auc=0.0,
        test_margin_vs_phase6_auc=0.0,
        test_margin_vs_best_heuristic_auc=None,
        notes=["Reference Phase 6 wallet-aware baseline carried inside the advanced experiment artifact."],
    )

    heuristic_rows: list[dict[str, Any]] = []
    for family_name, score_key in (
        ("severity_heuristic", "baseline_severity"),
        ("wallet_heuristic", "baseline_wallet"),
        ("velocity_heuristic", "baseline_velocity"),
    ):
        split_metrics = _stable_split_metrics(advanced_score_report, score_key=score_key)
        test_auc = _safe_float((split_metrics.get("test") or {}).get("auc"))
        phase6_test_auc = _safe_float((phase6_splits.get("test") or {}).get("auc"))
        validation_auc = _safe_float((split_metrics.get("validation") or {}).get("auc"))
        phase6_validation_auc = _safe_float((phase6_splits.get("validation") or {}).get("auc"))
        heuristic_rows.append(
            _build_family_row(
                family_key=family_name,
                dataset_hash=dataset_hash or None,
                artifact_path=artifact_path,
                model_version=family_name,
                experiment_version=experiment_version or None,
                split_metrics=split_metrics,
                strict_holdout_status="baseline_reference",
                strict_holdout_accepted=True,
                validation_margin_vs_phase6_auc=(
                    round(validation_auc - phase6_validation_auc, 6)
                    if validation_auc is not None and phase6_validation_auc is not None
                    else None
                ),
                test_margin_vs_phase6_auc=(
                    round(test_auc - phase6_test_auc, 6)
                    if test_auc is not None and phase6_test_auc is not None
                    else None
                ),
                test_margin_vs_best_heuristic_auc=0.0 if test_auc is not None else None,
                notes=["Reference heuristic baseline carried inside the advanced experiment artifact."],
            )
        )

    return family_row, advanced_splits, phase6_splits, heuristic_rows, split_comparisons


def build_ablation_summary(
    *,
    experiment_artifacts: list[dict[str, Any]],
) -> Phase7AblationSummary:
    family_rows: dict[str, dict[str, Any]] = {}
    split_rows: list[dict[str, Any]] = []
    dataset_hashes: set[str] = set()
    duplicate_families: list[str] = []
    notes: list[str] = []
    reference_loaded = False

    for artifact in experiment_artifacts:
        path = str(artifact.get("artifact_path") or "")
        payload = artifact.get("payload") or {}
        family_key = canonical_family_key(artifact.get("family_key"))
        if family_key in family_rows:
            duplicate_families.append(family_key)
            notes.append(f"Duplicate artifact for family `{family_key}` ignored after the first occurrence: {path}")
            continue

        family_row, advanced_splits, phase6_splits, heuristic_rows, _ = _normalize_advanced_artifact(
            payload=payload,
            family_key=family_key,
            artifact_path=path,
        )
        family_rows[family_key] = family_row
        if family_row.get("dataset_hash"):
            dataset_hashes.add(str(family_row["dataset_hash"]))
        _append_split_rows(
            split_rows,
            family_key=family_key,
            model_version=family_row.get("model_version"),
            dataset_hash=family_row.get("dataset_hash"),
            artifact_path=path,
            split_metrics=advanced_splits,
        )

        if not reference_loaded:
            reference_loaded = True
            phase6_row = _build_family_row(
                family_key="wallet_aware_phase6",
                dataset_hash=family_row.get("dataset_hash"),
                artifact_path=path,
                model_version=str(payload.get("phase6_baseline_model_version") or "phase6_baseline"),
                experiment_version=str(payload.get("experiment_version") or ""),
                split_metrics=phase6_splits,
                strict_holdout_status="baseline_reference",
                strict_holdout_accepted=True,
                validation_margin_vs_phase6_auc=0.0,
                test_margin_vs_phase6_auc=0.0,
                test_margin_vs_best_heuristic_auc=(
                    round(
                        _safe_float((phase6_splits.get("test") or {}).get("auc")) - _best_heuristic_test_auc(payload.get("experiment_report", {}).get("advanced_score_report") or {}),
                        6,
                    )
                    if _safe_float((phase6_splits.get("test") or {}).get("auc")) is not None
                    and _best_heuristic_test_auc(payload.get("experiment_report", {}).get("advanced_score_report") or {}) is not None
                    else None
                ),
                notes=["Reference Phase 6 wallet-aware baseline carried inside the first advanced artifact."],
            )
            family_rows["wallet_aware_phase6"] = phase6_row
            _append_split_rows(
                split_rows,
                family_key="wallet_aware_phase6",
                model_version=phase6_row.get("model_version"),
                dataset_hash=phase6_row.get("dataset_hash"),
                artifact_path=path,
                split_metrics=phase6_splits,
            )
            for row in heuristic_rows:
                family_rows[row["family_key"]] = row
                _append_split_rows(
                    split_rows,
                    family_key=row["family_key"],
                    model_version=row.get("model_version"),
                    dataset_hash=row.get("dataset_hash"),
                    artifact_path=path,
                    split_metrics=_stable_split_metrics(
                        (payload.get("experiment_report") or payload).get("advanced_score_report") or {},
                        score_key={
                            "severity_heuristic": "baseline_severity",
                            "wallet_heuristic": "baseline_wallet",
                            "velocity_heuristic": "baseline_velocity",
                        }[row["family_key"]],
                    ),
                )

    for family_key in EXPECTED_FAMILY_ORDER:
        if family_key not in family_rows:
            family_rows[family_key] = _placeholder_family_row(
                family_key,
                note="No saved experiment artifact was available for this family when the research package was generated.",
            )

    ordered_rows = [family_rows[family_key] for family_key in EXPECTED_FAMILY_ORDER]
    available_families = [row["family_key"] for row in ordered_rows if row["availability_status"] == "available"]
    missing_families = [row["family_key"] for row in ordered_rows if row["availability_status"] != "available"]

    if not experiment_artifacts:
        status = "no_experiment_artifacts"
        notes.append("No advanced experiment artifacts were supplied or discovered.")
    elif len(dataset_hashes) > 1:
        status = "dataset_hash_mismatch"
        notes.append("Ablation rows were built from multiple dataset hashes; headline claims must remain descriptive only.")
    elif available_families:
        status = "ready"
    else:
        status = "descriptive_only"

    return Phase7AblationSummary(
        family_rows=ordered_rows,
        split_rows=split_rows,
        available_families=available_families,
        missing_families=missing_families,
        duplicate_families=duplicate_families,
        dataset_hashes=sorted(dataset_hashes),
        status=status,
        notes=notes,
    )
