from __future__ import annotations

import json
import shutil
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from config import settings
from phase7.reporting import _load_latest_audit


REPO_ROOT = Path(__file__).resolve().parent.parent
PROTECTED_REPLAY_DIR_NAMES = {"phase9_task3", "phase10_task3"}


def _iso_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _bytes_to_gb(value: int) -> float:
    return round(float(value) / float(1024**3), 3)


def _directory_size(path: Path) -> int:
    if not path.exists():
        return 0
    if path.is_file():
        return int(path.stat().st_size)
    total = 0
    for file_path in path.rglob("*"):
        if file_path.is_file():
            total += int(file_path.stat().st_size)
    return total


def _path_exists(path: Path | None) -> bool:
    return path is not None and path.exists()


def _managed_paths() -> list[dict[str, Any]]:
    sqlite_path = Path(str(settings.DB_PATH))
    if not sqlite_path.is_absolute():
        sqlite_path = REPO_ROOT / sqlite_path

    paths: list[dict[str, Any]] = [
        {
            "storage_class": "raw_archives",
            "path": REPO_ROOT / "data" / "raw",
            "retention": {
                "hot_days": int(settings.PHASE7_HOT_RETENTION_DAYS),
                "warm_days": int(settings.PHASE7_WARM_RETENTION_DAYS),
                "cold_days": int(settings.PHASE7_COLD_RETENTION_DAYS),
            },
            "reproducibility_requirement": "required_for_raw_envelope_auditability",
        },
        {
            "storage_class": "detector_input",
            "path": REPO_ROOT / "data" / "detector_input",
            "retention": {
                "hot_days": int(settings.PHASE7_HOT_RETENTION_DAYS),
                "warm_days": int(settings.PHASE7_WARM_RETENTION_DAYS),
                "cold_days": int(settings.PHASE7_COLD_RETENTION_DAYS),
            },
            "reproducibility_requirement": "required_for_archived_window_phase3_replay",
        },
        {
            "storage_class": "database_state",
            "path": sqlite_path if settings.DB_BACKEND == "sqlite" else None,
            "retention": {
                "policy": "retain_canonical_runtime_state",
                "notes": "Do not prune canonical DB state without an explicit dump or backup.",
            },
            "reproducibility_requirement": "required_for_candidates_alerts_scores_and_audit_ledgers",
        },
        {
            "storage_class": "logs",
            "path": REPO_ROOT / "logs",
            "retention": {
                "retain_days": int(settings.PHASE11_LOG_RETENTION_DAYS),
                "prune_scope": "rotated_and_old_runtime_logs",
            },
            "reproducibility_requirement": "short_lived_operator_debugging_only",
        },
        {
            "storage_class": "manual_replay_artifacts",
            "path": REPO_ROOT / "reports" / "phase5" / "replay_runs" / "manual",
            "retention": {
                "retain_days": int(settings.PHASE11_MANUAL_REPLAY_RETENTION_DAYS),
                "protected_dirs": sorted(PROTECTED_REPLAY_DIR_NAMES),
            },
            "reproducibility_requirement": "manual_runs_only_frozen_phase9_phase10_packets_are_protected_elsewhere",
        },
        {
            "storage_class": "operator_reports",
            "path": REPO_ROOT / "reports" / "phase7",
            "retention": {
                "retain_days": int(settings.PHASE11_OPERATOR_REPORT_RETENTION_DAYS),
                "prune_scope": "refreshable_operator_reports",
            },
            "reproducibility_requirement": "refreshable_from_db_when_runtime_state_is_intact",
        },
    ]

    managed: list[dict[str, Any]] = []
    for item in paths:
        path = item["path"]
        managed.append(
            {
                **item,
                "path": None if path is None else str(path),
                "exists": _path_exists(path),
                "size_bytes": None if path is None else _directory_size(path),
            }
        )
    return managed


def _disk_headroom() -> dict[str, Any]:
    usage = shutil.disk_usage(REPO_ROOT)
    free_percent = round((float(usage.free) / float(usage.total)) * 100.0, 3) if usage.total else 0.0
    min_free_bytes = int(settings.PHASE11_RUNTIME_MIN_FREE_GB) * 1024**3
    min_free_percent = float(settings.PHASE11_RUNTIME_MIN_FREE_PERCENT)

    blockers: list[str] = []
    if usage.free < min_free_bytes:
        blockers.append("free_bytes_below_threshold")
    if free_percent < min_free_percent:
        blockers.append("free_percent_below_threshold")

    return {
        "total_bytes": int(usage.total),
        "used_bytes": int(usage.used),
        "free_bytes": int(usage.free),
        "free_gb": _bytes_to_gb(int(usage.free)),
        "free_percent": free_percent,
        "min_free_bytes": min_free_bytes,
        "min_free_gb": int(settings.PHASE11_RUNTIME_MIN_FREE_GB),
        "min_free_percent": min_free_percent,
        "blockers": blockers,
    }


def _retention_policy() -> list[dict[str, Any]]:
    return [
        {
            "storage_class": "raw_archives",
            "hot_days": int(settings.PHASE7_HOT_RETENTION_DAYS),
            "warm_days": int(settings.PHASE7_WARM_RETENTION_DAYS),
            "cold_days": int(settings.PHASE7_COLD_RETENTION_DAYS),
            "archive_only_after_days": int(settings.PHASE7_COLD_RETENTION_DAYS),
            "operator_rule": "Do not delete blindly; use storage audit, compaction plan, and restore plan first.",
        },
        {
            "storage_class": "detector_input",
            "hot_days": int(settings.PHASE7_HOT_RETENTION_DAYS),
            "warm_days": int(settings.PHASE7_WARM_RETENTION_DAYS),
            "cold_days": int(settings.PHASE7_COLD_RETENTION_DAYS),
            "archive_only_after_days": int(settings.PHASE7_COLD_RETENTION_DAYS),
            "operator_rule": "This is the minimum archive needed for archived-window Phase 3 replay.",
        },
        {
            "storage_class": "database_state",
            "retention_mode": "retain_indefinitely_with_backup_before_maintenance",
            "operator_rule": "SQLite file or PostgreSQL dump must exist before invasive cleanup.",
        },
        {
            "storage_class": "logs",
            "retain_days": int(settings.PHASE11_LOG_RETENTION_DAYS),
            "operator_rule": "Safe pruning target. Older logs are helpful but not part of the replay contract.",
        },
        {
            "storage_class": "manual_replay_artifacts",
            "retain_days": int(settings.PHASE11_MANUAL_REPLAY_RETENTION_DAYS),
            "protected_dirs": sorted(PROTECTED_REPLAY_DIR_NAMES),
            "operator_rule": "Prune ad hoc replay output; preserve frozen evidence packets referenced by docs.",
        },
        {
            "storage_class": "operator_reports",
            "retain_days": int(settings.PHASE11_OPERATOR_REPORT_RETENTION_DAYS),
            "operator_rule": "Safe pruning target because these reports can be regenerated from current state.",
        },
    ]


def _reproducibility_contract() -> list[dict[str, Any]]:
    return [
        {
            "artifact": "data/detector_input/",
            "required": True,
            "reason": "Archived-window Phase 3 replay and late detector activation recovery depend on detector-input partitions.",
        },
        {
            "artifact": "raw_archive_manifests + detector_input_manifests",
            "required": True,
            "reason": "These tables prove which partitions existed, which are missing, and what was lost.",
        },
        {
            "artifact": "database runtime state",
            "required": True,
            "reason": "Persisted candidates, alerts, scores, checkpoints, and restore/audit ledgers live here.",
        },
        {
            "artifact": "reports/phase5/replay_runs/phase9_task3 + phase10_task3",
            "required": True,
            "reason": "These are frozen reference evidence packets already cited by later-phase docs.",
        },
        {
            "artifact": "data/raw/",
            "required": True,
            "reason": "Raw envelopes preserve source-of-truth payloads for auditability and republish validation.",
        },
        {
            "artifact": "logs/",
            "required": False,
            "reason": "Useful for incident debugging but not a long-term reproducibility requirement.",
        },
    ]


def _safe_age_days(path: Path, *, now: datetime) -> float:
    return round((now - datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc)).total_seconds() / 86400.0, 3)


def _collect_prune_candidates() -> list[dict[str, Any]]:
    now = datetime.now(timezone.utc)
    candidates: list[dict[str, Any]] = []

    def add_candidates(root: Path, *, retain_days: int, category: str) -> None:
        if not root.exists():
            return
        cutoff = now - timedelta(days=max(0, retain_days))
        for path in root.rglob("*"):
            if not path.is_file():
                continue
            modified_at = datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc)
            if modified_at >= cutoff:
                continue
            candidates.append(
                {
                    "category": category,
                    "path": str(path),
                    "size_bytes": int(path.stat().st_size),
                    "age_days": _safe_age_days(path, now=now),
                    "recommended_action": "delete_file",
                }
            )

    add_candidates(REPO_ROOT / "logs", retain_days=int(settings.PHASE11_LOG_RETENTION_DAYS), category="logs")
    add_candidates(
        REPO_ROOT / "reports" / "phase5" / "replay_runs" / "manual",
        retain_days=int(settings.PHASE11_MANUAL_REPLAY_RETENTION_DAYS),
        category="manual_replay_artifacts",
    )
    add_candidates(
        REPO_ROOT / "reports" / "phase5" / "window_health",
        retain_days=int(settings.PHASE11_MANUAL_REPLAY_RETENTION_DAYS),
        category="window_health_artifacts",
    )
    add_candidates(
        REPO_ROOT / "reports" / "phase5" / "backfill_dispatch",
        retain_days=int(settings.PHASE11_MANUAL_REPLAY_RETENTION_DAYS),
        category="backfill_dispatch_artifacts",
    )
    add_candidates(
        REPO_ROOT / "reports" / "phase7",
        retain_days=int(settings.PHASE11_OPERATOR_REPORT_RETENTION_DAYS),
        category="operator_reports",
    )

    return sorted(candidates, key=lambda item: (-int(item["size_bytes"]), item["path"]))


@dataclass(slots=True)
class RuntimeStorageStatusSummary:
    status: str
    reason: str
    free_gb: float
    free_percent: float
    managed_bytes: int
    managed_gb: float
    prune_candidate_count: int
    prune_candidate_bytes: int
    latest_storage_audit_run_id: str | None
    output_path: str | None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class RuntimeArtifactPruneSummary:
    apply_mode: bool
    deleted_file_count: int
    deleted_bytes: int
    candidate_file_count: int
    candidate_bytes: int
    output_path: str | None
    status: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


class RuntimeStorageSafetyError(RuntimeError):
    """Raised when runtime storage headroom is below the canonical safety floor."""


def build_runtime_storage_status(*, output_path: str | None = None) -> tuple[RuntimeStorageStatusSummary, dict[str, Any]]:
    managed_paths = _managed_paths()
    disk = _disk_headroom()
    latest_audit = _load_latest_audit()
    prune_candidates = _collect_prune_candidates()

    managed_bytes = sum(int(item["size_bytes"] or 0) for item in managed_paths)
    prune_candidate_bytes = sum(int(item["size_bytes"] or 0) for item in prune_candidates)
    warn_managed_bytes = int(settings.PHASE11_RUNTIME_WARN_MANAGED_GB) * 1024**3

    status = "ok"
    reason = "storage_headroom_healthy"
    if disk["blockers"]:
        status = "blocked"
        reason = ",".join(disk["blockers"])
    elif latest_audit is not None and int(latest_audit.get("missing_file_count") or 0) > 0:
        status = "warning"
        reason = "missing_archive_files_detected"
    elif managed_bytes >= warn_managed_bytes:
        status = "warning"
        reason = "managed_repo_storage_above_warning_threshold"

    payload = {
        "generated_at": _iso_now(),
        "status": status,
        "status_reason": reason,
        "disk_headroom": disk,
        "managed_paths": managed_paths,
        "managed_totals": {
            "managed_bytes": managed_bytes,
            "managed_gb": _bytes_to_gb(managed_bytes),
            "warning_threshold_gb": int(settings.PHASE11_RUNTIME_WARN_MANAGED_GB),
        },
        "retention_policy": _retention_policy(),
        "reproducibility_contract": _reproducibility_contract(),
        "protected_replay_dirs": sorted(PROTECTED_REPLAY_DIR_NAMES),
        "prune_candidates": prune_candidates,
        "prune_candidate_totals": {
            "candidate_count": len(prune_candidates),
            "candidate_bytes": prune_candidate_bytes,
            "candidate_gb": _bytes_to_gb(prune_candidate_bytes),
        },
        "latest_storage_audit": latest_audit,
        "historical_loss_note": (
            "Phase 11 records that the 2026-04 raw and detector-input archive tree was already deleted "
            "before this runtime revision work, so some older windows are no longer locally restorable."
        ),
    }
    summary = RuntimeStorageStatusSummary(
        status=status,
        reason=reason,
        free_gb=float(disk["free_gb"]),
        free_percent=float(disk["free_percent"]),
        managed_bytes=managed_bytes,
        managed_gb=_bytes_to_gb(managed_bytes),
        prune_candidate_count=len(prune_candidates),
        prune_candidate_bytes=prune_candidate_bytes,
        latest_storage_audit_run_id=None if latest_audit is None else str(latest_audit["storage_audit_run_id"]),
        output_path=output_path,
    )
    return summary, payload


def enforce_runtime_storage_safety() -> RuntimeStorageStatusSummary:
    summary, payload = build_runtime_storage_status()
    if summary.status == "blocked":
        raise RuntimeStorageSafetyError(
            "Runtime storage guard blocked startup or continued collection: "
            f"reason={payload['status_reason']} free_gb={summary.free_gb} "
            f"free_percent={summary.free_percent} managed_gb={summary.managed_gb}"
        )
    return summary


def prune_runtime_artifacts(
    *,
    apply: bool = False,
    output_path: str | None = None,
) -> tuple[RuntimeArtifactPruneSummary, dict[str, Any]]:
    candidates = _collect_prune_candidates()
    deleted_items: list[dict[str, Any]] = []
    deleted_bytes = 0

    for item in candidates:
        path = Path(str(item["path"]))
        if not path.exists():
            continue
        if apply:
            path.unlink()
        deleted_items.append(
            {
                **item,
                "applied": bool(apply),
            }
        )
        deleted_bytes += int(item["size_bytes"] or 0)

    payload = {
        "generated_at": _iso_now(),
        "apply_mode": bool(apply),
        "candidate_count": len(candidates),
        "candidate_bytes": sum(int(item["size_bytes"] or 0) for item in candidates),
        "deleted_items": deleted_items,
        "notes": (
            "Only safe, regenerable artifacts are pruned here. Raw archives, detector-input partitions, "
            "database state, and frozen replay packets are intentionally excluded."
        ),
    }
    summary = RuntimeArtifactPruneSummary(
        apply_mode=bool(apply),
        deleted_file_count=len(deleted_items),
        deleted_bytes=deleted_bytes,
        candidate_file_count=len(candidates),
        candidate_bytes=sum(int(item["size_bytes"] or 0) for item in candidates),
        output_path=output_path,
        status="completed" if apply else "dry_run_completed",
    )
    return summary, payload
