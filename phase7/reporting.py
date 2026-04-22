from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from uuid import uuid4

from config.settings import PHASE7_DASHBOARD_TOP_N
from database.db_manager import get_conn


def _iso_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _parse_iso(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


@dataclass(slots=True)
class Phase7DashboardSummary:
    audit_run_id: str | None
    source_count: int
    total_partitions: int
    total_bytes: int
    missing_file_count: int
    compact_candidate_count: int
    cold_candidate_count: int
    output_path: str | None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class Phase7CompactionPlanSummary:
    compaction_plan_run_id: str
    storage_audit_run_id: str | None
    plan_scope: str
    total_items: int
    compact_item_count: int
    cold_archive_item_count: int
    output_path: str | None
    status: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class Phase7HealthSummary:
    status: str
    latest_audit_run_id: str | None
    audit_age_hours: float | None
    recent_alert_count: int
    recent_shadow_score_count: int
    missing_file_count: int
    output_path: str | None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class Phase7RestorePlanSummary:
    restore_plan_run_id: str
    storage_audit_run_id: str | None
    restore_scope: str
    total_items: int
    missing_item_count: int
    output_path: str | None
    status: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class Phase7IntegritySummary:
    integrity_summary_run_id: str
    summary_scope: str
    storage_audit_run_id: str | None
    source_count: int
    total_partitions: int
    missing_file_count: int
    compact_candidate_count: int
    cold_candidate_count: int
    output_path: str | None
    status: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def _load_latest_audit() -> dict[str, Any] | None:
    conn = get_conn()
    try:
        row = conn.execute(
            """
            SELECT
                storage_audit_run_id,
                audit_scope,
                total_partitions,
                total_bytes,
                missing_file_count,
                compact_candidate_count,
                cold_candidate_count,
                output_path,
                summary_json,
                created_at
            FROM storage_audit_runs
            ORDER BY created_at DESC, storage_audit_run_id DESC
            LIMIT 1
            """
        ).fetchone()
    finally:
        conn.close()
    if row is None:
        return None
    return {
        "storage_audit_run_id": row["storage_audit_run_id"],
        "audit_scope": row["audit_scope"],
        "total_partitions": int(row["total_partitions"] or 0),
        "total_bytes": int(row["total_bytes"] or 0),
        "missing_file_count": int(row["missing_file_count"] or 0),
        "compact_candidate_count": int(row["compact_candidate_count"] or 0),
        "cold_candidate_count": int(row["cold_candidate_count"] or 0),
        "output_path": row["output_path"],
        "summary_json": json.loads(row["summary_json"] or "{}"),
        "created_at": row["created_at"],
    }


def _load_tiering_decisions(storage_audit_run_id: str) -> list[dict[str, Any]]:
    conn = get_conn()
    try:
        rows = conn.execute(
            """
            SELECT
                tiering_decision_id,
                partition_path,
                source_system,
                storage_class,
                recommended_tier,
                recommended_action,
                byte_count,
                age_days,
                file_exists,
                metadata_json,
                created_at
            FROM archive_tiering_decisions
            WHERE storage_audit_run_id = ?
            ORDER BY byte_count DESC, partition_path ASC
            """,
            (storage_audit_run_id,),
        ).fetchall()
    finally:
        conn.close()
    return [
        {
            "tiering_decision_id": row["tiering_decision_id"],
            "partition_path": row["partition_path"],
            "source_system": row["source_system"],
            "storage_class": row["storage_class"],
            "recommended_tier": row["recommended_tier"],
            "recommended_action": row["recommended_action"],
            "byte_count": int(row["byte_count"] or 0),
            "age_days": row["age_days"],
            "file_exists": bool(row["file_exists"]),
            "metadata_json": json.loads(row["metadata_json"] or "{}"),
            "created_at": row["created_at"],
        }
        for row in rows
    ]


def _load_runtime_rollups() -> dict[str, Any]:
    conn = get_conn()
    try:
        now = datetime.now(timezone.utc)
        day_ago = (now - timedelta(days=1)).isoformat()
        candidate_count = conn.execute(
            "SELECT COUNT(*) AS count FROM signal_candidates WHERE trigger_time >= ?",
            (day_ago,),
        ).fetchone()
        alert_count = conn.execute(
            "SELECT COUNT(*) AS count FROM alerts WHERE created_at >= ?",
            (day_ago,),
        ).fetchone()
        shadow_score_count = conn.execute(
            "SELECT COUNT(*) AS count FROM shadow_model_scores WHERE created_at >= ?",
            (day_ago,),
        ).fetchone()
        active_shadow = conn.execute(
            """
            SELECT model_version, model_name, deployment_status, shadow_enabled, deployed_at
            FROM model_registry
            WHERE shadow_enabled = 1
            ORDER BY COALESCE(deployed_at, created_at) DESC
            LIMIT 1
            """
        ).fetchone()
    finally:
        conn.close()
    return {
        "last_24h_candidate_count": int((candidate_count or {}).get("count", 0) if candidate_count else 0),
        "last_24h_alert_count": int((alert_count or {}).get("count", 0) if alert_count else 0),
        "last_24h_shadow_score_count": int((shadow_score_count or {}).get("count", 0) if shadow_score_count else 0),
        "active_shadow_model": None
        if active_shadow is None
        else {
            "model_version": active_shadow["model_version"],
            "model_name": active_shadow["model_name"],
            "deployment_status": active_shadow["deployment_status"],
            "shadow_enabled": bool(active_shadow["shadow_enabled"]),
            "deployed_at": active_shadow["deployed_at"],
        },
    }


def _parse_partition_timestamp(partition_path: str) -> datetime | None:
    parts = {item.split("=", 1)[0]: item.split("=", 1)[1] for item in partition_path.split("/") if "=" in item}
    try:
        return datetime(
            year=int(parts["year"]),
            month=int(parts["month"]),
            day=int(parts["day"]),
            hour=int(parts["hour"]),
            tzinfo=timezone.utc,
        )
    except (KeyError, ValueError):
        return None


def build_phase7_dashboard(*, output_path: str | None = None) -> tuple[Phase7DashboardSummary, dict[str, Any]]:
    latest = _load_latest_audit()
    if latest is None:
        payload = {
            "status": "no_storage_audit_yet",
            "generated_at": _iso_now(),
        }
        return (
            Phase7DashboardSummary(
                audit_run_id=None,
                source_count=0,
                total_partitions=0,
                total_bytes=0,
                missing_file_count=0,
                compact_candidate_count=0,
                cold_candidate_count=0,
                output_path=output_path,
            ),
            payload,
        )

    decisions = _load_tiering_decisions(str(latest["storage_audit_run_id"]))
    source_rollups: dict[str, dict[str, Any]] = {}
    action_rollups: dict[str, int] = {}
    tier_rollups: dict[str, int] = {}
    for item in decisions:
        source_rollups.setdefault(
            item["source_system"],
            {
                "source_system": item["source_system"],
                "partition_count": 0,
                "total_bytes": 0,
                "missing_file_count": 0,
            },
        )
        source_rollups[item["source_system"]]["partition_count"] += 1
        source_rollups[item["source_system"]]["total_bytes"] += int(item["byte_count"] or 0)
        source_rollups[item["source_system"]]["missing_file_count"] += 0 if item["file_exists"] else 1
        action_rollups[item["recommended_action"]] = action_rollups.get(item["recommended_action"], 0) + 1
        tier_rollups[item["recommended_tier"]] = tier_rollups.get(item["recommended_tier"], 0) + 1

    payload = {
        "status": "ok",
        "generated_at": _iso_now(),
        "latest_storage_audit": latest,
        "runtime_rollups": _load_runtime_rollups(),
        "top_sources": sorted(source_rollups.values(), key=lambda item: (-item["total_bytes"], item["source_system"]))[:PHASE7_DASHBOARD_TOP_N],
        "action_rollups": action_rollups,
        "tier_rollups": tier_rollups,
        "largest_partitions": decisions[:PHASE7_DASHBOARD_TOP_N],
    }
    return (
        Phase7DashboardSummary(
            audit_run_id=str(latest["storage_audit_run_id"]),
            source_count=len(source_rollups),
            total_partitions=int(latest["total_partitions"]),
            total_bytes=int(latest["total_bytes"]),
            missing_file_count=int(latest["missing_file_count"]),
            compact_candidate_count=int(latest["compact_candidate_count"]),
            cold_candidate_count=int(latest["cold_candidate_count"]),
            output_path=output_path,
        ),
        payload,
    )


def build_compaction_plan(
    *,
    storage_audit_run_id: str | None = None,
    plan_scope: str = "default",
    output_path: str | None = None,
) -> tuple[Phase7CompactionPlanSummary, dict[str, Any]]:
    latest = _load_latest_audit()
    if latest is None and storage_audit_run_id is None:
        raise ValueError("No storage audit exists yet. Run run_phase7_storage_audit.py first.")
    audit_id = storage_audit_run_id or str((latest or {})["storage_audit_run_id"])
    decisions = _load_tiering_decisions(audit_id)
    plan_items = [
        item
        for item in decisions
        if item["recommended_action"] in {"compact_candidate", "compact_and_cold_archive", "cold_archive_candidate"}
    ]
    compact_count = sum(
        1 for item in plan_items if item["recommended_action"] in {"compact_candidate", "compact_and_cold_archive"}
    )
    cold_count = sum(
        1 for item in plan_items if item["recommended_action"] in {"cold_archive_candidate", "compact_and_cold_archive"}
    )
    payload = {
        "generated_at": _iso_now(),
        "storage_audit_run_id": audit_id,
        "plan_scope": plan_scope,
        "plan_items": plan_items,
        "totals": {
            "total_items": len(plan_items),
            "compact_item_count": compact_count,
            "cold_archive_item_count": cold_count,
        },
    }

    compaction_plan_run_id = uuid4().hex
    conn = get_conn()
    try:
        conn.execute(
            """
            INSERT INTO compaction_plan_runs (
                compaction_plan_run_id,
                storage_audit_run_id,
                plan_scope,
                status,
                total_items,
                compact_item_count,
                cold_archive_item_count,
                output_path,
                summary_json,
                completed_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                compaction_plan_run_id,
                audit_id,
                plan_scope,
                "completed",
                len(plan_items),
                compact_count,
                cold_count,
                output_path,
                json.dumps(payload, sort_keys=True),
                _iso_now(),
            ),
        )
        conn.commit()
    finally:
        conn.close()

    return (
        Phase7CompactionPlanSummary(
            compaction_plan_run_id=compaction_plan_run_id,
            storage_audit_run_id=audit_id,
            plan_scope=plan_scope,
            total_items=len(plan_items),
            compact_item_count=compact_count,
            cold_archive_item_count=cold_count,
            output_path=output_path,
            status="completed",
        ),
        payload,
    )


def build_phase7_health_summary(*, output_path: str | None = None) -> tuple[Phase7HealthSummary, dict[str, Any]]:
    latest = _load_latest_audit()
    runtime = _load_runtime_rollups()
    now = datetime.now(timezone.utc)
    audit_age_hours = None
    missing_file_count = 0
    status = "no_storage_audit_yet"
    recent_audit = None
    recent_compaction = None
    recent_restore = None

    conn = get_conn()
    try:
        recent_audit = conn.execute(
            """
            SELECT storage_audit_run_id, created_at, total_partitions, total_bytes, missing_file_count
            FROM storage_audit_runs
            ORDER BY created_at DESC
            LIMIT 1
            """
        ).fetchone()
        recent_compaction = conn.execute(
            """
            SELECT compaction_plan_run_id, created_at, total_items, compact_item_count, cold_archive_item_count
            FROM compaction_plan_runs
            ORDER BY created_at DESC
            LIMIT 1
            """
        ).fetchone()
        recent_restore = conn.execute(
            """
            SELECT restore_plan_run_id, created_at, total_items, missing_item_count
            FROM restore_plan_runs
            ORDER BY created_at DESC
            LIMIT 1
            """
        ).fetchone()
    finally:
        conn.close()

    if latest is not None:
        created_at = _parse_iso(latest.get("created_at"))
        audit_age_hours = None if created_at is None else round((now - created_at).total_seconds() / 3600.0, 3)
        missing_file_count = int(latest.get("missing_file_count") or 0)
        if missing_file_count > 0:
            status = "missing_archive_files"
        elif audit_age_hours is not None and audit_age_hours > 48.0:
            status = "stale_storage_audit"
        else:
            status = "ok"

    payload = {
        "status": status,
        "generated_at": _iso_now(),
        "latest_storage_audit": latest,
        "latest_compaction_plan": None
        if recent_compaction is None
        else {
            "compaction_plan_run_id": recent_compaction["compaction_plan_run_id"],
            "created_at": recent_compaction["created_at"],
            "total_items": int(recent_compaction["total_items"] or 0),
            "compact_item_count": int(recent_compaction["compact_item_count"] or 0),
            "cold_archive_item_count": int(recent_compaction["cold_archive_item_count"] or 0),
        },
        "latest_restore_plan": None
        if recent_restore is None
        else {
            "restore_plan_run_id": recent_restore["restore_plan_run_id"],
            "created_at": recent_restore["created_at"],
            "total_items": int(recent_restore["total_items"] or 0),
            "missing_item_count": int(recent_restore["missing_item_count"] or 0),
        },
        "runtime_rollups": runtime,
    }
    return (
        Phase7HealthSummary(
            status=status,
            latest_audit_run_id=None if latest is None else str(latest["storage_audit_run_id"]),
            audit_age_hours=audit_age_hours,
            recent_alert_count=int(runtime["last_24h_alert_count"]),
            recent_shadow_score_count=int(runtime["last_24h_shadow_score_count"]),
            missing_file_count=missing_file_count,
            output_path=output_path,
        ),
        payload,
    )


def build_restore_plan(
    *,
    start: str,
    end: str,
    restore_scope: str = "historical_window",
    storage_audit_run_id: str | None = None,
    output_path: str | None = None,
) -> tuple[Phase7RestorePlanSummary, dict[str, Any]]:
    latest = _load_latest_audit()
    if latest is None and storage_audit_run_id is None:
        raise ValueError("No storage audit exists yet. Run run_phase7_storage_audit.py first.")
    audit_id = storage_audit_run_id or str((latest or {})["storage_audit_run_id"])
    decisions = _load_tiering_decisions(audit_id)
    start_dt = _parse_iso(start)
    end_dt = _parse_iso(end)
    if start_dt is None or end_dt is None or end_dt <= start_dt:
        raise ValueError("A valid restore window is required and end must be later than start.")

    selected: list[dict[str, Any]] = []
    for item in decisions:
        partition_dt = _parse_partition_timestamp(str(item["partition_path"]))
        if partition_dt is None:
            continue
        if start_dt <= partition_dt < end_dt:
            selected.append(
                {
                    **item,
                    "restore_action": (
                        "restore_from_cold_storage"
                        if item["recommended_tier"] in {"cold", "archive_only"}
                        else "load_from_hot_storage"
                    ),
                }
            )

    missing_count = sum(1 for item in selected if not item["file_exists"])
    payload = {
        "generated_at": _iso_now(),
        "restore_scope": restore_scope,
        "storage_audit_run_id": audit_id,
        "window": {"start": start, "end": end},
        "total_items": len(selected),
        "missing_item_count": missing_count,
        "restore_items": selected,
    }

    restore_plan_run_id = uuid4().hex
    conn = get_conn()
    try:
        conn.execute(
            """
            INSERT INTO restore_plan_runs (
                restore_plan_run_id,
                storage_audit_run_id,
                restore_scope,
                requested_start_time,
                requested_end_time,
                status,
                total_items,
                missing_item_count,
                output_path,
                summary_json,
                completed_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                restore_plan_run_id,
                audit_id,
                restore_scope,
                start,
                end,
                "completed" if missing_count == 0 else "missing_partitions_detected",
                len(selected),
                missing_count,
                output_path,
                json.dumps(payload, sort_keys=True),
                _iso_now(),
            ),
        )
        conn.commit()
    finally:
        conn.close()

    return (
        Phase7RestorePlanSummary(
            restore_plan_run_id=restore_plan_run_id,
            storage_audit_run_id=audit_id,
            restore_scope=restore_scope,
            total_items=len(selected),
            missing_item_count=missing_count,
            output_path=output_path,
            status="completed" if missing_count == 0 else "missing_partitions_detected",
        ),
        payload,
    )


def build_integrity_summary(
    *,
    summary_scope: str = "daily",
    output_path: str | None = None,
) -> tuple[Phase7IntegritySummary, dict[str, Any]]:
    latest = _load_latest_audit()
    if latest is None:
        raise ValueError("No storage audit exists yet. Run run_phase7_storage_audit.py first.")

    audit_id = str(latest["storage_audit_run_id"])
    decisions = _load_tiering_decisions(audit_id)
    source_rollups: dict[str, dict[str, Any]] = {}
    compact_count = 0
    cold_count = 0
    missing_count = 0
    for item in decisions:
        source_rollups.setdefault(
            item["source_system"],
            {
                "source_system": item["source_system"],
                "partition_count": 0,
                "total_bytes": 0,
                "missing_file_count": 0,
                "compact_candidate_count": 0,
                "cold_candidate_count": 0,
            },
        )
        rollup = source_rollups[item["source_system"]]
        rollup["partition_count"] += 1
        rollup["total_bytes"] += int(item["byte_count"] or 0)
        if not item["file_exists"]:
            rollup["missing_file_count"] += 1
            missing_count += 1
        if item["recommended_action"] in {"compact_candidate", "compact_and_cold_archive"}:
            rollup["compact_candidate_count"] += 1
            compact_count += 1
        if item["recommended_action"] in {"cold_archive_candidate", "compact_and_cold_archive"}:
            rollup["cold_candidate_count"] += 1
            cold_count += 1

    runtime = _load_runtime_rollups()
    status = "ready"
    if missing_count > 0:
        status = "missing_partitions_detected"
    elif len(decisions) == 0:
        status = "empty_window"

    payload = {
        "generated_at": _iso_now(),
        "summary_scope": summary_scope,
        "storage_audit_run_id": audit_id,
        "status": status,
        "source_rollups": sorted(
            source_rollups.values(),
            key=lambda item: (-int(item["missing_file_count"]), -int(item["total_bytes"]), item["source_system"]),
        ),
        "totals": {
            "source_count": len(source_rollups),
            "total_partitions": len(decisions),
            "missing_file_count": missing_count,
            "compact_candidate_count": compact_count,
            "cold_candidate_count": cold_count,
        },
        "runtime_rollups": runtime,
        "latest_storage_audit": latest,
    }

    integrity_summary_run_id = uuid4().hex
    conn = get_conn()
    try:
        conn.execute(
            """
            INSERT INTO integrity_summary_runs (
                integrity_summary_run_id,
                storage_audit_run_id,
                summary_scope,
                status,
                source_count,
                total_partitions,
                missing_file_count,
                compact_candidate_count,
                cold_candidate_count,
                output_path,
                summary_json,
                completed_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                integrity_summary_run_id,
                audit_id,
                summary_scope,
                status,
                len(source_rollups),
                len(decisions),
                missing_count,
                compact_count,
                cold_count,
                output_path,
                json.dumps(payload, sort_keys=True),
                _iso_now(),
            ),
        )
        conn.commit()
    finally:
        conn.close()

    return (
        Phase7IntegritySummary(
            integrity_summary_run_id=integrity_summary_run_id,
            summary_scope=summary_scope,
            storage_audit_run_id=audit_id,
            source_count=len(source_rollups),
            total_partitions=len(decisions),
            missing_file_count=missing_count,
            compact_candidate_count=compact_count,
            cold_candidate_count=cold_count,
            output_path=output_path,
            status=status,
        ),
        payload,
    )


def build_redundancy_readiness_report(*, output_path: str | None = None) -> dict[str, Any]:
    latest = _load_latest_audit()
    runtime = _load_runtime_rollups()
    blockers: list[str] = []
    readiness_checks: dict[str, Any] = {
        "has_storage_audit": latest is not None,
        "missing_archive_files": None if latest is None else int(latest.get("missing_file_count") or 0),
        "last_24h_alert_count": int(runtime["last_24h_alert_count"]),
        "last_24h_shadow_score_count": int(runtime["last_24h_shadow_score_count"]),
        "active_shadow_model": runtime["active_shadow_model"],
    }

    if latest is None:
        blockers.append("no_storage_audit_yet")
    else:
        created_at = _parse_iso(latest.get("created_at"))
        if created_at is None:
            blockers.append("storage_audit_has_invalid_timestamp")
        else:
            age_hours = (datetime.now(timezone.utc) - created_at).total_seconds() / 3600.0
            readiness_checks["storage_audit_age_hours"] = round(age_hours, 3)
            if age_hours > 48.0:
                blockers.append("storage_audit_is_stale")
        if int(latest.get("missing_file_count") or 0) > 0:
            blockers.append("missing_archive_files_detected")

    if int(runtime["last_24h_shadow_score_count"]) <= 0:
        blockers.append("no_recent_shadow_scores")
    if runtime["active_shadow_model"] is None:
        blockers.append("no_active_shadow_model")

    status = "ready_for_redundancy_design" if not blockers else "not_ready_for_redundancy_design"
    payload = {
        "generated_at": _iso_now(),
        "status": status,
        "blockers": blockers,
        "readiness_checks": readiness_checks,
        "latest_storage_audit": latest,
    }
    if output_path:
        payload["output_path"] = output_path
    return payload
