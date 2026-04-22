from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from typing import Any
from uuid import uuid4

from config.settings import PHASE7_PROFILE_STALE_HOURS, PHASE7_PROFILE_TOP_N
from database.db_manager import get_conn
from phase7.reporting import _load_latest_audit
from phase7.storage import _iso_now, _parse_iso


@dataclass(slots=True)
class Phase7BottleneckInventorySummary:
    service_profile_run_id: str
    profile_scope: str
    service_count: int
    bottleneck_count: int
    failure_risk_count: int
    output_path: str | None
    status: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def _freshness_hours(value: str | None) -> float | None:
    parsed = _parse_iso(value)
    if parsed is None:
        return None
    return round((datetime.now(timezone.utc) - parsed).total_seconds() / 3600.0, 3)


def _load_runtime_profile() -> dict[str, Any]:
    conn = get_conn()
    try:
        checkpoint_rows = conn.execute(
            """
            SELECT source_system, MAX(updated_at) AS latest_updated_at, COUNT(*) AS row_count
            FROM detector_checkpoints
            GROUP BY source_system
            ORDER BY source_system ASC
            """
        ).fetchall()
        raw_rows = conn.execute(
            """
            SELECT source_system, MAX(last_captured_at) AS latest_captured_at, COUNT(*) AS partition_count
            FROM raw_archive_manifests
            GROUP BY source_system
            ORDER BY source_system ASC
            """
        ).fetchall()
        detector_rows = conn.execute(
            """
            SELECT source_system, MAX(last_captured_at) AS latest_captured_at, COUNT(*) AS partition_count
            FROM detector_input_manifests
            GROUP BY source_system
            ORDER BY source_system ASC
            """
        ).fetchall()
        day_ago = (datetime.now(timezone.utc) - timedelta(days=1)).isoformat()
        candidate_count = conn.execute(
            "SELECT COUNT(*) AS count FROM signal_candidates WHERE trigger_time >= ?",
            (day_ago,),
        ).fetchone()
        alert_count = conn.execute(
            "SELECT COUNT(*) AS count FROM alerts WHERE created_at >= ?",
            (day_ago,),
        ).fetchone()
        shadow_count = conn.execute(
            "SELECT COUNT(*) AS count FROM shadow_model_scores WHERE created_at >= ?",
            (day_ago,),
        ).fetchone()
        active_shadow = conn.execute(
            """
            SELECT model_version, model_name, deployed_at, created_at
            FROM model_registry
            WHERE shadow_enabled = 1
            ORDER BY COALESCE(deployed_at, created_at) DESC
            LIMIT 1
            """
        ).fetchone()
    finally:
        conn.close()

    return {
        "detector_checkpoints": [
            {
                "source_system": row["source_system"],
                "latest_updated_at": row["latest_updated_at"],
                "freshness_hours": _freshness_hours(row["latest_updated_at"]),
                "row_count": int(row["row_count"] or 0),
            }
            for row in checkpoint_rows
        ],
        "raw_archives": [
            {
                "source_system": row["source_system"],
                "latest_captured_at": row["latest_captured_at"],
                "freshness_hours": _freshness_hours(row["latest_captured_at"]),
                "partition_count": int(row["partition_count"] or 0),
            }
            for row in raw_rows
        ],
        "detector_inputs": [
            {
                "source_system": row["source_system"],
                "latest_captured_at": row["latest_captured_at"],
                "freshness_hours": _freshness_hours(row["latest_captured_at"]),
                "partition_count": int(row["partition_count"] or 0),
            }
            for row in detector_rows
        ],
        "last_24h_candidate_count": int(candidate_count["count"] or 0) if candidate_count else 0,
        "last_24h_alert_count": int(alert_count["count"] or 0) if alert_count else 0,
        "last_24h_shadow_score_count": int(shadow_count["count"] or 0) if shadow_count else 0,
        "active_shadow_model": None
        if active_shadow is None
        else {
            "model_version": active_shadow["model_version"],
            "model_name": active_shadow["model_name"],
            "deployed_at": active_shadow["deployed_at"],
            "created_at": active_shadow["created_at"],
        },
    }


def build_bottleneck_inventory(
    *,
    profile_scope: str = "default",
    output_path: str | None = None,
) -> tuple[Phase7BottleneckInventorySummary, dict[str, Any]]:
    latest_audit = _load_latest_audit()
    runtime = _load_runtime_profile()
    stale_hours = float(PHASE7_PROFILE_STALE_HOURS)

    services: list[dict[str, Any]] = []
    bottlenecks: list[dict[str, Any]] = []
    failure_risks: list[dict[str, Any]] = []

    for item in runtime["detector_checkpoints"]:
        freshness = item["freshness_hours"]
        status = "healthy"
        if freshness is None or freshness > stale_hours:
            status = "stale"
            bottlenecks.append(
                {
                    "service": f"detector_checkpoint:{item['source_system']}",
                    "severity": "high",
                    "reason": "detector checkpoint freshness is stale",
                    "freshness_hours": freshness,
                }
            )
        services.append(
            {
                "service": f"detector_checkpoint:{item['source_system']}",
                "category": "phase3_runtime",
                "status": status,
                "freshness_hours": freshness,
                "row_count": item["row_count"],
            }
        )

    for label, key in (("raw_archive", "raw_archives"), ("detector_input", "detector_inputs")):
        for item in runtime[key]:
            freshness = item["freshness_hours"]
            status = "healthy"
            if freshness is None or freshness > stale_hours:
                status = "stale"
                failure_risks.append(
                    {
                        "service": f"{label}:{item['source_system']}",
                        "severity": "medium",
                        "reason": f"{label} freshness is stale",
                        "freshness_hours": freshness,
                    }
                )
            services.append(
                {
                    "service": f"{label}:{item['source_system']}",
                    "category": "archive_capture",
                    "status": status,
                    "freshness_hours": freshness,
                    "partition_count": item["partition_count"],
                }
            )

    if latest_audit is None:
        failure_risks.append(
            {
                "service": "storage_audit",
                "severity": "high",
                "reason": "no storage audit has been run yet",
            }
        )
    else:
        missing_file_count = int(latest_audit.get("missing_file_count") or 0)
        services.append(
            {
                "service": "storage_audit",
                "category": "archive_integrity",
                "status": "degraded" if missing_file_count > 0 else "healthy",
                "missing_file_count": missing_file_count,
                "created_at": latest_audit.get("created_at"),
            }
        )
        if missing_file_count > 0:
            failure_risks.append(
                {
                    "service": "storage_audit",
                    "severity": "high",
                    "reason": "manifest-backed files are missing on disk",
                    "missing_file_count": missing_file_count,
                }
            )

    if runtime["active_shadow_model"] is None:
        bottlenecks.append(
            {
                "service": "phase6_shadow",
                "severity": "medium",
                "reason": "no active shadow model is registered",
            }
        )
    services.append(
        {
            "service": "phase6_shadow",
            "category": "ml_shadow",
            "status": "healthy" if runtime["active_shadow_model"] is not None else "degraded",
            "last_24h_shadow_score_count": runtime["last_24h_shadow_score_count"],
            "active_shadow_model": runtime["active_shadow_model"],
        }
    )
    if int(runtime["last_24h_shadow_score_count"]) <= 0:
        bottlenecks.append(
            {
                "service": "phase6_shadow",
                "severity": "medium",
                "reason": "no shadow scores were logged in the last 24 hours",
            }
        )

    services.append(
        {
            "service": "phase4_alerting",
            "category": "alert_pipeline",
            "status": "healthy" if int(runtime["last_24h_alert_count"]) > 0 else "quiet",
            "last_24h_alert_count": runtime["last_24h_alert_count"],
        }
    )
    services.append(
        {
            "service": "phase3_candidates",
            "category": "detector_output",
            "status": "healthy" if int(runtime["last_24h_candidate_count"]) > 0 else "quiet",
            "last_24h_candidate_count": runtime["last_24h_candidate_count"],
        }
    )

    prioritized_bottlenecks = sorted(
        bottlenecks,
        key=lambda item: (0 if item["severity"] == "high" else 1, item["service"]),
    )[: int(PHASE7_PROFILE_TOP_N)]
    prioritized_failure_risks = sorted(
        failure_risks,
        key=lambda item: (0 if item["severity"] == "high" else 1, item["service"]),
    )[: int(PHASE7_PROFILE_TOP_N)]

    status = "ready"
    if prioritized_failure_risks:
        status = "failure_risks_detected"
    elif prioritized_bottlenecks:
        status = "bottlenecks_detected"

    payload = {
        "generated_at": _iso_now(),
        "profile_scope": profile_scope,
        "status": status,
        "stale_hours_threshold": stale_hours,
        "service_count": len(services),
        "services": sorted(services, key=lambda item: item["service"]),
        "prioritized_bottlenecks": prioritized_bottlenecks,
        "prioritized_failure_risks": prioritized_failure_risks,
        "runtime_rollups": {
            "last_24h_candidate_count": runtime["last_24h_candidate_count"],
            "last_24h_alert_count": runtime["last_24h_alert_count"],
            "last_24h_shadow_score_count": runtime["last_24h_shadow_score_count"],
        },
    }

    service_profile_run_id = uuid4().hex
    conn = get_conn()
    try:
        conn.execute(
            """
            INSERT INTO service_profile_runs (
                service_profile_run_id,
                profile_scope,
                status,
                service_count,
                bottleneck_count,
                failure_risk_count,
                output_path,
                summary_json,
                completed_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                service_profile_run_id,
                profile_scope,
                status,
                len(services),
                len(prioritized_bottlenecks),
                len(prioritized_failure_risks),
                output_path,
                json.dumps(payload, sort_keys=True),
                _iso_now(),
            ),
        )
        conn.commit()
    finally:
        conn.close()

    return (
        Phase7BottleneckInventorySummary(
            service_profile_run_id=service_profile_run_id,
            profile_scope=profile_scope,
            service_count=len(services),
            bottleneck_count=len(prioritized_bottlenecks),
            failure_risk_count=len(prioritized_failure_risks),
            output_path=output_path,
            status=status,
        ),
        payload,
    )
