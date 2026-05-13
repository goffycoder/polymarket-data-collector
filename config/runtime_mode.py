from __future__ import annotations

from typing import Any


CANONICAL_V1_OPERATING_MODE = "rule_based_plus_shadow_ml"
CANONICAL_DEFAULT_RUNTIME_PROFILE = "alert_live"
PHASE4_RUNTIME_DECISION = "canonical_live_default"
PHASE6_RUNTIME_DECISION = "optional_shadow_live_extension"
ALERT_AUTHORITY = "phase4_rule_based"
ML_AUTHORITY = "phase6_shadow_only"


def classify_configured_runtime_profile(settings: Any) -> str:
    phase3_enabled = bool(getattr(settings, "ENABLE_PHASE3_DETECTOR", False))
    phase4_enabled = bool(getattr(settings, "ENABLE_PHASE4_RUNTIME", False))
    phase6_enabled = bool(getattr(settings, "ENABLE_PHASE6_LIVE_RUNTIME", False))
    collector_only_override = bool(getattr(settings, "ALLOW_COLLECTOR_ONLY_RUNTIME", False))

    if not phase3_enabled:
        return "collector_only" if collector_only_override else "phase3_disabled"
    if phase6_enabled and phase4_enabled:
        return "shadow_live"
    if phase4_enabled:
        return "alert_live"
    return "detector_live"


def classify_observed_runtime_profile(
    *,
    phase3_status: dict[str, Any] | None = None,
    phase4_status: dict[str, Any] | None = None,
    phase6_status: dict[str, Any] | None = None,
) -> str:
    phase3_status = phase3_status or {}
    phase4_status = phase4_status or {}
    phase6_status = phase6_status or {}

    if int(phase6_status.get("shadow_score_count_recent") or 0) > 0:
        return "shadow_live"
    if (
        int(phase4_status.get("alert_count_recent") or 0) > 0
        or int(phase4_status.get("evidence_query_count_recent") or 0) > 0
        or int(phase4_status.get("delivery_attempt_count_recent") or 0) > 0
    ):
        return "alert_live"
    if (
        int(phase3_status.get("checkpoint_count_recent") or 0) > 0
        or int(phase3_status.get("candidate_count_recent") or 0) > 0
    ):
        return "detector_live"
    if int(phase3_status.get("checkpoint_count") or 0) > 0 or int(phase3_status.get("candidate_count_total") or 0) > 0:
        return "historical_only"
    return "no_recent_live_activity"


def build_runtime_decision_summary(
    *,
    settings: Any,
    phase3_status: dict[str, Any] | None = None,
    phase4_status: dict[str, Any] | None = None,
    phase6_status: dict[str, Any] | None = None,
) -> dict[str, Any]:
    configured_profile = classify_configured_runtime_profile(settings)
    observed_profile = classify_observed_runtime_profile(
        phase3_status=phase3_status,
        phase4_status=phase4_status,
        phase6_status=phase6_status,
    )

    return {
        "canonical_v1_operating_mode": CANONICAL_V1_OPERATING_MODE,
        "canonical_default_runtime_profile": CANONICAL_DEFAULT_RUNTIME_PROFILE,
        "phase4_runtime_decision": PHASE4_RUNTIME_DECISION,
        "phase6_runtime_decision": PHASE6_RUNTIME_DECISION,
        "alert_authority": ALERT_AUTHORITY,
        "ml_authority": ML_AUTHORITY,
        "configured_runtime_profile": configured_profile,
        "observed_recent_runtime_profile": observed_profile,
    }
