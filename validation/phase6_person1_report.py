from __future__ import annotations

from phase6 import Phase6Repository


def build_phase6_person1_report(*, limit: int = 10) -> dict:
    status = Phase6Repository().build_registry_status(limit=max(1, limit)).to_dict()
    active = status.get("active_shadow_model")
    recent_scores = status.get("recent_shadow_scores", [])
    return {
        "active_shadow_model": active,
        "recent_models": status.get("recent_models", []),
        "recent_shadow_scores": recent_scores,
        "assessment": {
            "status": "shadow_ready" if active else "no_active_shadow_model",
            "recent_score_count": len(recent_scores),
            "active_model_version": (active or {}).get("model_version"),
        },
    }
