from __future__ import annotations

from typing import Any

from phase7.handoff import build_phase7_person2_handoff
from validation.phase7_person1_report import build_phase7_person1_report


def build_phase7_gate7_report() -> dict[str, Any]:
    report = build_phase7_person1_report()
    handoff = build_phase7_person2_handoff()
    assessment = report.get("assessment", {})
    blockers = list(assessment.get("blockers", []))
    status = "gate7_ready" if not blockers else "gate7_followup_required"
    return {
        "phase7_person1_report": report,
        "phase7_person2_handoff": handoff,
        "gate7_assessment": {
            "status": status,
            "blockers": blockers,
            "ready_for_push": not blockers,
            "handoff_ready": True,
        },
    }
