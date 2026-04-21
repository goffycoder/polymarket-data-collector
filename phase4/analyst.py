from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from phase4.repository import Phase4Repository
from phase4.timefmt import format_eastern
from utils.logger import get_logger

log = get_logger("phase4_analyst")

VALID_ANALYST_ACTIONS = {
    "acknowledge",
    "snooze",
    "dismiss",
    "mark_useful",
    "mark_false_positive",
    "add_notes",
}

ACTION_TO_STATUS = {
    "acknowledge": "acknowledged",
    "snooze": "snoozed",
    "dismiss": "dismissed",
    "mark_useful": "useful",
    "mark_false_positive": "false_positive",
    "add_notes": "annotated",
}


def _iso(dt: datetime) -> str:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).isoformat()


@dataclass(slots=True)
class AnalystWorkflowSummary:
    actions_recorded: int = 0

    def to_dict(self) -> dict[str, int]:
        return {"actions_recorded": self.actions_recorded}


class Phase4AnalystWorkflow:
    def __init__(self, *, repository: Phase4Repository):
        self.repository = repository
        self.summary = AnalystWorkflowSummary()

    def record_action(
        self,
        *,
        alert_id: str,
        action_type: str,
        actor: str | None = None,
        notes: str | None = None,
        follow_up_at: str | None = None,
    ) -> dict[str, Any]:
        normalized_action = action_type.strip().lower()
        if normalized_action not in VALID_ANALYST_ACTIONS:
            raise ValueError(
                f"Unsupported analyst action '{action_type}'. Expected one of: {sorted(VALID_ANALYST_ACTIONS)}"
            )

        feedback_id = self.repository.record_analyst_feedback(
            alert_id=alert_id,
            action_type=normalized_action,
            actor=actor,
            notes=notes,
            follow_up_at=follow_up_at,
        )
        self.repository.update_alert_status(
            alert_id=alert_id,
            alert_status=ACTION_TO_STATUS[normalized_action],
            suppression_state=normalized_action,
        )
        self.summary.actions_recorded += 1
        follow_up_value = follow_up_at or _iso(datetime.now(timezone.utc))
        payload = {
            "feedback_id": feedback_id,
            "alert_id": alert_id,
            "action_type": normalized_action,
            "actor": actor,
            "follow_up_at": follow_up_value,
            "follow_up_at_display": format_eastern(follow_up_value),
        }
        log.info(f"Phase 4 analyst action recorded: {payload}")
        return payload
