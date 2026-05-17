from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from phase4.alerts import NoopDeliveryChannel, Phase4AlertWorker


class FakePhase4Repository:
    def __init__(self, candidates: list[dict[str, Any]], *, existing_delivery_attempts: int = 0):
        self.candidates = candidates
        self.alerts_by_candidate: dict[str, dict[str, Any]] = {}
        self.alerts_by_id: dict[str, dict[str, Any]] = {}
        self.delivery_attempts: list[dict[str, Any]] = [
            {
                "delivery_attempt_id": f"existing-delivery-{index}",
                "alert_id": f"existing-alert-{index}",
                "delivery_channel": "telegram",
                "attempt_number": 1,
                "delivery_status": "sent",
            }
            for index in range(1, existing_delivery_attempts + 1)
        ]
        self._next_alert = 0

    def pending_candidates(
        self,
        *,
        limit: int = 10,
        include_existing_alerts: bool = False,
        min_trigger_time: str | None = None,
    ) -> list[dict[str, Any]]:
        rows = self.candidates[:limit]
        if include_existing_alerts:
            return rows
        return [row for row in rows if row["candidate_id"] not in self.alerts_by_candidate]

    def alert_for_candidate(self, candidate_id: str) -> dict[str, Any] | None:
        return self.alerts_by_candidate.get(candidate_id)

    def latest_evidence_snapshot_for_candidate(self, candidate_id: str) -> dict[str, Any] | None:
        return {
            "evidence_snapshot_id": f"evidence-{candidate_id}",
            "evidence_state": "weakly_public",
            "confidence_modifier": 0.0,
            "provider_summary": {"providers": ["fake"]},
        }

    def delivery_attempt_count(self, alert_id: str) -> int:
        return sum(1 for attempt in self.delivery_attempts if attempt["alert_id"] == alert_id)

    def delivery_attempt_count_since(
        self,
        *,
        since_time: str,
        delivery_channel: str | None = None,
        delivery_status: str | None = None,
    ) -> int:
        attempts = self.delivery_attempts
        if delivery_channel:
            attempts = [
                attempt for attempt in attempts if attempt["delivery_channel"] == delivery_channel
            ]
        if delivery_status:
            attempts = [
                attempt for attempt in attempts if attempt["delivery_status"] == delivery_status
            ]
        return len(attempts)

    def recent_alert_for_suppression(self, *, suppression_key: str, since_time: str) -> dict[str, Any] | None:
        for alert in reversed(list(self.alerts_by_id.values())):
            if alert.get("suppression_key") == suppression_key and alert.get("alert_status") != "suppressed":
                return {
                    **alert,
                    "public_evidence_state": (alert.get("rendered_payload") or {}).get("public_evidence_state"),
                }
        return None

    def record_alert(
        self,
        *,
        candidate_id: str,
        severity: str,
        alert_status: str,
        title: str,
        rendered_payload: dict[str, Any],
        detector_version: str | None,
        feature_schema_version: str | None,
        evidence_snapshot_id: str | None,
        suppression_key: str | None,
        suppression_state: str | None,
    ) -> str:
        self._next_alert += 1
        alert_id = f"fake-alert-{self._next_alert}"
        alert = {
            "alert_id": alert_id,
            "candidate_id": candidate_id,
            "severity": severity,
            "alert_status": alert_status,
            "title": title,
            "rendered_payload": rendered_payload,
            "suppression_key": suppression_key,
            "suppression_state": suppression_state,
        }
        self.alerts_by_id[alert_id] = alert
        self.alerts_by_candidate[candidate_id] = alert
        return alert_id

    def update_alert_status(self, *, alert_id: str, alert_status: str, **updates: Any) -> None:
        alert = self.alerts_by_id[alert_id]
        alert["alert_status"] = alert_status
        alert.update(updates)

    def record_delivery_attempt(
        self,
        *,
        alert_id: str,
        delivery_channel: str,
        attempt_number: int,
        delivery_status: str,
        provider_message_id: str | None,
        request_payload: dict[str, Any],
        response_metadata: dict[str, Any],
        error_message: str | None,
    ) -> str:
        attempt_id = f"fake-delivery-{len(self.delivery_attempts) + 1}"
        self.delivery_attempts.append(
            {
                "delivery_attempt_id": attempt_id,
                "alert_id": alert_id,
                "delivery_channel": delivery_channel,
                "attempt_number": attempt_number,
                "delivery_status": delivery_status,
            }
        )
        return attempt_id


def _candidate(
    index: int,
    *,
    event_id: str,
    score: float = 100.0,
    probability_velocity: float | None = None,
    probability_acceleration: float | None = None,
) -> dict[str, Any]:
    velocity = float(probability_velocity if probability_velocity is not None else index / 100.0)
    acceleration = float(
        probability_acceleration if probability_acceleration is not None else index / 1000.0
    )
    return {
        "candidate_id": f"candidate-{index}",
        "market_id": f"market-{index}",
        "event_id": event_id,
        "event_family_id": None,
        "event_slug": f"event-{event_id}",
        "market_slug": f"market-{index}",
        "condition_id": f"condition-{index}",
        "trigger_time": datetime.now(timezone.utc).isoformat(),
        "detector_version": "phase3_detector_v1",
        "feature_schema_version": "phase3_v1",
        "severity_score": score,
        "triggering_rules": ["normalization_smoke"],
        "feature_snapshot": {
            "probability_velocity": velocity,
            "probability_acceleration": acceleration,
        },
        "question": f"Candidate {index}",
        "event_title": f"Event {event_id}",
    }


def _run_case(
    name: str,
    candidates: list[dict[str, Any]],
    *,
    existing_delivery_attempts: int = 0,
) -> dict[str, Any]:
    repository = FakePhase4Repository(
        candidates,
        existing_delivery_attempts=existing_delivery_attempts,
    )
    worker = Phase4AlertWorker(
        repository=repository,  # type: ignore[arg-type]
        channels=[NoopDeliveryChannel("telegram")],
    )
    results = worker.process_pending_candidates(limit=len(candidates))
    return {
        "name": name,
        "results": results,
        "summary": worker.summary.to_dict(),
        "new_delivery_attempts": len(repository.delivery_attempts) - existing_delivery_attempts,
        "total_delivery_attempts": len(repository.delivery_attempts),
        "alert_statuses": [alert["alert_status"] for alert in repository.alerts_by_id.values()],
    }


def main() -> int:
    same_event = _run_case(
        "same_event_suppression",
        [_candidate(index, event_id="shared") for index in range(1, 5)],
    )
    delivery_budget = _run_case(
        "delivery_budget",
        [_candidate(index, event_id=f"event-{index}") for index in range(1, 5)],
    )
    hourly_budget = _run_case(
        "hourly_delivery_budget",
        [_candidate(index, event_id=f"hourly-{index}") for index in range(1, 3)],
        existing_delivery_attempts=6,
    )
    movement_ranking = _run_case(
        "movement_top_7",
        [
            _candidate(
                index,
                event_id=f"movement-{index}",
                probability_velocity=index / 100.0,
                probability_acceleration=index / 1000.0,
            )
            for index in range(1, 10)
        ],
    )
    movement_candidate_ids = [
        result["candidate_id"] for result in movement_ranking["results"]
    ]
    payload = {
        "status": "passed"
        if (
            same_event["new_delivery_attempts"] == 1
            and delivery_budget["new_delivery_attempts"] == 1
            and hourly_budget["new_delivery_attempts"] == 0
            and movement_ranking["summary"]["alert_candidates_filtered_by_movement"] == 2
            and movement_candidate_ids == [
                "candidate-9",
                "candidate-8",
                "candidate-7",
                "candidate-6",
                "candidate-5",
                "candidate-4",
                "candidate-3",
            ]
        )
        else "failed",
        "cases": [same_event, delivery_budget, hourly_budget, movement_ranking],
    }
    output = Path("reports/phase12/alert_normalization_smoke.json")
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    print(json.dumps(payload, indent=2, sort_keys=True))
    return 0 if payload["status"] == "passed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
