from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Protocol

from config.settings import (
    PHASE4_ALERT_ACTIONABLE_THRESHOLD,
    PHASE4_ALERT_CHANNELS,
    PHASE4_ALERT_INFO_THRESHOLD,
    PHASE4_ALERT_WATCH_THRESHOLD,
)
from phase4.repository import Phase4Repository
from utils.logger import get_logger

log = get_logger("phase4_alerts")


def derive_severity(*, severity_score: float | None, confidence_modifier: float | None) -> str:
    score = float(severity_score or 0.0) + float(confidence_modifier or 0.0)
    if score >= PHASE4_ALERT_ACTIONABLE_THRESHOLD:
        return "ACTIONABLE"
    if score >= PHASE4_ALERT_WATCH_THRESHOLD:
        return "WATCH"
    if score >= PHASE4_ALERT_INFO_THRESHOLD:
        return "INFO"
    return "INFO"


def render_alert_payload(
    candidate: dict[str, Any],
    evidence_snapshot: dict[str, Any] | None,
    *,
    severity: str,
) -> dict[str, Any]:
    feature_snapshot = candidate.get("feature_snapshot")
    if isinstance(feature_snapshot, str):
        feature_preview = feature_snapshot
    else:
        feature_preview = feature_snapshot or {}

    evidence_state = (evidence_snapshot or {}).get("evidence_state") or "pending_evidence"
    provider_summary = (evidence_snapshot or {}).get("provider_summary") or {}

    title = (
        candidate.get("event_title")
        or candidate.get("question")
        or candidate.get("event_slug")
        or f"Market {candidate.get('market_id')}"
    )

    return {
        "title": title,
        "severity": severity,
        "what_changed": (
            f"Candidate {candidate.get('candidate_id')} triggered at {candidate.get('trigger_time')}"
        ),
        "why_it_looks_informed": (
            f"Rules={candidate.get('triggering_rules')} "
            f"severity_score={candidate.get('severity_score')}"
        ),
        "market_evidence": {
            "market_id": candidate.get("market_id"),
            "event_id": candidate.get("event_id"),
            "feature_snapshot": feature_preview,
        },
        "public_evidence_state": evidence_state,
        "provider_summary": provider_summary,
        "invalidates_it": "Material contrary public evidence or a weaker replay-reconciled interpretation.",
        "detector_version": candidate.get("detector_version"),
        "feature_schema_version": candidate.get("feature_schema_version"),
    }


class DeliveryChannel(Protocol):
    name: str

    def send(self, alert: dict[str, Any]) -> dict[str, Any]:
        ...


class NoopDeliveryChannel:
    def __init__(self, name: str):
        self.name = name

    def send(self, alert: dict[str, Any]) -> dict[str, Any]:
        return {
            "status": "sent",
            "provider_message_id": f"noop:{self.name}:{alert['alert_id']}",
            "channel": self.name,
        }


def build_default_channels() -> list[DeliveryChannel]:
    return [NoopDeliveryChannel(name=channel_name) for channel_name in PHASE4_ALERT_CHANNELS]


@dataclass(slots=True)
class AlertWorkerSummary:
    candidates_seen: int = 0
    alerts_created: int = 0
    delivery_attempts_written: int = 0

    def to_dict(self) -> dict[str, int]:
        return {
            "candidates_seen": self.candidates_seen,
            "alerts_created": self.alerts_created,
            "delivery_attempts_written": self.delivery_attempts_written,
        }


class Phase4AlertWorker:
    def __init__(
        self,
        *,
        repository: Phase4Repository,
        channels: list[DeliveryChannel] | None = None,
    ):
        self.repository = repository
        self.channels = channels or build_default_channels()
        self.summary = AlertWorkerSummary()

    def process_pending_candidates(self, *, limit: int = 10) -> list[dict[str, Any]]:
        candidates = self.repository.pending_candidates(limit=limit)
        self.summary.candidates_seen += len(candidates)
        outputs: list[dict[str, Any]] = []

        for candidate in candidates:
            outputs.append(self.process_candidate(candidate))

        return outputs

    def process_candidate(self, candidate: dict[str, Any]) -> dict[str, Any]:
        existing_alert = self.repository.alert_for_candidate(str(candidate["candidate_id"]))
        if existing_alert is not None:
            return {
                "candidate_id": candidate["candidate_id"],
                "alert_id": existing_alert["alert_id"],
                "status": "existing",
            }

        evidence_snapshot = self.repository.latest_evidence_snapshot_for_candidate(
            str(candidate["candidate_id"])
        )
        severity = derive_severity(
            severity_score=candidate.get("severity_score"),
            confidence_modifier=(evidence_snapshot or {}).get("confidence_modifier"),
        )
        rendered_payload = render_alert_payload(
            candidate,
            evidence_snapshot,
            severity=severity,
        )
        title = str(rendered_payload["title"])
        suppression_key = str(candidate.get("event_family_id") or candidate.get("market_id") or "")
        alert_id = self.repository.record_alert(
            candidate_id=str(candidate["candidate_id"]),
            severity=severity,
            alert_status="created",
            title=title,
            rendered_payload=rendered_payload,
            detector_version=candidate.get("detector_version"),
            feature_schema_version=candidate.get("feature_schema_version"),
            evidence_snapshot_id=(evidence_snapshot or {}).get("evidence_snapshot_id"),
            suppression_key=suppression_key or None,
            suppression_state="new",
        )
        self.summary.alerts_created += 1

        delivery_results: list[dict[str, Any]] = []
        outbound_alert = {"alert_id": alert_id, **rendered_payload}
        for idx, channel in enumerate(self.channels, start=1):
            response = channel.send(outbound_alert)
            self.repository.record_delivery_attempt(
                alert_id=alert_id,
                delivery_channel=channel.name,
                attempt_number=idx,
                delivery_status=response.get("status", "attempted"),
                provider_message_id=response.get("provider_message_id"),
                request_payload=outbound_alert,
                response_metadata=response,
                error_message=response.get("error_message"),
            )
            self.summary.delivery_attempts_written += 1
            delivery_results.append(response)

        payload = {
            "candidate_id": candidate["candidate_id"],
            "alert_id": alert_id,
            "severity": severity,
            "delivery_channels": [result["channel"] for result in delivery_results],
        }
        log.info(f"Phase 4 alert created: {payload}")
        return payload
