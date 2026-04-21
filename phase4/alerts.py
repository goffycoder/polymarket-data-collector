from __future__ import annotations

import json
import urllib.error
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Protocol

from config.settings import (
    ENABLE_PHASE4_DISCORD,
    ENABLE_PHASE4_TELEGRAM,
    PHASE4_ALERT_ACTIONABLE_THRESHOLD,
    PHASE4_ALERT_CHANNELS,
    PHASE4_ALERT_INFO_THRESHOLD,
    PHASE4_ALERT_SUPPRESSION_SECONDS,
    PHASE4_ALERT_WATCH_THRESHOLD,
    PHASE4_DISCORD_WEBHOOK_URL,
    PHASE4_TELEGRAM_BOT_TOKEN,
    PHASE4_TELEGRAM_CHAT_ID,
)
from phase4.repository import Phase4Repository
from utils.logger import get_logger

log = get_logger("phase4_alerts")

SEVERITY_RANK = {
    "INFO": 1,
    "WATCH": 2,
    "ACTIONABLE": 3,
    "CRITICAL": 4,
}


def _iso(dt: datetime) -> str:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).isoformat()


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


class TelegramDeliveryChannel:
    name = "telegram"

    def __init__(self):
        self.enabled = ENABLE_PHASE4_TELEGRAM
        self.bot_token = PHASE4_TELEGRAM_BOT_TOKEN
        self.chat_id = PHASE4_TELEGRAM_CHAT_ID

    def send(self, alert: dict[str, Any]) -> dict[str, Any]:
        if not self.enabled:
            return {
                "status": "skipped",
                "channel": self.name,
                "reason": "telegram_disabled",
            }
        if not self.bot_token or not self.chat_id:
            return {
                "status": "skipped",
                "channel": self.name,
                "reason": "telegram_not_configured",
            }

        payload = {
            "chat_id": self.chat_id,
            "text": self._render_text(alert),
            "disable_web_page_preview": True,
        }
        url = f"https://api.telegram.org/bot{self.bot_token}/sendMessage"
        return _post_json(url=url, body=payload, channel=self.name)

    def _render_text(self, alert: dict[str, Any]) -> str:
        return "\n".join(
            [
                f"[{alert['severity']}] {alert['title']}",
                alert.get("what_changed", ""),
                alert.get("why_it_looks_informed", ""),
                f"Public evidence: {alert.get('public_evidence_state')}",
                f"Detector: {alert.get('detector_version')} / {alert.get('feature_schema_version')}",
                f"Alert ID: {alert.get('alert_id')}",
            ]
        ).strip()


class DiscordDeliveryChannel:
    name = "discord"

    def __init__(self):
        self.enabled = ENABLE_PHASE4_DISCORD
        self.webhook_url = PHASE4_DISCORD_WEBHOOK_URL

    def send(self, alert: dict[str, Any]) -> dict[str, Any]:
        if not self.enabled:
            return {
                "status": "skipped",
                "channel": self.name,
                "reason": "discord_disabled",
            }
        if not self.webhook_url:
            return {
                "status": "skipped",
                "channel": self.name,
                "reason": "discord_not_configured",
            }

        payload = {
            "content": self._render_text(alert),
        }
        return _post_json(url=self.webhook_url, body=payload, channel=self.name)

    def _render_text(self, alert: dict[str, Any]) -> str:
        return "\n".join(
            [
                f"**[{alert['severity']}] {alert['title']}**",
                alert.get("what_changed", ""),
                alert.get("why_it_looks_informed", ""),
                f"Public evidence: {alert.get('public_evidence_state')}",
                f"Alert ID: {alert.get('alert_id')}",
            ]
        ).strip()


def _post_json(*, url: str, body: dict[str, Any], channel: str) -> dict[str, Any]:
    raw = json.dumps(body).encode("utf-8")
    request = urllib.request.Request(
        url,
        data=raw,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=10) as response:
            response_text = response.read().decode("utf-8", errors="replace")
            return {
                "status": "sent",
                "channel": channel,
                "provider_message_id": response.headers.get("X-Telegram-Bot-Api-Secret-Token")
                or response.headers.get("X-Request-Id")
                or str(response.status),
                "http_status": response.status,
                "response_text_preview": response_text[:300],
            }
    except urllib.error.HTTPError as exc:
        response_text = exc.read().decode("utf-8", errors="replace")
        return {
            "status": "error",
            "channel": channel,
            "http_status": exc.code,
            "error_message": response_text[:300] or str(exc),
        }
    except Exception as exc:
        return {
            "status": "error",
            "channel": channel,
            "error_message": str(exc),
        }


def build_default_channels() -> list[DeliveryChannel]:
    channels: list[DeliveryChannel] = []
    for channel_name in PHASE4_ALERT_CHANNELS:
        if channel_name == "telegram":
            channels.append(TelegramDeliveryChannel())
        elif channel_name == "discord":
            channels.append(DiscordDeliveryChannel())
        else:
            channels.append(NoopDeliveryChannel(name=channel_name))
    return channels


@dataclass(slots=True)
class AlertWorkerSummary:
    candidates_seen: int = 0
    alerts_created: int = 0
    alerts_suppressed: int = 0
    delivery_attempts_written: int = 0

    def to_dict(self) -> dict[str, int]:
        return {
            "candidates_seen": self.candidates_seen,
            "alerts_created": self.alerts_created,
            "alerts_suppressed": self.alerts_suppressed,
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
        suppressed_by = None
        if suppression_key:
            recent_alert = self.repository.recent_alert_for_suppression(
                suppression_key=suppression_key,
                since_time=_iso(datetime.now(timezone.utc) - timedelta(seconds=PHASE4_ALERT_SUPPRESSION_SECONDS)),
            )
            if recent_alert is not None:
                recent_severity_rank = SEVERITY_RANK.get(str(recent_alert.get("severity") or "").upper(), 0)
                current_severity_rank = SEVERITY_RANK.get(severity, 0)
                recent_evidence_state = recent_alert.get("public_evidence_state")
                current_evidence_state = rendered_payload.get("public_evidence_state")
                if current_severity_rank <= recent_severity_rank and current_evidence_state == recent_evidence_state:
                    suppressed_by = str(recent_alert["alert_id"])

        alert_status = "suppressed" if suppressed_by else "created"
        suppression_state = f"suppressed_by:{suppressed_by}" if suppressed_by else "new"
        alert_id = self.repository.record_alert(
            candidate_id=str(candidate["candidate_id"]),
            severity=severity,
            alert_status=alert_status,
            title=title,
            rendered_payload=rendered_payload,
            detector_version=candidate.get("detector_version"),
            feature_schema_version=candidate.get("feature_schema_version"),
            evidence_snapshot_id=(evidence_snapshot or {}).get("evidence_snapshot_id"),
            suppression_key=suppression_key or None,
            suppression_state=suppression_state,
        )
        if suppressed_by:
            self.summary.alerts_suppressed += 1
            payload = {
                "candidate_id": candidate["candidate_id"],
                "alert_id": alert_id,
                "severity": severity,
                "status": "suppressed",
                "suppressed_by": suppressed_by,
            }
            log.info(f"Phase 4 alert suppressed: {payload}")
            return payload

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
