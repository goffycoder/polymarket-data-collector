from __future__ import annotations

import json
import re
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Protocol

from config.settings import (
    ENABLE_PHASE4_DISCORD,
    ENABLE_PHASE4_TELEGRAM,
    PHASE4_ALERT_ACTIONABLE_THRESHOLD,
    PHASE4_ALERT_ALLOWED_DOMAINS,
    PHASE4_ALERT_CHANNELS,
    PHASE4_ALERT_DELIVERY_MIN_SEVERITY,
    PHASE4_ALERT_EVENT_SUPPRESSION_SECONDS,
    PHASE4_ALERT_EXCLUDED_DOMAINS,
    PHASE4_ALERT_MAX_DELIVERIES_PER_HOUR,
    PHASE4_ALERT_MAX_DELIVERIES_PER_PASS,
    PHASE4_ALERT_MAX_YES_OUTCOME_PROBABILITY,
    PHASE4_ALERT_MIN_YES_OUTCOME_PROBABILITY,
    PHASE4_ALERT_MOVEMENT_RANKING_MIN_CANDIDATES,
    PHASE4_ALERT_MOVEMENT_TOP_N,
    PHASE4_ALERT_INFO_THRESHOLD,
    PHASE4_ALERT_SUPPRESSION_SECONDS,
    PHASE4_ALERT_WATCH_THRESHOLD,
    PHASE4_DISCORD_WEBHOOK_URL,
    PHASE4_TELEGRAM_BOT_TOKEN,
    PHASE4_TELEGRAM_CHAT_ID,
)
from phase4.repository import Phase4Repository
from phase4.timefmt import format_eastern
from utils.logger import get_logger

log = get_logger("phase4_alerts")

WALLET_PATTERN = re.compile(r"\b0x[a-fA-F0-9]{8,}\b")
PUBLIC_HEX_IDENTIFIER_KEYS = {
    "condition_id",
}

SEVERITY_RANK = {
    "INFO": 1,
    "WATCH": 2,
    "ACTIONABLE": 3,
    "CRITICAL": 4,
}
POLYMARKET_EVENT_BASE_URL = "https://polymarket.com/event"


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


def should_resend_alert(
    *,
    previous_alert: dict[str, Any],
    new_payload: dict[str, Any],
    new_severity: str,
) -> bool:
    previous_payload = previous_alert.get("rendered_payload") or {}
    previous_severity = str(previous_alert.get("severity") or "").upper()

    if SEVERITY_RANK.get(new_severity, 0) > SEVERITY_RANK.get(previous_severity, 0):
        return True
    if previous_payload.get("why_it_looks_informed") != new_payload.get("why_it_looks_informed"):
        return True
    return False


def is_delivery_eligible(severity: str) -> bool:
    min_rank = SEVERITY_RANK.get(PHASE4_ALERT_DELIVERY_MIN_SEVERITY, SEVERITY_RANK["ACTIONABLE"])
    return SEVERITY_RANK.get(str(severity or "").upper(), 0) >= min_rank


def _suppression_key(candidate: dict[str, Any]) -> str | None:
    event_key = candidate.get("event_family_id") or candidate.get("event_id") or candidate.get("event_slug")
    if event_key:
        return f"event:{event_key}"
    market_key = candidate.get("market_id") or candidate.get("market_slug")
    if market_key:
        return f"market:{market_key}"
    return None


def _candidate_domain_text(candidate: dict[str, Any]) -> str:
    parts = [
        candidate.get("event_category"),
        candidate.get("event_title"),
        candidate.get("event_slug"),
        candidate.get("question"),
        candidate.get("market_slug"),
    ]
    tags = candidate.get("event_tags") or []
    if isinstance(tags, list):
        parts.extend(str(tag) for tag in tags)
    tag_ids = candidate.get("event_tag_ids") or []
    if isinstance(tag_ids, list):
        parts.extend(str(tag_id) for tag_id in tag_ids)
    return " ".join(str(part).lower() for part in parts if part)


def candidate_domain_filter_reason(candidate: dict[str, Any]) -> str | None:
    domain_text = _candidate_domain_text(candidate)
    if not domain_text:
        return None
    for excluded in PHASE4_ALERT_EXCLUDED_DOMAINS:
        if excluded and excluded in domain_text:
            return f"excluded_domain:{excluded}"
    if not PHASE4_ALERT_ALLOWED_DOMAINS:
        return None
    if any(allowed and allowed in domain_text for allowed in PHASE4_ALERT_ALLOWED_DOMAINS):
        return None
    return "outside_allowed_domains"


def probability_movement_score(candidate: dict[str, Any]) -> float:
    feature_snapshot = candidate.get("feature_snapshot") or {}
    if not isinstance(feature_snapshot, dict):
        return 0.0
    try:
        velocity = abs(float(feature_snapshot.get("probability_velocity") or 0.0))
    except (TypeError, ValueError):
        velocity = 0.0
    try:
        acceleration = abs(float(feature_snapshot.get("probability_acceleration") or 0.0))
    except (TypeError, ValueError):
        acceleration = 0.0
    return velocity + acceleration


def prioritize_candidates_by_probability_movement(
    candidates: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], int]:
    if PHASE4_ALERT_MOVEMENT_TOP_N <= 0:
        return candidates, 0
    if len(candidates) <= PHASE4_ALERT_MOVEMENT_RANKING_MIN_CANDIDATES:
        return candidates, 0

    ranked = sorted(
        candidates,
        key=lambda candidate: (
            probability_movement_score(candidate),
            float(candidate.get("severity_score") or 0.0),
        ),
        reverse=True,
    )
    selected = ranked[:PHASE4_ALERT_MOVEMENT_TOP_N]
    return selected, max(0, len(candidates) - len(selected))


def candidate_outcome_probability(candidate: dict[str, Any], outcome_side: str) -> float | None:
    outcomes = candidate.get("outcomes") or []
    prices = candidate.get("outcome_prices") or []
    if not isinstance(outcomes, list) or not isinstance(prices, list) or not prices:
        return None

    requested = str(outcome_side or "").strip().lower()
    outcome_index = 0
    for idx, outcome in enumerate(outcomes):
        if str(outcome).strip().lower() == requested:
            outcome_index = idx
            break
    if outcome_index >= len(prices):
        return None
    try:
        return float(prices[outcome_index])
    except (TypeError, ValueError):
        return None


def candidate_active_outcome_side(candidate: dict[str, Any]) -> str | None:
    feature_snapshot = candidate.get("feature_snapshot") or {}
    if isinstance(feature_snapshot, dict):
        active_side = str(feature_snapshot.get("active_outcome_side") or "").upper()
        if active_side in {"YES", "NO"}:
            return active_side
        try:
            probability_velocity = float(feature_snapshot.get("probability_velocity") or 0.0)
        except (TypeError, ValueError):
            probability_velocity = 0.0
        if probability_velocity < 0:
            return "NO"
    return "YES"


def candidate_active_outcome_probability(candidate: dict[str, Any]) -> float | None:
    active_side = candidate_active_outcome_side(candidate)
    if not active_side:
        return None
    return candidate_outcome_probability(candidate, active_side)


def candidate_yes_probability(candidate: dict[str, Any]) -> float | None:
    return candidate_outcome_probability(candidate, "YES")


def candidate_probability_filter_reason(candidate: dict[str, Any]) -> str | None:
    min_probability = PHASE4_ALERT_MIN_YES_OUTCOME_PROBABILITY
    max_probability = PHASE4_ALERT_MAX_YES_OUTCOME_PROBABILITY
    if min_probability <= 0 and max_probability >= 1:
        return None
    yes_probability = candidate_yes_probability(candidate)
    if yes_probability is None:
        return None
    if min_probability > 0 and yes_probability < min_probability:
        return (
            "below_min_yes_probability:"
            f"{yes_probability:.4f}<{min_probability:.4f}"
        )
    if max_probability < 1 and yes_probability > max_probability:
        return (
            "above_max_yes_probability:"
            f"{yes_probability:.4f}>{max_probability:.4f}"
        )
    return None


def _slug_url(slug: Any) -> str | None:
    text = str(slug or "").strip().strip("/")
    if not text:
        return None
    return f"{POLYMARKET_EVENT_BASE_URL}/{urllib.parse.quote(text, safe='-')}"


def render_alert_payload(
    candidate: dict[str, Any],
    evidence_snapshot: dict[str, Any] | None,
    *,
    severity: str,
    shadow_score: dict[str, Any] | None = None,
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
    event_url = _slug_url(candidate.get("event_slug"))
    market_url = event_url or _slug_url(candidate.get("market_slug"))

    payload = {
        "title": title,
        "severity": severity,
        "trigger_time_display": format_eastern(candidate.get("trigger_time")),
        "what_changed": (
            f"Candidate {candidate.get('candidate_id')} triggered at {format_eastern(candidate.get('trigger_time'))}"
        ),
        "why_it_looks_informed": (
            f"Rules={candidate.get('triggering_rules')} "
            f"severity_score={candidate.get('severity_score')}"
        ),
        "market_evidence": {
            "market_id": candidate.get("market_id"),
            "event_id": candidate.get("event_id"),
            "condition_id": candidate.get("condition_id"),
            "market_slug": candidate.get("market_slug"),
            "event_slug": candidate.get("event_slug"),
            "market_url": market_url,
            "event_url": event_url,
            "market_slug_url": _slug_url(candidate.get("market_slug")),
            "feature_snapshot": feature_preview,
        },
        "market_url": market_url,
        "event_url": event_url,
        "market_slug_url": _slug_url(candidate.get("market_slug")),
        "market_slug": candidate.get("market_slug"),
        "event_slug": candidate.get("event_slug"),
        "condition_id": candidate.get("condition_id"),
        "active_outcome_side": candidate_active_outcome_side(candidate),
        "active_outcome_probability": candidate_active_outcome_probability(candidate),
        "public_evidence_state": evidence_state,
        "provider_summary": provider_summary,
        "ml_shadow_score": _render_shadow_score_payload(shadow_score),
        "invalidates_it": "Material contrary public evidence or a weaker replay-reconciled interpretation.",
        "detector_version": candidate.get("detector_version"),
        "feature_schema_version": candidate.get("feature_schema_version"),
    }
    return _redact_wallet_identifiers(payload)


def _render_shadow_score_payload(shadow_score: dict[str, Any] | None) -> dict[str, Any]:
    if not shadow_score:
        return {
            "state": "pending",
            "model_version": None,
            "score_label": None,
            "score_value": None,
            "scored_at": None,
        }
    score_value = shadow_score.get("score_value")
    try:
        normalized_score = round(float(score_value), 6)
    except (TypeError, ValueError):
        normalized_score = None
    return {
        "state": "scored",
        "model_version": shadow_score.get("model_version"),
        "score_label": shadow_score.get("score_label"),
        "score_value": normalized_score,
        "scored_at": shadow_score.get("scored_at"),
    }


def _render_shadow_score_text(alert: dict[str, Any]) -> str:
    shadow_score = alert.get("ml_shadow_score") or {}
    if shadow_score.get("state") != "scored":
        return "ML shadow: pending"
    score_value = shadow_score.get("score_value")
    score_text = "unavailable" if score_value is None else f"{float(score_value):.3f}"
    return (
        "ML shadow: "
        f"{shadow_score.get('score_label') or 'unlabeled'} "
        f"{score_text} "
        f"({shadow_score.get('model_version') or 'unknown_model'})"
    )


def _redact_wallet_identifiers(value: Any) -> Any:
    if isinstance(value, str):
        return WALLET_PATTERN.sub("[wallet_redacted]", value)
    if isinstance(value, list):
        return [_redact_wallet_identifiers(item) for item in value]
    if isinstance(value, dict):
        redacted: dict[str, Any] = {}
        for key, item in value.items():
            if str(key) in PUBLIC_HEX_IDENTIFIER_KEYS:
                redacted[key] = item
            else:
                redacted[key] = _redact_wallet_identifiers(item)
        return redacted
    return value


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
                f"Triggered: {alert.get('trigger_time_display')}",
                f"Market: {alert.get('market_url') or 'unavailable'}",
                f"Outcome slug: {alert.get('market_slug') or 'unavailable'}",
                f"Condition: {alert.get('condition_id') or 'unavailable'}",
                alert.get("why_it_looks_informed", ""),
                f"Public evidence: {alert.get('public_evidence_state')}",
                _render_shadow_score_text(alert),
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
                f"Triggered: {alert.get('trigger_time_display')}",
                f"Market: {alert.get('market_url') or 'unavailable'}",
                f"Outcome slug: {alert.get('market_slug') or 'unavailable'}",
                f"Condition: {alert.get('condition_id') or 'unavailable'}",
                alert.get("why_it_looks_informed", ""),
                f"Public evidence: {alert.get('public_evidence_state')}",
                _render_shadow_score_text(alert),
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
    alerts_updated: int = 0
    alerts_suppressed: int = 0
    alerts_delivery_suppressed: int = 0
    alert_candidates_filtered_by_movement: int = 0
    delivery_attempts_written: int = 0

    def to_dict(self) -> dict[str, int]:
        return {
            "candidates_seen": self.candidates_seen,
            "alerts_created": self.alerts_created,
            "alerts_updated": self.alerts_updated,
            "alerts_suppressed": self.alerts_suppressed,
            "alerts_delivery_suppressed": self.alerts_delivery_suppressed,
            "alert_candidates_filtered_by_movement": self.alert_candidates_filtered_by_movement,
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
        self._deliveries_this_pass = 0

    def process_pending_candidates(
        self,
        *,
        limit: int = 10,
        min_trigger_time: str | None = None,
    ) -> list[dict[str, Any]]:
        candidates = self.repository.pending_candidates(
            limit=limit,
            include_existing_alerts=True,
            min_trigger_time=min_trigger_time,
        )
        self.summary.candidates_seen += len(candidates)
        self._deliveries_this_pass = 0
        candidates, filtered_count = prioritize_candidates_by_probability_movement(candidates)
        self.summary.alert_candidates_filtered_by_movement += filtered_count
        outputs: list[dict[str, Any]] = []

        for candidate in candidates:
            outputs.append(self.process_candidate(candidate))

        return outputs

    def process_candidate(self, candidate: dict[str, Any]) -> dict[str, Any]:
        domain_filter_reason = candidate_domain_filter_reason(candidate)
        if domain_filter_reason is not None:
            self.summary.alerts_suppressed += 1
            payload = {
                "candidate_id": candidate["candidate_id"],
                "severity": "UNCLASSIFIED",
                "status": "domain_filtered",
                "delivery_block_reason": domain_filter_reason,
            }
            log.info(f"Phase 4 candidate domain-filtered: {payload}")
            return payload

        probability_filter_reason = candidate_probability_filter_reason(candidate)
        if probability_filter_reason is not None:
            self.summary.alerts_suppressed += 1
            payload = {
                "candidate_id": candidate["candidate_id"],
                "severity": "UNCLASSIFIED",
                "status": "probability_filtered",
                "delivery_block_reason": probability_filter_reason,
            }
            log.info(f"Phase 4 candidate probability-filtered: {payload}")
            return payload

        existing_alert = self.repository.alert_for_candidate(str(candidate["candidate_id"]))
        evidence_snapshot = self.repository.latest_evidence_snapshot_for_candidate(
            str(candidate["candidate_id"])
        )
        shadow_score = self.repository.latest_shadow_score_for_candidate(
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
            shadow_score=shadow_score,
        )
        title = str(rendered_payload["title"])

        if existing_alert is not None:
            resend = should_resend_alert(
                previous_alert=existing_alert,
                new_payload=rendered_payload,
                new_severity=severity,
            )
            updated_status = "updated_resend" if resend else "updated"
            self.repository.update_alert_status(
                alert_id=str(existing_alert["alert_id"]),
                alert_status=updated_status,
                title=title,
                rendered_payload=rendered_payload,
                suppression_state="updated",
                severity=severity,
                evidence_snapshot_id=(evidence_snapshot or {}).get("evidence_snapshot_id"),
            )
            self.summary.alerts_updated += 1

            delivery_results: list[dict[str, Any]] = []
            delivery_block_reason = self._delivery_block_reason(severity=severity) if resend else None
            if resend and delivery_block_reason is None:
                outbound_alert = {"alert_id": existing_alert["alert_id"], **rendered_payload}
                next_attempt_number = self.repository.delivery_attempt_count(str(existing_alert["alert_id"]))
                for offset, channel in enumerate(self.channels, start=1):
                    response = channel.send(outbound_alert)
                    self.repository.record_delivery_attempt(
                        alert_id=str(existing_alert["alert_id"]),
                        delivery_channel=channel.name,
                        attempt_number=next_attempt_number + offset,
                        delivery_status=response.get("status", "attempted"),
                        provider_message_id=response.get("provider_message_id"),
                        request_payload=outbound_alert,
                        response_metadata=response,
                        error_message=response.get("error_message"),
                    )
                    self.summary.delivery_attempts_written += 1
                    delivery_results.append(response)
                self._deliveries_this_pass += 1
            elif resend:
                self.summary.alerts_delivery_suppressed += 1
                self.repository.update_alert_status(
                    alert_id=str(existing_alert["alert_id"]),
                    alert_status=f"{updated_status}_silent",
                    suppression_state=delivery_block_reason,
                )

            payload = {
                "candidate_id": candidate["candidate_id"],
                "alert_id": existing_alert["alert_id"],
                "status": f"{updated_status}_silent" if delivery_block_reason is not None else updated_status,
                "severity": severity,
                "resent": resend and delivery_block_reason is None,
                "delivery_block_reason": delivery_block_reason,
            }
            log.info(f"Phase 4 alert updated: {payload}")
            return payload

        suppression_key = _suppression_key(candidate) or ""
        suppressed_by = None
        if suppression_key:
            suppression_seconds = (
                PHASE4_ALERT_EVENT_SUPPRESSION_SECONDS
                if suppression_key.startswith("event:")
                else PHASE4_ALERT_SUPPRESSION_SECONDS
            )
            recent_alert = self.repository.recent_alert_for_suppression(
                suppression_key=suppression_key,
                since_time=_iso(datetime.now(timezone.utc) - timedelta(seconds=suppression_seconds)),
            )
            if recent_alert is not None:
                recent_severity_rank = SEVERITY_RANK.get(str(recent_alert.get("severity") or "").upper(), 0)
                current_severity_rank = SEVERITY_RANK.get(severity, 0)
                if current_severity_rank <= recent_severity_rank:
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
        delivery_block_reason = self._delivery_block_reason(severity=severity)
        if delivery_block_reason is not None:
            self.summary.alerts_delivery_suppressed += 1
            self.repository.update_alert_status(
                alert_id=alert_id,
                alert_status="created_silent",
                suppression_state=delivery_block_reason,
            )
            payload = {
                "candidate_id": candidate["candidate_id"],
                "alert_id": alert_id,
                "severity": severity,
                "status": "created_silent",
                "delivery_block_reason": delivery_block_reason,
            }
            log.info(f"Phase 4 alert created without delivery: {payload}")
            return payload

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
        self._deliveries_this_pass += 1

        payload = {
            "candidate_id": candidate["candidate_id"],
            "alert_id": alert_id,
            "severity": severity,
            "delivery_channels": [result["channel"] for result in delivery_results],
        }
        log.info(f"Phase 4 alert created: {payload}")
        return payload

    def _delivery_block_reason(self, *, severity: str) -> str | None:
        if not is_delivery_eligible(severity):
            return f"below_delivery_min_severity:{PHASE4_ALERT_DELIVERY_MIN_SEVERITY}"
        if PHASE4_ALERT_MAX_DELIVERIES_PER_PASS > 0 and self._deliveries_this_pass >= PHASE4_ALERT_MAX_DELIVERIES_PER_PASS:
            return f"delivery_budget_exhausted:{PHASE4_ALERT_MAX_DELIVERIES_PER_PASS}"
        if PHASE4_ALERT_MAX_DELIVERIES_PER_HOUR > 0:
            hourly_sent = self.repository.delivery_attempt_count_since(
                since_time=_iso(datetime.now(timezone.utc) - timedelta(hours=1)),
                delivery_channel="telegram",
                delivery_status="sent",
            )
            if hourly_sent >= PHASE4_ALERT_MAX_DELIVERIES_PER_HOUR:
                return f"hourly_delivery_budget_exhausted:{PHASE4_ALERT_MAX_DELIVERIES_PER_HOUR}"
        return None
