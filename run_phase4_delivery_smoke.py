from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from config.runtime_env import load_runtime_env


DEFAULT_OUTPUT = "reports/phase12/delivery_smoke.json"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Smoke-test Phase 4 outbound delivery using a redacted, market-linked alert payload. "
            "If providers are not configured, writes an honest provider-disabled report."
        )
    )
    parser.add_argument("--env-file", default=".env.runtime", help="Runtime env file to load before provider setup.")
    parser.add_argument("--output", default=DEFAULT_OUTPUT, help="JSON output path.")
    parser.add_argument("--json", action="store_true", help="Emit the smoke payload to stdout.")
    return parser


def _iso_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _redact_response(response: dict[str, Any]) -> dict[str, Any]:
    redacted = dict(response)
    if "response_text_preview" in redacted:
        redacted["response_text_preview"] = str(redacted["response_text_preview"])[:300]
    return redacted


def _load_or_create_market_linked_alert() -> tuple[dict[str, Any], dict[str, Any]]:
    from database.db_manager import apply_schema
    from phase4.alerts import NoopDeliveryChannel, Phase4AlertWorker
    from phase4.repository import Phase4Repository

    apply_schema()
    repository = Phase4Repository()
    repository.register_workflow_version(notes="Phase 4 delivery smoke with market-linked alert payload.")

    candidates = repository.pending_candidates(limit=1, include_existing_alerts=True)
    if not candidates:
        raise SystemExit("No signal candidates are available for delivery smoke.")

    worker = Phase4AlertWorker(
        repository=repository,
        channels=[NoopDeliveryChannel(name="phase4_delivery_smoke_seed")],
    )
    worker_result = worker.process_candidate(candidates[0])
    alert = repository.alert_for_candidate(str(candidates[0]["candidate_id"]))
    if alert is None:
        raise SystemExit("Alert was not created or updated during delivery smoke setup.")

    outbound_alert = {"alert_id": alert["alert_id"], **dict(alert["rendered_payload"])}
    return outbound_alert, {
        "candidate": candidates[0],
        "alert": alert,
        "worker_result": worker_result,
    }


def _channel_statuses(alert: dict[str, Any]) -> list[dict[str, Any]]:
    from config import settings
    from phase4.alerts import DiscordDeliveryChannel, TelegramDeliveryChannel

    channels = [
        TelegramDeliveryChannel(),
        DiscordDeliveryChannel(),
    ]
    statuses: list[dict[str, Any]] = []
    for channel in channels:
        enabled = bool(getattr(channel, "enabled", False))
        configured = False
        if channel.name == "telegram":
            configured = bool(settings.PHASE4_TELEGRAM_BOT_TOKEN and settings.PHASE4_TELEGRAM_CHAT_ID)
        elif channel.name == "discord":
            configured = bool(settings.PHASE4_DISCORD_WEBHOOK_URL)

        preview_text = ""
        render_text = getattr(channel, "_render_text", None)
        if callable(render_text):
            preview_text = str(render_text(alert))

        if not enabled or not configured:
            statuses.append(
                {
                    "channel": channel.name,
                    "enabled": enabled,
                    "configured": configured,
                    "sent": False,
                    "status": "provider_disabled" if not enabled else "provider_not_configured",
                    "preview_text": preview_text,
                }
            )
            continue

        response = channel.send(alert)
        statuses.append(
            {
                "channel": channel.name,
                "enabled": enabled,
                "configured": configured,
                "sent": response.get("status") == "sent",
                "status": response.get("status", "unknown"),
                "provider_message_id_present": bool(response.get("provider_message_id")),
                "response": _redact_response(response),
                "preview_text": preview_text,
            }
        )
    return statuses


def main() -> int:
    args = build_parser().parse_args()
    load_runtime_env(args.env_file or None, override=True)

    alert, setup = _load_or_create_market_linked_alert()
    channel_statuses = _channel_statuses(alert)
    telegram_status = next((item for item in channel_statuses if item["channel"] == "telegram"), {})
    discord_status = next((item for item in channel_statuses if item["channel"] == "discord"), {})

    payload = {
        "generated_at": _iso_now(),
        "status": "sent" if any(item.get("sent") for item in channel_statuses) else "provider_disabled",
        "alert_id": alert["alert_id"],
        "candidate_id": setup["candidate"].get("candidate_id"),
        "market_trace": {
            "market_id": setup["candidate"].get("market_id"),
            "event_id": setup["candidate"].get("event_id"),
            "market_slug": alert.get("market_slug"),
            "event_slug": alert.get("event_slug"),
            "condition_id": alert.get("condition_id"),
            "market_url": alert.get("market_url"),
            "event_url": alert.get("event_url"),
            "market_slug_url": alert.get("market_slug_url"),
        },
        "telegram_configured": bool(telegram_status.get("enabled") and telegram_status.get("configured")),
        "telegram_sent": bool(telegram_status.get("sent")),
        "discord_configured": bool(discord_status.get("enabled") and discord_status.get("configured")),
        "discord_sent": bool(discord_status.get("sent")),
        "channels": channel_statuses,
        "notes": [
            "Secrets are never printed or written by this smoke.",
            "A provider_disabled status is an honest non-production-delivery result, not a failed runtime.",
        ],
    }

    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    if args.json:
        print(json.dumps(payload, indent=2, sort_keys=True))
    else:
        print(f"Status: {payload['status']}")
        print(f"Telegram configured: {payload['telegram_configured']} sent: {payload['telegram_sent']}")
        print(f"Discord configured: {payload['discord_configured']} sent: {payload['discord_sent']}")
        print(f"Market URL: {payload['market_trace']['market_url']}")
        print(f"Report: {output_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
