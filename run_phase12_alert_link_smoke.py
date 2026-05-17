from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from database.db_manager import apply_schema
from phase4.alerts import NoopDeliveryChannel, Phase4AlertWorker, TelegramDeliveryChannel, DiscordDeliveryChannel
from phase4.repository import Phase4Repository


DEFAULT_OUTPUT = "reports/phase12/alert_market_link_smoke.json"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Render one Phase 4 alert and prove it carries clickable Polymarket market links."
    )
    parser.add_argument("--output", default=DEFAULT_OUTPUT, help="JSON output path.")
    parser.add_argument("--json", action="store_true", help="Emit the smoke payload to stdout.")
    return parser


def _render_delivery_previews(alert_payload: dict[str, Any]) -> dict[str, str]:
    outbound_alert = {"alert_id": "phase12-alert-link-smoke", **alert_payload}
    return {
        "telegram": TelegramDeliveryChannel()._render_text(outbound_alert),
        "discord": DiscordDeliveryChannel()._render_text(outbound_alert),
    }


def _validate_payload(payload: dict[str, Any]) -> dict[str, Any]:
    required_fields = [
        "market_url",
        "event_url",
        "market_slug",
        "event_slug",
        "condition_id",
    ]
    missing = [field for field in required_fields if not payload.get(field)]
    market_evidence = payload.get("market_evidence") or {}
    nested_missing = [field for field in required_fields if not market_evidence.get(field)]
    market_url = str(payload.get("market_url") or "")
    return {
        "required_fields": required_fields,
        "missing_top_level_fields": missing,
        "missing_market_evidence_fields": nested_missing,
        "market_url_is_polymarket_event_url": market_url.startswith("https://polymarket.com/event/"),
        "passed": not missing
        and not nested_missing
        and market_url.startswith("https://polymarket.com/event/"),
    }


def main() -> int:
    args = build_parser().parse_args()
    apply_schema()

    repository = Phase4Repository()
    repository.register_workflow_version(
        notes="Phase 12 alert market-link smoke render."
    )
    candidates = repository.pending_candidates(limit=1, include_existing_alerts=True)
    if not candidates:
        raise SystemExit("No signal candidates are available for alert link smoke.")

    worker = Phase4AlertWorker(
        repository=repository,
        channels=[NoopDeliveryChannel(name="phase12_alert_link_smoke")],
    )
    result = worker.process_candidate(candidates[0])
    alert = repository.alert_for_candidate(str(candidates[0]["candidate_id"]))
    if alert is None:
        raise SystemExit("Alert was not created or updated during alert link smoke.")

    rendered_payload = dict(alert["rendered_payload"])
    validation = _validate_payload(rendered_payload)
    delivery_previews = _render_delivery_previews(rendered_payload)
    payload = {
        "status": "passed" if validation["passed"] else "failed",
        "candidate_id": candidates[0]["candidate_id"],
        "alert_id": alert["alert_id"],
        "worker_result": result,
        "validation": validation,
        "market_trace": {
            "market_id": candidates[0].get("market_id"),
            "event_id": candidates[0].get("event_id"),
            "market_slug": rendered_payload.get("market_slug"),
            "event_slug": rendered_payload.get("event_slug"),
            "condition_id": rendered_payload.get("condition_id"),
            "market_url": rendered_payload.get("market_url"),
            "event_url": rendered_payload.get("event_url"),
        },
        "delivery_previews": delivery_previews,
    }

    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    if args.json:
        print(json.dumps(payload, indent=2, sort_keys=True))
    else:
        print(f"Status: {payload['status']}")
        print(f"Alert: {payload['alert_id']}")
        print(f"Market URL: {payload['market_trace']['market_url']}")
        print(f"Report: {output_path}")
    return 0 if validation["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
