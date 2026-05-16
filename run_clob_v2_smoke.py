from __future__ import annotations

import argparse
import json
import sys
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from config.runtime_env import load_runtime_env
from config.settings import CLOB_API_URL


DATA_API_URL = "https://data-api.polymarket.com"
WS_MARKET_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
CLOB_V2_LIVE_DATE = "2026-04-28"


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _http_json(url: str, timeout_seconds: int) -> tuple[bool, Any, str | None]:
    request = urllib.request.Request(url, headers={"User-Agent": "polymarket-arb-clob-v2-smoke/1.0"})
    try:
        with urllib.request.urlopen(request, timeout=timeout_seconds) as response:
            body = response.read().decode("utf-8")
            return True, json.loads(body), None
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")[:500]
        return False, None, f"http_{exc.code}: {detail}"
    except (urllib.error.URLError, TimeoutError, json.JSONDecodeError) as exc:
        return False, None, str(exc)


def _pick_market() -> dict[str, str | None]:
    from database.db_manager import apply_schema, get_conn

    apply_schema()
    conn = get_conn()
    row = conn.execute(
        """
        SELECT market_id, condition_id, yes_token_id, no_token_id, question, volume, last_updated_at
        FROM markets
        WHERE condition_id IS NOT NULL
          AND condition_id <> ''
          AND COALESCE(status, '') <> 'closed'
          AND (
            (yes_token_id IS NOT NULL AND yes_token_id <> '')
            OR (no_token_id IS NOT NULL AND no_token_id <> '')
          )
        ORDER BY
          COALESCE(accepts_orders, 0) DESC,
          COALESCE(enable_order_book, 0) DESC,
          COALESCE(volume, 0) DESC,
          COALESCE(last_updated_at, '') DESC
        LIMIT 1
        """
    ).fetchone()
    conn.close()
    if row is None:
        return {
            "market_id": None,
            "condition_id": None,
            "token_id": None,
            "question": None,
        }
    token_id = row["yes_token_id"] or row["no_token_id"]
    return {
        "market_id": row["market_id"],
        "condition_id": row["condition_id"],
        "token_id": str(token_id) if token_id is not None else None,
        "question": row["question"],
    }


def _check_clob_time(timeout_seconds: int) -> dict[str, Any]:
    url = f"{CLOB_API_URL.rstrip('/')}/time"
    ok, data, error = _http_json(url, timeout_seconds)
    parsed_ok = isinstance(data, int) or (isinstance(data, str) and data.isdigit())
    return {
        "url": url,
        "ok": bool(ok and parsed_ok),
        "response_type": type(data).__name__ if ok else None,
        "server_time": data if parsed_ok else None,
        "error": error,
    }


def _check_book(token_id: str | None, timeout_seconds: int) -> dict[str, Any]:
    if not token_id:
        return {"ok": False, "skipped": True, "reason": "no_token_id_available"}
    query = urllib.parse.urlencode({"token_id": token_id})
    url = f"{CLOB_API_URL.rstrip('/')}/book?{query}"
    ok, data, error = _http_json(url, timeout_seconds)
    expected_fields = {"market", "asset_id", "bids", "asks"}
    present_fields = sorted(expected_fields.intersection(data.keys())) if isinstance(data, dict) else []
    return {
        "url": url,
        "ok": bool(ok and isinstance(data, dict) and expected_fields.issubset(data.keys())),
        "asset_id": data.get("asset_id") if isinstance(data, dict) else None,
        "market_ref": data.get("market") if isinstance(data, dict) else None,
        "bid_levels": len(data.get("bids") or []) if isinstance(data, dict) else None,
        "ask_levels": len(data.get("asks") or []) if isinstance(data, dict) else None,
        "tick_size": data.get("tick_size") if isinstance(data, dict) else None,
        "last_trade_price": data.get("last_trade_price") if isinstance(data, dict) else None,
        "present_fields": present_fields,
        "error": error,
    }


def _check_fee_rate(token_id: str | None, timeout_seconds: int) -> dict[str, Any]:
    if not token_id:
        return {"ok": False, "skipped": True, "reason": "no_token_id_available"}
    query = urllib.parse.urlencode({"token_id": token_id})
    url = f"{CLOB_API_URL.rstrip('/')}/fee-rate?{query}"
    ok, data, error = _http_json(url, timeout_seconds)
    return {
        "url": url,
        "ok": bool(ok and data is not None),
        "response": data if isinstance(data, (dict, int, float, str)) else None,
        "response_type": type(data).__name__ if ok else None,
        "error": error,
    }


def _check_data_trades(condition_id: str | None, timeout_seconds: int) -> dict[str, Any]:
    if not condition_id:
        return {"ok": False, "skipped": True, "reason": "no_condition_id_available"}
    query = urllib.parse.urlencode({"market": condition_id, "limit": 5})
    url = f"{DATA_API_URL.rstrip('/')}/trades?{query}"
    ok, data, error = _http_json(url, timeout_seconds)
    sample = data[0] if isinstance(data, list) and data else {}
    return {
        "url": url,
        "ok": bool(ok and isinstance(data, list)),
        "row_count": len(data) if isinstance(data, list) else None,
        "sample_has_proxy_wallet": isinstance(sample, dict) and bool(sample.get("proxyWallet")),
        "sample_has_transaction_hash": isinstance(sample, dict) and bool(sample.get("transactionHash")),
        "sample_keys": sorted(sample.keys()) if isinstance(sample, dict) else [],
        "error": error,
    }


def build_smoke_payload(timeout_seconds: int) -> dict[str, Any]:
    selected_market = _pick_market()
    token_id = selected_market.get("token_id")
    condition_id = selected_market.get("condition_id")
    checks = {
        "clob_time": _check_clob_time(timeout_seconds),
        "clob_book": _check_book(token_id, timeout_seconds),
        "clob_fee_rate": _check_fee_rate(token_id, timeout_seconds),
        "data_api_trades": _check_data_trades(condition_id, timeout_seconds),
        "ws_market_subscription_shape": {
            "ok": True,
            "url": WS_MARKET_URL,
            "subscription": {
                "type": "market",
                "assets_ids": [token_id] if token_id else [],
                "custom_feature_enabled": True,
            },
        },
    }
    required_checks = ("clob_time", "clob_book", "data_api_trades", "ws_market_subscription_shape")
    compatible = all(bool(checks[name].get("ok")) for name in required_checks)
    return {
        "checked_at": _utc_now(),
        "clob_v2_live_date": CLOB_V2_LIVE_DATE,
        "selected_market": selected_market,
        "summary": {
            "status": "compatible_public_data_smoke" if compatible else "needs_attention",
            "public_data_endpoints_compatible": compatible,
            "trading_sdk_or_order_signing_in_scope": False,
            "v2_production_clob_url": CLOB_API_URL,
            "data_api_url": DATA_API_URL,
            "ws_market_url": WS_MARKET_URL,
        },
        "checks": checks,
        "next_actions": [
            "Keep public collection on https://clob.polymarket.com and data-api trades.",
            "Do not claim V2 order-placement support; this repo does not currently sign or submit orders.",
            "Replace hardcoded simulator fee assumptions with live fee-rate enrichment before production economic claims.",
            "Refresh this smoke report before long organic runtime windows.",
        ],
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run a CLOB V2 public-data compatibility smoke check.")
    parser.add_argument("--env-file", default="", help="Optional runtime env file to load before DB selection.")
    parser.add_argument("--timeout-seconds", type=int, default=15, help="HTTP timeout per public endpoint.")
    parser.add_argument(
        "--output",
        default="reports/phase12/clob_v2_smoke.json",
        help="JSON output path for the smoke report.",
    )
    parser.add_argument("--json", action="store_true", help="Emit the JSON payload to stdout.")
    return parser


def main() -> int:
    args = build_parser().parse_args()
    load_runtime_env(args.env_file or None, override=True)
    payload = build_smoke_payload(timeout_seconds=args.timeout_seconds)
    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    if args.json:
        print(json.dumps(payload, indent=2, sort_keys=True))
    else:
        status = payload["summary"]["status"]
        print(f"CLOB V2 smoke: {status}")
        print(f"Report: {output_path}")
    return 0 if payload["summary"]["public_data_endpoints_compatible"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
