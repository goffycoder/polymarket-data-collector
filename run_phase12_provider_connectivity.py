from __future__ import annotations

import argparse
import json
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from config.runtime_env import load_runtime_env


DEFAULT_OUTPUT = "reports/phase12/provider_connectivity.json"
DEFAULT_USER_AGENT = "polymarket-arb-provider-connectivity/1.0"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Check direct provider connectivity before starting long-running collection."
    )
    parser.add_argument("--env-file", default=".env.runtime", help="Runtime env file to load.")
    parser.add_argument("--timeout-seconds", type=int, default=15, help="Timeout per provider check.")
    parser.add_argument("--output", default=DEFAULT_OUTPUT, help="JSON output path.")
    parser.add_argument("--json", action="store_true", help="Emit JSON report to stdout.")
    return parser


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _http_json(
    *,
    name: str,
    url: str,
    timeout_seconds: int,
    method: str = "GET",
    body: Any | None = None,
) -> dict[str, Any]:
    started_at = _utc_now()
    data = None if body is None else json.dumps(body).encode("utf-8")
    headers = {
        "Accept": "application/json, text/plain, */*",
        "User-Agent": DEFAULT_USER_AGENT,
    }
    if data is not None:
        headers["Content-Type"] = "application/json"
    request = urllib.request.Request(url, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(request, timeout=max(1, timeout_seconds)) as response:
            raw = response.read()
            decoded = raw.decode("utf-8", errors="replace")
            parsed = json.loads(decoded) if decoded else None
            return {
                "name": name,
                "url": url,
                "method": method,
                "ok": 200 <= int(response.status) < 300,
                "http_status": int(response.status),
                "started_at": started_at,
                "finished_at": _utc_now(),
                "response_shape": type(parsed).__name__,
                "response_preview": decoded[:300],
            }
    except urllib.error.HTTPError as exc:
        preview = exc.read().decode("utf-8", errors="replace")[:300]
        return {
            "name": name,
            "url": url,
            "method": method,
            "ok": False,
            "http_status": int(exc.code),
            "started_at": started_at,
            "finished_at": _utc_now(),
            "error_type": type(exc).__name__,
            "error_message": preview or str(exc),
        }
    except Exception as exc:
        return {
            "name": name,
            "url": url,
            "method": method,
            "ok": False,
            "http_status": None,
            "started_at": started_at,
            "finished_at": _utc_now(),
            "error_type": type(exc).__name__,
            "error_message": str(exc),
        }


def _load_sample_market() -> dict[str, str | None]:
    from database.db_manager import get_conn

    conn = get_conn()
    try:
        row = conn.execute(
            """
            SELECT condition_id, yes_token_id
            FROM markets
            WHERE condition_id IS NOT NULL
              AND yes_token_id IS NOT NULL
            ORDER BY last_updated_at DESC
            LIMIT 1
            """
        ).fetchone()
    finally:
        conn.close()
    if row is None:
        return {"condition_id": None, "token_id": None}
    return {
        "condition_id": row["condition_id"],
        "token_id": row["yes_token_id"],
    }


def build_payload(*, env_file: str, timeout_seconds: int) -> dict[str, Any]:
    env_result = load_runtime_env(env_file or None, override=True)

    from database.db_manager import apply_schema

    apply_schema()
    sample = _load_sample_market()
    checks = [
        _http_json(
            name="gamma_events",
            url="https://gamma-api.polymarket.com/events?"
            + urllib.parse.urlencode({"active": "true", "closed": "false", "limit": 1}),
            timeout_seconds=timeout_seconds,
        ),
        _http_json(
            name="gamma_markets",
            url="https://gamma-api.polymarket.com/markets?"
            + urllib.parse.urlencode({"active": "true", "closed": "false", "limit": 1}),
            timeout_seconds=timeout_seconds,
        ),
        _http_json(
            name="clob_time",
            url="https://clob.polymarket.com/time",
            timeout_seconds=timeout_seconds,
        ),
        _http_json(
            name="data_api_trades",
            url="https://data-api.polymarket.com/trades?"
            + urllib.parse.urlencode(
                {
                    "market": sample.get("condition_id") or "",
                    "limit": 1,
                }
            ),
            timeout_seconds=timeout_seconds,
        ),
    ]
    if sample.get("token_id"):
        checks.append(
            _http_json(
                name="clob_book",
                url="https://clob.polymarket.com/book?"
                + urllib.parse.urlencode({"token_id": sample["token_id"]}),
                timeout_seconds=timeout_seconds,
            )
        )

    ok_count = sum(1 for check in checks if check["ok"])
    required = {"gamma_events", "gamma_markets", "clob_time", "data_api_trades"}
    failed_required = sorted(check["name"] for check in checks if check["name"] in required and not check["ok"])
    status = "healthy" if not failed_required else "blocked"
    return {
        "generated_at": _utc_now(),
        "status": status,
        "ok_count": ok_count,
        "check_count": len(checks),
        "failed_required_checks": failed_required,
        "env_file": None if env_result.env_file is None else str(env_result.env_file),
        "sample_market": sample,
        "checks": checks,
        "next_action": (
            "Provider connectivity is good enough for a fresh live collection smoke."
            if status == "healthy"
            else "Fix network/VPN/DNS/provider blocking before claiming 24/7 fresh collection readiness."
        ),
    }


def main() -> int:
    args = build_parser().parse_args()
    payload = build_payload(env_file=args.env_file, timeout_seconds=max(1, args.timeout_seconds))
    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    if args.json:
        print(json.dumps(payload, indent=2, sort_keys=True))
    else:
        print(f"Provider connectivity: {payload['status']} ({payload['ok_count']}/{payload['check_count']})")
        print(f"Failed required checks: {', '.join(payload['failed_required_checks']) or 'none'}")
        print(f"Report: {output_path}")
    return 0 if payload["status"] == "healthy" else 2


if __name__ == "__main__":
    raise SystemExit(main())
