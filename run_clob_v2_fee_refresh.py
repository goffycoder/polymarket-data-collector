from __future__ import annotations

import argparse
import json
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from hashlib import sha1
from pathlib import Path
from typing import Any

from config.runtime_env import load_runtime_env
from config.settings import CLOB_API_URL
from database.db_manager import apply_schema, get_conn


REPORT_DIR = Path("reports/phase12")


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _iso(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat()


def _safe_float(value: Any) -> float | None:
    try:
        if value is None or value == "":
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _http_json(url: str, timeout_seconds: int) -> tuple[bool, Any, str | None]:
    request = urllib.request.Request(url, headers={"User-Agent": "polymarket-arb-clob-v2-fees/1.0"})
    try:
        with urllib.request.urlopen(request, timeout=timeout_seconds) as response:
            return True, json.loads(response.read().decode("utf-8")), None
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")[:500]
        return False, None, f"http_{exc.code}: {detail}"
    except (urllib.error.URLError, TimeoutError, json.JSONDecodeError) as exc:
        return False, None, str(exc)


def _load_tokens(limit_markets: int) -> list[dict[str, str | None]]:
    conn = get_conn()
    try:
        rows = conn.execute(
            """
            SELECT
                market_id,
                condition_id,
                question,
                yes_token_id,
                no_token_id,
                volume,
                last_updated_at
            FROM markets
            WHERE COALESCE(status, '') <> 'closed'
              AND COALESCE(enable_order_book, 0) = 1
              AND condition_id IS NOT NULL
              AND condition_id <> ''
              AND (
                (yes_token_id IS NOT NULL AND yes_token_id <> '')
                OR (no_token_id IS NOT NULL AND no_token_id <> '')
              )
            ORDER BY COALESCE(volume, 0) DESC, COALESCE(last_updated_at, '') DESC
            LIMIT ?
            """,
            (limit_markets,),
        ).fetchall()
    finally:
        conn.close()

    tokens: list[dict[str, str | None]] = []
    seen: set[str] = set()
    for row in rows:
        for outcome_side, token_key in (("YES", "yes_token_id"), ("NO", "no_token_id")):
            token_id = str(row[token_key] or "").strip()
            if not token_id or token_id in seen:
                continue
            seen.add(token_id)
            tokens.append(
                {
                    "token_id": token_id,
                    "market_id": row["market_id"],
                    "condition_id": row["condition_id"],
                    "outcome_side": outcome_side,
                    "question": row["question"],
                }
            )
    return tokens


def _fee_rate_id(token_id: str, captured_at: str) -> str:
    return sha1(f"{token_id}:{captured_at}".encode("utf-8")).hexdigest()


def _build_row(token: dict[str, str | None], captured_at: str, timeout_seconds: int) -> dict[str, Any]:
    query = urllib.parse.urlencode({"token_id": token["token_id"]})
    url = f"{CLOB_API_URL.rstrip('/')}/fee-rate?{query}"
    ok, payload, error = _http_json(url, timeout_seconds)
    if not isinstance(payload, dict):
        payload = {} if payload is None else {"value": payload}
    status = "ok" if ok else "error"
    return {
        "fee_rate_id": _fee_rate_id(str(token["token_id"]), captured_at),
        "token_id": token["token_id"],
        "market_id": token["market_id"],
        "condition_id": token["condition_id"],
        "outcome_side": token["outcome_side"],
        "captured_at": captured_at,
        "clob_url": url,
        "base_fee": _safe_float(payload.get("base_fee")),
        "fee_rate_bps": _safe_float(payload.get("fee_rate_bps") or payload.get("feeRateBps")),
        "raw_payload_json": json.dumps(payload, sort_keys=True),
        "status": status,
        "error": error,
        "updated_at": captured_at,
    }


def _upsert_rows(rows: list[dict[str, Any]]) -> None:
    if not rows:
        return
    conn = get_conn()
    try:
        conn.executemany(
            """
            INSERT INTO market_fee_rates (
                fee_rate_id,
                token_id,
                market_id,
                condition_id,
                outcome_side,
                captured_at,
                clob_url,
                base_fee,
                fee_rate_bps,
                raw_payload_json,
                status,
                error,
                updated_at
            ) VALUES (
                :fee_rate_id,
                :token_id,
                :market_id,
                :condition_id,
                :outcome_side,
                :captured_at,
                :clob_url,
                :base_fee,
                :fee_rate_bps,
                :raw_payload_json,
                :status,
                :error,
                :updated_at
            )
            ON CONFLICT(fee_rate_id) DO UPDATE SET
                base_fee = excluded.base_fee,
                fee_rate_bps = excluded.fee_rate_bps,
                raw_payload_json = excluded.raw_payload_json,
                status = excluded.status,
                error = excluded.error,
                updated_at = excluded.updated_at
            """,
            rows,
        )
        conn.commit()
    finally:
        conn.close()


def build_payload(*, limit_markets: int, timeout_seconds: int) -> dict[str, Any]:
    apply_schema()
    captured_at = _iso(_utc_now())
    tokens = _load_tokens(limit_markets)
    rows = [_build_row(token, captured_at, timeout_seconds) for token in tokens]
    _upsert_rows(rows)
    ok_rows = [row for row in rows if row["status"] == "ok"]
    error_rows = [row for row in rows if row["status"] != "ok"]
    return {
        "captured_at": captured_at,
        "summary": {
            "status": "ok" if rows and not error_rows else ("partial" if ok_rows else "error"),
            "requested_market_limit": limit_markets,
            "token_count": len(tokens),
            "fee_rows_written": len(rows),
            "ok_count": len(ok_rows),
            "error_count": len(error_rows),
            "clob_url": CLOB_API_URL,
            "trading_order_placement_in_scope": False,
        },
        "sample_rows": rows[:10],
        "errors": [
            {
                "token_id": row["token_id"],
                "market_id": row["market_id"],
                "error": row["error"],
            }
            for row in error_rows[:20]
        ],
        "next_actions": [
            "Use market_fee_rates as disclosure evidence for CLOB V2 fee assumptions.",
            "Do not convert base_fee into simulator PnL until the exact unit semantics are pinned to Polymarket docs.",
            "Refresh before long organic runtime or Phase 5 economic-review packets.",
        ],
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Refresh CLOB V2 fee-rate snapshots for active local markets.")
    parser.add_argument("--env-file", default="", help="Optional runtime env file to load.")
    parser.add_argument("--market-limit", type=int, default=25, help="Number of active order-book markets to sample.")
    parser.add_argument("--timeout-seconds", type=int, default=15, help="HTTP timeout per endpoint call.")
    parser.add_argument(
        "--output",
        default=str(REPORT_DIR / "clob_v2_fee_refresh.json"),
        help="Report output path.",
    )
    parser.add_argument("--json", action="store_true", help="Emit JSON to stdout.")
    return parser


def main() -> int:
    args = build_parser().parse_args()
    load_runtime_env(args.env_file or None, override=True)
    payload = build_payload(limit_markets=args.market_limit, timeout_seconds=args.timeout_seconds)
    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    if args.json:
        print(json.dumps(payload, indent=2, sort_keys=True))
    else:
        summary = payload["summary"]
        print(
            "CLOB V2 fee refresh: "
            f"{summary['status']} ({summary['ok_count']}/{summary['fee_rows_written']} ok)"
        )
        print(f"Report: {output_path}")
    return 0 if payload["summary"]["ok_count"] > 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
