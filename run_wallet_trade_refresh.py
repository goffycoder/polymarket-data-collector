from __future__ import annotations

import argparse
import asyncio
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from uuid import uuid4

from collectors.trades_collector import collect_trades
from collectors.universe_selector import load_universe_policy, select_runtime_universe
from config.runtime_env import load_runtime_env
from database.db_manager import apply_schema, get_conn


REPORT_DIR = Path("reports/phase12")


def _iso_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _count_wallet_linked_trades(conn) -> dict[str, int]:
    row = conn.execute(
        """
        SELECT
            COUNT(*) AS total,
            SUM(CASE WHEN proxy_wallet IS NOT NULL AND TRIM(proxy_wallet) != '' THEN 1 ELSE 0 END) AS wallet_linked,
            SUM(CASE WHEN transaction_hash IS NOT NULL AND TRIM(transaction_hash) != '' THEN 1 ELSE 0 END) AS transaction_linked,
            SUM(CASE WHEN source = 'clob' THEN 1 ELSE 0 END) AS clob_rows,
            SUM(CASE WHEN source = 'ws' THEN 1 ELSE 0 END) AS ws_rows
        FROM trades
        """
    ).fetchone()
    return {
        "total": int(row["total"] or 0),
        "wallet_linked": int(row["wallet_linked"] or 0),
        "transaction_linked": int(row["transaction_linked"] or 0),
        "clob_rows": int(row["clob_rows"] or 0),
        "ws_rows": int(row["ws_rows"] or 0),
    }


def _select_markets(*, limit: int, tier: int) -> list[Any]:
    conn = get_conn()
    try:
        policy = load_universe_policy()
        selection = select_runtime_universe(conn, policy, max_ws_tokens=2500)
        if tier == 1:
            markets = list(selection.tier1_markets)
        elif tier == 2:
            markets = list(selection.tier2_markets)
        else:
            markets = list(selection.tier1_markets) + list(selection.tier2_markets)
        return markets[:limit] if limit > 0 else markets
    finally:
        conn.close()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Refresh recent wallet-linked trade rows from the Polymarket Data API.")
    parser.add_argument("--env-file", default=None, help="Runtime env file to load before connecting.")
    parser.add_argument("--market-limit", type=int, default=10, help="Maximum approved markets to query.")
    parser.add_argument("--tier", type=int, choices=[0, 1, 2], default=1, help="Market tier selection: 1, 2, or 0 for both.")
    parser.add_argument(
        "--output-path",
        default=str(REPORT_DIR / "wallet_trade_refresh.json"),
        help="Path for the refresh report.",
    )
    parser.add_argument("--json", action="store_true", help="Print the report payload as JSON.")
    return parser


def main() -> int:
    args = build_parser().parse_args()
    load_runtime_env(args.env_file or None, override=True)
    apply_schema()

    conn = get_conn()
    try:
        before = _count_wallet_linked_trades(conn)
    finally:
        conn.close()

    started_at = _iso_now()
    markets = _select_markets(limit=args.market_limit, tier=args.tier)
    asyncio.run(collect_trades(markets))

    conn = get_conn()
    try:
        after = _count_wallet_linked_trades(conn)
    finally:
        conn.close()

    completed_at = _iso_now()
    payload = {
        "run_id": uuid4().hex,
        "started_at": started_at,
        "completed_at": completed_at,
        "market_limit": args.market_limit,
        "tier": args.tier,
        "selected_market_count": len(markets),
        "selected_markets": [
            {
                "market_id": market.market_id,
                "condition_id": market.condition_id,
                "question": market.question,
                "tier": market.tier,
            }
            for market in markets
        ],
        "trade_counts_before": before,
        "trade_counts_after": after,
        "deltas": {key: after[key] - before.get(key, 0) for key in after},
        "acceptance": {
            "queried_markets": len(markets) > 0,
            "wallet_linked_rows_increased": after["wallet_linked"] > before["wallet_linked"],
            "clob_rows_increased": after["clob_rows"] > before["clob_rows"],
        },
    }
    output_path = Path(args.output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    if args.json:
        print(json.dumps(payload, indent=2, sort_keys=True))
    else:
        print(f"Markets queried: {len(markets)}")
        print(f"Wallet-linked trades: {before['wallet_linked']} -> {after['wallet_linked']}")
        print(f"Report: {output_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
