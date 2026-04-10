"""
collectors/backfill.py — Historical trade backfill from the Polymarket Data API.

Fetches historical trades for approved markets using the market condition ID,
normalizes wallet-aware fields, and writes them into the trades table.
"""
import argparse
import asyncio
from datetime import datetime, timezone, timedelta
from typing import Sequence

from collectors.trade_utils import make_trade_row, parse_trade_time, upsert_trade_rows
from collectors.universe_selector import MarketDescriptor, load_universe_policy, select_runtime_universe
from database.db_manager import apply_schema, get_conn
from utils.http_client import make_client, safe_get
from utils.logger import get_logger

log = get_logger("backfill")

DATA_API_URL = "https://data-api.polymarket.com"
SEMAPHORE = asyncio.Semaphore(4)       # conservative — 4 concurrent requests
PAGE_SIZE = 500                        # max the API supports
MAX_PAGES_PER_MARKET = 100             # safety cap: 500×100 = 50,000 trades/market


async def _backfill_market(
    client,
    market: MarketDescriptor,
    conn,
    since_ts: float | None = None,
) -> int:
    """
    Fetch and insert historical trades for one approved market.
    Paginates backward until empty page or since_ts cutoff.
    Returns the number of normalized rows processed.
    """
    async with SEMAPHORE:
        total_inserted = 0
        offset = 0

        for _page in range(MAX_PAGES_PER_MARKET):
            params = {
                "market": market.condition_id,
                "limit": PAGE_SIZE,
                "offset": offset,
            }
            data = await safe_get(client, f"{DATA_API_URL}/trades", params=params)
            if not data:
                break

            trades = data if isinstance(data, list) else data.get("data", [])
            if not trades:
                break

            rows = []
            hit_cutoff = False

            for trade in trades:
                trade_time = parse_trade_time(
                    trade.get("timestamp") or trade.get("matchTime") or trade.get("createdAt")
                )
                if since_ts and trade_time:
                    trade_dt = datetime.fromisoformat(trade_time.replace("Z", "+00:00"))
                    if trade_dt.timestamp() < since_ts:
                        hit_cutoff = True
                        break

                row = make_trade_row(
                    trade,
                    market_id=market.market_id,
                    condition_id=market.condition_id,
                    source="clob_backfill",
                )
                if row:
                    rows.append(row)

            if rows:
                upsert_trade_rows(conn, rows)
                total_inserted += len(rows)

            if hit_cutoff or len(trades) < PAGE_SIZE:
                break

            offset += PAGE_SIZE

        if total_inserted > 0:
            log.debug(f"  Backfill market={market.market_id[:16]}…: +{total_inserted} trades")

        return total_inserted


def _load_backfill_markets(conn, market_limit: int | None = None) -> tuple[MarketDescriptor, ...]:
    """Reuse the runtime universe policy so backfill stays aligned with live ingestion."""
    selection = select_runtime_universe(conn, load_universe_policy(), max_ws_tokens=2500)
    approved_markets: Sequence[MarketDescriptor] = selection.tier1_markets
    if market_limit is not None:
        approved_markets = approved_markets[:market_limit]
    return tuple(approved_markets)


async def run_backfill(days: int | None = None, market_limit: int | None = None):
    """
    Backfill trade history for approved Tier 1 markets.

    Args:
        days:         If set, only fetch trades from the last N days.
        market_limit: If set, only process the first N approved markets.
    """
    apply_schema()

    conn = get_conn()
    since_ts = None
    if days:
        since_ts = (datetime.now(timezone.utc) - timedelta(days=days)).timestamp()
        log.info(f"📥 Backfill: approved Tier 1 trades since {days} days ago...")
    else:
        log.info("📥 Backfill: ALL approved Tier 1 trades (this may take a while)...")

    markets = _load_backfill_markets(conn, market_limit)
    total_markets = len(markets)
    log.info(f"  Markets to backfill: {total_markets}")

    total_trades = 0
    async with make_client() as client:
        tasks = [_backfill_market(client, market, conn, since_ts) for market in markets]
        results = await asyncio.gather(*tasks)
        total_trades = sum(results)

    conn.close()
    log.info(f"✅ Backfill complete: {total_trades:,} trades across {total_markets} markets")
    print(f"\n✅ Backfill complete: {total_trades:,} trades across {total_markets} markets")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Polymarket historical trade backfill")
    parser.add_argument(
        "--days", type=int, default=None,
        help="Only fetch trades from the last N days (default: all history)"
    )
    parser.add_argument(
        "--limit", type=int, default=None,
        help="Only process first N approved markets (for testing)"
    )
    args = parser.parse_args()

    asyncio.run(run_backfill(days=args.days, market_limit=args.limit))
