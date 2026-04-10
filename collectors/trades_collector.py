"""
collectors/trades_collector.py — Recent trade history from the Polymarket Data API.

Fetches recent trades for approved Tier 1 markets using the market condition ID,
normalizes wallet-aware fields, and writes them into the trades table.
"""
import asyncio
from typing import Sequence

from collectors.trade_utils import make_trade_row, upsert_trade_rows
from collectors.universe_selector import MarketDescriptor
from database.db_manager import get_conn
from utils.http_client import make_client, safe_get
from utils.logger import get_logger

log = get_logger("trades_collector")

DATA_API_URL = "https://data-api.polymarket.com"
SEMAPHORE = asyncio.Semaphore(3)
TRADES_PER_MARKET = 50  # Recent trades to fetch per market


async def _fetch_market_trades(client, market: MarketDescriptor, conn):
    """Fetch and upsert recent trades for a single approved market."""
    async with SEMAPHORE:
        params = {"market": market.condition_id, "limit": TRADES_PER_MARKET}
        data = await safe_get(client, f"{DATA_API_URL}/trades", params=params)

        if not data:
            return

        trades = data if isinstance(data, list) else data.get("data", [])
        rows = []

        for trade in trades:
            row = make_trade_row(
                trade,
                market_id=market.market_id,
                condition_id=market.condition_id,
                source="clob",
            )
            if row:
                rows.append(row)

        if rows:
            upsert_trade_rows(conn, rows)
            log.debug(f"  Trades market={market.market_id}: +{len(rows)} rows")


async def collect_trades(markets: Sequence[MarketDescriptor]):
    """
    Fetch recent trades for approved Tier 1 markets.
    Queries the Data API by condition ID, not by token ID.
    """
    if not markets:
        return

    log.info(f"🔄 Trades fetch: {len(markets)} approved Tier 1 markets...")
    conn = get_conn()

    async with make_client() as client:
        tasks = [_fetch_market_trades(client, market, conn) for market in markets]
        await asyncio.gather(*tasks)

    conn.close()
    log.info("✅ Trades fetch complete")
