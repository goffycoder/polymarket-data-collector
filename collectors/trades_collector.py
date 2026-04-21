"""
collectors/trades_collector.py — Recent trade history from the Polymarket Data API.

Fetches recent trades for approved Tier 1 markets using the market condition ID,
normalizes wallet-aware fields, and writes them into the trades table.
"""
import asyncio
from datetime import datetime, timezone
from typing import Sequence

from collectors.trade_utils import make_trade_row, trade_row_to_detector_payload, upsert_trade_rows
from collectors.universe_selector import MarketDescriptor
from database.db_manager import get_conn
from utils.event_log import archive_raw_event, publish_detector_input
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

        captured_at = datetime.now(timezone.utc).isoformat()
        archive_result = archive_raw_event(
            source_system="data_api_trades",
            event_type="recent_trades_page",
            payload=data,
            captured_at=captured_at,
            metadata={
                "market_id": market.market_id,
                "condition_id": market.condition_id,
                "params": params,
            },
        )
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
            detector_trades = [trade_row_to_detector_payload(row) for row in rows]
            publish_detector_input(
                source_system="data_api_trades",
                entity_type="recent_trades_page",
                captured_at=captured_at,
                ordering_key=f"{market.market_id}:{market.condition_id}",
                raw_partition_path=archive_result.partition_path,
                payload={
                    "market_id": market.market_id,
                    "condition_id": market.condition_id,
                    "row_count": len(rows),
                    "trade_ids": [row["trade_id"] for row in rows],
                    "trades": detector_trades,
                },
            )
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
