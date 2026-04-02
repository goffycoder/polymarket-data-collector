"""
collectors/trades_collector.py — Recent trade history from CLOB Data API.

Fetches recent trades for Tier 1 markets and inserts new ones into the trades table.
Dedupes by trade_id (PRIMARY KEY conflict ignored).
Rate: Data API /trades = 200 req/10s. We stay under 30 req/10s.
"""
import asyncio
from datetime import datetime, timezone

from database.db_manager import get_conn
from utils.http_client import make_client, safe_get
from utils.logger import get_logger

log = get_logger("trades_collector")

DATA_API_URL = "https://data-api.polymarket.com"
SEMAPHORE = asyncio.Semaphore(3)
TRADES_PER_MARKET = 50  # Recent trades to fetch per market


def _safe_float(val) -> float | None:
    try:
        return float(val) if val is not None else None
    except (TypeError, ValueError):
        return None


async def _fetch_market_trades(client, market_id: str, token_id: str, conn):
    """Fetch and insert recent trades for a single market."""
    async with SEMAPHORE:
        url = f"{DATA_API_URL}/trades"
        params = {"market": token_id, "limit": TRADES_PER_MARKET}
        data = await safe_get(client, url, params=params)

        if not data:
            return

        trades = data if isinstance(data, list) else data.get("data", [])
        rows = []

        for t in trades:
            tid = str(t.get("id") or t.get("tradeId") or "")
            if not tid:
                continue

            # Parse timestamp — API returns ms epoch or ISO string
            raw_ts = t.get("timestamp") or t.get("matchTime") or t.get("createdAt")
            trade_time = None
            if raw_ts:
                try:
                    if isinstance(raw_ts, (int, float)):
                        trade_time = datetime.fromtimestamp(raw_ts / 1000, tz=timezone.utc).isoformat()
                    else:
                        trade_time = str(raw_ts)
                except Exception:
                    pass

            rows.append((
                tid,
                market_id,
                token_id,
                str(t.get("side") or ""),
                _safe_float(t.get("price")),
                _safe_float(t.get("size") or t.get("amount")),
                str(t.get("feeRateBps") or ""),
                trade_time,
                datetime.now(timezone.utc).isoformat(),
                "clob",
            ))

        if rows:
            conn.executemany("""
                INSERT OR IGNORE INTO trades (
                    trade_id, market_id, token_id, side,
                    price, size, fee_rate_bps, trade_time, captured_at, source
                ) VALUES (?,?,?,?,?,?,?,?,?,?)
            """, rows)
            conn.commit()
            log.debug(f"  Trades market={market_id}: +{len(rows)} rows")


async def collect_trades(market_token_map: dict[str, str]):
    """
    Fetch recent trades for all Tier 1 markets.
    market_token_map: {market_id: yes_token_id}
    """
    if not market_token_map:
        return

    log.info(f"🔄 Trades fetch: {len(market_token_map)} Tier 1 markets...")
    conn = get_conn()

    async with make_client() as client:
        tasks = [
            _fetch_market_trades(client, mid, token_id, conn)
            for mid, token_id in market_token_map.items()
        ]
        await asyncio.gather(*tasks)

    conn.close()
    log.info(f"✅ Trades fetch complete")
