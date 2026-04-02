"""
collectors/backfill.py — Historical trade backfill from Polymarket Data API.

Fetches the full available trade history for Tier 1 markets using:
  https://data-api.polymarket.com/trades?token_id=TOKEN&limit=500

Paginates backward in time using cursor/offset until no more data.
Inserts into the trades table (INSERT OR IGNORE for deduplication).

Usage:
    python -m collectors.backfill                  # backfill all T1 markets
    python -m collectors.backfill --days 30        # only last N days
    python -m collectors.backfill --limit 5        # only first 5 markets (test)

Designed to run once at setup, or occasionally to catch up after downtime.
"""
import asyncio
import argparse
from datetime import datetime, timezone, timedelta

from database.db_manager import get_conn, apply_schema
from utils.http_client import make_client, safe_get
from utils.logger import get_logger

log = get_logger("backfill")

DATA_API_URL = "https://data-api.polymarket.com"
SEMAPHORE = asyncio.Semaphore(4)       # conservative — 4 concurrent requests
PAGE_SIZE = 500                         # max the API supports
MAX_PAGES_PER_MARKET = 100             # safety cap: 500×100 = 50,000 trades/market


def _safe_float(val) -> float | None:
    try:
        return float(val) if val is not None else None
    except (TypeError, ValueError):
        return None


async def _backfill_market(
    client,
    market_id: str,
    token_id: str,
    conn,
    since_ts: float | None = None,
) -> int:
    """
    Fetch and insert ALL historical trades for one market.
    Paginates backward until empty page or since_ts cutoff.
    Returns number of new rows inserted.
    """
    async with SEMAPHORE:
        total_inserted = 0
        offset = 0

        for page in range(MAX_PAGES_PER_MARKET):
            params = {
                "market": token_id,
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

            for t in trades:
                tid = str(t.get("id") or t.get("tradeId") or "")
                if not tid:
                    continue

                raw_ts = t.get("timestamp") or t.get("matchTime") or t.get("createdAt")
                trade_time = None
                trade_ts = None
                if raw_ts:
                    try:
                        if isinstance(raw_ts, (int, float)):
                            trade_ts = float(raw_ts) / 1000
                            trade_time = datetime.fromtimestamp(trade_ts, tz=timezone.utc).isoformat()
                        else:
                            trade_time = str(raw_ts)
                    except Exception:
                        pass

                # Stop pagination if we've gone back far enough
                if since_ts and trade_ts and trade_ts < since_ts:
                    hit_cutoff = True
                    break

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
                    "clob_backfill",
                ))

            if rows:
                conn.executemany("""
                    INSERT OR IGNORE INTO trades (
                        trade_id, market_id, token_id, side,
                        price, size, fee_rate_bps, trade_time, captured_at, source
                    ) VALUES (?,?,?,?,?,?,?,?,?,?)
                """, rows)
                conn.commit()
                total_inserted += len(rows)

            if hit_cutoff or len(trades) < PAGE_SIZE:
                break

            offset += PAGE_SIZE

        if total_inserted > 0:
            log.debug(f"  Backfill market={market_id[:16]}…: +{total_inserted} trades")

        return total_inserted


async def run_backfill(days: int | None = None, market_limit: int | None = None):
    """
    Backfill trade history for all Tier 1 markets.

    Args:
        days:         If set, only fetch trades from the last N days.
        market_limit: If set, only process the first N markets (for testing).
    """
    apply_schema()

    conn = get_conn()
    since_ts = None
    if days:
        since_ts = (datetime.now(timezone.utc) - timedelta(days=days)).timestamp()
        log.info(f"📥 Backfill: Tier 1 trades since {days} days ago...")
    else:
        log.info("📥 Backfill: ALL historical Tier 1 trades (this may take a while)...")

    # Load Tier 1 markets
    rows = conn.execute("""
        SELECT market_id, yes_token_id
        FROM markets
        WHERE tier = 1 AND yes_token_id IS NOT NULL AND status = 'active'
        ORDER BY volume DESC
    """).fetchall()

    if market_limit:
        rows = rows[:market_limit]

    total_markets = len(rows)
    log.info(f"  Markets to backfill: {total_markets}")

    total_trades = 0
    async with make_client() as client:
        tasks = [
            _backfill_market(client, r[0], r[1], conn, since_ts)
            for r in rows
        ]
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
        help="Only process first N markets (for testing)"
    )
    args = parser.parse_args()

    asyncio.run(run_backfill(days=args.days, market_limit=args.limit))
