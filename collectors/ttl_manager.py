"""
collectors/ttl_manager.py — 1-day decay and lifecycle management.

Rules:
  - Active markets: keep ALL snapshots (full history)
  - Closed markets: delete snapshots older than 24h (TTL decay)
  - Market/event metadata rows: NEVER deleted (status='closed' but kept)
  - order_book_snapshots for closed markets: delete older than 24h
  - trades for closed markets: delete older than 24h
"""
from datetime import datetime, timezone, timedelta

from database.db_manager import get_conn
from utils.logger import get_logger

log = get_logger("ttl_manager")

TTL_HOURS = 24


def run_maintenance():
    """
    Purge old data for closed markets and log what was cleaned.
    This is synchronous — called from the async loop via asyncio.to_thread.
    """
    log.info("🧹 Running TTL maintenance...")
    conn = get_conn()
    cutoff = (datetime.now(timezone.utc) - timedelta(hours=TTL_HOURS)).isoformat()

    # --- 1. Purge snapshots for closed markets older than TTL ---
    cursor = conn.execute("""
        DELETE FROM snapshots
        WHERE captured_at < ?
          AND market_id IN (
              SELECT market_id FROM markets WHERE status = 'closed'
          )
    """, (cutoff,))
    deleted_snapshots = cursor.rowcount

    # --- 2. Purge order book snapshots for closed markets ---
    cursor = conn.execute("""
        DELETE FROM order_book_snapshots
        WHERE captured_at < ?
          AND market_id IN (
              SELECT market_id FROM markets WHERE status = 'closed'
          )
    """, (cutoff,))
    deleted_obs = cursor.rowcount

    # --- 3. Purge trades for closed markets ---
    cursor = conn.execute("""
        DELETE FROM trades
        WHERE captured_at < ?
          AND market_id IN (
              SELECT market_id FROM markets WHERE status = 'closed'
          )
    """, (cutoff,))
    deleted_trades = cursor.rowcount

    conn.commit()

    # --- 4. Report DB stats ---
    stats = {}
    for table in ["events", "markets", "snapshots", "order_book_snapshots", "trades"]:
        row = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()
        stats[table] = row[0] if row else 0

    conn.close()

    log.info(
        f"✅ TTL done: -{deleted_snapshots} snapshots, "
        f"-{deleted_obs} order books, -{deleted_trades} trades | "
        f"DB totals: {stats}"
    )
