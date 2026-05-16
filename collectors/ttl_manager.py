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

from database.db_manager import get_conn, is_postgres_backend
from utils.logger import get_logger

log = get_logger("ttl_manager")

TTL_HOURS = 24
SQLITE_DELETE_BATCH_SIZE = 5000


def _delete_sqlite_closed_market_rows(conn, *, table: str, cutoff: str) -> int:
    deleted = 0
    while True:
        cursor = conn.execute(
            f"""
            DELETE FROM {table}
            WHERE rowid IN (
                SELECT rowid
                FROM {table}
                WHERE captured_at < ?
                  AND market_id IN (
                      SELECT market_id FROM markets WHERE status = 'closed'
                  )
                LIMIT ?
            )
            """,
            (cutoff, SQLITE_DELETE_BATCH_SIZE),
        )
        batch_count = max(0, cursor.rowcount)
        conn.commit()
        deleted += batch_count
        if batch_count < SQLITE_DELETE_BATCH_SIZE:
            break
    return deleted


def run_maintenance():
    """
    Purge old data for closed markets and log what was cleaned.
    This is synchronous — called from the async loop via asyncio.to_thread.
    """
    log.info("🧹 Running TTL maintenance...")
    conn = get_conn()
    cutoff = (datetime.now(timezone.utc) - timedelta(hours=TTL_HOURS)).isoformat()

    try:
        if is_postgres_backend():
            # --- 1. Purge snapshots for closed markets older than TTL ---
            cursor = conn.execute("""
                DELETE FROM snapshots
                WHERE captured_at < ?
                  AND market_id IN (
                      SELECT market_id FROM markets WHERE status = 'closed'
                  )
            """, (cutoff,))
            deleted_snapshots = cursor.rowcount
            conn.commit()

            # --- 2. Purge order book snapshots for closed markets ---
            cursor = conn.execute("""
                DELETE FROM order_book_snapshots
                WHERE captured_at < ?
                  AND market_id IN (
                      SELECT market_id FROM markets WHERE status = 'closed'
                  )
            """, (cutoff,))
            deleted_obs = cursor.rowcount
            conn.commit()

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
        else:
            deleted_snapshots = _delete_sqlite_closed_market_rows(
                conn,
                table="snapshots",
                cutoff=cutoff,
            )
            deleted_obs = _delete_sqlite_closed_market_rows(
                conn,
                table="order_book_snapshots",
                cutoff=cutoff,
            )
            deleted_trades = _delete_sqlite_closed_market_rows(
                conn,
                table="trades",
                cutoff=cutoff,
            )

        # --- 4. Report DB stats ---
        stats = {}
        for table in ["events", "markets", "snapshots", "order_book_snapshots", "trades"]:
            row = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()
            stats[table] = row[0] if row else 0
    finally:
        conn.close()

    log.info(
        f"✅ TTL done: -{deleted_snapshots} snapshots, "
        f"-{deleted_obs} order books, -{deleted_trades} trades | "
        f"DB totals: {stats}"
    )
