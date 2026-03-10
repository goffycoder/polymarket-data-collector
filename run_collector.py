"""
run_collector.py — Single entry point for the Polymarket v2 data collector.

Startup sequence:
  1. Apply canonical schema.sql (idempotent)
  2. Full events sync  (Gamma)
  3. Full markets sync (Gamma + free Tier 3 snapshots)

Concurrent background loops:
  - WebSocket loop:  Tier 1 real-time push feed (continuous)
  - Tier 1 loop:     CLOB bulk /books every 60s (order book depth)
  - Tier 2 loop:     CLOB bulk /prices every 5 min
  - Trades loop:     CLOB /trades every 5 min (Tier 1 only)
  - Sync loop:       Full Gamma re-sync every 30 min
  - TTL loop:        Decay cleanup every 30 min

Run as a background daemon (see polymarket.plist for launchd setup).
"""
import asyncio
import signal
import sys
from datetime import datetime, timezone

from database.db_manager import apply_schema, get_conn
from collectors.events_collector import sync_events
from collectors.markets_collector import sync_markets
from collectors.price_collector import collect_tier1, collect_tier2
from collectors.trades_collector import collect_trades
from collectors.ttl_manager import run_maintenance
from collectors.ws_listener import MarketWebSocketListener
from utils.logger import get_logger

log = get_logger("run_collector")

# --- Polling intervals (seconds) ---
TIER1_INTERVAL  = 60        # Order book depth every 1 min for hot markets
TIER2_INTERVAL  = 300       # Price poll every 5 min for mid-tier markets
TRADES_INTERVAL = 300       # Trades every 5 min for Tier 1
SYNC_INTERVAL   = 1800      # Full metadata re-sync every 30 min
TTL_INTERVAL    = 1800      # TTL cleanup every 30 min

# --- Shared state (refreshed each sync cycle) ---
_tier1_map: dict[str, str] = {}
_tier2_map: dict[str, str] = {}
_all_token_to_market: dict[str, str] = {}     # {token_id: market_id}
_ws_listener = MarketWebSocketListener()

_shutdown = asyncio.Event()


def _load_tiers():
    """Reload tiered markets from DB into shared state."""
    global _tier1_map, _tier2_map, _all_token_to_market
    conn = get_conn()
    rows = conn.execute(
        "SELECT market_id, yes_token_id, tier FROM markets WHERE status='active' AND yes_token_id IS NOT NULL"
    ).fetchall()
    conn.close()

    t1, t2 = {}, {}
    tok_map = {}
    for r in rows:
        mid, tok, tier = r[0], r[1], r[2]
        tok_map[tok] = mid
        if tier == 1:
            t1[mid] = tok
        elif tier == 2:
            t2[mid] = tok

    _tier1_map = t1
    _tier2_map = t2
    _all_token_to_market = tok_map
    log.info(f"🗂  Tiers loaded: T1={len(t1)}, T2={len(t2)}, total_tokens={len(tok_map)}")


# ---- BACKGROUND LOOPS ----

async def sync_loop():
    """Full Gamma metadata re-sync every 30 minutes."""
    while not _shutdown.is_set():
        try:
            log.info(f"⏱  [{datetime.now(timezone.utc).strftime('%H:%M:%S')} UTC] Starting full sync...")
            await sync_events()
            await sync_markets()
            _load_tiers()
            log.info("✅ Full sync complete")
        except Exception as e:
            log.error(f"Sync loop error: {e}")
        await asyncio.sleep(SYNC_INTERVAL)


async def tier1_loop():
    """Poll full order book for Tier 1 markets every 60 seconds."""
    while not _shutdown.is_set():
        try:
            if _tier1_map:
                await collect_tier1(_tier1_map)
        except Exception as e:
            log.error(f"Tier1 loop error: {e}")
        await asyncio.sleep(TIER1_INTERVAL)


async def tier2_loop():
    """Poll prices for Tier 2 markets every 5 minutes."""
    await asyncio.sleep(30)  # Stagger start
    while not _shutdown.is_set():
        try:
            if _tier2_map:
                await collect_tier2(_tier2_map)
        except Exception as e:
            log.error(f"Tier2 loop error: {e}")
        await asyncio.sleep(TIER2_INTERVAL)


async def trades_loop():
    """Fetch recent trades for Tier 1 every 5 minutes."""
    await asyncio.sleep(60)  # Stagger start
    while not _shutdown.is_set():
        try:
            if _tier1_map:
                await collect_trades(_tier1_map)
        except Exception as e:
            log.error(f"Trades loop error: {e}")
        await asyncio.sleep(TRADES_INTERVAL)


async def ttl_loop():
    """Run TTL maintenance every 30 minutes."""
    await asyncio.sleep(SYNC_INTERVAL)  # First TTL after first full sync
    while not _shutdown.is_set():
        try:
            await asyncio.to_thread(run_maintenance)
        except Exception as e:
            log.error(f"TTL loop error: {e}")
        await asyncio.sleep(TTL_INTERVAL)


async def ws_loop():
    """WebSocket real-time feed — persistent, auto-reconnects."""
    # Wait for first sync to populate tiers
    await asyncio.sleep(10)
    while not _shutdown.is_set():
        try:
            t1_tokens = list(_tier1_map.values())
            if t1_tokens:
                await _ws_listener.run(t1_tokens, _all_token_to_market)
            else:
                log.info("WS: No Tier 1 tokens yet, waiting 60s...")
                await asyncio.sleep(60)
        except Exception as e:
            log.error(f"WS loop top error: {e}")
            await asyncio.sleep(5)


# ---- GRACEFUL SHUTDOWN ----

_loop: asyncio.AbstractEventLoop | None = None


def _handle_signal(signum, frame):
    log.info(f"🛑 Signal {signum} received — shutting down gracefully...")
    _shutdown.set()
    if _loop and _loop.is_running():
        _loop.stop()  # Cancel all running tasks immediately


async def main():
    global _loop
    _loop = asyncio.get_event_loop()

    # Register shutdown signals
    signal.signal(signal.SIGTERM, _handle_signal)
    signal.signal(signal.SIGINT, _handle_signal)

    log.info("=" * 55)
    log.info("  POLYMARKET V2 HIGH-RESOLUTION DATA COLLECTOR")
    log.info("=" * 55)

    # ---- PHASE 1: Bootstrap ----
    log.info("📐 PHASE 1: Applying database schema...")
    apply_schema()
    log.info("✅ Schema applied")

    # ---- PHASE 2: Initial Full Sync ----
    log.info("🌐 PHASE 2: Initial full sync (events + markets)...")
    await sync_events()
    await sync_markets()
    _load_tiers()

    # ---- PHASE 3: Launch concurrent loops ----
    log.info("🚀 PHASE 3: Launching monitoring loops...")
    try:
        await asyncio.gather(
            ws_loop(),
            tier1_loop(),
            tier2_loop(),
            trades_loop(),
            sync_loop(),
            ttl_loop(),
            return_exceptions=True,
        )
    except asyncio.CancelledError:
        pass

    log.info("👋 Collector stopped.")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        log.info("Collector exited.")
        sys.exit(0)
