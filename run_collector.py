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
from collectors.universe_selector import (
    MarketDescriptor,
    TokenContext,
    build_token_context,
    load_universe_policy,
    select_runtime_universe,
)
from collectors.ws_listener import MarketWebSocketListener
from utils.logger import get_logger

log = get_logger("run_collector")

# --- Polling intervals (seconds) ---
TIER1_INTERVAL  = 60        # Order book depth every 1 min for hot markets
TIER2_INTERVAL  = 300       # Price poll every 5 min for mid-tier markets
TRADES_INTERVAL = 300       # Trades every 5 min for Tier 1
SYNC_INTERVAL   = 1800      # Full metadata re-sync every 30 min
TTL_INTERVAL    = 1800      # TTL cleanup every 30 min

# --- WebSocket stream cap (derived from Polymarket Cloudflare limits) ---
# Max safe concurrent WS connections: 5  (beyond ~8 simultaneous handshakes
# the server throttles / drops connections — observed in collector.log)
# Each connection holds up to 500 token subscriptions.
MAX_WS_STREAMS  = 5
TOKENS_PER_STREAM = 500
MAX_WS_TOKENS   = MAX_WS_STREAMS * TOKENS_PER_STREAM   # = 2,500

# --- Shared state (refreshed each sync cycle) ---
_tier0_markets: tuple[MarketDescriptor, ...] = ()
_tier1_markets: tuple[MarketDescriptor, ...] = ()
_tier2_markets: tuple[MarketDescriptor, ...] = ()
_all_token_context: dict[str, TokenContext] = {}
_ws_listener = MarketWebSocketListener()

_shutdown = asyncio.Event()


def _token_count(markets: tuple[MarketDescriptor, ...]) -> int:
    """Count the number of distinct token subscriptions in a market list."""
    return sum(len(market.tokens()) for market in markets)


def _refresh_runtime_universe():
    """Reload the approved runtime universe from local metadata and policy config."""
    global _tier0_markets, _tier1_markets, _tier2_markets, _all_token_context

    conn = get_conn()
    try:
        policy = load_universe_policy()
        selection = select_runtime_universe(conn, policy, MAX_WS_TOKENS)
    finally:
        conn.close()

    _tier0_markets = selection.tier0_markets
    _tier1_markets = selection.tier1_markets
    _tier2_markets = selection.tier2_markets
    _all_token_context = selection.token_context

    log.info(
        f"🗂  Universe loaded: "
        f"T0(WS)={len(_tier0_markets)} markets/{_token_count(_tier0_markets)} tokens, "
        f"T1(poll)={len(_tier1_markets)} markets, "
        f"T2(price)={len(_tier2_markets)} markets, "
        f"approved_tokens={len(_all_token_context)}, "
        f"review_candidates={len(selection.review_candidates)}"
    )
    if selection.review_candidates:
        preview = ", ".join(
            candidate.event_slug or candidate.event_title
            for candidate in selection.review_candidates[:5]
        )
        log.info(f"🧭 Review candidates: {preview}")


# ---- BACKGROUND LOOPS ----

async def sync_loop():
    """Full Gamma metadata re-sync every 30 minutes."""
    while not _shutdown.is_set():
        try:
            log.info(f"⏱  [{datetime.now(timezone.utc).strftime('%H:%M:%S')} UTC] Starting full sync...")
            await sync_events()
            await sync_markets()
            _refresh_runtime_universe()
            log.info("✅ Full sync complete")
        except Exception as e:
            log.error(f"Sync loop error: {e}")
        await asyncio.sleep(SYNC_INTERVAL)


async def tier1_loop():
    """Poll full order book for Tier 1 markets every 60 seconds."""
    while not _shutdown.is_set():
        try:
            if _tier1_markets:
                await collect_tier1(_tier1_markets)
        except Exception as e:
            log.error(f"Tier1 loop error: {e}")
        await asyncio.sleep(TIER1_INTERVAL)


async def tier2_loop():
    """Poll prices for Tier 2 markets every 5 minutes."""
    await asyncio.sleep(30)  # Stagger start
    while not _shutdown.is_set():
        try:
            if _tier2_markets:
                await collect_tier2(_tier2_markets)
        except Exception as e:
            log.error(f"Tier2 loop error: {e}")
        await asyncio.sleep(TIER2_INTERVAL)


async def trades_loop():
    """Fetch recent trades for top Tier 1 markets every 5 minutes.
    
    Limited to top 500 approved Tier 1 markets by volume — sending the full
    universe through the Data API would create unnecessary rate pressure.
    """
    await asyncio.sleep(60)  # Stagger start
    while not _shutdown.is_set():
        try:
            if _tier1_markets:
                top_500 = _tier1_markets[:500]
                await collect_trades(top_500)
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
    """WebSocket real-time feed for Tier 0 markets — persistent, auto-reconnects.

    Subscribes to both YES and NO asset IDs for approved Tier 1 markets until
    the MAX_WS_TOKENS (2,500) token cap is reached. Capped here so we never
    trigger the Cloudflare handshake-drop observed at 18+ simultaneous streams.
    """
    # Wait for first sync to populate tiers
    await asyncio.sleep(10)
    while not _shutdown.is_set():
        try:
            t0_context = build_token_context(list(_tier0_markets))
            t0_tokens = list(t0_context.keys())
            if t0_tokens:
                log.info(
                    f"📡 WS: Starting Tier 0 feed — "
                    f"{len(t0_tokens)} tokens across "
                    f"{min(MAX_WS_STREAMS, -(-len(t0_tokens)//TOKENS_PER_STREAM))} stream(s)"
                )
                await _ws_listener.run(t0_tokens, t0_context)
            else:
                log.info("WS: No Tier 0 tokens yet, waiting 60s...")
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
    _refresh_runtime_universe()

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
