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

# --- WebSocket stream cap (derived from Polymarket Cloudflare limits) ---
# Max safe concurrent WS connections: 5  (beyond ~8 simultaneous handshakes
# the server throttles / drops connections — observed in collector.log)
# Each connection holds up to 500 token subscriptions.
MAX_WS_STREAMS  = 5
TOKENS_PER_STREAM = 500
MAX_WS_TOKENS   = MAX_WS_STREAMS * TOKENS_PER_STREAM   # = 2,500

# --- Shared state (refreshed each sync cycle) ---
_tier0_map: dict[str, str] = {}              # top MAX_WS_TOKENS vol>$500 → real-time WS
_tier1_map: dict[str, str] = {}             # all vol>$500 (superset of T0) → book poll
_tier2_map: dict[str, str] = {}
_all_token_to_market: dict[str, str] = {}     # {token_id: market_id}
_ws_listener = MarketWebSocketListener()

_shutdown = asyncio.Event()


def _load_tiers():
    """Reload tiered markets from DB into shared state.

    Tier 0 is a runtime-only promotion: the top MAX_WS_TOKENS markets from
    the Tier 1 pool (volume > $500), sorted by volume DESC, that get
    dedicated real-time WebSocket coverage.  The DB tier column is unchanged.

    SPORTS FILTER IN EFFECT: Only markets tagged with sports keywords are 
    loaded into the system priority queues. All other APIs (CLOB books, 
    WebSockets, trades) strictly follow these queues.
    """
    global _tier0_map, _tier1_map, _tier2_map, _all_token_to_market
    
    SPORTS_KEYWORDS = [
        'Sport', 'Soccer', 'Basketball', 'Hockey', 'Football', 'Baseball', 
        'Tennis', 'Cricket', 'Golf', 'Boxing', 'MMA', 'UFC', 'F1', 
        'Olympics', 'Esports', 'Games', 'NFL', 'NBA', 'MLB', 'NHL'
    ]
    tag_conditions = " OR ".join(f"e.tags LIKE '%{kw}%'" for kw in SPORTS_KEYWORDS)

    conn = get_conn()
    rows = conn.execute(f"""
        SELECT m.market_id, m.yes_token_id, m.tier
        FROM markets m
        JOIN events e ON m.event_id = e.event_id
        WHERE m.status='active' 
          AND m.yes_token_id IS NOT NULL
          AND ({tag_conditions})
        ORDER BY m.volume DESC
    """).fetchall()
    conn.close()

    t0, t1, t2 = {}, {}, {}
    tok_map = {}
    tier1_candidates = []   # all vol>$500 markets in volume-DESC order

    for r in rows:
        mid, tok, tier = r[0], r[1], r[2]
        tok_map[tok] = mid
        if tier == 1:
            tier1_candidates.append((mid, tok))
            t1[mid] = tok
        elif tier == 2:
            t2[mid] = tok

    # Tier 0 = top MAX_WS_TOKENS by volume (already sorted DESC above)
    for mid, tok in tier1_candidates[:MAX_WS_TOKENS]:
        t0[mid] = tok

    _tier0_map = t0
    _tier1_map = t1
    _tier2_map = t2
    _all_token_to_market = tok_map
    log.info(
        f"🗂  Tiers loaded: "
        f"T0(WS)={len(t0)}/{MAX_WS_TOKENS}, "
        f"T1(poll)={len(t1)}, T2={len(t2)}, "
        f"total_tokens={len(tok_map)}"
    )


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
    """Fetch recent trades for top Tier 1 markets every 5 minutes.
    
    Limited to top 500 by volume — sending all 8,700 individual calls
    exceeds the data-api 200 req/10s rate limit.
    """
    await asyncio.sleep(60)  # Stagger start
    while not _shutdown.is_set():
        try:
            if _tier1_map:
                # Sort by... we only have token_ids in the map; take first 500
                # (already loaded in volume-DESC order from _load_tiers)
                top_500 = dict(list(_tier1_map.items())[:500])
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

    Subscribes to the top MAX_WS_TOKENS (2,500) volume>$500 markets across
    MAX_WS_STREAMS (5) concurrent connections.  Capped here so we never
    trigger the Cloudflare handshake-drop observed at 18+ simultaneous streams.
    """
    # Wait for first sync to populate tiers
    await asyncio.sleep(10)
    while not _shutdown.is_set():
        try:
            t0_tokens = list(_tier0_map.values())
            if t0_tokens:
                log.info(
                    f"📡 WS: Starting Tier 0 feed — "
                    f"{len(t0_tokens)} tokens across "
                    f"{min(MAX_WS_STREAMS, -(-len(t0_tokens)//TOKENS_PER_STREAM))} stream(s)"
                )
                await _ws_listener.run(t0_tokens, _all_token_to_market)
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
