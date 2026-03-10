"""
collectors/price_collector.py — CLOB bulk price + order book depth.

Uses BULK endpoints (POST body with multiple token_ids) to minimize API calls:
  - Tier 1 (volume > $500): POST /books  → full order book depth
  - Tier 2 ($50-$500):      POST /prices → best price per side

Concurrency: asyncio Semaphore to cap parallel requests.
Rate limits: /books = 500 req/10s, /prices = 500 req/10s. We stay well under.
"""
import asyncio
import json
from datetime import datetime, timezone

from database.db_manager import get_conn
from utils.http_client import make_client, safe_post
from utils.logger import get_logger

log = get_logger("price_collector")

CLOB_URL = "https://clob.polymarket.com"
SEMAPHORE = asyncio.Semaphore(8)  # Max 8 concurrent CLOB batch calls

# Batch sizes for bulk endpoints
BOOKS_BATCH = 50   # POST /books → up to 100 token_ids per call
PRICES_BATCH = 200  # POST /prices → up to 500 token_ids per call


def _safe_float(val) -> float | None:
    try:
        return float(val) if val is not None else None
    except (TypeError, ValueError):
        return None


def _parse_book_response(book: dict, market_lookup: dict) -> tuple[str | None, dict, dict]:
    """
    Extract structured data from a single CLOB /books entry.
    Returns (market_id, snapshot_dict, order_book_dict).
    """
    asset_id = str(book.get("asset_id") or book.get("token_id") or "")
    market_id = market_lookup.get(asset_id)

    if not market_id:
        return None, {}, {}

    bids = book.get("bids", [])
    asks = book.get("asks", [])

    best_bid = _safe_float(bids[0]["price"]) if bids else None
    best_ask = _safe_float(asks[0]["price"]) if asks else None
    spread = (best_ask - best_bid) if (best_bid is not None and best_ask is not None) else None
    mid = ((best_bid + best_ask) / 2) if (best_bid is not None and best_ask is not None) else None

    bid_vol = sum(_safe_float(b.get("size", 0)) or 0 for b in bids)
    ask_vol = sum(_safe_float(a.get("size", 0)) or 0 for a in asks)

    last_trade = _safe_float(book.get("last_trade_price"))
    now = datetime.now(timezone.utc).isoformat()

    snapshot = {
        "market_id":        market_id,
        "captured_at":      now,
        "yes_price":        best_bid,   # Best bid ≈ market-implied YES probability
        "last_trade_price": last_trade,
        "mid_price":        mid,
        "best_bid":         best_bid,
        "best_ask":         best_ask,
        "spread":           spread,
        "source":           "clob",
    }

    ob = {
        "market_id":  market_id,
        "token_id":   asset_id,
        "captured_at":now,
        "bids_json":  json.dumps(bids),
        "asks_json":  json.dumps(asks),
        "best_bid":   best_bid,
        "best_ask":   best_ask,
        "spread":     spread,
        "depth_bids": len(bids),
        "depth_asks": len(asks),
        "bid_volume": bid_vol,
        "ask_volume": ask_vol,
        "source":     "clob",
    }

    return market_id, snapshot, ob


async def _fetch_books_batch(client, token_ids: list[str], market_lookup: dict, conn):
    """Fetch full order book for a batch of token_ids and write to DB."""
    async with SEMAPHORE:
        data = await safe_post(client, f"{CLOB_URL}/books",
                               json_body=[{"token_id": tid} for tid in token_ids])
        if not data:
            return

        books = data if isinstance(data, list) else []
        snapshots = []
        obs = []

        for book in books:
            mid, snap, ob = _parse_book_response(book, market_lookup)
            if mid:
                snapshots.append(snap)
                obs.append(ob)

        if snapshots:
            conn.executemany("""
                INSERT INTO snapshots (
                    market_id, captured_at,
                    yes_price, last_trade_price, mid_price,
                    best_bid, best_ask, spread, source
                ) VALUES (
                    :market_id, :captured_at,
                    :yes_price, :last_trade_price, :mid_price,
                    :best_bid, :best_ask, :spread, :source
                )
            """, snapshots)

        if obs:
            conn.executemany("""
                INSERT INTO order_book_snapshots (
                    market_id, token_id, captured_at,
                    bids_json, asks_json,
                    best_bid, best_ask, spread,
                    depth_bids, depth_asks, bid_volume, ask_volume, source
                ) VALUES (
                    :market_id, :token_id, :captured_at,
                    :bids_json, :asks_json,
                    :best_bid, :best_ask, :spread,
                    :depth_bids, :depth_asks, :bid_volume, :ask_volume, :source
                )
            """, obs)

        conn.commit()
        log.debug(f"  Books batch: {len(snapshots)} snapshots, {len(obs)} order books")


async def _fetch_prices_batch(client, token_ids: list[str], market_lookup: dict, conn):
    """Fetch best prices for a batch (Tier 2) and write lightweight snapshots."""
    async with SEMAPHORE:
        # POST /prices expects list of {token_id, side}
        payload = [{"token_id": tid, "side": "buy"} for tid in token_ids]
        data = await safe_post(client, f"{CLOB_URL}/prices", json_body=payload)
        if not data:
            return

        prices_map = {}
        if isinstance(data, list):
            for item in data:
                tid = str(item.get("token_id") or item.get("asset_id") or "")
                p = _safe_float(item.get("price"))
                if tid and p is not None:
                    prices_map[tid] = p
        elif isinstance(data, dict):
            # Some versions return {token_id: price} dict
            for tid, p in data.items():
                prices_map[str(tid)] = _safe_float(p)

        now = datetime.now(timezone.utc).isoformat()
        rows = []
        for tid, price in prices_map.items():
            market_id = market_lookup.get(tid)
            if market_id and price is not None:
                rows.append({
                    "market_id":  market_id,
                    "captured_at":now,
                    "yes_price":  price,
                    "source":     "clob",
                })

        if rows:
            conn.executemany("""
                INSERT INTO snapshots (market_id, captured_at, yes_price, source)
                VALUES (:market_id, :captured_at, :yes_price, :source)
            """, rows)
            conn.commit()
        log.debug(f"  Prices batch: {len(rows)} snapshots")


async def collect_tier1(market_token_map: dict[str, str]):
    """
    Poll Tier 1 markets (high volume) for full order book depth.
    market_token_map: {market_id: yes_token_id}
    """
    if not market_token_map:
        return

    # Invert to {token_id: market_id} for response parsing
    token_to_market = {v: k for k, v in market_token_map.items()}
    token_ids = list(token_to_market.keys())

    log.info(f"📊 Tier 1 book poll: {len(token_ids)} markets...")
    conn = get_conn()

    async with make_client() as client:
        tasks = []
        for i in range(0, len(token_ids), BOOKS_BATCH):
            batch = token_ids[i:i + BOOKS_BATCH]
            tasks.append(_fetch_books_batch(client, batch, token_to_market, conn))
        await asyncio.gather(*tasks)

    conn.close()
    log.info(f"✅ Tier 1 poll complete ({len(token_ids)} markets)")


async def collect_tier2(market_token_map: dict[str, str]):
    """
    Poll Tier 2 markets (medium volume) for best price only.
    market_token_map: {market_id: yes_token_id}
    """
    if not market_token_map:
        return

    token_to_market = {v: k for k, v in market_token_map.items()}
    token_ids = list(token_to_market.keys())

    log.info(f"📉 Tier 2 price poll: {len(token_ids)} markets...")
    conn = get_conn()

    async with make_client() as client:
        tasks = []
        for i in range(0, len(token_ids), PRICES_BATCH):
            batch = token_ids[i:i + PRICES_BATCH]
            tasks.append(_fetch_prices_batch(client, batch, token_to_market, conn))
        await asyncio.gather(*tasks)

    conn.close()
    log.info(f"✅ Tier 2 poll complete ({len(token_ids)} markets)")
