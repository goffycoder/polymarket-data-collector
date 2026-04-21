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
from typing import Sequence

from collectors.universe_selector import MarketDescriptor, TokenContext, build_token_context
from database.db_manager import get_conn
from utils.event_log import archive_raw_event, publish_detector_input
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


def _empty_snapshot(market_id: str, captured_at: str, source: str) -> dict:
    """Create a partially populated market snapshot row."""
    return {
        "market_id": market_id,
        "captured_at": captured_at,
        "yes_price": None,
        "no_price": None,
        "last_trade_price": None,
        "mid_price": None,
        "best_bid": None,
        "best_ask": None,
        "spread": None,
        "source": source,
    }


def _merge_snapshot(target: dict, update: dict) -> None:
    """Merge a token-level update into a market snapshot row."""
    for key, value in update.items():
        if key in {"market_id", "captured_at", "source"}:
            continue
        if value is not None:
            target[key] = value


def _parse_book_response(
    book: dict,
    token_context: dict[str, TokenContext],
    captured_at: str,
) -> tuple[dict | None, dict | None]:
    """
    Extract structured data from a single CLOB /books entry.
    Returns (snapshot_update, order_book_dict).
    """
    asset_id = str(book.get("asset_id") or book.get("token_id") or "")
    token_meta = token_context.get(asset_id)

    if not token_meta:
        return None, None

    bids = book.get("bids", [])
    asks = book.get("asks", [])

    best_bid = _safe_float(bids[0]["price"]) if bids else None
    best_ask = _safe_float(asks[0]["price"]) if asks else None
    spread = (best_ask - best_bid) if (best_bid is not None and best_ask is not None) else None
    mid = ((best_bid + best_ask) / 2) if (best_bid is not None and best_ask is not None) else None

    bid_vol = sum(_safe_float(b.get("size", 0)) or 0 for b in bids)
    ask_vol = sum(_safe_float(a.get("size", 0)) or 0 for a in asks)

    last_trade = _safe_float(book.get("last_trade_price"))

    snapshot: dict[str, float | str | None] = {}
    if token_meta.outcome_side == "YES":
        snapshot.update(
            {
                "yes_price": best_bid,
                "last_trade_price": last_trade,
                "mid_price": mid,
                "best_bid": best_bid,
                "best_ask": best_ask,
                "spread": spread,
            }
        )
    else:
        snapshot.update({"no_price": best_bid})

    ob = {
        "market_id":  token_meta.market_id,
        "token_id":   asset_id,
        "captured_at": captured_at,
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

    return snapshot, ob


async def _fetch_books_batch(
    client,
    token_ids: list[str],
    token_context: dict[str, TokenContext],
    conn,
):
    """Fetch full order book for a batch of token_ids and write to DB."""
    async with SEMAPHORE:
        request_payload = [{"token_id": tid} for tid in token_ids]
        data = await safe_post(client, f"{CLOB_URL}/books", json_body=request_payload)
        if not data:
            return

        books = data if isinstance(data, list) else []
        captured_at = datetime.now(timezone.utc).isoformat()
        archive_result = archive_raw_event(
            source_system="clob_books",
            event_type="books_batch",
            payload=books,
            captured_at=captured_at,
            metadata={
                "requested_token_ids": token_ids,
                "request_size": len(request_payload),
            },
        )
        market_snapshots: dict[str, dict] = {}
        obs = []

        for book in books:
            asset_id = str(book.get("asset_id") or book.get("token_id") or "")
            token_meta = token_context.get(asset_id)
            if not token_meta:
                continue

            snapshot_update, ob = _parse_book_response(book, token_context, captured_at)
            if ob:
                obs.append(ob)
            if snapshot_update is None:
                continue

            market_snapshot = market_snapshots.setdefault(
                token_meta.market_id,
                _empty_snapshot(token_meta.market_id, captured_at, "clob"),
            )
            _merge_snapshot(market_snapshot, snapshot_update)

        if market_snapshots:
            market_snapshot_rows = list(market_snapshots.values())
            conn.executemany("""
                INSERT INTO snapshots (
                    market_id, captured_at,
                    yes_price, no_price, last_trade_price, mid_price,
                    best_bid, best_ask, spread, source
                ) VALUES (
                    :market_id, :captured_at,
                    :yes_price, :no_price, :last_trade_price, :mid_price,
                    :best_bid, :best_ask, :spread, :source
                )
            """, market_snapshot_rows)
        else:
            market_snapshot_rows = []

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
        publish_detector_input(
            source_system="clob_books",
            entity_type="books_batch",
            captured_at=captured_at,
            ordering_key=f"{captured_at}:{len(token_ids)}",
                raw_partition_path=archive_result.partition_path,
                payload={
                    "token_ids": token_ids,
                    "market_ids": sorted(market_snapshots.keys()),
                    "snapshot_count": len(market_snapshots),
                    "order_book_count": len(obs),
                    "market_snapshots": market_snapshot_rows,
                    "order_books": [
                        {
                            "market_id": ob["market_id"],
                            "token_id": ob["token_id"],
                            "captured_at": ob["captured_at"],
                            "best_bid": ob["best_bid"],
                            "best_ask": ob["best_ask"],
                            "spread": ob["spread"],
                            "depth_bids": ob["depth_bids"],
                            "depth_asks": ob["depth_asks"],
                            "bid_volume": ob["bid_volume"],
                            "ask_volume": ob["ask_volume"],
                            "source": ob["source"],
                        }
                        for ob in obs
                    ],
                },
            )
        log.debug(
            f"  Books batch: {len(market_snapshots)} market snapshots, {len(obs)} order books"
        )


async def _fetch_prices_batch(
    client,
    token_ids: list[str],
    token_context: dict[str, TokenContext],
    conn,
):
    """Fetch best prices for a batch (Tier 2) and write lightweight snapshots."""
    async with SEMAPHORE:
        # POST /prices expects list of {token_id, side}
        payload = [{"token_id": tid, "side": "buy"} for tid in token_ids]
        data = await safe_post(client, f"{CLOB_URL}/prices", json_body=payload)
        if not data:
            return

        captured_at = datetime.now(timezone.utc).isoformat()
        archive_result = archive_raw_event(
            source_system="clob_prices",
            event_type="prices_batch",
            payload=data,
            captured_at=captured_at,
            metadata={
                "requested_token_ids": token_ids,
                "request_size": len(payload),
            },
        )
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

        rows: dict[str, dict] = {}
        for tid, price in prices_map.items():
            token_meta = token_context.get(tid)
            if not token_meta or price is None:
                continue

            row = rows.setdefault(
                token_meta.market_id,
                _empty_snapshot(token_meta.market_id, captured_at, "clob"),
            )
            if token_meta.outcome_side == "YES":
                row["yes_price"] = price
            else:
                row["no_price"] = price

        if rows:
            market_snapshot_rows = list(rows.values())
            conn.executemany("""
                INSERT INTO snapshots (market_id, captured_at, yes_price, no_price, source)
                VALUES (:market_id, :captured_at, :yes_price, :no_price, :source)
            """, market_snapshot_rows)
            conn.commit()
            publish_detector_input(
                source_system="clob_prices",
                entity_type="prices_batch",
                captured_at=captured_at,
                ordering_key=f"{captured_at}:{len(token_ids)}",
                raw_partition_path=archive_result.partition_path,
                payload={
                    "token_ids": token_ids,
                    "market_ids": sorted(rows.keys()),
                    "snapshot_count": len(rows),
                    "market_snapshots": market_snapshot_rows,
                },
            )
        log.debug(f"  Prices batch: {len(rows)} market snapshots")


async def collect_tier1(markets: Sequence[MarketDescriptor]):
    """
    Poll Tier 1 markets (high volume) for full order book depth.
    Uses both YES and NO token IDs for every approved market.
    """
    if not markets:
        return

    token_context = build_token_context(list(markets))
    token_ids = list(token_context.keys())

    log.info(f"📊 Tier 1 book poll: {len(markets)} markets / {len(token_ids)} tokens...")
    conn = get_conn()

    async with make_client() as client:
        tasks = []
        for i in range(0, len(token_ids), BOOKS_BATCH):
            batch = token_ids[i:i + BOOKS_BATCH]
            tasks.append(_fetch_books_batch(client, batch, token_context, conn))
        await asyncio.gather(*tasks)

    conn.close()
    log.info(f"✅ Tier 1 poll complete ({len(markets)} markets)")


async def collect_tier2(markets: Sequence[MarketDescriptor]):
    """
    Poll Tier 2 markets (medium volume) for best price only.
    Uses both YES and NO token IDs for every approved market.
    """
    if not markets:
        return

    token_context = build_token_context(list(markets))
    token_ids = list(token_context.keys())

    log.info(f"📉 Tier 2 price poll: {len(markets)} markets / {len(token_ids)} tokens...")
    conn = get_conn()

    async with make_client() as client:
        tasks = []
        for i in range(0, len(token_ids), PRICES_BATCH):
            batch = token_ids[i:i + PRICES_BATCH]
            tasks.append(_fetch_prices_batch(client, batch, token_context, conn))
        await asyncio.gather(*tasks)

    conn.close()
    log.info(f"✅ Tier 2 poll complete ({len(markets)} markets)")
