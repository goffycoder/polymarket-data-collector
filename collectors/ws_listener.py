"""
collectors/ws_listener.py — Real-time WebSocket feed for Tier 1 markets.

Connects to the Polymarket CLOB WebSocket and subscribes to all Tier 1 token_ids.
Receives push events for: book, price_change, last_trade_price, best_bid_ask,
new_market, market_resolved, tick_size_change.

No polling needed — the server pushes every relevant state change.
Auto-reconnects with exponential backoff on disconnect.

Requires: pip install websockets
"""
import asyncio
import json
import time
from datetime import datetime, timezone

import websockets
from websockets.exceptions import ConnectionClosed, WebSocketException

from database.db_manager import get_conn
from utils.logger import get_logger

log = get_logger("ws_listener")

WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
RECONNECT_BASE = 2.0
RECONNECT_MAX = 60.0


def _safe_float(val) -> float | None:
    try:
        return float(val) if val is not None else None
    except (TypeError, ValueError):
        return None


def _ts_to_iso(ts) -> str:
    """Convert millisecond epoch or ISO string to ISO string."""
    if ts is None:
        return datetime.now(timezone.utc).isoformat()
    try:
        ms = float(ts)
        return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).isoformat()
    except (TypeError, ValueError):
        return str(ts)


class MarketWebSocketListener:
    """
    WebSocket listener for Polymarket CLOB market channel.

    Usage:
        listener = MarketWebSocketListener()
        await listener.run(token_ids, token_to_market)
    """

    def __init__(self):
        self._reconnect_delay = RECONNECT_BASE

    async def run(self, token_ids: list[str], token_to_market: dict[str, str]):
        """
        Maintain a persistent WebSocket connection, reconnecting on failure.
        token_ids: list of YES token IDs to subscribe to
        token_to_market: {token_id: market_id}
        """
        if not token_ids:
            log.warning("WS: No Tier 1 tokens to subscribe to, skipping WebSocket")
            return

        batches = [token_ids[i:i+500] for i in range(0, len(token_ids), 500)]
        log.info(f"📡 WS: Subscribing to {len(token_ids)} tokens in {len(batches)} batch(es)")

        while True:
            try:
                # Connect one WS per batch (250 token limit per connection)
                # Stagger connections 200ms apart to avoid simultaneous
                # TCP/TLS handshakes overloading the server
                tasks = []
                for i, batch in enumerate(batches):
                    tasks.append(asyncio.create_task(
                        self._listen_batch(batch, token_to_market)
                    ))
                    if i < len(batches) - 1:
                        await asyncio.sleep(0.2)  # 200ms stagger
                await asyncio.gather(*tasks)

            except Exception as e:
                log.warning(f"WS top-level error: {e}. Reconnecting in {self._reconnect_delay:.0f}s")
                await asyncio.sleep(self._reconnect_delay)
                self._reconnect_delay = min(self._reconnect_delay * 2, RECONNECT_MAX)

    async def _listen_batch(self, token_ids: list[str], token_to_market: dict[str, str]):
        """Connect and listen for one batch of token_ids."""
        delay = RECONNECT_BASE
        while True:
            try:
                async with websockets.connect(
                    WS_URL,
                    ping_interval=20,
                    ping_timeout=30,
                    close_timeout=10,
                    max_size=16 * 1024 * 1024,  # 16MB — initial book dump can be >1MB default
                ) as ws:
                    # Subscribe
                    sub_msg = {
                        "assets_ids": token_ids,
                        "type": "market",
                        "custom_feature_enabled": True,  # Enables best_bid_ask + new_market + market_resolved
                    }
                    await ws.send(json.dumps(sub_msg))
                    log.info(f"📡 WS connected: {len(token_ids)} tokens subscribed")
                    self._reconnect_delay = RECONNECT_BASE
                    delay = RECONNECT_BASE

                    conn = get_conn()
                    try:
                        async for raw_msg in ws:
                            try:
                                await self._handle_message(raw_msg, token_to_market, conn)
                            except Exception as e:
                                log.debug(f"WS message error: {e}")
                    finally:
                        conn.close()

            except (ConnectionClosed, WebSocketException) as e:
                log.warning(f"WS disconnected: {e}. Reconnecting in {delay:.0f}s")
                await asyncio.sleep(delay)
                delay = min(delay * 2, RECONNECT_MAX)

            except Exception as e:
                log.error(f"WS unexpected error: {e}. Reconnecting in {delay:.0f}s")
                await asyncio.sleep(delay)
                delay = min(delay * 2, RECONNECT_MAX)

    async def _handle_message(self, raw: str, token_to_market: dict, conn):
        """Dispatch incoming WS message to the appropriate handler.
        
        The server can send either a single event dict or an array of events.
        e.g. on initial subscribe it bursts a list of book snapshots.
        """
        try:
            msg = json.loads(raw)
        except json.JSONDecodeError:
            return

        # Server sometimes sends a JSON array (burst of events in one frame)
        if isinstance(msg, list):
            for item in msg:
                if isinstance(item, dict):
                    await self._dispatch(msg=item, token_to_market=token_to_market, conn=conn)
            return

        if isinstance(msg, dict):
            await self._dispatch(msg=msg, token_to_market=token_to_market, conn=conn)

    async def _dispatch(self, msg: dict, token_to_market: dict, conn):
        """Route a single event dict to the right handler."""
        event_type = msg.get("event_type")

        if event_type == "book":
            await self._handle_book(msg, token_to_market, conn)

        elif event_type == "price_change":
            await self._handle_price_change(msg, token_to_market, conn)

        elif event_type == "last_trade_price":
            await self._handle_last_trade(msg, token_to_market, conn)

        elif event_type == "best_bid_ask":
            await self._handle_best_bid_ask(msg, token_to_market, conn)

        elif event_type == "market_resolved":
            await self._handle_market_resolved(msg, token_to_market, conn)

        elif event_type == "new_market":
            log.info(f"🆕 WS: New market detected: {msg.get('question', '')[:60]}")

    async def _handle_book(self, msg: dict, token_to_market: dict, conn):
        """Full order book snapshot — emitted on subscribe or after a trade."""
        asset_id = str(msg.get("asset_id") or "")
        market_id = token_to_market.get(asset_id)
        if not market_id:
            return

        bids = msg.get("bids", [])
        asks = msg.get("asks", [])
        best_bid = _safe_float(bids[0]["price"]) if bids else None
        best_ask = _safe_float(asks[0]["price"]) if asks else None
        spread = (best_ask - best_bid) if (best_bid and best_ask) else None
        mid = ((best_bid + best_ask) / 2) if (best_bid and best_ask) else None
        now = _ts_to_iso(msg.get("timestamp"))

        bid_vol = sum(_safe_float(b.get("size", 0)) or 0 for b in bids)
        ask_vol = sum(_safe_float(a.get("size", 0)) or 0 for a in asks)

        conn.execute("""
            INSERT INTO order_book_snapshots (
                market_id, token_id, captured_at,
                bids_json, asks_json, best_bid, best_ask, spread,
                depth_bids, depth_asks, bid_volume, ask_volume, source
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (
            market_id, asset_id, now,
            json.dumps(bids), json.dumps(asks),
            best_bid, best_ask, spread,
            len(bids), len(asks), bid_vol, ask_vol, "ws",
        ))

        conn.execute("""
            INSERT INTO snapshots (market_id, captured_at, mid_price, best_bid, best_ask, spread, source)
            VALUES (?,?,?,?,?,?,?)
        """, (market_id, now, mid, best_bid, best_ask, spread, "ws"))

        conn.commit()

    async def _handle_price_change(self, msg: dict, token_to_market: dict, conn):
        """Price change — emitted when order placed or cancelled."""
        now = _ts_to_iso(msg.get("timestamp"))
        rows = []

        for change in msg.get("price_changes", []):
            asset_id = str(change.get("asset_id") or "")
            market_id = token_to_market.get(asset_id)
            if not market_id:
                continue

            price = _safe_float(change.get("price"))
            best_bid = _safe_float(change.get("best_bid"))
            best_ask = _safe_float(change.get("best_ask"))
            mid = ((best_bid + best_ask) / 2) if (best_bid and best_ask) else None
            spread = (best_ask - best_bid) if (best_bid is not None and best_ask is not None) else None

            rows.append((
                market_id, now,
                price, None, mid, best_bid, best_ask, spread, "ws",
            ))

        if rows:
            conn.executemany("""
                INSERT INTO snapshots (
                    market_id, captured_at,
                    yes_price, last_trade_price, mid_price, best_bid, best_ask, spread, source
                ) VALUES (?,?,?,?,?,?,?,?,?)
            """, rows)
            conn.commit()

    async def _handle_last_trade(self, msg: dict, token_to_market: dict, conn):
        """Trade matched — write to trades table."""
        asset_id = str(msg.get("asset_id") or "")
        market_id = token_to_market.get(asset_id)
        if not market_id:
            return

        price = _safe_float(msg.get("price"))
        size = _safe_float(msg.get("size"))
        now = _ts_to_iso(msg.get("timestamp"))
        # WS trade doesn't have unique trade_id — use asset+ts+price as surrogate
        trade_id = f"ws_{asset_id}_{msg.get('timestamp', int(time.time()*1000))}_{price}"

        conn.execute("""
            INSERT OR IGNORE INTO trades (
                trade_id, market_id, token_id, side, price, size, trade_time, captured_at, source
            ) VALUES (?,?,?,?,?,?,?,?,?)
        """, (
            trade_id, market_id, asset_id,
            str(msg.get("side") or ""),
            price, size, now,
            datetime.now(timezone.utc).isoformat(), "ws",
        ))

        # Also snapshot the last trade price
        conn.execute("""
            INSERT INTO snapshots (market_id, captured_at, last_trade_price, source)
            VALUES (?,?,?,?)
        """, (market_id, now, price, "ws"))

        conn.commit()

    async def _handle_best_bid_ask(self, msg: dict, token_to_market: dict, conn):
        """Best bid/ask update — highest resolution price signal."""
        asset_id = str(msg.get("asset_id") or "")
        market_id = token_to_market.get(asset_id)
        if not market_id:
            return

        best_bid = _safe_float(msg.get("best_bid"))
        best_ask = _safe_float(msg.get("best_ask"))
        spread = _safe_float(msg.get("spread"))
        mid = ((best_bid + best_ask) / 2) if (best_bid and best_ask) else None
        now = _ts_to_iso(msg.get("timestamp"))

        conn.execute("""
            INSERT INTO snapshots (market_id, captured_at, mid_price, best_bid, best_ask, spread, source)
            VALUES (?,?,?,?,?,?,?)
        """, (market_id, now, mid, best_bid, best_ask, spread, "ws"))
        conn.commit()

    async def _handle_market_resolved(self, msg: dict, token_to_market: dict, conn):
        """Market resolved — mark as closed in DB."""
        market_id = str(msg.get("market") or "")
        # The WS sends the conditionId as 'market' field — try to find by condition_id
        now = datetime.now(timezone.utc).isoformat()

        # Try exact match first (if market_id was in token_to_market)
        if market_id not in token_to_market.values():
            # Try matching by condition_id stored in markets table
            conn.execute("""
                UPDATE markets SET status = 'closed', closed_at = ?
                WHERE condition_id = ? AND status = 'active'
            """, (now, market_id))
        else:
            conn.execute("""
                UPDATE markets SET status = 'closed', closed_at = ?
                WHERE market_id = ? AND status = 'active'
            """, (now, market_id))

        conn.commit()
        log.info(f"✅ WS: Market resolved: {market_id}")
