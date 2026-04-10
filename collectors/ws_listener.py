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

from collectors.trade_utils import make_trade_row, upsert_trade_rows
from collectors.universe_selector import TokenContext
from database.db_manager import get_conn
from utils.logger import get_logger

log = get_logger("ws_listener")

WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
RECONNECT_BASE = 2.0
RECONNECT_MAX  = 60.0

# Hard cap on simultaneous WS connections.
# Beyond ~8 concurrent TLS handshakes the Cloudflare layer starts dropping
# them (observed: 18 connections → 380 handshake timeout errors in logs).
# 5 connections × 500 tokens = 2,500 markets live — the safe maximum.
MAX_BATCHES     = 5
WS_STAGGER_SECS = 3.0   # seconds between opening each new connection


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
        await listener.run(token_ids, token_context)
    """

    def __init__(self):
        self._reconnect_delay = RECONNECT_BASE

    async def run(self, token_ids: list[str], token_context: dict[str, TokenContext]):
        """
        Maintain a persistent WebSocket connection, reconnecting on failure.
        token_ids: list of YES and NO token IDs to subscribe to
        token_context: {token_id: TokenContext}
        """
        if not token_ids:
            log.warning("WS: No Tier 1 tokens to subscribe to, skipping WebSocket")
            return

        batches = [token_ids[i:i+500] for i in range(0, len(token_ids), 500)]

        # Enforce the hard cap — caller should already pass ≤ MAX_BATCHES×500 tokens
        # but guard here in case _load_tiers cap is bypassed.
        if len(batches) > MAX_BATCHES:
            log.warning(
                f"WS: {len(batches)} batches requested but capping at {MAX_BATCHES} "
                f"({MAX_BATCHES * 500} tokens) to prevent handshake saturation"
            )
            batches = batches[:MAX_BATCHES]

        log.info(
            f"📡 WS: Subscribing to {sum(len(b) for b in batches)} tokens "
            f"in {len(batches)} stream(s) "
            f"(stagger={WS_STAGGER_SECS:.0f}s each)"
        )

        while True:
            try:
                # One persistent WS connection per batch.
                # Stagger by WS_STAGGER_SECS so at most ~1 TLS handshake per window
                # — fixes the 380-error handshake storm seen with 200ms stagger.
                tasks = []
                for i, batch in enumerate(batches):
                    tasks.append(asyncio.create_task(
                        self._listen_batch(batch_id=i, token_ids=batch, token_context=token_context)
                    ))
                    if i < len(batches) - 1:
                        await asyncio.sleep(WS_STAGGER_SECS)
                await asyncio.gather(*tasks)

            except Exception as e:
                log.warning(f"WS top-level error: {e}. Reconnecting in {self._reconnect_delay:.0f}s")
                await asyncio.sleep(self._reconnect_delay)
                self._reconnect_delay = min(self._reconnect_delay * 2, RECONNECT_MAX)

    async def _listen_batch(self, batch_id: int, token_ids: list[str], token_context: dict[str, TokenContext]):
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
                                await self._handle_message(raw_msg, token_context, conn)
                            except Exception as e:
                                log.debug(f"WS message error: {e}")
                    finally:
                        conn.close()

            except (ConnectionClosed, WebSocketException) as e:
                # Add deterministic stagger based on batch_id so if all connections
                # drop simultaneously, they don't all attempt to reconnect simultaneously
                # and trigger the Cloudflare handshake storm.
                staggered_delay = delay + (batch_id * WS_STAGGER_SECS)
                log.warning(f"WS disconnected: {e}. Reconnecting in {staggered_delay:.0f}s (batch {batch_id})")
                await asyncio.sleep(staggered_delay)
                delay = min(delay * 2, RECONNECT_MAX)

            except Exception as e:
                staggered_delay = delay + (batch_id * WS_STAGGER_SECS)
                log.error(f"WS unexpected error: {e}. Reconnecting in {staggered_delay:.0f}s (batch {batch_id})")
                await asyncio.sleep(staggered_delay)
                delay = min(delay * 2, RECONNECT_MAX)

    async def _handle_message(self, raw: str, token_context: dict[str, TokenContext], conn):
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
                    await self._dispatch(msg=item, token_context=token_context, conn=conn)
            return

        if isinstance(msg, dict):
            await self._dispatch(msg=msg, token_context=token_context, conn=conn)

    async def _dispatch(self, msg: dict, token_context: dict[str, TokenContext], conn):
        """Route a single event dict to the right handler."""
        event_type = msg.get("event_type")

        if event_type == "book":
            await self._handle_book(msg, token_context, conn)

        elif event_type == "price_change":
            await self._handle_price_change(msg, token_context, conn)

        elif event_type == "last_trade_price":
            await self._handle_last_trade(msg, token_context, conn)

        elif event_type == "best_bid_ask":
            await self._handle_best_bid_ask(msg, token_context, conn)

        elif event_type == "market_resolved":
            await self._handle_market_resolved(msg, token_context, conn)

        elif event_type == "new_market":
            log.info(f"🆕 WS: New market detected: {msg.get('question', '')[:60]}")

    async def _handle_book(self, msg: dict, token_context: dict[str, TokenContext], conn):
        """Full order book snapshot — emitted on subscribe or after a trade."""
        asset_id = str(msg.get("asset_id") or "")
        token_meta = token_context.get(asset_id)
        if not token_meta:
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
            token_meta.market_id, asset_id, now,
            json.dumps(bids), json.dumps(asks),
            best_bid, best_ask, spread,
            len(bids), len(asks), bid_vol, ask_vol, "ws",
        ))

        conn.execute("""
            INSERT INTO snapshots (
                market_id, captured_at, yes_price, no_price, mid_price, best_bid, best_ask, spread, source
            ) VALUES (?,?,?,?,?,?,?,?,?)
        """, (
            token_meta.market_id,
            now,
            best_bid if token_meta.outcome_side == "YES" else None,
            best_bid if token_meta.outcome_side == "NO" else None,
            mid if token_meta.outcome_side == "YES" else None,
            best_bid if token_meta.outcome_side == "YES" else None,
            best_ask if token_meta.outcome_side == "YES" else None,
            spread if token_meta.outcome_side == "YES" else None,
            "ws",
        ))

        conn.commit()

    async def _handle_price_change(self, msg: dict, token_context: dict[str, TokenContext], conn):
        """Price change — emitted when order placed or cancelled."""
        now = _ts_to_iso(msg.get("timestamp"))
        rows = []

        for change in msg.get("price_changes", []):
            asset_id = str(change.get("asset_id") or "")
            token_meta = token_context.get(asset_id)
            if not token_meta:
                continue

            price = _safe_float(change.get("price"))
            best_bid = _safe_float(change.get("best_bid"))
            best_ask = _safe_float(change.get("best_ask"))
            mid = ((best_bid + best_ask) / 2) if (best_bid and best_ask) else None
            spread = (best_ask - best_bid) if (best_bid is not None and best_ask is not None) else None

            rows.append((
                token_meta.market_id,
                now,
                price if token_meta.outcome_side == "YES" else None,
                price if token_meta.outcome_side == "NO" else None,
                mid if token_meta.outcome_side == "YES" else None,
                best_bid if token_meta.outcome_side == "YES" else None,
                best_ask if token_meta.outcome_side == "YES" else None,
                spread if token_meta.outcome_side == "YES" else None,
                "ws",
            ))

        if rows:
            conn.executemany("""
                INSERT INTO snapshots (
                    market_id, captured_at,
                    yes_price, no_price, mid_price, best_bid, best_ask, spread, source
                ) VALUES (?,?,?,?,?,?,?,?)
            """, rows)
            conn.commit()

    async def _handle_last_trade(self, msg: dict, token_context: dict[str, TokenContext], conn):
        """Trade matched — write to trades table."""
        asset_id = str(msg.get("asset_id") or "")
        token_meta = token_context.get(asset_id)
        if not token_meta:
            return

        now = _ts_to_iso(msg.get("timestamp"))
        price = _safe_float(msg.get("price"))
        size = _safe_float(msg.get("size"))
        trade_id = f"ws_{asset_id}_{msg.get('timestamp', int(time.time()*1000))}_{price}_{size}"
        trade_row = make_trade_row(
            {
                "id": trade_id,
                "asset": asset_id,
                "conditionId": token_meta.condition_id,
                "outcome": token_meta.outcome_side,
                "side": msg.get("side"),
                "price": msg.get("price"),
                "size": msg.get("size"),
                "timestamp": msg.get("timestamp"),
            },
            market_id=token_meta.market_id,
            condition_id=token_meta.condition_id,
            source="ws",
        )
        if trade_row:
            upsert_trade_rows(conn, [trade_row])

        # Also snapshot the last trade price
        conn.execute("""
            INSERT INTO snapshots (market_id, captured_at, yes_price, no_price, last_trade_price, source)
            VALUES (?,?,?,?,?,?)
        """, (
            token_meta.market_id,
            now,
            price if token_meta.outcome_side == "YES" else None,
            price if token_meta.outcome_side == "NO" else None,
            price if token_meta.outcome_side == "YES" else None,
            "ws",
        ))

        conn.commit()

    async def _handle_best_bid_ask(self, msg: dict, token_context: dict[str, TokenContext], conn):
        """Best bid/ask update — highest resolution price signal."""
        asset_id = str(msg.get("asset_id") or "")
        token_meta = token_context.get(asset_id)
        if not token_meta:
            return

        best_bid = _safe_float(msg.get("best_bid"))
        best_ask = _safe_float(msg.get("best_ask"))
        spread = _safe_float(msg.get("spread"))
        mid = ((best_bid + best_ask) / 2) if (best_bid and best_ask) else None
        now = _ts_to_iso(msg.get("timestamp"))

        conn.execute("""
            INSERT INTO snapshots (
                market_id, captured_at, yes_price, no_price, mid_price, best_bid, best_ask, spread, source
            ) VALUES (?,?,?,?,?,?,?,?,?)
        """, (
            token_meta.market_id,
            now,
            best_bid if token_meta.outcome_side == "YES" else None,
            best_bid if token_meta.outcome_side == "NO" else None,
            mid if token_meta.outcome_side == "YES" else None,
            best_bid if token_meta.outcome_side == "YES" else None,
            best_ask if token_meta.outcome_side == "YES" else None,
            spread if token_meta.outcome_side == "YES" else None,
            "ws",
        ))
        conn.commit()

    async def _handle_market_resolved(self, msg: dict, token_context: dict[str, TokenContext], conn):
        """Market resolved — mark as closed and record YES/NO outcome for ML labels."""
        condition_id = str(msg.get("market") or "")
        now = datetime.now(timezone.utc).isoformat()

        # Determine outcome from winning token price
        # WS sends: {market: condition_id, asset_id: winning_token, price: "1" or "0"}
        asset_id = str(msg.get("asset_id") or "")
        final_price = _safe_float(msg.get("price"))

        # YES token won if its final price == 1.0, NO token won if 0.0
        if final_price is not None:
            # asset_id is the YES token that resolved
            # price 1.0 → this token won → YES. price 0.0 → this token lost → NO
            if final_price >= 0.99:
                outcome = "YES"
            elif final_price <= 0.01:
                outcome = "NO"
            else:
                outcome = "N/A"  # ambiguous / multi-outcome
        else:
            outcome = "N/A"

        # Find the market row — try condition_id match first, then token match
        market_row = conn.execute("""
            SELECT market_id, condition_id FROM markets
            WHERE condition_id = ? AND status = 'active'
            LIMIT 1
        """, (condition_id,)).fetchone()

        if not market_row and asset_id:
            market_row = conn.execute("""
                SELECT market_id, condition_id FROM markets
                WHERE (yes_token_id = ? OR no_token_id = ?) AND status = 'active'
                LIMIT 1
            """, (asset_id, asset_id)).fetchone()

        if market_row:
            market_id = market_row[0]
            stored_condition_id = market_row[1] or condition_id

            # Update market row: status + outcome
            conn.execute("""
                UPDATE markets
                SET status = 'closed', closed_at = ?, outcome = ?
                WHERE market_id = ? AND status = 'active'
            """, (now, outcome, market_id))

            # Insert into resolutions archive (ground truth for ML)
            conn.execute("""
                INSERT INTO market_resolutions
                    (market_id, condition_id, outcome, final_price, resolved_at, source)
                VALUES (?, ?, ?, ?, ?, 'ws')
            """, (market_id, stored_condition_id, outcome, final_price, now))

        else:
            # Fallback: close by condition_id even if we can't determine outcome
            conn.execute("""
                UPDATE markets SET status = 'closed', closed_at = ?
                WHERE condition_id = ? AND status = 'active'
            """, (now, condition_id))

        conn.commit()
        log.info(f"✅ WS: Market resolved — {condition_id[:16]}… → {outcome}")
