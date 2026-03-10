"""
collectors/markets_collector.py — Full Gamma markets sync.

Paginates /markets, captures ALL available fields, writes to markets table.
Also writes a FREE Tier 3 snapshot from the metadata response (no extra calls).
Rate: ~0.35s sleep per page → ~3 req/s (well under 300 req/10s limit).
"""
import asyncio
import json
from datetime import datetime, timezone

from database.db_manager import get_conn
from utils.http_client import make_client, safe_get
from utils.logger import get_logger

log = get_logger("markets_collector")

GAMMA_URL = "https://gamma-api.polymarket.com"
PAGE_SIZE = 100
PAGE_SLEEP = 0.35


def _safe_float(val) -> float | None:
    try:
        return float(val) if val is not None else None
    except (TypeError, ValueError):
        return None


def _safe_int(val) -> int | None:
    try:
        return int(val) if val is not None else None
    except (TypeError, ValueError):
        return None


def _parse_json_field(raw) -> str | None:
    """Normalize a field that can be a JSON string or already a list."""
    if raw is None:
        return None
    if isinstance(raw, (list, dict)):
        return json.dumps(raw)
    if isinstance(raw, str):
        try:
            json.loads(raw)  # Validate
            return raw
        except (json.JSONDecodeError, ValueError):
            return raw
    return str(raw)


def _extract_tokens(market: dict) -> tuple[str | None, str | None]:
    """Extract yes_token_id and no_token_id from clobTokenIds."""
    try:
        raw = market.get("clobTokenIds")
        tokens = json.loads(raw) if isinstance(raw, str) else raw
        raw_outcomes = market.get("outcomes")
        outcomes = json.loads(raw_outcomes) if isinstance(raw_outcomes, str) else raw_outcomes

        if not tokens or len(tokens) < 2:
            return None, None

        yes_idx, no_idx = 0, 1
        if outcomes and len(outcomes) >= 2:
            for i, o in enumerate(outcomes):
                if str(o).lower() == "yes":
                    yes_idx = i
                elif str(o).lower() == "no":
                    no_idx = i

        return str(tokens[yes_idx]), str(tokens[no_idx])
    except Exception:
        return None, None


def _assign_tier(volume: float | None) -> int:
    if volume is None:
        return 3
    if volume > 500:
        return 1
    if volume > 50:
        return 2
    return 3


async def sync_markets() -> dict[str, str]:
    """
    Fetch all active markets from Gamma, upsert into DB.
    Also writes a snapshot per market from the metadata (source='gamma').
    Returns dict of {market_id: yes_token_id} for all active markets.
    """
    log.info("📦 Markets sync starting...")
    conn = get_conn()
    active_token_map: dict[str, str] = {}  # market_id → yes_token_id
    offset = 0
    total_markets = 0
    total_snapshots = 0

    async with make_client() as client:
        while True:
            url = f"{GAMMA_URL}/markets"
            params = {
                "closed": "false",
                "active": "true",
                "limit": PAGE_SIZE,
                "offset": offset,
            }
            data = await safe_get(client, url, params=params)

            if not data:
                break

            rows = data if isinstance(data, list) else []
            if not rows:
                break

            now = datetime.now(timezone.utc).isoformat()
            snapshot_rows = []

            for m in rows:
                mid = str(m.get("id", ""))
                if not mid:
                    continue

                yes_token, no_token = _extract_tokens(m)
                if not yes_token:
                    continue  # Can't track without a token

                vol = _safe_float(m.get("volumeNum") or m.get("volume"))
                tier = _assign_tier(vol)
                active_token_map[mid] = yes_token

                # --- Upsert market record ---
                conn.execute("""
                    INSERT INTO markets (
                        market_id, event_id, question, description, slug, condition_id,
                        yes_token_id, no_token_id, outcomes, outcome_prices,
                        volume, volume_24hr, volume_1wk, volume_1mo, liquidity,
                        best_bid, best_ask, spread, last_trade_price,
                        price_change_1d, price_change_1wk,
                        min_tick_size, min_order_size, accepts_orders, enable_order_book,
                        neg_risk, restricted, automated,
                        start_date, end_date,
                        tier, status, last_updated_at
                    ) VALUES (
                        :market_id, :event_id, :question, :description, :slug, :condition_id,
                        :yes_token_id, :no_token_id, :outcomes, :outcome_prices,
                        :volume, :volume_24hr, :volume_1wk, :volume_1mo, :liquidity,
                        :best_bid, :best_ask, :spread, :last_trade_price,
                        :price_change_1d, :price_change_1wk,
                        :min_tick_size, :min_order_size, :accepts_orders, :enable_order_book,
                        :neg_risk, :restricted, :automated,
                        :start_date, :end_date,
                        :tier, 'active', :last_updated_at
                    )
                    ON CONFLICT(market_id) DO UPDATE SET
                        question            = excluded.question,
                        outcome_prices      = excluded.outcome_prices,
                        volume              = excluded.volume,
                        volume_24hr         = excluded.volume_24hr,
                        volume_1wk          = excluded.volume_1wk,
                        volume_1mo          = excluded.volume_1mo,
                        liquidity           = excluded.liquidity,
                        best_bid            = excluded.best_bid,
                        best_ask            = excluded.best_ask,
                        spread              = excluded.spread,
                        last_trade_price    = excluded.last_trade_price,
                        price_change_1d     = excluded.price_change_1d,
                        price_change_1wk    = excluded.price_change_1wk,
                        accepts_orders      = excluded.accepts_orders,
                        tier                = excluded.tier,
                        status              = 'active',
                        last_updated_at     = excluded.last_updated_at
                """, {
                    "market_id":        mid,
                    "event_id":         str(m.get("eventId") or ""),
                    "question":         m.get("question"),
                    "description":      m.get("description"),
                    "slug":             m.get("slug"),
                    "condition_id":     m.get("conditionId"),
                    "yes_token_id":     yes_token,
                    "no_token_id":      no_token,
                    "outcomes":         _parse_json_field(m.get("outcomes")),
                    "outcome_prices":   _parse_json_field(m.get("outcomePrices")),
                    "volume":           vol,
                    "volume_24hr":      _safe_float(m.get("volume24hr")),
                    "volume_1wk":       _safe_float(m.get("volume1wk") or m.get("volume1wkClob")),
                    "volume_1mo":       _safe_float(m.get("volume1mo") or m.get("volume1moClob")),
                    "liquidity":        _safe_float(m.get("liquidityNum") or m.get("liquidity")),
                    "best_bid":         _safe_float(m.get("bestBid")),
                    "best_ask":         _safe_float(m.get("bestAsk")),
                    "spread":           _safe_float(m.get("spread")),
                    "last_trade_price": _safe_float(m.get("lastTradePrice")),
                    "price_change_1d":  _safe_float(m.get("oneDayPriceChange")),
                    "price_change_1wk": _safe_float(m.get("oneWeekPriceChange")),
                    "min_tick_size":    _safe_float(m.get("orderPriceMinTickSize")),
                    "min_order_size":   _safe_float(m.get("orderMinSize")),
                    "accepts_orders":   1 if m.get("acceptingOrders") else 0,
                    "enable_order_book":1 if m.get("enableOrderBook") else 0,
                    "neg_risk":         1 if m.get("negRisk") else 0,
                    "restricted":       1 if m.get("restricted") else 0,
                    "automated":        1 if m.get("automaticallyResolved") else 0,
                    "start_date":       m.get("startDate") or m.get("startDateIso"),
                    "end_date":         m.get("endDate") or m.get("endDateIso"),
                    "tier":             tier,
                    "last_updated_at":  now,
                })
                total_markets += 1

                # --- Build a Tier 3 snapshot from the metadata (FREE) ---
                outcome_prices = m.get("outcomePrices")
                try:
                    prices = json.loads(outcome_prices) if isinstance(outcome_prices, str) else outcome_prices
                    yes_p = _safe_float(prices[0]) if prices and len(prices) > 0 else None
                    no_p  = _safe_float(prices[1]) if prices and len(prices) > 1 else None
                except Exception:
                    yes_p, no_p = None, None

                best_bid = _safe_float(m.get("bestBid"))
                best_ask = _safe_float(m.get("bestAsk"))
                mid_price = None
                if best_bid is not None and best_ask is not None:
                    mid_price = (best_bid + best_ask) / 2

                snapshot_rows.append((
                    mid,
                    now,
                    yes_p,
                    no_p,
                    _safe_float(m.get("lastTradePrice")),
                    mid_price,
                    best_bid,
                    best_ask,
                    _safe_float(m.get("spread")),
                    vol,
                    _safe_float(m.get("volume24hr")),
                    _safe_float(m.get("volume1wk") or m.get("volume1wkClob")),
                    _safe_float(m.get("volume1mo") or m.get("volume1moClob")),
                    _safe_float(m.get("liquidityNum") or m.get("liquidity")),
                    _safe_float(m.get("oneDayPriceChange")),
                    _safe_float(m.get("oneWeekPriceChange")),
                    "gamma",
                ))

            # Bulk insert snapshots
            if snapshot_rows:
                conn.executemany("""
                    INSERT INTO snapshots (
                        market_id, captured_at,
                        yes_price, no_price, last_trade_price, mid_price,
                        best_bid, best_ask, spread,
                        volume_total, volume_24hr, volume_1wk, volume_1mo,
                        liquidity, price_change_1d, price_change_1wk, source
                    ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """, snapshot_rows)
                total_snapshots += len(snapshot_rows)

            conn.commit()
            log.debug(f"  Markets offset={offset}: +{len(rows)} (snapshots={total_snapshots})")
            offset += PAGE_SIZE

            if len(rows) < PAGE_SIZE:
                break

            await asyncio.sleep(PAGE_SLEEP)

    # Mark markets absent from active feed as closed
    if active_token_map:
        active_ids = list(active_token_map.keys())
        placeholders = ",".join("?" * len(active_ids))
        now = datetime.now(timezone.utc).isoformat()
        conn.execute(f"""
            UPDATE markets
            SET status = 'closed', closed_at = ?
            WHERE status = 'active'
              AND market_id NOT IN ({placeholders})
        """, [now] + active_ids)
        conn.commit()

    conn.close()
    log.info(
        f"✅ Markets sync complete: {total_markets} markets, "
        f"{total_snapshots} gamma snapshots written."
    )
    return active_token_map
