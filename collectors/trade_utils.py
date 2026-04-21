"""
collectors/trade_utils.py — Shared helpers for wallet-aware trade ingestion.
"""
from __future__ import annotations

from datetime import datetime, timezone


SOURCE_PRIORITIES = {
    "ws": 1,
    "clob_backfill": 2,
    "clob": 3,
}


TRADE_UPSERT_SQL = """
INSERT INTO trades (
    trade_id,
    market_id,
    token_id,
    asset_id,
    condition_id,
    proxy_wallet,
    transaction_hash,
    outcome_side,
    side,
    price,
    size,
    usdc_notional,
    fee_rate_bps,
    trade_time,
    captured_at,
    source,
    dedupe_key,
    source_priority
) VALUES (
    :trade_id,
    :market_id,
    :token_id,
    :asset_id,
    :condition_id,
    :proxy_wallet,
    :transaction_hash,
    :outcome_side,
    :side,
    :price,
    :size,
    :usdc_notional,
    :fee_rate_bps,
    :trade_time,
    :captured_at,
    :source,
    :dedupe_key,
    :source_priority
)
ON CONFLICT(trade_id) DO UPDATE SET
    market_id = excluded.market_id,
    token_id = excluded.token_id,
    asset_id = excluded.asset_id,
    condition_id = excluded.condition_id,
    proxy_wallet = COALESCE(excluded.proxy_wallet, trades.proxy_wallet),
    transaction_hash = COALESCE(excluded.transaction_hash, trades.transaction_hash),
    outcome_side = COALESCE(excluded.outcome_side, trades.outcome_side),
    side = COALESCE(excluded.side, trades.side),
    price = COALESCE(excluded.price, trades.price),
    size = COALESCE(excluded.size, trades.size),
    usdc_notional = COALESCE(excluded.usdc_notional, trades.usdc_notional),
    fee_rate_bps = COALESCE(excluded.fee_rate_bps, trades.fee_rate_bps),
    trade_time = COALESCE(excluded.trade_time, trades.trade_time),
    captured_at = excluded.captured_at,
    source = excluded.source,
    dedupe_key = COALESCE(excluded.dedupe_key, trades.dedupe_key),
    source_priority = excluded.source_priority
WHERE excluded.source_priority >= trades.source_priority
"""


def safe_float(val) -> float | None:
    """Parse a value into float when possible."""
    try:
        return float(val) if val is not None else None
    except (TypeError, ValueError):
        return None


def parse_trade_time(raw_ts) -> str | None:
    """Normalize numeric or ISO timestamps into ISO 8601 strings."""
    if raw_ts is None:
        return None
    try:
        if isinstance(raw_ts, (int, float)):
            return datetime.fromtimestamp(float(raw_ts) / 1000, tz=timezone.utc).isoformat()
        text = str(raw_ts)
        return datetime.fromisoformat(text.replace("Z", "+00:00")).astimezone(timezone.utc).isoformat()
    except (TypeError, ValueError):
        return str(raw_ts)


def build_dedupe_key(
    transaction_hash: str | None,
    condition_id: str,
    asset_id: str,
    side: str,
    price: float | None,
    size: float | None,
    trade_time: str | None,
) -> str:
    """Build the canonical dedupe key used across live, backfill, and WS trades."""
    if transaction_hash:
        return transaction_hash.lower()

    normalized_time = trade_time or ""
    normalized_price = "" if price is None else f"{price:.12f}"
    normalized_size = "" if size is None else f"{size:.12f}"
    return "|".join(
        [
            condition_id or "",
            asset_id or "",
            side or "",
            normalized_price,
            normalized_size,
            normalized_time,
        ]
    )


def make_trade_row(
    trade: dict,
    *,
    market_id: str,
    condition_id: str,
    source: str,
) -> dict | None:
    """Normalize a trade payload from Polymarket into the local trade schema."""
    trade_id = str(trade.get("id") or trade.get("tradeId") or trade.get("tradeID") or "")
    if not trade_id:
        return None

    asset_id = str(trade.get("asset") or trade.get("asset_id") or trade.get("assetId") or "")
    proxy_wallet = str(trade.get("proxyWallet") or trade.get("proxy_wallet") or "").lower() or None
    transaction_hash = str(
        trade.get("transactionHash") or trade.get("transaction_hash") or ""
    ).lower() or None
    outcome_side = str(trade.get("outcome") or "").upper() or None
    side = str(trade.get("side") or "").upper() or None
    price = safe_float(trade.get("price"))
    size = safe_float(trade.get("size") or trade.get("amount"))
    size_usdc = safe_float(trade.get("sizeUsdc") or trade.get("size_usdc"))
    trade_time = parse_trade_time(
        trade.get("timestamp") or trade.get("matchTime") or trade.get("createdAt")
    )
    usdc_notional = size_usdc if size_usdc is not None else (
        (price * size) if price is not None and size is not None else None
    )
    canonical_condition_id = str(trade.get("conditionId") or condition_id or "")
    fee_rate_bps = trade.get("feeRateBps") or trade.get("fee_rate_bps")
    fee_rate_bps_text = str(fee_rate_bps) if fee_rate_bps is not None else None

    dedupe_key = build_dedupe_key(
        transaction_hash=transaction_hash,
        condition_id=canonical_condition_id,
        asset_id=asset_id,
        side=side or "",
        price=price,
        size=size,
        trade_time=trade_time,
    )

    return {
        "trade_id": trade_id,
        "market_id": market_id,
        "token_id": asset_id or None,
        "asset_id": asset_id or None,
        "condition_id": canonical_condition_id or None,
        "proxy_wallet": proxy_wallet,
        "transaction_hash": transaction_hash,
        "outcome_side": outcome_side,
        "side": side,
        "price": price,
        "size": size,
        "usdc_notional": usdc_notional,
        "fee_rate_bps": fee_rate_bps_text,
        "trade_time": trade_time,
        "captured_at": datetime.now(timezone.utc).isoformat(),
        "source": source,
        "dedupe_key": dedupe_key,
        "source_priority": SOURCE_PRIORITIES[source],
    }


def upsert_trade_rows(conn, rows: list[dict]) -> None:
    """Upsert normalized trades using source-priority aware conflict handling."""
    if not rows:
        return
    conn.executemany(TRADE_UPSERT_SQL, rows)
    conn.commit()


def trade_row_to_detector_payload(row: dict) -> dict:
    """Project a canonical trade row into the Phase 3 detector-input contract."""
    return {
        "trade_id": row.get("trade_id"),
        "market_id": row.get("market_id"),
        "asset_id": row.get("asset_id"),
        "condition_id": row.get("condition_id"),
        "proxy_wallet": row.get("proxy_wallet"),
        "transaction_hash": row.get("transaction_hash"),
        "outcome_side": row.get("outcome_side"),
        "side": row.get("side"),
        "price": row.get("price"),
        "size": row.get("size"),
        "usdc_notional": row.get("usdc_notional"),
        "trade_time": row.get("trade_time"),
        "captured_at": row.get("captured_at"),
        "source": row.get("source"),
        "dedupe_key": row.get("dedupe_key"),
        "source_priority": row.get("source_priority"),
    }
