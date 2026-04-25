"""Normalized envelope reconstruction for Person 2 Phase 2.

This module converts raw archive records into detector-facing normalized
envelopes while preserving input order. It does not perform replay, ordering
changes, or deduplication.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from typing import Any, Iterable, Iterator

from validation.phase2_archive_reader import RawArchiveRecord


CONTRACT_VERSION = "1.0"
DECIMAL_PLACES = Decimal("0.000000000001")
SOURCE_SYSTEM_PRIORITIES = {
    "ws_market": 1,
    "clob_rest": 2,
    "data_api": 3,
    "gamma": 4,
}
TARGET_TABLES = {
    "trade": "trades",
    "market_snapshot": "snapshots",
    "order_book_snapshot": "order_book_snapshots",
    "market_resolution": "market_resolutions",
}
ALL_ENVELOPE_FIELDS = (
    "contract_version",
    "envelope_id",
    "raw_event_uuid",
    "record_index",
    "record_type",
    "source_system",
    "source_endpoint",
    "collector_source",
    "collector_version",
    "raw_schema_version",
    "manifest_id",
    "archive_uri",
    "payload_hash",
    "event_time",
    "event_time_source",
    "ingest_time",
    "target_table",
    "market_id",
    "condition_id",
    "event_id",
    "asset_id",
    "wallet_id",
    "source_event_id",
    "transaction_hash",
    "watermark_time",
    "trade_id",
    "price",
    "size",
    "side",
    "yes_price",
    "no_price",
    "best_bid",
    "best_ask",
    "last_trade_price",
    "bids_json",
    "asks_json",
    "final_price",
    "outcome_side",
    "usdc_notional",
    "dedupe_key",
    "source_priority",
    "spread",
    "mid_price",
    "depth_bids",
    "depth_asks",
    "bid_volume",
    "ask_volume",
    "resolution_outcome",
)


@dataclass(slots=True)
class ReconstructionRejection:
    """Represent one explicit normalization failure."""

    reason_code: str
    message: str
    raw_event_uuid: str
    manifest_id: str
    archive_uri: str
    record_index: int
    source_system: str
    source_endpoint: str


def _reconstruct_envelopes(
    raw_records: Iterable[RawArchiveRecord],
) -> tuple[Iterator[dict[str, Any]], list[ReconstructionRejection]]:
    """Reconstruct normalized envelopes from raw records without reordering."""

    rejected_records: list[ReconstructionRejection] = []

    def _iterator() -> Iterator[dict[str, Any]]:
        for raw_record in raw_records:
            payload_items = _payload_items(raw_record.payload_json)
            for record_index, payload_item in enumerate(payload_items):
                try:
                    envelope = _build_normalized_envelope(
                        raw_record,
                        payload_item=payload_item,
                        record_index=record_index,
                    )
                except ValueError as exc:
                    rejected_records.append(
                        ReconstructionRejection(
                            reason_code=_reason_code_from_error(exc),
                            message=str(exc),
                            raw_event_uuid=raw_record.raw_event_uuid,
                            manifest_id=raw_record.manifest_id,
                            archive_uri=raw_record.archive_uri,
                            record_index=record_index,
                            source_system=raw_record.source_system,
                            source_endpoint=raw_record.source_endpoint,
                        )
                    )
                    continue
                yield envelope

    return _iterator(), rejected_records


def _build_normalized_envelope(
    raw_record: RawArchiveRecord,
    *,
    payload_item: Any | None = None,
    record_index: int = 0,
) -> dict[str, Any]:
    """Build one normalized envelope from one raw record payload item."""

    payload = raw_record.payload_json if payload_item is None else payload_item
    if not isinstance(payload, dict):
        raise ValueError("invalid_payload_item: expected object")

    record_type = _detect_record_type(raw_record, payload)
    base_envelope = _build_base_envelope(raw_record, payload, record_type, record_index)

    if record_type == "trade":
        mapped_fields = _map_trade_record(raw_record, payload)
    elif record_type == "market_snapshot":
        mapped_fields = _map_market_snapshot_record(raw_record, payload)
    elif record_type == "order_book_snapshot":
        mapped_fields = _map_order_book_snapshot_record(raw_record, payload)
    elif record_type == "market_resolution":
        mapped_fields = _map_market_resolution_record(raw_record, payload)
    else:  # pragma: no cover - protected by _detect_record_type
        raise ValueError(f"unsupported_record_type: {record_type}")

    envelope = {field_name: None for field_name in ALL_ENVELOPE_FIELDS}
    envelope.update(base_envelope)
    envelope.update(mapped_fields)
    _validate_envelope(envelope)
    return envelope


def _map_trade_record(raw_record: RawArchiveRecord, payload: dict[str, Any]) -> dict[str, Any]:
    """Map one raw trade payload into contract fields."""

    market_id = _required_text(_first_present(payload, "market_id"), "market_id", fallback=raw_record.market_id)
    condition_id = _required_text(
        _first_present(payload, "conditionId", "condition_id"),
        "condition_id",
        fallback=raw_record.condition_id,
    )
    asset_id = _required_text(
        _first_present(payload, "asset", "asset_id", "assetId"),
        "asset_id",
        fallback=raw_record.asset_id,
    )
    trade_id = _required_text(
        _first_present(payload, "id", "tradeId", "tradeID"),
        "trade_id",
        fallback=raw_record.source_event_id,
    )
    price = _normalize_decimal(_first_present(payload, "price"), "price")
    size = _normalize_decimal(_first_present(payload, "size", "amount"), "size")
    side = _normalize_enum(_first_present(payload, "side"), "side", allowed={"BUY", "SELL"})
    outcome_side = _normalize_enum(
        _first_present(payload, "outcome", "outcome_side"),
        "outcome_side",
        allowed={"YES", "NO"},
    )
    wallet_id = _normalize_optional_text(
        _first_present(payload, "proxyWallet", "proxy_wallet"),
        fallback=raw_record.wallet_id,
        lowercase=True,
    )
    transaction_hash = _normalize_optional_text(
        _first_present(payload, "transactionHash", "transaction_hash"),
        lowercase=True,
    )
    source_event_id = _normalize_optional_text(_first_present(payload, "id", "tradeId", "tradeID"))
    usdc_notional = _derive_usdc_notional(payload, price, size)
    dedupe_key = _build_dedupe_key(transaction_hash, condition_id, asset_id, side, price, size, raw_record.event_time)

    return {
        "market_id": market_id,
        "condition_id": condition_id,
        "asset_id": asset_id,
        "wallet_id": wallet_id,
        "source_event_id": source_event_id,
        "transaction_hash": transaction_hash,
        "trade_id": trade_id,
        "price": price,
        "size": size,
        "side": side,
        "outcome_side": outcome_side,
        "usdc_notional": usdc_notional,
        "dedupe_key": dedupe_key,
        "source_priority": _source_priority(raw_record.source_system),
    }


def _map_market_snapshot_record(raw_record: RawArchiveRecord, payload: dict[str, Any]) -> dict[str, Any]:
    """Map one raw market snapshot payload into contract fields."""

    market_id = _required_text(
        _first_present(payload, "market_id", "id"),
        "market_id",
        fallback=raw_record.market_id,
    )
    condition_id = _required_text(
        _first_present(payload, "conditionId", "condition_id"),
        "condition_id",
        fallback=raw_record.condition_id,
    )
    asset_id = _normalize_optional_text(_first_present(payload, "asset_id", "token_id"), fallback=raw_record.asset_id)
    yes_price, no_price = _extract_yes_no_prices(payload)
    best_bid = _normalize_optional_decimal(_first_present(payload, "best_bid", "bestBid"))
    best_ask = _normalize_optional_decimal(_first_present(payload, "best_ask", "bestAsk"))
    last_trade_price = _normalize_optional_decimal(
        _first_present(payload, "last_trade_price", "lastTradePrice", "price")
    )
    spread = _normalize_optional_decimal(_first_present(payload, "spread"))
    if spread is None and best_bid is not None and best_ask is not None:
        spread = _normalize_decimal(best_ask - best_bid, "spread")
    mid_price = None
    if best_bid is not None and best_ask is not None:
        mid_price = _normalize_decimal((best_bid + best_ask) / Decimal("2"), "mid_price")

    if all(value is None for value in (yes_price, no_price, best_bid, best_ask, last_trade_price)):
        raise ValueError("missing_required_field: market_snapshot_metrics")

    return {
        "market_id": market_id,
        "condition_id": condition_id,
        "asset_id": asset_id,
        "source_event_id": _normalize_optional_text(_first_present(payload, "id")),
        "event_id": _extract_event_id(payload),
        "yes_price": yes_price,
        "no_price": no_price,
        "best_bid": best_bid,
        "best_ask": best_ask,
        "last_trade_price": last_trade_price,
        "spread": spread,
        "mid_price": mid_price,
    }


def _map_order_book_snapshot_record(raw_record: RawArchiveRecord, payload: dict[str, Any]) -> dict[str, Any]:
    """Map one raw order-book payload into contract fields."""

    market_id = _required_text(
        _first_present(payload, "market_id"),
        "market_id",
        fallback=raw_record.market_id,
    )
    condition_id = _required_text(
        _first_present(payload, "conditionId", "condition_id"),
        "condition_id",
        fallback=raw_record.condition_id,
    )
    asset_id = _required_text(
        _first_present(payload, "asset_id", "token_id", "asset"),
        "asset_id",
        fallback=raw_record.asset_id,
    )
    bids = _normalize_book_side(_first_present(payload, "bids"), "bids")
    asks = _normalize_book_side(_first_present(payload, "asks"), "asks")
    best_bid = bids[0]["price_decimal"] if bids else None
    best_ask = asks[0]["price_decimal"] if asks else None
    spread = None
    if best_bid is not None and best_ask is not None:
        spread = _normalize_decimal(best_ask - best_bid, "spread")

    bid_volume = _normalize_decimal(sum((item["size_decimal"] for item in bids), Decimal("0")), "bid_volume")
    ask_volume = _normalize_decimal(sum((item["size_decimal"] for item in asks), Decimal("0")), "ask_volume")

    return {
        "market_id": market_id,
        "condition_id": condition_id,
        "asset_id": asset_id,
        "bids_json": [{"price": item["price"], "size": item["size"]} for item in bids],
        "asks_json": [{"price": item["price"], "size": item["size"]} for item in asks],
        "best_bid": best_bid,
        "best_ask": best_ask,
        "spread": spread,
        "depth_bids": len(bids),
        "depth_asks": len(asks),
        "bid_volume": bid_volume,
        "ask_volume": ask_volume,
    }


def _map_market_resolution_record(raw_record: RawArchiveRecord, payload: dict[str, Any]) -> dict[str, Any]:
    """Map one raw market-resolution payload into contract fields."""

    market_id = _required_text(
        _first_present(payload, "market_id"),
        "market_id",
        fallback=raw_record.market_id,
    )
    condition_id = _required_text(
        _first_present(payload, "market", "conditionId", "condition_id"),
        "condition_id",
        fallback=raw_record.condition_id,
    )
    asset_id = _required_text(
        _first_present(payload, "asset_id", "asset"),
        "asset_id",
        fallback=raw_record.asset_id,
    )
    final_price = _normalize_decimal(_first_present(payload, "price", "final_price"), "final_price")
    resolution_outcome = "N/A"
    if final_price >= Decimal("0.990000000000"):
        resolution_outcome = "YES"
    elif final_price <= Decimal("0.010000000000"):
        resolution_outcome = "NO"

    return {
        "market_id": market_id,
        "condition_id": condition_id,
        "asset_id": asset_id,
        "final_price": final_price,
        "resolution_outcome": resolution_outcome,
    }


def _build_base_envelope(
    raw_record: RawArchiveRecord,
    payload: dict[str, Any],
    record_type: str,
    record_index: int,
) -> dict[str, Any]:
    """Build the universal contract fields shared across all envelope types."""

    source_system = _normalize_source_system(raw_record.source_system)
    source_endpoint = _required_text(raw_record.source_endpoint, "source_endpoint")
    return {
        "contract_version": CONTRACT_VERSION,
        "envelope_id": _build_envelope_id(source_system, source_endpoint, raw_record.raw_event_uuid, record_index, record_type),
        "raw_event_uuid": raw_record.raw_event_uuid,
        "record_index": record_index,
        "record_type": record_type,
        "source_system": source_system,
        "source_endpoint": source_endpoint,
        "collector_source": _normalize_optional_text(raw_record.collector_source),
        "collector_version": _normalize_optional_text(raw_record.collector_version),
        "raw_schema_version": _normalize_optional_text(raw_record.raw_schema_version),
        "manifest_id": raw_record.manifest_id,
        "archive_uri": raw_record.archive_uri,
        "payload_hash": _required_text(raw_record.payload_hash, "payload_hash"),
        "event_time": _canonical_timestamp(raw_record.event_time),
        "event_time_source": _normalize_event_time_source(raw_record.event_time_source),
        "ingest_time": _canonical_timestamp(raw_record.ingest_time),
        "target_table": TARGET_TABLES[record_type],
        "market_id": None,
        "condition_id": None,
        "event_id": None,
        "asset_id": None,
        "wallet_id": None,
        "source_event_id": _normalize_optional_text(raw_record.source_event_id),
        "transaction_hash": None,
        "watermark_time": _normalize_optional_timestamp(_first_present(payload, "watermark_time")),
    }


def _detect_record_type(raw_record: RawArchiveRecord, payload: dict[str, Any]) -> str:
    """Detect the normalized record type from the raw payload and source metadata."""

    event_type = _normalize_optional_text(_first_present(payload, "event_type"), lowercase=True)
    source_endpoint = _normalize_endpoint_for_id(raw_record.source_endpoint)

    if event_type == "market_resolved":
        return "market_resolution"
    if event_type == "book" or "book" in source_endpoint:
        return "order_book_snapshot"
    if event_type in {"price_change", "best_bid_ask"}:
        return "market_snapshot"
    if source_endpoint == "trades" or event_type == "last_trade_price" or _looks_like_trade_payload(payload):
        return "trade"
    if source_endpoint in {"markets", "prices"}:
        return "market_snapshot"

    raise ValueError("unsupported_record_type: unable to classify payload")
    

def _validate_envelope(envelope: dict[str, Any]) -> None:
    """Validate required fields and record-type-specific contract requirements."""

    universal_required = (
        "contract_version",
        "envelope_id",
        "raw_event_uuid",
        "record_index",
        "record_type",
        "source_system",
        "source_endpoint",
        "manifest_id",
        "archive_uri",
        "payload_hash",
        "event_time",
        "event_time_source",
        "ingest_time",
        "target_table",
        "market_id",
        "condition_id",
    )
    for field_name in universal_required:
        if envelope.get(field_name) is None:
            raise ValueError(f"missing_required_field: {field_name}")
        if isinstance(envelope[field_name], str) and not envelope[field_name].strip():
            raise ValueError(f"missing_required_field: {field_name}")

    record_type = envelope["record_type"]
    if record_type == "trade":
        for field_name in (
            "asset_id",
            "trade_id",
            "price",
            "size",
            "side",
            "outcome_side",
            "usdc_notional",
            "dedupe_key",
            "source_priority",
        ):
            if envelope.get(field_name) is None:
                raise ValueError(f"missing_required_field: {field_name}")
    elif record_type == "market_snapshot":
        if all(envelope.get(field_name) is None for field_name in ("yes_price", "no_price", "best_bid", "best_ask", "last_trade_price")):
            raise ValueError("missing_required_field: market_snapshot_metrics")
    elif record_type == "order_book_snapshot":
        for field_name in ("asset_id", "bids_json", "asks_json", "depth_bids", "depth_asks", "bid_volume", "ask_volume"):
            if envelope.get(field_name) is None:
                raise ValueError(f"missing_required_field: {field_name}")
    elif record_type == "market_resolution":
        for field_name in ("asset_id", "final_price", "resolution_outcome"):
            if envelope.get(field_name) is None:
                raise ValueError(f"missing_required_field: {field_name}")


def _payload_items(payload_json: Any) -> list[Any]:
    """Return payload items in original order without reordering."""

    if isinstance(payload_json, list):
        return list(payload_json)
    return [payload_json]


def _build_envelope_id(
    source_system: str,
    source_endpoint: str,
    raw_event_uuid: str,
    record_index: int,
    record_type: str,
) -> str:
    """Build the deterministic envelope identifier from the contract."""

    return (
        f"{source_system}:"
        f"{_normalize_endpoint_for_id(source_endpoint)}:"
        f"{raw_event_uuid}:{record_index}:{record_type}"
    )


def _normalize_source_system(value: Any) -> str:
    """Normalize and validate one source-system enum."""

    normalized = _normalize_optional_text(value, lowercase=True)
    if normalized not in SOURCE_SYSTEM_PRIORITIES:
        raise ValueError("invalid_enum: source_system")
    return normalized


def _normalize_event_time_source(value: Any) -> str:
    """Normalize and validate the event-time-source enum."""

    normalized = _normalize_optional_text(value, lowercase=True)
    if normalized not in {"provider", "collector_observed"}:
        raise ValueError("invalid_enum: event_time_source")
    return normalized


def _source_priority(source_system: str) -> int:
    """Return the strict source priority derived from source_system only."""

    normalized = _normalize_source_system(source_system)
    return SOURCE_SYSTEM_PRIORITIES[normalized]


def _normalize_decimal(value: Any, field_name: str) -> Decimal:
    """Normalize a numeric value to ``decimal(20,12)``."""

    if isinstance(value, Decimal):
        candidate = value
    else:
        try:
            candidate = Decimal(str(value).strip())
        except (InvalidOperation, AttributeError) as exc:
            raise ValueError(f"invalid_decimal: {field_name}") from exc

    try:
        return candidate.quantize(DECIMAL_PLACES, rounding=ROUND_HALF_UP)
    except InvalidOperation as exc:  # pragma: no cover - defensive quantize guard
        raise ValueError(f"invalid_decimal: {field_name}") from exc


def _normalize_optional_decimal(value: Any) -> Decimal | None:
    """Normalize an optional decimal field."""

    if value is None:
        return None
    if isinstance(value, str) and not value.strip():
        return None
    return _normalize_decimal(value, "optional_decimal")


def _canonical_timestamp(value: Any) -> str:
    """Normalize one timestamp value to canonical RFC3339 UTC."""

    if isinstance(value, datetime):
        dt = value
    else:
        normalized = str(value).strip().replace("Z", "+00:00")
        if not normalized:
            raise ValueError("invalid_timestamp: blank")
        dt = datetime.fromisoformat(normalized)

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def _normalize_optional_timestamp(value: Any) -> str | None:
    """Normalize an optional timestamp field."""

    if value is None:
        return None
    if isinstance(value, str) and not value.strip():
        return None
    return _canonical_timestamp(value)


def _normalize_enum(value: Any, field_name: str, *, allowed: set[str]) -> str:
    """Normalize an uppercase enum and validate it against the allowed set."""

    normalized = _normalize_optional_text(value)
    if normalized is None:
        raise ValueError(f"missing_required_field: {field_name}")
    normalized = normalized.upper()
    if normalized not in allowed:
        raise ValueError(f"invalid_enum: {field_name}")
    return normalized


def _normalize_optional_text(value: Any, fallback: Any = None, *, lowercase: bool = False) -> str | None:
    """Normalize an optional text value to stripped text or ``None``."""

    candidate = value
    if value is None or (isinstance(value, str) and value == ""):
        candidate = fallback
    if candidate is None:
        return None
    text = str(candidate).strip()
    if not text:
        return None
    return text.lower() if lowercase else text


def _required_text(value: Any, field_name: str, *, fallback: Any = None) -> str:
    """Return one required non-blank text field after normalization."""

    normalized = _normalize_optional_text(value, fallback=fallback)
    if normalized is None:
        raise ValueError(f"missing_required_field: {field_name}")
    return normalized


def _first_present(payload: dict[str, Any], *field_names: str) -> Any:
    """Return the first present payload field value."""

    for field_name in field_names:
        if field_name in payload:
            return payload[field_name]
    return None


def _normalize_endpoint_for_id(value: Any) -> str:
    """Normalize source_endpoint for deterministic ID construction."""

    endpoint = _required_text(value, "source_endpoint")
    endpoint = endpoint.lower().strip()
    if endpoint.startswith("/"):
        endpoint = endpoint[1:]
    return endpoint


def _looks_like_trade_payload(payload: dict[str, Any]) -> bool:
    """Return whether a payload has the minimum trade shape."""

    return any(key in payload for key in ("id", "tradeId", "tradeID")) and "price" in payload and any(
        key in payload for key in ("size", "amount")
    )


def _derive_usdc_notional(payload: dict[str, Any], price: Decimal, size: Decimal) -> Decimal:
    """Derive the canonical USDC notional for one trade payload."""

    explicit_value = _first_present(payload, "sizeUsdc", "size_usdc")
    if explicit_value not in {None, ""}:
        return _normalize_decimal(explicit_value, "usdc_notional")
    return _normalize_decimal(price * size, "usdc_notional")


def _build_dedupe_key(
    transaction_hash: str | None,
    condition_id: str,
    asset_id: str,
    side: str,
    price: Decimal,
    size: Decimal,
    event_time: str,
) -> str:
    """Build the normalized dedupe key using the Step 1 contract rules."""

    if transaction_hash:
        return transaction_hash.lower()

    return "|".join(
        [
            condition_id.strip().lower(),
            asset_id.strip().lower(),
            side.strip().upper(),
            _decimal_string(price),
            _decimal_string(size),
            _canonical_timestamp(event_time),
        ]
    )


def _extract_yes_no_prices(payload: dict[str, Any]) -> tuple[Decimal | None, Decimal | None]:
    """Extract YES and NO prices from market snapshot payloads when available."""

    raw_outcomes = _coerce_json_value(_first_present(payload, "outcomes"))
    raw_prices = _coerce_json_value(_first_present(payload, "outcomePrices", "outcome_prices"))
    if not isinstance(raw_prices, list) or not raw_prices:
        return None, None

    outcomes = raw_outcomes if isinstance(raw_outcomes, list) else []
    yes_index = 0
    no_index = 1 if len(raw_prices) > 1 else None
    for index, outcome in enumerate(outcomes):
        label = str(outcome).strip().upper()
        if label == "YES":
            yes_index = index
        elif label == "NO":
            no_index = index

    yes_price = _normalize_optional_decimal(raw_prices[yes_index]) if len(raw_prices) > yes_index else None
    no_price = None
    if no_index is not None and len(raw_prices) > no_index:
        no_price = _normalize_optional_decimal(raw_prices[no_index])
    return yes_price, no_price


def _extract_event_id(payload: dict[str, Any]) -> str | None:
    """Extract the parent event identifier from market metadata payloads."""

    events = _coerce_json_value(_first_present(payload, "events"))
    if isinstance(events, list) and events:
        first_event = events[0]
        if isinstance(first_event, dict):
            return _normalize_optional_text(_first_present(first_event, "id"))
    return _normalize_optional_text(_first_present(payload, "event_id"))


def _normalize_book_side(value: Any, field_name: str) -> list[dict[str, Any]]:
    """Normalize one order-book side into contract JSON shape plus Decimal helpers."""

    entries = _coerce_json_value(value)
    if entries is None:
        return []
    if isinstance(entries, str) and not entries.strip():
        return []
    if not isinstance(entries, list):
        raise ValueError(f"invalid_payload_item: {field_name}")

    normalized_entries: list[dict[str, Any]] = []
    for entry in entries:
        if not isinstance(entry, dict):
            raise ValueError(f"invalid_payload_item: {field_name}")
        price = _normalize_decimal(_first_present(entry, "price"), f"{field_name}.price")
        size = _normalize_decimal(_first_present(entry, "size"), f"{field_name}.size")
        normalized_entries.append(
            {
                "price": _decimal_string(price),
                "size": _decimal_string(size),
                "price_decimal": price,
                "size_decimal": size,
            }
        )
    return normalized_entries


def _coerce_json_value(value: Any) -> Any:
    """Parse a JSON string when needed and otherwise return the original value."""

    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        try:
            return json.loads(text)
        except json.JSONDecodeError:
            return value
    return value


def _decimal_string(value: Decimal) -> str:
    """Render a quantized decimal as a fixed 12-decimal plain string."""

    return format(_normalize_decimal(value, "decimal_string"), "f")


def _reason_code_from_error(exc: ValueError) -> str:
    """Map one mapping error to a stable reconstruction reason code."""

    message = str(exc)
    if message.startswith("unsupported_record_type:"):
        return "unsupported_record_type"
    if message.startswith("missing_required_field:"):
        return "missing_required_field"
    if message.startswith("invalid_enum:"):
        return "invalid_enum"
    if message.startswith("invalid_decimal:"):
        return "invalid_decimal"
    if message.startswith("invalid_payload_item:"):
        return "invalid_payload_item"
    if message.startswith("invalid_timestamp:"):
        return "invalid_timestamp"
    return "reconstruction_error"
