from __future__ import annotations

import argparse
import json
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from uuid import uuid4

from config.runtime_env import load_runtime_env
from database.db_manager import apply_schema, get_conn


REPORT_DIR = Path("reports/phase12")
POSITION_SOURCE = "canonical_trades_net_estimate_v1"


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _iso(value: datetime | None) -> str | None:
    if value is None:
        return None
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).isoformat()


def _parse_dt(value: Any) -> datetime | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = f"{text[:-1]}+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        try:
            parsed = datetime.strptime(text, "%Y-%m-%d %H:%M:%S")
        except ValueError:
            return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _float(value: Any) -> float:
    try:
        if value is None:
            return 0.0
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _text(value: Any, *, default: str = "") -> str:
    text = str(value or "").strip()
    return text or default


def _wallet_id(value: Any) -> str | None:
    wallet = _text(value).lower()
    return wallet or None


@dataclass
class PositionAgg:
    wallet_id: str
    market_id: str
    outcome_side: str
    condition_counts: Counter[str]
    first_trade_at: datetime | None = None
    last_trade_at: datetime | None = None
    trade_count: int = 0
    buy_count: int = 0
    sell_count: int = 0
    gross_buy_size: float = 0.0
    gross_sell_size: float = 0.0
    gross_buy_notional: float = 0.0
    gross_sell_notional: float = 0.0
    net_size: float = 0.0
    net_notional: float = 0.0
    min_price: float | None = None
    max_price: float | None = None
    trade_ids: list[str] | None = None

    def observe(self, row: dict[str, Any]) -> None:
        side = _text(row.get("side")).upper()
        size = _float(row.get("size"))
        price = _float(row.get("price"))
        notional = _float(row.get("usdc_notional"))
        if notional <= 0 and price > 0:
            notional = price * size

        signed_multiplier = -1.0 if side == "SELL" else 1.0
        if side == "SELL":
            self.sell_count += 1
            self.gross_sell_size += size
            self.gross_sell_notional += notional
        else:
            self.buy_count += 1
            self.gross_buy_size += size
            self.gross_buy_notional += notional

        self.trade_count += 1
        self.net_size += signed_multiplier * size
        self.net_notional += signed_multiplier * notional

        condition_id = _text(row.get("condition_id"))
        if condition_id:
            self.condition_counts[condition_id] += 1

        trade_time = _parse_dt(row.get("trade_time")) or _parse_dt(row.get("captured_at"))
        if trade_time is not None:
            if self.first_trade_at is None or trade_time < self.first_trade_at:
                self.first_trade_at = trade_time
            if self.last_trade_at is None or trade_time > self.last_trade_at:
                self.last_trade_at = trade_time

        if price > 0:
            self.min_price = price if self.min_price is None else min(self.min_price, price)
            self.max_price = price if self.max_price is None else max(self.max_price, price)

        trade_id = _text(row.get("trade_id"))
        if trade_id:
            if self.trade_ids is None:
                self.trade_ids = []
            if len(self.trade_ids) < 5:
                self.trade_ids.append(trade_id)

    def row(self, *, captured_at: datetime) -> dict[str, Any]:
        condition_id = self.condition_counts.most_common(1)[0][0] if self.condition_counts else None
        avg_buy_price = self.gross_buy_notional / self.gross_buy_size if self.gross_buy_size else None
        avg_sell_price = self.gross_sell_notional / self.gross_sell_size if self.gross_sell_size else None
        payload = {
            "source": POSITION_SOURCE,
            "basis": "netted canonical_trades by wallet, market, and outcome_side",
            "caveat": "This is a trade-derived estimate, not an exchange/provider open-position snapshot.",
            "trade_count": self.trade_count,
            "buy_count": self.buy_count,
            "sell_count": self.sell_count,
            "gross_buy_size": round(self.gross_buy_size, 8),
            "gross_sell_size": round(self.gross_sell_size, 8),
            "gross_buy_notional": round(self.gross_buy_notional, 8),
            "gross_sell_notional": round(self.gross_sell_notional, 8),
            "avg_buy_price": round(avg_buy_price, 8) if avg_buy_price is not None else None,
            "avg_sell_price": round(avg_sell_price, 8) if avg_sell_price is not None else None,
            "min_price": round(self.min_price, 8) if self.min_price is not None else None,
            "max_price": round(self.max_price, 8) if self.max_price is not None else None,
            "first_trade_at": _iso(self.first_trade_at),
            "last_trade_at": _iso(self.last_trade_at),
            "sample_trade_ids": self.trade_ids or [],
        }
        return {
            "wallet_id": self.wallet_id,
            "market_id": self.market_id,
            "condition_id": condition_id,
            "outcome_side": self.outcome_side,
            "position_size": round(self.net_size, 8),
            "position_notional": round(self.net_notional, 8),
            "position_source": POSITION_SOURCE,
            "position_json": json.dumps(payload, sort_keys=True),
            "captured_at": _iso(captured_at),
        }


def _fetch_wallet_allowlist(conn, *, wallet_limit: int) -> set[str] | None:
    if wallet_limit <= 0:
        return None
    rows = conn.execute(
        """
        SELECT wallet_id
        FROM wallet_activity
        ORDER BY notional_24h DESC, notional_total DESC, last_trade_at DESC
        LIMIT ?
        """,
        (wallet_limit,),
    ).fetchall()
    return {str(row["wallet_id"]).lower() for row in rows}


def _fetch_canonical_trades(conn) -> list[dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT
            trade_id,
            market_id,
            condition_id,
            proxy_wallet,
            outcome_side,
            side,
            price,
            size,
            usdc_notional,
            trade_time,
            captured_at
        FROM canonical_trades
        WHERE proxy_wallet IS NOT NULL
          AND TRIM(proxy_wallet) != ''
          AND market_id IS NOT NULL
          AND TRIM(market_id) != ''
        """
    ).fetchall()
    return [{key: row[key] for key in row.keys()} for row in rows]


def _insert_many(conn, table: str, rows: list[dict[str, Any]]) -> None:
    if not rows:
        return
    columns = list(rows[0].keys())
    placeholders = ", ".join(f":{column}" for column in columns)
    updates = ", ".join(f"{column}=excluded.{column}" for column in columns if column not in {"wallet_id", "market_id", "outcome_side"})
    column_sql = ", ".join(columns)
    conn.executemany(
        f"""
        INSERT INTO {table} ({column_sql}) VALUES ({placeholders})
        ON CONFLICT(wallet_id, market_id, outcome_side) DO UPDATE SET {updates}
        """,
        rows,
    )


def _top_positions(position_rows: list[dict[str, Any]], *, limit: int = 10) -> list[dict[str, Any]]:
    ordered = sorted(position_rows, key=lambda row: abs(float(row["position_notional"] or 0.0)), reverse=True)
    return [
        {
            "wallet_id": row["wallet_id"],
            "market_id": row["market_id"],
            "condition_id": row["condition_id"],
            "outcome_side": row["outcome_side"],
            "position_size": row["position_size"],
            "position_notional": row["position_notional"],
        }
        for row in ordered[:limit]
    ]


def refresh_wallet_positions(
    *,
    output_path: Path,
    wallet_limit: int = 0,
    min_abs_size: float = 1e-9,
    dry_run: bool = False,
) -> dict[str, Any]:
    started_at = _utc_now()
    run_id = uuid4().hex
    conn = get_conn()
    try:
        allowlist = _fetch_wallet_allowlist(conn, wallet_limit=wallet_limit)
        trade_rows = _fetch_canonical_trades(conn)
        aggregates: dict[tuple[str, str, str], PositionAgg] = {}
        linked_trade_count = 0

        for row in trade_rows:
            wallet_id = _wallet_id(row.get("proxy_wallet"))
            if wallet_id is None:
                continue
            if allowlist is not None and wallet_id not in allowlist:
                continue
            market_id = _text(row.get("market_id"))
            outcome_side = _text(row.get("outcome_side"), default="UNKNOWN").upper()
            if not market_id:
                continue
            linked_trade_count += 1
            key = (wallet_id, market_id, outcome_side)
            aggregate = aggregates.get(key)
            if aggregate is None:
                aggregate = PositionAgg(
                    wallet_id=wallet_id,
                    market_id=market_id,
                    outcome_side=outcome_side,
                    condition_counts=Counter(),
                )
                aggregates[key] = aggregate
            aggregate.observe(row)

        completed_at = _utc_now()
        position_rows = [
            aggregate.row(captured_at=completed_at)
            for aggregate in aggregates.values()
            if abs(aggregate.net_size) > min_abs_size
        ]
        wallets_with_positions = {row["wallet_id"] for row in position_rows}
        markets_with_positions = {row["market_id"] for row in position_rows}
        summary = {
            "run_id": run_id,
            "status": "dry_run" if dry_run else "completed",
            "started_at": _iso(started_at),
            "completed_at": _iso(completed_at),
            "position_source": POSITION_SOURCE,
            "wallet_limit": wallet_limit,
            "min_abs_size": min_abs_size,
            "canonical_trade_rows_scanned": len(trade_rows),
            "wallet_linked_trade_rows_used": linked_trade_count,
            "position_rows": len(position_rows),
            "wallets_with_positions": len(wallets_with_positions),
            "markets_with_positions": len(markets_with_positions),
            "top_positions_by_abs_notional": _top_positions(position_rows),
        }
        payload = {
            "summary": summary,
            "acceptance": {
                "positions_nonzero": len(position_rows) > 0,
                "source_is_explicitly_trade_derived": POSITION_SOURCE,
                "provider_snapshot_claimed": False,
            },
        }
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")

        if not dry_run:
            conn.execute("DELETE FROM wallet_positions WHERE position_source = ?", (POSITION_SOURCE,))
            _insert_many(conn, "wallet_positions", position_rows)
            conn.execute(
                """
                INSERT INTO wallet_entity_runs (
                    wallet_entity_run_id,
                    run_type,
                    status,
                    started_at,
                    completed_at,
                    wallet_count,
                    activity_row_count,
                    profile_row_count,
                    cluster_row_count,
                    config_json,
                    summary_json,
                    output_path,
                    notes
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    run_id,
                    "wallet_position_refresh",
                    "completed",
                    _iso(started_at),
                    _iso(completed_at),
                    len(wallets_with_positions),
                    0,
                    0,
                    0,
                    json.dumps(
                        {
                            "position_source": POSITION_SOURCE,
                            "wallet_limit": wallet_limit,
                            "min_abs_size": min_abs_size,
                        },
                        sort_keys=True,
                    ),
                    json.dumps(summary, sort_keys=True),
                    str(output_path),
                    "Phase 12 trade-derived wallet position estimates from canonical trades.",
                ),
            )
            conn.commit()
        return payload
    finally:
        conn.close()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Refresh trade-derived wallet position estimates from canonical trades.")
    parser.add_argument("--env-file", default=None, help="Runtime env file to load before connecting.")
    parser.add_argument(
        "--wallet-limit",
        type=int,
        default=0,
        help="Limit to top wallets from wallet_activity; 0 refreshes every wallet seen in canonical trades.",
    )
    parser.add_argument(
        "--min-abs-size",
        type=float,
        default=1e-9,
        help="Drop positions with absolute net size below this threshold.",
    )
    parser.add_argument(
        "--output-path",
        default=str(REPORT_DIR / "wallet_position_refresh.json"),
        help="Path for the position refresh report.",
    )
    parser.add_argument("--dry-run", action="store_true", help="Write the report without modifying wallet_positions.")
    parser.add_argument("--json", action="store_true", help="Print the report payload as JSON.")
    return parser


def main() -> int:
    args = build_parser().parse_args()
    load_runtime_env(args.env_file or None, override=True)
    apply_schema()
    payload = refresh_wallet_positions(
        output_path=Path(args.output_path),
        wallet_limit=args.wallet_limit,
        min_abs_size=args.min_abs_size,
        dry_run=args.dry_run,
    )
    if args.json:
        print(json.dumps(payload, indent=2, sort_keys=True))
    else:
        summary = payload["summary"]
        print(f"Position rows: {summary['position_rows']}")
        print(f"Wallets with positions: {summary['wallets_with_positions']}")
        print(f"Report: {args.output_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
