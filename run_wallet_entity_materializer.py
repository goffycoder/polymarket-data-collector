from __future__ import annotations

import argparse
import json
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from uuid import uuid4

from config.runtime_env import load_runtime_env
from database.db_manager import apply_schema, get_conn


REPORT_DIR = Path("reports/phase12")


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


def _wallet_id(value: Any) -> str | None:
    wallet = str(value or "").strip().lower()
    return wallet or None


@dataclass
class WalletAgg:
    wallet_id: str
    first_seen_at: datetime | None = None
    last_seen_at: datetime | None = None
    first_trade_id: str | None = None
    first_market_id: str | None = None
    first_condition_id: str | None = None
    first_source: str | None = None
    trade_count: int = 0
    buy_count: int = 0
    sell_count: int = 0
    size_total: float = 0.0
    notional_total: float = 0.0
    notional_24h: float = 0.0
    notional_7d: float = 0.0
    market_counts: Counter[str] = field(default_factory=Counter)
    condition_counts: Counter[str] = field(default_factory=Counter)
    sources: set[str] = field(default_factory=set)

    def observe(self, row: dict[str, Any], *, reference_time: datetime) -> None:
        trade_time = _parse_dt(row.get("trade_time")) or _parse_dt(row.get("captured_at"))
        trade_id = str(row.get("trade_id") or "")
        market_id = str(row.get("market_id") or "")
        condition_id = str(row.get("condition_id") or "")
        source = str(row.get("source") or "")
        side = str(row.get("side") or "").upper()
        size = _float(row.get("size"))
        notional = _float(row.get("usdc_notional"))
        if notional <= 0:
            notional = _float(row.get("price")) * size

        self.trade_count += 1
        self.buy_count += 1 if side == "BUY" else 0
        self.sell_count += 1 if side == "SELL" else 0
        self.size_total += size
        self.notional_total += notional
        if market_id:
            self.market_counts[market_id] += 1
        if condition_id:
            self.condition_counts[condition_id] += 1
        if source:
            self.sources.add(source)

        if trade_time is not None:
            if trade_time >= reference_time - timedelta(hours=24):
                self.notional_24h += notional
            if trade_time >= reference_time - timedelta(days=7):
                self.notional_7d += notional
            if self.first_seen_at is None or trade_time < self.first_seen_at:
                self.first_seen_at = trade_time
                self.first_trade_id = trade_id or None
                self.first_market_id = market_id or None
                self.first_condition_id = condition_id or None
                self.first_source = source or None
            if self.last_seen_at is None or trade_time > self.last_seen_at:
                self.last_seen_at = trade_time

    def wallet_row(self, *, now: datetime) -> dict[str, Any]:
        return {
            "wallet_id": self.wallet_id,
            "proxy_wallet": self.wallet_id,
            "first_seen_at": _iso(self.first_seen_at),
            "last_seen_at": _iso(self.last_seen_at),
            "first_market_id": self.first_market_id,
            "first_condition_id": self.first_condition_id,
            "first_trade_id": self.first_trade_id,
            "source_count": len(self.sources),
            "updated_at": _iso(now),
        }

    def first_seen_row(self, *, now: datetime) -> dict[str, Any]:
        provenance = {
            "source": self.first_source,
            "trade_id": self.first_trade_id,
            "market_id": self.first_market_id,
            "condition_id": self.first_condition_id,
            "materializer": "run_wallet_entity_materializer.py",
        }
        return {
            "wallet_id": self.wallet_id,
            "first_seen_at": _iso(self.first_seen_at) or _iso(now),
            "first_seen_source": self.first_source,
            "first_seen_trade_id": self.first_trade_id,
            "first_seen_market_id": self.first_market_id,
            "first_seen_condition_id": self.first_condition_id,
            "provenance_json": json.dumps(provenance, sort_keys=True),
            "updated_at": _iso(now),
        }

    def activity_row(self, *, now: datetime) -> dict[str, Any]:
        top_market_id = self.market_counts.most_common(1)[0][0] if self.market_counts else None
        top_condition_id = self.condition_counts.most_common(1)[0][0] if self.condition_counts else None
        avg_trade_size = self.size_total / self.trade_count if self.trade_count else 0.0
        avg_notional = self.notional_total / self.trade_count if self.trade_count else 0.0
        features = {
            "top_markets": self.market_counts.most_common(5),
            "top_conditions": self.condition_counts.most_common(5),
            "source_count": len(self.sources),
            "sources": sorted(self.sources),
        }
        return {
            "wallet_id": self.wallet_id,
            "trade_count": self.trade_count,
            "buy_count": self.buy_count,
            "sell_count": self.sell_count,
            "market_count": len(self.market_counts),
            "condition_count": len(self.condition_counts),
            "notional_total": round(self.notional_total, 8),
            "notional_24h": round(self.notional_24h, 8),
            "notional_7d": round(self.notional_7d, 8),
            "avg_trade_size": round(avg_trade_size, 8),
            "avg_usdc_notional": round(avg_notional, 8),
            "first_trade_at": _iso(self.first_seen_at),
            "last_trade_at": _iso(self.last_seen_at),
            "top_market_id": top_market_id,
            "top_condition_id": top_condition_id,
            "feature_json": json.dumps(features, sort_keys=True),
            "materialized_at": _iso(now),
        }


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
            captured_at,
            source
        FROM canonical_trades
        WHERE proxy_wallet IS NOT NULL
          AND TRIM(proxy_wallet) != ''
        """
    ).fetchall()
    return [{key: row[key] for key in row.keys()} for row in rows]


def _clear_materialized_tables(conn) -> None:
    conn.execute("DELETE FROM wallet_activity")
    conn.execute("DELETE FROM wallet_first_seen")
    conn.execute("DELETE FROM wallets")


def _insert_many(conn, table: str, rows: list[dict[str, Any]]) -> None:
    if not rows:
        return
    columns = list(rows[0].keys())
    placeholders = ", ".join(f":{column}" for column in columns)
    column_sql = ", ".join(columns)
    conn.executemany(
        f"INSERT INTO {table} ({column_sql}) VALUES ({placeholders})",
        rows,
    )


def _top_wallets(activity_rows: list[dict[str, Any]], *, limit: int = 10) -> list[dict[str, Any]]:
    ordered = sorted(activity_rows, key=lambda row: float(row["notional_total"] or 0.0), reverse=True)
    return [
        {
            "wallet_id": row["wallet_id"],
            "trade_count": row["trade_count"],
            "market_count": row["market_count"],
            "notional_total": row["notional_total"],
            "last_trade_at": row["last_trade_at"],
            "top_market_id": row["top_market_id"],
        }
        for row in ordered[:limit]
    ]


def materialize_wallet_entities(*, output_path: Path, dry_run: bool = False) -> dict[str, Any]:
    started_at = _utc_now()
    run_id = uuid4().hex
    conn = get_conn()
    try:
        trade_rows = _fetch_canonical_trades(conn)
        reference_time = max(
            (_parse_dt(row.get("trade_time")) or _parse_dt(row.get("captured_at")) or started_at for row in trade_rows),
            default=started_at,
        )
        aggregates: dict[str, WalletAgg] = {}
        for row in trade_rows:
            wallet_id = _wallet_id(row.get("proxy_wallet"))
            if wallet_id is None:
                continue
            aggregate = aggregates.setdefault(wallet_id, WalletAgg(wallet_id=wallet_id))
            aggregate.observe(row, reference_time=reference_time)

        completed_at = _utc_now()
        wallet_rows = [aggregate.wallet_row(now=completed_at) for aggregate in aggregates.values()]
        first_seen_rows = [aggregate.first_seen_row(now=completed_at) for aggregate in aggregates.values()]
        activity_rows = [aggregate.activity_row(now=completed_at) for aggregate in aggregates.values()]
        top_wallets = _top_wallets(activity_rows)
        null_rate = 1.0
        total_trade_count = conn.execute("SELECT COUNT(*) AS count FROM canonical_trades").fetchone()["count"]
        if total_trade_count:
            null_rate = 1 - (len(trade_rows) / float(total_trade_count))

        summary = {
            "run_id": run_id,
            "status": "dry_run" if dry_run else "completed",
            "started_at": _iso(started_at),
            "completed_at": _iso(completed_at),
            "reference_time": _iso(reference_time),
            "canonical_trade_count": total_trade_count,
            "wallet_linked_trade_count": len(trade_rows),
            "wallet_field_null_rate": round(null_rate, 6),
            "wallet_count": len(wallet_rows),
            "wallets_with_first_seen": len(first_seen_rows),
            "activity_rows": len(activity_rows),
            "top_wallets_by_notional": top_wallets,
        }

        payload = {
            "summary": summary,
            "acceptance": {
                "wallet_rows_nonzero": len(wallet_rows) > 0,
                "first_seen_rows_match_wallets": len(first_seen_rows) == len(wallet_rows),
                "activity_rows_match_wallets": len(activity_rows) == len(wallet_rows),
                "idempotent_rebuild_strategy": "delete_and_rebuild_materialized_wallet_tables",
            },
        }
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")

        if not dry_run:
            _clear_materialized_tables(conn)
            _insert_many(conn, "wallets", wallet_rows)
            _insert_many(conn, "wallet_first_seen", first_seen_rows)
            _insert_many(conn, "wallet_activity", activity_rows)
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
                ) VALUES (
                    :wallet_entity_run_id,
                    :run_type,
                    :status,
                    :started_at,
                    :completed_at,
                    :wallet_count,
                    :activity_row_count,
                    :profile_row_count,
                    :cluster_row_count,
                    :config_json,
                    :summary_json,
                    :output_path,
                    :notes
                )
                """,
                {
                    "wallet_entity_run_id": run_id,
                    "run_type": "canonical_trades_materialization",
                    "status": "completed",
                    "started_at": _iso(started_at),
                    "completed_at": _iso(completed_at),
                    "wallet_count": len(wallet_rows),
                    "activity_row_count": len(activity_rows),
                    "profile_row_count": 0,
                    "cluster_row_count": 0,
                    "config_json": json.dumps({"source": "canonical_trades"}, sort_keys=True),
                    "summary_json": json.dumps(summary, sort_keys=True),
                    "output_path": str(output_path),
                    "notes": "Phase 12 first-class wallet entity materialization from canonical trades.",
                },
            )
            conn.commit()
        return payload
    finally:
        conn.close()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Materialize first-class wallet entities from canonical trades.")
    parser.add_argument(
        "--output-path",
        default=str(REPORT_DIR / "wallet_entity_materialization.json"),
        help="Path for the materialization report.",
    )
    parser.add_argument("--env-file", default=None, help="Runtime env file to load before connecting.")
    parser.add_argument("--dry-run", action="store_true", help="Write the report without modifying wallet tables.")
    parser.add_argument("--json", action="store_true", help="Print the report payload as JSON.")
    return parser


def main() -> int:
    args = build_parser().parse_args()
    load_runtime_env(args.env_file or None, override=True)
    apply_schema()
    payload = materialize_wallet_entities(output_path=Path(args.output_path), dry_run=args.dry_run)
    if args.json:
        print(json.dumps(payload, indent=2, sort_keys=True))
    else:
        summary = payload["summary"]
        print(f"Wallets materialized: {summary['wallet_count']}")
        print(f"Wallet-linked trades: {summary['wallet_linked_trade_count']}/{summary['canonical_trade_count']}")
        print(f"Report: {args.output_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
