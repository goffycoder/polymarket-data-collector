from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from uuid import uuid4

from config.runtime_env import load_runtime_env
from database.db_manager import apply_schema, get_conn


REPORT_DIR = Path("reports/phase12")
PROFILE_SOURCE = "canonical_wallet_activity_profile_v1"


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _iso(value: datetime | None) -> str | None:
    if value is None:
        return None
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).isoformat()


def _float(value: Any) -> float:
    try:
        if value is None:
            return 0.0
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _int(value: Any) -> int:
    try:
        if value is None:
            return 0
        return int(value)
    except (TypeError, ValueError):
        return 0


def _json_object(value: Any) -> dict[str, Any]:
    if not value:
        return {}
    try:
        parsed = json.loads(str(value))
    except json.JSONDecodeError:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _fetch_wallet_activity(conn, *, wallet_limit: int) -> list[dict[str, Any]]:
    limit_sql = ""
    params: tuple[Any, ...] = ()
    if wallet_limit > 0:
        limit_sql = "LIMIT ?"
        params = (wallet_limit,)
    rows = conn.execute(
        f"""
        SELECT
            w.wallet_id,
            w.proxy_wallet,
            w.first_seen_at,
            w.last_seen_at,
            w.first_market_id,
            w.first_condition_id,
            w.first_trade_id,
            a.trade_count,
            a.buy_count,
            a.sell_count,
            a.market_count,
            a.condition_count,
            a.notional_total,
            a.notional_24h,
            a.notional_7d,
            a.avg_trade_size,
            a.avg_usdc_notional,
            a.top_market_id,
            a.top_condition_id,
            a.feature_json,
            a.materialized_at
        FROM wallet_activity a
        JOIN wallets w ON w.wallet_id = a.wallet_id
        ORDER BY a.notional_24h DESC, a.notional_total DESC, a.last_trade_at DESC
        {limit_sql}
        """,
        params,
    ).fetchall()
    return [{key: row[key] for key in row.keys()} for row in rows]


def _fetch_position_summaries(conn) -> dict[str, dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT
            wallet_id,
            COUNT(*) AS position_count,
            SUM(CASE WHEN position_size > 0 THEN 1 ELSE 0 END) AS long_position_count,
            SUM(CASE WHEN position_size < 0 THEN 1 ELSE 0 END) AS short_position_count,
            SUM(ABS(COALESCE(position_size, 0))) AS gross_position_size,
            SUM(ABS(COALESCE(position_notional, 0))) AS gross_position_notional,
            MAX(ABS(COALESCE(position_notional, 0))) AS max_abs_position_notional
        FROM wallet_positions
        GROUP BY wallet_id
        """
    ).fetchall()
    return {str(row["wallet_id"]).lower(): {key: row[key] for key in row.keys()} for row in rows}


def _fetch_top_positions(conn, *, per_wallet_limit: int) -> dict[str, list[dict[str, Any]]]:
    rows = conn.execute(
        """
        SELECT
            wallet_id,
            market_id,
            condition_id,
            outcome_side,
            position_size,
            position_notional,
            position_source
        FROM wallet_positions
        ORDER BY wallet_id, ABS(COALESCE(position_notional, 0)) DESC
        """
    ).fetchall()
    top_by_wallet: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        wallet_id = str(row["wallet_id"]).lower()
        bucket = top_by_wallet.setdefault(wallet_id, [])
        if len(bucket) >= per_wallet_limit:
            continue
        bucket.append(
            {
                "market_id": row["market_id"],
                "condition_id": row["condition_id"],
                "outcome_side": row["outcome_side"],
                "position_size": row["position_size"],
                "position_notional": row["position_notional"],
                "position_source": row["position_source"],
            }
        )
    return top_by_wallet


def _fetch_cluster_summaries(conn) -> dict[str, dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT
            wallet_id,
            COUNT(*) AS cluster_count,
            MAX(confidence) AS max_cluster_confidence,
            GROUP_CONCAT(cluster_id) AS cluster_ids
        FROM wallet_cluster_membership
        GROUP BY wallet_id
        """
    ).fetchall()
    return {str(row["wallet_id"]).lower(): {key: row[key] for key in row.keys()} for row in rows}


def _profile_row(
    activity: dict[str, Any],
    *,
    position_summary: dict[str, Any] | None,
    top_positions: list[dict[str, Any]],
    cluster_summary: dict[str, Any] | None,
    captured_at: datetime,
) -> dict[str, Any]:
    wallet_id = str(activity["wallet_id"]).lower()
    feature_json = _json_object(activity.get("feature_json"))
    position_summary = position_summary or {}
    cluster_summary = cluster_summary or {}
    profile = {
        "source": PROFILE_SOURCE,
        "profile_kind": "trade_derived_activity_summary",
        "provider_profile_available": False,
        "caveats": [
            "Profile is derived from canonical trades and materialized activity, not a provider identity endpoint.",
            "Positions are netted from observed trades and should be treated as estimates.",
        ],
        "wallet": {
            "wallet_id": wallet_id,
            "proxy_wallet": activity.get("proxy_wallet"),
            "first_seen_at": activity.get("first_seen_at"),
            "last_seen_at": activity.get("last_seen_at"),
            "first_market_id": activity.get("first_market_id"),
            "first_condition_id": activity.get("first_condition_id"),
            "first_trade_id": activity.get("first_trade_id"),
        },
        "activity": {
            "trade_count": _int(activity.get("trade_count")),
            "buy_count": _int(activity.get("buy_count")),
            "sell_count": _int(activity.get("sell_count")),
            "market_count": _int(activity.get("market_count")),
            "condition_count": _int(activity.get("condition_count")),
            "notional_total": round(_float(activity.get("notional_total")), 8),
            "notional_24h": round(_float(activity.get("notional_24h")), 8),
            "notional_7d": round(_float(activity.get("notional_7d")), 8),
            "avg_trade_size": round(_float(activity.get("avg_trade_size")), 8),
            "avg_usdc_notional": round(_float(activity.get("avg_usdc_notional")), 8),
            "top_market_id": activity.get("top_market_id"),
            "top_condition_id": activity.get("top_condition_id"),
            "top_markets": feature_json.get("top_markets", []),
            "top_conditions": feature_json.get("top_conditions", []),
            "sources": feature_json.get("sources", []),
            "activity_materialized_at": activity.get("materialized_at"),
        },
        "positions": {
            "position_count": _int(position_summary.get("position_count")),
            "long_position_count": _int(position_summary.get("long_position_count")),
            "short_position_count": _int(position_summary.get("short_position_count")),
            "gross_position_size": round(_float(position_summary.get("gross_position_size")), 8),
            "gross_position_notional": round(_float(position_summary.get("gross_position_notional")), 8),
            "max_abs_position_notional": round(_float(position_summary.get("max_abs_position_notional")), 8),
            "top_positions_by_abs_notional": top_positions,
        },
        "clusters": {
            "cluster_count": _int(cluster_summary.get("cluster_count")),
            "max_cluster_confidence": round(_float(cluster_summary.get("max_cluster_confidence")), 6),
            "cluster_ids": str(cluster_summary.get("cluster_ids") or "").split(",")
            if cluster_summary.get("cluster_ids")
            else [],
        },
    }
    return {
        "wallet_id": wallet_id,
        "profile_source": PROFILE_SOURCE,
        "profile_status": "trade_derived",
        "profile_json": json.dumps(profile, sort_keys=True),
        "profile_captured_at": _iso(captured_at),
        "error_message": None,
        "updated_at": _iso(captured_at),
    }


def _upsert_profiles(conn, rows: list[dict[str, Any]]) -> None:
    if not rows:
        return
    conn.executemany(
        """
        INSERT INTO wallet_profiles (
            wallet_id,
            profile_source,
            profile_status,
            profile_json,
            profile_captured_at,
            error_message,
            updated_at
        ) VALUES (
            :wallet_id,
            :profile_source,
            :profile_status,
            :profile_json,
            :profile_captured_at,
            :error_message,
            :updated_at
        )
        ON CONFLICT(wallet_id) DO UPDATE SET
            profile_source=excluded.profile_source,
            profile_status=excluded.profile_status,
            profile_json=excluded.profile_json,
            profile_captured_at=excluded.profile_captured_at,
            error_message=excluded.error_message,
            updated_at=excluded.updated_at
        """,
        rows,
    )


def refresh_wallet_profiles(
    *,
    output_path: Path,
    wallet_limit: int = 0,
    top_positions_per_wallet: int = 5,
    dry_run: bool = False,
) -> dict[str, Any]:
    started_at = _utc_now()
    run_id = uuid4().hex
    conn = get_conn()
    try:
        activity_rows = _fetch_wallet_activity(conn, wallet_limit=wallet_limit)
        position_summaries = _fetch_position_summaries(conn)
        top_positions = _fetch_top_positions(conn, per_wallet_limit=top_positions_per_wallet)
        cluster_summaries = _fetch_cluster_summaries(conn)
        completed_at = _utc_now()
        profile_rows = [
            _profile_row(
                activity,
                position_summary=position_summaries.get(str(activity["wallet_id"]).lower()),
                top_positions=top_positions.get(str(activity["wallet_id"]).lower(), []),
                cluster_summary=cluster_summaries.get(str(activity["wallet_id"]).lower()),
                captured_at=completed_at,
            )
            for activity in activity_rows
        ]
        with_positions = sum(1 for row in profile_rows if str(row["wallet_id"]).lower() in position_summaries)
        summary = {
            "run_id": run_id,
            "status": "dry_run" if dry_run else "completed",
            "started_at": _iso(started_at),
            "completed_at": _iso(completed_at),
            "profile_source": PROFILE_SOURCE,
            "wallet_limit": wallet_limit,
            "top_positions_per_wallet": top_positions_per_wallet,
            "wallet_activity_rows": len(activity_rows),
            "profile_rows": len(profile_rows),
            "profiles_with_positions": with_positions,
            "wallet_position_summary_rows": len(position_summaries),
            "cluster_summary_rows": len(cluster_summaries),
            "top_profiles_by_24h_notional": [
                {
                    "wallet_id": row["wallet_id"],
                    "notional_24h": row["notional_24h"],
                    "notional_total": row["notional_total"],
                    "trade_count": row["trade_count"],
                    "top_market_id": row["top_market_id"],
                    "top_condition_id": row["top_condition_id"],
                }
                for row in activity_rows[:10]
            ],
        }
        payload = {
            "summary": summary,
            "acceptance": {
                "profiles_nonzero": len(profile_rows) > 0,
                "profile_source_is_explicitly_trade_derived": PROFILE_SOURCE,
                "provider_profile_claimed": False,
            },
        }
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")

        if not dry_run:
            _upsert_profiles(conn, profile_rows)
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
                    "wallet_profile_refresh",
                    "completed",
                    _iso(started_at),
                    _iso(completed_at),
                    len(profile_rows),
                    len(activity_rows),
                    len(profile_rows),
                    0,
                    json.dumps(
                        {
                            "profile_source": PROFILE_SOURCE,
                            "wallet_limit": wallet_limit,
                            "top_positions_per_wallet": top_positions_per_wallet,
                        },
                        sort_keys=True,
                    ),
                    json.dumps(summary, sort_keys=True),
                    str(output_path),
                    "Phase 12 trade-derived wallet profiles from wallet_activity, wallet_positions, and clusters.",
                ),
            )
            conn.commit()
        return payload
    finally:
        conn.close()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Refresh trade-derived wallet profiles from wallet activity and positions.")
    parser.add_argument("--env-file", default=None, help="Runtime env file to load before connecting.")
    parser.add_argument(
        "--wallet-limit",
        type=int,
        default=0,
        help="Limit to top wallet_activity rows; 0 refreshes every materialized wallet.",
    )
    parser.add_argument(
        "--top-positions-per-wallet",
        type=int,
        default=5,
        help="Number of largest estimated positions embedded in each profile JSON.",
    )
    parser.add_argument(
        "--output-path",
        default=str(REPORT_DIR / "wallet_profile_refresh.json"),
        help="Path for the profile refresh report.",
    )
    parser.add_argument("--dry-run", action="store_true", help="Write the report without modifying wallet_profiles.")
    parser.add_argument("--json", action="store_true", help="Print the report payload as JSON.")
    return parser


def main() -> int:
    args = build_parser().parse_args()
    load_runtime_env(args.env_file or None, override=True)
    apply_schema()
    payload = refresh_wallet_profiles(
        output_path=Path(args.output_path),
        wallet_limit=args.wallet_limit,
        top_positions_per_wallet=args.top_positions_per_wallet,
        dry_run=args.dry_run,
    )
    if args.json:
        print(json.dumps(payload, indent=2, sort_keys=True))
    else:
        summary = payload["summary"]
        print(f"Profile rows: {summary['profile_rows']}")
        print(f"Profiles with positions: {summary['profiles_with_positions']}")
        print(f"Report: {args.output_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
