from __future__ import annotations

import argparse
import hashlib
import json
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from uuid import uuid4

from config.runtime_env import load_runtime_env
from database.db_manager import apply_schema, get_conn


REPORT_DIR = Path("reports/phase12")
CLUSTER_VERSION = "wallet_cluster_heuristic_v1"


def _iso_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _cluster_id(condition_id: str) -> str:
    digest = hashlib.sha1(condition_id.encode("utf-8")).hexdigest()[:12]
    return f"condition_overlap_{digest}"


def _fetch_wallet_activity(conn) -> list[dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT
            wallet_id,
            trade_count,
            market_count,
            condition_count,
            notional_total,
            top_market_id,
            top_condition_id,
            feature_json
        FROM wallet_activity
        WHERE top_condition_id IS NOT NULL
          AND TRIM(top_condition_id) != ''
        """
    ).fetchall()
    return [{key: row[key] for key in row.keys()} for row in rows]


def materialize_clusters(*, output_path: Path, min_wallets: int, dry_run: bool = False) -> dict[str, Any]:
    started_at = _iso_now()
    run_id = uuid4().hex
    conn = get_conn()
    try:
        activity_rows = _fetch_wallet_activity(conn)
        by_condition: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for row in activity_rows:
            by_condition[str(row["top_condition_id"])].append(row)

        memberships: list[dict[str, Any]] = []
        clusters: list[dict[str, Any]] = []
        assigned_at = _iso_now()
        for condition_id, rows in sorted(by_condition.items()):
            if len(rows) < min_wallets:
                continue
            total_notional = sum(float(row["notional_total"] or 0.0) for row in rows)
            cluster_id = _cluster_id(condition_id)
            clusters.append(
                {
                    "cluster_id": cluster_id,
                    "condition_id": condition_id,
                    "wallet_count": len(rows),
                    "notional_total": round(total_notional, 8),
                }
            )
            for row in rows:
                wallet_notional = float(row["notional_total"] or 0.0)
                confidence = 0.5
                if total_notional > 0:
                    confidence += min(0.4, (wallet_notional / total_notional) * 0.4)
                if int(row["trade_count"] or 0) > 1:
                    confidence += 0.1
                features = {
                    "basis": "shared_top_condition",
                    "condition_id": condition_id,
                    "top_market_id": row["top_market_id"],
                    "wallet_trade_count": row["trade_count"],
                    "wallet_notional_total": row["notional_total"],
                    "cluster_wallet_count": len(rows),
                    "cluster_notional_total": round(total_notional, 8),
                }
                memberships.append(
                    {
                        "wallet_id": row["wallet_id"],
                        "cluster_id": cluster_id,
                        "cluster_version": CLUSTER_VERSION,
                        "method": "heuristic_shared_top_condition",
                        "confidence": round(min(confidence, 1.0), 6),
                        "features_json": json.dumps(features, sort_keys=True),
                        "assigned_at": assigned_at,
                    }
                )

        completed_at = _iso_now()
        summary = {
            "run_id": run_id,
            "status": "dry_run" if dry_run else "completed",
            "started_at": started_at,
            "completed_at": completed_at,
            "cluster_version": CLUSTER_VERSION,
            "min_wallets": min_wallets,
            "wallet_activity_rows": len(activity_rows),
            "cluster_count": len(clusters),
            "membership_count": len(memberships),
            "top_clusters": sorted(clusters, key=lambda item: item["notional_total"], reverse=True)[:10],
        }
        payload = {
            "summary": summary,
            "acceptance": {
                "wallet_activity_available": len(activity_rows) > 0,
                "cluster_memberships_nonzero": len(memberships) > 0,
                "method_is_statistical_not_identity": True,
            },
        }
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")

        if not dry_run:
            conn.execute(
                "DELETE FROM wallet_cluster_membership WHERE cluster_version = ?",
                (CLUSTER_VERSION,),
            )
            if memberships:
                conn.executemany(
                    """
                    INSERT INTO wallet_cluster_membership (
                        wallet_id,
                        cluster_id,
                        cluster_version,
                        method,
                        confidence,
                        features_json,
                        assigned_at
                    ) VALUES (
                        :wallet_id,
                        :cluster_id,
                        :cluster_version,
                        :method,
                        :confidence,
                        :features_json,
                        :assigned_at
                    )
                    """,
                    memberships,
                )
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
                    "wallet_cluster_materialization",
                    "completed",
                    started_at,
                    completed_at,
                    len({row["wallet_id"] for row in memberships}),
                    len(activity_rows),
                    0,
                    len(memberships),
                    json.dumps({"cluster_version": CLUSTER_VERSION, "min_wallets": min_wallets}, sort_keys=True),
                    json.dumps(summary, sort_keys=True),
                    str(output_path),
                    "Phase 12 heuristic wallet cluster materialization from wallet_activity.",
                ),
            )
            conn.commit()
        return payload
    finally:
        conn.close()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Materialize heuristic wallet cluster membership from wallet activity.")
    parser.add_argument("--env-file", default=None, help="Runtime env file to load before connecting.")
    parser.add_argument("--min-wallets", type=int, default=2, help="Minimum wallets sharing a top condition to form a cluster.")
    parser.add_argument(
        "--output-path",
        default=str(REPORT_DIR / "wallet_cluster_materialization.json"),
        help="Path for the cluster report.",
    )
    parser.add_argument("--dry-run", action="store_true", help="Write the report without modifying cluster rows.")
    parser.add_argument("--json", action="store_true", help="Print the report payload as JSON.")
    return parser


def main() -> int:
    args = build_parser().parse_args()
    load_runtime_env(args.env_file or None, override=True)
    apply_schema()
    payload = materialize_clusters(
        output_path=Path(args.output_path),
        min_wallets=args.min_wallets,
        dry_run=args.dry_run,
    )
    if args.json:
        print(json.dumps(payload, indent=2, sort_keys=True))
    else:
        summary = payload["summary"]
        print(f"Clusters materialized: {summary['cluster_count']}")
        print(f"Membership rows: {summary['membership_count']}")
        print(f"Report: {args.output_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
