from __future__ import annotations

import json
from collections import defaultdict, deque
from dataclasses import asdict, dataclass
from datetime import timedelta
from hashlib import sha256
from typing import Any

import pandas as pd

from config.settings import (
    PHASE7_GRAPH_FEATURE_SCHEMA_VERSION,
    PHASE7_GRAPH_LOOKBACK_DAYS,
    PHASE7_GRAPH_PERSISTENCE_MIN_DAYS,
)
from database.db_manager import get_conn
from phase5.models import EvaluationRow
from phase5.repository import Phase5Repository, _iso, _parse_iso
from phase6.training import NUMERIC_MODEL_FEATURES, build_training_frame


GRAPH_FEATURE_DEFINITIONS: dict[str, dict[str, str]] = {
    "graph_market_wallet_degree": {
        "description": "Unique wallets connected to the candidate market inside the lookback graph.",
        "family": "market_relationship",
        "stability_note": "Zero means no retained wallet graph was available before decision time.",
    },
    "graph_related_market_count": {
        "description": "Distinct non-candidate markets reached by wallets that traded the candidate market.",
        "family": "market_relationship",
        "stability_note": "Computed from a deterministic one-hop wallet-to-market expansion.",
    },
    "graph_cluster_wallet_count": {
        "description": "Unique wallets in the candidate market's two-hop market-wallet cluster.",
        "family": "cluster_size",
        "stability_note": "Uses only trades at or before the candidate decision timestamp.",
    },
    "graph_cluster_density_2hop": {
        "description": "Observed wallet-market edges divided by possible edges in the two-hop induced cluster.",
        "family": "cluster_structure",
        "stability_note": "Higher values indicate tighter repeated wallet-market reuse.",
    },
    "graph_same_event_market_share": {
        "description": "Share of markets in the two-hop cluster that belong to the candidate event.",
        "family": "event_alignment",
        "stability_note": "Falls to zero when no event mapping is available for related markets.",
    },
    "graph_repeat_wallet_share": {
        "description": "Share of candidate-market wallets that also traded at least one other market in lookback.",
        "family": "wallet_relationship",
        "stability_note": "Captures graph reuse instead of isolated one-off wallets.",
    },
    "graph_cross_event_wallet_share": {
        "description": "Share of candidate-market wallets connected to more than one event in lookback.",
        "family": "wallet_relationship",
        "stability_note": "Measures cross-event portability of wallet activity.",
    },
    "graph_same_event_repeat_wallet_share": {
        "description": "Share of candidate-market wallets that also traded another market from the same event.",
        "family": "event_alignment",
        "stability_note": "Highlights repeated positioning within one event complex.",
    },
    "graph_wallet_persistence_mean_days": {
        "description": "Mean count of distinct active trade days among candidate-market wallets in lookback.",
        "family": "cluster_persistence",
        "stability_note": "Uses retained trade-day counts only; no future trades are included.",
    },
    "graph_persistent_wallet_share": {
        "description": "Share of candidate-market wallets active on at least the configured persistence-day threshold.",
        "family": "cluster_persistence",
        "stability_note": "Threshold is versioned with the graph feature schema settings.",
    },
    "graph_cluster_notional_share": {
        "description": "Candidate market notional divided by total notional in the two-hop cluster.",
        "family": "cluster_structure",
        "stability_note": "Higher values indicate a tighter cluster centered on the candidate market.",
    },
}

GRAPH_FEATURE_COLUMNS = tuple(GRAPH_FEATURE_DEFINITIONS.keys())


@dataclass(slots=True)
class TradeGraphEdge:
    trade_time: Any
    wallet: str
    market_id: str
    event_id: str | None
    trade_day: str
    notional: float


@dataclass(slots=True)
class Phase7GraphFeatureBuildSummary:
    feature_schema_version: str
    dataset_hash: str
    row_count: int
    labeled_row_count: int
    graph_feature_count: int
    baseline_feature_count: int
    stable_graph_feature_count: int
    ready_for_controlled_experiments: bool

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def _dataset_hash(frame: pd.DataFrame) -> str:
    if frame.empty or "decision_timestamp" not in frame.columns or "candidate_id" not in frame.columns:
        return sha256(b"[]").hexdigest()
    records = frame.sort_values(["decision_timestamp", "candidate_id"]).to_dict(orient="records")
    payload = json.dumps(records, sort_keys=True, default=str, separators=(",", ":"))
    return sha256(payload.encode("utf-8")).hexdigest()


def _safe_ratio(numerator: float, denominator: float) -> float:
    if not denominator:
        return 0.0
    return float(numerator) / float(denominator)


def _load_graph_edges(
    *,
    start: str,
    end: str,
) -> tuple[list[TradeGraphEdge], dict[str, str | None]]:
    conn = get_conn()
    try:
        trade_rows = conn.execute(
            """
            SELECT
                t.trade_time,
                t.proxy_wallet,
                t.market_id,
                m.event_id,
                t.usdc_notional,
                t.size,
                t.price
            FROM canonical_trades t
            LEFT JOIN markets m ON m.market_id = t.market_id
            WHERE t.trade_time >= ?
              AND t.trade_time <= ?
              AND t.proxy_wallet IS NOT NULL
              AND TRIM(t.proxy_wallet) <> ''
              AND t.market_id IS NOT NULL
            ORDER BY t.trade_time ASC, t.market_id ASC, t.proxy_wallet ASC
            """,
            (start, end),
        ).fetchall()
        market_rows = conn.execute(
            """
            SELECT market_id, event_id
            FROM markets
            """
        ).fetchall()
    finally:
        conn.close()

    edges: list[TradeGraphEdge] = []
    for row in trade_rows:
        trade_time = _parse_iso(row["trade_time"])
        if trade_time is None:
            continue
        wallet = str(row["proxy_wallet"]).strip()
        market_id = str(row["market_id"]).strip()
        if not wallet or not market_id:
            continue
        if row["usdc_notional"] is not None:
            notional = float(row["usdc_notional"] or 0.0)
        else:
            notional = float(row["size"] or 0.0) * float(row["price"] or 0.0)
        edges.append(
            TradeGraphEdge(
                trade_time=trade_time,
                wallet=wallet,
                market_id=market_id,
                event_id=(str(row["event_id"]).strip() if row["event_id"] is not None else None),
                trade_day=trade_time.date().isoformat(),
                notional=max(0.0, notional),
            )
        )

    market_event_map = {
        str(row["market_id"]).strip(): (str(row["event_id"]).strip() if row["event_id"] is not None else None)
        for row in market_rows
        if row["market_id"] is not None
    }
    return edges, market_event_map


class _TradeGraphState:
    def __init__(self) -> None:
        self.wallet_markets: dict[str, dict[str, int]] = defaultdict(dict)
        self.market_wallets: dict[str, dict[str, int]] = defaultdict(dict)
        self.wallet_event_counts: dict[str, dict[str, int]] = defaultdict(dict)
        self.wallet_day_counts: dict[str, dict[str, int]] = defaultdict(dict)
        self.market_wallet_notional: dict[str, dict[str, float]] = defaultdict(dict)
        self.market_total_notional: dict[str, float] = defaultdict(float)

    def add(self, edge: TradeGraphEdge) -> None:
        wallet_market = self.wallet_markets[edge.wallet]
        wallet_market[edge.market_id] = int(wallet_market.get(edge.market_id, 0)) + 1

        market_wallet = self.market_wallets[edge.market_id]
        market_wallet[edge.wallet] = int(market_wallet.get(edge.wallet, 0)) + 1

        wallet_days = self.wallet_day_counts[edge.wallet]
        wallet_days[edge.trade_day] = int(wallet_days.get(edge.trade_day, 0)) + 1

        if edge.event_id:
            wallet_events = self.wallet_event_counts[edge.wallet]
            wallet_events[edge.event_id] = int(wallet_events.get(edge.event_id, 0)) + 1

        market_wallet_notional = self.market_wallet_notional[edge.market_id]
        market_wallet_notional[edge.wallet] = float(market_wallet_notional.get(edge.wallet, 0.0)) + edge.notional
        self.market_total_notional[edge.market_id] = float(self.market_total_notional.get(edge.market_id, 0.0)) + edge.notional

    def remove(self, edge: TradeGraphEdge) -> None:
        wallet_market = self.wallet_markets.get(edge.wallet, {})
        remaining_market = int(wallet_market.get(edge.market_id, 0)) - 1
        if remaining_market > 0:
            wallet_market[edge.market_id] = remaining_market
        else:
            wallet_market.pop(edge.market_id, None)
        if not wallet_market:
            self.wallet_markets.pop(edge.wallet, None)

        market_wallet = self.market_wallets.get(edge.market_id, {})
        remaining_wallet = int(market_wallet.get(edge.wallet, 0)) - 1
        if remaining_wallet > 0:
            market_wallet[edge.wallet] = remaining_wallet
        else:
            market_wallet.pop(edge.wallet, None)
        if not market_wallet:
            self.market_wallets.pop(edge.market_id, None)

        wallet_days = self.wallet_day_counts.get(edge.wallet, {})
        remaining_days = int(wallet_days.get(edge.trade_day, 0)) - 1
        if remaining_days > 0:
            wallet_days[edge.trade_day] = remaining_days
        else:
            wallet_days.pop(edge.trade_day, None)
        if not wallet_days:
            self.wallet_day_counts.pop(edge.wallet, None)

        if edge.event_id:
            wallet_events = self.wallet_event_counts.get(edge.wallet, {})
            remaining_event = int(wallet_events.get(edge.event_id, 0)) - 1
            if remaining_event > 0:
                wallet_events[edge.event_id] = remaining_event
            else:
                wallet_events.pop(edge.event_id, None)
            if not wallet_events:
                self.wallet_event_counts.pop(edge.wallet, None)

        market_wallet_notional = self.market_wallet_notional.get(edge.market_id, {})
        updated_notional = float(market_wallet_notional.get(edge.wallet, 0.0)) - edge.notional
        if updated_notional > 1e-12:
            market_wallet_notional[edge.wallet] = updated_notional
        else:
            market_wallet_notional.pop(edge.wallet, None)
        if not market_wallet_notional:
            self.market_wallet_notional.pop(edge.market_id, None)

        total_notional = float(self.market_total_notional.get(edge.market_id, 0.0)) - edge.notional
        if total_notional > 1e-12:
            self.market_total_notional[edge.market_id] = total_notional
        else:
            self.market_total_notional.pop(edge.market_id, None)


def _build_graph_feature_row(
    *,
    row: EvaluationRow,
    state: _TradeGraphState,
    market_event_map: dict[str, str | None],
    persistence_min_days: int,
) -> dict[str, float]:
    direct_wallets = sorted(state.market_wallets.get(row.market_id, {}).keys())
    direct_wallet_count = len(direct_wallets)
    if not direct_wallets:
        return {
            feature_name: 0.0
            for feature_name in GRAPH_FEATURE_COLUMNS
        }

    related_markets: set[str] = set()
    repeat_wallet_count = 0
    cross_event_wallet_count = 0
    same_event_repeat_wallet_count = 0
    active_day_counts: list[int] = []

    for wallet in direct_wallets:
        wallet_markets = set(state.wallet_markets.get(wallet, {}).keys())
        if len(wallet_markets) > 1:
            repeat_wallet_count += 1
        related_markets.update(wallet_markets)

        wallet_events = state.wallet_event_counts.get(wallet, {})
        if len(wallet_events) > 1:
            cross_event_wallet_count += 1

        active_days = len(state.wallet_day_counts.get(wallet, {}))
        active_day_counts.append(active_days)
        if row.event_id:
            same_event_other_market = any(
                market_id != row.market_id and market_event_map.get(market_id) == row.event_id
                for market_id in wallet_markets
            )
            if same_event_other_market:
                same_event_repeat_wallet_count += 1

    related_markets.discard(row.market_id)
    cluster_markets = {row.market_id, *related_markets}
    cluster_wallets: set[str] = set(direct_wallets)
    for market_id in cluster_markets:
        cluster_wallets.update(state.market_wallets.get(market_id, {}).keys())

    edge_count = 0
    for market_id in cluster_markets:
        edge_count += sum(
            1
            for wallet in state.market_wallets.get(market_id, {})
            if wallet in cluster_wallets
        )

    same_event_market_share = 0.0
    if row.event_id and cluster_markets:
        same_event_market_share = _safe_ratio(
            sum(1 for market_id in cluster_markets if market_event_map.get(market_id) == row.event_id),
            len(cluster_markets),
        )

    cluster_notional_total = sum(
        float(state.market_total_notional.get(market_id, 0.0))
        for market_id in cluster_markets
    )
    candidate_market_notional = float(state.market_total_notional.get(row.market_id, 0.0))
    possible_edges = max(1, len(cluster_wallets) * len(cluster_markets))
    persistent_wallet_count = sum(1 for days in active_day_counts if days >= persistence_min_days)

    return {
        "graph_market_wallet_degree": float(direct_wallet_count),
        "graph_related_market_count": float(len(related_markets)),
        "graph_cluster_wallet_count": float(len(cluster_wallets)),
        "graph_cluster_density_2hop": round(_safe_ratio(edge_count, possible_edges), 6),
        "graph_same_event_market_share": round(same_event_market_share, 6),
        "graph_repeat_wallet_share": round(_safe_ratio(repeat_wallet_count, direct_wallet_count), 6),
        "graph_cross_event_wallet_share": round(_safe_ratio(cross_event_wallet_count, direct_wallet_count), 6),
        "graph_same_event_repeat_wallet_share": round(
            _safe_ratio(same_event_repeat_wallet_count, direct_wallet_count),
            6,
        ),
        "graph_wallet_persistence_mean_days": round(
            sum(active_day_counts) / len(active_day_counts),
            6,
        ) if active_day_counts else 0.0,
        "graph_persistent_wallet_share": round(
            _safe_ratio(persistent_wallet_count, direct_wallet_count),
            6,
        ),
        "graph_cluster_notional_share": round(
            _safe_ratio(candidate_market_notional, cluster_notional_total),
            6,
        ),
    }


def _feature_diagnostics(frame: pd.DataFrame, *, columns: tuple[str, ...] | list[str]) -> dict[str, Any]:
    diagnostics: dict[str, Any] = {}
    for column in columns:
        if column not in frame.columns:
            diagnostics[column] = {
                "present": False,
                "non_null_ratio": 0.0,
                "non_zero_ratio": 0.0,
                "unique_value_count": 0,
                "variance": 0.0,
                "status": "missing",
            }
            continue
        series = pd.to_numeric(frame[column], errors="coerce")
        non_null_ratio = float(series.notna().mean()) if len(series) else 0.0
        non_zero_ratio = float((series.fillna(0.0).abs() > 1e-12).mean()) if len(series) else 0.0
        unique_value_count = int(series.dropna().nunique())
        variance = float(series.dropna().var()) if unique_value_count > 1 else 0.0
        if non_null_ratio == 0.0:
            status = "missing"
        elif unique_value_count <= 1:
            status = "constant"
        elif non_zero_ratio < 0.05:
            status = "sparse"
        else:
            status = "stable"
        diagnostics[column] = {
            "present": True,
            "non_null_ratio": round(non_null_ratio, 6),
            "non_zero_ratio": round(non_zero_ratio, 6),
            "unique_value_count": unique_value_count,
            "variance": round(variance, 6),
            "status": status,
        }
    return diagnostics


def build_graph_training_frame(
    rows: list[EvaluationRow],
    *,
    repository: Phase5Repository | None = None,
    feature_schema_version: str = PHASE7_GRAPH_FEATURE_SCHEMA_VERSION,
    lookback_days: int = PHASE7_GRAPH_LOOKBACK_DAYS,
    persistence_min_days: int = PHASE7_GRAPH_PERSISTENCE_MIN_DAYS,
) -> tuple[pd.DataFrame, Phase7GraphFeatureBuildSummary, dict[str, Any]]:
    repository = repository or Phase5Repository()
    baseline_frame, baseline_summary = build_training_frame(rows, repository=repository)
    if baseline_frame.empty:
        empty = baseline_frame.copy()
        empty["baseline_feature_schema_version"] = pd.Series(dtype="string")
        for column in GRAPH_FEATURE_COLUMNS:
            empty[column] = pd.Series(dtype="float64")
        diagnostics = {
            "baseline_contract": {
                "expected_columns": list(NUMERIC_MODEL_FEATURES),
                "present_columns": [column for column in NUMERIC_MODEL_FEATURES if column in empty.columns],
                "missing_columns": [column for column in NUMERIC_MODEL_FEATURES if column not in empty.columns],
            },
            "baseline_feature_diagnostics": _feature_diagnostics(empty, columns=NUMERIC_MODEL_FEATURES),
            "graph_feature_diagnostics": _feature_diagnostics(empty, columns=GRAPH_FEATURE_COLUMNS),
            "graph_feature_definitions": GRAPH_FEATURE_DEFINITIONS,
            "feature_readiness": {
                "ready_for_controlled_experiments": False,
                "stable_graph_feature_count": 0,
            },
        }
        summary = Phase7GraphFeatureBuildSummary(
            feature_schema_version=feature_schema_version,
            dataset_hash=_dataset_hash(empty),
            row_count=0,
            labeled_row_count=0,
            graph_feature_count=len(GRAPH_FEATURE_COLUMNS),
            baseline_feature_count=len(NUMERIC_MODEL_FEATURES),
            stable_graph_feature_count=0,
            ready_for_controlled_experiments=False,
        )
        return empty, summary, diagnostics

    parsed_rows = []
    for row in rows:
        decision_time = _parse_iso(row.decision_timestamp)
        if decision_time is None:
            continue
        parsed_rows.append((decision_time, row))

    if not parsed_rows:
        frame = baseline_frame.copy()
        frame["baseline_feature_schema_version"] = frame["feature_schema_version"]
        frame["feature_schema_version"] = feature_schema_version
        for column in GRAPH_FEATURE_COLUMNS:
            frame[column] = 0.0
        diagnostics = {
            "baseline_contract": {
                "expected_columns": list(NUMERIC_MODEL_FEATURES),
                "present_columns": [column for column in NUMERIC_MODEL_FEATURES if column in frame.columns],
                "missing_columns": [column for column in NUMERIC_MODEL_FEATURES if column not in frame.columns],
            },
            "baseline_feature_diagnostics": _feature_diagnostics(frame, columns=NUMERIC_MODEL_FEATURES),
            "graph_feature_diagnostics": _feature_diagnostics(frame, columns=GRAPH_FEATURE_COLUMNS),
            "graph_feature_definitions": GRAPH_FEATURE_DEFINITIONS,
            "feature_readiness": {
                "ready_for_controlled_experiments": False,
                "stable_graph_feature_count": 0,
            },
        }
        summary = Phase7GraphFeatureBuildSummary(
            feature_schema_version=feature_schema_version,
            dataset_hash=_dataset_hash(frame),
            row_count=int(len(frame)),
            labeled_row_count=baseline_summary.labeled_row_count,
            graph_feature_count=len(GRAPH_FEATURE_COLUMNS),
            baseline_feature_count=len(NUMERIC_MODEL_FEATURES),
            stable_graph_feature_count=0,
            ready_for_controlled_experiments=False,
        )
        return frame, summary, diagnostics

    parsed_rows.sort(key=lambda item: (item[0], item[1].candidate_id))
    earliest_decision = parsed_rows[0][0] - timedelta(days=max(1, lookback_days))
    latest_decision = parsed_rows[-1][0]

    edges, market_event_map = _load_graph_edges(
        start=_iso(earliest_decision),
        end=_iso(latest_decision),
    )
    for _, row in parsed_rows:
        market_event_map.setdefault(row.market_id, row.event_id)

    edge_index = 0
    active_edges: deque[TradeGraphEdge] = deque()
    state = _TradeGraphState()
    feature_rows: list[dict[str, Any]] = []

    for decision_time, row in parsed_rows:
        while edge_index < len(edges) and edges[edge_index].trade_time <= decision_time:
            edge = edges[edge_index]
            state.add(edge)
            active_edges.append(edge)
            edge_index += 1

        cutoff = decision_time - timedelta(days=max(1, lookback_days))
        while active_edges and active_edges[0].trade_time < cutoff:
            state.remove(active_edges.popleft())

        feature_rows.append(
            {
                "evaluation_row_id": row.evaluation_row_id,
                **_build_graph_feature_row(
                    row=row,
                    state=state,
                    market_event_map=market_event_map,
                    persistence_min_days=max(1, persistence_min_days),
                ),
            }
        )

    graph_frame = pd.DataFrame.from_records(feature_rows)
    frame = baseline_frame.merge(graph_frame, on="evaluation_row_id", how="left")
    frame["baseline_feature_schema_version"] = frame["feature_schema_version"]
    frame["feature_schema_version"] = feature_schema_version
    for column in GRAPH_FEATURE_COLUMNS:
        frame[column] = pd.to_numeric(frame[column], errors="coerce").fillna(0.0)

    baseline_contract = {
        "expected_columns": list(NUMERIC_MODEL_FEATURES),
        "present_columns": [column for column in NUMERIC_MODEL_FEATURES if column in frame.columns],
        "missing_columns": [column for column in NUMERIC_MODEL_FEATURES if column not in frame.columns],
    }
    baseline_feature_diagnostics = _feature_diagnostics(frame, columns=NUMERIC_MODEL_FEATURES)
    graph_feature_diagnostics = _feature_diagnostics(frame, columns=GRAPH_FEATURE_COLUMNS)
    stable_graph_feature_count = sum(
        1 for payload in graph_feature_diagnostics.values() if payload["status"] == "stable"
    )
    ready_for_controlled_experiments = bool(
        len(frame)
        and not baseline_contract["missing_columns"]
        and stable_graph_feature_count >= max(3, len(GRAPH_FEATURE_COLUMNS) // 3)
    )

    diagnostics = {
        "baseline_contract": baseline_contract,
        "baseline_feature_diagnostics": baseline_feature_diagnostics,
        "graph_feature_diagnostics": graph_feature_diagnostics,
        "graph_feature_definitions": GRAPH_FEATURE_DEFINITIONS,
        "graph_feature_settings": {
            "feature_schema_version": feature_schema_version,
            "lookback_days": max(1, lookback_days),
            "persistence_min_days": max(1, persistence_min_days),
        },
        "feature_readiness": {
            "ready_for_controlled_experiments": ready_for_controlled_experiments,
            "stable_graph_feature_count": stable_graph_feature_count,
            "baseline_missing_column_count": len(baseline_contract["missing_columns"]),
        },
    }

    summary = Phase7GraphFeatureBuildSummary(
        feature_schema_version=feature_schema_version,
        dataset_hash=_dataset_hash(frame),
        row_count=int(len(frame)),
        labeled_row_count=int(frame["label_available"].sum()),
        graph_feature_count=len(GRAPH_FEATURE_COLUMNS),
        baseline_feature_count=len(NUMERIC_MODEL_FEATURES),
        stable_graph_feature_count=stable_graph_feature_count,
        ready_for_controlled_experiments=ready_for_controlled_experiments,
    )
    return frame, summary, diagnostics
