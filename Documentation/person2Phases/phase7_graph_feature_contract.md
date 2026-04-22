# Phase 7 Graph Feature Contract

This file freezes the first graph and cluster-persistence feature schema used by Phase 7 Person 2 experiments.

Schema version: `phase7_graph_features_v1`

The graph is built only from retained wallet-market trades at or before each candidate decision timestamp. The default lookback is 90 days and the default persistence threshold is 2 distinct active trade days.

## Baseline compatibility

The Phase 7 graph dataset extends the Phase 6 training frame instead of replacing it.

The Phase 6 numeric baseline contract remains:

- `candidate_severity_score`
- `alert_severity_rank`
- `has_alert`
- `rule_count`
- `fresh_wallet_count`
- `fresh_wallet_notional_share`
- `directional_imbalance`
- `concentration_ratio`
- `probability_velocity_abs`
- `probability_acceleration_abs`
- `volume_acceleration`

## Graph features

- `graph_market_wallet_degree`
  Unique wallets connected to the candidate market inside the lookback graph.

- `graph_related_market_count`
  Distinct non-candidate markets reached by wallets that traded the candidate market.

- `graph_cluster_wallet_count`
  Unique wallets in the candidate market two-hop market-wallet cluster.

- `graph_cluster_density_2hop`
  Observed wallet-market edges divided by possible edges in the two-hop induced cluster.

- `graph_same_event_market_share`
  Share of markets in the two-hop cluster that belong to the candidate event.

- `graph_repeat_wallet_share`
  Share of candidate-market wallets that also traded at least one other market in lookback.

- `graph_cross_event_wallet_share`
  Share of candidate-market wallets connected to more than one event in lookback.

- `graph_same_event_repeat_wallet_share`
  Share of candidate-market wallets that also traded another market from the same event.

- `graph_wallet_persistence_mean_days`
  Mean count of distinct active trade days among candidate-market wallets in lookback.

- `graph_persistent_wallet_share`
  Share of candidate-market wallets active on at least the configured persistence-day threshold.

- `graph_cluster_notional_share`
  Candidate market notional divided by total notional in the two-hop cluster.

## Reproducibility rules

- Feature materialization must be deterministic for a fixed start time, end time, lookback, and persistence threshold.
- Only trades with non-empty `proxy_wallet`, valid `market_id`, and event time at or before the decision timestamp are eligible.
- The output dataset hash is computed from the full graph-augmented training frame sorted by `decision_timestamp` and `candidate_id`.
- Availability and stability diagnostics are written beside every materialized dataset.
