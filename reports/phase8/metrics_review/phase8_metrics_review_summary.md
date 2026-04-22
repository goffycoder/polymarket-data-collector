# Phase 8 Final Metrics and Limitations Review

- Contract version: `phase8_metrics_review_v1`
- Generated at: `2026-04-22T21:06:59.628554+00:00`
- Git commit: `96df0118cc4bf84262597dab67ee60d0c758d235`
- Overall status: `metrics_materialized_but_not_yet_defendable_for_srs_v1`
- Canonical v1 mode: `rule_based_plus_shadow_ml`

## Metrics Bundle
- [1] Ranked alert precision and operational usefulness: `materialized_seeded_local_only`
  Target: Precision@10 > 0.60 (aspirational), plus meaningful analyst usefulness evidence
  Evidence: The workspace now contains a replay-linked local alert packet with persisted alerts, delivery attempts, and one analyst-feedback row. Current alert usefulness precision is `0.5` on a two-alert seeded packet, with delivery summary `sent=0 skipped=4`. This is enough to show the loop exists, but not enough to claim robust real-world precision.
- [2] Calibration: `materialized_descriptive_only`
  Target: Brier score < 0.20 (aspirational), with Phase 6 shadow thresholds currently targeting WATCH 0.55, ACTIONABLE 0.65, CRITICAL 0.80 precision slices
  Evidence: The workspace now contains model evaluation rows, calibration profiles, a registered LightGBM shadow model, and shadow scores for the canonical window. The current required-baseline assessment is `descriptive_only_train_split`, which means the artifact contract is satisfied but the local evidence is still descriptive and not held-out-defendable.
- [3] Lead time over public corroboration: `materialized_seeded_local_only`
  Target: Median lead time > 30 minutes on the subset where corroboration exists
  Evidence: Lead-time analysis is now materialized for the canonical local packet. The current median lead time is `9495.0` seconds, which exceeds the 30-minute aspirational target on the single successful alert in the seeded packet. This demonstrates the reporting path, but the sample is too small and synthetic to treat as a strong production claim.
- [4] Economic edge under conservative execution: `materialized_small_sample`
  Target: Positive paper-trade edge after fees and slippage
  Evidence: The conservative paper-trading framework is now materially populated for the canonical window. The current backtest packet shows median bounded PnL `18.63986` across `2` paper trades after explicit conservative assumptions. This is enough to prove the workflow and artifact contract, but not enough for a broad edge claim.

## Stop Conditions
- raw archive gaps exceed 1 hour: `not_triggered_on_canonical_seeded_packet`
- duplicate trade inflation cannot be controlled: `not_triggered_on_canonical_seeded_packet`
- alert false-positive rate remains operationally unusable after suppression tuning: `not_yet_defendable_for_real_operations`
- evidence-provider spend exceeds budget without measurable lift: `not_triggered_in_current_workspace_but_not_validated_for_real_providers`
- replay cannot reproduce a historical alert end to end: `resolved_for_seeded_local_packet_not_for_real_provider_packet`

## Highest-Priority Gap
- The project now has a materially populated end-to-end local packet, but the remaining blocker is stronger real-provider-backed and held-out-sized evidence rather than missing artifacts.
