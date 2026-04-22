# Phase 8 Final Metrics and Limitations Review

- Contract version: `phase8_metrics_review_v1`
- Generated at: `2026-04-22T15:24:56.672869+00:00`
- Git commit: `baf344b8e3bff24ffcc37040d6114d628a3f729d`
- Overall status: `metrics_bundle_defined_but_not_materialized`
- Canonical v1 mode: `rule_based_plus_shadow_ml`

## Metrics Bundle
- [1] Ranked alert precision and operational usefulness: `not_materialized_in_workspace`
  Target: Precision@10 > 0.60 (aspirational), plus meaningful analyst usefulness evidence
  Evidence: No persisted alerts, delivery attempts, or analyst feedback rows are present in the current workspace snapshot, so alert precision/usefulness cannot be computed honestly.
- [2] Calibration: `not_materialized_in_workspace`
  Target: Brier score < 0.20 (aspirational), with Phase 6 shadow thresholds currently targeting WATCH 0.55, ACTIONABLE 0.65, CRITICAL 0.80 precision slices
  Evidence: No model evaluation runs, calibration profiles, or shadow-score rows are present locally. The code supports calibration, but no committed local evidence proves current calibration quality.
- [3] Lead time over public corroboration: `not_materialized_in_workspace`
  Target: Median lead time > 30 minutes on the subset where corroboration exists
  Evidence: Lead-time analysis requires real alerts, evidence states, and often shadow-score or review data. Those artifacts are absent in the current workspace snapshot.
- [4] Economic edge under conservative execution: `not_materialized_in_workspace`
  Target: Positive paper-trade edge after fees and slippage
  Evidence: The conservative paper-trading framework exists by design, but this workspace contains no backtest artifacts or validation runs that would justify a PnL or edge claim.

## Stop Conditions
- raw archive gaps exceed 1 hour: `unresolved_in_current_workspace`
- duplicate trade inflation cannot be controlled: `unresolved_in_current_workspace`
- alert false-positive rate remains operationally unusable after suppression tuning: `unresolved_in_current_workspace`
- evidence-provider spend exceeds budget without measurable lift: `not_triggered_in_current_workspace_but_not_validated_for_real_providers`
- replay cannot reproduce a historical alert end to end: `active_concern`

## Highest-Priority Gap
- No real alert/evaluation/backtest evidence exists locally, so none of the SRS priority metrics can be defended numerically in this workspace.
