# Phase 8 Canonical v1 Operating Mode

- Contract version: `phase8_v1_operating_mode_v1`
- Generated at: `2026-04-22T21:06:59.533909+00:00`
- Git commit: `96df0118cc4bf84262597dab67ee60d0c758d235`
- Canonical mode: `rule_based_plus_shadow_ml`

## Authoritative v1 Rule
- Rule-based candidate and alert behavior remains authoritative for v1. ML scores may be recorded, compared, and reviewed, but they do not decide whether an alert exists.
- If ML plumbing is disabled, shadow models can be retired or ignored without changing the authoritative rule-based alert path.

## Rejected Modes
- `rule_based_only`: The repo already contains committed Phase 6 registry, shadow-scoring, and evaluation plumbing that should remain active for v1 learning and auditability.
- `ml_backed_ranking_with_rollback`: The committed repo state still does not justify promotion of ML to decision authority: Phase 6 is shadow-first by design, the local LightGBM evidence packet is still descriptive rather than held-out-defensible, and the Phase 4 provider path is not yet real-provider-backed in this workspace.

## Phase 7 Classification
- Research-only: graph-derived feature families
- Research-only: graph-aware advanced ranker artifacts
- Research-only: marked Hawkes or TCN experiments
- Research-only: ablation tables and thesis-quality figures
- Research-only: phase7 research packages and experiment-ledger narratives as headline model claims
- Governance/ops influence only: observability and Goodhart warnings that constrain operator trust and deployment claims
- Governance/ops influence only: strict-holdout promotion discipline versus the Phase 6 baseline
- Governance/ops influence only: scale, storage, restore, and long-run dashboard guidance from the operational scale-up track
- Governance/ops influence only: reproducibility packaging standards for later thesis or defense artifacts

## Rationale
- The SRS definition of v1 complete requires one ranker to be evaluated against baselines; it does not require ML to become the authoritative alert path.
- The SRS and Phase 6 planning explicitly require shadow mode first.
- Phase 4's canonical single-owner plan keeps the alert loop rule-based and operator-facing before ML promotion.
- The committed Phase 6 implementation now supports a LightGBM shadow ranker, but its own reporting path still keeps thresholds advisory and the refreshed evidence remains too small for authoritative promotion.
- Phase 7 is explicitly framed as advanced research after v1 stability, so it should not silently redefine the canonical v1 operating mode.
