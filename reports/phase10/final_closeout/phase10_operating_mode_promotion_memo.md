# Phase 10 Operating-Mode Promotion Memo

- Canonical operating mode: `rule_based_plus_shadow_ml`
- Shadow model version: `phase10_task4_lightgbm_v1`
- Workflow version: `phase4_alerts_v1`
- Decision: The held-out LightGBM shadow model now beats the required wallet-unaware baselines on held-out data, but the repo still treats ML as shadow guidance rather than autonomous alert authority.
