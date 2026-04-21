# Phase 6 Person 1 Runbook

This runbook is the shortest practical path to exercise the Person 1 ML plumbing on the `phase6_person1` branch.

Current Person 1 capabilities:
- replay-derived feature materialization
- model registry writes
- shadow-score logging foundation with dedupe
- runnable shadow scoring with a registered JSON artifact
- active shadow model lookup / activation / retirement
- rolling live shadow polling
- registry and recent-score reporting

## 1. Move into the Person 1 worktree

```bash
cd /private/tmp/polymarket_arbitrage_phase6_person1
```

## 2. Load environment

```bash
source /Users/vrajpatel/All-projects/polymarket_arbitrage/.env
```

Then set the runtime:

```bash
export POLYMARKET_DB_BACKEND=postgres
export POLYMARKET_DATABASE_URL='postgresql+psycopg://localhost:5432/polymarket_phase3'
export POLYMARKET_ENABLE_PHASE6_SHADOW_MODE=true
```

## 3. Materialize replay-derived features

```bash
/Users/vrajpatel/All-projects/polymarket_arbitrage/venv/bin/python run_phase6_materialize_features.py \
  --start '2026-04-21T18:00:00+00:00' \
  --end '2026-04-21T19:00:00+00:00' \
  --mode training
```

This writes:
- one `feature_materialization_runs` row
- one feature artifact in `reports/phase6/features/`

## 4. Register a shadow model artifact

Create a simple JSON artifact like:

```json
{
  "kind": "linear_ranker",
  "weights": {
    "candidate_severity_score": 0.45,
    "fresh_wallet_count": 0.10,
    "fresh_wallet_notional_share": 0.15,
    "directional_imbalance": 0.10,
    "concentration_ratio": 0.10,
    "probability_velocity": 0.05,
    "probability_acceleration": 0.025,
    "volume_acceleration": 0.025
  },
  "thresholds": {
    "watch": 0.5,
    "actionable": 0.75,
    "critical": 0.9
  }
}
```

Then register it:

```bash
/Users/vrajpatel/All-projects/polymarket_arbitrage/venv/bin/python run_phase6_register_model.py \
  --model-version phase6_shadow_v1 \
  --artifact-path reports/phase6/model_artifacts/phase6_shadow_v1.json \
  --training-dataset-hash PASTE_DATASET_HASH \
  --deployment-status shadow \
  --shadow-enabled
```

## 5. Activate or retire the shadow model

Activate one model:

```bash
/Users/vrajpatel/All-projects/polymarket_arbitrage/venv/bin/python run_phase6_activate_model.py \
  --model-version phase6_shadow_v1
```

Retire one model:

```bash
/Users/vrajpatel/All-projects/polymarket_arbitrage/venv/bin/python run_phase6_activate_model.py \
  --model-version phase6_shadow_v1 \
  --action retire
```

## 6. Run shadow scoring for one window

```bash
/Users/vrajpatel/All-projects/polymarket_arbitrage/venv/bin/python run_phase6_shadow_score.py \
  --start '2026-04-21T18:00:00+00:00' \
  --end '2026-04-21T19:00:00+00:00'
```

This writes:
- one shadow-score artifact in `reports/phase6/shadow_scores/`
- one `shadow_model_scores` row per scored candidate

## 7. Run the rolling live shadow poller

One short pass:

```bash
/Users/vrajpatel/All-projects/polymarket_arbitrage/venv/bin/python run_phase6_shadow_live.py \
  --iterations 1 \
  --lookback-minutes 30
```

## 8. Inspect status and the Gate 6 report

Registry / score status:

```bash
/Users/vrajpatel/All-projects/polymarket_arbitrage/venv/bin/python run_phase6_registry_status.py --json
```

Person 1 report:

```bash
/Users/vrajpatel/All-projects/polymarket_arbitrage/venv/bin/python -m validation.run_phase6_person1_report --json
```

## 9. Useful tables to inspect

```sql
select * from feature_materialization_runs order by created_at desc limit 20;
select * from model_registry order by created_at desc limit 20;
select * from shadow_model_scores order by created_at desc limit 20;
```

## 10. What to hand to Person 2

Before Person 2 starts comparing or calibrating ML v1, give them:
- the materialized feature artifact path
- the dataset hash
- the registered model version
- the shadow-score artifact path
- any notes about shadow-mode behavior
