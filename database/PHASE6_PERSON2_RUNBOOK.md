# Phase 6 Person 2 Runbook

This is the shortest practical path to exercise the Person 2 modeling/evaluation flow on `phase6_person2`.

## 1. Move into the worktree

```bash
cd /private/tmp/polymarket_arbitrage_phase6_person2
git branch --show-current
```

Expected branch:

```bash
phase6_person2
```

## 2. Load env and point at the Phase 3 / Phase 4 / Phase 5 database

```bash
source /Users/vrajpatel/All-projects/polymarket_arbitrage/.env

export POLYMARKET_DB_BACKEND=postgres
export POLYMARKET_DATABASE_URL='postgresql+psycopg://localhost:5432/polymarket_phase3'
```

## 3. Build the replay-derived training dataset

```bash
/Users/vrajpatel/All-projects/polymarket_arbitrage/venv/bin/python run_phase6_build_training_dataset.py \
  --start '2026-04-20T00:00:00+00:00' \
  --end '2026-04-22T00:00:00+00:00' \
  --json
```

Outputs:
- one CSV in `reports/phase6/training_datasets/`
- one metadata JSON beside it

## 4. Train the starter Person 2 ranker and write evaluation artifacts

```bash
/Users/vrajpatel/All-projects/polymarket_arbitrage/venv/bin/python run_phase6_train_ranker.py \
  --start '2026-04-20T00:00:00+00:00' \
  --end '2026-04-22T00:00:00+00:00' \
  --model-version phase6_person2_ranker_v1 \
  --json
```

Outputs:
- model artifact JSON in `reports/phase6/model_artifacts/`
- scored CSV
- evaluation report JSON
- model card markdown
- one `model_evaluation_runs` row
- several `calibration_profiles` rows

## 5. Hand off to Person 1

Use the generated model artifact with:

```bash
/Users/vrajpatel/All-projects/polymarket_arbitrage/venv/bin/python run_phase6_register_model.py \
  --model-version phase6_person2_ranker_v1 \
  --artifact-path reports/phase6/model_artifacts/phase6_person2_ranker_v1.json \
  --training-dataset-hash '<dataset_hash_here>' \
  --deployment-status shadow \
  --shadow-enabled
```

## 6. Useful tables

```sql
select * from model_evaluation_runs order by created_at desc limit 20;
select * from calibration_profiles order by created_at desc limit 20;
select * from model_registry order by created_at desc limit 20;
```
