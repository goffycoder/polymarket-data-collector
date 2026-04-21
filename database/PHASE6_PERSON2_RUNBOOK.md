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

## 5. Render the Person 2 report

```bash
/Users/vrajpatel/All-projects/polymarket_arbitrage/venv/bin/python -m validation.run_phase6_person2_report --json
```

This summarizes:
- latest evaluation run
- baseline margin on the held-out test split
- calibration slices by liquidity bucket and category

## 6. Prepare the handoff for Person 1

```bash
/Users/vrajpatel/All-projects/polymarket_arbitrage/venv/bin/python run_phase6_prepare_handoff.py \
  --model-version phase6_person2_ranker_v1 \
  --json
```

This writes one bundle JSON in `reports/phase6/handoffs/` with:
- model artifact path
- dataset hash
- recommended calibration metadata
- the exact fields Person 1 needs for `run_phase6_register_model.py`

## 7. Hand off to Person 1

Use the generated model artifact with:

```bash
/Users/vrajpatel/All-projects/polymarket_arbitrage/venv/bin/python run_phase6_register_model.py \
  --model-version phase6_person2_ranker_v1 \
  --artifact-path reports/phase6/model_artifacts/phase6_person2_ranker_v1.json \
  --training-dataset-hash '<dataset_hash_here>' \
  --deployment-status shadow \
  --shadow-enabled
```

## 8. Useful tables

```sql
select * from model_evaluation_runs order by created_at desc limit 20;
select * from calibration_profiles order by created_at desc limit 20;
select * from model_registry order by created_at desc limit 20;
```
