# Phase 6 Single-Owner Runbook

This runbook is the shortest practical path to exercise the canonical single-owner Phase 6 workflow in this repo.

Use this when you want to:
- build the replay-derived Phase 6 dataset
- train the SRS-compliant LightGBM shadow ranker
- write baseline-comparison, calibration, threshold, and model-card artifacts
- register and activate the shadow model
- write shadow-score evidence for the same reference window

## 1. Move into the repo root

```bash
cd C:\Users\parik\Desktop\Poly\polymarket-data-collector
```

## 2. Choose the runtime

For the shared local SQLite workspace:

```bash
$env:POLYMARKET_DB_BACKEND='sqlite'
$env:POLYMARKET_SQLITE_PATH='C:\Users\parik\Desktop\Poly\polymarket-data-collector\database\polymarket_state.db'
$env:POLYMARKET_ENABLE_PHASE6_SHADOW_MODE='true'
```

## 3. Canonical Phase 9 reference-window model-completion path

To rebuild the single-owner Phase 6 evidence packet for the canonical hour:

```bash
python run_phase9_phase6_model_completion.py --json
```

This writes:
- a replay-derived training dataset under `reports/phase6/training_datasets/phase9_task4/`
- a LightGBM model artifact and model card under `reports/phase6/model_artifacts/phase9_task4/`
- required-baseline comparison outputs under `reports/phase6/baseline_comparisons/`
- calibration and threshold outputs under `reports/phase6/calibration/`
- shadow-score evidence under `reports/phase6/shadow_scores/`
- durable rows in `model_evaluation_runs`, `calibration_profiles`, `model_registry`, and `shadow_model_scores`

## 4. Generic dataset build

```bash
python run_phase6_build_training_dataset.py ^
  --start 2026-04-20T05:00:00+00:00 ^
  --end 2026-04-20T06:00:00+00:00 ^
  --output-dir reports/phase6/training_datasets/manual ^
  --json
```

## 5. Generic ranker training

The default training path is now the boosted-tree path:

```bash
python run_phase6_train_ranker.py ^
  --start 2026-04-20T05:00:00+00:00 ^
  --end 2026-04-20T06:00:00+00:00 ^
  --model-family lightgbm ^
  --model-version phase6_shadow_lightgbm_v1 ^
  --output-dir reports/phase6/model_artifacts/manual ^
  --json
```

If you need the old compatibility baseline:

```bash
python run_phase6_train_ranker.py ^
  --start 2026-04-20T05:00:00+00:00 ^
  --end 2026-04-20T06:00:00+00:00 ^
  --model-family linear ^
  --model-version phase6_linear_compat_v1 ^
  --json
```

## 6. Registry and shadow activation

If you want to register a model artifact manually:

```bash
python run_phase6_register_model.py ^
  --model-version phase6_shadow_lightgbm_v1 ^
  --artifact-path reports/phase6/model_artifacts/manual/phase6_shadow_lightgbm_v1.json ^
  --training-dataset-hash <dataset_hash> ^
  --deployment-status shadow ^
  --shadow-enabled ^
  --json
```

Then activate it:

```bash
python run_phase6_activate_model.py ^
  --model-version phase6_shadow_lightgbm_v1 ^
  --json
```

## 7. Generic shadow scoring

```bash
python run_phase6_shadow_score.py ^
  --start 2026-04-20T05:00:00+00:00 ^
  --end 2026-04-20T06:00:00+00:00 ^
  --model-version phase9_task4_lightgbm_v1 ^
  --json
```

## 8. Useful tables

```sql
select * from model_evaluation_runs order by created_at desc limit 20;
select * from calibration_profiles order by created_at desc limit 20;
select * from model_registry order by created_at desc limit 20;
select * from shadow_model_scores order by created_at desc limit 20;
```

## 9. Current truthfulness note

The current Task 4 packet satisfies the SRS artifact contract for a LightGBM shadow model, baseline comparison, calibration, thresholds, registry entry, and shadow scores. It does not yet justify a “held-out baseline victory” claim because the current local dataset is still tiny and train-only.
