# Phase 6 Model Card: phase9_task4_lightgbm_v1

## Summary
- Evaluation version: `phase6_eval_v1`
- Calibration version: `phase6_calibration_v1`
- Dataset hash: `cc06985f33016ced4570c465748866a33f67fed129db37d0f326acca5076cff7`
- Model kind: `phase6_lightgbm_ranker_v1`

## Split Metrics
### Train
- Model AUC: `0.5`
- Model Precision@10: `0.5`
- Model Precision@25: `0.5`
- Positive rate: `0.5`

### Validation
- Model AUC: `None`
- Model Precision@10: `None`
- Model Precision@25: `None`
- Positive rate: `None`

### Test
- Model AUC: `None`
- Model Precision@10: `None`
- Model Precision@25: `None`
- Positive rate: `None`

## Required Baselines
- Preferred comparison split: `train`
- Assessment: `descriptive_only_train_split`
- `baseline_probability_momentum`: auc_margin=`0.5`, precision_at_10_margin=`0.0`
- `baseline_order_imbalance`: auc_margin=`0.5`, precision_at_10_margin=`0.0`
- `baseline_microstructure`: auc_margin=`0.5`, precision_at_10_margin=`0.0`
- `baseline_external_evidence`: auc_margin=`0.0`, precision_at_10_margin=`0.0`
- `baseline_fresh_wallet`: auc_margin=`0.0`, precision_at_10_margin=`0.0`

## Calibration Coverage
- Profiles written: `3`
- Liquidity and category slices are advisory until larger labeled windows exist.

## Known Failure Modes
- Direction labels inherit Phase 5 directional inference and can fail when velocity is weak or stale.
- Sparse or missing market resolution rows reduce label coverage and can bias holdout metrics.
- Liquidity bucketing currently relies on early post-decision spread snapshots, so thin coverage can blur calibration quality.
- This artifact is a LightGBM boosted-tree shadow model, but the current local dataset is still tiny enough that its ranking quality remains only weakly evidenced.

## Deployment Guidance
- Treat thresholds as shadow-only recommendations until they survive larger replay windows.
- Refit and recalibrate whenever the feature schema or label contract changes.
