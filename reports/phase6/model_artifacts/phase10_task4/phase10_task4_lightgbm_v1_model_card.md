# Phase 6 Model Card: phase10_task4_lightgbm_v1

## Summary
- Evaluation version: `phase6_eval_v1`
- Calibration version: `phase6_calibration_v1`
- Dataset hash: `0c6df612d6820f1c58b692c542e9eb714d01dc4b1e0363dcc36bc8c35480de64`
- Model kind: `phase6_lightgbm_ranker_v1`

## Split Metrics
### Train
- Model AUC: `0.585938`
- Model Precision@10: `0.6`
- Model Precision@25: `0.52`
- Positive rate: `0.5`

### Validation
- Model AUC: `0.585938`
- Model Precision@10: `0.6`
- Model Precision@25: `0.52`
- Positive rate: `0.5`

### Test
- Model AUC: `0.585938`
- Model Precision@10: `0.6`
- Model Precision@25: `0.52`
- Positive rate: `0.5`

## Required Baselines
- Preferred comparison split: `test`
- Assessment: `model_beats_required_baselines`
- `baseline_probability_momentum`: auc_margin=`0.085938`, precision_at_10_margin=`0.1`
- `baseline_order_imbalance`: auc_margin=`0.085938`, precision_at_10_margin=`0.1`
- `baseline_microstructure`: auc_margin=`0.085938`, precision_at_10_margin=`0.1`
- `baseline_external_evidence`: auc_margin=`0.085938`, precision_at_10_margin=`0.1`
- `baseline_fresh_wallet`: auc_margin=`0.085938`, precision_at_10_margin=`0.1`

## Calibration Coverage
- Profiles written: `6`
- Liquidity and category slices are advisory until larger labeled windows exist.

## Known Failure Modes
- Direction labels inherit Phase 5 directional inference and can fail when velocity is weak or stale.
- Sparse or missing market resolution rows reduce label coverage and can bias holdout metrics.
- Liquidity bucketing currently relies on early post-decision spread snapshots, so thin coverage can blur calibration quality.
- This artifact is a LightGBM boosted-tree shadow model, but the current local dataset is still tiny enough that its ranking quality remains only weakly evidenced.

## Deployment Guidance
- Treat thresholds as shadow-only recommendations until they survive larger replay windows.
- Refit and recalibrate whenever the feature schema or label contract changes.
