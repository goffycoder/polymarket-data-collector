# Friend Handoff: Phase Outputs and Where They Are

Use this as a quick reference for asking another agent about the project. Each phase below tells you:
- what the phase produced,
- one concrete output or result to mention,
- and where that evidence lives.

## Phase 1
**What it produced**
- ingestion correctness foundation
- approved-universe filtering
- both YES and NO token tracking
- canonical trade identity and deduping
- validation checks for trade and wallet correctness

**Concrete output to mention**
- Phase 1 made the collector trustworthy enough for later phases by fixing market selection, token coverage, condition-level trade identity, and duplicate inflation.

**Where to look**
- [phase1.tex](/c:/Users/parik/Desktop/Poly/polymarket-data-collector/Documentation/phases/phase1.tex)
- [phase1_validators.py](/c:/Users/parik/Desktop/Poly/polymarket-data-collector/validation/phase1_validators.py)
- [trade_utils.py](/c:/Users/parik/Desktop/Poly/polymarket-data-collector/collectors/trade_utils.py)
- [schema.sql](/c:/Users/parik/Desktop/Poly/polymarket-data-collector/database/schema.sql)

## Phase 2
**What it produced**
- durable raw archive
- detector-input logs
- replay tooling
- PostgreSQL cutover path

**Concrete output to mention**
- replay parity for one reference hour matched exactly: raw rows `66`, detector rows `66`, raw manifest rows `66`, detector manifest rows `66`
- deterministic republish also reproduced `66` rows

**Where to look**
- [phase2.tex](/c:/Users/parik/Desktop/Poly/polymarket-data-collector/Documentation/phases/phase2.tex)
- [event_log.py](/c:/Users/parik/Desktop/Poly/polymarket-data-collector/utils/event_log.py)
- [phase2_replay.py](/c:/Users/parik/Desktop/Poly/polymarket-data-collector/validation/phase2_replay.py)
- [phase2_republish.py](/c:/Users/parik/Desktop/Poly/polymarket-data-collector/validation/phase2_republish.py)
- [postgres_migrate.py](/c:/Users/parik/Desktop/Poly/polymarket-data-collector/database/postgres_migrate.py)

## Phase 3
**What it produced**
- live candidate-detection engine
- rolling state in Redis
- streaming feature computation
- deterministic rule-based suspicious episode generation

**Concrete output to mention**
- Phase 3 built the candidate engine, but this repo snapshot is stronger on implementation and reconciliation tooling than on final committed report metrics.

**Where to look**
- [phase3.tex](/c:/Users/parik/Desktop/Poly/polymarket-data-collector/Documentation/phases/phase3.tex)
- [detector.py](/c:/Users/parik/Desktop/Poly/polymarket-data-collector/phase3/detector.py)
- [live_runner.py](/c:/Users/parik/Desktop/Poly/polymarket-data-collector/phase3/live_runner.py)
- [phase3_reconciliation.py](/c:/Users/parik/Desktop/Poly/polymarket-data-collector/validation/phase3_reconciliation.py)

## Phase 4
**What it produced**
- explainable alerts
- evidence snapshots
- delivery tracking
- analyst feedback workflow
- duplicate suppression

**Concrete output to mention**
- Phase 4 created the human-facing alert workflow; later materialized examples show persisted alerts, delivery attempts, evidence states, and analyst outcomes.

**Where to look**
- [phase4.tex](/c:/Users/parik/Desktop/Poly/polymarket-data-collector/Documentation/phases/phase4.tex)
- [evidence.py](/c:/Users/parik/Desktop/Poly/polymarket-data-collector/phase4/evidence.py)
- [alerts.py](/c:/Users/parik/Desktop/Poly/polymarket-data-collector/phase4/alerts.py)
- [analyst.py](/c:/Users/parik/Desktop/Poly/polymarket-data-collector/phase4/analyst.py)

## Phase 5
**What it produced**
- historical replay validation
- held-out evaluation
- conservative paper-trading backtest

**Concrete output to mention**
- held-out validation pack: evaluation rows `96`, alert rows `96`, paper trades `96`, assessment `promising`
- conservative backtest: median bounded PnL `14.076441`

**Where to look**
- [phase5.tex](/c:/Users/parik/Desktop/Poly/polymarket-data-collector/Documentation/phases/phase5.tex)
- [phase10_task3_heldout_validation.md](/c:/Users/parik/Desktop/Poly/polymarket-data-collector/reports/phase5/validation/phase10_task3_heldout_validation.md)
- [phase10_task3_conservative_backtest.md](/c:/Users/parik/Desktop/Poly/polymarket-data-collector/reports/phase5/backtests/phase10_task3_conservative_backtest.md)

## Phase 6
**What it produced**
- first ML ranker
- baseline comparisons
- calibration outputs
- model card
- shadow deployment path

**Concrete output to mention**
- LightGBM shadow model used `96` rows with `32/32/32` train-validation-test split
- test AUC `0.585938`
- test Precision@10 `0.6`
- assessment `model_beats_required_baselines`

**Where to look**
- [phase6_person2.tex](/c:/Users/parik/Desktop/Poly/polymarket-data-collector/Documentation/person2Phases/phase6_person2.tex)
- [phase10_task4_lightgbm_v1_model_card.md](/c:/Users/parik/Desktop/Poly/polymarket-data-collector/reports/phase6/model_artifacts/phase10_task4/phase10_task4_lightgbm_v1_model_card.md)
- [phase10_task4_lightgbm_v1_required_baselines.md](/c:/Users/parik/Desktop/Poly/polymarket-data-collector/reports/phase6/baseline_comparisons/phase10_task4_lightgbm_v1_required_baselines.md)

## Phase 7
**What it produced**
- advanced research track
- graph-derived feature contract
- cluster persistence and sequence-model planning
- thesis-quality ablation direction

**Concrete output to mention**
- Phase 7 is mostly a research extension and planning track in this repo, not a fully materialized final operational phase.

**Where to look**
- [SRS.tex](/c:/Users/parik/Desktop/Poly/polymarket-data-collector/Documentation/SRS.tex:2810)
- [phase7_person2.tex](/c:/Users/parik/Desktop/Poly/polymarket-data-collector/Documentation/person2Phases/phase7_person2.tex)
- [phase7_graph_feature_contract.md](/c:/Users/parik/Desktop/Poly/polymarket-data-collector/Documentation/person2Phases/phase7_graph_feature_contract.md)

## Phase 8
**What it produced**
- canonical inventory
- frozen reference path
- v1 operating-mode decision
- metrics review
- final closeout package

**Concrete output to mention**
- canonical v1 mode was frozen as `rule_based_plus_shadow_ml`
- Phase 8 concluded the repo was materially populated but not yet fully SRS-complete v1

**Where to look**
- [phase8.tex](/c:/Users/parik/Desktop/Poly/polymarket-data-collector/Documentation/phases/phase8.tex)
- [phase8_v1_operating_mode_summary.md](/c:/Users/parik/Desktop/Poly/polymarket-data-collector/reports/phase8/operating_mode/phase8_v1_operating_mode_summary.md)
- [phase8_metrics_review_summary.md](/c:/Users/parik/Desktop/Poly/polymarket-data-collector/reports/phase8/metrics_review/phase8_metrics_review_summary.md)
- [phase8_final_closeout_summary.md](/c:/Users/parik/Desktop/Poly/polymarket-data-collector/reports/phase8/final_closeout/phase8_final_closeout_summary.md)

## Phase 9
**What it produced**
- end-to-end evidence materialization
- persisted alerts and analyst feedback
- refreshed closeout evidence
- first stronger shadow-model completion step

**Concrete output to mention**
- reference window processed `12` envelopes and produced `2` candidates, `2` alerts, `4` evidence queries, `2` evidence snapshots, `4` delivery attempts, and `1` analyst action

**Where to look**
- [phase9.tex](/c:/Users/parik/Desktop/Poly/polymarket-data-collector/Documentation/phases/phase9.tex)
- [phase9_task2_review_summary.md](/c:/Users/parik/Desktop/Poly/polymarket-data-collector/reports/phase9/candidate_to_alert_materialization/phase9_task2_review_summary.md)
- [phase9_task4_summary.md](/c:/Users/parik/Desktop/Poly/polymarket-data-collector/reports/phase9/phase6_model_completion/phase9_task4_summary.md)
- [phase9_task5_closeout_refresh_summary.md](/c:/Users/parik/Desktop/Poly/polymarket-data-collector/reports/phase9/closeout_refresh/phase9_task5_closeout_refresh_summary.md)

## Phase 10
**What it produced**
- real-provider-backed evidence
- repeated analyst-loop examples
- held-out validation family
- held-out LightGBM model completion
- final operations and governance closeout

**Concrete output to mention**
- final repo answer: `SRS v1 complete: True`
- held-out validation: `96` evaluation rows, `96` alerts, `96` paper trades, assessment `promising`
- held-out model: `96` rows, `32/32/32` split, `model_beats_required_baselines`
- analyst-loop expansion: `3` persisted alerts, `2` analyst feedback rows, `1` suppressed alert
- real-provider evidence: `4` total provider query rows

**Where to look**
- [phase10.tex](/c:/Users/parik/Desktop/Poly/polymarket-data-collector/Documentation/phases/phase10.tex)
- [phase10_completion_memo.md](/c:/Users/parik/Desktop/Poly/polymarket-data-collector/reports/phase10/final_closeout/phase10_completion_memo.md)
- [phase10_task1_review_summary.md](/c:/Users/parik/Desktop/Poly/polymarket-data-collector/reports/phase10/real_provider_evidence_hardening/phase10_task1_review_summary.md)
- [phase10_task2_review_summary.md](/c:/Users/parik/Desktop/Poly/polymarket-data-collector/reports/phase10/analyst_loop_expansion/phase10_task2_review_summary.md)
- [phase10_task3_review_summary.md](/c:/Users/parik/Desktop/Poly/polymarket-data-collector/reports/phase10/heldout_validation_pack/phase10_task3_review_summary.md)
- [phase10_task4_summary.md](/c:/Users/parik/Desktop/Poly/polymarket-data-collector/reports/phase10/heldout_model_completion/phase10_task4_summary.md)

## Short Prompt Your Friend Can Give Another Agent
“Go through `reports/presentation/friend_handoff_phase_outputs.md` in this repo and explain each phase with the concrete output, the key result, and the source file paths. Focus especially on Phases 5, 6, 9, and 10 because those contain the strongest presentation-ready artifacts.”
