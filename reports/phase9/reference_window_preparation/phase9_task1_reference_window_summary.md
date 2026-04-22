# Phase 9 Task 1 - Reference Window Preparation

- Contract version: `phase9_task1_reference_window_v1`
- Generated at: `2026-04-22T20:09:28.652409+00:00`
- Git commit: `96df0118cc4bf84262597dab67ee60d0c758d235`
- Selected window: `2026-04-20T05:00:00+00:00` to `2026-04-20T06:00:00+00:00`
- Source system: `gamma_events`
- Overall readiness: `reference_window_frozen_but_not_locally_materialized`

## Local Availability Checks
- `data/` exists: `False`
- Raw partition exists: `False`
- Detector-input partition exists: `False`
- Replay republish partition exists: `False`
- SQLite database exists: `True`
- Non-zero later-phase tables: `none`

## Frozen Command Path
- `phase2_replay_validate`
  Purpose: Verify raw-archive and detector-input coverage for the exact selected window.
  Command: `python validation/run_phase2_replay.py --start 2026-04-20T05:00:00+00:00 --end 2026-04-20T06:00:00+00:00 --source-system gamma_events --json`
  Readiness: `ready_when_raw_and_detector_partitions_exist`
- `phase2_republish`
  Purpose: Republish the frozen raw window into detector-input form for downstream replay-safe processing.
  Command: `python validation/run_phase2_republish.py --start 2026-04-20T05:00:00+00:00 --end 2026-04-20T06:00:00+00:00 --source-system gamma_events --json`
  Readiness: `blocked_until_raw_archive_exists`
- `phase4_gate4_capture`
  Purpose: Freeze the later alert/evidence capture output path that Task 2 will populate for this same reference story.
  Command: `python run_phase4_gate4_capture.py --limit 10 --latest-alert-limit 5 --output reports/phase9/reference_window_preparation/phase9_task1_gate4_report.json`
  Readiness: `blocked_until_phase3_candidates_exist`
- `phase5_replay_bundle`
  Purpose: Freeze the canonical Phase 5 replay bundle command and artifact root for the selected window.
  Command: `python run_phase5_replay.py --start 2026-04-20T05:00:00+00:00 --end 2026-04-20T06:00:00+00:00 --source-system gamma_events --output-dir reports/phase9/reference_window_preparation/phase5_replay_bundle --json`
  Readiness: `blocked_until_raw_archive_and_phase4_outputs_exist`
- `phase6_training_dataset`
  Purpose: Freeze the replay-derived Phase 6 training-dataset output root tied to the same window.
  Command: `python run_phase6_build_training_dataset.py --start 2026-04-20T05:00:00+00:00 --end 2026-04-20T06:00:00+00:00 --output-dir reports/phase9/reference_window_preparation/phase6_training_datasets --json`
  Readiness: `blocked_until_phase5_evaluation_rows_exist`
- `phase6_feature_materialization`
  Purpose: Freeze the Phase 6 feature-materialization command and output root for later shadow evaluation.
  Command: `python run_phase6_materialize_features.py --start 2026-04-20T05:00:00+00:00 --end 2026-04-20T06:00:00+00:00 --mode inference --output-dir reports/phase9/reference_window_preparation/phase6_features --json`
  Readiness: `blocked_until_phase5_evaluation_rows_exist`
