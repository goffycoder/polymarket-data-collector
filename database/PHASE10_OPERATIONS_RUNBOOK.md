# Phase 10 Operations Runbook

This is the single-owner hardening runbook for the final Phase 10 operating path.

## Monitoring Coverage
- Collector health: run `python run_phase7_health_summary.py --json`.
- Storage and archive coverage: run `python run_phase7_storage_audit.py --json`.
- Integrity summary: run `python run_phase7_integrity_summary.py --json`.
- Replay validation family: run `python run_phase10_heldout_validation_pack.py --json`.
- Held-out model status: run `python run_phase10_heldout_model_completion.py --json`.

## Incident Response
- Collector death or reconnect storm: restart the collector, check latest health summary, and inspect logs before rerunning alerts.
- Disk pressure: run the storage audit and compaction plan before deleting any partitions manually.
- Schema drift: re-run `apply_schema()` through the canonical entrypoint and re-check integrity summary output.
- Replay failure: regenerate the held-out family and rerun the window-specific replay bundles before trusting any validation report.

## Backup Discipline
- Treat `database/polymarket_state.db`, `data/raw/`, and `data/detector_input/` as the minimum local backup set.
- Keep the latest Phase 10 closeout reports under `reports/phase10/` together with the current database snapshot.
