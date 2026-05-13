# Phase 11 Runtime Storage And Replay Runbook

This is the canonical Task 4 operator path for replay safety and storage hygiene.

Use it when you need to:
- replay one archived detector-input window intentionally
- prove whether a historical window is fully restorable locally
- see the current retention rules and disk-pressure headroom
- prune only safe, regenerable artifacts without touching canonical archives or DB state

## 1. Start from the repo root

```powershell
cd C:\Users\parik\Desktop\Poly\polymarket-data-collector
```

## 2. Load the canonical runtime env if you use one

```powershell
python run_runtime.py --env-file .\.env.runtime --check-only
```

## 3. Refresh storage audit plus compaction planning

```powershell
python run_runtime_storage_status.py --env-file .\.env.runtime --refresh-storage-audit --json
```

This writes:
- `reports/phase11/runtime_storage_status.json`
- `reports/phase11/storage_audit.json`
- `reports/phase11/compaction_plan.json`

It also tells you:
- free disk headroom
- managed repo storage size
- safe prune candidates
- the retention policy for raw, detector-input, DB, logs, and replay artifacts
- the reproducibility contract for what must be kept

## 4. Replay one archived detector-input window through Phase 3

```powershell
python run_runtime_replay_window.py `
  --env-file .\.env.runtime `
  --start 2026-04-20T05:00:00+00:00 `
  --end 2026-04-20T06:00:00+00:00 `
  --phase5-source-system phase9_seed_prices `
  --phase5-source-system phase9_seed_trades `
  --json
```

This path does three things together:
- builds a restore plan for the exact window
- replays archived detector-input through Phase 3
- optionally writes a matching Phase 5 replay bundle

Important behavior:
- archived-window replay defaults to isolated in-memory Phase 3 state
- it does not advance live detector checkpoints
- it blocks by default if required partitions are missing

If you need the command to continue despite gaps:

```powershell
python run_runtime_replay_window.py `
  --start 2026-04-20T05:00:00+00:00 `
  --end 2026-04-20T06:00:00+00:00 `
  --allow-missing-partitions `
  --json
```

If you want missing windows to create Phase 5 backfill requests automatically:

```powershell
python run_runtime_replay_window.py `
  --start 2026-04-20T05:00:00+00:00 `
  --end 2026-04-20T06:00:00+00:00 `
  --request-backfill-on-missing `
  --json
```

## 5. Safe artifact pruning

Dry-run via status:

```powershell
python run_runtime_storage_status.py --json
```

Apply safe pruning:

```powershell
python run_runtime_storage_status.py --apply-prune --json
```

This only touches regenerable artifacts such as:
- old logs
- old manual replay artifacts
- old window-health artifacts
- old Phase 7 operator reports

It does not prune:
- `data/raw/`
- `data/detector_input/`
- canonical DB state
- frozen replay packets under `reports/phase5/replay_runs/phase9_task3` and `phase10_task3`

## 6. Minimum reproducibility set

Keep these if you want future windows to stay honestly replayable:
- `data/detector_input/`
- `data/raw/`
- manifest tables for raw and detector-input
- canonical runtime DB state
- frozen replay evidence packets already cited by docs

Logs are useful, but they are not part of the minimum replay contract.

## 7. Current truthfulness note

Phase 11 explicitly records that the `2026-04` raw and detector-input archive tree was already deleted before this runtime revision pass. That means some older windows are no longer locally restorable, and the restore/replay commands are expected to say so instead of hiding it.
