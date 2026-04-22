# Phase 5 Single-Owner Runbook

This runbook is the shortest practical path to exercise the canonical single-owner Phase 5 workflow in this repo.

Use this when you want to:
- verify replay coverage for one historical window
- write replay bundle artifacts under `reports/phase5/`
- generate the Phase 5 holdout validation and conservative paper-trading artifacts
- inspect the durable summary rows in `replay_runs`, `validation_runs`, and `backtest_artifacts`

## 1. Move into the repo root

```bash
cd C:\Users\parik\Desktop\Poly\polymarket-data-collector
```

## 2. Choose the runtime

For the shared local SQLite workspace:

```bash
$env:POLYMARKET_DB_BACKEND='sqlite'
$env:POLYMARKET_SQLITE_PATH='C:\Users\parik\Desktop\Poly\polymarket-data-collector\database\polymarket_state.db'
```

If you are using PostgreSQL instead, point the canonical repo at that database before running the same commands.

## 3. Canonical Phase 9 reference-window replay path

The currently frozen reference hour is:

```text
2026-04-20T05:00:00+00:00 -> 2026-04-20T06:00:00+00:00
```

To rebuild the full single-owner Phase 5 evidence packet for that hour:

```bash
python run_phase9_phase5_evaluation.py --json
```

This writes:
- replay artifacts under `reports/phase5/replay_runs/phase9_task3/`
- holdout validation outputs under `reports/phase5/validation/`
- conservative backtest outputs under `reports/phase5/backtests/`
- durable summary rows in `replay_runs`, `validation_runs`, and `backtest_artifacts`

## 4. Generic replay bundle path

If you want to run replay for another window or source system:

```bash
python run_phase5_replay.py ^
  --start 2026-04-20T05:00:00+00:00 ^
  --end 2026-04-20T06:00:00+00:00 ^
  --source-system phase9_seed_prices ^
  --source-system phase9_seed_trades ^
  --output-dir reports/phase5/replay_runs/manual
```

## 5. Generic replay health check

```bash
python run_phase5_window_health.py ^
  --start 2026-04-20T05:00:00+00:00 ^
  --end 2026-04-20T06:00:00+00:00 ^
  --source-system phase9_seed_prices ^
  --source-system phase9_seed_trades ^
  --output-dir reports/phase5/window_health
```

Use this before trusting a new historical window.

## 6. Useful tables

```sql
select * from replay_runs order by created_at desc limit 20;
select * from validation_runs order by created_at desc limit 20;
select * from backtest_artifacts order by created_at desc limit 20;
select * from backfill_requests order by created_at desc limit 20;
```

## 7. What “good” looks like

A healthy single-owner Phase 5 packet has:
- a replay bundle with `overall_status = ready`
- non-zero `validation_runs` and `backtest_artifacts`
- a holdout report that does not rely on future leakage
- a conservative paper-trading artifact with explicit slippage and fee assumptions

## 8. Current truthfulness note

The current canonical Task 3 packet is materially populated and replay-safe, but still very small. It is acceptable as a concrete evidence packet for Phase 9, not as a claim of broad statistical strength.
