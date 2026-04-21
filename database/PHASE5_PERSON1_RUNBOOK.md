# Phase 5 Person 1 Runbook

This runbook is the shortest practical path to exercise the Person 1 replay workflow on the `phase5_person1` branch.

Current Person 1 capabilities:
- replay-run artifact logging
- replay integrity checks
- multi-source replay bundles
- window-health diagnostics
- stored backfill requests for degraded windows
- backfill request dispatch with a support matrix

## 1. Move into the Person 1 worktree

```bash
cd /private/tmp/polymarket_arbitrage_phase5_person1
```

## 2. Load your environment

If you are using the shared project environment:

```bash
source /Users/vrajpatel/All-projects/polymarket_arbitrage/.env
```

Then choose the DB target:

For canonical local runtime:

```bash
export POLYMARKET_DB_BACKEND=postgres
export POLYMARKET_DATABASE_URL='postgresql+psycopg://localhost:5432/polymarket_phase3'
```

For isolated SQLite testing:

```bash
export POLYMARKET_DB_BACKEND=sqlite
export POLYMARKET_SQLITE_PATH='/tmp/polymarket_phase5_person1.db'
```

## 3. Run a replay bundle

```bash
/Users/vrajpatel/All-projects/polymarket_arbitrage/venv/bin/python run_phase5_replay.py \
  --start '2026-04-21T18:00:00+00:00' \
  --end '2026-04-21T19:00:00+00:00' \
  --source-system clob_ws_market \
  --source-system clob_books
```

This writes:
- one `replay_runs` row per source system
- one replay artifact per source
- one bundle artifact summarizing the full request

## 4. Inspect window health

```bash
/Users/vrajpatel/All-projects/polymarket_arbitrage/venv/bin/python run_phase5_window_health.py \
  --start '2026-04-21T18:00:00+00:00' \
  --end '2026-04-21T19:00:00+00:00' \
  --source-system clob_ws_market \
  --source-system clob_books
```

Use this before trusting a replay window for later validation or backtesting.

## 5. Record a backfill request for degraded windows

```bash
/Users/vrajpatel/All-projects/polymarket_arbitrage/venv/bin/python run_phase5_backfill_request.py \
  --start '2026-04-21T18:00:00+00:00' \
  --end '2026-04-21T19:00:00+00:00' \
  --source-system clob_ws_market \
  --requested-by 'vraj' \
  --reason 'Window health reported missing raw partitions'
```

This writes:
- one `backfill_requests` row per source system
- one JSON request artifact per source system

## 6. Dispatch pending backfill requests

Dry-run planning only:

```bash
/Users/vrajpatel/All-projects/polymarket_arbitrage/venv/bin/python run_phase5_backfill_dispatch.py --limit 10
```

Execute currently supported requests:

```bash
/Users/vrajpatel/All-projects/polymarket_arbitrage/venv/bin/python run_phase5_backfill_dispatch.py \
  --limit 10 \
  --execute-supported
```

Important:
- today, the automated executor only supports `data_api_trades_backfill`
- other source systems are marked `manual_required` until a dedicated backfill path exists

## 7. Useful tables to inspect

In `psql` or your DB GUI:

```sql
select * from replay_runs order by created_at desc limit 20;
select * from validation_runs order by created_at desc limit 20;
select * from backtest_artifacts order by created_at desc limit 20;
select * from backfill_requests order by created_at desc limit 20;
```

## 8. What “good” looks like

A replay window is healthiest when:
- `overall_status` is `ready`
- there are no missing raw or detector partitions
- there are no manifest count mismatches
- replay artifact paths are written successfully

## 9. What to hand to Person 2

Before Person 2 starts validation work, give them:
- one replay bundle artifact
- one window-health artifact
- the exact replay command used
- any backfill requests for degraded windows
