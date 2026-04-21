# Phase 3 Local Runbook

This runbook is the operational path for the first real local Phase 3 run:

- start Redis locally
- enable the Phase 3 detector inside the collector runtime
- let the collector produce a non-trivial live detector window
- generate candidate, reconciliation, and Gate 3 reports for that window

Phase 3 can fall back to in-memory state, but that is only useful for smoke
tests. For a real Gate 3 run, use a real local Redis instance.

## 1. Start Redis Locally

### Option A: macOS with Homebrew

Redis documents the macOS path as:

```bash
brew --version
brew install redis
brew services start redis
brew services info redis
redis-cli ping
```

Expected healthy check:

```bash
redis-cli ping
PONG
```

To stop Redis later:

```bash
brew services stop redis
```

### Option B: Docker

If you prefer Docker, Redis documents the container path as:

```bash
docker run -d --name redis -p 6379:6379 redis:latest
docker exec -it redis redis-cli
```

If you already have a local `redis-cli`, you can test with:

```bash
redis-cli -h 127.0.0.1 -p 6379 ping
```

## 2. Enable Phase 3 in the Collector Runtime

Use the canonical Phase 2+ database runtime first. PostgreSQL is the preferred
operational backend.

```bash
export POLYMARKET_DB_BACKEND=postgres
export POLYMARKET_DATABASE_URL='postgresql+psycopg://localhost:5432/polymarket_phase3'
export POLYMARKET_REDIS_URL='redis://localhost:6379/0'
export POLYMARKET_ENABLE_PHASE3_DETECTOR=true
export POLYMARKET_PHASE3_STATE_BACKEND=redis
export POLYMARKET_PHASE3_SOURCE_SYSTEMS='clob_ws_market,data_api_trades,data_api_trades_backfill,clob_prices,clob_books'
export POLYMARKET_PHASE3_POLL_SECONDS=5
```

Recommended first-run detector thresholds:

```bash
export POLYMARKET_PHASE3_MIN_FRESH_WALLET_COUNT=3
export POLYMARKET_PHASE3_MIN_FRESH_WALLET_NOTIONAL_SHARE=0.35
export POLYMARKET_PHASE3_MIN_DIRECTIONAL_IMBALANCE=0.65
export POLYMARKET_PHASE3_MIN_CONCENTRATION_RATIO=0.45
export POLYMARKET_PHASE3_MIN_PROBABILITY_VELOCITY=0.02
export POLYMARKET_PHASE3_MIN_PROBABILITY_ACCELERATION=0.005
export POLYMARKET_PHASE3_MIN_VOLUME_ACCELERATION=1.25
export POLYMARKET_PHASE3_MIN_WINDOW_NOTIONAL=250
```

## 3. Start the Collector with Phase 3 Enabled

```bash
venv/bin/python run_collector.py
```

Look for these signs in the logs:

- Redis is reachable
- the collector enters Phase 3 loops normally
- `Phase 3 detector enabled` appears in logs
- detector-input partitions continue to grow under `data/detector_input/`

Let the system run for at least 15-30 minutes before judging candidate rate.
For a better first Gate 3 evidence window, run it longer.

## 4. Record the Window You Want to Evaluate

Pick exact UTC timestamps for the live window, for example:

```bash
export PHASE3_START='2026-04-21T13:00:00+00:00'
export PHASE3_END='2026-04-21T14:00:00+00:00'
```

Use one window consistently for all reports below.

## 5. Generate the Candidate Volume Report

```bash
venv/bin/python -m validation.run_phase3_candidate_report \
  --start "$PHASE3_START" \
  --end "$PHASE3_END"
```

This tells you:

- total candidates
- unique markets
- candidate counts by hour
- rule family counts
- top markets by candidate volume

## 6. Generate the Replay Reconciliation Report

```bash
venv/bin/python -m validation.run_phase3_reconciliation \
  --start "$PHASE3_START" \
  --end "$PHASE3_END"
```

This checks whether persisted live candidates align with replayed detector
output over the same detector-input window.

## 7. Generate the Combined Gate 3 Report

```bash
venv/bin/python -m validation.run_phase3_gate3_report \
  --start "$PHASE3_START" \
  --end "$PHASE3_END" \
  --json
```

This bundles:

- detector registration
- candidate report
- replay reconciliation
- overall status assessment

Save that JSON output as the first machine-readable Gate 3 evidence artifact.

## 8. Quick Tuning Loop

If candidate volume is too low:

- lower `POLYMARKET_PHASE3_MIN_WINDOW_NOTIONAL`
- lower `POLYMARKET_PHASE3_MIN_PROBABILITY_VELOCITY`
- lower `POLYMARKET_PHASE3_MIN_FRESH_WALLET_COUNT`

If candidate volume is too high:

- raise `POLYMARKET_PHASE3_MIN_WINDOW_NOTIONAL`
- raise `POLYMARKET_PHASE3_MIN_DIRECTIONAL_IMBALANCE`
- raise `POLYMARKET_PHASE3_MIN_CONCENTRATION_RATIO`
- raise `POLYMARKET_PHASE3_MIN_VOLUME_ACCELERATION`

After any threshold change, rerun a fresh live window and regenerate the three
reports. Do not compare windows with different configs as if they were the same
experiment.

## 9. Minimum Success Condition for the First Real Phase 3 Run

The first real local Phase 3 run is successful if:

- Redis-backed detector state is active
- detector-input partitions are consumed continuously
- at least one non-synthetic candidate is emitted on real live data
- the candidate report shows bounded and explainable output
- the reconciliation report runs without structural errors
- the combined Gate 3 report can be generated for the chosen window
