# Polymarket High-Resolution Data Collector

> Phase 8 Task 1 checkpoint: a canonical inventory and cleanup pass now maps the active single-owner documentation path across Phase 1 through Phase 7 outputs.

---

## Current Status

The canonical roadmap lives in [Documentation/SRS.tex](Documentation/SRS.tex), and the single-owner planning and closeout docs live in [Documentation/phases/](Documentation/phases).

- Phase 1 through Phase 5 have canonical single-owner phase docs under `Documentation/phases/`.
- Phase 6 and Phase 7 have active committed runtime code and runner entrypoints, but some detailed planning and runbook material still carries historical split-person filenames.
- The master artifact classification for committed Phase 1 through Phase 7 outputs now lives in [Documentation/phases/phase8_canonical_inventory.tex](Documentation/phases/phase8_canonical_inventory.tex).
- The frozen end-to-end reference path for Phase 8 Task 2 now lives in [Documentation/phases/phase8_reference_path.tex](Documentation/phases/phase8_reference_path.tex), with its generated manifest under `reports/phase8/reference_window_freeze/`.
- The formal v1 operating-mode decision for Phase 8 Task 3 now lives in [Documentation/phases/phase8_v1_operating_mode.tex](Documentation/phases/phase8_v1_operating_mode.tex), with its generated policy manifest under `reports/phase8/operating_mode/`.
- The final metrics bundle and stop-condition review for Phase 8 Task 4 now lives in [Documentation/phases/phase8_metrics_review.tex](Documentation/phases/phase8_metrics_review.tex), with its generated review manifest under `reports/phase8/metrics_review/`.
- The final handoff and closeout package for Phase 8 Task 5 now lives in [Documentation/phases/phase8_final_closeout.tex](Documentation/phases/phase8_final_closeout.tex), with its generated handoff manifest under `reports/phase8/final_closeout/`.
- The next single-owner remediation plan now lives in [Documentation/phases/phase9.tex](Documentation/phases/phase9.tex). It translates the Phase 8 closeout gaps into one explicit evidence-materialization path for reaching SRS-complete v1.
- The first concrete Phase 9 execution artifact now lives in [Documentation/phases/phase9_task1_reference_window.tex](Documentation/phases/phase9_task1_reference_window.tex), with generated Task 1 outputs under `reports/phase9/reference_window_preparation/`.
- The next concrete Phase 9 execution artifact now lives in [Documentation/phases/phase9_task2_candidate_to_alert.tex](Documentation/phases/phase9_task2_candidate_to_alert.tex), with generated Task 2 outputs under `reports/phase9/candidate_to_alert_materialization/`.
- The next concrete Phase 9 execution artifact now lives in [Documentation/phases/phase9_task3_replay_validation.tex](Documentation/phases/phase9_task3_replay_validation.tex), with generated Task 3 outputs under `reports/phase5/`.
- The next concrete Phase 9 execution artifact now lives in [Documentation/phases/phase9_task4_phase6_model_completion.tex](Documentation/phases/phase9_task4_phase6_model_completion.tex), with generated Task 4 outputs under `reports/phase6/`.
- The final concrete Phase 9 execution artifact now lives in [Documentation/phases/phase9_task5_closeout_refresh.tex](Documentation/phases/phase9_task5_closeout_refresh.tex), with refreshed closeout outputs under `reports/phase9/closeout_refresh/` and regenerated Phase 8 artifacts under `reports/phase8/`.
- The next single-owner hardening plan now lives in [Documentation/phases/phase10.tex](Documentation/phases/phase10.tex). It translates the remaining Phase 9 blocker list into one operational-hardening and evidence-quality path.
- The first concrete Phase 10 execution artifact now lives in [Documentation/phases/phase10_task1_real_provider_evidence.tex](Documentation/phases/phase10_task1_real_provider_evidence.tex), with generated Task 1 outputs under `reports/phase10/real_provider_evidence_hardening/`.
- The next concrete Phase 10 execution artifact now lives in [Documentation/phases/phase10_task2_analyst_loop_expansion.tex](Documentation/phases/phase10_task2_analyst_loop_expansion.tex), with generated Task 2 outputs under `reports/phase10/analyst_loop_expansion/`.
- The next concrete Phase 10 execution artifact now lives in [Documentation/phases/phase10_task3_heldout_validation.tex](Documentation/phases/phase10_task3_heldout_validation.tex), with generated Task 3 outputs under `reports/phase5/` and `reports/phase10/heldout_validation_pack/`.
- The next concrete Phase 10 execution artifact now lives in [Documentation/phases/phase10_task4_heldout_model_completion.tex](Documentation/phases/phase10_task4_heldout_model_completion.tex), with generated Task 4 outputs under `reports/phase6/` and `reports/phase10/heldout_model_completion/`.
- The final concrete Phase 10 execution artifact now lives in [Documentation/phases/phase10_task5_ops_governance_closeout.tex](Documentation/phases/phase10_task5_ops_governance_closeout.tex), with generated Task 5 outputs under `reports/phase10/final_closeout/`.
- Runtime outputs under `data/` and `reports/` are generated locally and are not the source of truth in git by themselves.

If you are orienting yourself quickly, read these in order:

1. [README.md](README.md)
2. [Documentation/INDEX.tex](Documentation/INDEX.tex)
3. [Documentation/SRS.tex](Documentation/SRS.tex)
4. [Documentation/phases/phase8_canonical_inventory.tex](Documentation/phases/phase8_canonical_inventory.tex)
5. [Documentation/phases/phase8_reference_path.tex](Documentation/phases/phase8_reference_path.tex)
6. [Documentation/phases/phase8_v1_operating_mode.tex](Documentation/phases/phase8_v1_operating_mode.tex)
7. [Documentation/phases/phase8_metrics_review.tex](Documentation/phases/phase8_metrics_review.tex)
8. [Documentation/phases/phase8_final_closeout.tex](Documentation/phases/phase8_final_closeout.tex)
9. [Documentation/phases/phase9.tex](Documentation/phases/phase9.tex)
10. [Documentation/phases/phase9_task1_reference_window.tex](Documentation/phases/phase9_task1_reference_window.tex)
11. [Documentation/phases/phase9_task2_candidate_to_alert.tex](Documentation/phases/phase9_task2_candidate_to_alert.tex)
12. [Documentation/phases/phase9_task3_replay_validation.tex](Documentation/phases/phase9_task3_replay_validation.tex)
13. [Documentation/phases/phase9_task4_phase6_model_completion.tex](Documentation/phases/phase9_task4_phase6_model_completion.tex)
14. [Documentation/phases/phase9_task5_closeout_refresh.tex](Documentation/phases/phase9_task5_closeout_refresh.tex)
15. [Documentation/phases/phase10.tex](Documentation/phases/phase10.tex)
16. [Documentation/phases/phase10_task1_real_provider_evidence.tex](Documentation/phases/phase10_task1_real_provider_evidence.tex)
17. [Documentation/phases/phase10_task2_analyst_loop_expansion.tex](Documentation/phases/phase10_task2_analyst_loop_expansion.tex)
18. [Documentation/phases/phase10_task3_heldout_validation.tex](Documentation/phases/phase10_task3_heldout_validation.tex)
19. [Documentation/phases/phase10_task4_heldout_model_completion.tex](Documentation/phases/phase10_task4_heldout_model_completion.tex)
20. [Documentation/phases/phase10_task5_ops_governance_closeout.tex](Documentation/phases/phase10_task5_ops_governance_closeout.tex)
21. [Documentation/phases/phase8.tex](Documentation/phases/phase8.tex)
22. [Documentation/phases/phase2.tex](Documentation/phases/phase2.tex)
23. [Documentation/phases/phase4_gate4_signoff.tex](Documentation/phases/phase4_gate4_signoff.tex)

## What This Is

Polymarket is a binary prediction market. Every question ("Will X happen by Y date?") is an **event** with one or more **markets** (YES/NO token pairs trading at $0.00–$1.00, where price = probability).

This repo started as a high-resolution collector and now serves as the local-first data foundation for later detection, replay, and research phases. The live collector captures markets into a structured operational store while Phase 2 adds durable archives and replay tooling so later feature and validation work can be reproduced honestly.

**Current collection rate:** ~182 snapshots/second (88% real-time WebSocket push)

## Active Repo Map

```
collectors/      live ingestion loops and backfill paths
database/        runtime schema, DB abstraction, PostgreSQL migration, runbook
utils/           logging, HTTP helpers, Phase 2 event-log helpers
validation/      Phase 1 validation plus Phase 2 replay/republish tools
Documentation/   canonical SRS, phase docs, signoff artifacts, reference notes
ml_pipeline/     future Phase 3+ feature and modeling work
Old-content/     legacy experiments kept for reference only
```

---

## Runtime Architecture

```
run_collector.py
├── Stage 1  apply_schema()         idempotent local schema on every start
├── Stage 2  initial full sync      all events + markets from Gamma API
└── Stage 3  concurrent loops
    ├── ws_loop()       WebSocket real-time feed    (continuous push)
    ├── tier1_loop()    CLOB full order books        every 60s
    ├── tier2_loop()    CLOB best prices             every 5 min
    ├── trades_loop()   CLOB matched trades          every 5 min
    ├── sync_loop()     full Gamma re-sync           every 30 min
    └── ttl_loop()      1-day decay cleanup          every 30 min
```

```
collectors/
├── events_collector.py    Gamma /events pagination
├── markets_collector.py   Gamma /markets pagination + Tier 3 snapshots
├── price_collector.py     CLOB /books (T1) and /prices (T2)
├── trades_collector.py    CLOB /trades — recent trades every 5 min
├── ws_listener.py         WebSocket (18 connections × 500 tokens)
├── ttl_manager.py         24h decay for closed market data
└── backfill.py            Historical trade backfill from Data API

database/
├── schema.sql             canonical schema, applied idempotently
├── db_manager.py          connection pool + schema migration
├── polymarket_state.db    ← local SQLite bootstrap/dev database (gitignored)
├── postgres_schema.sql    ← local PostgreSQL target schema
├── postgres_migrate.py    ← SQLite → PostgreSQL migration helper
└── POSTGRES_LOCAL_RUNBOOK.md

utils/
├── http_client.py         async httpx with retry + backoff
├── logger.py              structured output to logs/collector.log
└── event_log.py           raw archive + detector-input manifest helpers

phase3/
├── state_store.py         Redis / memory-backed online state
├── detector.py            deterministic Phase 3 candidate logic
└── live_runner.py         detector-input tailer with durable checkpoints

phase4/
├── evidence.py            async evidence worker and provider abstraction
├── alerts.py              alert rendering, suppression, delivery, updates
├── analyst.py             analyst action workflow
└── repository.py          Phase 4 persistence layer
```

---

## Market Tiering

Re-evaluated every 30 minutes based on total USD volume:

| Tier | Volume Threshold | Markets | Data Collected |
|------|-----------------|---------|----------------|
| 1 | > $500 | ~8,700 | WebSocket push + order books (60s) + trades (5 min) |
| 2 | $50 – $500 | ~2,000 | Best price REST poll every 5 min |
| 3 | < $50 | ~25,000 | Metadata sync every 30 min only |

---

## Database

For the delivered Phase 2 workflow, local PostgreSQL is the recommended canonical runtime because replay validation and migration proofs were completed on that path. SQLite can still be used as a bootstrap or developer convenience layer when needed.

Recommended Phase 2+ runtime:

```bash
export POLYMARKET_DB_BACKEND=postgres
export POLYMARKET_DATABASE_URL='postgresql+psycopg://USER:PASS@localhost:5432/polymarket'
```

SQLite fallback:

```bash
export POLYMARKET_DB_BACKEND=sqlite
export POLYMARKET_SQLITE_PATH='database/polymarket_state.db'
```

### `events` — question containers
| Column | Type | Description |
|--------|------|-------------|
| `event_id` | TEXT PK | Polymarket event UUID |
| `title` | TEXT | Question text |
| `category`, `tags` | TEXT | Classification |
| `volume`, `volume_24hr` | REAL | Total USD traded |
| `liquidity`, `open_interest` | REAL | Market depth |
| `start_date`, `end_date` | TEXT | Resolution timeline |
| `status` | TEXT | `active` \| `closed` |
| `neg_risk` | INT | Multi-outcome market flag |

### `markets` — tradeable YES/NO instruments
| Column | Type | Description |
|--------|------|-------------|
| `market_id` | TEXT PK | Polymarket market UUID |
| `event_id` | TEXT | Parent event |
| `question` | TEXT | Market question |
| `yes_token_id`, `no_token_id` | TEXT | ERC-1155 token IDs on Polygon |
| `condition_id` | TEXT | CLOB condition identifier |
| `volume`, `liquidity` | REAL | Trading activity |
| `best_bid`, `best_ask`, `spread` | REAL | Current book top |
| `tier` | INT | `1` / `2` / `3` |
| `status` | TEXT | `active` \| `closed` |
| `outcome` | TEXT | **`YES` \| `NO` \| `N/A`** — set on resolution |
| `closed_at` | DATETIME | Resolution timestamp |

### `snapshots` — price time-series (ML training data)
| Column | Type | Description |
|--------|------|-------------|
| `id` | INT AUTOINCREMENT | Row ID |
| `market_id` | TEXT | Which market |
| `captured_at` | DATETIME | Exact timestamp |
| `mid_price` | REAL | (best_bid + best_ask) / 2 |
| `best_bid`, `best_ask`, `spread` | REAL | Order book top |
| `yes_price`, `last_trade_price` | REAL | Alternative price signals |
| `volume_24hr`, `liquidity` | REAL | Activity metrics |
| `source` | TEXT | `ws` \| `clob` \| `gamma` |

### `order_book_snapshots` — full depth (Tier 1 only)
| Column | Type | Description |
|--------|------|-------------|
| `bids_json`, `asks_json` | TEXT | Full price\|size depth arrays |
| `depth_bids`, `depth_asks` | INT | Number of price levels |
| `bid_volume`, `ask_volume` | REAL | Total liquidity each side |
| `source` | TEXT | `ws` \| `clob` |

### `trades` — matched transactions
| Column | Type | Description |
|--------|------|-------------|
| `trade_id` | TEXT PK | Deduplicated trade ID |
| `side` | TEXT | `BUY` \| `SELL` |
| `price`, `size` | REAL | Execution details |
| `trade_time` | DATETIME | On-chain timestamp |
| `source` | TEXT | `ws` \| `clob` \| `clob_backfill` |

### `market_resolutions` — ground truth labels for ML
| Column | Type | Description |
|--------|------|-------------|
| `market_id` | TEXT | Resolved market |
| `outcome` | TEXT | **`YES` \| `NO` \| `N/A`** |
| `final_price` | REAL | 1.0 = YES won, 0.0 = NO won |
| `resolved_at` | DATETIME | Resolution timestamp |
| `source` | TEXT | `ws` \| `api` |

---

## 1-Day Decay (TTL)

The database stays lean while preserving all live market history:

- **Active markets** → all data kept indefinitely
- **Closed markets** → snapshots, order books, and trades older than 24h are deleted every 30 min
- **Market metadata + outcome labels** → never deleted (permanently archived)

---

## Setup

```bash
# 1. Clone and install
git clone <repo-url> && cd polymarket_arbitrage
python -m venv venv && source venv/bin/activate   # Windows: venv\Scripts\activate
pip install -r requirements.txt

# 2. Recommended: use local PostgreSQL for the canonical Phase 2 runtime
export POLYMARKET_DB_BACKEND=postgres
export POLYMARKET_DATABASE_URL='postgresql+psycopg://USER:PASS@localhost:5432/polymarket'

# 3. Run interactively (for testing)
python run_collector.py

# 4. Register as macOS background service
sed -e "s|__PROJECT_DIR__|$(pwd)|g" \
    -e "s|__PYTHON_PATH__|$(pwd)/venv/bin/python|g" \
    polymarket.plist > ~/Library/LaunchAgents/com.polymarket.collector.plist
launchctl load ~/Library/LaunchAgents/com.polymarket.collector.plist
```

To migrate an existing local SQLite dataset into PostgreSQL:

```bash
venv/bin/python database/postgres_migrate.py \
  --target-url 'postgresql+psycopg://USER:PASS@localhost:5432/polymarket'
```

The full local Phase 2 runbook is in [database/POSTGRES_LOCAL_RUNBOOK.md](database/POSTGRES_LOCAL_RUNBOOK.md).

For the first real Phase 3 live run and Gate 3 reporting workflow, use
[database/PHASE3_LOCAL_RUNBOOK.md](database/PHASE3_LOCAL_RUNBOOK.md).

## Phase 2 Deliverables In This Repo

- Durable raw envelope archives under `data/raw/`
- Detector-input logs under `data/detector_input/`
- Replay validation CLI in `validation/run_phase2_replay.py`
- Replay republish CLI in `validation/run_phase2_republish.py`
- PostgreSQL schema and migration tooling in `database/`
- Gate 2 delivery and signoff docs in `Documentation/phases/`

## Phase 3 Runtime Commands

Smoke-test the live detector worker:

```bash
venv/bin/python run_phase3_live.py --once
```

Run the combined Gate 3 evidence report for a chosen window:

```bash
venv/bin/python -m validation.run_phase3_gate3_report \
  --start '2026-04-21T13:00:00+00:00' \
  --end '2026-04-21T14:00:00+00:00' \
  --json
```

To run Phase 3 inside the main collector runtime, enable:

```bash
export POLYMARKET_ENABLE_PHASE3_DETECTOR=true
venv/bin/python run_collector.py
```

## What Is Legacy

`Old-content/` is historical.

`Documentation/person1Phases/` and `Documentation/person2Phases/` are historical by default, but a few later-phase artifacts inside them still remain supporting references until single-owner replacements are written. The authoritative classification now lives in [Documentation/phases/phase8_canonical_inventory.tex](Documentation/phases/phase8_canonical_inventory.tex).

---

## Process Control

```bash
# ── STATUS ────────────────────────────────────────────────────────
launchctl list | grep polymarket          # PID + last exit code (0 = running OK)

# ── LOGS ──────────────────────────────────────────────────────────
tail -f logs/collector.log                # live log stream (Ctrl+C just stops watching)
tail -100 logs/launchd_stderr.log         # crash tracebacks

# ── START / STOP ──────────────────────────────────────────────────
launchctl load   ~/Library/LaunchAgents/com.polymarket.collector.plist
launchctl unload ~/Library/LaunchAgents/com.polymarket.collector.plist

# ── FORCE KILL (if unresponsive) ──────────────────────────────────
pkill -9 -f run_collector.py

# ── DATA HEALTH CHECK ─────────────────────────────────────────────
python Old-content/audit_v2.py

# ── HISTORICAL TRADE BACKFILL ─────────────────────────────────────
python -m collectors.backfill               # all history, all T1 markets
python -m collectors.backfill --days 30     # last 30 days only
python -m collectors.backfill --limit 10    # test on 10 markets first

# ── GIT ───────────────────────────────────────────────────────────
git add -A && git commit -m "chore: ..." && git push
```

---

## Query Examples

```python
import sqlite3, pandas as pd
conn = sqlite3.connect('database/polymarket_state.db')

# Price history for a specific market
df = pd.read_sql("""
    SELECT captured_at, mid_price, best_bid, best_ask, spread, source
    FROM snapshots
    WHERE market_id = '<market_id>'
    ORDER BY captured_at
""", conn, parse_dates=['captured_at'])

# All resolved markets with their outcomes (ML ground truth)
labels = pd.read_sql("""
    SELECT m.market_id, m.question, m.volume, m.tier,
           r.outcome, r.final_price, r.resolved_at
    FROM market_resolutions r
    JOIN markets m ON m.market_id = r.market_id
    ORDER BY r.resolved_at DESC
""", conn)

# Order book depth over time (T1 only)
books = pd.read_sql("""
    SELECT market_id, captured_at, best_bid, best_ask, spread,
           depth_bids, depth_asks, bid_volume, ask_volume
    FROM order_book_snapshots
    WHERE captured_at > datetime('now', '-1 day')
    ORDER BY market_id, captured_at
""", conn)
```

---

## Collaboration

The **code** is fully cross-platform. The **database** is local to each machine.

| Platform | How to run |
|----------|-----------|
| macOS | `launchctl load ...` (background service) |
| Windows | `python run_collector.py` in terminal, or Task Scheduler |
| Linux | `nohup python run_collector.py &` or systemd unit |

To share data: export CSVs from one machine, or set up a shared server.

---

## API Limits

| Endpoint | Polymarket Limit | Our Usage |
|----------|-----------------|-----------|
| Gamma REST | 500 req/10s | ~3 req/s |
| CLOB REST | 500 req/10s | 8 concurrent (Semaphore-capped) |
| Data API `/trades` | 200 req/10s | 4 concurrent (backfill) |
| CLOB WebSocket | — | 18 persistent connections |

Exponential backoff on all timeout/rate-limit responses. No risk of ban.

---

*Last updated: March 2026*
