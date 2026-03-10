# Polymarket High-Resolution Data Collector

A production-grade, always-on data pipeline that captures the full state of the Polymarket prediction market at maximum resolution — events, markets, order books, trades, and real-time price feeds — into a local SQLite database for ML model training and research.

---

## What This Does

Polymarket is a binary prediction market. Every question ("Will X happen?") is an **event** containing one or more **markets** (YES/NO token pairs). Prices are probabilities in $0.00–$1.00.

This collector runs continuously in the background and captures:

| Data Type | Source | Frequency |
|---|---|---|
| Event metadata | Gamma REST API | Every 30 min |
| Market metadata + prices | Gamma REST API | Every 30 min |
| Tier 1 full order books | CLOB REST API | Every 60s |
| Tier 2 best prices | CLOB REST API | Every 5 min |
| Matched trades | CLOB REST API | Every 5 min |
| Real-time price/book events | CLOB WebSocket | Continuous push |

---

## Architecture

```
run_collector.py               ← single entry point, manages all loops
├── PHASE 1: apply_schema()    ← idempotent SQLite schema on every startup
├── PHASE 2: initial sync      ← full events + markets pull on startup
└── PHASE 3: concurrent loops
    ├── ws_loop()              ← WebSocket real-time feed (Tier 1)
    ├── tier1_loop()           ← CLOB /books every 60s
    ├── tier2_loop()           ← CLOB /prices every 5 min
    ├── trades_loop()          ← CLOB /trades every 5 min
    ├── sync_loop()            ← full Gamma re-sync every 30 min
    └── ttl_loop()             ← 1-day decay cleanup every 30 min

collectors/
├── events_collector.py        ← Gamma /events pagination
├── markets_collector.py       ← Gamma /markets pagination + Tier 3 snapshots
├── price_collector.py         ← CLOB /books (Tier 1) and /prices (Tier 2)
├── trades_collector.py        ← CLOB /trades
├── ws_listener.py             ← WebSocket subscriber (18 connections × 500 tokens)
└── ttl_manager.py             ← 1-day decay for closed market data

database/
├── schema.sql                 ← canonical schema, applied idempotently on startup
└── db_manager.py              ← SQLite connection pool + schema applier

utils/
├── http_client.py             ← async httpx wrapper with retry, rate-limit handling
└── logger.py                  ← structured logging to logs/collector.log
```

---

## Market Tiering

Every market is assigned a tier based on total USD volume traded, re-evaluated every 30 minutes:

```
Tier 1 (volume > $500)   → 8,700+ markets  → WebSocket + CLOB order books + trades
Tier 2 ($50–$500)        → 2,000+ markets  → REST price poll every 5 min
Tier 3 (< $50)           → 25,000+ markets → Gamma metadata sync every 30 min only
```

---

## Database Schema

**5 tables** — all in `database/polymarket_state.db`

### `events`
Top-level question containers (e.g. "2024 US Election").
```
event_id PK | title | description | tags | category
volume | volume_24hr | liquidity | open_interest
start_date | end_date | status ('active'|'closed')
neg_risk | featured | first_seen_at | last_updated_at | closed_at
```

### `markets`
Individual YES/NO tradeable instruments inside an event.
```
market_id PK | event_id FK | question | condition_id
yes_token_id | no_token_id    ← ERC-1155 token IDs on Polygon
outcomes | outcome_prices     ← JSON arrays
volume | liquidity | best_bid | best_ask | spread | last_trade_price
tier (1|2|3) | status ('active'|'closed')
start_date | end_date | first_seen_at | last_updated_at | closed_at
```

### `snapshots`
Time-series price observations — the ML training data.
```
id AUTOINCREMENT | market_id | captured_at
yes_price | no_price | last_trade_price | mid_price
best_bid | best_ask | spread
volume_total | volume_24hr | liquidity
price_change_1d | price_change_1wk
source ('ws'|'clob'|'gamma')
```

### `order_book_snapshots`
Full order book depth for Tier 1 markets (WebSocket + 60s CLOB poll).
```
id | market_id | token_id | captured_at
bids_json | asks_json          ← full JSON depth arrays
best_bid | best_ask | spread
depth_bids | depth_asks        ← count of price levels
bid_volume | ask_volume        ← total liquidity each side
source ('ws'|'clob')
```

### `trades`
Individual matched transactions for Tier 1 markets.
```
trade_id PK | market_id | token_id
side ('BUY'|'SELL') | price | size
trade_time | captured_at
source ('ws'|'clob')
```

---

## 1-Day Decay (TTL)

The collector maintains a **live + recent** database — not an archive:

- **Active market data**: kept forever (full price history for ML)
- **Closed market data**: snapshots, order books, and trades older than 24 hours are deleted every 30 minutes
- **Market/event metadata rows**: never deleted (status = `'closed'` is preserved)

This keeps the DB lean while retaining the complete history of everything currently tradeable.

---

## Quick Start

```bash
# 1. Install dependencies
python -m venv venv && source venv/bin/activate
pip install -r requirements.txt

# 2. Run directly (interactive, for testing)
python run_collector.py

# 3. Register as background service (auto-starts on login)
sed -e "s|__PROJECT_DIR__|$(pwd)|g" \
    -e "s|__PYTHON_PATH__|$(pwd)/venv/bin/python|g" \
    polymarket.plist > ~/Library/LaunchAgents/com.polymarket.collector.plist
launchctl load ~/Library/LaunchAgents/com.polymarket.collector.plist
```

## Service Management

```bash
# Status
launchctl list | grep polymarket     # shows PID + exit code

# Logs
tail -f logs/collector.log           # live log stream
tail -100 logs/launchd_stderr.log    # crash tracebacks

# Stop / start
launchctl unload ~/Library/LaunchAgents/com.polymarket.collector.plist
launchctl load  ~/Library/LaunchAgents/com.polymarket.collector.plist

# Force-kill if stuck
pkill -9 -f run_collector.py
```

## Data Verification

```bash
python audit_v2.py      # full health check with date ranges + TTL validation
```

---

## API Rate Limits (Polymarket)

| Endpoint | Limit | Our Usage |
|---|---|---|
| Gamma REST | 500 req/10s | ~3 req/s (well under) |
| CLOB REST | 500 req/10s | ~8 concurrent, Semaphore-capped |
| CLOB WebSocket | No documented limit | 18 persistent connections |

The collector uses exponential backoff on 429s and timeouts. It will **never** get banned under normal operation.

---

## Project Structure

```
polymarket_arbitrage/
├── run_collector.py         ← entry point
├── audit_v2.py              ← data health check
├── requirements.txt
├── polymarket.plist         ← launchd service template
├── setup.sh                 ← one-time setup helper
│
├── collectors/              ← data pipeline modules
│   ├── events_collector.py
│   ├── markets_collector.py
│   ├── price_collector.py
│   ├── trades_collector.py
│   ├── ws_listener.py
│   └── ttl_manager.py
│
├── database/
│   ├── schema.sql           ← canonical schema
│   ├── db_manager.py        ← connection + schema applier
│   └── polymarket_state.db  ← SQLite database (gitignored)
│
├── utils/
│   ├── http_client.py       ← async HTTP with retry
│   └── logger.py
│
├── logs/                    ← gitignored
│   ├── collector.log
│   └── launchd_stderr.log
│
├── Documentation/           ← Polymarket API docs reference
└── Old-content/             ← archived v1 scripts (reference only)
```

---

## Data for ML

Each `snapshots` row is one timestamped observation of a market's price state. To build a price history for a market:

```python
import sqlite3, pandas as pd

conn = sqlite3.connect('database/polymarket_state.db')
df = pd.read_sql("""
    SELECT s.captured_at, s.mid_price, s.best_bid, s.best_ask, s.spread,
           s.volume_24hr, s.source, m.question, m.tier
    FROM snapshots s
    JOIN markets m ON m.market_id = s.market_id
    WHERE m.tier = 1 AND s.captured_at > datetime('now', '-7 days')
    ORDER BY s.market_id, s.captured_at
""", conn)
```

Order book depth (for spread/liquidity features):
```python
df_books = pd.read_sql("""
    SELECT market_id, captured_at, best_bid, best_ask, spread,
           depth_bids, depth_asks, bid_volume, ask_volume, bids_json, asks_json
    FROM order_book_snapshots
    WHERE captured_at > datetime('now', '-1 day')
    ORDER BY market_id, captured_at
""", conn)
```
