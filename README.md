# Polymarket High-Resolution Data Collector

> Capturing Polymarket prediction markets at **182 snapshots/second** — real-time WebSocket feed, full order book depth, matched trades, and automatic outcome labels — all in a local SQLite database ready for ML.

---

## What This Is

Polymarket is a binary prediction market. Every question ("Will X happen by Y date?") is an **event** with one or more **markets** (YES/NO token pairs trading at $0.00–$1.00, where price = probability).

This collector runs as a background service and captures the complete state of **35,000+ markets** across all resolution timelines into a structured database — designed for ML model training and quantitative research.

**Current collection rate:** ~182 snapshots/second (88% real-time WebSocket push)

---

## Architecture

```
run_collector.py
├── PHASE 1  apply_schema()         idempotent SQLite schema on every start
├── PHASE 2  initial full sync      all events + markets from Gamma API
└── PHASE 3  concurrent loops
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
└── polymarket_state.db    ← your database (gitignored)

utils/
├── http_client.py         async httpx with retry + backoff
└── logger.py              structured output to logs/collector.log
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

## Database — `database/polymarket_state.db`

Six tables, single SQLite file, WAL mode.

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

# 2. Run interactively (for testing)
python run_collector.py

# 3. Register as macOS background service
sed -e "s|__PROJECT_DIR__|$(pwd)|g" \
    -e "s|__PYTHON_PATH__|$(pwd)/venv/bin/python|g" \
    polymarket.plist > ~/Library/LaunchAgents/com.polymarket.collector.plist
launchctl load ~/Library/LaunchAgents/com.polymarket.collector.plist
```

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
python audit_v2.py

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
