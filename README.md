# Polymarket Data Collector

High-resolution Polymarket data collection pipeline for ML applications.

## Overview

This project collects live prediction market data from [Polymarket](https://polymarket.com) via the Gamma API and CLOB API, stores it in a local SQLite database, and maintains a rolling 1-day decay window for active/closed markets.

## Project Structure

```
polymarket_arbitrage/
├── core/
│   ├── ingestor.py         # Event ingestion from Gamma API
│   ├── expand_markets.py   # Market expansion + token ID discovery
│   └── monitor.py          # CLOB price + order book monitoring
├── collectors/             # (planned) New unified collectors
├── database/
│   ├── schema.sql          # Database schema
│   └── db_manager.py       # DB connection management
├── config/
│   ├── settings.py         # API URLs and DB path
│   └── watchlists.yaml     # Tag/market watchlists
├── utilities_Scripts/
│   └── fetch_full_packet.py  # Dev tool: inspect raw API packets
├── ml_pipeline/            # (planned) ML feature engineering
├── run_monitor.py          # Main entry point
├── ingest_all_events.py    # Standalone event ingestion
├── audit_data.py           # Data quality audit tool
└── requirements.txt
```

## APIs Used

- **Gamma API**: `https://gamma-api.polymarket.com` — market metadata, events, prices
- **CLOB API**: `https://clob.polymarket.com` — live order book, trades, mid price

## Setup

```bash
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

## Running

```bash
# Full monitoring loop (events + markets + price polling)
python run_monitor.py

# One-off event ingestion
python ingest_all_events.py

# Audit snapshot data quality
python audit_data.py
```

## Status

> ⚠️ **Pre-revamp snapshot** — this branch captures the initial working state before the v2 architecture refactor. See `Documentation/` for the planned revamp.

## Planned v2 Changes
- Single unified entry point
- Rich snapshot schema (bid/ask/spread/volume_24hr/liquidity/price_change signals)
- Proper retry/backoff on all API calls
- WebSocket support for Tier 1 real-time price streams
- Full ML feature export pipeline
