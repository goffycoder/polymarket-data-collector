# Runner Index

This repo intentionally keeps phase runners at the repo root for now so old reports
and runbooks keep working. Use this index as the active operating surface.

## Runtime

```bash
venv/bin/python run_runtime_status.py --json
venv/bin/python run_runtime_storage_status.py --env-file .env.runtime --json
venv/bin/python run_runtime.py --check-only
```

## CLOB V2 Public Data

```bash
venv/bin/python run_clob_v2_smoke.py --env-file .env.runtime --json
venv/bin/python run_clob_v2_fee_refresh.py --env-file .env.runtime --market-limit 10 --json
```

These checks prove public data compatibility only. They do not prove live order
placement, pUSD collateral handling, builder attribution, or settlement.

## Wallet Plane

```bash
venv/bin/python run_wallet_trade_refresh.py --env-file .env.runtime --market-limit 10 --json
venv/bin/python run_wallet_entity_materializer.py --env-file .env.runtime --json
venv/bin/python run_wallet_position_refresh.py --env-file .env.runtime --json
venv/bin/python run_wallet_cluster_materializer.py --env-file .env.runtime --json
venv/bin/python run_wallet_profile_refresh.py --env-file .env.runtime --json
```

## Replay and Storage

```bash
venv/bin/python run_phase7_storage_audit.py --output reports/phase12/storage_audit.json --json
venv/bin/python run_runtime_replay_window.py --start <UTC_START> --end <UTC_END> --json
```

For SSD-backed archives, set these in `.env.runtime`:

```env
POLYMARKET_RAW_ARCHIVE_ROOT="/Volumes/<SSD>/polymarket_archive/raw"
POLYMARKET_DETECTOR_INPUT_ROOT="/Volumes/<SSD>/polymarket_archive/detector_input"
POLYMARKET_ARCHIVE_ROOT_READONLY=true
```

## Legacy

`Old-content/` is historical. Do not use it for current health checks or runtime
operations.
