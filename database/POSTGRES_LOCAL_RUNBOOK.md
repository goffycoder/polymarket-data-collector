# Local PostgreSQL Runbook

This runbook is the Phase 2 path for switching the collector from the default
local SQLite runtime to a local PostgreSQL runtime on one machine.

## 1. Start PostgreSQL

This repo detected a Homebrew PostgreSQL 14 install with a data directory at:

`/opt/homebrew/var/postgresql@14`

Start it with:

```bash
brew services start postgresql@14
```

If startup fails with a `Library not loaded` error referencing
`libicui18n.74.dylib`, repair the Homebrew package first:

```bash
brew reinstall postgresql@14
brew services start postgresql@14
```

Confirm it is listening:

```bash
lsof -iTCP:5432 -sTCP:LISTEN
psql -d postgres -c "select version();"
```

## 2. Create a local database

Use the current macOS user as the PostgreSQL role unless you already manage a
separate local role.

```bash
createdb polymarket_phase2
```

If the database already exists:

```bash
psql -d postgres -c "select datname from pg_database where datname = 'polymarket_phase2';"
```

## 3. Migrate the existing SQLite dataset

```bash
venv/bin/python database/postgres_migrate.py \
  --target-url 'postgresql+psycopg://localhost:5432/polymarket_phase2'
```

Optional narrower migration for smoke tests:

```bash
venv/bin/python database/postgres_migrate.py \
  --target-url 'postgresql+psycopg://localhost:5432/polymarket_phase2' \
  --tables events markets raw_archive_manifests detector_input_manifests schema_versions replay_runs
```

## 4. Point the collector at PostgreSQL

```bash
export POLYMARKET_DB_BACKEND=postgres
export POLYMARKET_DATABASE_URL='postgresql+psycopg://localhost:5432/polymarket_phase2'
```

Verify backend selection:

```bash
venv/bin/python -c "from database.db_manager import backend_name; print(backend_name())"
```

Expected output:

```text
postgres
```

## 5. Smoke test the runtime

Apply the PostgreSQL schema:

```bash
POLYMARKET_DB_BACKEND=postgres \
POLYMARKET_DATABASE_URL='postgresql+psycopg://localhost:5432/polymarket_phase2' \
venv/bin/python -c "from database.db_manager import apply_schema; apply_schema(); print('apply_schema_ok')"
```

Check row counts:

```bash
psql -d polymarket_phase2 -c "select count(*) as events from events;"
psql -d polymarket_phase2 -c "select count(*) as markets from markets;"
psql -d polymarket_phase2 -c "select count(*) as trades from trades;"
```

Run the collector against PostgreSQL:

```bash
POLYMARKET_DB_BACKEND=postgres \
POLYMARKET_DATABASE_URL='postgresql+psycopg://localhost:5432/polymarket_phase2' \
venv/bin/python run_collector.py
```

The collector startup log should include:

```text
Runtime DB backend: postgres
```

## 6. Replay smoke test on PostgreSQL runtime

```bash
POLYMARKET_DB_BACKEND=postgres \
POLYMARKET_DATABASE_URL='postgresql+psycopg://localhost:5432/polymarket_phase2' \
venv/bin/python -m validation.run_phase2_replay \
  --start 2026-04-19T00:00:00+00:00 \
  --end 2026-04-19T01:00:00+00:00 \
  --source-system gamma_events
```

## 7. Roll back to SQLite

Unset the PostgreSQL env vars:

```bash
unset POLYMARKET_DB_BACKEND
unset POLYMARKET_DATABASE_URL
```

Or force SQLite explicitly:

```bash
export POLYMARKET_DB_BACKEND=sqlite
export POLYMARKET_SQLITE_PATH='database/polymarket_state.db'
```
