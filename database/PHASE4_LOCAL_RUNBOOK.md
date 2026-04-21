# Phase 4 Local Runbook

This runbook is the shortest practical path to exercise the Phase 4 local alert loop from a developer machine and capture a usable Gate 4 evidence packet.

Phase 4 assumes:
- Phase 3 candidate generation already exists or can be seeded
- local environment variables are exported in the current shell
- Telegram is the primary live delivery channel

The current implementation supports:
- evidence query logging
- evidence snapshot persistence
- alert creation
- alert suppression
- analyst feedback actions
- alert update/resend behavior
- Gate 4 summary reporting

## 1. Load environment

In the main repo:

```bash
cd /Users/vrajpatel/All-projects/polymarket_arbitrage
source .env
```

Recommended minimum Telegram variables:

```bash
export POLYMARKET_PHASE4_TELEGRAM_BOT_TOKEN='YOUR_TELEGRAM_BOT_TOKEN'
export POLYMARKET_PHASE4_TELEGRAM_CHAT_ID='YOUR_TELEGRAM_CHAT_ID'
export POLYMARKET_ENABLE_PHASE4_TELEGRAM=true
export POLYMARKET_ENABLE_PHASE4_DISCORD=false
```

Important:
- rotate the Telegram bot token if it was ever pasted into chat or logs
- Phase 4 reads environment variables from the current shell, so re-run `source .env` after editing the token

## 2. Move into the Phase 4 worktree

```bash
cd /private/tmp/polymarket_arbitrage_phase4
```

## 3. Choose a database target

For quick isolated testing:

```bash
export POLYMARKET_DB_BACKEND=sqlite
export POLYMARKET_SQLITE_PATH='/tmp/polymarket_phase4_live.db'
```

For a canonical local run tied to your Phase 3 PostgreSQL runtime:

```bash
export POLYMARKET_DB_BACKEND=postgres
export POLYMARKET_DATABASE_URL='postgresql+psycopg://localhost:5432/polymarket_phase3'
```

## 4. Bootstrap the Phase 4 workflow

```bash
/Users/vrajpatel/All-projects/polymarket_arbitrage/venv/bin/python run_phase4_bootstrap.py --pending-limit 5
```

This confirms:
- workflow registration exists
- alert/evidence tables exist
- pending candidate visibility works

## 5. Run the end-to-end pipeline

```bash
/Users/vrajpatel/All-projects/polymarket_arbitrage/venv/bin/python run_phase4_pipeline.py --limit 10
```

This performs:
- evidence pass
- alert pass
- delivery attempts

If Telegram is configured correctly, the delivered alert should appear in your Telegram bot chat.

To watch the full flow twice and confirm update/resend semantics, run the pipeline a second time after the candidate changes materially.

## 6. Record an analyst action

Find the most recent alert:

```bash
/Users/vrajpatel/All-projects/polymarket_arbitrage/venv/bin/python - <<'PY'
from database.db_manager import get_conn
conn = get_conn()
row = conn.execute("SELECT alert_id, title, alert_status FROM alerts ORDER BY created_at DESC LIMIT 5").fetchall()
conn.close()
for item in row:
    print(item[0], item[1], item[2])
PY
```

Then acknowledge one alert:

```bash
/Users/vrajpatel/All-projects/polymarket_arbitrage/venv/bin/python run_phase4_analyst_action.py \
  --alert-id 'PASTE_ALERT_ID' \
  --action acknowledge \
  --actor 'vraj' \
  --notes 'Reviewed locally'
```

Supported actions:
- `acknowledge`
- `snooze`
- `dismiss`
- `mark_useful`
- `mark_false_positive`
- `add_notes`

## 7. Run the Gate 4 report

```bash
/Users/vrajpatel/All-projects/polymarket_arbitrage/venv/bin/python -m validation.run_phase4_gate4_report --json
```

The report summarizes:
- workflow registration
- alert counts
- suppressed alerts
- delivery attempts by channel
- analyst feedback counts
- latest alert example
- assessment status

## 8. One-command Gate 4 capture

If you want the shortest full signoff path, use:

```bash
mkdir -p reports

/Users/vrajpatel/All-projects/polymarket_arbitrage/venv/bin/python run_phase4_gate4_capture.py \
  --limit 10 \
  --output reports/phase4_gate4_report.json
```

This performs:
- workflow bootstrap
- evidence pass
- alert pass
- latest-alert summary
- Gate 4 JSON report write

The output gives you:
- the latest alerts in readable form
- the Gate 4 assessment status
- the exact report path to archive

## 9. Alert update / resend test

If you want to test the update semantics, run the pipeline once, then change the candidate severity or evidence conditions, and run the pipeline again.

The expected behavior is:
- alert row count stays stable for the same candidate
- alert payload updates in place
- delivery attempts increase when the update is material enough to resend

## 10. Build a simple Gate 4 evidence packet

For a clean local signoff packet, capture all of:
- one Telegram alert screenshot
- one Gate 4 JSON report
- one analyst action example

Create the report file:

```bash
mkdir -p reports

/Users/vrajpatel/All-projects/polymarket_arbitrage/venv/bin/python -m validation.run_phase4_gate4_report \
  --json > reports/phase4_gate4_report.json
```

Then keep:
- `reports/phase4_gate4_report.json`
- the Telegram screenshot
- the alert id used for the analyst action

## 11. Time display

Human-facing Phase 4 outputs now render timestamps in:
- Eastern Time
- 12-hour format
- AM/PM
- `ET` label

Database timestamps remain stored in UTC/ISO for correctness.

## 12. Common troubleshooting

### Telegram message did not arrive

Check:

```bash
echo "$POLYMARKET_ENABLE_PHASE4_TELEGRAM"
echo "$POLYMARKET_PHASE4_TELEGRAM_CHAT_ID"
```

The token should be non-empty, the chat id should be your Telegram chat id, and the enabled flag should be `true`.

If you recently edited `.env`, reload it before retrying:

```bash
cd /Users/vrajpatel/All-projects/polymarket_arbitrage
source .env
cd /private/tmp/polymarket_arbitrage_phase4
```

### Delivery attempts are recorded as `skipped`

That means Phase 4 is healthy, but Telegram or Discord delivery is disabled or not configured in the current shell.

### No alerts were created

That usually means:
- there were no Phase 3 candidates available in the selected DB
- the candidate was already suppressed
- the candidate already had an alert and no material update occurred

### Gate 4 report says `no_alerts_yet`

Phase 4 tables exist, but there is no live or seeded alert traffic in the selected DB yet.

## 13. Safe local testing pattern

For isolated experimentation, prefer:

```bash
export POLYMARKET_DB_BACKEND=sqlite
export POLYMARKET_SQLITE_PATH='/tmp/polymarket_phase4_sandbox.db'
```

For professor/demo evidence tied to the live project runtime, prefer the same PostgreSQL database you used for the Phase 3 candidate engine.
