# Phase 7 Person 1 Runbook

This runbook starts the Phase 7 operational scale-up track on `phase7_person1`.

Current capabilities in this first block:
- storage / archive audit
- archive tiering recommendations
- persisted audit runs
- persisted partition-level tiering decisions

Current capabilities in this second block:
- long-run dashboard JSON
- compaction / cold-archive planning
- persisted compaction plan runs

Current capabilities in this third block:
- health summary JSON
- restore-plan generation for historical windows
- persisted restore-plan runs

Current capabilities in this fourth block:
- daily / weekly integrity summaries
- redundancy-readiness reporting
- persisted integrity summary runs

Current capabilities in this fifth block:
- policy-enforcement dry-run batches
- persisted archive action runs
- persisted archive action items

Current capabilities in this sixth block:
- bottleneck and failure inventory
- persisted service profile runs
- ranked operator-facing problem list

Current capabilities in this seventh block:
- consolidated Phase 7 Person 1 report
- one assessment payload for audit / integrity / policy / profiling state

Current capabilities in this final close-out block:
- Person 2 handoff artifact
- Gate 7 report
- Gate 7 signoff memo

## 1. Move into the worktree

```bash
cd /private/tmp/polymarket_arbitrage_phase7_person1
git branch --show-current
```

Expected branch:

```bash
phase7_person1
```

## 2. Load environment

```bash
source /Users/vrajpatel/All-projects/polymarket_arbitrage/.env

export POLYMARKET_DB_BACKEND=postgres
export POLYMARKET_DATABASE_URL='postgresql+psycopg://localhost:5432/polymarket_phase3'
```

## 3. Run the first storage audit

```bash
/Users/vrajpatel/All-projects/polymarket_arbitrage/venv/bin/python run_phase7_storage_audit.py \
  --audit-scope full_repo \
  --json
```

This writes:
- one `storage_audit_runs` row
- many `archive_tiering_decisions` rows
- one JSON report at `reports/phase7/storage_audit.json`

## 4. Build the dashboard view

```bash
/Users/vrajpatel/All-projects/polymarket_arbitrage/venv/bin/python run_phase7_dashboard.py --json
```

This writes:
- one JSON dashboard at `reports/phase7/dashboard.json`
- runtime rollups for candidates, alerts, and shadow-score activity
- top source and largest-partition summaries

## 5. Build the compaction plan

```bash
/Users/vrajpatel/All-projects/polymarket_arbitrage/venv/bin/python run_phase7_compaction_plan.py --json
```

This writes:
- one `compaction_plan_runs` row
- one JSON plan at `reports/phase7/compaction_plan.json`

## 6. Build the health summary

```bash
/Users/vrajpatel/All-projects/polymarket_arbitrage/venv/bin/python run_phase7_health_summary.py --json
```

This writes:
- one JSON summary at `reports/phase7/health_summary.json`
- latest audit freshness and missing-partition status
- recent alert / shadow-score activity rollups

## 7. Build a restore plan for a historical window

```bash
/Users/vrajpatel/All-projects/polymarket_arbitrage/venv/bin/python run_phase7_restore_plan.py \
  --start '2026-04-20T00:00:00+00:00' \
  --end '2026-04-21T00:00:00+00:00' \
  --json
```

This writes:
- one `restore_plan_runs` row
- one JSON restore plan at `reports/phase7/restore_plan.json`
- one list of required partitions and whether they are missing / hot / cold

## 8. Useful tables

```sql
select * from storage_audit_runs order by created_at desc limit 20;
select * from archive_tiering_decisions order by created_at desc limit 50;
select * from compaction_plan_runs order by created_at desc limit 20;
select * from restore_plan_runs order by created_at desc limit 20;
select * from integrity_summary_runs order by created_at desc limit 20;
```

## 9. Build integrity and redundancy summaries

```bash
/Users/vrajpatel/All-projects/polymarket_arbitrage/venv/bin/python run_phase7_integrity_summary.py --json
/Users/vrajpatel/All-projects/polymarket_arbitrage/venv/bin/python run_phase7_redundancy_readiness.py --json
```

This writes:
- one `integrity_summary_runs` row
- one JSON summary at `reports/phase7/integrity_summary.json`
- one JSON readiness report at `reports/phase7/redundancy_readiness.json`

## 10. Build one dry-run policy enforcement batch

```bash
/Users/vrajpatel/All-projects/polymarket_arbitrage/venv/bin/python run_phase7_policy_enforcement.py --json
```

This writes:
- one `archive_action_runs` row
- many `archive_action_items` rows
- one JSON action batch at `reports/phase7/policy_enforcement.json`

The current implementation is intentionally safe:
- it does not delete files
- it does not move files
- it only records the batch of compaction / cold-archive / investigation actions that would be taken

## 11. Build the bottleneck inventory

```bash
/Users/vrajpatel/All-projects/polymarket_arbitrage/venv/bin/python run_phase7_bottleneck_inventory.py --json
```

This writes:
- one `service_profile_runs` row
- one JSON inventory at `reports/phase7/bottleneck_inventory.json`
- a ranked list of stale subsystems and likely operational pain points

## 12. Render the consolidated Phase 7 Person 1 report

```bash
/Users/vrajpatel/All-projects/polymarket_arbitrage/venv/bin/python validation/run_phase7_person1_report.py --json
```

This prints:
- the latest audit / integrity / action-batch / service-profile state
- one assessment object saying whether follow-up is still required

## 13. Build the Person 2 handoff and Gate 7 report

```bash
/Users/vrajpatel/All-projects/polymarket_arbitrage/venv/bin/python run_phase7_person2_handoff.py --json
/Users/vrajpatel/All-projects/polymarket_arbitrage/venv/bin/python validation/run_phase7_gate7_report.py --json
```

This writes or prints:
- one handoff JSON at `reports/phase7/person2_handoff.json`
- one Gate 7 assessment payload
- one signoff memo at `Documentation/phases/phase7_gate7_signoff.tex`

## 14. What these blocks are for

This is the Phase 7 operational baseline:
- identify the largest retained partitions
- identify missing archive files vs manifest rows
- classify hot / warm / cold / archive-only age tiers
- identify compaction and cold-archive candidates before later dashboard or HA work
- create one operator-facing dashboard summary before redundancy work starts
- define how one historical window would be restored before actual cold-storage moves happen
- track daily / weekly integrity trends before scale-up decisions
- state clearly whether the system is operationally ready for redundancy design
- create a safe execution ledger before any later archive policy automation is allowed
- measure bottlenecks and failure risks before making scale-up or HA decisions
- package the current Phase 7 Person 1 state into one reviewable report before push or merge
- hand Person 2 the final retained-data and restore guarantees they need for research work
- close the phase with an explicit Gate 7 assessment and signoff memo
