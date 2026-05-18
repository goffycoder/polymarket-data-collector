# Phase 13 Final 24-Hour Presentation Plan

Date: May 18, 2026
Presentation deadline: May 19, 2026 at 2:00 PM ET
Window available: roughly 24 hours

## Executive Answer

Keep the collector running for now.

The current runtime is alive and writing fresh state. The correct posture for the next 24 hours is to treat this as the final organic proof window, not as an unlimited production run. Do not stop it just because the repo is dirty or because Phase 12 still has open work. Stop or restart only for one of the explicit stop conditions below.

The most valuable presentation outcome is not "everything possible is implemented." The strongest outcome is:

1. A live collector ran through a long organic window.
2. The probability-band alert gate is proven in live data.
3. Phase 4 remains the authoritative alert path.
4. Phase 6 LightGBM remains shadow-only and is reviewed as priority guidance.
5. Remaining Phase 12 work is honestly classified as implemented, validated, deferred, or out of scope.
6. The repo has a final evidence pack with status, screenshots/links, reports, and a crisp narrative.

## Current Runtime Posture

Observed at the start of this plan:

- `run_runtime.py --env-file .env.runtime` is running.
- `run_collector.py` is running.
- `run_phase4_live.py` is running.
- `run_phase6_shadow_live.py --iterations 0` is running.
- Disk has roughly 54 GiB free on the local data volume.
- Runtime status shows fresh Phase 3 checkpoints, fresh Phase 4 evidence activity, and fresh Phase 6 shadow scoring.
- Runtime profile is `shadow_live`.
- Alert authority is still `phase4_rule_based`.
- ML authority is still `phase6_shadow_only`.

Important caveat: local storage is already in a warning posture because archive reconciliation still reports missing external archive files. This does not mean the live proof is invalid, but it does mean the collector should be monitored and not left unattended beyond the presentation window.

## Stop Or Keep Running?

### Keep Running If

- Disk free space stays above 35 GiB.
- The process list still shows the runtime, collector, Phase 4 live, and Phase 6 shadow processes.
- Telegram delivery volume stays within the configured cap.
- New alerts are mostly inside the intended YES probability band.
- Runtime status continues to show fresh checkpoints or evidence progress.

### Stop Gracefully If

- Disk free space drops below 35 GiB.
- Telegram sends become noisy again.
- Phase 4 starts alerting markets below 10% YES probability or above 95% YES probability after the latest filter commit.
- The collector repeatedly crashes or restarts.
- You need to apply code changes that affect collector, detector, Phase 4, settings, database schema, or alert rendering.
- It is within 2 hours of the presentation and the proof pack has not been captured yet.

### Do Not Do During The Final Window

- Do not promote Phase 6 ML to control alerts.
- Do not chase full autonomous trading support.
- Do not spend the window on broad refactors.
- Do not try to make Discord production-ready unless Telegram breaks.
- Do not claim wallet identity or real-world person identification.
- Do not claim archive completeness until SSD reconciliation passes.

## 24-Hour Work Plan

### Block 1: Now To +2 Hours

Goal: prove the current run is valid and not wasting compute.

Actions:

1. Keep collector running.
2. Capture current process, disk, and runtime status.
3. Check recent alert payloads for probability-band compliance.
4. Confirm that filtered candidates are recorded as `probability_filtered` instead of silently passing downstream.
5. Save a short note with current candidate, alert, delivery, and shadow-score counts.

Acceptance:

- Runtime still alive.
- Disk still above 35 GiB.
- Probability-band filters are active.
- No immediate Telegram-noise failure.

### Block 2: +2 To +6 Hours

Goal: convert the live run into evidence.

Actions:

1. Let the collector keep accumulating fresh raw, detector-input, candidate, evidence, alert, and shadow-score rows.
2. Run a Phase 12 ML live alert review over the latest 24-hour window.
3. Summarize score distribution, coverage, top-scored alerts, and delivered-alert coverage.
4. Sample a few alerts manually against Polymarket to confirm the alert URLs and displayed probabilities make sense.
5. Record whether low-probability and near-certain markets are being blocked.

Acceptance:

- ML coverage is measurable.
- ML remains non-authoritative.
- There is evidence that the 10%-95% YES probability band is reducing bad candidates.
- Alert URLs and condition IDs are traceable.

### Block 3: +6 To +12 Hours

Goal: finish the highest-value remaining Phase 12 items that fit in one night.

Feasible implementation items:

1. Add a small final-status report command or manual report summarizing Phase 12 completion by area.
2. Add a probability-band audit report for recent candidates and alerts.
3. Add or refresh a repo hygiene note separating source changes from generated runtime artifacts.
4. Re-run wallet entity and cluster materializers if safe.
5. Decide wallet profile/position status: either implement a small provider-disabled report or explicitly mark it deferred.

Do not attempt:

- Full wallet profile provider integration unless endpoint behavior is already known.
- Large ML retraining from scratch.
- Full SSD archive reconciliation unless the SSD is visible immediately.
- Any risky DB migration while the live proof is running.

Acceptance:

- A final status artifact exists.
- Probability-band behavior is auditable.
- Wallet profile/position gap is honestly handled.
- Repo hygiene risk is documented.

### Block 4: +12 To +18 Hours

Goal: produce the final proof pack.

Actions:

1. Run runtime status.
2. Run storage status.
3. Run Phase 6 registry status.
4. Run ML live alert review.
5. Run archive reconciliation if SSD is visible; otherwise preserve the `external_archive_not_visible` result honestly.
6. Capture a concise table:
   - runtime duration,
   - raw archive growth,
   - detector-input growth,
   - trade delta,
   - candidate delta,
   - evidence delta,
   - alert delta,
   - delivered alert count,
   - shadow-score count,
   - filtered probability-band count,
   - storage free space.

Acceptance:

- Proof pack is machine-readable and presentation-readable.
- It is clear what was proven live versus replay/offline.
- It is clear what remains deferred.

### Block 5: +18 To +22 Hours

Goal: stabilize, clean, and freeze presentation claims.

Actions:

1. Stop the collector gracefully if the proof pack is sufficient or disk is getting tight.
2. Keep it running only if it is still healthy and still adding meaningful evidence.
3. Do not keep making core code changes unless a presentation-blocking issue appears.
4. Review dirty files and decide what should be committed:
   - source changes,
   - final docs,
   - compact reports,
   - not giant live-shadow/report noise.
5. Prepare final claim language.

Acceptance:

- The repo can be explained without apologizing for mixed source/generated artifacts.
- The presentation story is frozen.
- Any remaining dirty files are either intentionally included or intentionally ignored.

### Block 6: Final 2 Hours

Goal: presentation readiness.

Actions:

1. Stop long-running collection unless there is a specific reason to keep it alive for a live demo.
2. Capture one last process/status snapshot.
3. Capture final runtime report paths and commit hash.
4. Prepare a 5-minute demo path:
   - open SRS/scope,
   - show runtime status,
   - show alert sample,
   - show probability filtering,
   - show ML shadow review,
   - show remaining work table.
5. Avoid any new feature work.

Acceptance:

- No last-minute risky changes.
- The project has a defensible final phase narrative.
- The live collector proof is either still running intentionally or stopped cleanly with evidence preserved.

## Major Jobs Left And 24-Hour Decision

| Area | Current status | Can finish before 2 PM? | 24-hour action |
| --- | --- | --- | --- |
| Long organic runtime proof | Running now | Yes | Keep running, capture final proof pack |
| Probability-band filtering | Implemented in collector, Phase 3, evidence, and alerts | Yes | Audit live results and report filtered counts |
| Telegram delivery | Proven, but token rotation remains | Partly | Keep secrets out of git; rotate token if practical |
| Wallet entities/activity/clusters | Partially populated | Yes | Refresh materializers if safe |
| Wallet profiles/positions | Empty/deferred | Probably not fully | Write provider-disabled/deferred status or minimal report |
| Evidence benchmark | Not implemented | Partial only | Create small benchmark skeleton or mark as deferred with clear criteria |
| SSD archive reconciliation | Blocked if SSD invisible | Only if SSD visible | Re-run if mounted; otherwise preserve blocked status |
| Shadow ML expansion | Current evidence still limited | No | Run live review; keep ML shadow-only |
| Repo cleanup | Dirty with generated artifacts | Partly | Commit docs/source only; avoid committing bulk live noise |
| Presentation narrative | Needs final pack | Yes | Build final status doc and proof summary |

## Final Presentation Claim

The strongest honest claim is:

This project has reached a working local-first wallet-aware Polymarket intelligence runtime. It collects public market data, produces Phase 3 rule-based candidates, enriches them with Phase 4 evidence, delivers operator alerts with market traceability, and runs a Phase 6 LightGBM model in shadow mode for priority review. In the final 24-hour proof window, the collector was kept running to validate organic runtime behavior and the newly added 10%-95% YES probability-band filtering. The system is not an autonomous trading bot, does not identify real-world people, and does not let ML suppress or promote alerts.

## Remaining Work After Presentation

1. Promote probability-band audit into a permanent report.
2. Rotate Telegram token and document secret rotation.
3. Finish wallet profile/position provider story or explicitly remove it from v1 scope.
4. Build a 20-50 case evidence benchmark.
5. Re-run archive reconciliation when SSD is visible to the runtime shell.
6. Expand shadow ML with larger replay windows.
7. Add CI or smoke-test wrappers for the key runtime gates.
8. Clean generated report noise and preserve only compact proof artifacts.

## Final Recommendation

Keep the collector running until at least one strong proof checkpoint is captured. With the current disk level, do not let it run blindly all the way to presentation without monitoring. The practical target is to capture a strong proof pack by tomorrow morning, then either stop gracefully or keep it alive only for a controlled demo.
