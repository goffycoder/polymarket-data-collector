from __future__ import annotations

import argparse
import asyncio
import json
import sqlite3

from config.settings import (
    ENABLE_PHASE4_DISCORD,
    ENABLE_PHASE4_RUNTIME,
    ENABLE_PHASE4_TELEGRAM,
    PHASE4_DISCORD_WEBHOOK_URL,
    PHASE4_RUNTIME_PENDING_LIMIT,
    PHASE4_RUNTIME_POLL_SECONDS,
    PHASE4_TELEGRAM_BOT_TOKEN,
    PHASE4_TELEGRAM_CHAT_ID,
)
from database.db_manager import apply_schema
from phase4 import Phase4AlertWorker, Phase4EvidenceWorker, Phase4Repository
from phase4.repository import is_sqlite_lock_error
from utils.logger import get_logger

log = get_logger("run_phase4_live")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run the Phase 4 evidence-alert pipeline in a polling loop."
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=PHASE4_RUNTIME_PENDING_LIMIT,
        help="Maximum number of pending candidates to process per pass.",
    )
    parser.add_argument(
        "--poll-seconds",
        type=int,
        default=PHASE4_RUNTIME_POLL_SECONDS,
        help="Seconds to wait between passes.",
    )
    parser.add_argument(
        "--iterations",
        type=int,
        default=0,
        help="Number of passes to run; 0 means forever.",
    )
    parser.add_argument("--json", action="store_true", help="Emit machine-readable JSON at exit.")
    return parser


async def run_pipeline_iteration(repository: Phase4Repository, *, limit: int) -> dict[str, object]:
    evidence_worker = Phase4EvidenceWorker(repository=repository)
    evidence_results = await evidence_worker.process_pending_candidates(limit=max(0, limit))

    alert_worker = Phase4AlertWorker(repository=repository)
    alert_results = alert_worker.process_pending_candidates(limit=max(0, limit))

    return {
        "evidence_results": evidence_results,
        "evidence_summary": evidence_worker.summary.to_dict(),
        "alert_results": alert_results,
        "alert_summary": alert_worker.summary.to_dict(),
    }


async def _main() -> int:
    args = build_parser().parse_args()
    apply_schema()

    if not ENABLE_PHASE4_RUNTIME:
        raise SystemExit("Phase 4 live runtime is disabled. Set POLYMARKET_ENABLE_PHASE4_RUNTIME=true.")

    repository = Phase4Repository()
    repository.register_workflow_version(
        notes="Phase 4 live polling workflow initialized from the canonical runtime launcher."
    )

    channels = []
    if ENABLE_PHASE4_TELEGRAM:
        if not PHASE4_TELEGRAM_BOT_TOKEN or not PHASE4_TELEGRAM_CHAT_ID:
            raise SystemExit(
                "Phase 4 Telegram delivery is enabled, but the Telegram token/chat id is missing. "
                "Use shell environment variables or .env.runtime.secrets."
            )
        channels.append("telegram")
    if ENABLE_PHASE4_DISCORD:
        if not PHASE4_DISCORD_WEBHOOK_URL:
            raise SystemExit(
                "Phase 4 Discord delivery is enabled, but the webhook URL is missing. "
                "Use shell environment variables or .env.runtime.secrets."
            )
        channels.append("discord")
    if not channels:
        log.warning("Phase 4 live runtime enabled with no outbound delivery channels configured.")

    iteration = 0
    summaries: list[dict[str, object]] = []
    while True:
        iteration += 1
        try:
            summary = await run_pipeline_iteration(repository, limit=args.limit)
        except sqlite3.OperationalError as exc:
            if not is_sqlite_lock_error(exc):
                raise
            summary = {
                "status": "skipped_database_locked",
                "error": str(exc),
                "evidence_results": [],
                "alert_results": [],
            }
            log.warning(
                "Phase 4 live iteration skipped because SQLite was locked; "
                "the next polling pass will retry."
            )
        summary["iteration"] = iteration
        summaries.append(summary)
        log.info(f"Phase 4 live summary: {json.dumps(summary, sort_keys=True, default=str)}")

        if args.iterations and iteration >= args.iterations:
            break
        await asyncio.sleep(max(1, args.poll_seconds))

    payload = {
        "iteration_count": len(summaries),
        "channels": channels,
        "summaries": summaries,
    }
    if args.json:
        print(json.dumps(payload, indent=2, sort_keys=True, default=str))
    else:
        latest = summaries[-1] if summaries else {}
        print(f"Iterations: {len(summaries)}")
        print(f"Channels: {', '.join(channels) if channels else 'none'}")
        print(f"Latest evidence processed: {len(latest.get('evidence_results', []))}")
        print(f"Latest alerts processed: {len(latest.get('alert_results', []))}")
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(_main()))
