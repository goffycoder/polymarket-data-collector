from __future__ import annotations

import argparse
import asyncio
import json

from config.settings import PHASE3_POLL_SECONDS
from database.db_manager import apply_schema
from phase3.detector import DEFAULT_PHASE3_SOURCE_SYSTEMS, Phase3Repository
from phase3.live_runner import Phase3LiveRunner
from phase3.state_store import create_state_store
from utils.logger import get_logger

log = get_logger("run_phase3_live")


async def _main() -> None:
    parser = argparse.ArgumentParser(description="Run the live Phase 3 detector worker.")
    parser.add_argument(
        "--source-system",
        action="append",
        default=None,
        help="Restrict processing to one detector-input source system. Repeatable.",
    )
    parser.add_argument(
        "--poll-seconds",
        type=float,
        default=PHASE3_POLL_SECONDS,
        help="Polling interval for tailing detector-input partitions.",
    )
    parser.add_argument(
        "--once",
        action="store_true",
        help="Process currently available envelopes once and exit.",
    )
    args = parser.parse_args()

    apply_schema()
    state_context = await create_state_store()
    repository = Phase3Repository()
    repository.register_detector_version(
        backend_name=state_context.backend_name,
        notes=state_context.notes,
    )

    runner = Phase3LiveRunner(
        store=state_context.store,
        repository=repository,
        source_systems=args.source_system or list(DEFAULT_PHASE3_SOURCE_SYSTEMS),
        poll_seconds=args.poll_seconds,
    )

    try:
        if args.once:
            processed = await runner.run_once()
            payload = {
                "mode": "once",
                "processed_envelopes": processed,
                "runner_summary": runner.summary.to_dict(),
                "detector_summary": runner.detector.summary.to_dict(),
                "state_backend": state_context.backend_name,
            }
            log.info(f"Phase 3 live-once summary: {json.dumps(payload, sort_keys=True)}")
            print(json.dumps(payload, indent=2, sort_keys=True))
            return

        log.info(
            "Starting Phase 3 live worker "
            f"(backend={state_context.backend_name}, sources={args.source_system or DEFAULT_PHASE3_SOURCE_SYSTEMS})"
        )
        await runner.run_forever()
    finally:
        await state_context.store.aclose()


if __name__ == "__main__":
    asyncio.run(_main())
