from __future__ import annotations

import argparse
import asyncio
import json
from datetime import datetime, timedelta, timezone

from database.db_manager import apply_schema
from phase3.detector import Phase3Repository, run_phase3_detector_window
from phase3.state_store import create_state_store
from utils.logger import get_logger

log = get_logger("run_phase3_detector")


def _default_window() -> tuple[str, str]:
    end_dt = datetime.now(timezone.utc)
    start_dt = end_dt - timedelta(hours=1)
    return start_dt.isoformat(), end_dt.isoformat()


async def _main() -> None:
    parser = argparse.ArgumentParser(description="Phase 3 detector runner over detector-input windows.")
    parser.add_argument("--start", default=None, help="Inclusive UTC ISO-8601 start timestamp.")
    parser.add_argument("--end", default=None, help="Exclusive UTC ISO-8601 end timestamp.")
    parser.add_argument(
        "--source-system",
        action="append",
        default=None,
        help="Restrict processing to one detector-input source system. Repeatable.",
    )
    parser.add_argument(
        "--limit-envelopes",
        type=int,
        default=None,
        help="Optional cap on the number of ordered envelopes to process.",
    )
    args = parser.parse_args()

    default_start, default_end = _default_window()
    start = args.start or default_start
    end = args.end or default_end

    apply_schema()
    state_context = await create_state_store()
    repository = Phase3Repository()
    repository.register_detector_version(
        backend_name=state_context.backend_name,
        notes=state_context.notes,
    )

    try:
        summary = await run_phase3_detector_window(
            start=start,
            end=end,
            store=state_context.store,
            repository=repository,
            source_systems=args.source_system,
            limit_envelopes=args.limit_envelopes,
        )
    finally:
        await state_context.store.aclose()

    payload = {
        "start": start,
        "end": end,
        "state_backend": state_context.backend_name,
        "summary": summary.to_dict(),
    }
    log.info(f"Phase 3 detector summary: {json.dumps(payload, sort_keys=True)}")
    print(json.dumps(payload, indent=2, sort_keys=True))


if __name__ == "__main__":
    asyncio.run(_main())
