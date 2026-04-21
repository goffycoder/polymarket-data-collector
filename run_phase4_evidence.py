from __future__ import annotations

import argparse
import asyncio
import json

from database.db_manager import apply_schema
from phase4 import Phase4EvidenceWorker, Phase4Repository
from utils.logger import get_logger

log = get_logger("run_phase4_evidence")


async def _main() -> None:
    parser = argparse.ArgumentParser(
        description="Run the Phase 4 evidence worker for pending Phase 3 candidates."
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=10,
        help="Maximum number of pending candidates to process.",
    )
    args = parser.parse_args()

    apply_schema()
    repository = Phase4Repository()
    repository.register_workflow_version(
        notes="Phase 4 evidence worker initialized from the latest Phase 3 candidate engine."
    )

    worker = Phase4EvidenceWorker(repository=repository)
    processed = await worker.process_pending_candidates(limit=max(0, args.limit))
    payload = {
        "processed_candidates": processed,
        "summary": worker.summary.to_dict(),
    }
    log.info(f"Phase 4 evidence run summary: {json.dumps(payload, sort_keys=True)}")
    print(json.dumps(payload, indent=2, sort_keys=True, default=str))


if __name__ == "__main__":
    asyncio.run(_main())
