from __future__ import annotations

import argparse
import asyncio
import json

from database.db_manager import apply_schema
from phase4 import Phase4AlertWorker, Phase4EvidenceWorker, Phase4Repository
from utils.logger import get_logger

log = get_logger("run_phase4_pipeline")


async def _main() -> None:
    parser = argparse.ArgumentParser(
        description="Run the Phase 4 end-to-end pipeline: evidence first, then alerts."
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
        notes="Phase 4 end-to-end pipeline initialized from the latest Phase 3 candidate engine."
    )

    evidence_worker = Phase4EvidenceWorker(repository=repository)
    evidence_results = await evidence_worker.process_pending_candidates(limit=max(0, args.limit))

    alert_worker = Phase4AlertWorker(repository=repository)
    alert_results = alert_worker.process_pending_candidates(limit=max(0, args.limit))

    payload = {
        "evidence_results": evidence_results,
        "evidence_summary": evidence_worker.summary.to_dict(),
        "alert_results": alert_results,
        "alert_summary": alert_worker.summary.to_dict(),
    }
    log.info(f"Phase 4 pipeline summary: {json.dumps(payload, sort_keys=True)}")
    print(json.dumps(payload, indent=2, sort_keys=True, default=str))


if __name__ == "__main__":
    asyncio.run(_main())
