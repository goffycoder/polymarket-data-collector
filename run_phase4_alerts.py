from __future__ import annotations

import argparse
import json

from database.db_manager import apply_schema
from phase4 import Phase4AlertWorker, Phase4Repository
from utils.logger import get_logger

log = get_logger("run_phase4_alerts")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run the Phase 4 alert worker for pending Phase 3 candidates."
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
        notes="Phase 4 alert worker initialized from the latest Phase 3 candidate engine."
    )

    worker = Phase4AlertWorker(repository=repository)
    processed = worker.process_pending_candidates(limit=max(0, args.limit))
    payload = {
        "processed_alerts": processed,
        "summary": worker.summary.to_dict(),
    }
    log.info(f"Phase 4 alert run summary: {json.dumps(payload, sort_keys=True)}")
    print(json.dumps(payload, indent=2, sort_keys=True, default=str))


if __name__ == "__main__":
    main()
