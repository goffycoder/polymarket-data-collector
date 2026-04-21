from __future__ import annotations

import argparse
import json

from database.db_manager import apply_schema
from phase4 import Phase4Repository
from utils.logger import get_logger

log = get_logger("run_phase4_bootstrap")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Bootstrap the Phase 4 evidence/alerts layer and summarize pending work."
    )
    parser.add_argument(
        "--pending-limit",
        type=int,
        default=10,
        help="Number of pending Phase 3 candidates to preview.",
    )
    args = parser.parse_args()

    apply_schema()
    repository = Phase4Repository()
    repository.register_workflow_version(
        notes="Phase 4 bootstrap initialized from the latest Phase 3 candidate engine."
    )

    summary = repository.bootstrap_summary()
    pending_candidates = repository.pending_candidates(limit=max(0, args.pending_limit))
    payload = {
        "summary": summary.to_dict(),
        "pending_candidates": pending_candidates,
    }
    log.info(f"Phase 4 bootstrap summary: {json.dumps(payload, sort_keys=True)}")
    print(json.dumps(payload, indent=2, sort_keys=True, default=str))


if __name__ == "__main__":
    main()
