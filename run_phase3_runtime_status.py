from __future__ import annotations

import argparse
import json

from database.db_manager import apply_schema
from phase3.detector import Phase3Repository


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Inspect persisted Phase 3 detector registration, checkpoints, and candidate activity."
    )
    parser.add_argument(
        "--recent-hours",
        type=int,
        default=24,
        help="How many recent hours to include in the recent candidate count.",
    )
    args = parser.parse_args()

    apply_schema()
    repository = Phase3Repository()
    payload = {
        "detector_registration": repository.load_detector_registration(),
        "runtime_status": repository.live_runtime_status(recent_hours=args.recent_hours),
    }
    print(json.dumps(payload, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
