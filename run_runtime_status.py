from __future__ import annotations

import argparse
import json
import os

from config.runtime_env import load_runtime_env


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Show the configured and observed local runtime mode for collector, detector, alerts, and shadow ML."
    )
    parser.add_argument(
        "--env-file",
        default="",
        help="Explicit runtime env file. Defaults to .env.runtime, then legacy .env, then shell-only.",
    )
    parser.add_argument(
        "--recent-hours",
        type=int,
        default=24,
        help="How many recent hours to inspect when classifying observed live activity.",
    )
    args = parser.parse_args()

    env_result = load_runtime_env(args.env_file or None, override=True)
    os.environ["POLYMARKET_RUNTIME_LAUNCHED"] = "1"

    from config import settings
    from config.runtime_mode import build_runtime_decision_summary
    from database.db_manager import apply_schema
    from phase3.detector import Phase3Repository
    from phase4 import Phase4Repository
    from phase6 import Phase6Repository

    apply_schema()
    phase3_status = Phase3Repository().live_runtime_status(recent_hours=args.recent_hours)
    phase4_repository = Phase4Repository()
    phase4_status = phase4_repository.live_runtime_status(recent_hours=args.recent_hours)
    phase6_repository = Phase6Repository()
    phase6_status = phase6_repository.live_runtime_status(recent_hours=args.recent_hours)

    payload = {
        "env_loading": {
            "primary_env_file": None if env_result.env_file is None else str(env_result.env_file),
            "primary_env_source": env_result.source,
            "secret_env_file": None if env_result.secret_env_file is None else str(env_result.secret_env_file),
            "secret_env_source": env_result.secret_source,
            "warnings": list(env_result.warnings),
            "secret_keys_in_primary_env": list(env_result.secret_keys_in_primary_env),
            "secret_keys_in_secret_env": list(env_result.secret_keys_in_secret_env),
        },
        "runtime_decision": build_runtime_decision_summary(
            settings=settings,
            phase3_status=phase3_status,
            phase4_status=phase4_status,
            phase6_status=phase6_status,
        ),
        "archive_loss_truth": {
            "status": "partially_degraded_locally",
            "recorded_on": "2026-05-13",
            "summary": (
                "The 2026-04 raw and detector-input archive tree was previously deleted under disk pressure, "
                "so some older windows are no longer locally restorable from archives alone."
            ),
        },
        "phase3_runtime_status": phase3_status,
        "phase4_runtime_status": phase4_status,
        "phase4_workflow_registration": phase4_repository.load_workflow_registration(),
        "phase6_runtime_status": phase6_status,
        "phase6_registry_status": phase6_repository.build_registry_status(limit=10).to_dict(),
    }
    print(json.dumps(payload, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
