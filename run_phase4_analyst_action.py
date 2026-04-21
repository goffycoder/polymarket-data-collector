from __future__ import annotations

import argparse
import json

from database.db_manager import apply_schema
from phase4 import Phase4AnalystWorkflow, Phase4Repository
from utils.logger import get_logger

log = get_logger("run_phase4_analyst_action")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Record one Phase 4 analyst action for an existing alert."
    )
    parser.add_argument("--alert-id", required=True, help="Alert identifier to annotate.")
    parser.add_argument(
        "--action",
        required=True,
        help="Analyst action: acknowledge, snooze, dismiss, mark_useful, mark_false_positive, add_notes.",
    )
    parser.add_argument("--actor", default=None, help="Actor name or handle.")
    parser.add_argument("--notes", default=None, help="Optional free-form notes.")
    parser.add_argument("--follow-up-at", default=None, help="Optional ISO timestamp for follow-up.")
    args = parser.parse_args()

    apply_schema()
    workflow = Phase4AnalystWorkflow(repository=Phase4Repository())
    payload = workflow.record_action(
        alert_id=args.alert_id,
        action_type=args.action,
        actor=args.actor,
        notes=args.notes,
        follow_up_at=args.follow_up_at,
    )
    log.info(f"Phase 4 analyst action summary: {json.dumps(payload, sort_keys=True)}")
    print(json.dumps(payload, indent=2, sort_keys=True, default=str))


if __name__ == "__main__":
    main()
