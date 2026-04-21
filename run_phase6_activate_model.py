from __future__ import annotations

import argparse
import json

from database.db_manager import apply_schema
from phase6 import Phase6Repository


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Activate or retire a Phase 6 Person 1 model in the registry.")
    parser.add_argument("--model-version", required=True, help="Registered model version.")
    parser.add_argument(
        "--action",
        default="activate",
        choices=("activate", "retire"),
        help="Registry action to perform.",
    )
    parser.add_argument("--keep-existing", action="store_true", help="Do not retire other active shadow models.")
    parser.add_argument("--notes", default="", help="Optional operator notes.")
    parser.add_argument("--json", action="store_true", help="Emit machine-readable JSON.")
    return parser


def main() -> int:
    args = build_parser().parse_args()
    apply_schema()
    repo = Phase6Repository()
    if args.action == "retire":
        payload = repo.retire_model(
            model_version=args.model_version,
            notes=args.notes or None,
        )
    else:
        payload = repo.activate_shadow_model(
            model_version=args.model_version,
            retire_previous=not args.keep_existing,
            notes=args.notes or None,
        )
    if args.json:
        print(json.dumps(payload, indent=2, sort_keys=True))
    else:
        print(f"Model version: {payload['model_version']}")
        print(f"Deployment status: {payload['deployment_status']}")
        print(f"Shadow enabled: {payload['shadow_enabled']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
