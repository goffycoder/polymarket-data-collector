from __future__ import annotations

import argparse
import json

from database.db_manager import apply_schema
from phase6 import Phase6Repository


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Show Phase 6 Person 1 registry and shadow-score status.")
    parser.add_argument("--limit", type=int, default=10, help="Recent rows to display.")
    parser.add_argument("--json", action="store_true", help="Emit machine-readable JSON.")
    return parser


def main() -> int:
    args = build_parser().parse_args()
    apply_schema()
    payload = Phase6Repository().build_registry_status(limit=max(1, args.limit)).to_dict()
    if args.json:
        print(json.dumps(payload, indent=2, sort_keys=True))
    else:
        active = payload.get("active_shadow_model") or {}
        print(f"Active shadow model: {active.get('model_version', 'none')}")
        print(f"Recent models: {len(payload.get('recent_models', []))}")
        print(f"Recent shadow scores: {len(payload.get('recent_shadow_scores', []))}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
