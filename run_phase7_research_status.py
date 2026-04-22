from __future__ import annotations

import argparse
import json

from database.db_manager import apply_schema
from phase7 import Phase7Repository


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Show the Phase 7 Person 2 research index and experiment-ledger status."
    )
    parser.add_argument("--limit", type=int, default=20, help="Maximum rows to display per section.")
    parser.add_argument("--json", action="store_true", help="Emit machine-readable JSON.")
    return parser


def _render_text(payload: dict) -> str:
    latest = payload["latest_dataset"] or {}
    return "\n".join(
        [
            f"Latest dataset key: {latest.get('dataset_key', 'none')}",
            f"Latest dataset hash: {latest.get('dataset_hash', 'none')}",
            f"Latest manifest hash: {latest.get('manifest_hash', 'none')}",
            f"Latest restore guarantee: {latest.get('restore_guarantee_status', 'none')}",
            f"Frozen windows: {len(payload['latest_dataset_windows'])}",
            f"Frozen scopes: {len(payload['latest_dataset_scopes'])}",
            f"Recent datasets: {len(payload['recent_datasets'])}",
            f"Recent experiments: {len(payload['recent_experiments'])}",
            f"Traceable experiments: {payload['traceable_experiment_count']}",
        ]
    )


def main() -> int:
    args = build_parser().parse_args()
    apply_schema()
    payload = Phase7Repository().build_reproducibility_status(limit=max(1, args.limit)).to_dict()
    if args.json:
        print(json.dumps(payload, indent=2, sort_keys=True))
    else:
        print(_render_text(payload))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
