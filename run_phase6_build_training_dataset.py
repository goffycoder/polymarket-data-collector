from __future__ import annotations

import argparse
import json
from pathlib import Path

from phase5.repository import Phase5Repository
from phase6 import build_training_frame


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Build a Phase 6 Person 2 replay-derived training dataset artifact.")
    parser.add_argument("--start", required=True, help="UTC ISO8601 inclusive start timestamp.")
    parser.add_argument("--end", required=True, help="UTC ISO8601 exclusive end timestamp.")
    parser.add_argument(
        "--output-dir",
        default="reports/phase6/training_datasets",
        help="Directory for CSV/JSON training dataset artifacts.",
    )
    parser.add_argument("--json", action="store_true", help="Emit JSON summary to stdout.")
    return parser


def main() -> int:
    args = _build_parser().parse_args()
    repository = Phase5Repository()
    rows = repository.load_evaluation_rows(start=args.start, end=args.end)
    frame, summary = build_training_frame(rows, repository=repository)

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    csv_path = output_dir / f"{summary.dataset_hash}.csv"
    metadata_path = output_dir / f"{summary.dataset_hash}.json"
    frame.to_csv(csv_path, index=False)
    metadata = {
        "summary": summary.to_dict(),
        "csv_path": str(csv_path),
        "row_columns": list(frame.columns),
    }
    metadata_path.write_text(json.dumps(metadata, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    payload = {
        **summary.to_dict(),
        "csv_path": str(csv_path),
        "metadata_path": str(metadata_path),
    }
    if args.json:
        print(json.dumps(payload, indent=2, sort_keys=True))
    else:
        print(f"Phase 6 training dataset rows: {summary.row_count} (labeled={summary.labeled_row_count})")
        print(f"Dataset hash: {summary.dataset_hash}")
        print(f"CSV: {csv_path}")
        print(f"Metadata: {metadata_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
