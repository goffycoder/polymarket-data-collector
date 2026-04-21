from __future__ import annotations

import argparse
import json
from pathlib import Path

from config.settings import PHASE6_FEATURE_SCHEMA_VERSION
from database.db_manager import apply_schema
from ml_pipeline.feature_builder import build_features, dataset_hash_from_frame
from phase5.repository import Phase5Repository
from phase6 import Phase6Repository


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Materialize Phase 6 Person 1 training or inference features from replayable evaluation rows."
    )
    parser.add_argument("--start", required=True, help="Window start timestamp (ISO 8601).")
    parser.add_argument("--end", required=True, help="Window end timestamp (ISO 8601).")
    parser.add_argument(
        "--mode",
        default="training",
        choices=("training", "inference"),
        help="Materialization mode.",
    )
    parser.add_argument(
        "--output-dir",
        default="reports/phase6/features",
        help="Relative output directory for feature artifacts.",
    )
    parser.add_argument("--json", action="store_true", help="Emit machine-readable JSON.")
    return parser


def _render_text(payload: dict) -> str:
    return "\n".join(
        [
            f"Materialization run id: {payload['materialization_run_id']}",
            f"Mode: {payload['materialization_mode']}",
            f"Feature schema: {payload['feature_schema_version']}",
            f"Window: {payload['start']} -> {payload['end']}",
            f"Source rows: {payload['source_row_count']}",
            f"Feature rows: {payload['feature_row_count']}",
            f"Dataset hash: {payload['dataset_hash']}",
            f"Artifact path: {payload['output_path']}",
            f"Status: {payload['status']}",
        ]
    )


def main() -> int:
    args = build_parser().parse_args()
    apply_schema()

    evaluation_rows = Phase5Repository().load_evaluation_rows(start=args.start, end=args.end)
    frame = build_features(
        evaluation_rows,
        feature_schema_version=PHASE6_FEATURE_SCHEMA_VERSION,
    )
    dataset_hash = dataset_hash_from_frame(frame)

    artifact_dir = Path(args.output_dir)
    artifact_dir.mkdir(parents=True, exist_ok=True)
    artifact_path = artifact_dir / f"{dataset_hash}.jsonl"
    with artifact_path.open("w", encoding="utf-8") as handle:
        for record in frame.to_dict(orient="records"):
            handle.write(json.dumps(record, sort_keys=True) + "\n")

    summary = Phase6Repository().record_materialization_run(
        feature_schema_version=PHASE6_FEATURE_SCHEMA_VERSION,
        materialization_mode=args.mode,
        start=args.start,
        end=args.end,
        source_row_count=len(evaluation_rows),
        feature_row_count=len(frame.index),
        dataset_hash=dataset_hash,
        output_path=str(artifact_path),
        status="completed",
        notes="Phase 6 Person 1 feature materialization foundation run.",
    )
    payload = summary.to_dict()
    if args.json:
        print(json.dumps(payload, indent=2, sort_keys=True))
    else:
        print(_render_text(payload))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
