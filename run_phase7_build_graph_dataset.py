from __future__ import annotations

import argparse
import json
from pathlib import Path

from config.settings import (
    PHASE7_GRAPH_FEATURE_SCHEMA_VERSION,
    PHASE7_GRAPH_LOOKBACK_DAYS,
    PHASE7_GRAPH_PERSISTENCE_MIN_DAYS,
)
from database.db_manager import apply_schema
from phase5.repository import Phase5Repository
from phase6 import Phase6Repository
from phase7 import build_graph_training_frame


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Build a Phase 7 graph and cluster-persistence feature dataset from replay-derived rows."
    )
    parser.add_argument("--start", required=True, help="UTC ISO8601 inclusive start timestamp.")
    parser.add_argument("--end", required=True, help="UTC ISO8601 exclusive end timestamp.")
    parser.add_argument(
        "--output-dir",
        default="reports/phase7/graph_datasets",
        help="Directory for CSV and JSON graph-feature artifacts.",
    )
    parser.add_argument(
        "--lookback-days",
        type=int,
        default=PHASE7_GRAPH_LOOKBACK_DAYS,
        help="Lookback window used to build the wallet-market graph.",
    )
    parser.add_argument(
        "--persistence-min-days",
        type=int,
        default=PHASE7_GRAPH_PERSISTENCE_MIN_DAYS,
        help="Minimum distinct active days required to count a wallet as persistent.",
    )
    parser.add_argument(
        "--feature-schema-version",
        default=PHASE7_GRAPH_FEATURE_SCHEMA_VERSION,
        help="Version tag written into the graph feature dataset.",
    )
    parser.add_argument("--json", action="store_true", help="Emit JSON summary to stdout.")
    return parser


def main() -> int:
    args = _build_parser().parse_args()
    apply_schema()
    repository = Phase5Repository()
    rows = repository.load_evaluation_rows(start=args.start, end=args.end)
    frame, summary, diagnostics = build_graph_training_frame(
        rows,
        repository=repository,
        feature_schema_version=args.feature_schema_version,
        lookback_days=max(1, args.lookback_days),
        persistence_min_days=max(1, args.persistence_min_days),
    )

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    csv_path = output_dir / f"{summary.dataset_hash}.csv"
    metadata_path = output_dir / f"{summary.dataset_hash}.json"
    frame.to_csv(csv_path, index=False)

    materialization_summary = Phase6Repository().record_materialization_run(
        feature_schema_version=args.feature_schema_version,
        materialization_mode="phase7_graph_features",
        start=args.start,
        end=args.end,
        source_row_count=len(rows),
        feature_row_count=len(frame),
        dataset_hash=summary.dataset_hash,
        output_path=str(csv_path),
        status="completed",
        notes=json.dumps(
            {
                "lookback_days": max(1, args.lookback_days),
                "persistence_min_days": max(1, args.persistence_min_days),
                "ready_for_controlled_experiments": summary.ready_for_controlled_experiments,
            },
            sort_keys=True,
        ),
    )

    metadata = {
        "summary": summary.to_dict(),
        "materialization_run": materialization_summary.to_dict(),
        "csv_path": str(csv_path),
        "row_columns": list(frame.columns),
        "diagnostics": diagnostics,
    }
    metadata_path.write_text(json.dumps(metadata, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    payload = {
        **summary.to_dict(),
        "csv_path": str(csv_path),
        "metadata_path": str(metadata_path),
        "materialization_run_id": materialization_summary.materialization_run_id,
    }
    if args.json:
        print(json.dumps(payload, indent=2, sort_keys=True))
    else:
        print(
            "Phase 7 graph feature dataset rows: "
            f"{summary.row_count} (labeled={summary.labeled_row_count}, stable_graph_features={summary.stable_graph_feature_count})"
        )
        print(f"Dataset hash: {summary.dataset_hash}")
        print(f"Ready for controlled experiments: {summary.ready_for_controlled_experiments}")
        print(f"CSV: {csv_path}")
        print(f"Metadata: {metadata_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
