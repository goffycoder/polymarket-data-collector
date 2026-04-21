from __future__ import annotations

import argparse
import json
from pathlib import Path

from config.settings import ENABLE_PHASE6_SHADOW_MODE, PHASE6_FEATURE_SCHEMA_VERSION
from database.db_manager import apply_schema
from ml_pipeline.feature_builder import build_features
from phase5.repository import Phase5Repository
from phase6 import Phase6Repository, build_shadow_scores, load_model_spec


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run Phase 6 Person 1 shadow scoring for one historical window using a registered model artifact."
    )
    parser.add_argument("--start", required=True, help="Window start timestamp (ISO 8601).")
    parser.add_argument("--end", required=True, help="Window end timestamp (ISO 8601).")
    parser.add_argument("--model-version", default="", help="Optional model version override.")
    parser.add_argument(
        "--output-dir",
        default="reports/phase6/shadow_scores",
        help="Relative output directory for shadow score artifacts.",
    )
    parser.add_argument("--json", action="store_true", help="Emit machine-readable JSON.")
    return parser


def _render_text(payload: dict) -> str:
    return "\n".join(
        [
            f"Model version: {payload['model_version']}",
            f"Feature schema: {payload['feature_schema_version']}",
            f"Score count: {payload['score_count']}",
            f"Artifact path: {payload['output_path']}",
        ]
    )


def main() -> int:
    args = build_parser().parse_args()
    apply_schema()
    repo = Phase6Repository()
    model_entry = (
        repo.load_model_registry_entry(model_version=args.model_version)
        if args.model_version
        else repo.load_active_shadow_model()
    )
    if model_entry is None:
        raise SystemExit("No registered shadow-capable model found. Register a model first.")
    if not ENABLE_PHASE6_SHADOW_MODE:
        raise SystemExit("Phase 6 shadow mode is disabled. Set POLYMARKET_ENABLE_PHASE6_SHADOW_MODE=true.")

    evaluation_rows = Phase5Repository().load_evaluation_rows(start=args.start, end=args.end)
    feature_frame = build_features(
        evaluation_rows,
        feature_schema_version=PHASE6_FEATURE_SCHEMA_VERSION,
    )
    model_spec = load_model_spec(str(model_entry["artifact_path"]))
    score_rows = build_shadow_scores(feature_frame, model_spec=model_spec)

    artifact_dir = Path(args.output_dir)
    artifact_dir.mkdir(parents=True, exist_ok=True)
    artifact_path = artifact_dir / f"{model_entry['model_version']}_{args.start}_{args.end}".replace(":", "-")
    artifact_path = artifact_path.with_suffix(".json")
    artifact_payload = {
        "model_version": model_entry["model_version"],
        "feature_schema_version": PHASE6_FEATURE_SCHEMA_VERSION,
        "window": {"start": args.start, "end": args.end},
        "score_rows": score_rows,
    }
    artifact_path.write_text(json.dumps(artifact_payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    summaries = []
    for row in score_rows:
        summaries.append(
            repo.log_shadow_score(
                model_version=str(model_entry["model_version"]),
                feature_schema_version=PHASE6_FEATURE_SCHEMA_VERSION,
                candidate_id=str(row["candidate_id"]),
                alert_id=row.get("alert_id"),
                market_id=str(row["market_id"]),
                score_value=float(row["score_value"]),
                score_label=row.get("score_label"),
                score_metadata=row.get("score_metadata"),
                scored_at=str(row["decision_timestamp"]),
            ).to_dict()
        )

    payload = {
        "model_version": model_entry["model_version"],
        "feature_schema_version": PHASE6_FEATURE_SCHEMA_VERSION,
        "score_count": len(summaries),
        "output_path": str(artifact_path),
        "scores_preview": summaries[:5],
    }
    if args.json:
        print(json.dumps(payload, indent=2, sort_keys=True))
    else:
        print(_render_text(payload))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
