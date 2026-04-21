from __future__ import annotations

import argparse
import json
from pathlib import Path

from database.db_manager import apply_schema
from phase6 import Phase6Repository


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Prepare a Phase 6 Person 2 handoff bundle for Person 1 registry/shadow deployment.")
    parser.add_argument("--model-version", required=True, help="Model version to package.")
    parser.add_argument(
        "--output-dir",
        default="reports/phase6/handoffs",
        help="Directory for handoff bundle JSON.",
    )
    parser.add_argument("--json", action="store_true", help="Emit machine-readable JSON.")
    return parser


def main() -> int:
    args = build_parser().parse_args()
    apply_schema()
    repo = Phase6Repository()
    evaluation_runs = repo.list_recent_evaluation_runs(limit=200)
    matching = next((item for item in evaluation_runs if item["model_version"] == args.model_version), None)
    if matching is None:
        raise SystemExit(f"No evaluation run found for model version: {args.model_version}")

    calibration_profiles = repo.list_calibration_profiles(model_version=args.model_version, limit=200)
    global_profile = next(
        (
            item
            for item in calibration_profiles
            if item["profile_scope"] == "global" and item["profile_key"] == "global"
        ),
        None,
    )
    summary_json = matching.get("summary_json") or {}
    artifacts = summary_json.get("artifacts") or {}
    bundle = {
        "model_version": args.model_version,
        "feature_schema_version": matching["feature_schema_version"],
        "training_dataset_hash": matching["dataset_hash"],
        "model_artifact_path": artifacts.get("model_path"),
        "evaluation_report_path": matching.get("output_path"),
        "scored_csv_path": artifacts.get("scored_csv_path"),
        "recommended_calibration_metadata": {
            "global_profile": global_profile,
            "profile_count": len(calibration_profiles),
        },
        "registration_command": {
            "model_version": args.model_version,
            "artifact_path": artifacts.get("model_path"),
            "training_dataset_hash": matching["dataset_hash"],
        },
    }

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / f"{args.model_version}_handoff.json"
    output_path.write_text(json.dumps(bundle, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    payload = {
        "output_path": str(output_path),
        "bundle": bundle,
    }
    if args.json:
        print(json.dumps(payload, indent=2, sort_keys=True))
    else:
        print(f"Handoff bundle: {output_path}")
        print(f"Model version: {args.model_version}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
