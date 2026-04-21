from __future__ import annotations

import argparse
import json

from config.settings import PHASE6_DEFAULT_MODEL_NAME, PHASE6_FEATURE_SCHEMA_VERSION
from database.db_manager import apply_schema
from phase6 import Phase6Repository


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Register one Phase 6 Person 1 model artifact in the local model registry."
    )
    parser.add_argument("--model-version", required=True, help="Unique model version id.")
    parser.add_argument(
        "--model-name",
        default=PHASE6_DEFAULT_MODEL_NAME,
        help="Logical model family name.",
    )
    parser.add_argument("--artifact-path", required=True, help="Relative or absolute path to the model artifact.")
    parser.add_argument("--training-dataset-hash", required=True, help="Dataset hash used for training.")
    parser.add_argument(
        "--deployment-status",
        default="registered",
        choices=("registered", "shadow", "deployed", "retired"),
        help="Registry deployment status.",
    )
    parser.add_argument("--shadow-enabled", action="store_true", help="Mark the artifact as shadow enabled.")
    parser.add_argument(
        "--calibration-metadata",
        default="{}",
        help="JSON object describing calibration metadata.",
    )
    parser.add_argument("--notes", default="", help="Optional operator notes.")
    parser.add_argument("--json", action="store_true", help="Emit machine-readable JSON.")
    return parser


def _render_text(payload: dict) -> str:
    return "\n".join(
        [
            f"Model version: {payload['model_version']}",
            f"Model name: {payload['model_name']}",
            f"Artifact path: {payload['artifact_path']}",
            f"Feature schema: {payload['feature_schema_version']}",
            f"Dataset hash: {payload['training_dataset_hash']}",
            f"Deployment status: {payload['deployment_status']}",
            f"Shadow enabled: {payload['shadow_enabled']}",
        ]
    )


def main() -> int:
    args = build_parser().parse_args()
    apply_schema()
    calibration_metadata = json.loads(args.calibration_metadata)
    summary = Phase6Repository().register_model(
        model_name=args.model_name,
        model_version=args.model_version,
        artifact_path=args.artifact_path,
        feature_schema_version=PHASE6_FEATURE_SCHEMA_VERSION,
        training_dataset_hash=args.training_dataset_hash,
        calibration_metadata=calibration_metadata,
        deployment_status=args.deployment_status,
        shadow_enabled=args.shadow_enabled,
        notes=args.notes or None,
    )
    payload = summary.to_dict()
    if args.json:
        print(json.dumps(payload, indent=2, sort_keys=True))
    else:
        print(_render_text(payload))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
