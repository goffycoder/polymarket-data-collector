from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from config.settings import (
    PHASE6_FEATURE_SCHEMA_VERSION,
    PHASE7_DEFAULT_DATASET_ROLE,
    PHASE7_LABEL_SCHEMA_VERSION,
)
from database.db_manager import apply_schema
from phase7 import Phase7Repository


REPO_ROOT = Path(__file__).resolve().parent


def _load_json(path: str) -> dict[str, Any]:
    return json.loads(Path(path).read_text(encoding="utf-8-sig"))


def _load_inline_json_items(values: list[str] | None) -> list[dict[str, Any]]:
    return [json.loads(value) for value in values or []]


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Register the Phase 7 Person 2 research dataset index, window index, and scope index."
    )
    parser.add_argument(
        "--manifest-path",
        default="",
        help="Optional JSON manifest from Person 1 with dataset, windows, and scopes.",
    )
    parser.add_argument("--dataset-key", default="", help="Stable research dataset key.")
    parser.add_argument("--dataset-hash", default="", help="Stable dataset hash from Person 1.")
    parser.add_argument(
        "--dataset-role",
        default=PHASE7_DEFAULT_DATASET_ROLE,
        help="Logical role for the research dataset.",
    )
    parser.add_argument(
        "--feature-schema-version",
        default=PHASE6_FEATURE_SCHEMA_VERSION,
        help="Feature schema version attached to the research dataset.",
    )
    parser.add_argument(
        "--label-schema-version",
        default=PHASE7_LABEL_SCHEMA_VERSION,
        help="Label schema version attached to the research dataset.",
    )
    parser.add_argument("--dataset-path", default="", help="Optional relative or absolute dataset artifact path.")
    parser.add_argument("--dataset-created-at", default="", help="Optional dataset creation timestamp.")
    parser.add_argument("--handoff-source", default="person1", help="Who handed off the research dataset.")
    parser.add_argument("--handoff-artifact-path", default="", help="Optional handoff bundle path.")
    parser.add_argument(
        "--restore-guarantee-json",
        default="",
        help="Optional JSON object describing restore guarantees if no manifest file is used.",
    )
    parser.add_argument(
        "--baseline-model-version",
        action="append",
        default=[],
        help="Repeatable Phase 6 baseline model version.",
    )
    parser.add_argument(
        "--category",
        action="append",
        default=[],
        help="Repeatable category to freeze as a research scope.",
    )
    parser.add_argument(
        "--evaluation-scope",
        action="append",
        default=[],
        help="Repeatable evaluation scope to freeze as a research scope.",
    )
    parser.add_argument(
        "--scope-json",
        action="append",
        default=[],
        help="Repeatable inline JSON object for one scope definition.",
    )
    parser.add_argument(
        "--window-json",
        action="append",
        default=[],
        help="Repeatable inline JSON object for one frozen research window.",
    )
    parser.add_argument("--notes", default="", help="Optional operator notes.")
    parser.add_argument(
        "--output-dir",
        default="reports/phase7/research_setup",
        help="Directory for the registered research setup artifact.",
    )
    parser.add_argument("--json", action="store_true", help="Emit machine-readable JSON.")
    return parser


def _build_payload_from_args(args: argparse.Namespace) -> dict[str, Any]:
    if args.manifest_path:
        payload = _load_json(args.manifest_path)
        if "dataset" in payload:
            return payload
        return {
            "dataset": {
                key: value
                for key, value in payload.items()
                if key not in {"windows", "scopes"}
            },
            "windows": payload.get("windows") or [],
            "scopes": payload.get("scopes") or [],
        }

    if not args.dataset_key or not args.dataset_hash:
        raise SystemExit("Either --manifest-path or both --dataset-key and --dataset-hash are required.")

    scopes = _load_inline_json_items(args.scope_json)
    scopes.extend(
        {
            "scope_type": "category",
            "scope_key": category,
            "scope_label": category,
            "scope_definition": {"category": category},
        }
        for category in args.category
    )
    scopes.extend(
        {
            "scope_type": "evaluation",
            "scope_key": scope,
            "scope_label": scope,
            "scope_definition": {"evaluation_scope": scope},
        }
        for scope in args.evaluation_scope
    )
    restore_payload = json.loads(args.restore_guarantee_json) if args.restore_guarantee_json else {}
    return {
        "dataset": {
            "dataset_key": args.dataset_key,
            "dataset_hash": args.dataset_hash,
            "dataset_role": args.dataset_role,
            "feature_schema_version": args.feature_schema_version,
            "label_schema_version": args.label_schema_version,
            "dataset_path": args.dataset_path or None,
            "dataset_created_at": args.dataset_created_at or None,
            "handoff_source": args.handoff_source or None,
            "handoff_artifact_path": args.handoff_artifact_path or None,
            "restore_guarantee": restore_payload,
            "baseline_model_versions": args.baseline_model_version,
            "notes": args.notes or None,
        },
        "windows": _load_inline_json_items(args.window_json),
        "scopes": scopes,
    }


def _render_text(payload: dict[str, Any]) -> str:
    summary = payload["summary"]
    return "\n".join(
        [
            f"Dataset key: {summary['dataset_key']}",
            f"Dataset hash: {summary['dataset_hash']}",
            f"Manifest hash: {summary['manifest_hash']}",
            f"Feature schema: {summary['feature_schema_version']}",
            f"Label schema: {summary['label_schema_version']}",
            f"Restore guarantee: {summary['restore_guarantee_status']}",
            f"Frozen windows: {summary['window_count']}",
            f"Frozen scopes: {summary['scope_count']}",
            f"Artifact path: {payload['artifact_path']}",
        ]
    )


def main() -> int:
    args = build_parser().parse_args()
    apply_schema()

    payload = _build_payload_from_args(args)
    dataset = payload.get("dataset") or {}
    windows = payload.get("windows") or []
    scopes = payload.get("scopes") or []

    summary = Phase7Repository().register_research_setup(
        dataset_key=str(dataset.get("dataset_key") or "").strip(),
        dataset_hash=str(dataset.get("dataset_hash") or "").strip(),
        dataset_role=str(dataset.get("dataset_role") or PHASE7_DEFAULT_DATASET_ROLE).strip(),
        feature_schema_version=str(
            dataset.get("feature_schema_version") or PHASE6_FEATURE_SCHEMA_VERSION
        ).strip(),
        label_schema_version=str(
            dataset.get("label_schema_version") or PHASE7_LABEL_SCHEMA_VERSION
        ).strip(),
        dataset_path=(str(dataset.get("dataset_path")).strip() if dataset.get("dataset_path") else None),
        dataset_created_at=(
            str(dataset.get("dataset_created_at")).strip() if dataset.get("dataset_created_at") else None
        ),
        handoff_source=(str(dataset.get("handoff_source")).strip() if dataset.get("handoff_source") else None),
        handoff_artifact_path=(
            str(dataset.get("handoff_artifact_path")).strip()
            if dataset.get("handoff_artifact_path")
            else None
        ),
        restore_guarantee=(dataset.get("restore_guarantee") or {}),
        baseline_model_versions=list(dataset.get("baseline_model_versions") or []),
        windows=windows,
        scopes=scopes,
        notes=(str(dataset.get("notes")).strip() if dataset.get("notes") else None),
    )

    artifact_dir = REPO_ROOT / args.output_dir
    artifact_dir.mkdir(parents=True, exist_ok=True)
    artifact_path = artifact_dir / f"{summary.dataset_key}.json"
    artifact_payload = {
        "summary": summary.to_dict(),
        "manifest": payload,
    }
    artifact_path.write_text(json.dumps(artifact_payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    result = {
        "artifact_path": str(artifact_path.relative_to(REPO_ROOT)),
        "summary": summary.to_dict(),
    }
    if args.json:
        print(json.dumps(result, indent=2, sort_keys=True))
    else:
        print(_render_text(result))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
