from __future__ import annotations

import argparse
import json
import subprocess
from pathlib import Path
from typing import Any

from config.settings import (
    PHASE7_CONFIG_VERSION,
    PHASE7_DEFAULT_EXPERIMENT_FAMILY,
    PHASE7_DEFAULT_RANDOM_SEED,
)
from database.db_manager import apply_schema
from phase7 import Phase7Repository


REPO_ROOT = Path(__file__).resolve().parent


def _git_head() -> str:
    try:
        completed = subprocess.run(
            ["git", "rev-parse", "--short", "HEAD"],
            cwd=REPO_ROOT,
            capture_output=True,
            text=True,
            check=True,
        )
    except Exception:
        return "unknown"
    return completed.stdout.strip() or "unknown"


def _load_config(args: argparse.Namespace) -> dict[str, Any]:
    if args.config_path:
        return json.loads(Path(args.config_path).read_text(encoding="utf-8-sig"))
    if args.config_json:
        return json.loads(args.config_json)
    return {}


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Record one Phase 7 advanced-research experiment in the reproducibility ledger."
    )
    parser.add_argument("--dataset-key", required=True, help="Registered Phase 7 research dataset key.")
    parser.add_argument("--experiment-name", required=True, help="Stable experiment name.")
    parser.add_argument("--experiment-version", required=True, help="Stable experiment version.")
    parser.add_argument("--model-version", required=True, help="Advanced model version or artifact id.")
    parser.add_argument(
        "--experiment-family",
        default=PHASE7_DEFAULT_EXPERIMENT_FAMILY,
        help="Logical experiment family, for example graph_features or temporal_models.",
    )
    parser.add_argument(
        "--baseline-model-version",
        action="append",
        default=[],
        help="Repeatable Phase 6 baseline model version.",
    )
    parser.add_argument(
        "--config-version",
        default=PHASE7_CONFIG_VERSION,
        help="Version tag for the experiment config contract.",
    )
    parser.add_argument("--config-path", default="", help="Optional JSON file with the experiment config.")
    parser.add_argument("--config-json", default="", help="Optional inline JSON config.")
    parser.add_argument(
        "--code-version",
        default="",
        help="Optional code or commit version override. Defaults to the current git short SHA.",
    )
    parser.add_argument(
        "--random-seed",
        type=int,
        default=PHASE7_DEFAULT_RANDOM_SEED,
        help="Random seed recorded in the ledger for reproducibility.",
    )
    parser.add_argument(
        "--status",
        default="planned",
        choices=("planned", "running", "completed", "failed", "cancelled"),
        help="Current experiment status.",
    )
    parser.add_argument("--started-at", default="", help="Optional experiment start timestamp.")
    parser.add_argument("--completed-at", default="", help="Optional experiment completion timestamp.")
    parser.add_argument(
        "--output-path",
        default="",
        help="Optional path to the real experiment output artifact if one already exists.",
    )
    parser.add_argument("--notes", default="", help="Optional experiment notes.")
    parser.add_argument(
        "--ledger-output-dir",
        default="reports/phase7/experiment_ledger",
        help="Directory for the ledger artifact JSON.",
    )
    parser.add_argument("--json", action="store_true", help="Emit machine-readable JSON.")
    return parser


def _render_text(payload: dict[str, Any]) -> str:
    summary = payload["summary"]
    return "\n".join(
        [
            f"Experiment run id: {summary['experiment_run_id']}",
            f"Experiment: {summary['experiment_name']} ({summary['experiment_version']})",
            f"Dataset key: {summary['dataset_key']}",
            f"Dataset hash: {summary['dataset_hash']}",
            f"Model version: {summary['model_version']}",
            f"Config hash: {summary['config_hash']}",
            f"Code version: {summary['code_version']}",
            f"Status: {summary['status']}",
            f"Ledger artifact: {payload['ledger_artifact_path']}",
        ]
    )


def main() -> int:
    args = build_parser().parse_args()
    apply_schema()

    config_json = _load_config(args)
    code_version = args.code_version.strip() or _git_head()
    ledger_dir = REPO_ROOT / args.ledger_output_dir
    ledger_dir.mkdir(parents=True, exist_ok=True)

    repo = Phase7Repository()
    summary = repo.record_experiment_run(
        dataset_key=args.dataset_key,
        experiment_name=args.experiment_name,
        experiment_family=args.experiment_family,
        experiment_version=args.experiment_version,
        model_version=args.model_version,
        baseline_model_versions=args.baseline_model_version,
        config_version=args.config_version,
        config_json=config_json,
        code_version=code_version,
        random_seed=args.random_seed,
        status=args.status,
        output_path=args.output_path or None,
        notes=args.notes or None,
        started_at=args.started_at or None,
        completed_at=args.completed_at or None,
    )

    ledger_artifact_path = ledger_dir / f"{summary.experiment_run_id}.json"
    ledger_output_path = args.output_path or str(ledger_artifact_path.relative_to(REPO_ROOT))
    summary_payload = {
        **summary.to_dict(),
        "output_path": ledger_output_path,
    }
    ledger_payload = {
        "summary": summary_payload,
        "config_json": config_json,
        "recorded_with": {
            "config_version": args.config_version,
            "code_version": code_version,
            "baseline_model_versions": args.baseline_model_version,
        },
    }
    ledger_artifact_path.write_text(
        json.dumps(ledger_payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    repo.update_experiment_output_path(
        experiment_run_id=summary.experiment_run_id,
        output_path=ledger_output_path,
    )

    result = {
        "ledger_artifact_path": ledger_output_path,
        "summary": summary_payload,
    }
    if args.json:
        print(json.dumps(result, indent=2, sort_keys=True))
    else:
        print(_render_text(result))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
