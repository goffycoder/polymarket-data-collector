from __future__ import annotations

import argparse
import json
import subprocess
from pathlib import Path

from config.settings import (
    PHASE7_ADVANCED_EXPERIMENT_VERSION,
    PHASE7_ADVANCED_MODEL_NAME,
    PHASE7_GRAPH_FEATURE_SCHEMA_VERSION,
    PHASE7_GRAPH_LOOKBACK_DAYS,
    PHASE7_GRAPH_PERSISTENCE_MIN_DAYS,
)
from database.db_manager import apply_schema
from phase5.repository import Phase5Repository
from phase6 import build_score_report, fit_linear_ranker
from phase7 import (
    Phase7Repository,
    build_advanced_experiment_report,
    build_advanced_model_card_markdown,
    build_graph_training_frame,
    fit_graph_aware_ranker,
    score_with_model_spec,
)


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


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Train and evaluate one Phase 7 graph-aware advanced ranker against the Phase 6 baseline."
    )
    parser.add_argument("--start", required=True, help="UTC ISO8601 inclusive start timestamp.")
    parser.add_argument("--end", required=True, help="UTC ISO8601 exclusive end timestamp.")
    parser.add_argument(
        "--model-version",
        default=f"{PHASE7_ADVANCED_MODEL_NAME}_v1",
        help="Model version label for the advanced graph-aware ranker.",
    )
    parser.add_argument(
        "--phase6-baseline-model-version",
        default="",
        help="Optional explicit label for the internally refit Phase 6 baseline model.",
    )
    parser.add_argument(
        "--dataset-key",
        default="",
        help="Optional registered Phase 7 research dataset key for experiment-ledger recording.",
    )
    parser.add_argument(
        "--baseline-model-version",
        action="append",
        default=[],
        help="Repeatable registered baseline model version(s) to attach to the experiment ledger.",
    )
    parser.add_argument(
        "--output-dir",
        default="reports/phase7/model_artifacts",
        help="Directory for model, report, and model-card artifacts.",
    )
    parser.add_argument(
        "--lookback-days",
        type=int,
        default=PHASE7_GRAPH_LOOKBACK_DAYS,
        help="Lookback window used for graph feature materialization.",
    )
    parser.add_argument(
        "--persistence-min-days",
        type=int,
        default=PHASE7_GRAPH_PERSISTENCE_MIN_DAYS,
        help="Minimum distinct trade days to count a wallet as persistent.",
    )
    parser.add_argument(
        "--feature-schema-version",
        default=PHASE7_GRAPH_FEATURE_SCHEMA_VERSION,
        help="Feature schema version for the advanced graph-aware model input.",
    )
    parser.add_argument("--notes", default="", help="Optional notes for the model artifact and ledger.")
    parser.add_argument("--json", action="store_true", help="Emit JSON summary to stdout.")
    return parser


def _render_text(payload: dict) -> str:
    assessment = payload["experiment_report"]["holdout_assessment"]
    return "\n".join(
        [
            f"Advanced model: {payload['advanced_model_version']}",
            f"Dataset hash: {payload['dataset_hash']}",
            f"Holdout status: {assessment['status']}",
            f"Validation margin AUC: {assessment['validation_margin_auc']}",
            f"Test margin AUC: {assessment['test_margin_auc']}",
            f"Report: {payload['artifacts']['report_path']}",
        ]
    )


def main() -> int:
    args = _build_parser().parse_args()
    apply_schema()

    repository = Phase5Repository()
    rows = repository.load_evaluation_rows(start=args.start, end=args.end)
    frame, dataset_summary, graph_diagnostics = build_graph_training_frame(
        rows,
        repository=repository,
        feature_schema_version=args.feature_schema_version,
        lookback_days=max(1, args.lookback_days),
        persistence_min_days=max(1, args.persistence_min_days),
    )

    advanced_model_spec, advanced_fit_summary = fit_graph_aware_ranker(
        frame,
        model_version=args.model_version,
        dataset_hash=dataset_summary.dataset_hash,
    )
    advanced_fit_payload = advanced_fit_summary.to_dict()
    phase6_baseline_model_version = (
        args.phase6_baseline_model_version.strip()
        or f"{args.model_version}__phase6_baseline"
    )
    try:
        phase6_model_spec, phase6_fit_summary = fit_linear_ranker(
            frame,
            model_version=phase6_baseline_model_version,
            dataset_hash=dataset_summary.dataset_hash,
        )
        phase6_fit_payload = {
            **phase6_fit_summary.to_dict(),
            "status": "trained",
            "notes": [],
        }
    except ValueError as exc:
        phase6_model_spec = None
        phase6_fit_payload = {
            "model_version": phase6_baseline_model_version,
            "dataset_hash": dataset_summary.dataset_hash,
            "labeled_row_count": int(frame["label_available"].sum()) if "label_available" in frame.columns else 0,
            "train_row_count": int(
                ((frame["dataset_partition"] == "train") & (frame["label_available"])).sum()
            ) if {"dataset_partition", "label_available"}.issubset(frame.columns) else 0,
            "feature_count": 11,
            "base_rate": 0.0,
            "status": "insufficient_training_data",
            "notes": [str(exc)],
        }

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    report_path = output_dir / f"{args.model_version}_report.json"
    model_card_path = output_dir / f"{args.model_version}_model_card.md"

    if advanced_model_spec is None or phase6_model_spec is None:
        sparse_payload = {
            "experiment_version": PHASE7_ADVANCED_EXPERIMENT_VERSION,
            "advanced_model_version": args.model_version,
            "phase6_baseline_model_version": phase6_baseline_model_version,
            "dataset_hash": dataset_summary.dataset_hash,
            "dataset_summary": dataset_summary.to_dict(),
            "graph_diagnostics": graph_diagnostics,
            "advanced_fit_summary": advanced_fit_payload,
            "phase6_fit_summary": phase6_fit_payload,
            "experiment_report": {
                "holdout_assessment": {
                    "status": "insufficient_training_data",
                    "accepted": False,
                    "validation_margin_auc": None,
                    "test_margin_auc": None,
                    "reasons": list(advanced_fit_payload.get("notes") or []) + list(phase6_fit_payload.get("notes") or []),
                }
            },
        }
        report_path.write_text(json.dumps(sparse_payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        model_card_path.write_text(
            "\n".join(
                [
                    f"# Phase 7 Advanced Model Card: {args.model_version}",
                    "",
                    "## Status",
                    "- Training did not run because the selected historical window did not contain enough labeled train rows.",
                    f"- Dataset hash: `{dataset_summary.dataset_hash}`",
                    f"- Reasons: `{', '.join(list(advanced_fit_payload.get('notes') or []) + list(phase6_fit_payload.get('notes') or [])) or 'unknown'}`",
                    "",
                ]
            ),
            encoding="utf-8",
        )
        payload = {
            "advanced_model_version": args.model_version,
            "phase6_baseline_model_version": phase6_baseline_model_version,
            "dataset_hash": dataset_summary.dataset_hash,
            "artifacts": {
                "report_path": str(report_path),
                "model_card_path": str(model_card_path),
            },
            "experiment_report": sparse_payload["experiment_report"],
        }
        if args.json:
            print(json.dumps(payload, indent=2, sort_keys=True))
        else:
            print(_render_text(payload))
        return 0

    advanced_scored = score_with_model_spec(frame, model_spec=advanced_model_spec)
    phase6_scored = score_with_model_spec(frame, model_spec=phase6_model_spec)

    phase6_baseline_scores = phase6_scored[["candidate_id", "model_score"]].rename(
        columns={"model_score": "phase6_baseline_model_score"}
    )
    combined_scored = advanced_scored.merge(
        phase6_baseline_scores,
        on="candidate_id",
        how="left",
    )

    advanced_score_report = build_score_report(advanced_scored)
    phase6_score_report = build_score_report(phase6_scored)
    experiment_report = build_advanced_experiment_report(
        dataset_summary=dataset_summary.to_dict(),
        graph_diagnostics=graph_diagnostics,
        advanced_model_version=args.model_version,
        phase6_baseline_model_version=phase6_baseline_model_version,
        advanced_score_report=advanced_score_report,
        phase6_score_report=phase6_score_report,
    )

    advanced_model_path = output_dir / f"{args.model_version}.json"
    phase6_model_path = output_dir / f"{phase6_baseline_model_version}.json"
    scored_csv_path = output_dir / f"{args.model_version}_scored.csv"
    advanced_model_path.write_text(json.dumps(advanced_model_spec, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    phase6_model_path.write_text(json.dumps(phase6_model_spec, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    combined_scored.to_csv(scored_csv_path, index=False)

    report_payload = {
        "experiment_version": PHASE7_ADVANCED_EXPERIMENT_VERSION,
        "advanced_model_version": args.model_version,
        "phase6_baseline_model_version": phase6_baseline_model_version,
        "dataset_hash": dataset_summary.dataset_hash,
        "dataset_summary": dataset_summary.to_dict(),
        "graph_diagnostics": graph_diagnostics,
        "advanced_fit_summary": advanced_fit_payload,
        "phase6_fit_summary": phase6_fit_payload,
        "advanced_model_spec": advanced_model_spec,
        "phase6_model_spec": phase6_model_spec,
        "experiment_report": experiment_report,
        "artifacts": {
            "advanced_model_path": str(advanced_model_path),
            "phase6_model_path": str(phase6_model_path),
            "scored_csv_path": str(scored_csv_path),
        },
    }
    report_path.write_text(json.dumps(report_payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    model_card_path.write_text(
        build_advanced_model_card_markdown(
            advanced_model_version=args.model_version,
            phase6_baseline_model_version=phase6_baseline_model_version,
            dataset_hash=dataset_summary.dataset_hash,
            experiment_report=experiment_report,
        ),
        encoding="utf-8",
    )

    ledger_payload = None
    if args.dataset_key.strip():
        repo = Phase7Repository()
        ledger_summary = repo.record_experiment_run(
            dataset_key=args.dataset_key.strip(),
            experiment_name="graph_aware_ranker",
            experiment_family="graph_aware_model",
            experiment_version=PHASE7_ADVANCED_EXPERIMENT_VERSION,
            model_version=args.model_version,
            baseline_model_versions=args.baseline_model_version,
            config_json={
                "start": args.start,
                "end": args.end,
                "feature_schema_version": args.feature_schema_version,
                "lookback_days": max(1, args.lookback_days),
                "persistence_min_days": max(1, args.persistence_min_days),
                "phase6_baseline_model_version": phase6_baseline_model_version,
                "strict_holdout_status": experiment_report["holdout_assessment"]["status"],
            },
            code_version=_git_head(),
            random_seed=17,
            status="completed",
            output_path=str(report_path.relative_to(REPO_ROOT)),
            notes=args.notes or None,
        )
        ledger_payload = ledger_summary.to_dict()

    payload = {
        "advanced_model_version": args.model_version,
        "phase6_baseline_model_version": phase6_baseline_model_version,
        "dataset_hash": dataset_summary.dataset_hash,
        "advanced_fit_summary": advanced_fit_payload,
        "phase6_fit_summary": phase6_fit_payload,
        "experiment_report": experiment_report,
        "ledger_summary": ledger_payload,
        "artifacts": {
            "advanced_model_path": str(advanced_model_path),
            "phase6_model_path": str(phase6_model_path),
            "report_path": str(report_path),
            "model_card_path": str(model_card_path),
            "scored_csv_path": str(scored_csv_path),
        },
    }
    if args.json:
        print(json.dumps(payload, indent=2, sort_keys=True))
    else:
        print(_render_text(payload))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
