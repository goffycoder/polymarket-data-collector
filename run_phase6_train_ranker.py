from __future__ import annotations

import argparse
import json
from pathlib import Path

from config.settings import PHASE6_DEFAULT_MODEL_NAME
from phase5.repository import Phase5Repository
from phase6 import (
    Phase6Repository,
    build_calibration_profiles,
    build_model_card_markdown,
    build_score_report,
    build_training_frame,
    fit_linear_ranker,
    score_training_frame,
)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Train and evaluate the Phase 6 Person 2 starter ranker.")
    parser.add_argument("--start", required=True, help="UTC ISO8601 inclusive start timestamp.")
    parser.add_argument("--end", required=True, help="UTC ISO8601 exclusive end timestamp.")
    parser.add_argument(
        "--model-version",
        default=f"{PHASE6_DEFAULT_MODEL_NAME}_person2_v1",
        help="Model version label for the generated artifact.",
    )
    parser.add_argument(
        "--output-dir",
        default="reports/phase6/model_artifacts",
        help="Directory for model, report, and model-card artifacts.",
    )
    parser.add_argument("--json", action="store_true", help="Emit JSON summary to stdout.")
    return parser


def main() -> int:
    args = _build_parser().parse_args()
    repository = Phase5Repository()
    rows = repository.load_evaluation_rows(start=args.start, end=args.end)
    frame, dataset_summary = build_training_frame(rows, repository=repository)
    model_spec, fit_summary = fit_linear_ranker(
        frame,
        model_version=args.model_version,
        dataset_hash=dataset_summary.dataset_hash,
    )
    scored = score_training_frame(frame, model_spec=model_spec)
    score_report = build_score_report(scored)
    calibration_profiles = build_calibration_profiles(scored)

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    model_path = output_dir / f"{args.model_version}.json"
    report_path = output_dir / f"{args.model_version}_report.json"
    model_card_path = output_dir / f"{args.model_version}_model_card.md"
    scored_csv_path = output_dir / f"{args.model_version}_scored.csv"

    model_path.write_text(json.dumps(model_spec, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    scored.to_csv(scored_csv_path, index=False)

    score_payload = {
        "dataset_summary": dataset_summary.to_dict(),
        "fit_summary": fit_summary.to_dict(),
        "score_report": score_report,
        "calibration_profiles": [profile.to_dict() for profile in calibration_profiles],
        "artifacts": {
            "model_path": str(model_path),
            "scored_csv_path": str(scored_csv_path),
        },
    }
    report_path.write_text(json.dumps(score_payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    model_card_path.write_text(
        build_model_card_markdown(
            model_version=args.model_version,
            dataset_hash=dataset_summary.dataset_hash,
            score_report=score_report,
            calibration_profiles=calibration_profiles,
        ),
        encoding="utf-8",
    )

    repo = Phase6Repository()
    evaluation_summary = repo.record_evaluation_run(
        model_version=args.model_version,
        feature_schema_version=str(model_spec["feature_schema_version"]),
        dataset_hash=dataset_summary.dataset_hash,
        start=args.start,
        end=args.end,
        train_row_count=dataset_summary.train_row_count,
        validation_row_count=dataset_summary.validation_row_count,
        test_row_count=dataset_summary.test_row_count,
        labeled_row_count=dataset_summary.labeled_row_count,
        output_path=str(report_path),
        summary_json=score_payload,
    )
    calibration_summaries = [
        repo.record_calibration_profile(
            model_version=args.model_version,
            profile_scope=profile.profile_scope,
            profile_key=profile.profile_key,
            sample_count=profile.sample_count,
            positive_rate=profile.positive_rate,
            watch_threshold=profile.watch_threshold,
            actionable_threshold=profile.actionable_threshold,
            critical_threshold=profile.critical_threshold,
            metadata_json=profile.metadata,
        ).to_dict()
        for profile in calibration_profiles
    ]

    payload = {
        "dataset_summary": dataset_summary.to_dict(),
        "fit_summary": fit_summary.to_dict(),
        "evaluation_summary": evaluation_summary.to_dict(),
        "calibration_profiles_written": calibration_summaries,
        "artifacts": {
            "model_path": str(model_path),
            "report_path": str(report_path),
            "model_card_path": str(model_card_path),
            "scored_csv_path": str(scored_csv_path),
        },
    }
    if args.json:
        print(json.dumps(payload, indent=2, sort_keys=True))
    else:
        print(f"Phase 6 Person 2 model: {args.model_version}")
        print(f"Dataset hash: {dataset_summary.dataset_hash}")
        print(f"Report: {report_path}")
        print(f"Model card: {model_card_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
