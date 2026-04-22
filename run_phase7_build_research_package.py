from __future__ import annotations

import argparse
import json
import subprocess
from pathlib import Path
from typing import Any

from config.settings import PHASE7_RESEARCH_PACKAGE_VERSION
from database.db_manager import apply_schema
from phase7 import (
    Phase7Repository,
    build_ablation_summary,
    build_research_manifest,
    canonical_family_key,
    load_json,
    render_ablation_table_markdown,
    render_auc_figure_svg,
    render_margin_figure_svg,
    render_methodology_markdown,
    render_observability_figure_svg,
    sha256_file,
    write_csv,
    write_json,
    write_markdown,
    write_text,
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
        description="Build the Phase 7 final research package with ablations, figures, and a reproducibility manifest."
    )
    parser.add_argument(
        "--dataset-key",
        default="",
        help="Optional registered Phase 7 dataset key. If set, saved experiment artifacts are auto-discovered from the ledger.",
    )
    parser.add_argument(
        "--report-spec",
        action="append",
        default=[],
        help="Repeatable family=path pair for explicit advanced experiment artifacts. Families: graph_aware, temporal, other_advanced.",
    )
    parser.add_argument(
        "--goodhart-report",
        default="",
        help="Optional explicit path to a saved Goodhart / observability JSON artifact.",
    )
    parser.add_argument(
        "--output-dir",
        default="reports/phase7/research_package",
        help="Directory for final package outputs.",
    )
    parser.add_argument("--notes", default="", help="Optional notes to attach when recording the package in the ledger.")
    parser.add_argument("--json", action="store_true", help="Emit JSON summary to stdout.")
    return parser


def _resolve_repo_path(path_value: str) -> Path:
    candidate = Path(path_value)
    if candidate.is_absolute():
        return candidate
    return (REPO_ROOT / candidate).resolve()


def _parse_report_specs(specs: list[str]) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    for spec in specs:
        family_part, separator, path_part = str(spec).partition("=")
        if not separator or not path_part.strip():
            raise ValueError(f"Invalid --report-spec value: {spec!r}. Expected family=path.")
        artifact_path = _resolve_repo_path(path_part.strip())
        results.append(
            {
                "family_key": canonical_family_key(family_part.strip()),
                "artifact_path": str(artifact_path),
                "payload": load_json(artifact_path),
                "source": "explicit",
            }
        )
    return results


def _discover_ledger_artifacts(dataset_key: str) -> tuple[list[dict[str, Any]], dict[str, Any] | None]:
    repo = Phase7Repository()
    runs = repo.list_experiment_runs(dataset_key=dataset_key, limit=200)
    experiment_artifacts: list[dict[str, Any]] = []
    selected_families: set[str] = set()
    goodhart_report = None

    for run in runs:
        output_path = str(run.get("output_path") or "").strip()
        if not output_path:
            continue
        artifact_path = _resolve_repo_path(output_path)
        if not artifact_path.exists():
            continue

        family_key = canonical_family_key(run.get("experiment_family"))
        name = str(run.get("experiment_name") or "").lower()
        if "goodhart" in name or family_key == "other_advanced" and "observability" in name:
            if goodhart_report is None:
                goodhart_report = load_json(artifact_path)
            continue

        payload = load_json(artifact_path)
        if "experiment_report" not in payload:
            continue
        if family_key in selected_families:
            continue
        selected_families.add(family_key)
        experiment_artifacts.append(
            {
                "family_key": family_key,
                "artifact_path": str(artifact_path),
                "payload": payload,
                "source": "ledger",
                "experiment_run_id": run.get("experiment_run_id"),
            }
        )
    return experiment_artifacts, goodhart_report


def _artifact_entry(*, family_key: str, path: Path, payload: dict[str, Any], kind: str) -> dict[str, Any]:
    return {
        "family_key": family_key,
        "kind": kind,
        "path": str(path),
        "sha256": sha256_file(path),
        "dataset_hash": payload.get("dataset_hash") or ((payload.get("dataset_summary") or {}).get("dataset_hash")),
        "model_version": payload.get("advanced_model_version") or payload.get("model_version"),
    }


def _goodhart_summary(report: dict[str, Any] | None) -> dict[str, Any] | None:
    if not report:
        return None
    measurable = report.get("measurable_risks") or {}
    return {
        "window": {
            "start": report.get("start"),
            "end": report.get("end"),
        },
        "finding_count": len(report.get("findings") or []),
        "top_findings": [finding.get("title") for finding in (report.get("findings") or [])[:3]],
        "threshold_bunching_rate": (measurable.get("threshold_bunching") or {}).get("threshold_bunching_rate"),
        "top_category_dependency_share": measurable.get("top_category_dependency_share"),
        "top_event_family_dependency_share": measurable.get("top_event_family_dependency_share"),
    }


def _render_text(payload: dict[str, Any]) -> str:
    summary = payload["ablation_summary"]
    return "\n".join(
        [
            f"Package status: {summary['status']}",
            f"Available families: {', '.join(summary['available_families']) or 'none'}",
            f"Missing families: {', '.join(summary['missing_families']) or 'none'}",
            f"Manifest: {payload['artifacts']['manifest_path']}",
            f"Summary: {payload['artifacts']['summary_path']}",
        ]
    )


def main() -> int:
    args = _build_parser().parse_args()
    apply_schema()

    explicit_experiments = _parse_report_specs(args.report_spec)
    ledger_experiments: list[dict[str, Any]] = []
    discovered_goodhart = None
    dataset_key = args.dataset_key.strip() or None
    if dataset_key:
        ledger_experiments, discovered_goodhart = _discover_ledger_artifacts(dataset_key)

    experiment_artifacts = explicit_experiments or ledger_experiments
    goodhart_report = load_json(_resolve_repo_path(args.goodhart_report)) if args.goodhart_report.strip() else discovered_goodhart
    ablation_summary = build_ablation_summary(experiment_artifacts=experiment_artifacts)

    output_dir = REPO_ROOT / args.output_dir
    output_dir.mkdir(parents=True, exist_ok=True)
    package_stem = dataset_key or "adhoc_phase7"

    ablation_json_path = output_dir / f"{package_stem}_ablation_summary.json"
    ablation_csv_path = output_dir / f"{package_stem}_ablation_rows.csv"
    split_csv_path = output_dir / f"{package_stem}_split_metrics.csv"
    ablation_md_path = output_dir / f"{package_stem}_ablation_table.md"
    summary_md_path = output_dir / f"{package_stem}_research_summary.md"
    auc_svg_path = output_dir / f"{package_stem}_test_auc.svg"
    margin_svg_path = output_dir / f"{package_stem}_margin_vs_phase6.svg"
    observability_svg_path = output_dir / f"{package_stem}_observability_risks.svg"
    manifest_path = output_dir / f"{package_stem}_manifest.json"

    input_artifacts = [
        _artifact_entry(
            family_key=str(item["family_key"]),
            path=Path(str(item["artifact_path"])),
            payload=item["payload"],
            kind="advanced_experiment_report",
        )
        for item in experiment_artifacts
    ]
    if goodhart_report:
        goodhart_path = _resolve_repo_path(args.goodhart_report) if args.goodhart_report.strip() else None
        if goodhart_path is None and dataset_key:
            repo = Phase7Repository()
            for run in repo.list_experiment_runs(dataset_key=dataset_key, limit=200):
                output_path = str(run.get("output_path") or "").strip()
                if not output_path:
                    continue
                name = str(run.get("experiment_name") or "").lower()
                if "goodhart" in name or "observability" in name:
                    candidate = _resolve_repo_path(output_path)
                    if candidate.exists():
                        goodhart_path = candidate
                        break
        if goodhart_path is not None:
            input_artifacts.append(
                {
                    "family_key": "goodhart_observability",
                    "kind": "goodhart_report",
                    "path": str(goodhart_path),
                    "sha256": sha256_file(goodhart_path),
                    "dataset_hash": None,
                    "model_version": goodhart_report.get("model_version"),
                }
            )

    output_artifacts: list[dict[str, Any]] = []
    output_artifacts.append(
        {
            "name": "ablation_summary_json",
            "path": str(ablation_json_path),
            "sha256": write_json(ablation_json_path, ablation_summary.to_dict()),
        }
    )
    output_artifacts.append(
        {
            "name": "ablation_rows_csv",
            "path": str(ablation_csv_path),
            "sha256": write_csv(
                ablation_csv_path,
                ablation_summary.family_rows,
                fieldnames=[
                    "family_key",
                    "display_name",
                    "availability_status",
                    "dataset_hash",
                    "artifact_path",
                    "model_version",
                    "experiment_version",
                    "train_auc",
                    "validation_auc",
                    "test_auc",
                    "train_precision_at_10",
                    "validation_precision_at_10",
                    "test_precision_at_10",
                    "train_row_count",
                    "validation_row_count",
                    "test_row_count",
                    "strict_holdout_status",
                    "strict_holdout_accepted",
                    "validation_margin_vs_phase6_auc",
                    "test_margin_vs_phase6_auc",
                    "test_margin_vs_best_heuristic_auc",
                    "uses_wallet_features",
                    "uses_graph_features",
                    "uses_temporal_model",
                    "uses_advanced_modeling",
                    "notes",
                ],
            ),
        }
    )
    output_artifacts.append(
        {
            "name": "split_metrics_csv",
            "path": str(split_csv_path),
            "sha256": write_csv(
                split_csv_path,
                ablation_summary.split_rows,
                fieldnames=[
                    "family_key",
                    "display_name",
                    "split_name",
                    "dataset_hash",
                    "artifact_path",
                    "model_version",
                    "auc",
                    "precision_at_10",
                    "precision_at_25",
                    "positive_rate",
                    "row_count",
                    "mean_score",
                ],
            ),
        }
    )
    output_artifacts.append(
        {
            "name": "ablation_table_markdown",
            "path": str(ablation_md_path),
            "sha256": write_markdown(ablation_md_path, render_ablation_table_markdown(ablation_summary)),
        }
    )
    output_artifacts.append(
        {
            "name": "test_auc_figure_svg",
            "path": str(auc_svg_path),
            "sha256": write_text(auc_svg_path, render_auc_figure_svg(ablation_summary)),
        }
    )
    output_artifacts.append(
        {
            "name": "margin_figure_svg",
            "path": str(margin_svg_path),
            "sha256": write_text(margin_svg_path, render_margin_figure_svg(ablation_summary)),
        }
    )
    if goodhart_report:
        output_artifacts.append(
            {
                "name": "observability_figure_svg",
                "path": str(observability_svg_path),
                "sha256": write_text(observability_svg_path, render_observability_figure_svg(goodhart_report)),
            }
        )
    output_artifacts.append(
        {
            "name": "research_summary_markdown",
            "path": str(summary_md_path),
            "sha256": write_markdown(
                summary_md_path,
                render_methodology_markdown(
                    dataset_key=dataset_key,
                    ablation_summary=ablation_summary,
                    goodhart_report=goodhart_report,
                    input_artifacts=input_artifacts,
                ),
            ),
        }
    )

    manifest = build_research_manifest(
        dataset_key=dataset_key,
        code_version=_git_head(),
        ablation_summary=ablation_summary,
        input_artifacts=input_artifacts,
        output_artifacts=output_artifacts,
        goodhart_summary=_goodhart_summary(goodhart_report),
    )
    output_artifacts.append(
        {
            "name": "manifest_json",
            "path": str(manifest_path),
            "sha256": write_json(manifest_path, manifest),
        }
    )

    ledger_summary = None
    if dataset_key:
        repo = Phase7Repository()
        ledger_summary = repo.record_experiment_run(
            dataset_key=dataset_key,
            experiment_name="final_research_package",
            experiment_family="research_package",
            experiment_version=PHASE7_RESEARCH_PACKAGE_VERSION,
            model_version="phase7_research_package",
            baseline_model_versions=[],
            config_json={
                "input_artifact_count": len(input_artifacts),
                "output_artifact_count": len(output_artifacts),
                "package_status": ablation_summary.status,
                "missing_families": ablation_summary.missing_families,
                "notes": args.notes or None,
            },
            code_version=_git_head(),
            random_seed=17,
            status="completed",
            output_path=str(manifest_path.relative_to(REPO_ROOT)),
            notes=args.notes or None,
        ).to_dict()

    payload = {
        "dataset_key": dataset_key,
        "ablation_summary": ablation_summary.to_dict(),
        "goodhart_summary": _goodhart_summary(goodhart_report),
        "ledger_summary": ledger_summary,
        "artifacts": {
            "manifest_path": str(manifest_path),
            "summary_path": str(summary_md_path),
            "ablation_table_path": str(ablation_md_path),
            "ablation_rows_csv_path": str(ablation_csv_path),
            "split_metrics_csv_path": str(split_csv_path),
            "auc_figure_path": str(auc_svg_path),
            "margin_figure_path": str(margin_svg_path),
            "observability_figure_path": str(observability_svg_path) if goodhart_report else None,
        },
    }
    if args.json:
        print(json.dumps(payload, indent=2, sort_keys=True))
    else:
        print(_render_text(payload))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
