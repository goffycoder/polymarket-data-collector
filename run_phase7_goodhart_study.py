from __future__ import annotations

import argparse
import json
import subprocess
from pathlib import Path

from database.db_manager import apply_schema
from phase7 import Phase7Repository, build_goodhart_observability_study, render_goodhart_memo


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
        description="Run the Phase 7 Goodhart / observability study and write a reproducible memo."
    )
    parser.add_argument("--start", required=True, help="UTC ISO8601 inclusive start timestamp.")
    parser.add_argument("--end", required=True, help="UTC ISO8601 exclusive end timestamp.")
    parser.add_argument("--model-version", default="", help="Optional shadow model version override.")
    parser.add_argument("--dataset-key", default="", help="Optional Phase 7 dataset key for experiment-ledger recording.")
    parser.add_argument(
        "--baseline-model-version",
        action="append",
        default=[],
        help="Repeatable baseline model version(s) to attach when recording the study in the ledger.",
    )
    parser.add_argument(
        "--output-dir",
        default="reports/phase7/goodhart",
        help="Directory for the memo markdown and JSON report.",
    )
    parser.add_argument("--notes", default="", help="Optional notes for the study and experiment ledger.")
    parser.add_argument("--json", action="store_true", help="Emit JSON summary to stdout.")
    return parser


def _render_text(payload: dict) -> str:
    summary = payload["report"]["summary"]
    return "\n".join(
        [
            f"Window: {payload['report']['start']} -> {payload['report']['end']}",
            f"Shadow model: {payload['report']['model_version'] or 'none'}",
            f"Candidates: {summary['candidate_count']}",
            f"Alerts: {summary['alert_count']}",
            f"Findings: {len(payload['report']['findings'])}",
            f"Memo: {payload['memo_path']}",
        ]
    )


def main() -> int:
    args = _build_parser().parse_args()
    apply_schema()

    report = build_goodhart_observability_study(
        start=args.start,
        end=args.end,
        model_version=args.model_version.strip() or None,
    )

    output_dir = REPO_ROOT / args.output_dir
    output_dir.mkdir(parents=True, exist_ok=True)
    stem = f"goodhart_{args.start}_{args.end}".replace(":", "-")
    json_path = output_dir / f"{stem}.json"
    memo_path = output_dir / f"{stem}.md"
    json_path.write_text(json.dumps(report.to_dict(), indent=2, sort_keys=True) + "\n", encoding="utf-8")
    memo_path.write_text(render_goodhart_memo(report), encoding="utf-8")

    ledger_summary = None
    if args.dataset_key.strip():
        repo = Phase7Repository()
        ledger_summary = repo.record_experiment_run(
            dataset_key=args.dataset_key.strip(),
            experiment_name="goodhart_observability_study",
            experiment_family="observability_study",
            experiment_version="phase7_goodhart_v1",
            model_version=report.model_version or "observability_study_no_model",
            baseline_model_versions=args.baseline_model_version,
            config_json={
                "start": args.start,
                "end": args.end,
                "model_version": report.model_version,
                "finding_count": len(report.findings),
                "top_finding_titles": [finding["title"] for finding in report.findings[:3]],
            },
            code_version=_git_head(),
            random_seed=17,
            status="completed",
            output_path=str(json_path.relative_to(REPO_ROOT)),
            notes=args.notes or None,
        ).to_dict()

    payload = {
        "json_path": str(json_path),
        "memo_path": str(memo_path),
        "ledger_summary": ledger_summary,
        "report": report.to_dict(),
    }
    if args.json:
        print(json.dumps(payload, indent=2, sort_keys=True))
    else:
        print(_render_text(payload))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
