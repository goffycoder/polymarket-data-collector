from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from phase7 import write_json, write_markdown
from phase8.closeout import (
    build_phase8_final_closeout_manifest,
    render_phase8_final_closeout_markdown,
)
from phase8.freeze import build_reference_freeze_manifest, render_reference_freeze_markdown
from phase8.metrics_review import (
    build_phase8_metrics_review_manifest,
    render_phase8_metrics_review_markdown,
)
from phase8.operating_mode import build_v1_operating_mode_manifest, render_v1_operating_mode_markdown


PHASE9_TASK5_CONTRACT_VERSION = "phase9_task5_closeout_refresh_v1"


def _iso_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _write_bundle(
    *,
    output_dir: Path,
    manifest_name: str,
    summary_name: str,
    manifest: dict[str, Any],
    markdown: str,
) -> dict[str, Any]:
    output_dir.mkdir(parents=True, exist_ok=True)
    manifest_path = output_dir / manifest_name
    summary_path = output_dir / summary_name
    manifest_sha256 = write_json(manifest_path, manifest)
    summary_sha256 = write_markdown(summary_path, markdown)
    return {
        "manifest_path": str(manifest_path).replace("\\", "/"),
        "manifest_sha256": manifest_sha256,
        "summary_path": str(summary_path).replace("\\", "/"),
        "summary_sha256": summary_sha256,
    }


def render_phase9_task5_markdown(summary: dict[str, Any]) -> str:
    closeout = summary["phase8_final_closeout"]["final_closeout_memo"]
    lines = [
        "# Phase 9 Task 5 Closeout Refresh",
        "",
        f"- Contract version: `{summary['task_contract_version']}`",
        f"- Generated at: `{summary['generated_at']}`",
        f"- Canonical v1 mode: `{closeout['canonical_v1_mode']}`",
        f"- SRS v1 complete: `{closeout['srs_v1_complete']}`",
        f"- Overall status: `{closeout['overall_status']}`",
        "",
        "## Direct Answer",
        f"- {closeout['direct_answer']}",
        "",
        "## Refreshed Artifacts",
    ]
    for key in (
        "phase8_reference_freeze",
        "phase8_operating_mode",
        "phase8_metrics_review",
        "phase8_final_closeout",
    ):
        artifact = summary["artifacts"][key]
        lines.append(f"- `{key}`: `{artifact['manifest_path']}` and `{artifact['summary_path']}`")
    lines.extend(["", "## Remaining Blockers"])
    for item in closeout["primary_blockers"]:
        lines.append(f"- {item}")
    return "\n".join(lines) + "\n"


def run_phase9_task5_closeout_refresh(
    *,
    phase8_reference_dir: str = "reports/phase8/reference_window_freeze",
    phase8_operating_dir: str = "reports/phase8/operating_mode",
    phase8_metrics_dir: str = "reports/phase8/metrics_review",
    phase8_closeout_dir: str = "reports/phase8/final_closeout",
    task5_output_dir: str = "reports/phase9/closeout_refresh",
) -> dict[str, Any]:
    reference_manifest = build_reference_freeze_manifest()
    reference_artifacts = _write_bundle(
        output_dir=Path(phase8_reference_dir),
        manifest_name="phase8_reference_window_manifest.json",
        summary_name="phase8_reference_window_summary.md",
        manifest=reference_manifest,
        markdown=render_reference_freeze_markdown(reference_manifest),
    )

    operating_manifest = build_v1_operating_mode_manifest()
    operating_artifacts = _write_bundle(
        output_dir=Path(phase8_operating_dir),
        manifest_name="phase8_v1_operating_mode_manifest.json",
        summary_name="phase8_v1_operating_mode_summary.md",
        manifest=operating_manifest,
        markdown=render_v1_operating_mode_markdown(operating_manifest),
    )

    metrics_manifest = build_phase8_metrics_review_manifest()
    metrics_artifacts = _write_bundle(
        output_dir=Path(phase8_metrics_dir),
        manifest_name="phase8_metrics_review_manifest.json",
        summary_name="phase8_metrics_review_summary.md",
        manifest=metrics_manifest,
        markdown=render_phase8_metrics_review_markdown(metrics_manifest),
    )

    closeout_manifest = build_phase8_final_closeout_manifest()
    closeout_artifacts = _write_bundle(
        output_dir=Path(phase8_closeout_dir),
        manifest_name="phase8_final_closeout_manifest.json",
        summary_name="phase8_final_closeout_summary.md",
        manifest=closeout_manifest,
        markdown=render_phase8_final_closeout_markdown(closeout_manifest),
    )

    summary = {
        "task_contract_version": PHASE9_TASK5_CONTRACT_VERSION,
        "generated_at": _iso_now(),
        "phase8_reference_freeze": {
            "overall_status": reference_manifest["overall_status"],
            "reference_window": reference_manifest["reference_window"],
        },
        "phase8_operating_mode": {
            "decision": operating_manifest["decision"],
        },
        "phase8_metrics_review": {
            "readiness_summary": metrics_manifest["readiness_summary"],
        },
        "phase8_final_closeout": {
            "final_closeout_memo": closeout_manifest["final_closeout_memo"],
        },
        "artifacts": {
            "phase8_reference_freeze": reference_artifacts,
            "phase8_operating_mode": operating_artifacts,
            "phase8_metrics_review": metrics_artifacts,
            "phase8_final_closeout": closeout_artifacts,
        },
        "single_owner_runbooks": [
            "database/PHASE5_SINGLE_OWNER_RUNBOOK.md",
            "database/PHASE6_SINGLE_OWNER_RUNBOOK.md",
        ],
    }

    task5_dir = Path(task5_output_dir)
    task5_dir.mkdir(parents=True, exist_ok=True)
    summary_path = task5_dir / "phase9_task5_closeout_refresh_summary.json"
    summary_markdown_path = task5_dir / "phase9_task5_closeout_refresh_summary.md"
    summary["task5_artifacts"] = {
        "summary_path": str(summary_path).replace("\\", "/"),
        "summary_markdown_path": str(summary_markdown_path).replace("\\", "/"),
    }
    summary_markdown_sha256 = write_markdown(summary_markdown_path, render_phase9_task5_markdown(summary))
    summary["task5_artifacts"].update(
        {
            "summary_markdown_sha256": summary_markdown_sha256,
        }
    )
    write_json(summary_path, summary)
    return summary
