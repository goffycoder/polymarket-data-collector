from __future__ import annotations

import argparse
import json
from pathlib import Path

from phase7 import write_json, write_markdown
from phase8.metrics_review import build_phase8_metrics_review_manifest, render_phase8_metrics_review_markdown


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Build the Phase 8 final metrics bundle, limitations review, and stop-condition ledger."
    )
    parser.add_argument(
        "--output-dir",
        default="reports/phase8/metrics_review",
        help="Directory for the generated metrics review manifest and summary.",
    )
    parser.add_argument("--json", action="store_true", help="Emit the full manifest JSON to stdout.")
    return parser


def _render_text(payload: dict[str, object]) -> str:
    return "\n".join(
        [
            f"Overall status: {payload['readiness_summary']['overall_status']}",
            f"Highest-priority gap: {payload['readiness_summary']['highest_priority_gap']}",
            f"Manifest: {payload['artifacts']['manifest_path']}",
            f"Manifest sha256: {payload['artifacts']['manifest_sha256']}",
            f"Summary: {payload['artifacts']['summary_path']}",
            f"Summary sha256: {payload['artifacts']['summary_sha256']}",
        ]
    )


def main() -> int:
    args = _build_parser().parse_args()
    manifest = build_phase8_metrics_review_manifest()

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    manifest_path = output_dir / "phase8_metrics_review_manifest.json"
    summary_path = output_dir / "phase8_metrics_review_summary.md"

    manifest_sha256 = write_json(manifest_path, manifest)
    summary_sha256 = write_markdown(summary_path, render_phase8_metrics_review_markdown(manifest))

    payload = {
        "readiness_summary": manifest["readiness_summary"],
        "artifacts": {
            "manifest_path": str(manifest_path),
            "manifest_sha256": manifest_sha256,
            "summary_path": str(summary_path),
            "summary_sha256": summary_sha256,
        },
    }
    if args.json:
        print(json.dumps({"manifest": manifest, **payload}, indent=2, sort_keys=True))
    else:
        print(_render_text(payload))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
