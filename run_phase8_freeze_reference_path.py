from __future__ import annotations

import argparse
import json
from pathlib import Path

from phase7 import write_json, write_markdown
from phase8 import (
    DEFAULT_REFERENCE_WINDOW_END,
    DEFAULT_REFERENCE_WINDOW_START,
    build_reference_freeze_manifest,
    render_reference_freeze_markdown,
)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Freeze one Phase 8 end-to-end reproducibility reference window with exact paths, versions, and hashes."
    )
    parser.add_argument("--start", default=DEFAULT_REFERENCE_WINDOW_START, help="UTC ISO8601 inclusive start timestamp.")
    parser.add_argument("--end", default=DEFAULT_REFERENCE_WINDOW_END, help="UTC ISO8601 exclusive end timestamp.")
    parser.add_argument(
        "--output-dir",
        default="reports/phase8/reference_window_freeze",
        help="Directory for the generated freeze manifest and summary.",
    )
    parser.add_argument("--json", action="store_true", help="Emit the full manifest JSON to stdout.")
    return parser


def _render_text(payload: dict[str, object]) -> str:
    return "\n".join(
        [
            f"Reference window: {payload['reference_window']['start']} -> {payload['reference_window']['end']}",
            f"Overall status: {payload['overall_status']}",
            f"Manifest: {payload['artifacts']['manifest_path']}",
            f"Manifest sha256: {payload['artifacts']['manifest_sha256']}",
            f"Summary: {payload['artifacts']['summary_path']}",
            f"Summary sha256: {payload['artifacts']['summary_sha256']}",
        ]
    )


def main() -> int:
    args = _build_parser().parse_args()
    manifest = build_reference_freeze_manifest(start=args.start, end=args.end)

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    manifest_path = output_dir / "phase8_reference_window_manifest.json"
    summary_path = output_dir / "phase8_reference_window_summary.md"

    manifest_sha256 = write_json(manifest_path, manifest)
    summary_sha256 = write_markdown(summary_path, render_reference_freeze_markdown(manifest))

    payload = {
        "reference_window": manifest["reference_window"],
        "overall_status": manifest["overall_status"],
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
