from __future__ import annotations

import argparse
import json

from phase9.closeout_refresh import run_phase9_task5_closeout_refresh


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Refresh the Phase 8 closeout artifacts using the Phase 9 materialized evidence and single-owner runbooks."
    )
    parser.add_argument("--json", action="store_true", help="Emit the full Task 5 summary JSON to stdout.")
    return parser


def _render_text(summary: dict[str, object]) -> str:
    memo = summary["phase8_final_closeout"]["final_closeout_memo"]
    artifacts = summary["task5_artifacts"]
    return "\n".join(
        [
            f"SRS v1 complete: {memo['srs_v1_complete']}",
            f"Overall status: {memo['overall_status']}",
            f"Direct answer: {memo['direct_answer']}",
            f"Summary: {artifacts['summary_path']}",
            f"Summary markdown: {artifacts['summary_markdown_path']}",
            f"Summary markdown sha256: {artifacts['summary_markdown_sha256']}",
        ]
    )


def main() -> int:
    args = _build_parser().parse_args()
    summary = run_phase9_task5_closeout_refresh()
    if args.json:
        print(json.dumps(summary, indent=2, sort_keys=True))
    else:
        print(_render_text(summary))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
