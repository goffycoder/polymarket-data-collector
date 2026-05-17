from __future__ import annotations

import argparse
import json
import os
import signal
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from config.runtime_env import load_runtime_env


REPO_ROOT = Path(__file__).resolve().parent
REPORT_DIR = REPO_ROOT / "reports" / "phase12"


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _directory_size(path: Path) -> int:
    if not path.exists():
        return 0
    total = 0
    for item in path.rglob("*"):
        if item.is_file():
            total += int(item.stat().st_size)
    return total


def _db_counts() -> dict[str, int | None]:
    from database.db_manager import get_conn

    tables = [
        "raw_archive_manifests",
        "detector_input_manifests",
        "trades",
        "snapshots",
        "order_book_snapshots",
        "signal_candidates",
        "alerts",
        "alert_delivery_attempts",
        "evidence_snapshots",
        "shadow_model_scores",
    ]
    conn = get_conn()
    try:
        counts: dict[str, int | None] = {}
        for table in tables:
            try:
                row = conn.execute(f"SELECT COUNT(*) AS count FROM {table}").fetchone()
                counts[table] = int(row["count"] if "count" in row.keys() else row[0])
            except Exception:
                counts[table] = None
        return counts
    finally:
        conn.close()


def _snapshot_state() -> dict[str, Any]:
    from config import settings

    raw_root = Path(str(settings.RAW_ARCHIVE_ROOT))
    detector_root = Path(str(settings.DETECTOR_INPUT_ROOT))
    return {
        "captured_at": _utc_now(),
        "archive_roots": {
            "raw_archive_root": str(raw_root),
            "detector_input_root": str(detector_root),
            "archive_root_readonly": bool(settings.ARCHIVE_ROOT_READONLY),
        },
        "archive_sizes": {
            "raw_archive_bytes": _directory_size(raw_root),
            "detector_input_bytes": _directory_size(detector_root),
        },
        "db_counts": _db_counts(),
    }


def _diff_counts(before: dict[str, int | None], after: dict[str, int | None]) -> dict[str, int | None]:
    deltas: dict[str, int | None] = {}
    for key in sorted(set(before) | set(after)):
        left = before.get(key)
        right = after.get(key)
        deltas[key] = None if left is None or right is None else right - left
    return deltas


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run a bounded Phase 12 organic live runtime smoke.")
    parser.add_argument("--env-file", default=".env.runtime", help="Runtime env file.")
    parser.add_argument("--duration-seconds", type=int, default=600, help="Smoke duration before graceful stop.")
    parser.add_argument(
        "--output",
        default=str(REPORT_DIR / "organic_runtime_smoke.json"),
        help="Output report path.",
    )
    parser.add_argument("--json", action="store_true", help="Emit JSON report to stdout.")
    return parser


def main() -> int:
    args = build_parser().parse_args()
    env_result = load_runtime_env(args.env_file or None, override=True)

    from database.db_manager import apply_schema

    apply_schema()
    before = _snapshot_state()
    env = os.environ.copy()
    env["POLYMARKET_RUNTIME_LAUNCHED"] = "1"
    command = [
        sys.executable,
        str(REPO_ROOT / "run_runtime.py"),
        "--env-file",
        args.env_file,
    ]
    started_at = _utc_now()
    process = subprocess.Popen(command, cwd=str(REPO_ROOT), env=env, start_new_session=True)
    status = "completed"
    exit_code: int | None = None
    try:
        deadline = time.monotonic() + max(1, args.duration_seconds)
        while time.monotonic() < deadline:
            exit_code = process.poll()
            if exit_code is not None:
                status = "runtime_exited_early"
                break
            time.sleep(1)
        if process.poll() is None:
            status = "terminated_after_duration"
            os.killpg(process.pid, signal.SIGTERM)
            try:
                exit_code = process.wait(timeout=45)
            except subprocess.TimeoutExpired:
                status = "forced_kill_after_timeout"
                os.killpg(process.pid, signal.SIGKILL)
                exit_code = process.wait(timeout=30)
    finally:
        after = _snapshot_state()

    finished_at = _utc_now()
    archive_size_delta = {
        "raw_archive_bytes": (
            int(after["archive_sizes"]["raw_archive_bytes"])
            - int(before["archive_sizes"]["raw_archive_bytes"])
        ),
        "detector_input_bytes": (
            int(after["archive_sizes"]["detector_input_bytes"])
            - int(before["archive_sizes"]["detector_input_bytes"])
        ),
    }
    count_delta = _diff_counts(before["db_counts"], after["db_counts"])
    payload = {
        "status": status,
        "started_at": started_at,
        "finished_at": finished_at,
        "duration_seconds_requested": args.duration_seconds,
        "runtime_exit_code": exit_code,
        "env_file": str(env_result.env_file) if env_result.env_file is not None else None,
        "before": before,
        "after": after,
        "archive_size_delta": archive_size_delta,
        "db_count_delta": count_delta,
        "acceptance": {
            "raw_archive_bytes_increased": archive_size_delta["raw_archive_bytes"] > 0,
            "detector_input_bytes_increased": archive_size_delta["detector_input_bytes"] > 0,
            "trade_rows_increased": (count_delta.get("trades") or 0) > 0,
            "runtime_completed_gracefully": status in {"completed", "terminated_after_duration"}
            and exit_code in {0, -15, 143},
        },
        "notes": [
            "This bounded smoke intentionally terminates the canonical runtime after the requested duration.",
            "A no-candidate result is acceptable for organic smoke if fresh raw/detector-input artifacts are written.",
        ],
    }
    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    if args.json:
        print(json.dumps(payload, indent=2, sort_keys=True))
    else:
        print(f"Live smoke status: {status} exit={exit_code}")
        print(f"Raw archive bytes delta: {archive_size_delta['raw_archive_bytes']}")
        print(f"Detector-input bytes delta: {archive_size_delta['detector_input_bytes']}")
        print(f"Report: {output_path}")
    return 0 if payload["acceptance"]["runtime_completed_gracefully"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
