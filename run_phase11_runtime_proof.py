from __future__ import annotations

import argparse
import asyncio
import json
import os
from collections import defaultdict
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from uuid import uuid4

from config.runtime_env import REPO_ROOT, load_runtime_env


SEED_START = "2026-04-20T05:00:00+00:00"
SEED_END = "2026-04-20T06:00:00+00:00"
SEED_SOURCE_MAP = {
    "phase10_task2_seed_prices": "prices",
    "phase10_task2_seed_trades": "trades",
}
TIMESTAMP_KEYS = {"captured_at", "trade_time"}
SYNTHETIC_MARKET_ID = "phase11_synthetic_market"


def _parse_iso(value: str | None) -> datetime | None:
    if not value:
        return None
    parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _iso(dt: datetime) -> str:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).isoformat()


def _hour_floor(dt: datetime) -> datetime:
    return dt.replace(minute=0, second=0, microsecond=0)


def _iter_hours(start: datetime, end: datetime) -> list[datetime]:
    current = _hour_floor(start)
    values: list[datetime] = []
    while current < end:
        values.append(current)
        current += timedelta(hours=1)
    return values


def _detector_partition_file(*, source_system: str, dt: datetime) -> Path:
    return (
        REPO_ROOT
        / "data"
        / "detector_input"
        / f"year={dt:%Y}"
        / f"month={dt:%m}"
        / f"day={dt:%d}"
        / f"hour={dt:%H}"
        / f"source_system={source_system}"
        / "events.ndjson"
    )


def _load_seed_rows(*, start: datetime, end: datetime) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for source_system in SEED_SOURCE_MAP:
        for hour in _iter_hours(start, end):
            path = _detector_partition_file(source_system=source_system, dt=hour)
            if not path.exists():
                continue
            with path.open("r", encoding="utf-8") as handle:
                for raw_line in handle:
                    line = raw_line.strip()
                    if not line:
                        continue
                    row = json.loads(line)
                    captured_at = _parse_iso(row.get("captured_at"))
                    if captured_at is None or not (start <= captured_at < end):
                        continue
                    rows.append(row)
    rows.sort(key=lambda row: (row.get("captured_at") or "", row.get("ordering_key") or "", row.get("envelope_id") or ""))
    return rows


def _build_synthetic_seed_rows(*, start: datetime) -> list[dict[str, Any]]:
    """Create a tiny labeled detector-input window when archived seeds are missing locally."""
    price_source = "phase11_synthetic_seed_prices"
    trade_source = "phase11_synthetic_seed_trades"
    points = [
        (
            price_source,
            start + timedelta(minutes=54),
            "prices_batch",
            {
                "market_snapshots": [
                    {
                        "market_id": SYNTHETIC_MARKET_ID,
                        "captured_at": _iso(start + timedelta(minutes=54)),
                        "yes_price": 0.25,
                        "best_bid": 0.24,
                        "best_ask": 0.26,
                        "spread": 0.02,
                        "source": "phase11_synthetic_seed",
                    }
                ]
            },
        ),
        (
            price_source,
            start + timedelta(minutes=57),
            "prices_batch",
            {
                "market_snapshots": [
                    {
                        "market_id": SYNTHETIC_MARKET_ID,
                        "captured_at": _iso(start + timedelta(minutes=57)),
                        "yes_price": 0.58,
                        "best_bid": 0.57,
                        "best_ask": 0.59,
                        "spread": 0.02,
                        "source": "phase11_synthetic_seed",
                    }
                ]
            },
        ),
        (
            trade_source,
            start + timedelta(minutes=57, seconds=10),
            "recent_trades_page",
            {
                "trades": [
                    {
                        "trade_id": "phase11_synthetic_trade_1",
                        "market_id": SYNTHETIC_MARKET_ID,
                        "trade_time": _iso(start + timedelta(minutes=57, seconds=10)),
                        "side": "BUY",
                        "outcome_side": "YES",
                        "price": 0.58,
                        "size": 250.0,
                        "usdc_notional": 145.0,
                        "proxy_wallet": "0xphase110001",
                    },
                    {
                        "trade_id": "phase11_synthetic_trade_2",
                        "market_id": SYNTHETIC_MARKET_ID,
                        "trade_time": _iso(start + timedelta(minutes=57, seconds=20)),
                        "side": "BUY",
                        "outcome_side": "YES",
                        "price": 0.59,
                        "size": 250.0,
                        "usdc_notional": 147.5,
                        "proxy_wallet": "0xphase110002",
                    },
                    {
                        "trade_id": "phase11_synthetic_trade_3",
                        "market_id": SYNTHETIC_MARKET_ID,
                        "trade_time": _iso(start + timedelta(minutes=57, seconds=30)),
                        "side": "BUY",
                        "outcome_side": "YES",
                        "price": 0.6,
                        "size": 250.0,
                        "usdc_notional": 150.0,
                        "proxy_wallet": "0xphase110003",
                    },
                ]
            },
        ),
    ]
    rows: list[dict[str, Any]] = []
    for idx, (source_system, captured_at, entity_type, payload) in enumerate(points, start=1):
        rows.append(
            {
                "captured_at": _iso(captured_at),
                "entity_type": entity_type,
                "envelope_id": uuid4().hex,
                "ordering_key": f"{source_system}:{idx:06d}",
                "payload": payload,
                "raw_partition_path": f"phase11/synthetic_seed/{source_system}",
                "schema_version": "normalized_envelope.v2",
                "source_system": source_system,
            }
        )
    return rows


def _shift_timestamps(value: Any, *, delta: timedelta) -> Any:
    if isinstance(value, dict):
        shifted: dict[str, Any] = {}
        for key, item in value.items():
            if key in TIMESTAMP_KEYS and isinstance(item, str):
                parsed = _parse_iso(item)
                shifted[key] = _iso(parsed + delta) if parsed is not None else item
            else:
                shifted[key] = _shift_timestamps(item, delta=delta)
        return shifted
    if isinstance(value, list):
        return [_shift_timestamps(item, delta=delta) for item in value]
    return value


def _materialize_proof_rows(
    *,
    seed_rows: list[dict[str, Any]],
    proof_end: datetime,
    run_id: str,
) -> tuple[list[dict[str, Any]], dict[str, str]]:
    captured_times = [_parse_iso(row.get("captured_at")) for row in seed_rows]
    valid_times = [item for item in captured_times if item is not None]
    if not valid_times:
        raise RuntimeError("No detector-input seed rows were available for the requested proof window.")

    historical_end = max(valid_times)
    delta = proof_end - historical_end
    source_mapping = {
        source_system: f"phase11_runtime_proof_{run_id}_{SEED_SOURCE_MAP.get(source_system, source_system)}"
        for source_system in sorted({str(row.get("source_system") or "unknown") for row in seed_rows})
    }

    proof_rows: list[dict[str, Any]] = []
    for idx, row in enumerate(seed_rows, start=1):
        proof_row = _shift_timestamps(deepcopy(row), delta=delta)
        original_source = str(row.get("source_system") or "")
        proof_source = source_mapping.get(original_source, f"phase11_runtime_proof_{run_id}_misc")
        proof_row["source_system"] = proof_source
        proof_row["envelope_id"] = uuid4().hex
        proof_row["ordering_key"] = f"{proof_source}:{idx:06d}"
        proof_row["raw_partition_path"] = f"phase11/runtime_proof/{run_id}/{original_source or 'unknown'}"
        proof_rows.append(proof_row)
    return proof_rows, source_mapping


def _write_proof_partitions(*, proof_rows: list[dict[str, Any]]) -> dict[str, list[str]]:
    grouped: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in proof_rows:
        captured_at = _parse_iso(row.get("captured_at"))
        if captured_at is None:
            continue
        source_system = str(row.get("source_system") or "")
        path = _detector_partition_file(source_system=source_system, dt=captured_at)
        grouped[(source_system, str(path))].append(row)

    written_paths: dict[str, list[str]] = defaultdict(list)
    for (source_system, path_text), rows in grouped.items():
        path = Path(path_text)
        path.parent.mkdir(parents=True, exist_ok=True)
        rows.sort(key=lambda row: (row.get("captured_at") or "", row.get("ordering_key") or ""))
        with path.open("w", encoding="utf-8") as handle:
            for row in rows:
                handle.write(json.dumps(row, sort_keys=True) + "\n")
        written_paths[source_system].append(str(path))
    return {key: sorted(values) for key, values in written_paths.items()}


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run a bounded Phase 11 proof that exercises abnormal-activity detection and flagging end to end."
    )
    parser.add_argument(
        "--env-file",
        default="",
        help="Explicit runtime env file. Defaults to .env.runtime, then legacy .env, then shell-only.",
    )
    parser.add_argument("--seed-start", default=SEED_START, help="Historical seed window start timestamp.")
    parser.add_argument("--seed-end", default=SEED_END, help="Historical seed window end timestamp.")
    parser.add_argument(
        "--output",
        default="reports/phase11/runtime_proof_result.json",
        help="JSON output path.",
    )
    parser.add_argument(
        "--recent-hours",
        type=int,
        default=24,
        help="How many recent hours to inspect after the proof run.",
    )
    parser.add_argument("--json", action="store_true", help="Emit machine-readable JSON.")
    return parser


async def _main() -> int:
    args = build_parser().parse_args()
    env_result = load_runtime_env(args.env_file or None, override=True)
    os.environ["POLYMARKET_RUNTIME_LAUNCHED"] = "1"

    from config import settings
    from database.db_manager import apply_schema
    from phase3.detector import Phase3Repository
    from phase3.live_runner import Phase3LiveRunner
    from phase3.state_store import SQLiteStateStore
    from phase4 import NoopEvidenceProvider, Phase4AlertWorker, Phase4EvidenceWorker, Phase4Repository
    from phase4.alerts import NoopDeliveryChannel

    seed_start = _parse_iso(args.seed_start)
    seed_end = _parse_iso(args.seed_end)
    if seed_start is None or seed_end is None or seed_end <= seed_start:
        raise SystemExit("A valid seed window is required and seed-end must be later than seed-start.")

    apply_schema()

    now = datetime.now(timezone.utc)
    run_id = now.strftime("%Y%m%dT%H%M%S")
    proof_end = now - timedelta(minutes=1)
    seed_rows = _load_seed_rows(start=seed_start, end=seed_end)
    seed_mode = "archived_detector_input"
    if not seed_rows:
        seed_rows = _build_synthetic_seed_rows(start=seed_start)
        seed_mode = "synthetic_detector_input_fallback"
    proof_rows, source_mapping = _materialize_proof_rows(seed_rows=seed_rows, proof_end=proof_end, run_id=run_id)
    written_paths = _write_proof_partitions(proof_rows=proof_rows)

    proof_state_path = REPO_ROOT / "database" / f"phase11_runtime_proof_state_{run_id}.db"
    store = SQLiteStateStore(proof_state_path)
    phase3_repository = Phase3Repository()
    phase3_repository.register_detector_version(
        backend_name=store.backend_name,
        notes=(
            "Phase 11 bounded runtime proof over shifted seeded abnormal-activity detector-input "
            f"window (run_id={run_id}, state_path={proof_state_path})."
        ),
    )

    phase3_runner = Phase3LiveRunner(
        store=store,
        repository=phase3_repository,
        source_systems=sorted(written_paths.keys()),
        poll_seconds=max(1.0, float(settings.PHASE3_POLL_SECONDS)),
    )

    try:
        phase3_processed = await phase3_runner.run_once()
    finally:
        await store.aclose()

    phase4_repository = Phase4Repository()
    phase4_repository.register_workflow_version(
        notes=f"Phase 11 bounded runtime proof run (run_id={run_id}) using noop evidence and local noop delivery."
    )
    evidence_worker = Phase4EvidenceWorker(
        repository=phase4_repository,
        providers=[
            NoopEvidenceProvider(name="noop_news", query_type="web_news"),
            NoopEvidenceProvider(name="noop_social", query_type="social"),
        ],
    )
    evidence_results = await evidence_worker.process_pending_candidates(limit=5)
    alert_worker = Phase4AlertWorker(
        repository=phase4_repository,
        channels=[NoopDeliveryChannel(name="local_noop")],
    )
    alert_results = alert_worker.process_pending_candidates(limit=5)

    phase3_status = phase3_repository.live_runtime_status(recent_hours=args.recent_hours)
    phase4_status = phase4_repository.live_runtime_status(recent_hours=args.recent_hours)

    payload = {
        "generated_at": _iso(datetime.now(timezone.utc)),
        "run_id": run_id,
        "env_loading": {
            "primary_env_file": None if env_result.env_file is None else str(env_result.env_file),
            "primary_env_source": env_result.source,
            "secret_env_file": None if env_result.secret_env_file is None else str(env_result.secret_env_file),
            "secret_env_source": env_result.secret_source,
            "warnings": list(env_result.warnings),
        },
        "historical_seed_window": {
            "start": _iso(seed_start),
            "end": _iso(seed_end),
            "seed_mode": seed_mode,
            "source_systems": sorted({str(row.get("source_system") or "") for row in seed_rows}),
            "seed_row_count": len(seed_rows),
        },
        "proof_window": {
            "approximate_end": _iso(proof_end),
            "proof_source_mapping": source_mapping,
            "written_detector_input_partitions": written_paths,
            "proof_state_path": str(proof_state_path),
        },
        "phase3": {
            "processed_envelopes": phase3_processed,
            "runner_summary": phase3_runner.summary.to_dict(),
            "detector_summary": phase3_runner.detector.summary.to_dict(),
            "runtime_status": phase3_status,
            "detector_registration": phase3_repository.load_detector_registration(),
        },
        "phase4": {
            "evidence_results": evidence_results,
            "evidence_summary": evidence_worker.summary.to_dict(),
            "alert_results": alert_results,
            "alert_summary": alert_worker.summary.to_dict(),
            "runtime_status": phase4_status,
            "workflow_registration": phase4_repository.load_workflow_registration(),
        },
        "assessment": {
            "phase3_recent_candidates": int(phase3_status.get("candidate_count_recent") or 0),
            "phase3_recent_checkpoints": int(phase3_status.get("checkpoint_count_recent") or 0),
            "phase4_recent_alerts": int(phase4_status.get("alert_count_recent") or 0),
            "phase4_recent_deliveries": int(phase4_status.get("delivery_attempt_count_recent") or 0),
            "status": (
                "proof_succeeded"
                if int(phase3_status.get("candidate_count_recent") or 0) > 0
                and int(phase3_status.get("checkpoint_count_recent") or 0) > 0
                and int(phase4_status.get("evidence_query_count_recent") or 0) > 0
                and int(phase4_status.get("alert_count_recent") or 0) > 0
                and int(phase4_status.get("delivery_attempt_count_recent") or 0) > 0
                else "proof_incomplete"
            ),
        },
    }

    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    if args.json:
        print(json.dumps(payload, indent=2, sort_keys=True))
    else:
        print(json.dumps(payload, indent=2, sort_keys=True))
        print(f"\nReport: {output_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(_main()))
