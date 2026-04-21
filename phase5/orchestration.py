from __future__ import annotations

import json
import math
import subprocess
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from config.settings import PHASE5_BACKFILL_MARKET_LIMIT, PHASE5_BACKFILL_REQUEST_LIMIT
from database.db_manager import get_conn
from utils.logger import get_logger


REPO_ROOT = Path(__file__).resolve().parent.parent
log = get_logger("phase5_orchestration")


def _parse_iso(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _iso_now() -> str:
    return datetime.now(timezone.utc).isoformat()


@dataclass(slots=True)
class Phase5BackfillDispatchItem:
    backfill_request_id: str
    source_system: str
    request_status: str
    strategy: str
    command_preview: str | None
    executed: bool
    output_path: str | None
    notes: str | None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class Phase5BackfillDispatchSummary:
    request_count: int
    execute_supported: bool
    dispatch_items: list[dict[str, Any]]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def _load_requests(limit: int) -> list[dict[str, Any]]:
    conn = get_conn()
    try:
        rows = conn.execute(
            """
            SELECT
                backfill_request_id,
                source_system,
                start_time,
                end_time,
                request_status,
                priority,
                requested_by,
                reason,
                request_payload,
                output_path,
                notes,
                created_at
            FROM backfill_requests
            WHERE request_status IN ('requested', 'planned')
            ORDER BY
                CASE priority
                    WHEN 'high' THEN 0
                    WHEN 'normal' THEN 1
                    ELSE 2
                END,
                created_at ASC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
    finally:
        conn.close()
    return [
        {
            "backfill_request_id": row["backfill_request_id"],
            "source_system": row["source_system"],
            "start_time": row["start_time"],
            "end_time": row["end_time"],
            "request_status": row["request_status"],
            "priority": row["priority"],
            "requested_by": row["requested_by"],
            "reason": row["reason"],
            "request_payload": json.loads(row["request_payload"] or "{}"),
            "output_path": row["output_path"],
            "notes": row["notes"],
            "created_at": row["created_at"],
        }
        for row in rows
    ]


def _update_request(
    *,
    backfill_request_id: str,
    request_status: str,
    output_path: str | None,
    notes: str | None,
    completed: bool = False,
) -> None:
    conn = get_conn()
    try:
        conn.execute(
            """
            UPDATE backfill_requests
            SET
                request_status = ?,
                output_path = COALESCE(?, output_path),
                notes = ?,
                completed_at = ?
            WHERE backfill_request_id = ?
            """,
            (
                request_status,
                output_path,
                notes,
                _iso_now() if completed else None,
                backfill_request_id,
            ),
        )
        conn.commit()
    finally:
        conn.close()


def _plan_request(request: dict[str, Any]) -> dict[str, Any]:
    source_system = str(request["source_system"])
    start_dt = _parse_iso(str(request["start_time"]))
    now = datetime.now(timezone.utc)
    days = max(1, math.ceil((now - start_dt).total_seconds() / 86400))
    market_limit = PHASE5_BACKFILL_MARKET_LIMIT if PHASE5_BACKFILL_MARKET_LIMIT > 0 else None

    if source_system == "data_api_trades_backfill":
        command = [
            str(REPO_ROOT / "venv" / "bin" / "python"),
            "collectors/backfill.py",
            "--days",
            str(days),
        ]
        if market_limit:
            command.extend(["--limit", str(market_limit)])
        return {
            "strategy": "collectors_backfill_days",
            "supported": True,
            "command": command,
            "command_preview": " ".join(command),
            "notes": (
                "This uses the existing broad since-start trades backfill path. "
                "It is suitable for recent degraded windows but not exact bounded historical reconstruction."
            ),
        }

    return {
        "strategy": "manual_required",
        "supported": False,
        "command": None,
        "command_preview": None,
        "notes": f"No automated backfill executor exists yet for source_system={source_system}.",
    }


def dispatch_phase5_backfill_requests(
    *,
    limit: int | None = None,
    execute_supported: bool = False,
) -> Phase5BackfillDispatchSummary:
    limit = limit or PHASE5_BACKFILL_REQUEST_LIMIT
    requests = _load_requests(limit=max(1, limit))
    dispatch_items: list[dict[str, Any]] = []

    for request in requests:
        plan = _plan_request(request)
        request_id = str(request["backfill_request_id"])
        source_system = str(request["source_system"])
        executed = False
        output_path = request.get("output_path")
        notes = plan["notes"]
        next_status = "planned"

        if not plan["supported"]:
            next_status = "manual_required"
            _update_request(
                backfill_request_id=request_id,
                request_status=next_status,
                output_path=output_path,
                notes=notes,
                completed=False,
            )
        elif execute_supported:
            command = plan["command"]
            assert command is not None
            try:
                completed = subprocess.run(
                    command,
                    cwd=REPO_ROOT,
                    capture_output=True,
                    text=True,
                    check=True,
                )
                executed = True
                next_status = "completed"
                artifact_dir = REPO_ROOT / "reports" / "phase5" / "backfill_dispatch"
                artifact_dir.mkdir(parents=True, exist_ok=True)
                artifact_path = artifact_dir / f"{request_id}.log"
                artifact_path.write_text(
                    completed.stdout + ("\n" + completed.stderr if completed.stderr else ""),
                    encoding="utf-8",
                )
                output_path = str(artifact_path.relative_to(REPO_ROOT))
                notes = json.dumps(
                    {
                        "strategy": plan["strategy"],
                        "command_preview": plan["command_preview"],
                        "message": plan["notes"],
                    },
                    sort_keys=True,
                )
                _update_request(
                    backfill_request_id=request_id,
                    request_status=next_status,
                    output_path=output_path,
                    notes=notes,
                    completed=True,
                )
            except subprocess.CalledProcessError as exc:
                next_status = "failed"
                artifact_dir = REPO_ROOT / "reports" / "phase5" / "backfill_dispatch"
                artifact_dir.mkdir(parents=True, exist_ok=True)
                artifact_path = artifact_dir / f"{request_id}.log"
                artifact_path.write_text(
                    (exc.stdout or "") + ("\n" + exc.stderr if exc.stderr else ""),
                    encoding="utf-8",
                )
                output_path = str(artifact_path.relative_to(REPO_ROOT))
                notes = json.dumps(
                    {
                        "strategy": plan["strategy"],
                        "command_preview": plan["command_preview"],
                        "error": str(exc),
                    },
                    sort_keys=True,
                )
                _update_request(
                    backfill_request_id=request_id,
                    request_status=next_status,
                    output_path=output_path,
                    notes=notes,
                    completed=True,
                )
                log.warning("Backfill dispatch failed for %s (%s)", request_id, source_system)
        else:
            next_status = "planned"
            _update_request(
                backfill_request_id=request_id,
                request_status=next_status,
                output_path=output_path,
                notes=notes,
                completed=False,
            )

        dispatch_items.append(
            Phase5BackfillDispatchItem(
                backfill_request_id=request_id,
                source_system=source_system,
                request_status=next_status,
                strategy=str(plan["strategy"]),
                command_preview=plan["command_preview"],
                executed=executed,
                output_path=output_path,
                notes=notes,
            ).to_dict()
        )

    return Phase5BackfillDispatchSummary(
        request_count=len(dispatch_items),
        execute_supported=execute_supported,
        dispatch_items=dispatch_items,
    )
