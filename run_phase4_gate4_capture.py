from __future__ import annotations

import argparse
import asyncio
import json
from pathlib import Path

from database.db_manager import apply_schema, get_conn
from phase4 import Phase4AlertWorker, Phase4EvidenceWorker, Phase4Repository
from phase4.timefmt import format_eastern
from validation.phase4_gate4_report import build_phase4_gate4_report


def _latest_alerts(limit: int) -> list[dict[str, str | None]]:
    conn = get_conn()
    try:
        rows = conn.execute(
            """
            SELECT alert_id, candidate_id, title, severity, alert_status, created_at
            FROM alerts
            ORDER BY created_at DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
    finally:
        conn.close()

    return [
        {
            "alert_id": row["alert_id"],
            "candidate_id": row["candidate_id"],
            "title": row["title"],
            "severity": row["severity"],
            "alert_status": row["alert_status"],
            "created_at": row["created_at"],
            "created_at_display": format_eastern(row["created_at"]),
        }
        for row in rows
    ]


async def _main() -> int:
    parser = argparse.ArgumentParser(
        description="Run one full Gate 4 capture: bootstrap, pipeline, latest alerts, and Gate 4 report."
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=10,
        help="Maximum number of candidates to process in evidence/alert passes.",
    )
    parser.add_argument(
        "--latest-alert-limit",
        type=int,
        default=5,
        help="Number of latest alerts to include in the summary output.",
    )
    parser.add_argument(
        "--output",
        default="reports/phase4_gate4_report.json",
        help="Path to write the Gate 4 JSON report.",
    )
    args = parser.parse_args()

    apply_schema()
    repository = Phase4Repository()
    repository.register_workflow_version(
        notes="Phase 4 Gate 4 capture initialized from the latest Phase 3 candidate engine."
    )

    bootstrap_summary = repository.bootstrap_summary().to_dict()
    pending_candidates = repository.pending_candidates(limit=max(0, args.limit))

    evidence_worker = Phase4EvidenceWorker(repository=repository)
    evidence_results = await evidence_worker.process_pending_candidates(limit=max(0, args.limit))

    alert_worker = Phase4AlertWorker(repository=repository)
    alert_results = alert_worker.process_pending_candidates(limit=max(0, args.limit))

    report_payload = build_phase4_gate4_report().to_dict()
    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(report_payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    payload = {
        "bootstrap_summary": bootstrap_summary,
        "pending_candidates_preview": pending_candidates,
        "evidence_results": evidence_results,
        "evidence_summary": evidence_worker.summary.to_dict(),
        "alert_results": alert_results,
        "alert_summary": alert_worker.summary.to_dict(),
        "latest_alerts": _latest_alerts(max(0, args.latest_alert_limit)),
        "gate4_assessment": report_payload["assessment"],
        "gate4_report_path": str(output_path),
    }
    print(json.dumps(payload, indent=2, sort_keys=True, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(_main()))
