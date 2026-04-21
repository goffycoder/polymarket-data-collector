from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from typing import Any

from database.db_manager import get_conn
from phase3.detector import PHASE3_DETECTOR_VERSION


@dataclass(slots=True)
class Phase3CandidateReport:
    start: str
    end: str
    detector_version: str
    total_candidates: int
    unique_markets: int
    hourly_counts: list[dict[str, Any]]
    rule_family_counts: list[dict[str, Any]]
    top_markets: list[dict[str, Any]]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def build_phase3_candidate_report(*, start: str, end: str) -> Phase3CandidateReport:
    conn = get_conn()
    try:
        total_row = conn.execute(
            """
            SELECT
                COUNT(*) AS total_candidates,
                COUNT(DISTINCT market_id) AS unique_markets
            FROM signal_candidates
            WHERE detector_version = ?
              AND trigger_time >= ?
              AND trigger_time < ?
            """,
            (PHASE3_DETECTOR_VERSION, start, end),
        ).fetchone()

        hourly_rows = conn.execute(
            """
            SELECT substr(trigger_time, 1, 13) || ':00:00' AS hour_bucket, COUNT(*) AS candidate_count
            FROM signal_candidates
            WHERE detector_version = ?
              AND trigger_time >= ?
              AND trigger_time < ?
            GROUP BY hour_bucket
            ORDER BY hour_bucket ASC
            """,
            (PHASE3_DETECTOR_VERSION, start, end),
        ).fetchall()

        market_rows = conn.execute(
            """
            SELECT market_id, COUNT(*) AS candidate_count, MAX(severity_score) AS max_severity_score
            FROM signal_candidates
            WHERE detector_version = ?
              AND trigger_time >= ?
              AND trigger_time < ?
            GROUP BY market_id
            ORDER BY candidate_count DESC, max_severity_score DESC, market_id ASC
            LIMIT 10
            """,
            (PHASE3_DETECTOR_VERSION, start, end),
        ).fetchall()

        candidate_rows = conn.execute(
            """
            SELECT triggering_rules
            FROM signal_candidates
            WHERE detector_version = ?
              AND trigger_time >= ?
              AND trigger_time < ?
            """,
            (PHASE3_DETECTOR_VERSION, start, end),
        ).fetchall()
    finally:
        conn.close()

    rule_counts: dict[str, int] = {}
    for row in candidate_rows:
        rules = json.loads(row["triggering_rules"]) if row["triggering_rules"] else []
        for rule in rules:
            key = str(rule)
            rule_counts[key] = rule_counts.get(key, 0) + 1

    return Phase3CandidateReport(
        start=start,
        end=end,
        detector_version=PHASE3_DETECTOR_VERSION,
        total_candidates=int(total_row["total_candidates"] or 0),
        unique_markets=int(total_row["unique_markets"] or 0),
        hourly_counts=[
            {
                "hour_bucket": row["hour_bucket"],
                "candidate_count": int(row["candidate_count"] or 0),
            }
            for row in hourly_rows
        ],
        rule_family_counts=[
            {"rule_family": rule_family, "candidate_count": count}
            for rule_family, count in sorted(rule_counts.items(), key=lambda item: (-item[1], item[0]))
        ],
        top_markets=[
            {
                "market_id": row["market_id"],
                "candidate_count": int(row["candidate_count"] or 0),
                "max_severity_score": float(row["max_severity_score"] or 0.0),
            }
            for row in market_rows
        ],
    )
