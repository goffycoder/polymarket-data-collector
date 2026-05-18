from __future__ import annotations

import argparse
import json
from collections import Counter
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from config.runtime_env import load_runtime_env
from database.db_manager import apply_schema, get_conn


REPORT_DIR = Path("reports/phase12")
DEFAULT_JSON = REPORT_DIR / "ml_live_alert_review.json"
DEFAULT_MARKDOWN = REPORT_DIR / "ml_live_alert_review.md"


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _iso(value: datetime | None) -> str | None:
    if value is None:
        return None
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).isoformat()


def _parse_dt(value: Any) -> datetime | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = f"{text[:-1]}+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        try:
            parsed = datetime.strptime(text, "%Y-%m-%d %H:%M:%S")
        except ValueError:
            return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _float(value: Any) -> float | None:
    try:
        if value is None:
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _json_loads(value: Any, default: Any) -> Any:
    if value is None:
        return default
    if isinstance(value, (dict, list)):
        return value
    try:
        return json.loads(str(value))
    except (TypeError, ValueError, json.JSONDecodeError):
        return default


def _rows(sql: str, params: tuple[Any, ...] = ()) -> list[dict[str, Any]]:
    conn = get_conn()
    try:
        return [dict(row) for row in conn.execute(sql, params).fetchall()]
    finally:
        conn.close()


def _table_count(table: str) -> int:
    conn = get_conn()
    try:
        row = conn.execute(f"SELECT COUNT(*) AS n FROM {table}").fetchone()
        return int(row["n"] or 0)
    finally:
        conn.close()


def _latest_by(rows: list[dict[str, Any]], key: str, time_fields: tuple[str, ...]) -> dict[str, dict[str, Any]]:
    latest: dict[str, dict[str, Any]] = {}
    latest_time: dict[str, datetime] = {}
    for row in rows:
        row_key = str(row.get(key) or "")
        if not row_key:
            continue
        observed = None
        for field in time_fields:
            observed = _parse_dt(row.get(field))
            if observed is not None:
                break
        if row_key not in latest or (observed or datetime.min.replace(tzinfo=timezone.utc)) >= latest_time[row_key]:
            latest[row_key] = row
            latest_time[row_key] = observed or datetime.min.replace(tzinfo=timezone.utc)
    return latest


def _coalesced_time(row: dict[str, Any], fields: tuple[str, ...]) -> datetime | None:
    for field in fields:
        parsed = _parse_dt(row.get(field))
        if parsed is not None:
            return parsed
    return None


def _polymarket_url(event_slug: str | None, market_slug: str | None) -> str | None:
    slug = (event_slug or market_slug or "").strip()
    if not slug:
        return None
    return f"https://polymarket.com/event/{slug}"


def build_ml_live_alert_review(*, lookback_hours: float, max_examples: int) -> dict[str, Any]:
    apply_schema()
    now = _utc_now()
    cutoff = now - timedelta(hours=max(0.1, lookback_hours))

    alerts = _rows("SELECT * FROM alerts")
    candidates = {
        str(row["candidate_id"]): row
        for row in _rows(
            """
            SELECT
                sc.*,
                m.question AS market_question,
                m.slug AS market_slug,
                m.condition_id AS market_condition_id,
                e.title AS event_title,
                e.slug AS event_slug
            FROM signal_candidates sc
            LEFT JOIN markets m ON m.market_id = sc.market_id
            LEFT JOIN events e ON e.event_id = sc.event_id
            """
        )
    }
    scores = _latest_by(
        _rows("SELECT * FROM shadow_model_scores"),
        "candidate_id",
        ("scored_at", "created_at"),
    )
    evidence = _latest_by(
        _rows("SELECT * FROM evidence_snapshots"),
        "candidate_id",
        ("snapshot_time", "created_at"),
    )
    delivery_attempts = _rows("SELECT * FROM alert_delivery_attempts")
    deliveries_by_alert: dict[str, list[dict[str, Any]]] = {}
    for attempt in delivery_attempts:
        deliveries_by_alert.setdefault(str(attempt.get("alert_id")), []).append(attempt)

    reviewed_alerts: list[dict[str, Any]] = []
    for alert in alerts:
        candidate_id = str(alert.get("candidate_id") or "")
        candidate = candidates.get(candidate_id, {})
        relevant_time = _coalesced_time(
            {**candidate, **alert},
            ("updated_at", "created_at", "first_delivery_at", "last_delivery_at", "trigger_time"),
        )
        if relevant_time is not None and relevant_time < cutoff:
            continue

        rendered = _json_loads(alert.get("rendered_payload"), {})
        score = scores.get(candidate_id)
        score_value = _float((score or {}).get("score_value"))
        candidate_evidence = evidence.get(candidate_id)
        attempts = deliveries_by_alert.get(str(alert.get("alert_id")), [])
        delivered_attempts = [row for row in attempts if row.get("delivery_status") == "sent"]
        failed_attempts = [row for row in attempts if row.get("delivery_status") == "error"]
        triggering_rules = _json_loads(candidate.get("triggering_rules"), [])
        feature_snapshot = _json_loads(candidate.get("feature_snapshot"), {})
        event_slug = candidate.get("event_slug") or rendered.get("event_slug")
        market_slug = candidate.get("market_slug") or rendered.get("market_slug")

        reviewed_alerts.append(
            {
                "alert_id": alert.get("alert_id"),
                "candidate_id": candidate_id,
                "title": alert.get("title")
                or rendered.get("title")
                or candidate.get("event_title")
                or candidate.get("market_question"),
                "market_id": candidate.get("market_id"),
                "market_url": rendered.get("market_url") or _polymarket_url(event_slug, market_slug),
                "severity": alert.get("severity"),
                "alert_status": alert.get("alert_status"),
                "trigger_time": candidate.get("trigger_time"),
                "alert_updated_at": alert.get("updated_at"),
                "triggering_rules": triggering_rules,
                "severity_score": _float(candidate.get("severity_score")),
                "public_evidence_state": (candidate_evidence or {}).get("evidence_state")
                or rendered.get("public_evidence_state"),
                "confidence_modifier": _float((candidate_evidence or {}).get("confidence_modifier")),
                "delivery": {
                    "attempt_count": len(attempts),
                    "sent_count": len(delivered_attempts),
                    "error_count": len(failed_attempts),
                    "channels": sorted(
                        {
                            str(row.get("delivery_channel"))
                            for row in attempts
                            if row.get("delivery_channel")
                        }
                    ),
                },
                "ml_shadow": {
                    "state": "scored" if score else "missing",
                    "model_version": (score or {}).get("model_version"),
                    "score_label": (score or {}).get("score_label"),
                    "score_value": score_value,
                    "scored_at": (score or {}).get("scored_at"),
                    "feature_schema_version": (score or {}).get("feature_schema_version"),
                },
                "feature_snapshot_keys": sorted(feature_snapshot.keys())[:20]
                if isinstance(feature_snapshot, dict)
                else [],
            }
        )

    reviewed_alerts.sort(
        key=lambda row: (
            row["ml_shadow"]["score_value"] is not None,
            row["ml_shadow"]["score_value"] or -1.0,
            row.get("alert_updated_at") or "",
        ),
        reverse=True,
    )

    scored = [row for row in reviewed_alerts if row["ml_shadow"]["state"] == "scored"]
    score_values = [
        float(row["ml_shadow"]["score_value"])
        for row in scored
        if row["ml_shadow"]["score_value"] is not None
    ]
    delivered = [row for row in reviewed_alerts if row["delivery"]["sent_count"] > 0]
    evidence_states = Counter(str(row.get("public_evidence_state") or "unknown") for row in reviewed_alerts)
    severity_counts = Counter(str(row.get("severity") or "unknown") for row in reviewed_alerts)
    label_counts = Counter(str(row["ml_shadow"].get("score_label") or "missing") for row in reviewed_alerts)
    model_counts = Counter(str(row["ml_shadow"].get("model_version") or "missing") for row in reviewed_alerts)

    distinct_scores = sorted({round(value, 6) for value in score_values})
    coverage = (len(scored) / len(reviewed_alerts)) if reviewed_alerts else 0.0
    delivered_with_score = [
        row for row in delivered if row["ml_shadow"]["state"] == "scored"
    ]

    promotion_readiness = "not_ready"
    readiness_reasons: list[str] = []
    if not reviewed_alerts:
        readiness_reasons.append("No alerts in the selected lookback window.")
    if coverage < 0.9:
        readiness_reasons.append(
            f"Shadow-score coverage is {coverage:.1%}; promotion needs near-complete coverage."
        )
    if len(score_values) < 25:
        readiness_reasons.append(
            f"Only {len(score_values)} scored alert examples in scope; keep gathering live data."
        )
    if len(distinct_scores) <= 2:
        readiness_reasons.append(
            "Shadow scores are nearly flat, so the model is not yet separating alert quality."
        )
    if delivered and len(delivered_with_score) < len(delivered):
        readiness_reasons.append("Some delivered alerts do not have shadow scores.")
    if not readiness_reasons:
        promotion_readiness = "review_for_priority_only"
        readiness_reasons.append(
            "Coverage and variation are sufficient for a human review of ML-assisted priority, not autonomous suppression."
        )

    summary = {
        "lookback_hours": lookback_hours,
        "cutoff_at": _iso(cutoff),
        "generated_at": _iso(now),
        "database_counts": {
            "alerts": _table_count("alerts"),
            "signal_candidates": _table_count("signal_candidates"),
            "shadow_model_scores": _table_count("shadow_model_scores"),
            "alert_delivery_attempts": _table_count("alert_delivery_attempts"),
        },
        "alerts_reviewed": len(reviewed_alerts),
        "alerts_scored_by_ml": len(scored),
        "ml_score_coverage": round(coverage, 4),
        "delivered_alerts_reviewed": len(delivered),
        "delivered_alerts_with_ml_score": len(delivered_with_score),
        "severity_counts": dict(sorted(severity_counts.items())),
        "evidence_state_counts": dict(sorted(evidence_states.items())),
        "ml_label_counts": dict(sorted(label_counts.items())),
        "ml_model_counts": dict(sorted(model_counts.items())),
        "ml_score_summary": {
            "count": len(score_values),
            "min": min(score_values) if score_values else None,
            "max": max(score_values) if score_values else None,
            "distinct_values": distinct_scores[:25],
            "distinct_value_count": len(distinct_scores),
        },
        "promotion_readiness": promotion_readiness,
        "readiness_reasons": readiness_reasons,
    }

    return {
        "contract_version": "phase12_ml_live_alert_review_v1",
        "summary": summary,
        "top_scored_alerts": reviewed_alerts[:max_examples],
        "recent_delivered_alerts": delivered[:max_examples],
        "missing_ml_score_alerts": [
            row for row in reviewed_alerts if row["ml_shadow"]["state"] != "scored"
        ][:max_examples],
    }


def render_markdown(report: dict[str, Any]) -> str:
    summary = report["summary"]
    lines = [
        "# Phase 12 ML Live Alert Review",
        "",
        f"- Generated at: `{summary['generated_at']}`",
        f"- Lookback hours: `{summary['lookback_hours']}`",
        f"- Alerts reviewed: `{summary['alerts_reviewed']}`",
        f"- ML-scored alerts: `{summary['alerts_scored_by_ml']}`",
        f"- ML score coverage: `{summary['ml_score_coverage']:.1%}`",
        f"- Delivered alerts reviewed: `{summary['delivered_alerts_reviewed']}`",
        f"- Delivered alerts with ML score: `{summary['delivered_alerts_with_ml_score']}`",
        f"- Promotion readiness: `{summary['promotion_readiness']}`",
        "",
        "## Readiness Notes",
        "",
    ]
    for reason in summary["readiness_reasons"]:
        lines.append(f"- {reason}")

    lines.extend(
        [
            "",
            "## Distributions",
            "",
            f"- Severity: `{json.dumps(summary['severity_counts'], sort_keys=True)}`",
            f"- Evidence state: `{json.dumps(summary['evidence_state_counts'], sort_keys=True)}`",
            f"- ML labels: `{json.dumps(summary['ml_label_counts'], sort_keys=True)}`",
            f"- ML models: `{json.dumps(summary['ml_model_counts'], sort_keys=True)}`",
            f"- ML score summary: `{json.dumps(summary['ml_score_summary'], sort_keys=True)}`",
            "",
            "## Top Scored Alerts",
            "",
        ]
    )

    for row in report["top_scored_alerts"][:10]:
        score = row["ml_shadow"].get("score_value")
        score_text = "missing" if score is None else f"{float(score):.3f}"
        lines.extend(
            [
                f"### {row.get('title') or row.get('alert_id')}",
                "",
                f"- Alert: `{row.get('alert_id')}`",
                f"- Candidate: `{row.get('candidate_id')}`",
                f"- Severity: `{row.get('severity')}`",
                f"- ML: `{row['ml_shadow'].get('score_label')}` `{score_text}`",
                f"- Evidence: `{row.get('public_evidence_state')}`",
                f"- Delivered: `{row['delivery'].get('sent_count')}` sent attempts",
                f"- Market: {row.get('market_url') or 'unavailable'}",
                "",
            ]
        )

    missing = report.get("missing_ml_score_alerts", [])
    if missing:
        lines.extend(["## Missing ML Scores", ""])
        for row in missing[:10]:
            lines.append(f"- `{row.get('alert_id')}` {row.get('title') or ''}")
        lines.append("")

    return "\n".join(lines).rstrip() + "\n"


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Review live Phase 4 alerts against Phase 6 LightGBM shadow scores."
    )
    parser.add_argument("--env-file", default=None, help="Runtime env file to load before DB access.")
    parser.add_argument("--lookback-hours", type=float, default=24.0)
    parser.add_argument("--max-examples", type=int, default=20)
    parser.add_argument("--output-json", default=str(DEFAULT_JSON))
    parser.add_argument("--output-markdown", default=str(DEFAULT_MARKDOWN))
    parser.add_argument("--json", action="store_true", help="Print the JSON report to stdout.")
    args = parser.parse_args()

    load_runtime_env(args.env_file)
    report = build_ml_live_alert_review(
        lookback_hours=args.lookback_hours,
        max_examples=max(1, args.max_examples),
    )

    json_path = Path(args.output_json)
    markdown_path = Path(args.output_markdown)
    json_path.parent.mkdir(parents=True, exist_ok=True)
    markdown_path.parent.mkdir(parents=True, exist_ok=True)
    json_path.write_text(json.dumps(report, allow_nan=False, indent=2, sort_keys=True) + "\n")
    markdown_path.write_text(render_markdown(report))

    if args.json:
        print(json.dumps(report, allow_nan=False, indent=2, sort_keys=True))
    else:
        summary = report["summary"]
        print(
            json.dumps(
                {
                    "status": "ok",
                    "output_json": str(json_path),
                    "output_markdown": str(markdown_path),
                    "alerts_reviewed": summary["alerts_reviewed"],
                    "ml_score_coverage": summary["ml_score_coverage"],
                    "promotion_readiness": summary["promotion_readiness"],
                },
                allow_nan=False,
                sort_keys=True,
            )
        )


if __name__ == "__main__":
    main()
