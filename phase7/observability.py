from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from statistics import median
from typing import Any

from database.db_manager import get_conn
from phase5.models import EvaluationRow
from phase5.repository import Phase5Repository, _parse_iso
from phase5.simulator import infer_direction


PUBLIC_EVIDENCE_STATES = {"already_public", "weakly_public"}
OBSERVABILITY_BUCKET_ORDER = (
    "candidate_only",
    "internal_scored_only",
    "alert_created_not_delivered",
    "operator_visible",
    "operator_reviewed",
    "publicly_explained",
)


@dataclass(slots=True)
class Phase7GoodhartStudyReport:
    start: str
    end: str
    model_version: str | None
    summary: dict[str, Any]
    observability_regimes: list[dict[str, Any]]
    measurable_risks: dict[str, Any]
    findings: list[dict[str, Any]]
    deployment_implications: list[str]
    thesis_implications: list[str]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def _safe_ratio(numerator: int | float, denominator: int | float) -> float:
    if not denominator:
        return 0.0
    return float(numerator) / float(denominator)


def _success_for_row(row: EvaluationRow) -> bool | None:
    direction = infer_direction(row)
    if direction is None or not row.resolution_outcome:
        return None
    decision_time = _parse_iso(row.alert_created_at or row.candidate_trigger_time)
    resolution_time = _parse_iso(row.resolution_time)
    if decision_time is None or resolution_time is None or resolution_time <= decision_time:
        return None
    return row.resolution_outcome == direction


def _lead_time_seconds(row: EvaluationRow) -> float | None:
    decision_time = _parse_iso(row.alert_created_at or row.candidate_trigger_time)
    resolution_time = _parse_iso(row.resolution_time)
    if decision_time is None or resolution_time is None or resolution_time <= decision_time:
        return None
    return (resolution_time - decision_time).total_seconds()


def _classify_observability_bucket(
    row: EvaluationRow,
    *,
    has_shadow_score: bool,
    delivery_attempt_count: int,
    feedback_count: int,
) -> str:
    evidence_state = str(row.evidence_state_at_alert or "missing")
    if evidence_state in PUBLIC_EVIDENCE_STATES and row.alert_id is not None:
        return "publicly_explained"
    if feedback_count > 0 and row.alert_id is not None:
        return "operator_reviewed"
    if delivery_attempt_count > 0 and row.alert_id is not None:
        return "operator_visible"
    if row.alert_id is not None:
        return "alert_created_not_delivered"
    if has_shadow_score:
        return "internal_scored_only"
    return "candidate_only"


def _group_observability_metrics(enriched_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = {key: [] for key in OBSERVABILITY_BUCKET_ORDER}
    for item in enriched_rows:
        grouped.setdefault(str(item["observability_bucket"]), []).append(item)

    summaries: list[dict[str, Any]] = []
    for bucket in OBSERVABILITY_BUCKET_ORDER:
        items = grouped.get(bucket, [])
        successes = [item["success"] for item in items if item["success"] is not None]
        lead_times = [item["lead_time_seconds"] for item in items if item["lead_time_seconds"] is not None]
        summaries.append(
            {
                "bucket": bucket,
                "row_count": len(items),
                "alert_count": sum(1 for item in items if item["alert_id"]),
                "scored_count": sum(1 for item in items if item["score_value"] is not None),
                "delivered_count": sum(1 for item in items if item["delivery_attempt_count"] > 0),
                "feedback_count": sum(1 for item in items if item["feedback_count"] > 0),
                "public_evidence_count": sum(
                    1 for item in items if item["evidence_state_at_alert"] in PUBLIC_EVIDENCE_STATES
                ),
                "label_coverage": round(_safe_ratio(len(successes), len(items)), 6),
                "success_rate": round(_safe_ratio(sum(1 for item in successes if item), len(successes)), 6),
                "median_lead_time_seconds": round(median(lead_times), 6) if lead_times else None,
                "median_score_value": round(
                    median([float(item["score_value"]) for item in items if item["score_value"] is not None]),
                    6,
                ) if any(item["score_value"] is not None for item in items) else None,
            }
        )
    return summaries


def _load_latest_shadow_scores(
    *,
    candidate_ids: list[str],
    model_version: str | None,
) -> tuple[dict[str, dict[str, Any]], str | None]:
    if not candidate_ids:
        return {}, model_version
    conn = get_conn()
    try:
        if not model_version:
            latest = conn.execute(
                """
                SELECT model_version
                FROM shadow_model_scores
                ORDER BY scored_at DESC, created_at DESC
                LIMIT 1
                """
            ).fetchone()
            model_version = str(latest["model_version"]) if latest and latest["model_version"] is not None else None
        placeholders = ", ".join("?" for _ in candidate_ids)
        params: list[Any] = list(candidate_ids)
        if model_version:
            params.append(model_version)
        rows = conn.execute(
            f"""
            SELECT
                candidate_id,
                model_version,
                score_value,
                score_label,
                score_metadata,
                scored_at
            FROM shadow_model_scores
            WHERE candidate_id IN ({placeholders})
            {"AND model_version = ?" if model_version else ""}
            ORDER BY scored_at DESC, created_at DESC, candidate_id ASC
            """,
            tuple(params),
        ).fetchall()
    finally:
        conn.close()

    latest_by_candidate: dict[str, dict[str, Any]] = {}
    for row in rows:
        candidate_id = str(row["candidate_id"])
        if candidate_id in latest_by_candidate:
            continue
        latest_by_candidate[candidate_id] = {
                "model_version": row["model_version"],
                "score_value": float(row["score_value"]) if row["score_value"] is not None else None,
                "score_label": row["score_label"],
                "score_metadata": json.loads(row["score_metadata"] or "{}"),
                "scored_at": row["scored_at"],
            }
    return latest_by_candidate, model_version


def _load_delivery_counts(*, alert_ids: list[str]) -> dict[str, int]:
    if not alert_ids:
        return {}
    placeholders = ", ".join("?" for _ in alert_ids)
    conn = get_conn()
    try:
        rows = conn.execute(
            f"""
            SELECT alert_id, COUNT(*) AS delivery_attempt_count
            FROM alert_delivery_attempts
            WHERE alert_id IN ({placeholders})
            GROUP BY alert_id
            """,
            tuple(alert_ids),
        ).fetchall()
    finally:
        conn.close()
    return {str(row["alert_id"]): int(row["delivery_attempt_count"] or 0) for row in rows}


def _load_feedback_counts(*, alert_ids: list[str]) -> dict[str, int]:
    if not alert_ids:
        return {}
    placeholders = ", ".join("?" for _ in alert_ids)
    conn = get_conn()
    try:
        rows = conn.execute(
            f"""
            SELECT alert_id, COUNT(*) AS feedback_count
            FROM analyst_feedback
            WHERE alert_id IN ({placeholders})
            GROUP BY alert_id
            """,
            tuple(alert_ids),
        ).fetchall()
    finally:
        conn.close()
    return {str(row["alert_id"]): int(row["feedback_count"] or 0) for row in rows}


def _load_evidence_query_metrics(*, candidate_ids: list[str]) -> dict[str, Any]:
    if not candidate_ids:
        return {
            "query_count": 0,
            "timeout_rate": 0.0,
            "error_rate": 0.0,
            "median_latency_ms": None,
        }
    placeholders = ", ".join("?" for _ in candidate_ids)
    conn = get_conn()
    try:
        rows = conn.execute(
            f"""
            SELECT query_status, latency_ms
            FROM evidence_queries
            WHERE candidate_id IN ({placeholders})
            """,
            tuple(candidate_ids),
        ).fetchall()
    finally:
        conn.close()
    latencies = [float(row["latency_ms"]) for row in rows if row["latency_ms"] is not None]
    return {
        "query_count": len(rows),
        "timeout_rate": round(_safe_ratio(sum(1 for row in rows if row["query_status"] == "timeout"), len(rows)), 6),
        "error_rate": round(_safe_ratio(sum(1 for row in rows if row["query_status"] == "error"), len(rows)), 6),
        "median_latency_ms": round(median(latencies), 6) if latencies else None,
    }


def _score_threshold_bunching(enriched_rows: list[dict[str, Any]], *, epsilon: float = 0.02) -> dict[str, Any]:
    scored = [item for item in enriched_rows if item["score_value"] is not None]
    if not scored:
        return {
            "scored_row_count": 0,
            "threshold_bunching_rate": 0.0,
            "thresholds": {},
        }
    thresholds = {}
    for item in scored:
        metadata = item.get("score_metadata") or {}
        raw = metadata.get("thresholds") or {}
        if raw:
            thresholds = {
                key: float(value)
                for key, value in raw.items()
                if value is not None
            }
            break
    if not thresholds:
        thresholds = {"watch": 0.5, "actionable": 0.75, "critical": 0.9}
    bunching = 0
    for item in scored:
        score_value = float(item["score_value"])
        if any(abs(score_value - threshold) <= epsilon for threshold in thresholds.values()):
            bunching += 1
    return {
        "scored_row_count": len(scored),
        "threshold_bunching_rate": round(_safe_ratio(bunching, len(scored)), 6),
        "thresholds": thresholds,
    }


def _measurable_risks(enriched_rows: list[dict[str, Any]], observability_regimes: list[dict[str, Any]]) -> dict[str, Any]:
    successful = [item for item in enriched_rows if item["success"] is True]
    alerts = [item for item in enriched_rows if item["alert_id"]]
    delivered = [item for item in alerts if item["delivery_attempt_count"] > 0]
    scored = [item for item in enriched_rows if item["score_value"] is not None]
    successful_scored = [item for item in scored if item["success"] is True]

    low_buckets = {"candidate_only", "internal_scored_only"}
    high_buckets = {"operator_visible", "operator_reviewed", "publicly_explained"}
    low_regime = next((item for item in observability_regimes if item["bucket"] == "internal_scored_only"), None)
    high_regime = next((item for item in observability_regimes if item["bucket"] == "publicly_explained"), None)
    if low_regime is None:
        low_items = [item for item in observability_regimes if item["bucket"] in low_buckets]
        low_regime = {
            "success_rate": round(_safe_ratio(sum(item["success_rate"] * item["row_count"] for item in low_items), sum(item["row_count"] for item in low_items)), 6) if low_items and sum(item["row_count"] for item in low_items) else None,
            "median_lead_time_seconds": next((item["median_lead_time_seconds"] for item in low_items if item["median_lead_time_seconds"] is not None), None),
        }
    if high_regime is None:
        high_items = [item for item in observability_regimes if item["bucket"] in high_buckets]
        high_regime = {
            "success_rate": round(_safe_ratio(sum(item["success_rate"] * item["row_count"] for item in high_items), sum(item["row_count"] for item in high_items)), 6) if high_items and sum(item["row_count"] for item in high_items) else None,
            "median_lead_time_seconds": next((item["median_lead_time_seconds"] for item in high_items if item["median_lead_time_seconds"] is not None), None),
        }

    lead_time_decay = None
    if low_regime.get("median_lead_time_seconds") is not None and high_regime.get("median_lead_time_seconds") is not None:
        lead_time_decay = round(float(high_regime["median_lead_time_seconds"]) - float(low_regime["median_lead_time_seconds"]), 6)

    score_label_counts: dict[str, int] = {}
    for item in successful_scored:
        key = str(item["score_label"] or "missing")
        score_label_counts[key] = score_label_counts.get(key, 0) + 1
    top_score_label_dependency_share = round(
        _safe_ratio(max(score_label_counts.values()) if score_label_counts else 0, len(successful_scored)),
        6,
    )
    category_counts: dict[str, int] = {}
    event_family_counts: dict[str, int] = {}
    for item in successful:
        category_counts[item["category_key"]] = category_counts.get(item["category_key"], 0) + 1
        event_family_counts[item["event_family_id"]] = event_family_counts.get(item["event_family_id"], 0) + 1

    threshold_bunching = _score_threshold_bunching(enriched_rows)
    return {
        "candidate_success_without_alert_rate": round(
            _safe_ratio(sum(1 for item in successful if not item["alert_id"]), len(successful)),
            6,
        ),
        "delivered_alert_without_feedback_rate": round(
            _safe_ratio(sum(1 for item in delivered if item["feedback_count"] == 0), len(delivered)),
            6,
        ),
        "pending_or_missing_evidence_alert_share": round(
            _safe_ratio(
                sum(
                    1
                    for item in alerts
                    if str(item["evidence_state_at_alert"] or "missing") not in PUBLIC_EVIDENCE_STATES
                ),
                len(alerts),
            ),
            6,
        ),
        "multi_attempt_alert_share": round(
            _safe_ratio(sum(1 for item in alerts if item["delivery_attempt_count"] > 1), len(alerts)),
            6,
        ),
        "observability_precision_gap": (
            round(float(high_regime["success_rate"]) - float(low_regime["success_rate"]), 6)
            if high_regime.get("success_rate") is not None and low_regime.get("success_rate") is not None
            else None
        ),
        "observability_lead_time_gap_seconds": lead_time_decay,
        "score_label_dependency_share": top_score_label_dependency_share,
        "top_category_dependency_share": round(
            _safe_ratio(max(category_counts.values()) if category_counts else 0, len(successful)),
            6,
        ),
        "top_event_family_dependency_share": round(
            _safe_ratio(max(event_family_counts.values()) if event_family_counts else 0, len(successful)),
            6,
        ),
        "threshold_bunching": threshold_bunching,
    }


def _findings(measurable_risks: dict[str, Any], evidence_query_metrics: dict[str, Any]) -> list[dict[str, Any]]:
    findings: list[dict[str, Any]] = []
    suppression_blind_spot = float(measurable_risks.get("candidate_success_without_alert_rate") or 0.0)
    if suppression_blind_spot >= 0.2:
        findings.append(
            {
                "title": "Alerting policy may be hiding useful candidate flow",
                "severity": "high",
                "evidence": f"Successful candidates without alerts rate = {suppression_blind_spot:.3f}",
                "deployment_implication": "Review suppression and alert thresholds before increasing operator trust in alert-level precision.",
                "thesis_implication": "Observed edge may partly live in unalerted candidates, not only in visible alerts.",
            }
        )

    lead_time_gap = measurable_risks.get("observability_lead_time_gap_seconds")
    precision_gap = measurable_risks.get("observability_precision_gap")
    if lead_time_gap is not None and precision_gap is not None and float(lead_time_gap) < 0 and float(precision_gap) >= 0:
        findings.append(
            {
                "title": "Observability may preserve calibration while decaying lead time",
                "severity": "high",
                "evidence": (
                    f"Observability precision gap = {float(precision_gap):.3f}, "
                    f"lead-time gap seconds = {float(lead_time_gap):.1f}"
                ),
                "deployment_implication": "Do not treat better visible precision as proof that tradable or investigative edge is preserved.",
                "thesis_implication": "Supports the H1 framing that Goodhart can appear as lower edge without worse calibration.",
            }
        )

    unreviewed = float(measurable_risks.get("delivered_alert_without_feedback_rate") or 0.0)
    if unreviewed >= 0.5:
        findings.append(
            {
                "title": "Operator feedback coverage is too sparse for strong online policy claims",
                "severity": "medium",
                "evidence": f"Delivered alert without feedback rate = {unreviewed:.3f}",
                "deployment_implication": "Avoid tuning suppression or severity policies primarily from visible alert outcomes until feedback coverage improves.",
                "thesis_implication": "Human-validated usefulness labels may be selection-biased.",
            }
        )

    threshold_bunching = float((measurable_risks.get("threshold_bunching") or {}).get("threshold_bunching_rate") or 0.0)
    if threshold_bunching >= 0.25:
        findings.append(
            {
                "title": "Operator-facing score thresholds look brittle",
                "severity": "medium",
                "evidence": f"Threshold bunching rate = {threshold_bunching:.3f}",
                "deployment_implication": "Threshold-tied alerting may overreact to small score noise around WATCH/ACTIONABLE/CRITICAL cutoffs.",
                "thesis_implication": "Measured policy performance may be sensitive to arbitrary decision boundaries rather than robust rank ordering.",
            }
        )

    concentration = max(
        float(measurable_risks.get("score_label_dependency_share") or 0.0),
        float(measurable_risks.get("top_category_dependency_share") or 0.0),
        float(measurable_risks.get("top_event_family_dependency_share") or 0.0),
    )
    if concentration >= 0.5:
        findings.append(
            {
                "title": "Success metrics appear concentrated in a narrow regime",
                "severity": "medium",
                "evidence": f"Max concentration share = {concentration:.3f}",
                "deployment_implication": "Rollout claims should be scoped by category/event-family rather than presented as uniform system-wide performance.",
                "thesis_implication": "Aggregate headline metrics may overstate generality.",
            }
        )

    timeout_rate = float(evidence_query_metrics.get("timeout_rate") or 0.0)
    if timeout_rate >= 0.1:
        findings.append(
            {
                "title": "Evidence observability is degraded by provider timeouts",
                "severity": "low",
                "evidence": f"Evidence query timeout rate = {timeout_rate:.3f}",
                "deployment_implication": "Public-explanation states may partly reflect retrieval noise, not only true lack of corroboration.",
                "thesis_implication": "Goodhart conclusions involving public evidence need retrieval-quality controls.",
            }
        )

    if not findings:
        findings.append(
            {
                "title": "No strong measurable Goodhart risk surfaced in the available window",
                "severity": "info",
                "evidence": "Current risk thresholds were not crossed by the available metrics.",
                "deployment_implication": "Keep observing larger windows before declaring H1 unsupported.",
                "thesis_implication": "Absence of evidence in one window does not falsify H1.",
            }
        )

    return findings


def _deployment_implications(findings: list[dict[str, Any]]) -> list[str]:
    return list(dict.fromkeys(finding["deployment_implication"] for finding in findings))


def _thesis_implications(findings: list[dict[str, Any]]) -> list[str]:
    return list(dict.fromkeys(finding["thesis_implication"] for finding in findings))


def build_goodhart_observability_study(
    *,
    start: str,
    end: str,
    model_version: str | None = None,
) -> Phase7GoodhartStudyReport:
    repository = Phase5Repository()
    rows = repository.load_evaluation_rows(start=start, end=end)
    candidate_ids = [row.candidate_id for row in rows]
    alert_ids = [row.alert_id for row in rows if row.alert_id]

    latest_scores, resolved_model_version = _load_latest_shadow_scores(
        candidate_ids=candidate_ids,
        model_version=model_version,
    )
    delivery_counts = _load_delivery_counts(alert_ids=alert_ids)
    feedback_counts = _load_feedback_counts(alert_ids=alert_ids)
    evidence_query_metrics = _load_evidence_query_metrics(candidate_ids=candidate_ids)

    enriched_rows: list[dict[str, Any]] = []
    for row in rows:
        score = latest_scores.get(row.candidate_id) or {}
        delivery_attempt_count = delivery_counts.get(str(row.alert_id), 0) if row.alert_id else 0
        feedback_count = feedback_counts.get(str(row.alert_id), 0) if row.alert_id else 0
        enriched_rows.append(
            {
                "candidate_id": row.candidate_id,
                "alert_id": row.alert_id,
                "event_family_id": row.event_family_id,
                "category_key": row.category_key,
                "evidence_state_at_alert": str(row.evidence_state_at_alert or "missing"),
                "score_value": score.get("score_value"),
                "score_label": score.get("score_label"),
                "score_metadata": score.get("score_metadata") or {},
                "delivery_attempt_count": delivery_attempt_count,
                "feedback_count": feedback_count,
                "success": _success_for_row(row),
                "lead_time_seconds": _lead_time_seconds(row),
                "observability_bucket": _classify_observability_bucket(
                    row,
                    has_shadow_score=bool(score),
                    delivery_attempt_count=delivery_attempt_count,
                    feedback_count=feedback_count,
                ),
            }
        )

    observability_regimes = _group_observability_metrics(enriched_rows)
    measurable_risks = {
        **_measurable_risks(enriched_rows, observability_regimes),
        "evidence_query_metrics": evidence_query_metrics,
    }
    findings = _findings(measurable_risks, evidence_query_metrics)

    return Phase7GoodhartStudyReport(
        start=start,
        end=end,
        model_version=resolved_model_version,
        summary={
            "candidate_count": len(rows),
            "alert_count": sum(1 for row in rows if row.alert_id),
            "scored_candidate_count": len(latest_scores),
            "delivered_alert_count": sum(1 for item in enriched_rows if item["delivery_attempt_count"] > 0),
            "reviewed_alert_count": sum(1 for item in enriched_rows if item["feedback_count"] > 0),
        },
        observability_regimes=observability_regimes,
        measurable_risks=measurable_risks,
        findings=findings,
        deployment_implications=_deployment_implications(findings),
        thesis_implications=_thesis_implications(findings),
    )


def render_goodhart_memo(report: Phase7GoodhartStudyReport) -> str:
    lines = [
        "# Phase 7 Goodhart / Observability Memo",
        "",
        "## Scope",
        f"- Window: `{report.start}` to `{report.end}`",
        f"- Shadow model version: `{report.model_version or 'none'}`",
        f"- Candidate rows: `{report.summary['candidate_count']}`",
        f"- Alert rows: `{report.summary['alert_count']}`",
        "",
        "## Headline Findings",
    ]
    for finding in report.findings:
        lines.extend(
            [
                f"- [{finding['severity']}] {finding['title']}",
                f"  Evidence: {finding['evidence']}",
            ]
        )
    lines.extend(["", "## Observability Regimes"])
    for regime in report.observability_regimes:
        lines.append(
            "- "
            f"{regime['bucket']}: rows={regime['row_count']}, success_rate={regime['success_rate']}, "
            f"median_lead_time_seconds={regime['median_lead_time_seconds']}"
        )
    lines.extend(
        [
            "",
            "## Measurable Risks",
            f"- Candidate success without alert rate: `{report.measurable_risks['candidate_success_without_alert_rate']}`",
            f"- Delivered alert without feedback rate: `{report.measurable_risks['delivered_alert_without_feedback_rate']}`",
            f"- Pending/missing evidence alert share: `{report.measurable_risks['pending_or_missing_evidence_alert_share']}`",
            f"- Multi-attempt alert share: `{report.measurable_risks['multi_attempt_alert_share']}`",
            f"- Observability precision gap: `{report.measurable_risks['observability_precision_gap']}`",
            f"- Observability lead-time gap seconds: `{report.measurable_risks['observability_lead_time_gap_seconds']}`",
            f"- Threshold bunching rate: `{report.measurable_risks['threshold_bunching']['threshold_bunching_rate']}`",
            f"- Top category dependency share: `{report.measurable_risks['top_category_dependency_share']}`",
            f"- Top event-family dependency share: `{report.measurable_risks['top_event_family_dependency_share']}`",
            "",
            "## Deployment Implications",
        ]
    )
    for line in report.deployment_implications:
        lines.append(f"- {line}")
    lines.extend(["", "## Thesis Implications"])
    for line in report.thesis_implications:
        lines.append(f"- {line}")
    return "\n".join(lines) + "\n"
