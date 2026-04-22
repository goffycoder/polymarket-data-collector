from __future__ import annotations

import csv
import hashlib
import json
from datetime import datetime, timezone
from html import escape
from pathlib import Path
from typing import Any

from config.settings import PHASE7_RESEARCH_PACKAGE_VERSION, PHASE7_THESIS_FIGURE_STYLE_VERSION
from phase7.ablations import Phase7AblationSummary


def _iso_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _safe_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _stable_json(value: Any) -> str:
    return json.dumps(value, indent=2, sort_keys=True) + "\n"


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def sha256_file(path: Path) -> str:
    return sha256_bytes(path.read_bytes())


def load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8-sig"))


def write_json(path: Path, payload: dict[str, Any]) -> str:
    data = _stable_json(payload)
    path.write_text(data, encoding="utf-8")
    return sha256_bytes(data.encode("utf-8"))


def write_markdown(path: Path, text: str) -> str:
    if not text.endswith("\n"):
        text += "\n"
    path.write_text(text, encoding="utf-8")
    return sha256_bytes(text.encode("utf-8"))


def write_csv(path: Path, rows: list[dict[str, Any]], *, fieldnames: list[str]) -> str:
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow(row)
    return sha256_file(path)


def write_text(path: Path, text: str) -> str:
    path.write_text(text, encoding="utf-8")
    return sha256_file(path)


def _format_metric(value: Any) -> str:
    float_value = _safe_float(value)
    if float_value is None:
        return "NA"
    return f"{float_value:.3f}"


def _format_bool(value: Any) -> str:
    if value is None:
        return "NA"
    return "yes" if bool(value) else "no"


def render_ablation_table_markdown(summary: Phase7AblationSummary) -> str:
    lines = [
        "# Phase 7 Ablation Table",
        "",
        "| Family | Test AUC | Validation AUC | Delta vs Phase 6 Test AUC | Delta vs Best Heuristic | Strict Holdout | Accepted | Status |",
        "| --- | ---: | ---: | ---: | ---: | --- | --- | --- |",
    ]
    for row in summary.family_rows:
        lines.append(
            "| "
            f"{row['display_name']} | "
            f"{_format_metric(row.get('test_auc'))} | "
            f"{_format_metric(row.get('validation_auc'))} | "
            f"{_format_metric(row.get('test_margin_vs_phase6_auc'))} | "
            f"{_format_metric(row.get('test_margin_vs_best_heuristic_auc'))} | "
            f"{row.get('strict_holdout_status') or 'NA'} | "
            f"{_format_bool(row.get('strict_holdout_accepted'))} | "
            f"{row.get('availability_status') or 'NA'} |"
        )
    if summary.notes:
        lines.extend(["", "## Notes"])
        for note in summary.notes:
            lines.append(f"- {note}")
    return "\n".join(lines) + "\n"


def render_methodology_markdown(
    *,
    dataset_key: str | None,
    ablation_summary: Phase7AblationSummary,
    goodhart_report: dict[str, Any] | None,
    input_artifacts: list[dict[str, Any]],
) -> str:
    lines = [
        "# Phase 7 Final Research Package",
        "",
        "## Reproducibility Contract",
        f"- Package version: `{PHASE7_RESEARCH_PACKAGE_VERSION}`",
        f"- Figure style version: `{PHASE7_THESIS_FIGURE_STYLE_VERSION}`",
        f"- Dataset key: `{dataset_key or 'adhoc'}`",
        f"- Dataset hashes seen: `{', '.join(ablation_summary.dataset_hashes) or 'none'}`",
        f"- Package status: `{ablation_summary.status}`",
        "",
        "## Inputs",
    ]
    if input_artifacts:
        for artifact in input_artifacts:
            lines.append(
                "- "
                f"{artifact.get('family_key', artifact.get('kind', 'artifact'))}: "
                f"`{artifact.get('path')}` "
                f"(sha256 `{artifact.get('sha256')}`, dataset `{artifact.get('dataset_hash') or 'unknown'}`)"
            )
    else:
        lines.append("- No experiment artifacts were supplied or discovered.")
    lines.extend(
        [
            "",
            "## Methodology Notes",
            "- All ablation rows are derived from saved report artifacts rather than rerunning training during package assembly.",
            "- Family comparisons remain directly comparable only when dataset hashes match exactly.",
            "- Advanced-family gains are treated as substantive only when their own strict holdout gate is accepted.",
            "- Missing families remain visible as explicit placeholders instead of being hidden from the package.",
        ]
    )
    if goodhart_report:
        lines.extend(
            [
                "",
                "## Observability Study Linkage",
                f"- Goodhart window: `{goodhart_report.get('start')}` to `{goodhart_report.get('end')}`",
                f"- Goodhart findings: `{len(goodhart_report.get('findings') or [])}`",
                "- Observability risks are packaged separately from predictive gains so operator-facing brittleness cannot be hidden behind model accuracy alone.",
            ]
        )
    return "\n".join(lines) + "\n"


def _svg_document(*, width: int, height: int, body: str) -> str:
    return (
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}">'
        '<style>'
        "text{font-family:Georgia, 'Times New Roman', serif; fill:#1e293b;}"
        ".title{font-size:26px;font-weight:700;}"
        ".subtitle{font-size:13px;fill:#475569;}"
        ".axis{stroke:#475569;stroke-width:1.5;}"
        ".grid{stroke:#cbd5e1;stroke-width:1;stroke-dasharray:4 4;}"
        ".label{font-size:13px;}"
        ".metric{font-size:12px;font-weight:700;}"
        '</style>'
        f"{body}</svg>"
    )


def render_auc_figure_svg(summary: Phase7AblationSummary) -> str:
    rows = [row for row in summary.family_rows if _safe_float(row.get("test_auc")) is not None]
    width = 980
    height = max(320, 180 + (len(summary.family_rows) * 48))
    left = 260
    chart_width = 620
    top = 110
    row_height = 42
    if not rows:
        body = (
            '<rect x="0" y="0" width="980" height="320" fill="#fffaf4"/>'
            '<text x="50" y="70" class="title">Phase 7 Test AUC Comparison</text>'
            '<text x="50" y="110" class="subtitle">No scored experiment artifacts were available when the package was generated.</text>'
        )
        return _svg_document(width=980, height=320, body=body)

    body_parts = [
        f'<rect x="0" y="0" width="{width}" height="{height}" fill="#fffaf4"/>',
        '<text x="50" y="52" class="title">Phase 7 Test AUC Comparison</text>',
        f'<text x="50" y="82" class="subtitle">Package version {escape(PHASE7_RESEARCH_PACKAGE_VERSION)} | Dataset status {escape(summary.status)}</text>',
    ]
    for tick in range(6):
        value = tick / 5
        x = left + (chart_width * value)
        body_parts.append(f'<line x1="{x:.1f}" y1="{top - 10}" x2="{x:.1f}" y2="{height - 45}" class="grid"/>')
        body_parts.append(f'<text x="{x:.1f}" y="{height - 18}" text-anchor="middle" class="label">{value:.1f}</text>')
    body_parts.append(f'<line x1="{left}" y1="{top - 10}" x2="{left}" y2="{height - 45}" class="axis"/>')
    for index, row in enumerate(summary.family_rows):
        y = top + (index * row_height)
        label = escape(row["display_name"])
        value = _safe_float(row.get("test_auc"))
        availability = row.get("availability_status")
        fill = "#b45309" if row["family_key"] == "wallet_aware_phase6" else "#0f766e"
        if row["family_key"] in {"severity_heuristic", "wallet_heuristic", "velocity_heuristic"}:
            fill = "#64748b"
        if availability != "available" or value is None:
            fill = "#cbd5e1"
            value = 0.0
        width_value = chart_width * max(0.0, min(1.0, value))
        body_parts.append(f'<text x="{left - 12}" y="{y + 18}" text-anchor="end" class="label">{label}</text>')
        body_parts.append(f'<rect x="{left}" y="{y}" width="{width_value:.1f}" height="22" rx="4" fill="{fill}"/>')
        metric_label = "NA" if availability != "available" or _safe_float(row.get("test_auc")) is None else f"{_safe_float(row.get('test_auc')):.3f}"
        body_parts.append(f'<text x="{left + width_value + 8:.1f}" y="{y + 16}" class="metric">{metric_label}</text>')
    return _svg_document(width=width, height=height, body="".join(body_parts))


def render_margin_figure_svg(summary: Phase7AblationSummary) -> str:
    advanced_rows = [
        row for row in summary.family_rows
        if row["family_key"] in {"graph_aware", "temporal", "other_advanced"}
    ]
    width = 1000
    height = max(300, 180 + (len(advanced_rows) * 90))
    if not advanced_rows:
        body = (
            '<rect x="0" y="0" width="1000" height="320" fill="#f8fafc"/>'
            '<text x="50" y="70" class="title">Phase 7 Margin vs Phase 6</text>'
            '<text x="50" y="110" class="subtitle">No advanced-family artifacts were available for margin plotting.</text>'
        )
        return _svg_document(width=1000, height=320, body=body)

    values: list[float] = []
    for row in advanced_rows:
        for key in ("validation_margin_vs_phase6_auc", "test_margin_vs_phase6_auc"):
            value = _safe_float(row.get(key))
            if value is not None:
                values.append(value)
    bound = max(0.05, max((abs(value) for value in values), default=0.0))
    left = 390
    chart_width = 500
    zero_x = left + (chart_width / 2)
    top = 110
    row_height = 72

    body_parts = [
        f'<rect x="0" y="0" width="{width}" height="{height}" fill="#f8fafc"/>',
        '<text x="50" y="52" class="title">Phase 7 Margin vs Phase 6</text>',
        '<text x="50" y="82" class="subtitle">Validation and test AUC margins for advanced families only.</text>',
    ]
    for tick in range(-4, 5):
        value = (bound * tick) / 4
        x = zero_x + (chart_width / 2) * (value / bound)
        body_parts.append(f'<line x1="{x:.1f}" y1="{top - 15}" x2="{x:.1f}" y2="{height - 40}" class="grid"/>')
        body_parts.append(f'<text x="{x:.1f}" y="{height - 15}" text-anchor="middle" class="label">{value:.3f}</text>')
    body_parts.append(f'<line x1="{zero_x:.1f}" y1="{top - 15}" x2="{zero_x:.1f}" y2="{height - 40}" class="axis"/>')
    for index, row in enumerate(advanced_rows):
        base_y = top + (index * row_height)
        body_parts.append(f'<text x="{left - 18}" y="{base_y + 20}" text-anchor="end" class="label">{escape(row["display_name"])}</text>')
        for bar_index, (label, key, fill, offset) in enumerate(
            (
                ("validation", "validation_margin_vs_phase6_auc", "#0ea5e9", 0),
                ("test", "test_margin_vs_phase6_auc", "#f97316", 26),
            )
        ):
            value = _safe_float(row.get(key))
            width_value = 0.0 if value is None else (chart_width / 2) * (min(bound, max(-bound, value)) / bound)
            x = zero_x if width_value >= 0 else zero_x + width_value
            body_parts.append(f'<rect x="{x:.1f}" y="{base_y + offset}" width="{abs(width_value):.1f}" height="18" rx="3" fill="{fill}"/>')
            metric = "NA" if value is None else f"{value:.3f}"
            text_x = zero_x + width_value + (8 if width_value >= 0 else -8)
            anchor = "start" if width_value >= 0 else "end"
            body_parts.append(f'<text x="{text_x:.1f}" y="{base_y + offset + 14}" text-anchor="{anchor}" class="metric">{label}: {metric}</text>')
    return _svg_document(width=width, height=height, body="".join(body_parts))


def render_observability_figure_svg(goodhart_report: dict[str, Any]) -> str:
    measurable = goodhart_report.get("measurable_risks") or {}
    figure_rows = [
        ("Success Without Alert", _safe_float(measurable.get("candidate_success_without_alert_rate"))),
        ("No Feedback After Delivery", _safe_float(measurable.get("delivered_alert_without_feedback_rate"))),
        ("Missing Evidence Share", _safe_float(measurable.get("pending_or_missing_evidence_alert_share"))),
        ("Multi-Attempt Alert Share", _safe_float(measurable.get("multi_attempt_alert_share"))),
        ("Threshold Bunching", _safe_float((measurable.get("threshold_bunching") or {}).get("threshold_bunching_rate"))),
    ]
    width = 980
    height = 180 + (len(figure_rows) * 44)
    left = 300
    chart_width = 560
    top = 95
    body_parts = [
        f'<rect x="0" y="0" width="{width}" height="{height}" fill="#fff7ed"/>',
        '<text x="50" y="52" class="title">Phase 7 Observability Risk Profile</text>',
        '<text x="50" y="82" class="subtitle">Operator-facing Goodhart and visibility proxies from the saved observability study.</text>',
    ]
    for tick in range(6):
        value = tick / 5
        x = left + (chart_width * value)
        body_parts.append(f'<line x1="{x:.1f}" y1="{top - 10}" x2="{x:.1f}" y2="{height - 40}" class="grid"/>')
        body_parts.append(f'<text x="{x:.1f}" y="{height - 14}" text-anchor="middle" class="label">{value:.1f}</text>')
    body_parts.append(f'<line x1="{left}" y1="{top - 10}" x2="{left}" y2="{height - 40}" class="axis"/>')
    for index, (label, value) in enumerate(figure_rows):
        y = top + (index * 38)
        body_parts.append(f'<text x="{left - 14}" y="{y + 16}" text-anchor="end" class="label">{escape(label)}</text>')
        width_value = chart_width * max(0.0, min(1.0, value or 0.0))
        body_parts.append(f'<rect x="{left}" y="{y}" width="{width_value:.1f}" height="20" rx="4" fill="#c2410c"/>')
        metric = "NA" if value is None else f"{value:.3f}"
        body_parts.append(f'<text x="{left + width_value + 8:.1f}" y="{y + 15}" class="metric">{metric}</text>')
    return _svg_document(width=width, height=height, body="".join(body_parts))


def build_research_manifest(
    *,
    dataset_key: str | None,
    code_version: str,
    ablation_summary: Phase7AblationSummary,
    input_artifacts: list[dict[str, Any]],
    output_artifacts: list[dict[str, Any]],
    goodhart_summary: dict[str, Any] | None,
) -> dict[str, Any]:
    return {
        "package_version": PHASE7_RESEARCH_PACKAGE_VERSION,
        "figure_style_version": PHASE7_THESIS_FIGURE_STYLE_VERSION,
        "generated_at": _iso_now(),
        "dataset_key": dataset_key,
        "code_version": code_version,
        "ablation_summary": ablation_summary.to_dict(),
        "goodhart_summary": goodhart_summary,
        "input_artifacts": input_artifacts,
        "output_artifacts": output_artifacts,
        "reproducibility_notes": {
            "artifact_driven": True,
            "requires_matching_dataset_hashes_for_strong_claims": True,
            "missing_families_are_retained": True,
            "headlines_must_respect_strict_holdout_status": True,
        },
    }
