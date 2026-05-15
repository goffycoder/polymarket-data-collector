#!/usr/bin/env python3
"""Generate lightweight SVG plots for the presentation.

This script uses only the standard library so it can run without extra
plotting dependencies.
"""

from __future__ import annotations

import json
from pathlib import Path


ROOT = Path("/Users/vrajpatel/All-projects/polymarket_arbitrage")
OUT_DIR = ROOT / "reports" / "presentation"
PHASE5_JSON = Path(
    "/private/tmp/polymarket_arbitrage_main/reports/phase5/validation/"
    "phase10_task3_heldout_validation.json"
)
PHASE6_JSON = Path(
    "/private/tmp/polymarket_arbitrage_main/reports/phase6/model_artifacts/"
    "phase10_task4/phase10_task4_lightgbm_v1_report.json"
)


RED = "#9b111e"
RED_LIGHT = "#e34b5b"
INK = "#1f2937"
GRID = "#d1d5db"
BLUE = "#2563eb"
BLUE_LIGHT = "#93c5fd"
GREEN = "#15803d"
BG = "#ffffff"


def load_json(path: Path) -> dict:
    with path.open() as f:
        return json.load(f)


def svg_header(width: int, height: int) -> str:
    return (
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" '
        f'height="{height}" viewBox="0 0 {width} {height}">'
    )


def text(x: float, y: float, value: str, size: int = 20, weight: str = "400",
         fill: str = INK, anchor: str = "start") -> str:
    return (
        f'<text x="{x}" y="{y}" font-family="Arial, Helvetica, sans-serif" '
        f'font-size="{size}" font-weight="{weight}" fill="{fill}" '
        f'text-anchor="{anchor}">{value}</text>'
    )


def rect(x: float, y: float, w: float, h: float, fill: str,
         stroke: str = "none", rx: int = 8) -> str:
    return (
        f'<rect x="{x}" y="{y}" width="{w}" height="{h}" rx="{rx}" '
        f'fill="{fill}" stroke="{stroke}" />'
    )


def line(x1: float, y1: float, x2: float, y2: float, stroke: str = GRID,
         width: int = 1) -> str:
    return (
        f'<line x1="{x1}" y1="{y1}" x2="{x2}" y2="{y2}" '
        f'stroke="{stroke}" stroke-width="{width}" />'
    )


def metric_card(x: float, y: float, w: float, h: float, title: str,
                value: str, accent: str = RED) -> str:
    parts = [
        rect(x, y, w, h, "#f8fafc", stroke="#e5e7eb"),
        rect(x, y, 8, h, accent, rx=8),
        text(x + 24, y + 34, title, size=20, weight="600"),
        text(x + 24, y + 82, value, size=34, weight="700", fill=accent),
    ]
    return "".join(parts)


def phase5_validation_overview(data: dict) -> str:
    width, height = 1280, 720
    category_rows = data["metrics"]["alert_regimes"]["category_holdout"]
    category_precision = {}
    for row in category_rows:
        key = row["split_key"].split(":")[-1]
        category_precision[key] = row["metrics"]["alert_usefulness_precision"]

    coverage = data["coverage_summary"]
    assessment = data["assessment"]
    paper_trade = data["metrics"]["paper_trade_overall"]

    chart_x = 80
    chart_y = 320
    chart_w = 1120
    chart_h = 280
    bar_w = 180
    gap = 80
    labels = ["crypto", "macro", "politics", "sports"]

    parts = [svg_header(width, height), rect(0, 0, width, height, BG)]
    parts.append(text(80, 70, "Phase 5 Validation Overview", size=34, weight="700"))
    parts.append(
        text(
            80,
            108,
            "Held-out pack summary and category-level alert precision",
            size=20,
            fill="#4b5563",
        )
    )

    parts.append(metric_card(80, 145, 250, 110, "Evaluation Rows", str(coverage["total_rows"])))
    parts.append(metric_card(360, 145, 250, 110, "Alert Rows", str(coverage["alert_rows"]), accent=BLUE))
    parts.append(metric_card(640, 145, 250, 110, "Paper Trade Fills", str(coverage["paper_trade_fills"]), accent=GREEN))
    parts.append(metric_card(920, 145, 280, 110, "Median Bounded PnL", f'{assessment["median_bounded_pnl"]:.2f}', accent=RED_LIGHT))

    parts.append(text(80, 290, "Category Holdout Alert Precision", size=24, weight="700"))

    for i in range(6):
        y = chart_y + chart_h - (i * chart_h / 5)
        label = f"{i * 0.2:.1f}"
        parts.append(line(chart_x, y, chart_x + chart_w, y, stroke=GRID))
        parts.append(text(chart_x - 18, y + 6, label, size=16, fill="#6b7280", anchor="end"))

    for idx, label in enumerate(labels):
        value = category_precision[label]
        x = chart_x + 60 + idx * (bar_w + gap)
        h = value * chart_h
        y = chart_y + chart_h - h
        color = GREEN if value >= 0.5 else RED
        parts.append(rect(x, y, bar_w, h, color, rx=10))
        parts.append(text(x + bar_w / 2, chart_y + chart_h + 32, label.title(), size=18, anchor="middle"))
        parts.append(text(x + bar_w / 2, y - 12, f"{value:.2f}", size=18, weight="700", fill=color, anchor="middle"))

    parts.append(
        text(
            80,
            660,
            (
                f'Assessment: {assessment["status"]} | Alert precision overall: '
                f'{assessment["alert_usefulness_precision"]:.2f} | '
                f'Paper-trade hit rate: {paper_trade["hit_rate"]:.2f}'
            ),
            size=20,
            weight="600",
        )
    )
    parts.append("</svg>")
    return "\n".join(parts)


def phase6_model_vs_baselines(data: dict) -> str:
    width, height = 1280, 720
    split = data["score_report"]["splits"]["test"]
    baseline_order = [
        ("Model", split["model"]["auc"], split["model"]["precision_at_10"], RED),
        ("Severity", split["baseline_severity"]["auc"], split["baseline_severity"]["precision_at_10"], BLUE),
        ("Momentum", split["baseline_probability_momentum"]["auc"], split["baseline_probability_momentum"]["precision_at_10"], BLUE_LIGHT),
        ("Order Imb.", split["baseline_order_imbalance"]["auc"], split["baseline_order_imbalance"]["precision_at_10"], BLUE_LIGHT),
        ("Microstruct.", split["baseline_microstructure"]["auc"], split["baseline_microstructure"]["precision_at_10"], BLUE_LIGHT),
        ("External", split["baseline_external_evidence"]["auc"], split["baseline_external_evidence"]["precision_at_10"], BLUE_LIGHT),
        ("Fresh Wallet", split["baseline_fresh_wallet"]["auc"], split["baseline_fresh_wallet"]["precision_at_10"], BLUE_LIGHT),
    ]

    parts = [svg_header(width, height), rect(0, 0, width, height, BG)]
    parts.append(text(80, 70, "Phase 6 LightGBM vs Baselines", size=34, weight="700"))
    parts.append(
        text(
            80,
            108,
            "Test-split comparison using AUC and Precision@10 on the 32-row held-out test block",
            size=20,
            fill="#4b5563",
        )
    )

    parts.append(metric_card(80, 145, 220, 110, "Dataset Rows", str(data["dataset_summary"]["row_count"])))
    parts.append(metric_card(330, 145, 220, 110, "Test Rows", str(data["dataset_summary"]["test_row_count"]), accent=BLUE))
    parts.append(metric_card(580, 145, 270, 110, "Model AUC", f'{split["model"]["auc"]:.3f}', accent=RED))
    parts.append(metric_card(880, 145, 320, 110, "Model Precision@10", f'{split["model"]["precision_at_10"]:.2f}', accent=GREEN))

    chart_top = 320
    chart_h = 250
    left_x = 80
    right_x = 680
    chart_w = 520
    bar_h = 22
    gap = 10
    max_auc = 0.65
    max_p10 = 0.65

    parts.append(text(left_x, 295, "AUC", size=24, weight="700"))
    parts.append(text(right_x, 295, "Precision@10", size=24, weight="700"))

    for i, (_, auc, p10, color) in enumerate(baseline_order):
        y = chart_top + i * (bar_h + gap)
        auc_w = (auc / max_auc) * (chart_w - 140)
        p10_w = (p10 / max_p10) * (chart_w - 140)

        parts.append(text(left_x, y + 17, baseline_order[i][0], size=16))
        parts.append(rect(left_x + 120, y, chart_w - 140, bar_h, "#f3f4f6", stroke="#e5e7eb", rx=6))
        parts.append(rect(left_x + 120, y, auc_w, bar_h, color, rx=6))
        parts.append(text(left_x + 120 + auc_w + 8, y + 17, f"{auc:.3f}", size=15, weight="700"))

        parts.append(text(right_x, y + 17, baseline_order[i][0], size=16))
        parts.append(rect(right_x + 120, y, chart_w - 140, bar_h, "#f3f4f6", stroke="#e5e7eb", rx=6))
        parts.append(rect(right_x + 120, y, p10_w, bar_h, color, rx=6))
        parts.append(text(right_x + 120 + p10_w + 8, y + 17, f"{p10:.2f}", size=15, weight="700"))

    parts.append(
        text(
            80,
            660,
            "Assessment: model_beats_required_baselines | Precision@25 is 0.52 for both model and baselines on this held-out pack",
            size=20,
            weight="600",
        )
    )
    parts.append("</svg>")
    return "\n".join(parts)


def phase6_dataset_split(data: dict) -> str:
    width, height = 1280, 720
    ds = data["dataset_summary"]
    parts = [svg_header(width, height), rect(0, 0, width, height, BG)]
    parts.append(text(80, 80, "What the 96 ML Rows Mean", size=34, weight="700"))
    parts.append(
        text(
            80,
            120,
            "The ML dataset is episode-level, not raw market ticks or raw trades",
            size=20,
            fill="#4b5563",
        )
    )

    parts.append(metric_card(80, 170, 250, 120, "Total Episode Rows", str(ds["row_count"])))
    parts.append(metric_card(360, 170, 250, 120, "Train", str(ds["train_row_count"]), accent=BLUE))
    parts.append(metric_card(640, 170, 250, 120, "Validation", str(ds["validation_row_count"]), accent=GREEN))
    parts.append(metric_card(920, 170, 250, 120, "Test", str(ds["test_row_count"]), accent=RED_LIGHT))

    stack_x = 160
    stack_y = 390
    stack_w = 960
    stack_h = 80
    total = ds["row_count"]
    train_w = stack_w * ds["train_row_count"] / total
    val_w = stack_w * ds["validation_row_count"] / total
    test_w = stack_w * ds["test_row_count"] / total

    parts.append(text(80, 350, "Dataset split", size=24, weight="700"))
    parts.append(rect(stack_x, stack_y, stack_w, stack_h, "#f3f4f6", stroke="#e5e7eb", rx=12))
    parts.append(rect(stack_x, stack_y, train_w, stack_h, BLUE, rx=12))
    parts.append(rect(stack_x + train_w, stack_y, val_w, stack_h, GREEN, rx=0))
    parts.append(rect(stack_x + train_w + val_w, stack_y, test_w, stack_h, RED_LIGHT, rx=12))

    parts.append(text(stack_x + train_w / 2, stack_y + 48, "Train 32", size=24, weight="700", fill="#ffffff", anchor="middle"))
    parts.append(text(stack_x + train_w + val_w / 2, stack_y + 48, "Validation 32", size=24, weight="700", fill="#ffffff", anchor="middle"))
    parts.append(text(stack_x + train_w + val_w + test_w / 2, stack_y + 48, "Test 32", size=24, weight="700", fill="#ffffff", anchor="middle"))

    notes = [
        "Each row = one candidate/alert episode",
        "Features summarize wallet flow, imbalance, severity, and price/volume dynamics",
        "Rows carry a final success label based on the eventual outcome",
        "Used for held-out ranking evaluation, not autonomous trading",
    ]
    start_y = 560
    for idx, note in enumerate(notes):
        parts.append(text(100, start_y + idx * 32, f"• {note}", size=22))

    parts.append("</svg>")
    return "\n".join(parts)


def main() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    phase5 = load_json(PHASE5_JSON)
    phase6 = load_json(PHASE6_JSON)

    outputs = {
        OUT_DIR / "phase5_validation_overview.svg": phase5_validation_overview(phase5),
        OUT_DIR / "phase6_model_vs_baselines.svg": phase6_model_vs_baselines(phase6),
        OUT_DIR / "phase6_dataset_split.svg": phase6_dataset_split(phase6),
    }

    for path, content in outputs.items():
        path.write_text(content)
        print(path)


if __name__ == "__main__":
    main()
