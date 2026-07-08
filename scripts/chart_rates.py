#!/usr/bin/env python3
import argparse
import json
from collections import defaultdict
from datetime import datetime
from pathlib import Path

import matplotlib

matplotlib.use("Agg")

import matplotlib.dates as mdates
import matplotlib.pyplot as plt


BG = "#0d1117"
GRID = "#2d333b"
TEXT = "#e6edf3"
MUTED = "#8b949e"


def parse_args():
    parser = argparse.ArgumentParser(description="Render agg-btc 1min trade count chart")
    parser.add_argument("--input", default="/tmp/agg-trade-rate-history.jsonl")
    parser.add_argument("--output", default="/tmp/agg-chart.png")
    parser.add_argument("--minutes", type=int, default=60)
    return parser.parse_args()


def load_samples(history_path):
    if not history_path.exists() or history_path.stat().st_size == 0:
        return []
    entries = []
    with history_path.open("r") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                entries.append(json.loads(line))
            except json.JSONDecodeError:
                continue
    return entries


def apply_style():
    import matplotlib.font_manager as fm
    # Register Noto Sans CJK JP for Japanese text
    jp_font = "/usr/share/fonts/opentype/noto/NotoSansCJK-Regular.ttc"
    try:
        fm.fontManager.addfont(jp_font)
    except Exception:
        pass
    plt.rcParams.update({
        "font.family": "Noto Sans CJK JP",
        "axes.unicode_minus": False,
        "text.color": TEXT,
        "axes.labelcolor": TEXT,
        "axes.edgecolor": GRID,
        "xtick.color": MUTED,
        "ytick.color": MUTED,
        "grid.color": GRID,
        "figure.facecolor": BG,
        "axes.facecolor": BG,
    })


def build_palette(size):
    cmap = plt.get_cmap("tab20")
    if size <= 20:
        return [cmap(i % 20) for i in range(size)]
    return [plt.cm.hsv(i / max(size, 1)) for i in range(size)]


def render_placeholder(output_path, title, detail):
    apply_style()
    fig, ax = plt.subplots(figsize=(8, 4), dpi=120)
    fig.patch.set_facecolor(BG)
    ax.set_facecolor(BG)
    ax.text(0.5, 0.55, title, ha="center", va="center", fontsize=14, color=TEXT,
            transform=ax.transAxes)
    ax.text(0.5, 0.44, detail, ha="center", va="center", fontsize=11, color=MUTED,
            transform=ax.transAxes)
    ax.set_xlim(0, 1)
    ax.set_ylim(0, 1)
    ax.axis("off")
    fig.savefig(output_path, facecolor=BG, bbox_inches="tight")
    plt.close(fig)


def render_chart(entries, output_path, minutes):
    if not entries:
        render_placeholder(output_path, "No trade history yet",
                           "Waiting for dashboard samples.")
        return

    latest_ts = max(int(item.get("ts", 0) or 0) for item in entries)
    if latest_ts <= 0:
        render_placeholder(output_path, "No valid samples yet",
                           "History file exists but timestamps were missing.")
        return

    cutoff_ts = latest_ts - (minutes * 60 * 1000)

    # Group by market, then bucket into 1-minute intervals
    # For each market, compute 1min trade count = max(tradeMsgCount) - min(tradeMsgCount) per bucket
    market_buckets = defaultdict(lambda: defaultdict(list))

    for item in entries:
        ts = int(item.get("ts", 0) or 0)
        if ts < cutoff_ts:
            continue
        market = item.get("market")
        if not market:
            continue
        # Round down to 1-minute bucket
        bucket_ts = (ts // 60000) * 60000
        tmc = item.get("tradeMsgCount", 0)
        if tmc is not None:
            market_buckets[market][bucket_ts].append(tmc)

    # Compute 1min trade count per bucket per market
    series_by_market = {}
    latest_counts = {}
    for market, buckets in market_buckets.items():
        points = []
        for bucket_ts, values in sorted(buckets.items()):
            if len(values) < 2:
                continue
            count_1min = max(values) - min(values)
            if count_1min < 0:
                count_1min = 0
            dt = datetime.fromtimestamp(bucket_ts / 1000)
            points.append((dt, count_1min))
        if points:
            series_by_market[market] = points
            latest_counts[market] = points[-1][1]

    if not series_by_market:
        render_placeholder(output_path, "Collecting 1-minute trade data",
                           f"Need at least 2 samples per 1min bucket. Try again shortly.")
        return

    ordered_markets = sorted(
        series_by_market,
        key=lambda m: (-latest_counts.get(m, 0), m),
    )
    # Fix color assignment: sort alphabetically first, then assign palette
    # This way each market always gets the same color regardless of rate order
    fixed_order = sorted(series_by_market.keys())
    palette = build_palette(len(fixed_order))
    color_map = {market: palette[i] for i, market in enumerate(fixed_order)}
    main_markets = set(ordered_markets[:8])

    apply_style()
    fig, ax = plt.subplots(figsize=(7, 9), dpi=150)
    fig.patch.set_facecolor(BG)
    ax.set_facecolor(BG)

    for idx, market in enumerate(ordered_markets):
        series = sorted(series_by_market[market], key=lambda p: p[0])
        xs = [p[0] for p in series]
        ys = [p[1] for p in series]
        is_main = market in main_markets
        label = f"{market}  {latest_counts.get(market, 0):,}/min"
        ax.plot(
            xs, ys,
            label=label,
            color=color_map[market],
            linewidth=2.0 if is_main else 1.2,
            alpha=0.95 if is_main else 0.65,
        )

    ax.set_title("取引メッセージ数 — 1分合計（直近60分）", fontsize=16, loc="left", pad=16, fontweight="bold")
    ax.set_ylabel("メッセージ/分", fontsize=13)
    ax.yaxis.set_label_position("right")
    ax.yaxis.tick_right()
    ax.tick_params(axis="both", labelsize=11)
    ax.grid(True, which="major", linestyle="-", linewidth=0.8, alpha=0.9)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M"))
    ax.xaxis.set_major_locator(mdates.AutoDateLocator(minticks=6, maxticks=10))
    ax.tick_params(axis="x", rotation=0)
    ax.margins(x=0.01, y=0.08)

    legend = ax.legend(
        loc="upper center",
        bbox_to_anchor=(0.5, -0.12),
        frameon=False,
        fontsize=10,
        ncol=3,
    )
    for text in legend.get_texts():
        text.set_color(TEXT)

    latest_dt = datetime.fromtimestamp(latest_ts / 1000)
    ax.text(
        0.0, 1.02,
        f"更新: {latest_dt.strftime('%Y-%m-%d %H:%M:%S')}",
        transform=ax.transAxes,
        fontsize=10,
        color=MUTED,
    )

    fig.tight_layout(rect=(0.0, 0.0, 1.0, 0.92))
    fig.savefig(output_path, facecolor=BG)
    plt.close(fig)


def main():
    args = parse_args()
    history_path = Path(args.input)
    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    entries = load_samples(history_path)
    render_chart(entries, output_path, args.minutes)


if __name__ == "__main__":
    main()
