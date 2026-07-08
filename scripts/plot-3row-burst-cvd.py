#!/usr/bin/env python3
"""Compact 3-row burst CVD chart.

Reads:
  data/burst_agg/summary/*.jsonl

Writes:
  data/burst_agg/charts/btc_price_spot_perp_burst_cvd.png
"""

import json
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

import matplotlib

matplotlib.use("Agg")

import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
from matplotlib.lines import Line2D

ROOT = Path(__file__).resolve().parents[1]
SUMMARY_DIR = ROOT / "data" / "burst_agg" / "summary"
CHARTS_DIR = ROOT / "data" / "burst_agg" / "charts"
CHARTS_DIR.mkdir(parents=True, exist_ok=True)

THRESHOLDS = {
    "spot": {"small_medium": 10_000.0, "medium_large": 100_000.0},
    "perp": {"small_medium": 100_000.0, "medium_large": 1_000_000.0},
}
BUCKETS = ("small", "medium", "large")
COLORS = {
    "small": "#22d3ee",
    "medium": "#f59e0b",
    "large": "#f43f5e",
    "price": "#93c5fd",
}
BG_COLOR = "#1a1a2e"
PANEL_COLOR = "#16213e"
GRID_COLOR = "#2d405f"
TEXT_COLOR = "#e5e7eb"
MUTED_TEXT = "#94a3b8"

Y_AXIS_OFFSETS = {
    "small": 0,
    "medium": 38,
    "large": 76,
}


def group_of(market: str) -> str:
    return "perp" if "perp" in market else "spot"


def classify_bucket(group: str, signed: float) -> str | None:
    size = abs(signed)
    if size == 0:
        return None
    threshold = THRESHOLDS[group]
    if size < threshold["small_medium"]:
        return "small"
    if size < threshold["medium_large"]:
        return "medium"
    return "large"


def read_rows():
    rows = []
    for path in sorted(SUMMARY_DIR.glob("*.jsonl")):
        market = path.stem
        group = group_of(market)
        with path.open() as handle:
            for line in handle:
                if not line.strip():
                    continue
                raw = json.loads(line)
                ts = int(raw["ts"])
                signed = float(raw.get("burst_delta_notional") or 0.0)
                rows.append({
                    "ts": ts,
                    "dt": datetime.fromtimestamp(ts / 1000, tz=timezone.utc),
                    "market": market,
                    "group": group,
                    "bucket": classify_bucket(group, signed),
                    "signed": signed,
                    "close": float(raw.get("close") or 0.0),
                })
    return rows


def build_group_cvd(rows, group: str):
    cumulative = {bucket: 0.0 for bucket in BUCKETS}
    by_ts = defaultdict(lambda: {"dt": None, "buckets": {bucket: 0.0 for bucket in BUCKETS}})

    for row in rows:
        if row["group"] != group:
            continue
        cell = by_ts[row["ts"]]
        cell["dt"] = row["dt"]
        if row["bucket"] is not None:
            cell["buckets"][row["bucket"]] += row["signed"]

    series = []
    for ts in sorted(by_ts):
        cell = by_ts[ts]
        for bucket in BUCKETS:
            cumulative[bucket] += cell["buckets"][bucket]
        item = {"ts": ts, "dt": cell["dt"]}
        for bucket in BUCKETS:
            item[f"{bucket}_cvd"] = cumulative[bucket]
        series.append(item)
    return series


def build_btc_price(rows):
    preferred = defaultdict(list)
    fallback_perp = defaultdict(list)
    fallback_any = defaultdict(list)

    for row in rows:
        close = row["close"]
        if close <= 0:
            continue
        ts = row["ts"]
        if row["market"] == "binance_perp":
            preferred[ts].append(close)
        if row["group"] == "perp":
            fallback_perp[ts].append(close)
        fallback_any[ts].append(close)

    source = preferred or fallback_perp or fallback_any
    return [
        {
            "dt": datetime.fromtimestamp(ts / 1000, tz=timezone.utc),
            "price": sum(values) / len(values),
        }
        for ts, values in sorted(source.items())
    ]


def apply_theme():
    plt.rcParams.update({
        "figure.facecolor": BG_COLOR,
        "axes.facecolor": PANEL_COLOR,
        "savefig.facecolor": BG_COLOR,
        "axes.edgecolor": GRID_COLOR,
        "axes.labelcolor": TEXT_COLOR,
        "text.color": TEXT_COLOR,
        "xtick.color": MUTED_TEXT,
        "ytick.color": MUTED_TEXT,
        "grid.color": GRID_COLOR,
        "grid.alpha": 0.30,
        "grid.linestyle": "-",
        "font.size": 13,
        "axes.titlesize": 16,
        "axes.labelsize": 14,
        "xtick.labelsize": 13,
        "ytick.labelsize": 13,
        "figure.dpi": 120,
    })


def format_compact(value, _):
    abs_value = abs(value)
    if abs_value >= 1_000_000:
        return f"{value / 1_000_000:.1f}M"
    if abs_value >= 1_000:
        return f"{value / 1_000:.0f}K"
    return f"{value:.0f}"


def style_panel_frame(ax):
    ax.spines["top"].set_visible(False)
    ax.spines["left"].set_visible(False)
    ax.spines["right"].set_color(GRID_COLOR)
    ax.spines["bottom"].set_color(GRID_COLOR)
    ax.grid(axis="x")
    ax.grid(axis="y", alpha=0.18)
    ax.tick_params(axis="x", pad=6)


def threshold_label(group: str, bucket: str) -> str:
    threshold = THRESHOLDS[group]
    sm = threshold["small_medium"]
    ml = threshold["medium_large"]
    if bucket == "small":
        return f"Small <{sm/1000:.0f}K"
    if bucket == "medium":
        return f"Medium {sm/1000:.0f}K-{ml/1000:.0f}K"
    if ml >= 1_000_000:
        return f"Large >={ml/1_000_000:.0f}M"
    return f"Large >={ml/1000:.0f}K"


def add_cvd_panel(ax, series, group: str):
    base = ax
    extra_axes = {
        "small": base,
        "medium": base.twinx(),
        "large": base.twinx(),
    }

    for bucket, axis in extra_axes.items():
        axis.patch.set_alpha(0.0)
        axis.spines["top"].set_visible(False)
        axis.spines["left"].set_visible(False)
        axis.spines["bottom"].set_visible(False)
        axis.spines["right"].set_position(("outward", Y_AXIS_OFFSETS[bucket]))
        axis.spines["right"].set_color(COLORS[bucket])
        axis.yaxis.set_label_position("right")
        axis.yaxis.tick_right()
        axis.tick_params(axis="y", colors=COLORS[bucket], labelsize=13, pad=4, width=0.8)
        axis.yaxis.set_major_formatter(mticker.FuncFormatter(format_compact))
        axis.set_ylabel(f"{bucket.title()} CVD", color=COLORS[bucket], rotation=270, labelpad=12)
        axis.grid(False)

        values = [row[f"{bucket}_cvd"] for row in series]
        limit = max((abs(value) for value in values), default=1.0)
        limit = max(limit * 1.12, 1.0)
        axis.set_ylim(-limit, limit)
        axis.step(
            [row["dt"] for row in series],
            values,
            where="post",
            color=COLORS[bucket],
            linewidth=1.5,
        )

    style_panel_frame(base)
    base.axhline(0.0, color=GRID_COLOR, linewidth=1.0, alpha=0.7)
    base.set_title(f"{group.title()} Burst CVD", loc="left", fontweight="bold")

    legend = base.legend(
        handles=[
            Line2D([0], [0], color=COLORS[bucket], lw=2.0, label=threshold_label(group, bucket))
            for bucket in BUCKETS
        ],
        loc="upper left",
        ncol=3,
        fontsize=12,
        frameon=False,
        handlelength=2.4,
        columnspacing=1.4,
    )
    for text, bucket in zip(legend.get_texts(), BUCKETS):
        text.set_color(COLORS[bucket])


def main():
    apply_theme()

    rows = read_rows()
    if not rows:
        raise SystemExit("No summary data found in data/burst_agg/summary/*.jsonl")

    btc = build_btc_price(rows)
    spot_cvd = build_group_cvd(rows, "spot")
    perp_cvd = build_group_cvd(rows, "perp")
    if not btc or not spot_cvd or not perp_cvd:
        raise SystemExit("Insufficient data to plot BTC/spot/perp panels")

    fig, (ax_price, ax_spot, ax_perp) = plt.subplots(
        3,
        1,
        figsize=(12, 7),
        sharex=True,
        gridspec_kw={"height_ratios": [1.0, 1.1, 1.1]},
    )
    fig.subplots_adjust(left=0.07, right=0.77, top=0.92, bottom=0.10, hspace=0.16)

    style_panel_frame(ax_price)
    ax_price.plot(
        [row["dt"] for row in btc],
        [row["price"] for row in btc],
        color=COLORS["price"],
        linewidth=1.6,
    )
    ax_price.set_title("BTC Price", loc="left", fontweight="bold")
    ax_price.yaxis.set_label_position("right")
    ax_price.yaxis.tick_right()
    ax_price.tick_params(axis="y", colors=COLORS["price"], labelsize=13, pad=4)
    ax_price.spines["right"].set_color(COLORS["price"])
    ax_price.set_ylabel("BTC Price", color=COLORS["price"], rotation=270, labelpad=16)
    ax_price.yaxis.set_major_formatter(mticker.FuncFormatter(lambda value, _: f"${value:,.0f}"))
    ax_price.tick_params(labelbottom=False)

    add_cvd_panel(ax_spot, spot_cvd, "spot")
    ax_spot.tick_params(labelbottom=False)

    add_cvd_panel(ax_perp, perp_cvd, "perp")

    locator = mdates.AutoDateLocator(minticks=5, maxticks=8)
    formatter = mdates.ConciseDateFormatter(locator)
    ax_perp.xaxis.set_major_locator(locator)
    ax_perp.xaxis.set_major_formatter(formatter)
    ax_perp.set_xlabel("Time (UTC)", fontsize=14, color=TEXT_COLOR)

    fig.suptitle("BTC Price / Spot Burst CVD / Perp Burst CVD", fontsize=17, fontweight="bold")

    out = CHARTS_DIR / "btc_price_spot_perp_burst_cvd.png"
    fig.savefig(out, dpi=140, pad_inches=0.12)
    plt.close(fig)
    print(f"wrote {out}")


if __name__ == "__main__":
    main()
