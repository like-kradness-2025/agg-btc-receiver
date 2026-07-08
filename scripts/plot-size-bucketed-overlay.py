#!/usr/bin/env python3
"""Plot small/medium/large Burst CVD overlaid on a single chart with independent y-axes.
"""

import json
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

import matplotlib.dates as mdates
import matplotlib.pyplot as plt

ROOT = Path(__file__).resolve().parents[1]
SUMMARY_DIR = ROOT / "data" / "burst_agg" / "summary"
OUT_DIR = ROOT / "data" / "burst_agg" / "charts"
OUT_DIR.mkdir(parents=True, exist_ok=True)

THRESHOLDS = {
    "spot": {"small_medium": 10_000.0, "medium_large": 100_000.0},
    "perp": {"small_medium": 100_000.0, "medium_large": 1_000_000.0},
}
BUCKETS = ("small", "medium", "large")
COLORS = {"small": "#4c78a8", "medium": "#f58518", "large": "#e45756"}
YLIM_MARGIN = 0.08


def group_of(market):
    return "perp" if "perp" in market else "spot"


def read_rows():
    rows = []
    for path in sorted(SUMMARY_DIR.glob("*.jsonl")):
        market = path.stem
        group = group_of(market)
        with path.open() as f:
            for line in f:
                if not line.strip():
                    continue
                r = json.loads(line)
                signed = float(r.get("burst_delta_notional") or 0.0)
                x = abs(signed)
                t = THRESHOLDS[group]
                if x == 0:
                    continue
                if x < t["small_medium"]:
                    bucket = "small"
                elif x < t["medium_large"]:
                    bucket = "medium"
                else:
                    bucket = "large"
                rows.append({
                    "ts": int(r["ts"]),
                    "dt": datetime.fromtimestamp(int(r["ts"]) / 1000, tz=timezone.utc),
                    "market": market,
                    "group": group,
                    "bucket": bucket,
                    "signed": signed,
                })
    return rows


def cumulative(rows, group=None, market=None):
    cum = {b: 0.0 for b in BUCKETS}
    ts_rows = defaultdict(lambda: {b: 0.0 for b in BUCKETS})
    for r in rows:
        if group and r["group"] != group:
            continue
        if market and r["market"] != market:
            continue
        ts_rows[(r["ts"], r["dt"])][r["bucket"]] += r["signed"]
    series = []
    for ts, dt in sorted(ts_rows):
        for b in BUCKETS:
            cum[b] += ts_rows[(ts, dt)][b]
        series.append({"dt": dt, **{f"{b}_cvd": cum[b] for b in BUCKETS}})
    return series


def plot_overlay(rows, group, title):
    series = cumulative(rows, group=group)
    if not series:
        return None
    xs = [r["dt"] for r in series]
    t = THRESHOLDS[group]

    fig, ax1 = plt.subplots(figsize=(16, 7))
    ax2 = ax1.twinx()
    ax3 = ax1.twinx()
    ax3.spines["right"].set_position(("axes", 1.12))

    axes_map = {"small": ax1, "medium": ax2, "large": ax3}
    ylabels = {
        "small": f"small (<{t['small_medium']/1000:.0f}K)",
        "medium": f"medium ({t['small_medium']/1000:.0f}K-{t['medium_large']/1000:.0f}K)",
        "large": f"large (≥{t['medium_large']/1000:.0f}K)",
    }

    for b, ax in axes_map.items():
        ys = [r[f"{b}_cvd"] for r in series]
        ax.plot(xs, ys, color=COLORS[b], lw=1.8, label=b)
        ax.axhline(0, color="black", lw=0.4, alpha=0.4)
        ax.set_ylabel(ylabels[b], color=COLORS[b], fontsize=10)
        ax.tick_params(axis="y", colors=COLORS[b])
        ax.ticklabel_format(axis="y", style="plain")
        ax.grid(True, alpha=0.15)

    ax1.set_xlabel("Time (UTC)")
    ax1.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M"))
    ax1.set_title(
        f"{title}  |  "
        f"small <{t['small_medium']:,.0f}, medium <{t['medium_large']:,.0f}, large ≥{t['medium_large']:,.0f}",
        fontsize=12,
    )

    # Legend
    lines = [plt.Line2D([0], [0], color=COLORS[b], lw=1.8) for b in BUCKETS]
    labels = [f"{b} ({ylabels[b]})" for b in BUCKETS]
    ax1.legend(lines, labels, loc="upper left", fontsize=9)

    fig.tight_layout()
    out = OUT_DIR / f"size_bucketed_overlay_{group}.png"
    fig.savefig(out, dpi=160, bbox_inches="tight")
    plt.close(fig)
    return out


def plot_market_overlay(rows, market):
    series = cumulative(rows, market=market)
    if not series:
        return None
    xs = [r["dt"] for r in series]
    group = group_of(market)
    t = THRESHOLDS[group]

    fig, ax1 = plt.subplots(figsize=(16, 6))
    ax2 = ax1.twinx()
    ax3 = ax1.twinx()
    ax3.spines["right"].set_position(("axes", 1.12))

    axes_map = {"small": ax1, "medium": ax2, "large": ax3}
    ylabels = {
        "small": f"small (<{t['small_medium']/1000:.0f}K)",
        "medium": f"medium ({t['small_medium']/1000:.0f}K-{t['medium_large']/1000:.0f}K)",
        "large": f"large (≥{t['medium_large']/1000:.0f}K)",
    }

    for b, ax in axes_map.items():
        ys = [r[f"{b}_cvd"] for r in series]
        ax.plot(xs, ys, color=COLORS[b], lw=1.4, label=b)
        ax.axhline(0, color="black", lw=0.4, alpha=0.4)
        ax.set_ylabel(ylabels[b], color=COLORS[b], fontsize=9)
        ax.tick_params(axis="y", colors=COLORS[b])
        ax.ticklabel_format(axis="y", style="plain")
        ax.grid(True, alpha=0.15)

    ax1.set_xlabel("Time (UTC)")
    ax1.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M"))
    ax1.set_title(
        f"{market}  |  "
        f"small <{t['small_medium']:,.0f}, medium <{t['medium_large']:,.0f}, large ≥{t['medium_large']:,.0f}",
        fontsize=11,
    )

    lines = [plt.Line2D([0], [0], color=COLORS[b], lw=1.4) for b in BUCKETS]
    labels = list(axes_map.keys())
    ax1.legend(lines, labels, loc="upper left", fontsize=9)

    fig.tight_layout()
    out = OUT_DIR / f"size_bucketed_overlay_{market}.png"
    fig.savefig(out, dpi=160, bbox_inches="tight")
    plt.close(fig)
    return out


def main():
    rows = read_rows()
    if not rows:
        raise SystemExit("No burst_agg summary rows found")

    spot_png = plot_overlay(rows, "spot", "SPOT Burst CVD")
    perp_png = plot_overlay(rows, "perp", "PERP Burst CVD")

    market_pngs = []
    for market in sorted({r["market"] for r in rows}):
        p = plot_market_overlay(rows, market)
        if p:
            market_pngs.append(p)

    print(f"rows={len(rows)}")
    print(f"wrote {spot_png}")
    print(f"wrote {perp_png}")
    for p in market_pngs:
        print(f"wrote {p}")


if __name__ == "__main__":
    main()
