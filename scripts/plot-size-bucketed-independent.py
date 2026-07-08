#!/usr/bin/env python3
"""Plot size-bucketed Burst CVD with independent y-axes per subplot.

Spot/perp合算 + market別、すべて縦軸独立。
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
YLABELS = {"small": "small\n(<10K / <100K)", "medium": "medium\n(10K-100K / 100K-1M)", "large": "large\n(≥100K / ≥1M)"}


def group_of(market: str) -> str:
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


def plot_group_panel(rows, group):
    series = cumulative(rows, group=group)
    if not series:
        return None
    xs = [r["dt"] for r in series]
    t = THRESHOLDS[group]
    fig, axes = plt.subplots(3, 1, figsize=(16, 12), sharex=True)
    for ax, b in zip(axes, BUCKETS):
        ys = [r[f"{b}_cvd"] for r in series]
        ax.plot(xs, ys, color=COLORS[b], lw=1.5)
        ax.axhline(0, color="black", lw=0.5, alpha=0.5)
        ax.set_ylabel(f"{b}\n({t['small_medium']:,} / {t['medium_large']:,})", fontsize=10)
        ax.set_title(f"{group.upper()} Burst CVD — {b}", fontsize=11, loc="left")
        ax.grid(True, alpha=0.25)
        ax.ticklabel_format(axis="y", style="plain")
    axes[-1].xaxis.set_major_formatter(mdates.DateFormatter("%H:%M"))
    fig.suptitle(
        f"{group.upper()} size-bucketed Burst CVD  |  "
        f"small <{t['small_medium']:,.0f}, medium <{t['medium_large']:,.0f}, large ≥{t['medium_large']:,.0f}",
        y=1.005,
        fontsize=13,
    )
    fig.tight_layout(rect=(0, 0, 1, 1))
    out = OUT_DIR / f"size_bucketed_cvd_independent_{group}.png"
    fig.savefig(out, dpi=160, bbox_inches="tight")
    plt.close(fig)
    return out


def plot_by_market(rows):
    markets = sorted({r["market"] for r in rows})
    figs = []
    for market in markets:
        series = cumulative(rows, market=market)
        if not series:
            continue
        xs = [r["dt"] for r in series]
        group = group_of(market)
        t = THRESHOLDS[group]
        fig, axes = plt.subplots(3, 1, figsize=(16, 10), sharex=True)
        for ax, b in zip(axes, BUCKETS):
            ys = [r[f"{b}_cvd"] for r in series]
            ax.plot(xs, ys, color=COLORS[b], lw=1.2)
            ax.axhline(0, color="black", lw=0.5, alpha=0.5)
            ax.set_ylabel(b, fontsize=10)
            ax.set_title(f"{b}", fontsize=10, loc="left")
            ax.grid(True, alpha=0.25)
            ax.ticklabel_format(axis="y", style="plain")
        axes[-1].xaxis.set_major_formatter(mdates.DateFormatter("%H:%M"))
        fig.suptitle(
            f"{market}  |  "
            f"small <{t['small_medium']:,.0f}, medium <{t['medium_large']:,.0f}, large ≥{t['medium_large']:,.0f}",
            y=1.005,
            fontsize=12,
        )
        fig.tight_layout(rect=(0, 0, 1, 1))
        out = OUT_DIR / f"size_bucketed_cvd_independent_{market}.png"
        fig.savefig(out, dpi=160, bbox_inches="tight")
        plt.close(fig)
        figs.append(out)
    return figs


def main():
    rows = read_rows()
    if not rows:
        raise SystemExit("No burst_agg summary rows found")

    spot_png = plot_group_panel(rows, "spot")
    perp_png = plot_group_panel(rows, "perp")
    market_pngs = plot_by_market(rows)

    print(f"rows={len(rows)}")
    print(f"wrote {spot_png}")
    print(f"wrote {perp_png}")
    for p in market_pngs:
        print(f"wrote {p}")


if __name__ == "__main__":
    main()
