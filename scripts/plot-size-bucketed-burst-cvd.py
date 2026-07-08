#!/usr/bin/env python3
"""Plot size-bucketed Burst CVD using selected spot/perp thresholds.

Metric: signed burst_delta_notional per 30s summary row.
Size bucket is based on abs(burst_delta_notional):
- spot: small < 10k, medium < 100k, large >= 100k
- perp: small < 100k, medium < 1.0M, large >= 1.0M

Outputs:
- data/burst_agg/charts/size_bucketed_burst_cvd_spot_perp.png
- data/burst_agg/charts/size_bucketed_burst_cvd_by_market.png
- data/burst_agg/charts/size_bucketed_burst_cvd.csv
"""

import csv
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


def group_of(market: str) -> str:
    return "perp" if "perp" in market else "spot"


def bucket_for(group: str, signed_delta: float) -> str:
    x = abs(signed_delta)
    if x == 0:
        return "zero"
    t = THRESHOLDS[group]
    if x < t["small_medium"]:
        return "small"
    if x < t["medium_large"]:
        return "medium"
    return "large"


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
                bucket = bucket_for(group, signed)
                rows.append({
                    "ts": int(r["ts"]),
                    "dt": datetime.fromtimestamp(int(r["ts"]) / 1000, tz=timezone.utc),
                    "market": market,
                    "group": group,
                    "bucket": bucket,
                    "burst_delta_notional": signed,
                    "abs_burst_delta_notional": abs(signed),
                    "burst_count": int(r.get("burst_count") or 0),
                    "trade_count": int(r.get("trade_count") or 0),
                })
    rows.sort(key=lambda r: (r["ts"], r["market"]))
    return rows


def cumulative_by_key(rows, key_fields):
    """Return long rows with cumulative CVD for each key tuple and bucket."""
    grouped = defaultdict(lambda: {b: 0.0 for b in BUCKETS})
    out = []
    for r in rows:
        if r["bucket"] not in BUCKETS:
            continue
        key = tuple(r[k] for k in key_fields)
        grouped[key][r["bucket"]] += r["burst_delta_notional"]
        row = {k: r[k] for k in key_fields}
        row.update({"ts": r["ts"], "dt": r["dt"]})
        for b in BUCKETS:
            row[f"{b}_burst_cvd"] = grouped[key][b]
        out.append(row)
    return out


def build_dense_group_series(rows):
    """Aggregate per group per timestamp, then cumulative per bucket."""
    inc = defaultdict(lambda: {b: 0.0 for b in BUCKETS})
    for r in rows:
        if r["bucket"] in BUCKETS:
            inc[(r["group"], r["ts"], r["dt"])][r["bucket"]] += r["burst_delta_notional"]

    cum = {"spot": {b: 0.0 for b in BUCKETS}, "perp": {b: 0.0 for b in BUCKETS}}
    series = {"spot": [], "perp": []}
    for group in ("spot", "perp"):
        keys = sorted([k for k in inc if k[0] == group], key=lambda k: k[1])
        for _, ts, dt in keys:
            for b in BUCKETS:
                cum[group][b] += inc[(group, ts, dt)][b]
            series[group].append({
                "ts": ts,
                "dt": dt,
                **{f"{b}_burst_cvd": cum[group][b] for b in BUCKETS},
            })
    return series


def save_csv(rows):
    csv_path = OUT_DIR / "size_bucketed_burst_cvd.csv"
    # CSV at market-window increment level with bucket and thresholds.
    with csv_path.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=[
            "ts", "iso", "market", "group", "bucket", "burst_delta_notional",
            "abs_burst_delta_notional", "small_medium_threshold", "medium_large_threshold",
            "burst_count", "trade_count",
        ])
        w.writeheader()
        for r in rows:
            t = THRESHOLDS[r["group"]]
            w.writerow({
                "ts": r["ts"],
                "iso": r["dt"].isoformat(),
                "market": r["market"],
                "group": r["group"],
                "bucket": r["bucket"],
                "burst_delta_notional": round(r["burst_delta_notional"], 8),
                "abs_burst_delta_notional": round(r["abs_burst_delta_notional"], 8),
                "small_medium_threshold": t["small_medium"],
                "medium_large_threshold": t["medium_large"],
                "burst_count": r["burst_count"],
                "trade_count": r["trade_count"],
            })
    return csv_path


def money_axis(ax):
    ax.axhline(0, color="black", lw=0.7, alpha=0.5)
    ax.grid(True, alpha=0.25)
    ax.ticklabel_format(axis="y", style="plain")
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M"))


def plot_group(series):
    fig, axes = plt.subplots(2, 1, figsize=(16, 10), sharex=True)
    for ax, group in zip(axes, ["spot", "perp"]):
        s = series[group]
        xs = [r["dt"] for r in s]
        for b in BUCKETS:
            ax.plot(xs, [r[f"{b}_burst_cvd"] for r in s], label=f"{b}", color=COLORS[b], lw=1.5)
        t = THRESHOLDS[group]
        ax.set_title(f"{group.upper()} size-bucketed Burst CVD  |  small<{t['small_medium']:,.0f}, medium<{t['medium_large']:,.0f}, large≥{t['medium_large']:,.0f}")
        ax.legend(loc="best")
        money_axis(ax)
    fig.tight_layout()
    out = OUT_DIR / "size_bucketed_burst_cvd_spot_perp.png"
    fig.savefig(out, dpi=160)
    plt.close(fig)
    return out


def plot_by_market(rows):
    markets = sorted({r["market"] for r in rows})
    n = len(markets)
    cols = 3
    rows_n = (n + cols - 1) // cols
    fig, axes = plt.subplots(rows_n, cols, figsize=(18, rows_n * 3.4), sharex=True)
    axes = axes.ravel()

    # Build cumulative per market.
    per_market = {}
    for market in markets:
        ms = sorted([r for r in rows if r["market"] == market], key=lambda r: r["ts"])
        cum = {b: 0.0 for b in BUCKETS}
        series = []
        for r in ms:
            if r["bucket"] in BUCKETS:
                cum[r["bucket"]] += r["burst_delta_notional"]
            series.append({"dt": r["dt"], **{f"{b}_burst_cvd": cum[b] for b in BUCKETS}})
        per_market[market] = series

    for ax, market in zip(axes, markets):
        s = per_market[market]
        xs = [r["dt"] for r in s]
        for b in BUCKETS:
            ax.plot(xs, [r[f"{b}_burst_cvd"] for r in s], label=b, color=COLORS[b], lw=1.0)
        ax.set_title(market)
        money_axis(ax)
    for ax in axes[n:]:
        ax.axis("off")
    handles, labels = axes[0].get_legend_handles_labels()
    fig.legend(handles, labels, loc="upper center", ncol=3)
    fig.suptitle("Size-bucketed Burst CVD by Market", y=0.995, fontsize=16)
    fig.tight_layout(rect=(0, 0, 1, 0.97))
    out = OUT_DIR / "size_bucketed_burst_cvd_by_market.png"
    fig.savefig(out, dpi=160)
    plt.close(fig)
    return out


def summarize(rows):
    summary = defaultdict(lambda: defaultdict(lambda: {"n": 0, "sum": 0.0}))
    for r in rows:
        if r["bucket"] in BUCKETS:
            d = summary[r["group"]][r["bucket"]]
            d["n"] += 1
            d["sum"] += r["burst_delta_notional"]
    return summary


def main():
    rows = read_rows()
    if not rows:
        raise SystemExit("No burst_agg summary rows found")
    csv_path = save_csv(rows)
    group_series = build_dense_group_series(rows)
    group_png = plot_group(group_series)
    market_png = plot_by_market(rows)
    summary = summarize(rows)

    print(f"rows={len(rows)}")
    for group in ["spot", "perp"]:
        print(group)
        for b in BUCKETS:
            d = summary[group][b]
            print(f"  {b}: n={d['n']} cvd={d['sum']:.2f}")
    print(f"wrote {group_png}")
    print(f"wrote {market_png}")
    print(f"wrote {csv_path}")


if __name__ == "__main__":
    main()
