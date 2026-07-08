#!/usr/bin/env python3
"""Plot Burst CVD size distributions from burst_agg summary JSONL.

Definitions:
- burst_delta_notional: 30s signed Burst CVD increment per market/window.
- burst_print_sizes: signed notional of individual prints belonging to bursts.

Outputs:
- data/burst_agg/charts/burst_cvd_delta_distribution.png
- data/burst_agg/charts/burst_print_size_distribution.png
- data/burst_agg/charts/burst_cvd_size_distribution.csv
"""

import csv
import json
from pathlib import Path
from statistics import median

import matplotlib.pyplot as plt
import numpy as np

ROOT = Path(__file__).resolve().parents[1]
SUMMARY_DIR = ROOT / "data" / "burst_agg" / "summary"
OUT_DIR = ROOT / "data" / "burst_agg" / "charts"
OUT_DIR.mkdir(parents=True, exist_ok=True)


def load_rows():
    rows = []
    print_sizes = []
    for path in sorted(SUMMARY_DIR.glob("*.jsonl")):
        market = path.stem
        with path.open() as f:
            for line in f:
                if not line.strip():
                    continue
                r = json.loads(line)
                bdelta = float(r.get("burst_delta_notional") or 0.0)
                rows.append({
                    "market": market,
                    "ts": int(r["ts"]),
                    "burst_delta_notional": bdelta,
                    "abs_burst_delta_notional": abs(bdelta),
                    "burst_count": int(r.get("burst_count") or 0),
                    "trade_count": int(r.get("trade_count") or 0),
                })
                for v in r.get("burst_print_sizes") or []:
                    fv = float(v)
                    print_sizes.append({
                        "market": market,
                        "ts": int(r["ts"]),
                        "signed_print_notional": fv,
                        "abs_print_notional": abs(fv),
                        "side": "buy" if fv > 0 else "sell" if fv < 0 else "zero",
                    })
    return rows, print_sizes


def q(vals, pct):
    if not vals:
        return 0.0
    return float(np.percentile(np.asarray(vals, dtype=float), pct))


def main():
    rows, print_sizes = load_rows()
    if not rows:
        raise SystemExit("No summary rows found")

    nonzero_deltas = [r for r in rows if r["burst_delta_notional"] != 0]
    abs_deltas = [r["abs_burst_delta_notional"] for r in nonzero_deltas]
    signed_deltas = [r["burst_delta_notional"] for r in nonzero_deltas]
    abs_prints = [p["abs_print_notional"] for p in print_sizes if p["abs_print_notional"] > 0]
    buy_prints = [p["abs_print_notional"] for p in print_sizes if p["side"] == "buy"]
    sell_prints = [p["abs_print_notional"] for p in print_sizes if p["side"] == "sell"]

    # CSV summary per market.
    csv_path = OUT_DIR / "burst_cvd_size_distribution.csv"
    by_market = {}
    for r in nonzero_deltas:
        by_market.setdefault(r["market"], []).append(r["burst_delta_notional"])

    with csv_path.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=[
            "market", "windows", "nonzero_windows", "buy_windows", "sell_windows",
            "median_abs_burst_delta", "p75_abs_burst_delta", "p90_abs_burst_delta",
            "p95_abs_burst_delta", "p99_abs_burst_delta", "max_abs_burst_delta",
            "net_burst_delta_sum",
        ])
        w.writeheader()
        for market in sorted({r["market"] for r in rows}):
            all_rows = [r for r in rows if r["market"] == market]
            vals = by_market.get(market, [])
            abs_vals = [abs(v) for v in vals]
            w.writerow({
                "market": market,
                "windows": len(all_rows),
                "nonzero_windows": len(vals),
                "buy_windows": sum(1 for v in vals if v > 0),
                "sell_windows": sum(1 for v in vals if v < 0),
                "median_abs_burst_delta": round(q(abs_vals, 50), 4),
                "p75_abs_burst_delta": round(q(abs_vals, 75), 4),
                "p90_abs_burst_delta": round(q(abs_vals, 90), 4),
                "p95_abs_burst_delta": round(q(abs_vals, 95), 4),
                "p99_abs_burst_delta": round(q(abs_vals, 99), 4),
                "max_abs_burst_delta": round(max(abs_vals) if abs_vals else 0, 4),
                "net_burst_delta_sum": round(sum(vals), 4),
            })

    # Figure 1: 30s Burst CVD increment distribution.
    fig, axes = plt.subplots(2, 2, figsize=(16, 11))
    ax = axes[0, 0]
    ax.hist(signed_deltas, bins=80, color="#4c78a8", alpha=0.85)
    ax.axvline(0, color="black", lw=1)
    ax.set_title("30s Burst CVD increment distribution (signed)")
    ax.set_xlabel("burst_delta_notional per 30s")
    ax.set_ylabel("window count")
    ax.grid(True, alpha=0.25)

    ax = axes[0, 1]
    if abs_deltas:
        bins = np.logspace(np.log10(max(min(abs_deltas), 1e-9)), np.log10(max(abs_deltas)), 80)
        ax.hist(abs_deltas, bins=bins, color="#f58518", alpha=0.85)
        ax.set_xscale("log")
    ax.set_title("30s Burst CVD increment size (absolute, log x)")
    ax.set_xlabel("abs(burst_delta_notional) per 30s")
    ax.set_ylabel("window count")
    ax.grid(True, alpha=0.25)

    ax = axes[1, 0]
    market_names = sorted(by_market)
    p95s = [q([abs(v) for v in by_market[m]], 95) for m in market_names]
    ax.barh(market_names, p95s, color="#54a24b")
    ax.set_title("p95 abs Burst CVD increment by market")
    ax.set_xlabel("p95 abs(burst_delta_notional)")
    ax.grid(True, axis="x", alpha=0.25)

    ax = axes[1, 1]
    # Stacked direction counts by market.
    buys = [sum(1 for v in by_market[m] if v > 0) for m in market_names]
    sells = [sum(1 for v in by_market[m] if v < 0) for m in market_names]
    y = np.arange(len(market_names))
    ax.barh(y, buys, color="#2ca02c", label="buy windows")
    ax.barh(y, [-s for s in sells], color="#d62728", label="sell windows")
    ax.set_yticks(y)
    ax.set_yticklabels(market_names)
    ax.axvline(0, color="black", lw=1)
    ax.set_title("Burst CVD increment direction count")
    ax.set_xlabel("windows (+buy / -sell)")
    ax.legend(loc="best")
    ax.grid(True, axis="x", alpha=0.25)

    fig.tight_layout()
    delta_path = OUT_DIR / "burst_cvd_delta_distribution.png"
    fig.savefig(delta_path, dpi=160)
    plt.close(fig)

    # Figure 2: burst print size distribution.
    fig, axes = plt.subplots(1, 2, figsize=(16, 6))
    ax = axes[0]
    if abs_prints:
        bins = np.logspace(np.log10(max(min(abs_prints), 1e-9)), np.log10(max(abs_prints)), 100)
        ax.hist(buy_prints, bins=bins, color="#2ca02c", alpha=0.55, label="buy prints")
        ax.hist(sell_prints, bins=bins, color="#d62728", alpha=0.55, label="sell prints")
        ax.set_xscale("log")
    ax.set_title("Burst print notional size distribution (absolute, log x)")
    ax.set_xlabel("abs(signed burst print notional)")
    ax.set_ylabel("print count")
    ax.legend(loc="best")
    ax.grid(True, alpha=0.25)

    ax = axes[1]
    # Boxplot by market, clipped to p99.5 to keep readable.
    box_data = []
    labels = []
    cap = q(abs_prints, 99.5) if abs_prints else 0
    for market in sorted({p["market"] for p in print_sizes}):
        vals = [p["abs_print_notional"] for p in print_sizes if p["market"] == market and p["abs_print_notional"] > 0]
        if vals:
            vals = [min(v, cap) for v in vals]
            box_data.append(vals)
            labels.append(market)
    ax.boxplot(box_data, tick_labels=labels, vert=False, showfliers=False)
    ax.set_title("Burst print notional by market (clipped at p99.5)")
    ax.set_xlabel("abs(print notional)")
    ax.grid(True, axis="x", alpha=0.25)

    fig.tight_layout()
    print_path = OUT_DIR / "burst_print_size_distribution.png"
    fig.savefig(print_path, dpi=160)
    plt.close(fig)

    print(f"summary_windows={len(rows)} nonzero_burst_windows={len(nonzero_deltas)} burst_prints={len(print_sizes)}")
    print(f"delta_abs_p50={q(abs_deltas,50):.2f} p90={q(abs_deltas,90):.2f} p95={q(abs_deltas,95):.2f} p99={q(abs_deltas,99):.2f} max={max(abs_deltas):.2f}")
    print(f"print_abs_p50={q(abs_prints,50):.2f} p90={q(abs_prints,90):.2f} p95={q(abs_prints,95):.2f} p99={q(abs_prints,99):.2f} max={max(abs_prints):.2f}")
    print(f"wrote {delta_path}")
    print(f"wrote {print_path}")
    print(f"wrote {csv_path}")


if __name__ == "__main__":
    main()
