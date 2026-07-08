#!/usr/bin/env python3
"""Analyze 3-level Burst CVD size thresholds for spot vs perp.

Uses 30s summary burst_delta_notional as Burst CVD increment.
Outputs:
- data/burst_agg/charts/burst_cvd_spot_perp_thresholds.csv
- data/burst_agg/charts/burst_cvd_spot_perp_thresholds.png
"""

import csv
import json
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np

ROOT = Path(__file__).resolve().parents[1]
SUMMARY_DIR = ROOT / "data" / "burst_agg" / "summary"
OUT_DIR = ROOT / "data" / "burst_agg" / "charts"
OUT_DIR.mkdir(parents=True, exist_ok=True)


def group_of(market: str) -> str:
    return "perp" if "perp" in market else "spot"


def q(vals, pct):
    if not vals:
        return 0.0
    return float(np.percentile(np.asarray(vals, dtype=float), pct))


def main():
    rows = []
    for path in sorted(SUMMARY_DIR.glob("*.jsonl")):
        market = path.stem
        group = group_of(market)
        with path.open() as f:
            for line in f:
                if not line.strip():
                    continue
                r = json.loads(line)
                bdelta = float(r.get("burst_delta_notional") or 0.0)
                if bdelta == 0:
                    continue
                rows.append({
                    "market": market,
                    "group": group,
                    "ts": int(r["ts"]),
                    "signed": bdelta,
                    "abs": abs(bdelta),
                    "direction": "buy" if bdelta > 0 else "sell",
                })

    groups = {"spot": [], "perp": []}
    for r in rows:
        groups[r["group"]].append(r["abs"])

    # Candidate three-level scheme:
    # small: < p50, medium: p50..p90, large: >= p90.
    # Also expose p95/p99 for stricter future 4/5-level use.
    csv_path = OUT_DIR / "burst_cvd_spot_perp_thresholds.csv"
    with csv_path.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=[
            "group", "n", "p25", "p50_small_medium", "p75", "p90_medium_large", "p95", "p99", "max",
            "buy_count", "sell_count",
        ])
        w.writeheader()
        for g in ["spot", "perp"]:
            vals = groups[g]
            signed = [r for r in rows if r["group"] == g]
            w.writerow({
                "group": g,
                "n": len(vals),
                "p25": round(q(vals, 25), 4),
                "p50_small_medium": round(q(vals, 50), 4),
                "p75": round(q(vals, 75), 4),
                "p90_medium_large": round(q(vals, 90), 4),
                "p95": round(q(vals, 95), 4),
                "p99": round(q(vals, 99), 4),
                "max": round(max(vals) if vals else 0, 4),
                "buy_count": sum(1 for r in signed if r["signed"] > 0),
                "sell_count": sum(1 for r in signed if r["signed"] < 0),
            })

    # Count categories using candidate thresholds.
    category_rows = []
    for g in ["spot", "perp"]:
        vals = groups[g]
        t1 = q(vals, 50)
        t2 = q(vals, 90)
        for label, pred in [
            ("small", lambda x: x < t1),
            ("medium", lambda x: t1 <= x < t2),
            ("large", lambda x: x >= t2),
        ]:
            category_rows.append((g, label, sum(1 for x in vals if pred(x))))

    # Plot distributions.
    fig, axes = plt.subplots(2, 2, figsize=(15, 10))
    for ax, g, color in [(axes[0, 0], "spot", "#1f77b4"), (axes[0, 1], "perp", "#ff7f0e")]:
        vals = groups[g]
        if vals:
            bins = np.logspace(np.log10(max(min(vals), 1e-9)), np.log10(max(vals)), 90)
            ax.hist(vals, bins=bins, color=color, alpha=0.8)
            ax.set_xscale("log")
            for pct, ls in [(50, "--"), (90, "-"), (95, ":")]:
                ax.axvline(q(vals, pct), color="black", linestyle=ls, linewidth=1, label=f"p{pct}={q(vals,pct):,.0f}")
        ax.set_title(f"{g.upper()} abs Burst CVD increment distribution")
        ax.set_xlabel("abs(burst_delta_notional) per 30s")
        ax.set_ylabel("window count")
        ax.grid(True, alpha=0.25)
        ax.legend(loc="best")

    # Category counts
    for ax, g in [(axes[1, 0], "spot"), (axes[1, 1], "perp")]:
        labels = [r[1] for r in category_rows if r[0] == g]
        counts = [r[2] for r in category_rows if r[0] == g]
        ax.bar(labels, counts, color=["#8dd3c7", "#ffffb3", "#fb8072"])
        ax.set_title(f"{g.upper()} candidate 3-level bucket counts")
        ax.set_ylabel("window count")
        ax.grid(True, axis="y", alpha=0.25)
        total = sum(counts)
        for i, c in enumerate(counts):
            ax.text(i, c, f"{c}\n{c/total*100:.1f}%" if total else "0", ha="center", va="bottom")

    fig.tight_layout()
    fig_path = OUT_DIR / "burst_cvd_spot_perp_thresholds.png"
    fig.savefig(fig_path, dpi=160)
    plt.close(fig)

    print(f"rows={len(rows)} spot={len(groups['spot'])} perp={len(groups['perp'])}")
    for g in ["spot", "perp"]:
        vals = groups[g]
        print(g, {
            "p50": round(q(vals, 50), 2),
            "p90": round(q(vals, 90), 2),
            "p95": round(q(vals, 95), 2),
            "p99": round(q(vals, 99), 2),
            "max": round(max(vals) if vals else 0, 2),
        })
    print(f"wrote {csv_path}")
    print(f"wrote {fig_path}")


if __name__ == "__main__":
    main()
