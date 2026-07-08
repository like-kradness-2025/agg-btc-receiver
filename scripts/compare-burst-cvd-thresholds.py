#!/usr/bin/env python3
"""Compare BurstCVD three-level threshold candidates.

Metric: abs(burst_delta_notional) per nonzero 30s window.
Groups: spot vs perp.
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


def group_of(market):
    return "perp" if "perp" in market else "spot"


def q(vals, pct):
    if not vals:
        return 0.0
    return float(np.percentile(np.asarray(vals, dtype=float), pct))


def rounded(x):
    if x >= 1_000_000:
        return round(x / 100_000) * 100_000
    if x >= 100_000:
        return round(x / 10_000) * 10_000
    if x >= 10_000:
        return round(x / 1_000) * 1_000
    return round(x / 100) * 100


def classify_counts(vals, t1, t2):
    return {
        "small": sum(1 for v in vals if v < t1),
        "medium": sum(1 for v in vals if t1 <= v < t2),
        "large": sum(1 for v in vals if v >= t2),
    }


def main():
    vals = {"spot": [], "perp": []}
    by_market = {}
    for path in sorted(SUMMARY_DIR.glob("*.jsonl")):
        market = path.stem
        g = group_of(market)
        by_market[market] = []
        for line in path.open():
            if not line.strip():
                continue
            r = json.loads(line)
            x = abs(float(r.get("burst_delta_notional") or 0.0))
            if x == 0:
                continue
            vals[g].append(x)
            by_market[market].append(x)

    schemes = {
        "p50_p90_balanced": lambda v: (q(v, 50), q(v, 90)),
        "p60_p95_stricter_large": lambda v: (q(v, 60), q(v, 95)),
        "p75_p95_high_signal": lambda v: (q(v, 75), q(v, 95)),
        "p50_p95_large_top5": lambda v: (q(v, 50), q(v, 95)),
    }

    rows = []
    for g in ["spot", "perp"]:
        v = vals[g]
        for name, fn in schemes.items():
            t1, t2 = fn(v)
            rt1, rt2 = rounded(t1), rounded(t2)
            c = classify_counts(v, rt1, rt2)
            total = len(v)
            rows.append({
                "group": g,
                "scheme": name,
                "t1_raw": t1,
                "t2_raw": t2,
                "t1_rounded": rt1,
                "t2_rounded": rt2,
                "n": total,
                "small": c["small"],
                "medium": c["medium"],
                "large": c["large"],
                "small_pct": c["small"] / total * 100 if total else 0,
                "medium_pct": c["medium"] / total * 100 if total else 0,
                "large_pct": c["large"] / total * 100 if total else 0,
                "p50": q(v, 50),
                "p75": q(v, 75),
                "p90": q(v, 90),
                "p95": q(v, 95),
                "p99": q(v, 99),
                "max": max(v) if v else 0,
            })

    out_csv = OUT_DIR / "burst_cvd_threshold_candidate_comparison.csv"
    with out_csv.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        w.writeheader()
        for r in rows:
            rr = r.copy()
            for k, v in rr.items():
                if isinstance(v, float):
                    rr[k] = round(v, 4)
            w.writerow(rr)

    # Plot candidate large rates and thresholds.
    fig, axes = plt.subplots(1, 2, figsize=(16, 6))
    for ax, g in zip(axes, ["spot", "perp"]):
        rs = [r for r in rows if r["group"] == g]
        labels = [r["scheme"].replace("_", "\n") for r in rs]
        large_pct = [r["large_pct"] for r in rs]
        t1 = [r["t1_rounded"] for r in rs]
        t2 = [r["t2_rounded"] for r in rs]
        x = np.arange(len(rs))
        ax.bar(x, large_pct, color="#fb8072", alpha=0.75)
        ax.set_xticks(x)
        ax.set_xticklabels(labels, fontsize=9)
        ax.set_ylabel("large bucket %")
        ax.set_title(f"{g.upper()} candidate large-rate / thresholds")
        for i, r in enumerate(rs):
            ax.text(i, large_pct[i], f"{large_pct[i]:.1f}%\n{t1[i]:,.0f}/{t2[i]:,.0f}", ha="center", va="bottom", fontsize=8)
        ax.grid(True, axis="y", alpha=0.25)
    fig.tight_layout()
    out_png = OUT_DIR / "burst_cvd_threshold_candidate_comparison.png"
    fig.savefig(out_png, dpi=160)
    plt.close(fig)

    print(f"wrote {out_csv}")
    print(f"wrote {out_png}")
    for r in rows:
        if r["scheme"] in ("p50_p90_balanced", "p60_p95_stricter_large", "p50_p95_large_top5"):
            print(r["group"], r["scheme"], "t", int(r["t1_rounded"]), int(r["t2_rounded"]), "pct", round(r["small_pct"],1), round(r["medium_pct"],1), round(r["large_pct"],1))


if __name__ == "__main__":
    main()
