#!/usr/bin/env python3
"""Plot CVD from burst_agg summary JSONL.

Outputs:
- data/burst_agg/charts/cvd_all_markets.png
- data/burst_agg/charts/cvd_binance_perp_detail.png
- data/burst_agg/charts/cvd_latest.csv
"""

import csv
import json
from pathlib import Path
from datetime import datetime, timezone

import matplotlib.pyplot as plt
import matplotlib.dates as mdates

ROOT = Path(__file__).resolve().parents[1]
SUMMARY_DIR = ROOT / "data" / "burst_agg" / "summary"
OUT_DIR = ROOT / "data" / "burst_agg" / "charts"
OUT_DIR.mkdir(parents=True, exist_ok=True)


def read_market(path: Path):
    rows = []
    cvd = 0.0
    burst_cvd = 0.0
    non_burst_cvd = 0.0
    with path.open() as f:
        for line in f:
            if not line.strip():
                continue
            r = json.loads(line)
            delta = float(r.get("delta_notional") or 0.0)
            burst_delta = float(r.get("burst_delta_notional") or 0.0)
            non_burst_delta = delta - burst_delta
            cvd += delta
            burst_cvd += burst_delta
            non_burst_cvd += non_burst_delta
            rows.append({
                "ts": int(r["ts"]),
                "dt": datetime.fromtimestamp(int(r["ts"]) / 1000, tz=timezone.utc),
                "market": r.get("market") or path.stem,
                "delta_notional": delta,
                "burst_delta_notional": burst_delta,
                "non_burst_delta_notional": non_burst_delta,
                "cvd": cvd,
                "burst_cvd": burst_cvd,
                "non_burst_cvd": non_burst_cvd,
                "trade_count": int(r.get("trade_count") or 0),
                "burst_count": int(r.get("burst_count") or 0),
            })
    return rows


def money_axis(ax):
    ax.grid(True, alpha=0.25)
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))
    ax.ticklabel_format(axis='y', style='plain')


def main():
    files = sorted(SUMMARY_DIR.glob("*.jsonl"))
    if not files:
        raise SystemExit(f"No summary JSONL files under {SUMMARY_DIR}")

    data = {p.stem: read_market(p) for p in files}
    data = {m: rows for m, rows in data.items() if rows}
    if not data:
        raise SystemExit("No rows in summary JSONL files")

    # CSV export, long format.
    csv_path = OUT_DIR / "cvd_latest.csv"
    with csv_path.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=[
            "ts", "iso", "market", "delta_notional", "burst_delta_notional",
            "non_burst_delta_notional", "cvd", "burst_cvd", "non_burst_cvd",
            "trade_count", "burst_count",
        ])
        w.writeheader()
        for market, rows in data.items():
            for r in rows:
                w.writerow({
                    "ts": r["ts"],
                    "iso": r["dt"].isoformat(),
                    "market": market,
                    "delta_notional": round(r["delta_notional"], 8),
                    "burst_delta_notional": round(r["burst_delta_notional"], 8),
                    "non_burst_delta_notional": round(r["non_burst_delta_notional"], 8),
                    "cvd": round(r["cvd"], 8),
                    "burst_cvd": round(r["burst_cvd"], 8),
                    "non_burst_cvd": round(r["non_burst_cvd"], 8),
                    "trade_count": r["trade_count"],
                    "burst_count": r["burst_count"],
                })

    # All markets: total CVD small multiples.
    markets = sorted(data)
    n = len(markets)
    cols = 3
    rows_n = (n + cols - 1) // cols
    fig, axes = plt.subplots(rows_n, cols, figsize=(18, 3.4 * rows_n), sharex=True)
    axes = axes.ravel()
    for ax, market in zip(axes, markets):
        rows = data[market]
        xs = [r["dt"] for r in rows]
        ax.plot(xs, [r["cvd"] for r in rows], lw=1.3, label="total CVD", color="#1f77b4")
        ax.plot(xs, [r["burst_cvd"] for r in rows], lw=1.0, label="burst CVD", color="#ff7f0e", alpha=0.85)
        ax.axhline(0, color="black", lw=0.7, alpha=0.5)
        ax.set_title(market)
        money_axis(ax)
    for ax in axes[n:]:
        ax.axis("off")
    handles, labels = axes[0].get_legend_handles_labels()
    fig.legend(handles, labels, loc="upper center", ncol=2)
    fig.suptitle("Burst Aggregated CVD by Market (30s summary)", y=0.995, fontsize=16)
    fig.tight_layout(rect=(0, 0, 1, 0.97))
    all_path = OUT_DIR / "cvd_all_markets.png"
    fig.savefig(all_path, dpi=160)
    plt.close(fig)

    # Detail: prefer binance_perp, fallback first market.
    detail_market = "binance_perp" if "binance_perp" in data else markets[0]
    rows = data[detail_market]
    xs = [r["dt"] for r in rows]
    fig, axes = plt.subplots(3, 1, figsize=(16, 10), sharex=True)
    axes[0].plot(xs, [r["cvd"] for r in rows], label="total CVD", color="#1f77b4")
    axes[0].plot(xs, [r["burst_cvd"] for r in rows], label="burst CVD", color="#ff7f0e")
    axes[0].plot(xs, [r["non_burst_cvd"] for r in rows], label="non-burst CVD", color="#2ca02c")
    axes[0].axhline(0, color="black", lw=0.7, alpha=0.5)
    axes[0].set_title(f"{detail_market}: total / burst / non-burst CVD")
    axes[0].legend(loc="best")
    money_axis(axes[0])

    axes[1].bar(xs, [r["delta_notional"] for r in rows], width=0.00025, color=["#2ca02c" if r["delta_notional"] >= 0 else "#d62728" for r in rows])
    axes[1].set_title("30s delta notional")
    money_axis(axes[1])

    axes[2].plot(xs, [r["trade_count"] for r in rows], label="trade_count", color="#9467bd")
    axes[2].plot(xs, [r["burst_count"] for r in rows], label="burst_count", color="#8c564b")
    axes[2].set_title("activity")
    axes[2].legend(loc="best")
    money_axis(axes[2])

    fig.tight_layout()
    detail_path = OUT_DIR / f"cvd_{detail_market}_detail.png"
    fig.savefig(detail_path, dpi=160)
    plt.close(fig)

    print(f"markets={len(data)} rows={sum(len(v) for v in data.values())}")
    print(f"wrote {all_path}")
    print(f"wrote {detail_path}")
    print(f"wrote {csv_path}")


if __name__ == "__main__":
    main()
