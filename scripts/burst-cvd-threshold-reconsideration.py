#!/usr/bin/env python3
"""BurstCVD サイズ閾値の統計的再検討 — 5方式比較.

Metric: abs(burst_delta_notional) per 30s window.
Split: spot vs perp (market name に 'perp' を含むか).

Schemes evaluated:
  A. p50/p90 (現行案)
  B. p60/p95
  C. p75/p95
  D. log-scale k-means (k=3, on log10)
  E. robust rounded (人為的にキリの良い値)
"""

import csv
import json
import math
from pathlib import Path

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


def rounded_nice(x, base=5_000):
    """Round to nearest 'nice' increment. Uses tiered bases."""
    if x >= 1_000_000:
        return round(x / 100_000) * 100_000
    if x >= 100_000:
        return round(x / 10_000) * 10_000
    if x >= 10_000:
        return round(x / 5_000) * 5_000
    if x >= 1_000:
        return round(x / 500) * 500
    return round(x / 100) * 100


def kmeans_1d(vals, k=3, n_iter=20):
    """Simple 1D k-means (equivalent to Jenks natural breaks for 1D)."""
    arr = np.sort(np.asarray(vals, dtype=float))
    n = len(arr)
    if n <= k:
        return sorted(arr.tolist()) + [arr[-1]] * (k - n)

    # Initialize centroids at equal percentiles
    idx = [int(n * (i + 1) / (k + 1)) for i in range(k)]
    centroids = arr[idx].copy()

    for _ in range(n_iter):
        # Assign to nearest centroid
        dist = np.abs(arr[:, None] - centroids[None, :])
        labels = np.argmin(dist, axis=1)
        new_centroids = np.array([arr[labels == j].mean() if np.sum(labels == j) > 0 else centroids[j] for j in range(k)])
        if np.allclose(centroids, new_centroids, rtol=1e-4):
            break
        centroids = new_centroids

    # Thresholds are midpoints between sorted centroids
    sorted_c = np.sort(centroids)
    thresholds = [(sorted_c[i] + sorted_c[i+1]) / 2 for i in range(k-1)]
    return thresholds


def classify_counts(vals, t1, t2):
    return {
        "small": sum(1 for v in vals if v < t1),
        "medium": sum(1 for v in vals if t1 <= v < t2),
        "large": sum(1 for v in vals if v >= t2),
    }


def classify_entropy(counts, total):
    """Compute classification entropy (higher = more balanced)."""
    if total == 0:
        return 0.0
    probs = [c / total for c in counts if c > 0]
    if not probs:
        return 0.0
    return -sum(p * math.log2(p) for p in probs)


def gini(vals):
    """Gini coefficient of distribution (0=uniform, 1=concentrated)."""
    arr = np.sort(np.asarray(vals, dtype=float))
    n = len(arr)
    if n == 0:
        return 0.0
    index = np.arange(1, n + 1)
    return (2 * np.sum(index * arr) - (n + 1) * np.sum(arr)) / (n * np.sum(arr))


def main():
    # ── Load data ──
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

    # ── Distribution statistics ──
    print("=" * 70)
    print("BurstCVD サイズ分布統計")
    print("=" * 70)
    for g in ["spot", "perp"]:
        v = vals[g]
        print(f"\n{g.upper()}  n={len(v):,}")
        print(f"  p50={q(v,50):,.0f}  p60={q(v,60):,.0f}  p75={q(v,75):,.0f}")
        print(f"  p90={q(v,90):,.0f}  p95={q(v,95):,.0f}  p99={q(v,99):,.0f}")
        print(f"  max={max(v):,.0f}  gini={gini(v):.3f}")
        print(f"  log10 mean={np.mean(np.log10(np.asarray(v)+1)):.2f}  log10 std={np.std(np.log10(np.asarray(v)+1)):.2f}")

    # ── Scheme A: p50/p90 (current) ──
    # ── Scheme B: p60/p95 ──
    # ── Scheme C: p75/p95 ──
    # ── Scheme D: log-scale k-means ──
    # ── Scheme E: robust rounded ──

    print("\n" + "=" * 70)
    print("閾値スキーム比較 (3-tier: small/medium/large)")
    print("=" * 70)

    csv_path = OUT_DIR / "burst_cvd_threshold_reconsideration.csv"
    rows = []

    for g in ["spot", "perp"]:
        v = vals[g]
        total = len(v)
        if total == 0:
            continue

        # ── All scheme computations ──
        # A: p50/p90
        t1_a, t2_a = q(v, 50), q(v, 90)
        # B: p60/p95
        t1_b, t2_b = q(v, 60), q(v, 95)
        # C: p75/p95
        t1_c, t2_c = q(v, 75), q(v, 95)
        # D: log-scale k-means (on log10)
        log_vals = np.log10(np.asarray(v) + 1)
        log_thresholds = kmeans_1d(log_vals, k=3)
        t1_d = 10**log_thresholds[0] - 1 if len(log_thresholds) > 0 else q(v, 50)
        t2_d = 10**log_thresholds[1] - 1 if len(log_thresholds) > 1 else q(v, 90)
        # E: robust rounded
        t1_e = rounded_nice(q(v, 60))
        t2_e = rounded_nice(q(v, 95))
        # F: p50/p95 (mixed)
        t1_f, t2_f = q(v, 50), q(v, 95)
        # G: p75/p90
        t1_g, t2_g = q(v, 75), q(v, 90)

        schemes = [
            ("A_p50/p90", t1_a, t2_a),
            ("B_p60/p95", t1_b, t2_b),
            ("C_p75/p95", t1_c, t2_c),
            ("D_log-kmeans", t1_d, t2_d),
            ("E_robust_rounded", t1_e, t2_e),
            ("F_p50/p95", t1_f, t2_f),
            ("G_p75/p90", t1_g, t2_g),
        ]

        for name, t1_raw, t2_raw in schemes:
            t1_r = rounded_nice(t1_raw)
            t2_r = rounded_nice(t2_raw)
            c = classify_counts(v, t1_r, t2_r)
            ent = classify_entropy([c["small"], c["medium"], c["large"]], total)
            rows.append({
                "group": g,
                "scheme": name,
                "t1_raw": round(t1_raw, 2),
                "t2_raw": round(t2_raw, 2),
                "t1_rounded": t1_r,
                "t2_rounded": t2_r,
                "n": total,
                "small": c["small"],
                "medium": c["medium"],
                "large": c["large"],
                "small_pct": round(c["small"] / total * 100, 2),
                "medium_pct": round(c["medium"] / total * 100, 2),
                "large_pct": round(c["large"] / total * 100, 2),
                "entropy": round(ent, 4),
                "p50": round(q(v, 50), 2),
                "p75": round(q(v, 75), 2),
                "p90": round(q(v, 90), 2),
                "p95": round(q(v, 95), 2),
                "p99": round(q(v, 99), 2),
            })

        # Also compute per-market thresholds for reference
        print(f"\n--- {g.upper()} per-market p50/p90/p95 ---")
        for market in sorted(by_market):
            mv = by_market[market]
            if not mv:
                continue
            print(f"  {market:28s} n={len(mv):>4d}  p50={q(mv,50):>12,.0f}  p90={q(mv,90):>12,.0f}  p95={q(mv,95):>12,.0f}")

    # Write CSV
    with csv_path.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        w.writeheader()
        for r in rows:
            w.writerow(r)

    # ── Print report ──
    print("\n" + "=" * 70)
    print("スキーム一覧（キリの良い値に丸め適用後）")
    print("=" * 70)
    fmt = "  {scheme:20s} t1={t1_rounded:>12,}  t2={t2_rounded:>12,}  small={small_pct:5.1f}%  medium={medium_pct:5.1f}%  large={large_pct:5.1f}%  entropy={entropy:.4f}"
    for g in ["spot", "perp"]:
        print(f"\n  [{g.upper()}]")
        for r in rows:
            if r["group"] == g:
                print(fmt.format(**r))

    print(f"\nwrote {csv_path}")

    # ── Recommendations ──
    print("\n" + "=" * 70)
    print("推奨と根拠")
    print("=" * 70)
    print("""
【SPOT】
- 分布は激しいロングテール。n=約3,200。
- p50≈14k, p90≈120k, p95≈200k, p99≈560k
- p50/p90（現行案）: Small<14k, Medium14k-120k, Large≥120k
  → Medium=40.5%, Large=10.0%。Mediumがやや広いが実用上問題ない。
- p60/p95: Small<23k, Medium23k-200k, Large≥200k
  → Large=5.2%に減少。Largeが小さすぎて希少シグナル化。
- p75/p95: Large=5.2%だがMediumが19.5%に激減。Smallが75%と支配的。
- log-kmeans: 対数空間での自然な切れ目。分布の歪みを吸収する。
- robust rounded: p60/p95を丸めたもの。

【PERP】
- 分布はspotより1桁大きい。n=約2,100。
- p50≈161k, p90≈1.21M, p95≈2.02M, p99≈4.82M
- p50/p90（現行案）: Small<160k, Medium160k-1.2M, Large≥1.2M
  → バランス良好。Medium=39.8%, Large=10.2%
- p60/p95: Large=5.1%に。Small=60%で支配的。
- p75/p95: Large=5.1%, Medium=19.7%。Small=75%と高集中。

【総合推奨】
1. 現行 p50/p90 を維持推奨。Mediumが実用的なボリューム（40%）を維持し、
   Largeが10%前後で十分なサンプル数を持つ。
2. p60/p95 や p75/p95 は Large が5%以下になり、検知数が不足。
3. log-kmeans は対数正規性を仮定するが、実際の分布はマルチモーダルな
   可能性があり、単純な3分割よりデータ構造を反映する可能性がある。
4. robust rounded は閾値の覚えやすさ・説明可能性を重視するなら検討価値あり。
   ただし丸め方次第で分類が変わるため、一貫性に注意。

推奨順位：
  第1位: p50/p90（現行維持）— バランス・実用性・解釈容易性
  第2位: log-kmeans — データ駆動だが、閾値がマーケット構造の変化で
         大きく動く可能性があり、安定性に懸念
  第3位: robust rounded — 説明可能性重視だが恣意性あり
  第4位: p60/p95 — Largeが少なすぎる
  第5位: p75/p95 — 同上
""")

    # ── Detailed per-market analysis ──
    print("\n" + "=" * 70)
    print("市場別 p50/p90/p95（参考）")
    print("=" * 70)
    for market in sorted(by_market):
        mv = by_market[market]
        if not mv:
            continue
        print(f"  {market:30s} n={len(mv):>5d}  p50={q(mv,50):>12,.0f}  p90={q(mv,90):>12,.0f}  p95={q(mv,95):>12,.0f}  p99={q(mv,99):>12,.0f}")


if __name__ == "__main__":
    main()
