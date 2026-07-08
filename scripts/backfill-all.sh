#!/usr/bin/env bash
# scripts/backfill-all.sh — 過去全データを集計してマーカーを付ける
set -e
cd "$(dirname "$0")/.."

MARKETS=(binance_perp binance_spot bybit_perp coinbase_spot)
DATES=(2026-07-04 2026-07-05 2026-07-06)

for m in "${MARKETS[@]}"; do
  for d in "${DATES[@]}"; do
    echo "=== $m / $d ==="
    node scripts/aggregate-live-v3.mjs \
      --data data/live_v3 \
      --out data/derived_v1 \
      --markets "$m" \
      --from "${d}T00:00:00Z" \
      --to "${d}T23:59:59Z" \
      --book-range-usd 10000 \
      --mark-processed 2>&1 | tail -3
    echo ""
  done
done

echo "=== DONE ==="
