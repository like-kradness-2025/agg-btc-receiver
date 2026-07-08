#!/usr/bin/env bash
# backfill-all-markets.sh — Convert all remaining markets (15 total, binance_perp already done)
# Each market: 2026-07-06 15:00〜23:00 in 2h slices
# Parallel: 5 markets at a time

set -euo pipefail
cd /home/weed420/dev/github/like-kradness-2025/agg-btc-receiver

OUT=/tmp/burst-agg-all
DATA=data/live_v3

TIMESLOTS=(
  "2026-07-06T15:00:00Z 2026-07-06T17:00:00Z"
  "2026-07-06T17:00:00Z 2026-07-06T19:00:00Z"
  "2026-07-06T19:00:00Z 2026-07-06T21:00:00Z"
  "2026-07-06T21:00:00Z 2026-07-06T23:00:00Z"
)

# Markets excluding binance_perp (already done)
MARKETS=(
  binance_perp_btcusdc
  binance_spot
  binance_spot_usdc
  bitfinex_spot
  bitmex_perp
  bitstamp_spot
  bybit_perp
  bybit_spot
  coinbase_spot
  crypto_com_spot
  hyperliquid_perp
  kraken_spot
  okx_perp
  okx_spot
)

echo "=== Backfill all markets ==="
echo "Output: $OUT"
echo "Markets: ${#MARKETS[@]}"
echo "Timeslots: ${#TIMESLOTS[@]}"
echo ""

mkdir -p "$OUT"

for slot_idx in "${!TIMESLOTS[@]}"; do
  slot="${TIMESLOTS[$slot_idx]}"
  FROM="${slot%% *}"
  TO="${slot##* }"
  echo ""
  echo "============================================"
  echo "SLOT $slot_idx: $FROM → $TO"
  echo "============================================"
  
  # Run markets in parallel (5 at a time)
  for mkt in "${MARKETS[@]}"; do
    echo "  Starting $mkt..."
    node scripts/burst-agg.mjs \
      --data "$DATA" \
      --out "$OUT" \
      --markets "$mkt" \
      --from "$FROM" \
      --to "$TO" \
      --book-range-usd 10000 &
  done
  
  # Wait for all of this batch
  wait
  echo "  SLOT $slot_idx done."
done

echo ""
echo "=== All done ==="

# Summary
echo ""
echo "=== Final summary ==="
python3 -c "
import json, glob
total_trades = 0
total_bursts = 0
total_summary = 0
total_features = 0
for f in glob.glob('$OUT/.staging/run-*/run-report.json'):
    r = json.load(open(f))
    for m, v in r.get('markets', {}).items():
        total_trades += v.get('trades_read', 0)
        total_bursts += v.get('bursts_detected', 0)
        total_summary += v.get('summary_windows', 0)
        total_features += v.get('feature_seconds', 0)
        print(f\"  {m}: trades={v.get('trades_read')} bursts={v.get('bursts_detected')} summary={v.get('summary_windows')} features={v.get('feature_seconds')}\")
print(f'TOTAL: trades={total_trades} bursts={total_bursts} summary={total_summary} features={total_features}')
"
