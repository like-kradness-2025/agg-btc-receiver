#!/usr/bin/env bash
# finish-slot3.sh — legacy burst_agg helper for one historical backfill slot.

echo "finish-slot3.sh is legacy and targets burst_agg outputs." >&2
echo "Set BURST_AGG_LEGACY_OK=1 if you intentionally need the old backfill path." >&2
if [[ "${BURST_AGG_LEGACY_OK:-0}" != "1" ]]; then
  exit 1
fi

set -euo pipefail
cd /home/weed420/dev/github/like-kradness-2025/agg-btc-receiver
DATA=data/live_v3
OUT=/tmp/burst-agg-all
for m in binance_perp_btcusdc binance_spot binance_spot_usdc bitstamp_spot bybit_perp bybit_spot coinbase_spot crypto_com_spot hyperliquid_perp kraken_spot okx_perp okx_spot; do
  node scripts/burst-agg.mjs --data "$DATA" --out "$OUT" --markets "$m" --from 2026-07-06T21:00:00Z --to 2026-07-06T23:00:00Z --book-range-usd 10000 &
done
wait
echo "=== SLOT 3 done ==="
