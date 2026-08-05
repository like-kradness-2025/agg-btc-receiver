#!/usr/bin/env bash
# scripts/backfill-all-markets-serial.sh — 全マーケットburst reducerを直列実行
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR/.."

MARKETS=(
  binance_perp binance_perp_btcusdc binance_spot binance_spot_usdc
  bitfinex_spot bitmex_perp bitstamp_spot bybit_perp bybit_spot
  coinbase_spot crypto_com_spot hyperliquid_perp kraken_spot
  okx_perp okx_spot
)

OUTPUT_ROOT="data/derived/burst_features_v2"
TO_TS=$(TZ=UTC date +%s)
TO_ISO=$(TZ=UTC date -d "@$TO_TS" +%Y-%m-%dT%H:%M:%S+00:00)
FROM_ISO="2026-07-07T00:00:00+00:00"

LOG_DIR="logs/backfill-serial"
mkdir -p "$LOG_DIR"

echo "=== Serial Backfill Start: $(TZ=UTC date -Iseconds) ==="
echo "FROM: $FROM_ISO"
echo "TO:   $TO_ISO"

TOTAL_P=0
TOTAL_E=0

for MARKET in "${MARKETS[@]}"; do
  if [ ! -d "data/live_v3/trades/${MARKET}" ]; then
    echo "SKIP (no data dir): $MARKET"
    continue
  fi

  # Run pipeline (tfp.mjs handles per-market flock internally — Gate A)
  LOG_FILE="${LOG_DIR}/${MARKET}.log"
  echo "--- $(TZ=UTC date -Iseconds) Processing: $MARKET ---"

  START_TS=$(TZ=UTC date +%s)
  set +e
  node scripts/tfp.mjs \
    --markets "$MARKET" \
    --from "$FROM_ISO" \
    --to "$TO_ISO" \
    --output-root "$OUTPUT_ROOT" > "$LOG_FILE" 2>&1
  EXIT_CODE=$?
  set -e
  END_TS=$(TZ=UTC date +%s)
  DURATION=$((END_TS - START_TS))

  SHARD_COUNT=$(ls -d ${OUTPUT_ROOT}/features_1s/${MARKET}/2026-07-*/ 2>/dev/null | wc -l || true)
  MANIFEST_PATH="${OUTPUT_ROOT}/manifests/${MARKET}.json"
  BLOCKS="?"
  if [ -f "$MANIFEST_PATH" ]; then
    BLOCKS=$(python3 -c "import json; m=json.load(open('$MANIFEST_PATH')); print(len(m.get('processed_blocks',{})))" 2>/dev/null || echo "?")
  fi

  if [ "$EXIT_CODE" -eq 0 ]; then
    echo "     OK: ${DURATION}s, ${BLOCKS} blocks, ${SHARD_COUNT} date-dirs"
    TOTAL_P=$((TOTAL_P + 1))
  else
    echo "     FAIL (exit=$EXIT_CODE): ${DURATION}s, ${BLOCKS} blocks, ${SHARD_COUNT} date-dirs"
    TOTAL_E=$((TOTAL_E + 1))
  fi
done

echo "=== Complete: $(TZ=UTC date -Iseconds) ok=${TOTAL_P} failed=${TOTAL_E} ==="
