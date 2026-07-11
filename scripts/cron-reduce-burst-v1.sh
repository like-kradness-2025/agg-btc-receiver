#!/usr/bin/env bash
# scripts/cron-reduce-burst-v1.sh — Cron entry point for burst reducer
# Runs every cycle: finds latest 2 consecutive 30s blocks, reduces them
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR/.."

# Source lock helper (P0-1: single-writer safety)
source scripts/lock-helper.sh

MARKETS=(
  binance_perp binance_perp_btcusdc binance_spot binance_spot_usdc
  bitfinex_spot bitmex_perp bitstamp_spot bybit_perp bybit_spot
  coinbase_spot crypto_com_spot hyperliquid_perp kraken_spot
  okx_perp okx_spot
)

ALL_OUTPUT_ROOT="data/derived/burst_features_v1"
TODAY=$(TZ=UTC date +%Y-%m-%d)
NOW_TS=$(TZ=UTC date +%s)

# Round down to nearest 30s boundary
CURRENT_BLOCK_START=$(( (NOW_TS / 30) * 30 ))
FROM_TS=$(( CURRENT_BLOCK_START - 60 ))   # 2 blocks back = 60s
TO_TS=$(( CURRENT_BLOCK_START + 30 ))     # cover current block

# Convert to ISO 8601 (date -u on this system)
FROM_ISO=$(TZ=UTC date -d "@$FROM_TS" +%Y-%m-%dT%H:%M:%S+00:00)
TO_ISO=$(TZ=UTC date -d "@$TO_TS" +%Y-%m-%dT%H:%M:%S+00:00)

echo "CRON: [$(TZ=UTC date -Iseconds)] Range: ${FROM_ISO} → ${TO_ISO}"

TOTAL_PROCESSED=0
TOTAL_ERRORS=0
FAILED_MARKETS=""

for MARKET in "${MARKETS[@]}"; do
  # Skip if market has no data dir
  if [ ! -d "data/live_v3/trades/${MARKET}" ]; then
    continue
  fi

  # P0-1: Acquire same-host flock for this market
  acquire_market_lock "$MARKET" "$ALL_OUTPUT_ROOT" || continue

  # Check if pipeline output already exists for this block range (idempotency check)
  # If manifest has the latest block committed, skip (fast path)
  MANIFEST_PATH="${ALL_OUTPUT_ROOT}/manifests/${MARKET}.json"
  LATEST_COMMITTED=""
  if [ -f "$MANIFEST_PATH" ]; then
    LATEST_COMMITTED=$(python3 -c "
import json, sys
try:
    m = json.load(open('$MANIFEST_PATH'))
    print(m.get('last_checkpoint_block_start', '') or '')
except: pass
" 2>/dev/null || true)
  fi

  # Run pipeline
  OUTPUT=$(node scripts/tfp.mjs \
    --markets "${MARKET}" \
    --from "${FROM_ISO}" \
    --to "${TO_ISO}" \
    --output-root "${ALL_OUTPUT_ROOT}" 2>&1) || true

  EXIT_CODE=$?
  if echo "$OUTPUT" | grep -q '"processed":0'; then
    # No new blocks processed — normal
    :
  elif echo "$OUTPUT" | grep -q '"processed":'; then
    PROCESSED=$(echo "$OUTPUT" | python3 -c "
import sys, json
for line in sys.stdin:
    try:
        d = json.loads(line.strip())
        if 'processed' in d and d.get('level') == 'INFO' and 'msg' in d and 'complete' in str(d.get('msg','')):
            print(d['processed'])
            break
    except: pass
" 2>/dev/null || echo "?")
    if [ -n "$PROCESSED" ] && [ "$PROCESSED" != "?" ]; then
      TOTAL_PROCESSED=$((TOTAL_PROCESSED + PROCESSED))
    fi
  fi

  # Check for FATAL
  if echo "$OUTPUT" | grep -q '"level":"FATAL"'; then
    TOTAL_ERRORS=$((TOTAL_ERRORS + 1))
    FAILED_MARKETS="${FAILED_MARKETS} ${MARKET}"
  fi
done

echo "CRON: processed=${TOTAL_PROCESSED} errors=${TOTAL_ERRORS}${FAILED_MARKETS:+ failed:${FAILED_MARKETS}}"
