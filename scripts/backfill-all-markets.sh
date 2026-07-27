#!/usr/bin/env bash
# scripts/backfill-all-markets.sh — 全マーケットのburst reducerをバックグラウンド並列実行
set -euo pipefail

cd /home/weed420/dev/github/like-kradness-2025/agg-btc-receiver

MARKETS=(
  binance_perp binance_perp_btcusdc binance_spot binance_spot_usdc
  bitfinex_spot bitmex_perp bitstamp_spot bybit_perp bybit_spot
  coinbase_spot crypto_com_spot hyperliquid_perp kraken_spot
  okx_perp okx_spot
)

OUTPUT_ROOT="data/derived/burst_features_v2"
TO_TS=$(TZ=UTC date +%s)
TO_ISO=$(TZ=UTC date -d "@$TO_TS" +%Y-%m-%dT%H:%M:%S+00:00)

echo "=== Backfill start: $(TZ=UTC date -Iseconds) ==="
echo "TO: $TO_ISO"

LOG_DIR="logs/backfill"
mkdir -p "$LOG_DIR"

PIDS=()

for MARKET in "${MARKETS[@]}"; do
  if [ ! -d "data/live_v3/trades/${MARKET}" ]; then
    echo "SKIP (no data): $MARKET"
    continue
  fi

  # Determine FROM: from manifest last checkpoint or epoch 2026-07-07
  MANIFEST_PATH="${OUTPUT_ROOT}/manifests/${MARKET}.json"
  FROM_TS=1783382400  # 2026-07-07T00:00:00 UTC

  if [ -f "$MANIFEST_PATH" ]; then
    LAST_CP=$(python3 -c "
import json
try:
    m = json.load(open('$MANIFEST_PATH'))
    v = m.get('last_checkpoint_block_start')
    if v: print(v // 1000)
except: pass
" 2>/dev/null || true)
    if [ -n "$LAST_CP" ] && [ "$LAST_CP" -gt "$FROM_TS" ]; then
      FROM_TS=$LAST_CP
    fi
  fi

  FROM_ISO=$(TZ=UTC date -d "@$FROM_TS" +%Y-%m-%dT%H:%M:%S+00:00)

  LOG_FILE="${LOG_DIR}/${MARKET}.log"
  echo "LAUNCH: $MARKET from=$FROM_ISO to=$TO_ISO (log: $LOG_FILE)"

  # Launch in background, kills on parent exit
  (
    exec > "$LOG_FILE" 2>&1
    node scripts/tfp.mjs \
      --markets "$MARKET" \
      --from "$FROM_ISO" \
      --to "$TO_ISO"
    echo "EXIT: $?"
  ) &

  PIDS+=($!)
done

echo "=== Launched ${#PIDS[@]} processes. PIDs: ${PIDS[*]} ==="
echo "=== Waiting... ==="

# Monitor: print summary every 15s
FAILED=0
MAX_WAIT=600  # 10 min max
INTERVAL=15
ELAPSED=0
while [ $ELAPSED -lt $MAX_WAIT ]; do
  sleep $INTERVAL
  ELAPSED=$((ELAPSED + INTERVAL))
  RUNNING=0
  for pid in "${PIDS[@]}"; do
    if kill -0 "$pid" 2>/dev/null; then
      RUNNING=$((RUNNING + 1))
    fi
  done
  echo "[${ELAPSED}s] running=${RUNNING}/${#PIDS[@]}"
  if [ "$RUNNING" -eq 0 ]; then
    break
  fi
done

# Collect exit codes
for pid in "${PIDS[@]}"; do
  wait "$pid" || FAILED=$((FAILED + 1))
done

echo "=== Backfill complete: $(TZ=UTC date -Iseconds) failed=${FAILED} ==="

# Print output shard counts
echo "=== Output summary ==="
for MARKET in "${MARKETS[@]}"; do
  COUNT=$(ls ${OUTPUT_ROOT}/features_1s/${MARKET}/2026-07-1[0-9]/ 2>/dev/null | wc -l || true)
  if [ "$COUNT" -gt 0 ]; then
    echo "  $MARKET: ${COUNT} shards (1s)"
  fi
done
echo "=== Done ==="
