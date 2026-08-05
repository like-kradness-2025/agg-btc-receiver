#!/usr/bin/env bash
# ⚠️  LEGACY — Incremental live TFP converter using old JSONL/v4 raw path.
#    The live production pipeline does not use TFP in Receiver.
#    See docs/current/canonical-pipeline.md for the canonical architecture.
# Incremental live TFP conversion. Each market resumes from its committed
# manifest checkpoint and advances to the current finalized input horizon.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$ROOT"

DATA_ROOT="data/live_v4"
OUTPUT_ROOT="data/derived/burst_features_v2"
LOG_DIR="logs/tfp-live"
mkdir -p "$LOG_DIR"

MARKETS=(
  binance_perp binance_perp_btcusdc binance_spot binance_spot_usdc
  bitfinex_spot bitmex_perp bitstamp_spot bybit_perp bybit_spot
  coinbase_spot crypto_com_spot hyperliquid_perp kraken_spot
  okx_perp okx_spot
)

# Finalization is an exclusive, 30s-aligned boundary. Keep one block of
# margin so the receiver has finished rotating the current window.
NOW_SEC="$(date -u +%s)"
FINALIZED_SEC="$((NOW_SEC - NOW_SEC % 30 - 30))"
FINALIZED_THROUGH="$(TZ=UTC date -d "@$FINALIZED_SEC" +%Y-%m-%dT%H:%M:%S+00:00)"
TO_ISO="$FINALIZED_THROUGH"
RUN_LOG="$LOG_DIR/run-$(TZ=UTC date +%Y%m%dT%H%M%SZ).log"
exec > >(tee -a "$RUN_LOG") 2>&1

echo "=== TFP live start: $(TZ=UTC date -Iseconds) to=$TO_ISO finalized-through=$FINALIZED_THROUGH ==="

failed_markets=()
for market in "${MARKETS[@]}"; do
  [[ -d "$DATA_ROOT/trades/$market" ]] || continue

  # Start from the earliest raw segment when a market has no usable
  # checkpoint. The old fixed 2026-07-18 fallback caused long scans of
  # nonexistent 30s blocks before the first v4 file.
  first_raw="$(find "$DATA_ROOT/trades/$market" -mindepth 2 -maxdepth 2 -type f \( -name '*.jsonl' -o -name '*.jsonl.active' \) -printf '%p\n' | LC_ALL=C sort | head -n 1)"
  if [[ -z "$first_raw" ]]; then
    echo "TFP_SKIP market=$market reason=no-raw-segment"
    continue
  fi
  raw_date="$(basename "$(dirname "$first_raw")")"
  raw_segment="${first_raw##*/}"
  raw_segment="${raw_segment%.active}"
  raw_segment="${raw_segment%.jsonl}"
  raw_hm="${raw_segment//-/:}"
  earliest_sec="$(date -u -d "$raw_date $raw_hm:00" +%s)"
  from_iso="$(TZ=UTC date -d "@$earliest_sec" +%Y-%m-%dT%H:%M:%S+00:00)"
  manifest="$OUTPUT_ROOT/manifests/$market.json"
  if [[ -f "$manifest" ]]; then
    last_cp="$(python3 - "$manifest" <<'PY'
import json, sys
try:
    value = json.load(open(sys.argv[1])).get('last_checkpoint_block_start')
    if value:
        print(int(value) // 1000)
except Exception:
    pass
PY
    )"
    if [[ "$last_cp" =~ ^[0-9]+$ && "$last_cp" -ge "$earliest_sec" ]]; then
      from_iso="$(TZ=UTC date -d "@$last_cp" +%Y-%m-%dT%H:%M:%S+00:00)"
    fi
  fi

  echo "--- $market from=$from_iso to=$TO_ISO ---"
  if ! node scripts/tfp.mjs \
    --markets "$market" \
    --data "$DATA_ROOT" \
    --raw-layout v4 \
    --from "$from_iso" \
    --to "$TO_ISO" \
    --finalized-through "$FINALIZED_THROUGH" \
    --output-root "$OUTPUT_ROOT"; then
    echo "TFP_FAILED market=$market"
    failed_markets+=("$market")
  fi
done

if (( ${#failed_markets[@]} > 0 )); then
  echo "=== TFP live failed: markets=${failed_markets[*]} ==="
  exit 1
fi

echo "=== TFP live complete: $(TZ=UTC date -Iseconds) ==="
