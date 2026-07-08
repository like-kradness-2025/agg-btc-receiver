#!/usr/bin/env bash
# Deterministic burst aggregation cron runner.
# Processes the oldest remaining finalized raw window up to a safe cutoff.
# Avoids LLM-derived from/to gaps.

set -euo pipefail

cd /home/weed420/dev/github/like-kradness-2025/agg-btc-receiver

DATA_DIR="data/live_v3"
OUT_DIR="data/burst_agg"
MARKETS="binance_perp,binance_spot,bybit_perp,coinbase_spot,binance_perp_btcusdc,binance_spot_usdc,bitfinex_spot,bitmex_perp,bitstamp_spot,bybit_spot,crypto_com_spot,hyperliquid_perp,kraken_spot,okx_perp,okx_spot"

# Keep a safety lag so receiver has finalized 30s files before we process/delete.
SAFE_LAG_SEC=90
# Max one run slice. Small enough for 1-minute cadence, large enough to drain backlog.
MAX_SLICE_SEC=300

python3 - <<'PY' > /tmp/burst_agg_window.env
import glob, os, re, time
from datetime import datetime, timezone

files = []
for kind in ("trades",):
    for p in glob.glob(f"data/live_v3/{kind}/**/*.jsonl", recursive=True):
        if p.endswith('.open'):
            continue
        m = re.search(r'/(\d{4}-\d{2}-\d{2})/(\d{2})-(\d{2})-(\d{2})\.jsonl$', p)
        if not m:
            continue
        dt = datetime.fromisoformat(f"{m.group(1)}T{m.group(2)}:{m.group(3)}:{m.group(4)}+00:00")
        files.append(int(dt.timestamp() * 1000))

if not files:
    print('HAS_WORK=0')
    raise SystemExit

oldest = min(files)
now_ms = int(time.time() * 1000)
safe_to = now_ms - 90_000
# Align safe_to down to 30s boundary.
safe_to = safe_to - (safe_to % 30000)

# Align oldest down to 30s boundary.
from_ms = oldest - (oldest % 30000)
# Process at most 5 minutes, but never beyond safe_to.
to_ms = min(from_ms + 300_000, safe_to)
# Need at least one full 30s window.
if to_ms <= from_ms:
    print('HAS_WORK=0')
    raise SystemExit

print('HAS_WORK=1')
print('FROM=' + datetime.fromtimestamp(from_ms/1000, tz=timezone.utc).isoformat().replace('+00:00','Z'))
print('TO=' + datetime.fromtimestamp(to_ms/1000, tz=timezone.utc).isoformat().replace('+00:00','Z'))
PY

source /tmp/burst_agg_window.env

if [[ "${HAS_WORK}" != "1" ]]; then
  # Stay quiet when no work; no_agent cron sends nothing on empty stdout.
  exit 0
fi

OUT=$(node scripts/burst-agg.mjs \
  --data "$DATA_DIR" \
  --out "$OUT_DIR" \
  --markets "$MARKETS" \
  --from "$FROM" \
  --to "$TO" \
  --book-range-usd 10000 \
  --delete-processed 2>&1)

# Only print a compact message when meaningful work was done or there was output worth seeing.
echo "burst-agg processed $FROM -> $TO"
echo "$OUT" | grep -E "trades, building bursts|Deleted|Run report|Error|error|FAILED|failed" || true
