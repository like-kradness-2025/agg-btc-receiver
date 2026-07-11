#!/usr/bin/env bash
# run-burst-agg-backfill.sh — drain backlog in larger chunks, then exit.
# Intended to run alongside the regular 1-min cron which handles steady-state.
set -euo pipefail

cd /home/weed420/dev/github/like-kradness-2025/agg-btc-receiver
DATA_DIR="data/live_v3"
OUT_DIR="data/burst_agg"
MARKETS="binance_perp,binance_spot,bybit_perp,coinbase_spot,binance_perp_btcusdc,binance_spot_usdc,bitfinex_spot,bitmex_perp,bitstamp_spot,bybit_spot,crypto_com_spot,hyperliquid_perp,kraken_spot,okx_perp,okx_spot"
SAFE_LAG_SEC=90
MAX_SLICE_SEC=600   # 10 min chunks — larger than cron for faster drain

while true; do
  # Check if there's work
  python3 - <<'PY' > /tmp/burst_agg_window.env
import glob, os, re, time
from datetime import datetime, timezone

files = []
for p in glob.glob(f"data/live_v3/trades/**/*.jsonl", recursive=True):
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
safe_to = safe_to - (safe_to % 30000)
from_ms = oldest - (oldest % 30000)
to_ms = min(from_ms + 600_000, safe_to)
if to_ms <= from_ms:
    print('HAS_WORK=0')
    raise SystemExit
print('HAS_WORK=1')
print('FROM=' + datetime.fromtimestamp(from_ms/1000, tz=timezone.utc).isoformat().replace('+00:00','Z'))
print('TO=' + datetime.fromtimestamp(to_ms/1000, tz=timezone.utc).isoformat().replace('+00:00','Z'))
PY
  source /tmp/burst_agg_window.env
  [[ "${HAS_WORK}" != "1" ]] && { echo "backlog drained"; break; }
  echo "=== processing $FROM -> $TO ==="
  BURST_AGG_LEGACY_OK=1 bash scripts/run-burst-agg-cron.sh 2>&1 && echo "=== done ==="
  sleep 2
done
