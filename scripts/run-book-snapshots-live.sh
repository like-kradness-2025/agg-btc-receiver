#!/usr/bin/env bash
# ⚠️  LEGACY — Live book snapshot timer wrapper for old JSONL-based path.
#    The live production pipeline uses:
#      Receiver SQLite → agg-btc-downstream-live → agg-btc-orderheatmap-publisher
#    See docs/current/canonical-pipeline.md for the canonical architecture.
set -euo pipefail

REPO=/home/weed420/Tool/agg-btc-receiver
cd "$REPO"

DATA_ROOT=data/live_v4
DERIVED_ROOT=data/derived/burst_features_v2
SNAPSHOT_ROOT="$DERIVED_ROOT/book_snapshots_v2"
ORDERHEATMAP_ROOT="$DERIVED_ROOT/orderheatmap_1s"

mapfile -t MARKETS < <(find "$DATA_ROOT/book_updates" -mindepth 1 -maxdepth 1 -type d -printf '%f\n' 2>/dev/null | sort)
if ((${#MARKETS[@]} == 0)); then
  echo "No v4 book markets found under $DATA_ROOT/book_updates"
  exit 0
fi
MARKET_CSV="$(IFS=,; echo "${MARKETS[*]}")"

# Re-scan a bounded recent horizon so late/finalized raw blocks are picked up.
# The materializer is idempotent and never overwrites an existing block unless
# --force is explicitly supplied.
# GNU date on this host expands %3N as a 3-digit nanosecond prefix rather
# than milliseconds. Derive epoch milliseconds without relying on %N.
NOW_MS=$(( $(date +%s) * 1000 ))
FROM_MS=$((NOW_MS - 7200000))
/usr/bin/node scripts/materialize-book-snapshots.mjs \
  --raw-layout v4 \
  --data "$DATA_ROOT" \
  --output-root "$SNAPSHOT_ROOT" \
  --markets "$MARKET_CSV" \
  --from "$FROM_MS" \
  --to "$NOW_MS" \
  --incremental
/usr/bin/node scripts/materialize-orderheatmap.mjs \
  --snapshot-root "$SNAPSHOT_ROOT" \
  --output-root "$ORDERHEATMAP_ROOT" \
  --markets "$MARKET_CSV" \
  --from "$FROM_MS" \
  --to "$NOW_MS" \
  --incremental \
  --skip-initial-unseeded \
  --continue-on-market-error
