#!/usr/bin/env bash
# run-cleanup-raw.sh — maintenance entrypoint for deleting raw windows
# that have a committed/finalized TFP feature shard.

set -euo pipefail

cd /home/weed420/Tool/agg-btc-receiver

DATA_ROOT=data/live_v4
DERIVED_ROOT=data/derived/burst_features_v2

if [[ ! -d "$DATA_ROOT" ]]; then
  echo "data root not found; nothing to clean"
  exit 0
fi

node scripts/cleanup-raw.mjs \
  --data "$DATA_ROOT" \
  --raw-layout v4 \
  --derived "$DERIVED_ROOT" \
  --consumer-cursors "$DERIVED_ROOT" \
  --consumer-manifests "$DERIVED_ROOT" \
  "$@"
