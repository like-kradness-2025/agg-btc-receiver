#!/usr/bin/env bash
# run-cleanup-raw.sh — maintenance entrypoint for deleting raw windows
# that have a committed/finalized TFP feature shard.

set -euo pipefail

cd /home/weed420/Tool/agg-btc-receiver

# Preserve the price-level TFP footprint before deleting its raw input.
/home/weed420/Tool/tv-footprint-agg-btc/scripts/materialize-tfp-footprint.sh

node scripts/cleanup-raw.mjs \
  --data data/live_v3 \
  --derived data/derived/burst_features_v1 \
  --safety-margin 300 \
  "$@"
