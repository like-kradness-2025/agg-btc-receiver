#!/usr/bin/env bash
# run-cleanup-raw.sh — maintenance entrypoint for deleting raw windows
# that are already represented in data/1s_features.

set -euo pipefail

cd /home/weed420/dev/github/like-kradness-2025/agg-btc-receiver

node scripts/cleanup-raw.mjs \
  --data data/live_v3 \
  --features data/1s_features \
  --safety-margin 300 \
  "$@"
