#!/usr/bin/env bash
# Daily raw-v4 archive. Closed raw stays local for 24h; Parquet stays 90d.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$ROOT"

mkdir -p data/archive
exec 9>>data/archive/archive.lock
if ! flock -n 9; then
  echo "[archive] another archive run is already in progress; skipping"
  exit 0
fi

exec node scripts/archive-raw-v4.mjs \
  --data data/live_v4 \
  --archive data/archive/raw_v4 \
  --manifests data/archive/manifests \
  --raw-retention-hours 24 \
  --archive-retention-days 90 \
  "$@"
