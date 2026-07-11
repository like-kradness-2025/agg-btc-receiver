#!/usr/bin/env bash
# scripts/lock-helper.sh — same-host flock-based market lock for burst reducer
# Contract (plan §P0-1, pre-implementation contract #4):
#   - flock -x -n on <output_root>/locks/<market>.lock
#   - Lock acquisition failure = skip with structured stderr, exit 0
#   - Kernel releases advisory lock on process death (no mkdir stale-lock)
#   - Backfill and cron MUST use the same lock protocol.
#
# Usage (source this file):
#   source scripts/lock-helper.sh
#   acquire_market_lock "binance_perp" "data/derived/burst_features_v1" || exit 0
#   # ... do work ...
#   release_market_lock  # optional; exit/process death also releases

LOCK_FD=""

# Acquire exclusive non-blocking flock for a market.
# Args: $1 = market name, $2 = output root dir (default: data/derived/burst_features_v1)
# Returns: 0 on success (lock acquired), 1 on contention (skip)
acquire_market_lock() {
  local market="$1"
  local output_root="${2:-data/derived/burst_features_v1}"
  local lock_dir="${output_root}/locks"
  local lock_file="${lock_dir}/${market}.lock"

  mkdir -p "${lock_dir}" 2>/dev/null || true

  # Open lock file on a new fd and try exclusive non-blocking flock
  exec {LOCK_FD}<>"${lock_file}"
  if ! flock -x -n "${LOCK_FD}" 2>/dev/null; then
    exec {LOCK_FD}>&- 2>/dev/null || true
    LOCK_FD=""
    echo "{\"ts\":\"$(date -u +%Y-%m-%dT%H:%M:%SZ)\",\"level\":\"SKIP\",\"reason\":\"lock-contention\",\"market\":\"${market}\",\"lock_file\":\"${lock_file}\"}" >&2
    return 1
  fi

  echo "{\"ts\":\"$(date -u +%Y-%m-%dT%H:%M:%SZ)\",\"level\":\"INFO\",\"msg\":\"lock-acquired\",\"market\":\"${market}\"}" >&2
  return 0
}

# Release the currently held lock.
release_market_lock() {
  if [ -n "${LOCK_FD}" ]; then
    flock -u "${LOCK_FD}" 2>/dev/null || true
    exec {LOCK_FD}>&- 2>/dev/null || true
    LOCK_FD=""
  fi
}
