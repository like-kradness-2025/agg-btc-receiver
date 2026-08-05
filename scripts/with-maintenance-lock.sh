#!/usr/bin/env bash
# Run one maintenance command while holding the shared, kernel-backed lock.
# Contention is a normal skip (exit 0); lock I/O and command failures propagate.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
LOCK_FILE="${MAINTENANCE_LOCK_FILE:-$REPO_ROOT/data/live_v4/state/maintenance.lock}"

usage() {
  echo "usage: $0 [--lock-file PATH] -- COMMAND [ARG ...]" >&2
}

while (($#)); do
  case "$1" in
    --lock-file)
      if (($# < 2)); then usage; exit 64; fi
      LOCK_FILE="$2"
      shift 2
      ;;
    --)
      shift
      break
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      usage
      exit 64
      ;;
  esac
done

if (($# == 0)); then
  usage
  exit 64
fi

LOCK_DIR="${LOCK_FILE%/*}"
[[ "$LOCK_DIR" == "$LOCK_FILE" ]] && LOCK_DIR="."
mkdir -p -- "$LOCK_DIR"

# The descriptor stays open across exec; the kernel releases the lock on exit.
exec 9>>"$LOCK_FILE"
if ! flock -x -n 9; then
  echo "maintenance-lock: skip (contention) file=$LOCK_FILE" >&2
  exit 0
fi

echo "maintenance-lock: acquired file=$LOCK_FILE" >&2
exec "$@"
