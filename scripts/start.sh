#!/usr/bin/env bash
# scripts/start.sh — Start agg-btc-receiver in daemon mode via screen
# Usage: bash scripts/start.sh [start|stop|restart|attach|logs]

set -euo pipefail

SESSION="agg-btc-receiver"
DIR="$(cd "$(dirname "$0")/.." && pwd)"
LOG="$DIR/runtime/receiver_$(date +%Y%m%d).log"
RESTART_LOG="$DIR/runtime/restart.log"

mkdir -p "$DIR/runtime"

case "${1:-start}" in
  start)
    if screen -ls 2>/dev/null | grep -q "$SESSION"; then
      echo "[agg-btc] already running (screen session: $SESSION)"
      exit 0
    fi
    echo "[agg-btc] starting in screen session: $SESSION"
    echo "[agg-btc] log: $LOG"
    screen -dmS "$SESSION" bash -c "
      cd '$DIR'
      while true; do
        echo \"[\$(date '+%H:%M:%S')] starting...\" >> '$RESTART_LOG'
        node fairprice_monitor.mjs --config config.v3.json --seconds 0 >> '$LOG' 2>&1
        EC=\$?
        echo \"[\$(date '+%H:%M:%S')] exited with code \$EC, restarting in 10s...\" >> '$RESTART_LOG'
        sleep 10
      done
    "
    sleep 2
    if screen -ls 2>/dev/null | grep -q "$SESSION"; then
      echo "[agg-btc] started ✅"
    else
      echo "[agg-btc] failed to start ❌"
      exit 1
    fi
    ;;
  stop)
    if screen -ls 2>/dev/null | grep -q "$SESSION"; then
      screen -S "$SESSION" -X quit
      echo "[agg-btc] stopped"
    else
      echo "[agg-btc] not running"
    fi
    ;;
  restart)
    "$0" stop
    sleep 1
    "$0" start
    ;;
  attach)
    screen -r "$SESSION"
    ;;
  logs)
    tail -f "$LOG"
    ;;
  status)
    if screen -ls 2>/dev/null | grep -q "$SESSION"; then
      echo "[agg-btc] running ✅"
    else
      echo "[agg-btc] not running"
    fi
    ;;
  *)
    echo "Usage: $0 [start|stop|restart|attach|logs|status]"
    exit 1
    ;;
esac
