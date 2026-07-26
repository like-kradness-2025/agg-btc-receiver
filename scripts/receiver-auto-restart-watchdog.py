#!/usr/bin/env python3
"""Guarded health watchdog for the SQLite receiver.

Read-only checks are performed against health.jsonl. A restart is requested
only after repeated evidence, with cooldown and a rolling restart budget.
Use --dry-run for inspection without receiver side effects.
"""
from __future__ import annotations
import argparse, json, os, subprocess, time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
HEALTH = Path(os.environ.get("RECEIVER_HEALTH_PATH", ROOT / "data/live_sqlite/health.jsonl"))
STATE = Path(os.environ.get("XDG_RUNTIME_DIR", f"/run/user/{os.getuid()}")) / "agg-btc-receiver-watchdog.json"
SERVICE = "agg-btc-receiver.service"
MODULE_REQUEST = STATE.parent / "agg-btc-receiver-module-restart.json"
REQUIRED = tuple(x for x in os.environ.get("RECEIVER_REQUIRED_MARKETS", "binance_perp").split(",") if x)
STALE_MS = int(os.environ.get("RECEIVER_HEALTH_STALE_MS", "90000"))
COOLDOWN_S = int(os.environ.get("RECEIVER_RESTART_COOLDOWN_S", "600"))
MAX_RESTARTS = int(os.environ.get("RECEIVER_MAX_RESTARTS", "3"))
WINDOW_S = int(os.environ.get("RECEIVER_RESTART_WINDOW_S", "3600"))
BAD_BATCHES = int(os.environ.get("RECEIVER_BAD_BATCHES", "3"))


def read_json(path: Path):
    try:
        lines = path.read_text().splitlines()
        return json.loads(lines[-1]) if lines else None
    except (OSError, ValueError):
        return None


def evaluate(now_ms: int):
    reasons = []
    health = read_json(HEALTH)
    if not health:
        reasons.append("health_missing_or_invalid")
    else:
        age = now_ms - int(health.get("ts", 0))
        if age > STALE_MS:
            reasons.append(f"health_stale:{age}ms")
        for market in REQUIRED:
            m = health.get("markets", {}).get(market, {})
            last = int(m.get("lastDepthMsgAt", 0) or 0)
            if last and now_ms - last > STALE_MS:
                reasons.append(f"depth_stale:{market}:{now_ms-last}ms")
            if m.get("state") in {"error", "reconnecting"}:
                reasons.append(f"market_state:{market}:{m['state']}")
            if m.get("ioFailure"):
                reasons.append(f"writer_io_failure:{market}")
    return reasons


def load_state():
    try: return json.loads(STATE.read_text())
    except (OSError, ValueError): return {"failures": 0, "restarts": []}


def save_state(state):
    STATE.parent.mkdir(parents=True, exist_ok=True)
    tmp = STATE.with_suffix(".tmp")
    tmp.write_text(json.dumps(state, separators=(",", ":")))
    os.replace(tmp, STATE)


def restart_market(market: str, reason: str):
    request_tmp = MODULE_REQUEST.with_suffix(".tmp")
    request_tmp.write_text(json.dumps({"market": market, "reason": reason}))
    os.replace(request_tmp, MODULE_REQUEST)
    pid = int(subprocess.check_output(["systemctl", "--user", "show", SERVICE, "-p", "MainPID", "--value"], text=True).strip())
    if pid <= 1:
        raise RuntimeError(f"invalid receiver MainPID: {pid}")
    os.kill(pid, 12)  # SIGUSR2: main routes restart to the owning worker


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()
    now = time.time()
    reasons = evaluate(int(now * 1000))
    state = load_state()
    state.setdefault("failures", 0); state.setdefault("restarts", [])
    state["restarts"] = [x for x in state["restarts"] if now - x < WINDOW_S]
    if reasons:
        state["failures"] += 1
    else:
        state["failures"] = 0
    action = "none"
    if state["failures"] >= BAD_BATCHES and reasons:
        last_restart = state["restarts"][-1] if state["restarts"] else 0
        if now - last_restart < COOLDOWN_S:
            action = "cooldown"
        elif len(state["restarts"]) >= MAX_RESTARTS:
            action = "restart_budget_exhausted"
        elif not args.dry_run:
            market = next((m for m in REQUIRED if any(f"{prefix}:{m}:" in item for prefix in ("depth_stale", "market_state", "writer_io_failure") for item in reasons)), REQUIRED[0])
            restart_market(market, ";".join(reasons))
            state["restarts"].append(now); state["failures"] = 0; action = f"module_restarted:{market}"
        else:
            action = "would_restart"
    save_state(state)
    print(json.dumps({"action": action, "failures": state["failures"], "reasons": reasons}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
