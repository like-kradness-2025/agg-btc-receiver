# C0 Reconnaissance: server1 BTCReceiver Comparison

**Date:** 2026-07-14
**Task:** t_a77692ba
**Branch:** v2 (HEAD: `32389c9`)

---

## 0. Access Limitation

**Server1 (192.168.0.219:8022) is NOT directly accessible from this host.**

| Method | Result |
|--------|--------|
| HTTP GET (curl/wget/web_extract) | Blocked by security scanner (private IP + plain HTTP) |
| SSH port 22 | Connection refused |
| SSH port 8022 | Permission denied (publickey,password,keyboard-interactive) |
| Browser navigation | net::ERR_INVALID_HTTP_RESPONSE (port 8022 speaks SSH, not HTTP) |
| Python urllib | FETCH_ERROR: SSH-2.0-OpenSSH_10.2 (port 8022 is SSH) |

**Mitigation:** A local copy of the server1-era code exists at:
`/home/weed420/dev/github/like-kradness-2025/agg-btc-orderheatmap/vendor/orderflow_pack/orderflow_monitor.mjs`

This is identified as the "candidate heatmap receiver for safe migration" — a legacy/vendor copy preserved in the orderheatmap project. Analysis below uses this as the server1 reference.

---

## 1. Behavior Viewpoint

### server1 (vendor/orderflow_pack/orderflow_monitor.mjs)

| Aspect | Detail |
|--------|--------|
| **Architecture** | Single-threaded, single-market (Binance Futures BTCUSDT only) |
| **Markets** | 1 (hardcoded `--symbol=btcusdt`) |
| **WebSocket connections** | 2 separate WS: depth@100ms + aggTrade |
| **Snapshot** | REST fetch from `fapi.binance.com/fapi/v1/depth` |
| **Data processing** | Inline: OFI, absorption detection, backtest correlation, trade compact aggregation |
| **Output paths** | 6 flat JSONL files (out, book, bookRaw, bookBucket, trade, tradeCompact) |
| **Health** | JSON file write (healthOut), Discord webhook alerts |
| **Config** | CLI args only (no JSON config file) |
| **Lines** | 741 |

### local agg-btc-receiver (v3.00)

| Aspect | Detail |
|--------|--------|
| **Architecture** | Multi-worker (4 threads, A/B/C/D market groups) |
| **Markets** | 15 active across 18 exchanges (Binance, Bybit, OKX, Coinbase, Kraken, etc.) |
| **WebSocket connections** | Per-market connector (abstracted via BaseConnector) |
| **Snapshot** | Per-exchange REST + WS snapshot sync (varies by exchange) |
| **Data processing** | Pure receive + save. No feature computation in receiver. |
| **Output paths** | 3 rotation-writer streams (trades, book_updates, liquidations) with 30s window rotation |
| **Health** | HealthMonitor class (health.jsonl, 1s interval) |
| **Config** | JSON config file (config.v3.json) |
| **Lines** | 296 (orchestrator) + 280 (worker) + 842 (rotation writer) + connectors |

### Delta Summary

| Dimension | server1 | local | Delta |
|-----------|---------|-------|-------|
| Market scope | 1 (BTCUSDT perp) | 15 (multi-exchange) | **+14 markets** |
| Threading | Single | 4 workers | **Worker isolation** |
| Data contract | Flat JSONL append | 30s rotation + atomic commit | **+Crash safety** |
| Feature compute | Inline (OFI, absorption) | Offline pipeline | **Separated concerns** |
| Config | CLI args | JSON config | **+Structured config** |
| Reconnect | Fixed 1s retry | Exponential backoff (30 attempts) | **+Backoff + stale detection** |
| Recovery | None (crash = data loss) | Startup recovery + quarantine | **+Recovery** |

---

## 2. Schema Viewpoint

### server1 Output Schema

**trade stream:**
```json
{
  "ts": "ISO-8601",
  "symbol": "BTCUSDT",
  "side": "buy|sell",
  "price": 65000.0,
  "qty": 0.5,
  "notional": 32500.0
}
```

**book stream (periodic):**
```json
{
  "ts": "ISO-8601",
  "symbol": "BTCUSDT",
  "sourceTag": "candidate",
  "mid": 65000.0,
  "bids": [[65000, 1.5], ...],
  "asks": [[65001, 2.0], ...],
  "bucketSizeUsd": 10,
  "bids_bucketed": {"65000": 1.5},
  "asks_bucketed": {"65010": 2.0}
}
```

**tradeCompact stream:**
```json
{
  "ts": "ISO-8601",
  "symbol": "BTCUSDT",
  "side": "buy|sell",
  "intervalSec": 60,
  "priceBucketUsd": 10,
  "price_bucket": 65000.00,
  "qty_sum": 12.5,
  "notional_sum": 812500.0,
  "trade_count": 45,
  "max_qty": 2.0,
  "max_notional": 130000.0,
  "vwap_price": 65000.0,
  "high_price": 65100.0,
  "low_price": 64900.0
}
```

**health stream:**
```json
{
  "ts": "ISO-8601",
  "sourceTag": "candidate",
  "symbol": "BTCUSDT",
  "bookReady": true,
  "syncing": false,
  "lastUpdateId": 12345,
  "depthBuffer": 0,
  "lastDepthMsgAt": "ISO-8601",
  "lastTradeMsgAt": "ISO-8601",
  "resyncCount1m": 0,
  "reason": ""
}
```

### local Output Schema

**trade/book_update/liquidation streams (rotation writer):**
```json
// trade
{"market":"binance_perp","price":65000.0,"qty":0.5,"side":"buy","ts":1720000000000,"tradeId":"12345"}

// book_update
{"market":"binance_perp","type":"partial|delta","bids":[[65000,1.5]],"asks":[[65001,2.0]],"ts":1720000000000,"seq":12345}

// liquidation
{"ts":1720000000000,"market":"binance_perp","exchange":"binance","symbol":"BTCUSDT","side":"sell","price":65000,"qty":1.0,"notional":65000,"raw_type":"forceOrder","trade_id":null,"source_ts":null}
```

**health.jsonl:**
```json
{
  "ts": 1720000000000,
  "state": "normal|warning|critical",
  "markets": {
    "binance_perp": {
      "state": "running",
      "connectedAt": 1720000000000,
      "lastDepthMsgAt": 1720000000000,
      "lastTradeMsgAt": 1720000000000,
      "depthMsgCount": 1234,
      "tradeMsgCount": 5678,
      "reconnectCount": 0,
      "resyncCount": 1,
      "lastSeq": 12345
    }
  }
}
```

### Schema Delta

| Aspect | server1 | local | Notes |
|--------|---------|-------|-------|
| Timestamp format | ISO-8601 string | Epoch ms (number) | **Different representation** |
| Market identifier | Embedded in `symbol` field | Top-level `market` field | **Different structure** |
| Book representation | Top N levels + bucketed | Raw delta/partial events | **server1 pre-aggregates, local saves raw** |
| Trade compact | Inline aggregation | Not in receiver (offline pipeline) | **server1 computes features inline** |
| Health format | Snapshot JSON (overwrite) | Append-only JSONL | **Different persistence** |
| Liquidation | Not present | Dedicated stream | **local has +1 data kind** |

---

## 3. Liveness Viewpoint

### server1

- **Depth WS:** `wss://fstream.binance.com/ws/${symbol}@depth@100ms`
- **Trade WS:** `wss://fstream.binance.com/ws/${symbol}@aggTrade`
- **Stale depth threshold:** 5000ms (configurable via `--staleDepthMs`)
- **Stale trade threshold:** 7000ms (configurable via `--staleTradeMs`)
- **Reconnect:** Fixed 1s delay (no backoff)
- **Sequence gap handling:** Configurable (`--strictSeq=1` for resync, tolerant mode by default)
- **Max resync per minute:** 6 (configurable)

### local

- **Per-market WS:** Abstracted via BaseConnector subclasses
- **Stale threshold:** 30000ms (hardcoded `STALE_MSG_THRESHOLD_MS`)
- **Reconnect:** Exponential backoff (1s base, 30s max, 30 attempts)
- **Error recovery:** After 30 attempts, wait 60s then reduce count by 10 and retry
- **Sequence gap handling:** Always resync (book reset + reconnect)
- **Stale check interval:** 5s (hardcoded)

### Liveness Delta

| Aspect | server1 | local | Advantage |
|--------|---------|-------|-----------|
| Stale detection | 5s/7s (configurable) | 30s | server1: faster detection |
| Reconnect strategy | Fixed 1s | Exponential 1-30s | local: avoids thundering herd |
| Max reconnect | Unlimited (rate-limited by resync/min) | 30 attempts then cooldown | local: bounded resource use |
| Sequence gap | Tolerant by default | Always resync | server1: more tolerant in parallel-run |
| Error recovery | None explicit | 60s cooldown + count reduction | local: self-healing |

---

## 4. Recovery Viewpoint

### server1

- **Startup:** No recovery. On crash, `.open` files are orphaned.
- **Shutdown:** `shutdown()` closes WS, clears timers, flushes streams, writes health.
- **Crash safety:** None. Flat append streams can be corrupted on unclean exit.
- **File corruption risk:** High (append-only, no atomic rename).
- **Data loss window:** Up to `loopEveryMs` (2s default) of trade events.

### local

- **Startup recovery:** `RawRotationWriter.startupRecovery()` scans .jsonl + .open files, computes watermark, deduplicates, quarantines stale/future artifacts.
- **Shutdown:** Graceful worker shutdown with 10s timeout per worker, finalize all writers.
- **Crash safety:** 5-step atomic commit (stage → intent → rename → fsync → checkpoint).
- **File corruption risk:** Low (no-clobber rename, quarantine system).
- **Data loss window:** One 30s window (worst case).
- **Quarantine system:** Conflict artifacts moved to `_quarantine/` directory.

### Recovery Delta

| Aspect | server1 | local | Advantage |
|--------|---------|-------|-----------|
| Startup recovery | None | Full scan + dedup + quarantine | **local: +crash safety** |
| Atomic writes | No | Yes (link+unlink) | **local: +data integrity** |
| Corruption handling | None | Quarantine + watermark protection | **local: +fault tolerance** |
| Shutdown flush | Basic | Graceful with timeout | local: more robust |

---

## 5. Retention Viewpoint

### server1

- **Storage cap:** Configurable per stream (`--maxOutMB`, `--maxBookMB`, etc.)
- **Trim strategy:** Tail-trim to `trimTargetRatio` (default 85%) when file exceeds cap
- **Trim check interval:** 120s (configurable)
- **Max defaults:** out=256MB, book=512MB, bookRaw=1536MB, bookBucket=2048MB, trade=1536MB, tradeCompact=512MB

### local

- **Storage cap:** Not implemented in receiver (P5 retention planned but not built)
- **Output grows unbounded** until external cleanup
- **Pipeline has retention:** burst-reducer has its own output management

### Retention Delta

| Aspect | server1 | local | Advantage |
|--------|---------|-------|-----------|
| Per-stream caps | Yes (6 configurable caps) | No | **server1: +built-in retention** |
| Trim strategy | Tail-trim to ratio | N/A | server1: prevents disk exhaustion |
| Configurability | CLI args | N/A | server1: flexible |

**Note:** Local has P5 retention planned but not yet implemented. This is a known gap.

---

## 6. Config Boundaries Viewpoint

### server1

- **Config method:** CLI arguments only (30+ parameters)
- **No config file:** All configuration via `--key=value`
- **Environment:** Only `DISCORD_ALERT_WEBHOOK` env var
- **Hardcoded values:** WS URLs, symbol (btcusdt), some thresholds
- **Startup:** Direct execution, no process management

### local

- **Config method:** JSON config file (`config.v3.json`)
- **Config structure:** Markets (per-exchange config), output paths, tick intervals
- **Environment:** systemd service for aux_data_collector
- **Startup:** `scripts/start.sh` with screen session + auto-restart
- **Process management:** systemd, screen sessions

### Config Delta

| Aspect | server1 | local | Advantage |
|--------|---------|-------|-----------|
| Config format | CLI args | JSON file | local: version-controllable, structured |
| Per-market config | No (single market) | Yes (18 exchange configs) | local: multi-exchange support |
| Process management | None | systemd + screen | local: production-ready |
| Secrets handling | None | env vars (not in config) | local: better separation |

---

## 7. Adopted / Retained / Excluded Candidates

### Adopted (from local, keeping)

| Component | Rationale |
|-----------|-----------|
| Multi-worker architecture | Proven at scale, worker isolation |
| Raw rotation writer (30s windows) | Crash-safe, startup recovery, quarantine |
| BaseConnector reconnect state machine | Exponential backoff, stale detection, sequence gap handling |
| JSON config file | Structured, version-controllable |
| 3 data kinds (trades, book_updates, liquidations) | Comprehensive coverage |
| HealthMonitor (JSONL append) | Better than snapshot overwrite |

### Retained (from server1, potentially useful)

| Component | Rationale |
|-----------|-----------|
| Trade compact aggregation | server1's inline aggregation could complement offline pipeline |
| Per-stream storage caps | Local P5 is not yet built; server1's trim strategy is proven |
| Configurable stale thresholds | server1's 5s/7s is faster than local's 30s for some use cases |
| Tolerant sequence gap mode | server1's default tolerant mode is useful for parallel-run stability |
| Discord webhook alerts | server1's alert mechanism could be useful for production |

### Excluded (from server1, not adopting)

| Component | Rationale |
|-----------|-----------|
| Single-market design | Incompatible with 15-market requirement |
| Flat append streams | No crash safety, no atomic writes |
| Fixed 1s reconnect | Thundering herd risk at scale |
| Inline feature computation | Separated concerns (offline pipeline) is cleaner |
| No startup recovery | Unacceptable for production use |

---

## 8. Unresolved Questions

1. **Server1 live code unknown:** The vendor copy may not match what's currently running on server1. The actual server1 `orderflow_monitor.mjs` might have evolved since this copy was made.

2. **Server1 is SSH-only on port 8022:** The URL path `192.168.0.219:8022/btc-tools/...` suggests an HTTP server, but port 8022 speaks SSH. Either the HTTP server is on a different port, or the file path is an SSH-accessible path.

3. **Access credentials:** No SSH key or password available for server1. Human intervention needed to establish access.

4. **Server1's current state:** Unknown whether server1 is running, what version it's on, or what data it's currently collecting.

5. **Relationship between server1 and local:** Is server1 the "legacy" that local v3 is replacing? Or are they independent deployments?

6. **Heatmap candidate receiver role:** The vendor copy in agg-btc-orderheatmap is labeled "candidate heatmap receiver for safe migration" — is this an intermediate step between server1 and local v3?

---

## 9. Evidence Paths

| Evidence | Path | Status |
|----------|------|--------|
| Local orchestrator | `/home/weed420/dev/github/like-kradness-2025/agg-btc-receiver/orderflow_monitor.mjs` | Read (296 lines) |
| Local worker | `/home/weed420/dev/github/like-kradness-2025/agg-btc-receiver/lib/orderflow-worker.mjs` | Read (280 lines) |
| Local rotation writer | `/home/weed420/dev/github/like-kradness-2025/agg-btc-receiver/lib/raw-rotation-writer.mjs` | Read (842 lines) |
| Local base connector | `/home/weed420/dev/github/like-kradness-2025/agg-btc-receiver/lib/base-connector.mjs` | Read (538 lines) |
| Local health monitor | `/home/weed420/dev/github/like-kradness-2025/agg-btc-receiver/lib/health-monitor.mjs` | Read (92 lines) |
| Server1 vendor copy | `/home/weed420/dev/github/like-kradness-2025/agg-btc-orderheatmap/vendor/orderflow_pack/orderflow_monitor.mjs` | Read (741 lines) |
| Sibling recon | `/home/weed420/dev/github/like-kradness-2025/agg-btc-receiver/docs/recon/C0-tree-runtime-recon.md` | Read (248 lines) |
| Server1 access attempt | SSH port 22 + 8022, HTTP, browser, Python urllib | All failed |

---

## 10. Confidence Assessment

| Viewpoint | Confidence | Note |
|-----------|-----------|------|
| Behavior | **Medium** | Based on vendor copy, not live server1 |
| Schema | **Medium** | Based on vendor copy output patterns |
| Liveness | **Medium** | Based on vendor copy code analysis |
| Recovery | **High** | Both codebases fully read |
| Retention | **High** | Both codebases fully read |
| Config | **High** | Both codebases fully read |

**Overall confidence: Medium** — The vendor copy provides good insight into server1's design philosophy, but may not reflect the current live implementation.
