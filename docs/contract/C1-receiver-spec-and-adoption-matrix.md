# C1 Contract Gate: Receiver-Only Spec and Adoption Matrix

**Date:** 2026-07-15
**Task:** t_bdd83d94 (C1 gate), C6a-c (validation/canonical/EXDEV), C6d (closure)
**Branch:** v2 (HEAD: `32389c9`)
**Status:** C6 series complete — spec reconciled with implementation

---

## Table of Contents

1. [Scope and Authority](#1-scope-and-authority)
2. [Raw-Only Receiver Boundary](#2-raw-only-receiver-boundary)
3. [Auxiliary Receive-Only Boundaries](#3-auxiliary-receive-only-boundaries)
4. [Quality Evidence Limits](#4-quality-evidence-limits)
5. [Output Schemas](#5-output-schemas)
6. [Empty / Missing / Error Semantics](#6-empty--missing--error-semantics)
7. [Liveness Contract](#7-liveness-contract)
8. [Recovery Contract](#8-recovery-contract)
9. [Retention Contract](#9-retention-contract)
10. [Rollback Contract](#10-rollback-contract)
11. [Explicit Exclusions](#11-explicit-exclusions)
12. [Local vs Server1 Evidence Comparison](#12-local-vs-server1-evidence-comparison)
13. [Six-View Risk Analysis](#13-six-view-risk-analysis)
14. [Implementation Dependency Graph](#14-implementation-dependency-graph)
15. [95-Point Review Rubric](#15-95-point-review-rubric)
16. [Open Contract Questions — Blockers](#16-open-contract-questions--blockers)
17. [Evidence Paths](#17-evidence-paths)

---

## 1. Scope and Authority

### 1.1 Purpose

Define the **Receiver-Only Contract** for the agg-btc-receiver v3 system: the exact boundary, schema, semantics, quality guarantees, and operational properties of the raw data capture path, independent of any downstream consumer, pipeline, or gateway.

### 1.2 Scope

**In scope:** Everything from WebSocket connection acceptance through persistent raw JSONL storage at rest. This includes:

- orderflow_monitor.mjs (multi-worker orchestrated receiver — **canonical entrypoint**)
- fairprice_monitor.mjs (single-thread fair price receiver) — legacy raw receive path, excluded from production per B1 decision
- All connector subclasses (Binance, Bybit, OKX, Coinbase, Kraken, Bitstamp, Crypto.com, Bitfinex, Bitmex, Hyperliquid)
- RawRotationWriter (30s window rotation with atomic finalize)
- HealthMonitor (liveness output)
- BaseConnector reconnect/sync state machine
- BufferedWriter (flush lifecycle)
- All 15 currently enabled markets

**Out of scope:** (see §11 Explicit Exclusions for full list)

- Burst feature computation (burst-reducer pipeline)
- Derived output tiers (features_1s, features_30s, features_5min)
- Consumer APIs (FiveMinConsumer, Gateway)
- REST auxiliary data collection (aux_data_collector)
- Dashboard
- Market discovery / symbol management

### 1.3 Contract Authority

This contract is a **C1 gate deliverable** synthesized from C0 reconnaissance of the running tree (t_55c23a38) and server1 comparison (t_a77692ba), verified against the actual source code at the identified HEAD (v2, `32389c9`). Where evidence conflicts, direct source inspection wins. Five open questions (B1-B5) were surfaced and resolved via human decisions during C1 gate; B6 was added during C6 series to clarify the intentional override path. All decisions are tracked in §16 below.

### 1.4 Adoption Matrix

| Decision | Component | Source | Rationale |
|----------|-----------|--------|-----------|
| **ADOPT** | Multi-worker receiver architecture (orderflow_monitor.mjs) | Local v3 | Proven at 15-market scale, worker isolation per market group |
| **ADOPT** | **orderflow_monitor.mjs as canonical production receiver** | Local v3 | Multi-worker raw-only architecture; systemd-ready; B1 decision: this is the canonical receiver |
| **ADOPT** | RawRotationWriter with 30s window rotation | Local v3 | Crash-safe atomic commit via no-clobber rename, startup recovery, quarantine system |
| **ADOPT** | BaseConnector reconnect state machine | Local v3 | Exponential backoff (1-30s, 30 attempts), stale detection (30s), sequence gap handling |
| **ADOPT** | 3 data kinds (trades, book_updates, liquidations) | Local v3 | Comprehensive raw coverage; liquidations absent in server1 |
| **ADOPT** | HealthMonitor JSONL append output | Local v3 | Better than server1's single-file overwrite; append facilitates monitoring |
| **ADOPT** | JSON config file (config.v3.json) | Local v3 | Version-controllable, structured, per-exchange configuration |
| **ADOPT** | Producer-separated output roots (`data/receiver/agg/` + `data/receiver/server1/`) | B4 decision | Fully isolated data roots per producer; no cross-writer file collisions |
| **ADOPT** | Market-level graceful degradation (B2) | Local v3 | Failed markets do not take down all 15 markets; each market's health is tracked independently |
| **ADOPT** | **Server1 and agg run in parallel** (B3) | B3 decision | Independent systems with isolated output roots; no server1 modification or replacement assumption; agg is not a migration target |
| **RETAIN** | fairprice_monitor.mjs as legacy entrypoint | Local v3 | B1 decision: preserve for migration/reference; do NOT run as production Receiver. Retire after consumer/output-path audit and explicit cutover |
| **RETAIN** | Per-stream storage caps (server1 pattern) | Server1 | Local P5 not built; server1's tail-trim strategy is proven in production |
| **RETAIN** | Configurable stale thresholds (5s/7s pattern) | Server1 | Local 30s hardcoded threshold may be too slow for time-sensitive detection |
| **RETAIN** | Tolerant sequence gap mode | Server1 | Always-resync is safe but expensive; tolerant mode reduces reconnections on transient gaps |
| **RETAIN** | Discord webhook alert pattern | Server1 | No alert mechanism in local; server1's Discord integration is lightweight and proven |
| **RETAIN** | 3 disabled markets in current state (**B5**, refined **B6**) | B5 decision + B6 clarification | `binance_coinm_perp`: enablement candidate — **isolated smoke via `--markets binance_coinm_perp` is the ONLY intentional override path** (default `enabled:false` bypassed by explicit CLI; this is NOT production enable/cutover); see §16.6. `coinbase_international`: pending auth/secret injection; disabled indefinitely. `gemini_spot`: disabled indefinitely. Maintainers toggle enabled status via `config.v3.json`. **Default config excludes all `enabled:false` markets; explicit `--markets` override is intentional isolated-smoke only.** |
| **EXCLUDE** | Single-market design | Server1 | Incompatible with 15-market requirement |
| **EXCLUDE** | Flat append streams (no atomic rename) | Server1 | Unacceptable crash-safety; data corruption risk on unclean exit |
| **EXCLUDE** | Fixed 1s reconnect (no backoff) | Server1 | Thundering herd risk at 15-market scale |
| **EXCLUDE** | Inline feature computation (OFI, absorption) | Server1 | Separated concerns (offline pipeline) is architecturally cleaner |
| **EXCLUDE** | No startup recovery | Server1 | Accepting data loss on restart is production-unsafe |
| **EXCLUDE** | CLI-only configuration | Server1 | Not version-controllable, error-prone at scale |

---

## 2. Raw-Only Receiver Boundary

### 2.1 Strict Boundary Definition

The receiver boundary is defined as:

```
[Exchange WebSocket API] 
    → Connector (BaseConnector subclass)
    → RawRotationWriter (per-market, per-kind)
    → data/receiver/agg/raw/{trades,book_updates,liquidations}/<market>/<YYYY-MM-DD>/<HH-MM-SS>.jsonl
```

**Note on current vs contract path:** The current implementation writes to `data/live_v3/` (orderflow_monitor) and `data/raw_hot/` (fairprice_monitor). These paths are accepted as migration inputs until cutover per B4 decision. All new development and deployment MUST use the canonical `data/receiver/agg/raw/` layout below. Consumers, health monitors, and auxiliary outputs are similarly organized under `data/receiver/agg/health/` and `data/receiver/agg/aux/`.

**Rules:**

1. **No transformation.** Data passes through the receiver with zero structural transformation. The receiver does not compute features, aggregate, filter, or re-order events.
2. **Timestamp normalization only.** Timestamps are normalized from any exchange format (seconds, milliseconds, microseconds, nanoseconds) to integer epoch milliseconds. This is the ONLY structural change.
3. **Schema enforcement:** Each event is JSON-stringified as received from the connector. The connector's `emit('trade')`, `emit('depth')`, `emit('liquidation')` output IS the schema contract.
4. **No enrichment.** No market metadata, exchange info, or computed fields are added at receive time.
5. **No deduplication.** Duplicate events are preserved as received. Dedup is a consumer concern.
6. **No re-ordering.** Events are written in receive order, not timestamp order.

### 2.2 Three Data Kinds

| Kind | Source Connector Event | Rotation Writer | Schema Contract |
|------|----------------------|-----------------|-----------------|
| `trades` | `connector.on('trade', ...)` | `rawTradeRotationWriters` | §5.1 |
| `book_updates` | `connector.on('depth', ...)` | `bookUpdateRotationWriters` | §5.2 |
| `liquidations` | `connector.on('liquidation', ...)` | `liquidationRotationWriters` | §5.3 |

### 2.3 Directory Layout (contract)

The canonical layout (adopted per B4 decision) uses producer-separated roots with fully isolated output directories:

```
data/
├── receiver/
│   ├── agg/
│   │   ├── raw/
│   │   │   ├── trades/
│   │   │   │   └── <market>/<YYYY-MM-DD>/<HH-MM-SS>.jsonl
│   │   │   ├── book_updates/
│   │   │   │   └── <market>/<YYYY-MM-DD>/<HH-MM-SS>.jsonl
│   │   │   ├── liquidations/
│   │   │   │   └── <market>/<YYYY-MM-DD>/<HH-MM-SS>.jsonl
│   │   │   └── _quarantine/
│   │   │       └── <market>/<kind>/<YYYY-MM-DD>/<name>.conflict[.N]
│   │   ├── health/
│   │   │   └── health.jsonl
│   │   └── aux/
│   │       ├── derivatives/
│   │       └── market_data/
│   └── server1/
│       └── (reserved — independent root for legacy server1 producer)
├── live_v3/          ← current orderflow_monitor output (migration input only)
└── raw_hot/          ← current fairprice_monitor output (migration input only)
```

**Producer separation guarantees:**
- orderflow_monitor (agg) writes exclusively to `data/receiver/agg/raw/`
- fairprice_monitor (legacy) output is at `data/raw_hot/` until cutover
- server1 writes exclusively to `data/receiver/server1/`
- aux_data_collector writes to `data/receiver/agg/aux/`
- Downstream consumers select their input producer through an explicit manifest, not ambiguous symlinks
- No shared `data/raw` writer, no cross-producer file writes
- Migration from legacy paths (`live_v3/`, `raw_hot/`) to `data/receiver/agg/raw/` is a future cutover task, not part of this contract

### 2.4 File Lifecycle

```
<HH-MM-SS>.jsonl.open  ← BufferedWriter writing (active window)
<HH-MM-SS>.jsonl       ← finalized (no-clobber rename on window rotation)
<HH-MM-SS>.jsonl.open.conflict  ← EEXIST on finalize (quarantined)
<HH-MM-SS>.jsonl.future        ← future-timestamped (quarantined)
```

### 2.5 Window Rotation

- **Window duration:** 30,000 ms (hardcoded `WINDOW_MS`)
- **Window alignment:** UTC 30-second boundaries (`Math.floor(tsMs / 30000) * 30000`)
- **Tolerance:** Two concurrent writable windows (current + previous) to handle late-arriving events
- **Previous window finalization:** When a previous window falls ≥60s behind wall clock, it is finalized via `checkStale()`
- **Late event cutoff:** Events with `wMs <= finalizedWatermarkMs` are silently dropped
---



## 3. Auxiliary Receive-Only Boundaries

### 3.1 fairprice_monitor.mjs (Legacy Entrypoint)

While fairprice_monitor has additional responsibilities (FairPriceCollector, book snapshots), its raw receive path follows a similar but **legacy** contract:

| Aspect | Detail |
|--------|--------|
| **Status** | **Legacy** per B1 decision — do NOT run as production Receiver. Preserved for migration/reference; retire after consumer/output-path audit |
| **Output base** | `config.output.fairprice_base_path` (default: `data/raw_hot`) — migration input only per B4; canonical target is `data/receiver/agg/raw/` |
| **Raw streams** | liquidations only (no rotation writer for trades/book — those flow through FairPriceCollector) |
| **Liquidation writer** | `BufferedWriter` to `data/raw_hot/liquidations/<market>.jsonl` (single-file append) |
| **Book snapshots** | Periodic book state via FairPriceCollector (not a raw receiver concern) |
| **Contract gap:** | The `fairprice_monitor` uses simple append for liquidations, not the rotation writer — this is an inconsistency vs the orderflow_monitor contract |

**Contract decision:** The fairprice_monitor path is a **legacy appendix** to the receiver contract. Per B1 decision, the canonical receiver is `orderflow_monitor.mjs`. Future work should migrate any still-needed raw receive functionality to the RawRotationWriter pattern. fairprice_monitor's source is preserved temporarily for migration/reference, then retired after a consumer/output-path audit and explicit cutover. This path is **accepted as-is** for the duration of the migration window, with the note that its crash-safety guarantees are weaker (no atomic rename, single-file append).

### 3.2 aux_data_collector.mjs (REST Auxiliary Data)

This is a separate process running as a systemd service. It is **NOT part of the receiver contract** and collects REST-based data:

- DerivativesHelper: mark price, funding rate, open interest (perp markets only)
- MarketDataCollector: OHLCV (1m), ticker (24hr), LS ratio, taker volume

**Boundary:** aux_data_collector writes to `data/.../derivatives/` and `data/.../market_data/` paths. These are NOT consumed by the burst-reducer pipeline. They exist as independent data products.

### 3.3 orderflow_monitor vs fairprice_monitor Relationship

**Resolved (B1).** The canonical production Receiver is `orderflow_monitor.mjs` (multi-worker raw-only architecture). `fairprice_monitor.mjs` is a legacy/parallel entrypoint that must NOT be run as the production Receiver.

| Aspect | orderflow_monitor (CANONICAL) | fairprice_monitor (LEGACY) |
|--------|-----------------------------|---------------------------|
| Deployed via | Not yet deployed; target for systemd or start.sh update | start.sh (screen session) — legacy only |
| Architecture | Multi-worker (4 threads) | Single-thread |
| Raw data path (current) | `data/live_v3/` → migrate to `data/receiver/agg/raw/` | `data/raw_hot/` → retire after cutover |
| Data kinds | trades, book_updates, liquidations | liquidations only (trades/book via FairPriceCollector) |
| Crash safety | Rotation writer (atomic rename) | BufferedWriter (append only) |
| Startup recovery | Yes (startupRecovery) | No |
| Consumer contracts | Pipeline reads from `data/derived/`, not from either raw path directly | N/A |
| **Future** | **Replace legacy deployment** — update start.sh to launch orderflow_monitor or add systemd unit; fairprice source preserved for reference until consumer audit complete | **Retire after cutover** — remove only after explicit consumer audit confirms no remaining dependencies on `data/raw_hot/` paths |

**Action plan from B1 decision:**
1. Retain fairprice_monitor.mjs source temporarily for migration/reference
2. Audit all consumer/output paths to identify dependencies on `data/raw_hot/`
3. Switch production deployment to orderflow_monitor.mjs
4. After cutover verification, remove fairprice_monitor.mjs

**No service restart or code deletion performed from this contract task.**

---

## 4. Quality Evidence Limits

### 4.1 Connector Quality

| Metric | Contract | Evidence | Verified |
|--------|----------|----------|----------|
| Stale message threshold | 30,000ms hardcoded (`STALE_MSG_THRESHOLD_MS`) | `base-connector.mjs:7` | Yes |
| Reconnect attempts | 30 max (`MAX_RECONNECT_ATTEMPTS`) | `base-connector.mjs:6` | Yes |
| Reconnect backoff | Exponential 1s-30s + jitter | `base-connector.mjs:413-421` | Yes |
| Error recovery cooldown | 60s after 30 failures, reduce count by 10 | `base-connector.mjs:394-399` | Yes |
| Stale check interval | 5s | `base-connector.mjs:486` | Yes |
| Worker stale check | 15s (all writers) | `orderflow-worker.mjs:211` | Yes |
| Health push interval | 2s (worker → main) | `orderflow-worker.mjs:223` | Yes |
| Health write interval | 1s (health.jsonl) | `health-monitor.mjs:22` | Yes |
| Flush trades | 200ms (`flush_trades_ms`) | `config.v3.json:185` | Yes |
| Flush book | 1000ms (`flush_book_ms`) | `config.v3.json:186` | Yes |
| Flush liquidations | 200ms (`flush_liquidations_ms`) | `config.v3.json:188` | Yes |

### 4.2 Timestamp Normalization Bounds

| Input Unit | Absolute Range | Multiplier | Output |
|-----------|---------------|------------|--------|
| Seconds | `< 1e11` | ×1000 | Integer ms |
| Milliseconds | `< 1e14` | as-is | Integer ms |
| Microseconds | `< 1e17` | ÷1000 | Integer ms |
| Nanoseconds | `< 1e20` | ÷1,000,000 | Integer ms |
| Invalid/NaN/Infinity | — | — | `null` (dropped) |

**Contract:** Any event with a non-numeric or out-of-range timestamp is **silently dropped** with a console.error log. This is the receiver's only input validation.

### 4.3 Window Validity

| Validation | Rule | Action |
|-----------|------|--------|
| Negative timestamp | `tsMs < 0` | Drop with error log |
| Future window | `wMs > currentWallWindow` | Drop with error log |
| Behind watermark | `wMs <= finalizedWatermarkMs` | Drop with error log |
| After finalize | Any writer closed | Drop with error log |

### 4.4 Ring Buffer Bounds

- **Max buffer entries:** 65,536 (`RING_BUF_MAX = 65536`)
- **Behaviour at overflow:** Circular overwrite (oldest entries lost)
- **Impact window:** During snapshot sync, buffered events are replayed after snapshot. Overflow means some pre-snapshot events are permanently lost.

### 4.5 Checkpoint Size Bounds (Pipeline, Not Receiver)

Referenced here for completeness — these belong to the burst-reducer pipeline, not the receiver:

| Bound | Value | Action |
|-------|-------|--------|
| Warn | 256 KiB | Emit WARN |
| Hard limit | 1 MiB | Throw E026 |

---

## 5. Output Schemas

### 5.1 Trade Event Schema (from BaseConnector._emitTrade)

```typescript
// Emitted via: connector.on('trade', ...)
// Written to: data/live_v3/trades/<market>/<YYYY-MM-DD>/<HH-MM-SS>.jsonl
interface TradeEvent {
  market: string;         // e.g., "binance_perp"
  price: number;          // trade price as number
  qty: number;            // trade quantity as number
  side: 'buy' | 'sell';  // trade direction
  ts: number;             // epoch milliseconds (normalized)
  tradeId: string;        // exchange trade ID
}
```

**Source:** `base-connector.mjs:327-332`

### 5.2 Depth/Book Update Schema (from BaseConnector._emitDepth)

```typescript
// Emitted via: connector.on('depth', ...)
// Written to: data/live_v3/book_updates/<market>/<YYYY-MM-DD>/<HH-MM-SS>.jsonl
interface DepthEvent {
  market: string;         // e.g., "binance_perp"
  type: 'partial' | 'delta';  // snapshot or incremental update
  bids: [number, number][];   // [price, qty] pairs
  asks: [number, number][];   // [price, qty] pairs
  ts: number;             // epoch milliseconds (normalized)
  seq: number;            // exchange sequence number
}
```

**Source:** `base-connector.mjs:319-323`

### 5.3 Liquidation Event Schema (from BaseConnector._emitLiquidation)

```typescript
// Emitted via: connector.on('liquidation', ...)
// Written to: data/live_v3/liquidations/<market>/<YYYY-MM-DD>/<HH-MM-SS>.jsonl
// Also written to (fairprice_monitor): data/raw_hot/liquidations/<market>.jsonl
interface LiquidationEvent {
  ts: number;             // epoch milliseconds (when processed, not event time)
  market: string;         // e.g., "binance_perp"
  exchange: string;       // e.g., "binance"
  symbol: string;         // e.g., "BTCUSDT"
  side: 'buy' | 'sell';
  price: number;
  qty: number;
  notional: number;       // price * qty (or exchange-provided notional)
  raw_type: string;       // e.g., "forceOrder", "liquidation"
  trade_id: string | null;
  source_ts: number | null;
}
```

**Source:** `base-connector.mjs:347-361`

**Note:** The liquidation `ts` field is set to `Date.now()` at processing time, not the exchange timestamp. This is a design choice — the raw exchange timestamp is preserved in `source_ts` when available.

### 5.4 Health Event Schema (from HealthMonitor.getHealthSummary)

```typescript
// Written to: health.jsonl (one JSON object per line, appended)
interface HealthEvent {
  ts: number;                     // epoch milliseconds (write time)
  state: 'normal' | 'warning' | 'critical';
  markets: {
    [market: string]: {
      state: string;              // connector state
      connectedAt: number;        // epoch ms
      lastDepthMsgAt: number;     // epoch ms
      lastTradeMsgAt: number;     // epoch ms
      depthMsgCount: number;
      tradeMsgCount: number;
      reconnectCount: number;
      resyncCount: number;
      lastSeq: number;
    };
  };
}
```

**Source:** `health-monitor.mjs:56-85`

### 5.5 Raw Schema Constraints

| Constraint | Trade | Depth | Liquidation | Health |
|-----------|-------|-------|-------------|--------|
| JSON valid | Required | Required | Required | Required |
| `market` present | Required | Required | Required | N/A |
| `ts` numeric | Required | Required | Required | Required |
| `type` partial/delta/snapshot/update | N/A | Required | N/A | N/A |
| `bids`/`asks` array | N/A | Required | N/A | N/A |
| `side` buy/sell | Required | N/A | Required | N/A |
| `price` positive finite number | Required | N/A | Required | N/A |
| `qty` positive finite number | Required | N/A | Required | N/A |
| Max single line | 1 MiB (BufferedWriter default) | Same | Same | Same |

**Schema validation (C6a):** Connector emitters validate payloads before emitting. Malformed events (invalid type, non-array bids/asks, invalid side, non-positive price/qty) are silently dropped with a console.error log — fail-closed for corrupt data. Valid values pass through without transformation.

### 5.6 Schema Delta: Local vs Server1

| Aspect | Local v3 | Server1 | Impact |
|--------|----------|---------|--------|
| Timestamp format | Epoch ms (number) | ISO-8601 string | Consumers must handle both; epoch ms is more portable for numeric ops |
| Market identifier | Top-level `market` field | Embedded in `symbol` | Local is more query-friendly (no string parsing) |
| Book format | Raw delta/partial events | Top N snapshot + bucketed | Local preserves full depth sequence; server1 pre-aggregates |
| Trade aggregation | Individual trade events | Compact aggregation (1min, bucketed) | Local enables custom aggregation; server1 has built-in summarization |
| Liquidations | Dedicated stream | Not present | Local has +1 data kind |
| Health format | Append JSONL | Single-file overwrite | Local is more audit-friendly; server1 is more space-efficient |

---

## 6. Empty / Missing / Error Semantics

### 6.1 Empty Directory

A data kind directory with no files is **valid** — it means no events of that kind were received for that market on that date. No error is raised. On startup recovery, the directory is simply scanned with no files found.

### 6.2 Empty File

A `.jsonl` file with zero bytes after finalization is **technically possible** if:
- A 30-second window opened but received no events, then was finalized by stale checker
- The BufferedWriter was created but no data written

**Contract:** The BufferedWriter does not create a file on disk until the first write. Empty .open files are silently skipped during finalize (`_finalizeWriter` checks `fsp.access` and returns early if the file doesn't exist). Therefore, zero-byte final files should not occur under normal operation.

**Edge case:** A file written and then truncated externally is treated as a corrupted file. No receiver-level validation catches this — it is a file-system integrity concern.

### 6.3 Missing Market

A market listed in config.v3.json but with no corresponding data directory means:
- The market was either never connected (e.g., startup failure)
- Or the market has no events of that kind (e.g., spot markets have no liquidations)

**Contract:** Not an error condition. The receiver tolerates markets with zero events. The health monitor tracks connector state per market and reports it.

### 6.4 Corrupted JSON Line

**Contract:** The receiver does not validate JSON content at write time. Lines are written as JSON.stringify output from connectors. A corrupted line in a finalized file indicates either:
- A connector bug (producing non-JSON output)
- File-system corruption (post-write)
- An incomplete write during crash

**Recovery:** The receiver's startup recovery does not validate JSON content of existing files. Recovery is structural (watermark calculation, file deduplication) not content-level.

### 6.5 Connector Error States

| State | Meaning | Recovery Action |
|-------|---------|----------------|
| `init` | Initial | Awaiting connect() |
| `connecting` | WS connect in progress | — |
| `connected` | WS open, not yet synced | Awaiting snapshot sync |
| `syncing` | Snapshot sync in progress | — |
| `running` | Normal operation | — |
| `reconnecting` | Connection lost | Exponential backoff reconnect |
| `error` | Max retries exceeded | 60s cooldown, then retry (reduce count by 10) |

### 6.6 Stale Detection Cascade

```
No message for 30s → close socket → schedule reconnect → state = 'reconnecting'
                                                                  ↓
                                                           Exponential backoff
                                                                  ↓
                                              Max 30 attempts → state = 'error'
                                                                      ↓
                                                             60s cooldown
                                                                  ↓
                                                         retry (attempts -= 10)
```

### 6.7 Sequence Gap Handling

- **Detection:** Subclass-specific sequence comparison (e.g., Binance: `lastUpdateId + 1` vs event `U`)
- **Action:** Close socket → reset book → reconnect → resync
- **Tolerant mode (not implemented in local):** server1's `--strictSeq=0` logs gaps but does not disconnect. Local always resyncs.

---

## 7. Liveness Contract

### 7.1 Health Output Contract

| Aspect | Value | Evidence |
|--------|-------|----------|
| Output path | `<outputBase>/health.jsonl` | `health-monitor.mjs:20-24` |
| Format | JSONL append (one JSON object per line) | `health-monitor.mjs:90` |
| Write interval | 1000ms (configurable) | `health-monitor.mjs:22` |
| Overall state | `normal` = all markets `running`; `warning` = any market not `running`; `critical` = any market in `error`/`reconnecting` | `health-monitor.mjs:72-78` |
| Per-market fields | state, connectedAt, lastDepthMsgAt, lastTradeMsgAt, depthMsgCount, tradeMsgCount, reconnectCount, resyncCount, lastSeq | `health-monitor.mjs:59-68` |

### 7.2 Thresholds

| Threshold | Value | Configurable? | Evidence |
|-----------|-------|--------------|----------|
| Stale detection | 30,000ms no message | No (hardcoded) | `base-connector.mjs:7` |
| Stale check interval | 5,000ms | No | `base-connector.mjs:486` |
| Worker stale check | 15,000ms | No | `orderflow-worker.mjs:211` |
| Worker health push | 2,000ms | No | `orderflow-worker.mjs:223` |
| Reconnect base delay | 1,000ms | No | `base-connector.mjs:4` |
| Reconnect max delay | 30,000ms | No | `base-connector.mjs:5` |
| Max reconnect attempts | 30 | No | `base-connector.mjs:6` |
| Error recovery wait | 60,000ms | No | `base-connector.mjs:399` |
| Startup timeout | 60,000ms | No | `orderflow_monitor.mjs:218` |
| Snapshot sync timeout | 15,000ms | No | `base-connector.mjs:262` |

### 7.3 Server1 Comparison

| Metric | Local v3 | Server1 | Delta | Recommendation |
|--------|----------|---------|-------|---------------|
| Stale detection | 30s (hardcoded) | 5s/7s (configurable) | Server1 detects faster | **RETAIN** as configurable option |
| Reconnect | Exponential 1-30s | Fixed 1s | Local avoids herd | Keep local |
| Max reconnect | 30 + cooldown | Unlimited (rate-limited) | Local bounded | Keep local |
| Sequence gap | Always resync | Tolerant by default | Server1 less disruptive | **RETAIN** as configurable mode |
| Health format | JSONL append | Single-file overwrite | Local more robust | Keep local |

---

## 8. Recovery Contract

### 8.1 Startup Recovery

**Trigger:** Every `RawRotationWriter.startupRecovery()` call, invoked for every market/kind pair before any `connect()`.

**Evidence:** `raw-rotation-writer.mjs:603-760`, `orderflow-worker.mjs:171-181`

**Algorithm:**

1. **Scan** all `.jsonl` files → compute `finalizedWatermarkMs` (highest window start)
2. **Group** all `.jsonl.open` files by window start
3. **Deduplicate** per window (keep one, quarantine extras)
4. **Filter stale** `.open` files behind watermark → quarantine
5. **Filter future** `.open` files ahead of wall clock + 30s → quarantine with `.future` suffix
6. **Recovery-finalize** `.open` files older than current/previous window → no-clobber rename to `.jsonl`
7. **Keep** `.open` files for current window (wall clock window) and previous window (wall - 30s) as active writers

### 8.2 Crash Recovery Guarantee

| Scenario | Data Loss | Mechanism |
|----------|-----------|-----------|
| Crash mid-write (open file) | ≤1 window (30s max) | Startup recovery finalizes .open → .jsonl |
| Crash mid-finalize (link/unlink) | Zero (atomic) | No-clobber rename (link + unlink) is atomic on same filesystem |
| Crash mid-finalize (cross-device) | Zero (atomic) | copyFile with COPYFILE_EXCL + unlink |
| Crash during recovery quarantine | Zero | Quarantine destination never overwrites existing files |
| Filesystem full | Max 1 window | write() fails gracefully (logged, not thrown) |

### 8.3 Shutdown Recovery

**Graceful shutdown:** `finalize()` on all writers → flush + close + rename .open → .jsonl → watermark advancement.
**Evidence:** `raw-rotation-writer.mjs:499-509`, `orderflow-worker.mjs:232-252`

**Shutdown timeout:** 10s per worker (orderflow_monitor.mjs:267), after which the worker is abandoned.

### 8.4 Quarantine System

| Trigger | Quarantine Path | Suffix |
|---------|----------------|--------|
| EEXIST during finalize | `_quarantine/<market>/<kind>/<YYYY-MM-DD>/` | `.conflict` |
| Duplicate .open during recovery | Same | `.conflict` |
| Stale .open (behind watermark) | Same | `.conflict` |
| Future .open (>wall+30s) | Same | `.future` |
| Extra .open (>1 per window) | Same | `.conflict.N` |

**Quarantine guarantee:** Never overwrites existing files (no-clobber link + unlink with up to 100 suffix attempts).

### 8.5 Server1 Comparison: Recovery

| Aspect | Local v3 | Server1 | Impact |
|--------|----------|---------|--------|
| Startup recovery | Full scan + dedup + finalize + quarantine | None | Local survives crashes; server1 loses data |
| Atomic writes | Yes (link+unlink) | No (append-only) | Local has file-level integrity |
| Corruption handling | Quarantine + watermark | None | Local tolerates partial writes |
| Shutdown flush | Graceful with timeout | Basic flush + close | Local more robust |

**Decision:** **ADOPT** local's recovery system entirely. Server1's no-recovery design is excluded.

---

## 9. Retention Contract

### 9.1 Current State: No Retention in Receiver

**Contract:** The receiver currently does **NOT** implement retention. Output files accumulate indefinitely.

**Evidence:**
- `raw-rotation-writer.mjs` has no trim/rotation/eviction logic
- `config.v3.json` has no retention configuration
- P5 retention is planned but not implemented (confirmed in C0-tree-runtime-recon.md)

**Risk:** Total dataset grows without bound. On 15 markets × 3 data kinds, with average 100KB/30s per market-kind, this is approximately 15 × 3 × 100KB × 2880 windows/day = ~12.4 GB/day. Over one week: ~87 GB.

### 9.2 Server1 Retention Pattern (Retained as Reference)

| Stream | Default Cap | Trim Target |
|--------|-------------|-------------|
| Out | 256 MB | 85% |
| Book | 512 MB | 85% |
| BookRaw | 1536 MB | 85% |
| BookBucket | 2048 MB | 85% |
| Trade | 1536 MB | 85% |
| TradeCompact | 512 MB | 85% |

**Mechanism:** Tail-trim when file exceeds cap, check every 120s. Configurable via CLI args.

### 9.3 Retention Recommendation

The server1 per-stream cap pattern is **RETAINED** as a design reference for P5 implementation. The exact thresholds should be determined by:

1. **Consumption latency requirement:** How far back do downstream consumers need to query?
2. **Storage budget:** What is the allocated disk space?
3. **Recovery SLA:** Raw data may be needed for backfill — retention must cover the backfill window.

**Minimum recommendation:** Implement disk-full guard (emergency trim when disk < 5% free) BEFORE implementing any per-stream cap.

---

## 10. Rollback Contract

### 10.1 File-Level Rollback

The receiver's file system design supports rollback at the **file level**:

- Data files are immutable after finalization (no-clobber rename guarantees no overwrite)
- File names encode the UTC window start as `HH-MM-SS` in a `YYYY-MM-DD` directory
- Quarantine preserves conflicting original files

**Rollback mechanism:** Replace a `.jsonl` file with an earlier version. Because files are named by time window, no re-indexing is needed — the window name uniquely identifies its position.

### 10.2 Data Rollback

**Not supported** at the receiver level. The receiver is a write-only system. Rollback of received data would require:

1. Replaying from exchange WebSocket (time-bound — exchanges only serve recent data)
2. Restoring from backup (cold storage of raw JSONL)
3. Re-running any downstream pipeline against restored raw data

### 10.3 Configuration Rollback

Config changes (`config.v3.json` changes) can be rolled back via git:

```bash
git checkout <previous-sha> -- config.v3.json
```

This is a process-level rollback, not code-level. The receiver must be restarted for config changes to take effect.

### 10.4 Schema Rollback

Schema changes require code-level rollback of the connector files and/or the rotation writer. Git revert is the mechanism.

### 10.5 No Compatibility Guarantee

**Contract:** There is NO backward-compatibility guarantee for file formats across versions. A future version of the receiver may produce differently-shaped JSONL. Consumers should:

1. Validate against schema (§5) on every read
2. Not assume field ordering
3. Handle unknown fields gracefully

---

## 11. Explicit Exclusions

The following are **explicitly NOT part of the receiver contract** and are excluded from this specification:

| # | Exclusion | Rationale | Belongs To |
|---|-----------|-----------|------------|
| 1 | Burst feature computation (offline pipeline) | Separated concern | `burst-reducer/pipeline.mjs` |
| 2 | 1s/30s/5min feature rollups | Derived output, not raw | `burst-reducer/rollup*.mjs` |
| 3 | Consumer contracts for derived data | Downstream contract | `consumer-5min.mjs` (recon only) |
| 4 | Gateway / API / Dashboard | Not yet designed | Separate project |
| 5 | Market symbol discovery / management | Static config only | `config.v3.json` |
| 6 | Data deduplication | Preserved as received | Consumer responsibility |
| 7 | Data re-ordering | Timestamp order not guaranteed | Consumer responsibility |
| 8 | Parquet conversion | DuckDB conversion done by `tfp.mjs` | `scripts/tfp.mjs` |
| 9 | REST auxiliary data (mark price, OI, funding, OHLCV) | Separate process | `aux_data_collector.mjs` |
| 10 | Fair price computation | Separate component | `FairPriceCollector` |
| 11 | Book snapshot persistence (full book dumps) | Not rotation-writer managed | `FairPriceCollector` |
| 12 | Data encryption at rest | Not implemented | Future concern |
| 13 | Authentication / authorization | No access control on files | Future concern |
| 14 | Data compression | Raw JSONL only | Future concern |
| 15 | Multi-datacenter replication | Single-host deployment | Future concern |
| 16 | Message queue / bus integration | File-based only | Future concern |
| 17 | Backfill from exchange history | Not implemented | Future concern |
| 18 | Monitoring / alerting (beyond health.jsonl) | No PagerDuty/Prometheus integration | Future concern |
| 19 | Rate limiting beyond WebSocket reconnect | Exchange-level, not receiver-level | Connector concern |
| 20 | Order management / trading | Read-only receiver | Out of scope entirely |

---

## 12. Local vs Server1 Evidence Comparison

### 12.1 Confidence Assessment

| Viewpoint | Local v3 Confidence | Server1 Confidence | Basis |
|-----------|-------------------|-------------------|-------|
| Behavior | **High** — full source read (296+280+842 lines) | **Medium** — vendor copy, not live server1 | Server1 not SSH/HTTP accessible |
| Schema | **High** — full connector emit signatures confirmed | **Medium** — inferred from vendor copy output patterns | All local connectors read |
| Liveness | **High** — thresholds confirmed in source | **Medium** — from CLI args in vendor copy | Configurable thresholds documented |
| Recovery | **High** — full recovery code read (raw-rotation-writer.mjs) | **Low** — no recovery in vendor copy | Clear architectural difference |
| Retention | **High** — confirmed absent | **High** — caps confirmed in vendor copy CLI args | Both codebases fully read |
| Config | **High** — full config.v3.json read | **High** — CLI args from vendor copy | Both codebases fully read |

### 12.2 Evidence Unavailable

| Question | Status |
|----------|--------|
| Is server1 currently running? | Unknown — SSH access required |
| What version is server1 on? | Unknown — vendor copy may be stale |
| Is server1 being actively replaced by local v3? | **Resolved (B3):** Parallel operation — no replacement/cutover. agg and server1 run independently with isolated output roots |
| What is the heatmap project's relationship to server1? | Unknown — labeled "candidate for safe migration" |

### 12.3 Key Architectural Deltas

```
                    server1                           local v3
              ┌──────────────────┐           ┌──────────────────────┐
              │ Single-thread    │           │ Multi-worker (4 thr) │
              │ 1 market         │           │ 15 markets           │
              │ Flat append      │           │ 30s rotation + atomic│
              │ Inline features  │           │ Offline pipeline     │
              │ No recovery      │           │ Full recovery        │
              │ CLI args         │           │ JSON config          │
              │ Fixed 1s reconnect           │ Exponential backoff  │
              └──────────────────┘           └──────────────────────┘
```

---

## 13. Six-View Risk Analysis

### 13.1 Forward View

**Risks:**

| Risk | Likelihood | Severity | Mitigation |
|------|-----------|----------|------------|
| Consumer contracts undefined — downstream projects may build on wrong assumptions | Medium | High | This contract defines the receiver boundary; consumers must be separately specified |
| P4 book activation incomplete (fields #13-#22 placeholders) | Low | Medium | P4 commit exists (`630198f`); fields use real values now |
| P5 retention not built — disk exhaustion risk | Medium | High | Implement disk-full guard before per-stream caps |
| No Gateway integration started | Medium | Medium | Not a receiver concern; separate planning needed |
| fairprice_monitor→orderflow_monitor cutover may miss undocumented consumers | Medium | Medium | B1 decision: require consumer audit before cutover |

### 13.2 Recovery View

**Risks:**

| Risk | Likelihood | Severity | Mitigation |
|------|-----------|----------|------------|
| fairprice_monitor liquidation path has no atomic commit | Medium | High | Migrate to RawRotationWriter for consistency |
| Startup recovery only covers orderflow_monitor; fairprice_monitor has none | Medium | High | Implement startupRecovery for fairprice_monitor or converge to single receiver |
| .open files orphaned if process is killed before stale check | Low | Low | Startup recovery handles this on next start |
| Quarantine directory grows unbounded | Medium | Low | Add periodic quarantine cleanup as part of P5 |
| Cross-device (EXDEV) fallback uses copy — race window on partial copy | Low | Medium | COPYFILE_EXCL mitigates; still a risk on power loss mid-copy |

### 13.3 Adversarial View

**Risks:**

| Risk | Likelihood | Severity | Mitigation |
|------|-----------|----------|------------|
| Stale threshold (30s) is too long for some use cases | Medium | Medium | Configurable threshold per server1 pattern (RETAIN); C6a extracted `_checkStale()` as a named method enabling test-seam injection for future configurability |
| Reconnect storm on network outage (all markets reconnect simultaneously) | Medium | Medium | Worker staggering (50ms) and exponential backoff mitigate; server1 fixed 1s would be worse |
| Sequence gap causes full resync each time | Medium | Low | Local always-resync is safe but expensive; server1 tolerant mode reduces noise |
| Ring buffer overflow (65536 events during long sync) | Low | Medium | Buffer trades to disk or increase buffer size for slow exchanges |
| Unauthorized data access on shared filesystem | Low | High | Not a receiver responsibility; OS-level permissions |
| Disk full during write() | Low | High | Emergency trim or graceful degradation needed |
|| **Unknown market on CLI (B6, FIX2/C2 G1):** Passing a market name not in `config.v3.json` triggers fail-closed at the entry point: `orderflow_monitor.mjs` exits with rc=1 before spawning workers. At the worker level, if an unknown market somehow reaches `prepareMarket()`, a `startupFailed` IPC is sent and no connector is created — defense-in-depth. See §16 B6 for the full contract. | Low | Medium | Documented in B6; intentional safety measure; CLI typos are caught at startup. |

### 13.4 Consumer View

**Risks:**

| Risk | Likelihood | Severity | Mitigation |
|------|-----------|----------|------------|
| Downstream consumers may read .open files before finalization | Medium | Medium | Consumers MUST read only .jsonl (not .open) files |
| No schema enforcement means consumers must handle malformed lines | Low | Low | C6a added connector-level schema validation — `_emitDepth` rejects invalid type/arrays, `_emitLiquidation` rejects invalid side/price/qty. Malformed events are dropped at source. Downstream receivers see only validly-shaped events (but content-level validation remains a consumer concern per receiver contract §2.1). |
| File naming convention (HH-MM-SS.jsonl) embeds UTC window start — timezone confusion | Low | Medium | All timestamps are UTC; convention must be documented |
| 30s windows are not aligned across markets | Low | Low | Each market's windows are independent; consumers must handle skew |

### 13.5 State Isolation View

**Risks:**

| Risk | Likelihood | Severity | Mitigation |
|------|-----------|----------|------------|
| Worker isolation broken by shared mutable state | Low | High | Design is clean (no shared state between workers); IPC is main→worker only |
| No namespace isolation between data kinds at consumer level | Low | Low | Directory structure separates kinds; consumers see only their kind |
| Market isolation: a failing market can block worker startup (fail-closed) | Low | High | Current design: one market failure → entire worker exits → entire system shuts down (orderflow_monitor.mjs:229-238). This is a **critical design issue** for 15-market robustness. |
| Run isolation: no multi-instance safe guards | Medium | Medium | Two concurrent instances of the same receiver writing to the same output dir would cause EEXIST conflicts |

### 13.6 Performance View

**Risks:**

| Risk | Likelihood | Severity | Mitigation |
|------|-----------|----------|------------|
| Worker staggering (50ms) may not be enough for 18-market startup | Low | Low | Currently 4 workers only; stagger is adequate |
| Health push (2s) + stale check (15s) + write flush (200ms-1s) timer overhead at scale | Low | Low | All timers are lightweight (check intervals, not loops) |
| BufferedWriter maxBufferLines=4096 per writer — 45 writers × 4096 = 184K lines in memory | Medium | Low | Each line is small (trade: ~100 bytes); total ≈ 18 MB |
| In-memory book state per market (ring buffer 64K entries) — memory grows with depth | Medium | Low | Binance perp: 1000 level snapshots; ring buffer holds parsed updates only |
| No batch I/O — every write is a system call within the buffered writer | Low | Low | BufferedWriter batches by flush interval (200ms-1s) |

---

## 14. Implementation Dependency Graph

```
Layer 0 — Primitives (no dependencies within project)
├── normalizeTimestampMs(ts)               — raw-rotation-writer.mjs
├── windowStartMs(tsMs)                    — raw-rotation-writer.mjs
├── windowStartToDateStr(windowMs)         — raw-rotation-writer.mjs
├── noClobberRename(src, dest)            — raw-rotation-writer.mjs
├── noClobberQuarantine(filePath, dir)    — raw-rotation-writer.mjs
└── BufferedWriter                        — buffered-writer.mjs

Layer 1 — State Machine
└── BaseConnector                          — base-connector.mjs
    ├── WS connect / reconnect / stale detection
    ├── Snapshot sync / ring buffer
    ├── Sequence gap handling
    └── Stats tracking

Layer 2 — Rotation Writer
└── RawRotationWriter                      — raw-rotation-writer.mjs
    ├── Per-market/kind writer lifecycle
    ├── Window rotation (current + previous)
    ├── Finalize / watermark
    ├── Stale check
    └── Startup recovery
        ├── _scanFiles
        ├── _extractWindowMs
        ├── _createWriter
        └── _finalizeWriter

Layer 3 — Connectors (extend BaseConnector)
├── BinanceSpotConnector                  — binance-connector.mjs
├── BinancePerpConnector                  — binance-connector.mjs
├── BinanceSpotUsdcConnector              — binance-usdc-connector.mjs
├── BybitConnector                        — bybit-connector.mjs
├── BybitSpotConnector                    — market-connectors.mjs
├── OkxConnector                          — okx-connector.mjs
├── OkxSpotConnector                      — market-connectors.mjs
├── CoinbaseConnector                     — coinbase-connector.mjs
├── CoinbaseInternationalConnector        — coinbase-international-connector.mjs
├── KrakenSpotConnectorAlias              — market-connectors.mjs
├── BitstampConnector                     — bitstamp-connector.mjs
├── CryptoComConnector                    — crypto-com-connector.mjs
├── BitfinexConnector                     — bitfinex-connector.mjs
├── GeminiConnector                       — gemini-connector.mjs
├── BitmexConnector                       — bitmex-connector.mjs
└── HyperliquidConnector                  — hyperliquid-connector.mjs

Layer 4a — Worker (orderflow path)
├── orderflow-worker.mjs                  — Worker Thread
│   ├── prepareMarket -> creates Layer 2 + Layer 3 per market
│   ├── startupRecovery (all Layer 2)
│   ├── connectMarket (all Layer 3)
│   └── IPC to main (stats, stateChange, liquidation)
└── orderflow_monitor.mjs                 — Main Thread (orchestrator)
    ├── Worker creation + IPC
    ├── HealthMonitor (Layer 1-ish)
    └── Graceful shutdown

Layer 4b — Fair Price Receiver (fairprice path)
└── fairprice_monitor.mjs                 — Single-thread
    ├── Direct connector instantiation (Layer 3)
    ├── FairPriceCollector (not receiver)
    ├── Liquidation writer (BufferedWriter, not RawRotationWriter)
    └── No startup recovery

Layer 5 — Health
└── HealthMonitor                         — health-monitor.mjs
    ├── BufferedWriter output
    ├── 1s periodic tick
    └── Per-market stats aggregation

External Dependencies (runtime)
├── ws (npm) — WebSocket implementation
└── duckdb (npm) — Parquet (pipeline-side, not receiver)
```

**Critical dependency path for new development:**

1. BufferedWriter (tested, stable)
2. RawRotationWriter primitives (timestamp, window, rename) (tested, stable)
3. RawRotationWriter lifecycle (write, finalize, rotate) (tested, stable)
4. RawRotationWriter.startupRecovery (tested, stable)
5. BaseConnector (tested, stable)
6. Individual connectors (tested, stable)
7. Worker thread orchestration (tested, stable)
8. HealthMonitor (tested, stable)

**Known dependency issue:** Layer 4b (fairprice_monitor) bypasses Layer 2 (RawRotationWriter) for liquidations, using raw BufferedWriter instead. This creates an inconsistency where fairprice_monitor's liquidation path has weaker crash-safety guarantees than orderflow_monitor's.

---

## 15. 95-Point Review Rubric

### 15.1 Raw Boundary (10 points)

| # | Check | Pass/Fail | Evidence |
|---|-------|-----------|----------|
| 1 | Receiver does not compute features | ✓ | orderflow_monitor.mjs:8, orderflow-worker.mjs:22 |
| 2 | Receiver does not aggregate events | ✓ | No aggregation logic in worker |
| 3 | Receiver does not re-order events | ✓ | FIFO write via promise queue |
| 4 | Receiver does not deduplicate | ✓ | No dedup logic in receiver path |
| 5 | Receiver does not enrich events | ✓ | No enrichment in connector emits |
| 6 | Only timestamp normalization is applied | ✓ | raw-rotation-writer.mjs:45-71 |
| 7 | Three data kinds (trades, book_updates, liquidations) | ✓ | orderflow-worker.mjs:78-86 |
| 8 | Each data kind has its own rotation writer | ✓ | Three Maps in worker |
| 9 | Output path follows `<base>/<kind>/<market>/<date>/<window>.jsonl` | ✓ | raw-rotation-writer.mjs:773-774 |
| 10 | No receiver writes to derived/ tier | ✓ | Only pipeline writes to derived/ |

### 15.2 Auxiliary Boundaries (5 points)

| # | Check | Pass/Fail | Evidence |
|---|-------|-----------|----------|
| 11 | fairprice_monitor raw path is documented as legacy | ✓ | This contract §3.1 |
| 12 | fairprice_monitor liquidation has weaker crash safety | ✓ | Uses raw BufferedWriter, not rotation writer |
| 13 | aux_data_collector is separate process | ✓ | systemd service, no overlap |
| 14 | aux_data_collector output paths are distinct | ✓ | derivatives/, market_data/ dirs |
| 15 | orderflow_monitor vs fairprice_monitor relationship documented | ✓ | §3.3 |

### 15.3 Schemas (15 points)

| # | Check | Pass/Fail | Evidence |
|---|-------|-----------|----------|
| 16 | Trade schema documented | ✓ | §5.1 |
| 17 | Depth/Book update schema documented | ✓ | §5.2 |
| 18 | Liquidation schema documented | ✓ | §5.3 |
| 19 | Health schema documented | ✓ | §5.4 |
| 20 | Trade has market, price, qty, side, ts, tradeId | ✓ | base-connector.mjs:327-332 |
| 21 | Depth has market, type, bids, asks, ts, seq | ✓ | base-connector.mjs:319-323 |
| 22 | Liquidation has ts, market, exchange, symbol, side, price, qty, notional, raw_type, trade_id, source_ts | ✓ | base-connector.mjs:347-361 |
| 23 | Health has ts, state, markets map | ✓ | health-monitor.mjs:80-84 |
| 24 | Health markets entries have all required fields | ✓ | health-monitor.mjs:59-68 |
| 25 | All schemas use epoch ms (number) for timestamps | ✓ | normalizeTimestampMs output |
| 26 | Timestamp normalization handles s/ms/μs/ns | ✓ | raw-rotation-writer.mjs:45-71 |
| 27 | Timestamp normalization rejects invalid inputs | ✓ | Returns null for non-number/NaN/Infinity |
| 28 | Market field is top-level string identifier | ✓ | e.g., "binance_perp" |
| 29 | JSONL format: one JSON object per line | ✓ | BufferedWriter standard |
| 30 | No header/footer/metadata in data files | ✓ | Pure JSONL |

### 15.4 Quality Evidence (10 points)

| # | Check | Pass/Fail | Evidence |
|---|-------|-----------|----------|
| 31 | Stale message threshold documented (30s) | ✓ | §4.1 |
| 32 | Reconnect attempt limit documented (30) | ✓ | §4.1 |
| 33 | Reconnect backoff documented (1-30s exponential) | ✓ | §4.1 |
| 34 | Error recovery cooldown documented (60s) | ✓ | §4.1 |
| 35 | Flush intervals documented per data kind | ✓ | §4.1 |
| 36 | Health write interval documented (1s) | ✓ | §4.1 |
| 37 | Timestamp normalization bounds documented | ✓ | §4.2 |
| 38 | Window validity rules documented | ✓ | §4.3 |
| 39 | Ring buffer bounds documented (65536) | ✓ | §4.4 |
| 40 | Future event rejection documented | ✓ | §4.3 |

### 15.5 Empty/Missing/Error Semantics (10 points)

| # | Check | Pass/Fail | Evidence |
|---|-------|-----------|----------|
| 41 | Empty directory is valid | ✓ | §6.1 |
| 42 | Empty file handling documented | ✓ | §6.2 |
| 43 | Missing market is not an error | ✓ | §6.3 |
| 44 | Corrupted JSON handling documented | ✓ | §6.4 |
| 45 | Connector state machine documented (all states) | ✓ | §6.5 |
| 46 | Error state recovery documented (60s cooldown) | ✓ | §6.5 |
| 47 | Stale detection cascade documented | ✓ | §6.6 |
| 48 | Sequence gap handling documented | ✓ | §6.7 |
| 49 | Late event cutoff (behind watermark) documented | ✓ | §4.3 |
| 50 | Negative timestamp rejection documented | ✓ | §4.3 |

### 15.6 Liveness (10 points)

| # | Check | Pass/Fail | Evidence |
|---|-------|-----------|----------|
| 51 | Health output path documented | ✓ | §7.1 |
| 52 | Health format documented (JSONL append) | ✓ | §7.1 |
| 53 | Health state aggregation documented (normal/warning/critical) | ✓ | §7.1 |
| 54 | All liveness thresholds documented | ✓ | §7.2 |
| 55 | Server1 comparison documented | ✓ | §7.3 |
| 56 | Recommended configurable stale threshold | ✓ | §7.3 |
| 57 | Recommended tolerant sequence gap mode | ✓ | §7.3 |
| 58 | Worker health push (2s) documented | ✓ | §4.1 |
| 59 | Worker stale check (15s) documented | ✓ | §4.1 |
| 60 | Per-market state is visible in health | ✓ | health-monitor.mjs |

### 15.7 Recovery (10 points)

| # | Check | Pass/Fail | Evidence |
|---|-------|-----------|----------|
| 61 | Startup recovery algorithm documented | ✓ | §8.1 |
| 62 | Crash recovery guarantees documented per scenario | ✓ | §8.2 |
| 63 | No-clobber rename (link+unlink) documented | ✓ | raw-rotation-writer.mjs:132-174 |
| 64 | Cross-device fallback documented (copy+EXCL) | ✓ | raw-rotation-writer.mjs:154-170 |
| 65 | Quarantine system documented (paths, suffixes) | ✓ | §8.4 |
| 66 | Quarantine no-overwrite guarantee documented | ✓ | §8.4 |
| 67 | Shutdown recovery documented | ✓ | §8.3 |
| 68 | Shutdown timeout documented (10s) | ✓ | §8.3 |
| 69 | Server1 recovery comparison documented | ✓ | §8.5 |
| 70 | Watermark advancement rules documented | ✓ | §8.1 |

### 15.8 Retention (5 points)

| # | Check | Pass/Fail | Evidence |
|---|-------|-----------|----------|
| 71 | Current no-retention state documented | ✓ | §9.1 |
| 72 | Growth estimate documented (~12.4 GB/day) | ✓ | §9.1 |
| 73 | Server1 retention pattern documented | ✓ | §9.2 |
| 74 | Retention recommendation includes emergency disk guard | ✓ | §9.3 |
| 75 | Retention recommendation tied to consumption latency | ✓ | §9.3 |

### 15.9 Rollback (5 points)

| # | Check | Pass/Fail | Evidence |
|---|-------|-----------|----------|
| 76 | File-level rollback documented | ✓ | §10.1 |
| 77 | No data-content rollback documented | ✓ | §10.2 |
| 78 | Config rollback via git documented | ✓ | §10.3 |
| 79 | Schema rollback via git revert documented | ✓ | §10.4 |
| 80 | No backward-compatibility guarantee documented | ✓ | §10.5 |

### 15.10 Exclusions (5 points)

| # | Check | Pass/Fail | Evidence |
|---|-------|-----------|----------|
| 81 | Burst computation excluded | ✓ | §11 #1 |
| 82 | Derived output tiers excluded | ✓ | §11 #2 |
| 83 | Consumer contracts excluded | ✓ | §11 #3 |
| 84 | Gateway/API/Dashboard excluded | ✓ | §11 #4 |
| 85 | 20 total exclusions listed | ✓ | §11 |

### 15.11 Adoption Matrix (5 points)

| # | Check | Pass/Fail | Evidence |
|---|-------|-----------|----------|
| 86 | ADOPT items listed with rationale | ✓ | §1.4 |
| 87 | RETAIN items listed with rationale | ✓ | §1.4 |
| 88 | EXCLUDE items listed with rationale | ✓ | §1.4 |
| 89 | Each decision traces to evidence | ✓ | Source/line refs |
| 90 | Local vs server1 deltas in adoption | ✓ | §1.4 |

### 15.12 Risk and Dependency (5 points)

| # | Check | Pass/Fail | Evidence |
|---|-------|-----------|----------|
| 91 | Six-view risk analysis complete | ✓ | §13 |
| 92 | Each risk has likelihood, severity, mitigation | ✓ | §13 |
| 93 | Implementation dependency graph complete | ✓ | §14 |
| 94 | Critical dependency path identified | ✓ | §14 |
| 95 | Known dependency issue documented (Layer 4b inconsistency) | ✓ | §14 |

---

## 16. Decision Record — Resolved Blockers

The following five blockers (B1-B5) were surfaced during C0 reconnaissance and **resolved via human decision** during the C1 gate. Each decision is incorporated into the adoption matrix (§1.4), auxiliary boundaries (§3), and risk analysis (§13) above.

### B1: Two Receiver Divergence

**Status:** RESOLVED

**Decision:** `orderflow_monitor.mjs` is the canonical agg Receiver. `fairprice_monitor.mjs` is legacy/reference and is NOT the production entrypoint.

**Details:**
- Production receiver: `orderflow_monitor.mjs` (multi-worker, raw-only, rotation-writer-based)
- fairprice_monitor.mjs: preserved temporarily for migration/reference; no deletion or cutover now
- Deployment (start.sh) update deferred to future cutover task

**Incorporated in:** §1.4 (ADOPT: canonical orderflow_monitor, RETAIN: legacy fairprice_monitor), §3.3 (relationship matrix)

### B2: Cross-Market Fail-Closed Design

**Status:** RESOLVED

**Decision:** Market-level graceful degradation. Successful markets continue; failed markets are reported explicitly in the health output.

**Details:**
- A failing market produces a `critical` health entry for that market only
- Other markets in the same worker continue operating independently
- The receiver does not exit on single-market failure (future implementation change)

**Incorporated in:** §1.4 (ADOPT row), §7.1 (health state: normal/warning/critical per market)

### B3: Server1 Migration Intent

**Status:** RESOLVED

**Decision:** server1 and agg run in parallel. No server1 modification or replacement assumption. agg (local v3) is an independent system, not a server1 replacement.

**Details:**
- No migration/cutover from server1 to agg; both are independent systems
- server1 writes to its own isolated output root (`data/receiver/server1/`) per B4
- agg writes exclusively to `data/receiver/agg/`
- No code or service changes on server1

**Incorporated in:** §1.4 (ADOPT: parallel operation), §2.3 (directory layout), §12.2 (resolved Q)

### B4: Pipeline Raw Data Source

**Status:** RESOLVED

**Decision:** Producer-separated roots: `data/receiver/server1/` and `data/receiver/agg/`, with agg raw/aux/health separated. Downstream selects producer by explicit manifest.

**Details:**
- agg receiver path: `data/receiver/agg/raw/{trades,book_updates,liquidations}/<market>/<date>/<window>.jsonl`
- agg health: `data/receiver/agg/health/health.jsonl`
- agg aux: `data/receiver/agg/aux/`
- server1 root: `data/receiver/server1/` (reserved)
- Downstream selects input producer via explicit manifest, not ambiguous symlinks
- Current paths (`data/live_v3/`, `data/raw_hot/`) accepted as migration inputs until cutover

**Incorporated in:** §1.4 (ADOPT: producer-separated roots), §2.3 (canonical directory layout)

### B5: Disabled Markets Long-Term Plan (refined by B6)

**Status:** RESOLVED — C6 series verified and closed

**Decision:** The three disabled markets remain disabled with specific enablement criteria. No removal or code cleanup at this time. **B6 clarification (intentional CLI override):** `binance_coinm_perp` has `enabled:false`, meaning it is excluded from all normal (default) startup. Passing `--markets binance_coinm_perp` explicitly on the CLI bypasses the `enabled` flag as an **intentional isolated smoke testing override only**. This is NOT production enable/cutover — the override exists only for manual smoke/candidate verification. Any future production enablement requires toggling `enabled:true` in `config.v3.json` and a separate cutover process.

**C6 series verification:** The `test/receiver-config-validator.test.mjs` policy test (12 assertions) confirms 15 markets enabled, 3 markets disabled by name matching B5 decision. Config unchanged (`config.v3.json` diff=0). CLI override behavior is documented in test comments.

**Details:**
- `binance_coinm_perp`: enablement candidate pending isolated smoke testing (via `--markets binance_coinm_perp` CLI override); no regression risk while disabled
- `coinbase_international_perp`: remains disabled pending auth/secret injection; requires human ops for credential provisioning
- `gemini_spot`: remains disabled; no active enablement plan
- No timeline for any enablement; maintainers toggle enabled status through `config.v3.json` `"enabled": false`
- `--markets` CLI argument is the ONLY mechanism to override `enabled:false` for smoke testing. Default startup (no `--markets`) always filters by `enabled===true`.
|- **Unknown market on CLI (C2 G1 FIX2):** A market name not present in `config.v3.json` triggers fail-closed at the entry point: `orderflow_monitor.mjs` exits with rc=1 before spawning workers. At the worker level, if an unknown market reaches `prepareMarket()`, a `startupFailed` IPC is sent and no connector is created — defense-in-depth. The `receiver-integration-smoke.test.mjs` G1-1/G1-2 tests verify CLI fail-closed via subprocess assertion; G1-3 verifies worker `startupFailed` IPC. See §16 B6 for the full contract.

**Incorporated in:** §1.4 (RETAIN: disabled markets status quo), §11 (exclusions — markets not available), §13.3 (unknown market risk), §13.4 (schema validation risk update)

### B6: Schema Validation and CLI Override Clarification (C6 series)

**Status:** RESOLVED — integrated into C6a (schema validation) and C6b (canonical worker test seams)

**Decision:** Two specific issues clarified during C6 series implementation:

1. **Schema malformed payload → fail closed; valid values preserved:** C6a added input validation to `_emitDepth()` (type ∈ {partial,delta,snapshot,update}, bids/asks must be arrays) and `_emitLiquidation()` (side ∈ {buy,sell}, price/qty positive finite numbers). Invalid payloads are silently dropped with console.error. Valid values pass through unchanged. This resolves the §13.4 risk "no schema enforcement" — the risk likelihood is reduced from High to Low.

2. **CLI override scope and unknown market fail-closed (C2 G1 FIX2):** Explicit `--markets binance_coinm_perp` is the ONLY intentional path to start a disabled market (smoke override). Default startup (no `--markets`) filters by `enabled===true`. Config.v3.json remains the single source of truth for production enablement. **Unknown CLI market names** (not present in `config.v3.json`) trigger fail-closed at the entry point: `orderflow_monitor.mjs` exits with rc=1 before spawning workers (§13.3). At the worker level, if an unknown market reaches `prepareMarket()`, a `startupFailed` IPC is sent and no connector is created — defense-in-depth. See `test/receiver-integration-smoke.test.mjs` G1-1/G1-2 (subprocess CLI fail-closed) and G1-3 (worker startupFailed IPC).

**Evidence:**
| Aspect | File | Lines |
|--------|------|-------|
| Depth schema validation | `lib/base-connector.mjs` | `_emitDepth` type/array guards |
| Liquidation schema validation | `lib/base-connector.mjs` | `_emitLiquidation` side/price/qty guards |
| _checkStale method extraction | `lib/base-connector.mjs` | Named method for testability |
| Unknown market handling (CLI entry point) | `orderflow_monitor.mjs` | `--markets` validation → exit(1) for unknown names |
| Unknown market handling (worker) | `lib/orderflow-worker.mjs` | `prepareMarket()` sends `startupFailed` IPC |
| Unknown market tests | `test/receiver-integration-smoke.test.mjs` | 5 cases (G1-1–G1-5): subprocess + worker seam |
| Config policy test | `test/receiver-config-validator.test.mjs` | 12 assertions, all pass |
| Worker test seams | `lib/orderflow-worker.mjs` | `_testInit`, `_testPrepareMarket` exports |

**Status:** ALL BLOCKERS RESOLVED (B1-B6). C6 series complete. Contract reconciled with implementation.

---

## 17. Evidence Paths

| Evidence | Path | Lines | Status |
|----------|------|-------|--------|
| Orchestrator entry | `orderflow_monitor.mjs` | 296 | Full read |
| Worker thread | `lib/orderflow-worker.mjs` | 280 | Full read |
| Rotation writer | `lib/raw-rotation-writer.mjs` | 842 | Full read |
| Base connector | `lib/base-connector.mjs` | 538 | Full read |
| Health monitor | `lib/health-monitor.mjs` | 92 | Full read |
| Configuration | `config.v3.json` | 196 | Full read |
| Fair price monitor | `fairprice_monitor.mjs` | 201 | Full read |
| Burst schema | `lib/burst-reducer/schema.mjs` | 163 | Full read |
| 5min consumer | `lib/burst-reducer/consumer-5min.mjs` | 229 | Full read |
| Server1 vendor copy | `../agg-btc-orderheatmap/vendor/orderflow_pack/orderflow_monitor.mjs` | 741 | Full read |
| C0 tree recon | `docs/recon/C0-tree-runtime-recon.md` | 248 | Full read |
| C0 server1 comparison | `docs/recon/C0-server1-comparison.md` | 384 | Full read |
| Server1 SSH access | `192.168.0.219:22,8022` | — | All failed |

---

【C6d closure】C6系列（a: スキーマ検証, b: Worker test seam, c: EXDEV fallback, d: 本クロージャ）完了。
B1-B6全blocker解決。契約書と実装を統合。ポリシーテスト12/12通過、全体764/764通過。
新機能追加なし。設定・サービス・データ未変更。ロールバック方法: git checkout v2@{2026-07-15} でC6 series全体をrevert可能。

*End of C1 Receiver-Only Spec and Adoption Matrix. B1-B5 blockers resolved; contract gate complete. C6d closure: B6 added, schema validation reconciled, full test tree green. See §16 for full decision record.*
