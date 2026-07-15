# C0 Reconnaissance: agg-btc-receiver Current Tree and Runtime

**Date:** 2026-07-14
**Task:** t_55c23a38
**Branch:** v2 (HEAD: `32389c9`)
**Git status:** clean, no uncommitted changes
**No services running** (fairprice_monitor / orderflow_monitor / aux_data_collector all stopped)

---

## 1. Project Overview

agg-btc-receiver v3.0.0 is a BTC multi-exchange orderbook and trade receiver with downstream burst feature computation.

**Core purpose:** Receive real-time BTC trade/depth/liquidation data from 18 exchanges via WebSocket, save raw JSONL, compute burst features at 1s/30s/5min granularities, and persist to a derived output tier.

**Dependencies:** Only `ws` (WebSocket) and `duckdb` (for Parquet conversion).

---

## 2. Enabled Markets (15 active, 3 disabled)

**Active (15):**
- binance_spot, binance_perp, binance_perp_btcusdc, binance_spot_usdc
- bybit_perp, bybit_spot
- okx_perp, okx_spot
- coinbase_spot, kraken_spot, bitstamp_spot
- crypto_com_spot, bitfinex_spot, bitmex_perp, hyperliquid_perp

**Disabled (3):** binance_coinm_perp, gemini_spot, coinbase_international_perp

---

## 3. Architecture — Three Independent Processes

### 3.1 orderflow_monitor.mjs (Multi-worker raw receiver)
- **Type:** Main thread + 4 worker threads (A/B/C/D market groups)
- **Worker:** lib/orderflow-worker.mjs
- **Output:** `data/live_v3/{trades,book_updates,liquidations}/<market>/<date>/<HH-MM-SS>.jsonl`
- **Role:** Pure receive + save. No feature computation.
- **Key detail:** Raw rotation writer (30s window rotation), startup recovery, stale detection
- **NOT started by scripts/start.sh** (start.sh runs fairprice_monitor instead)

### 3.2 fairprice_monitor.mjs (Single-thread fair price receiver)
- **Type:** Single-thread, all markets in one process
- **Output:** `data/raw_hot/` (default output base)
- **Includes:** FairPriceCollector, liquidation writers, book snapshots
- **Role:** Receives trades + book, writes liquidations, produces book snapshots for downstream
- **Started by:** `scripts/start.sh` (screen session `agg-btc-receiver`, auto-restart loop)
- **THIS IS THE PRODUCTION ENTRYPOINT** (start.sh runs this, not orderflow_monitor)

### 3.3 aux_data_collector.mjs (REST auxiliary data)
- **Type:** Single-thread REST poller
- **Output:** DerivativesHelper (mark price, funding, OI) + MarketDataCollector (OHLCV, ticker, LS ratio, taker vol)
- **Deployment:** systemd service (`agg-btc-aux-collector.service`)
- **Heartbeat:** writes `data/.../health/aux_collector.json`

### 3.4 Pipeline (burst-reducer, offline)
- **Entry:** `lib/burst-reducer/pipeline.mjs` (called via scripts/tfp.mjs or similar)
- **Role:** Reads raw 30s blocks → computes 1s features → 30s rollup → 5min rollup
- **Output:** `data/derived/burst_features_v1/{features_1s,features_30s,features_5min}/<market>/<date>/<HH-MM-SS>.jsonl`

---

## 4. Source Code Map

### Top-level entry points
- `orderflow_monitor.mjs` — 296 lines, multi-worker raw receiver
- `fairprice_monitor.mjs` — 201 lines, single-thread fair price receiver
- `aux_data_collector.mjs` — 189 lines, REST auxiliary collector
- `dashboard.mjs` — (exists, not inspected in detail)

### lib/ (11,293 lines total in .mjs)
| File | Lines | Role |
|------|-------|------|
| raw-rotation-writer.mjs | 842 | 30s window rotation, timestamp normalization, startup recovery |
| market-data-collector.mjs | 810 | REST market data (OHLCV, ticker, LS ratio, taker vol) |
| base-connector.mjs | 538 | WS reconnect state machine (30 attempts, exponential backoff) |
| binance-connector.mjs | 359 | Binance spot + perp connectors |
| book-state-machine.mjs | 358 | Pure book state machine (snapshot + diff apply) |
| full-book.mjs | 318 | Full orderbook management |
| orderflow-worker.mjs | 280 | Worker thread for multi-worker orchestrator |
| fair-price-collector.mjs | 279 | Fair price computation and periodic snapshot |
| derivatives-helper.mjs | 269 | Perp derivatives data (mark price, funding, OI) |
| burst-builder.mjs | 258 | Burst detection from raw trades |
| buffered-writer.mjs | ~200 | Buffered JSONL writer with flush interval |

### lib/burst-reducer/ (feature computation pipeline)
| File | Lines | Role |
|------|-------|------|
| pipeline.mjs | 1028 | Main pipeline: scan → validate → compute → commit (1-block lag) |
| rollup-5min-committer.mjs | 491 | 5min persistence (atomic commit, manifest, gap tracking) |
| rollup-output-committer.mjs | 293 | 30s persistence |
| output-committer.mjs | 174 | 1s persistence (5-step atomic commit) |
| recovery.mjs | 305 | Crash recovery (manifest/checkpoint reconciliation) |
| manifest-manager.mjs | ~300 | Manifest/checkpoint CRUD |
| rollup-5min.mjs | 157 | Pure 5min aggregation from 10×30s rows |
| rollup.mjs | 144 | Pure 30s aggregation |
| schema.mjs | 163 | Contract definitions (22 feature fields, quality envelope) |
| feature-computer-1s.mjs | ~250 | 1s feature computation from burst detector + book |
| consumer-5min.mjs | 229 | Consumer contract for 5min rows |
| burst-detector.mjs | ~200 | Burst detection logic |
| block-scanner.mjs | ~150 | Block file discovery and ordering |
| input-validator.mjs | ~100 | Trade input validation |
| pending-block-manager.mjs | ~100 | Pending block lifecycle |

### test/ (15,733 lines total)
- **686 tests, all passing** (verified this run)
- **167 test suites**
- burst-reducer tests: ~30 test files covering pipeline, recovery, rollup, consumers, adversarial cases
- Connector tests: binance, bybit, okx, bitstamp, gemini, hyperliquid
- Integration tests: active-book-sync, binance-sync, pipeline-rollup-wiring, tfp-lock-integration

---

## 5. Data Flow (Current)

```
[Exchange WS] → Connector → RawRotationWriter → data/live_v3/{trades,book_updates,liquidations}/
                                                         ↓
                              [offline pipeline: tfp.mjs]
                                                         ↓
                              data/derived/burst_features_v1/features_1s/  (1s burst features)
                                                         ↓
                              data/derived/burst_features_v1/features_30s/ (30s rollup)
                                                         ↓
                              data/derived/burst_features_v1/features_5min/ (5min rollup)
```

Auxiliary path:
```
[Exchange REST] → DerivativesHelper → data/.../derivatives/
[Exchange REST] → MarketDataCollector → data/.../market_data/
```

---

## 6. Deployment State

| Component | Deployment | Status |
|-----------|-----------|--------|
| fairprice_monitor | screen session `agg-btc-receiver` (scripts/start.sh) | NOT RUNNING |
| aux_data_collector | systemd `agg-btc-aux-collector.service` | Active (enabled) |
| orderflow_monitor | manual / not deployed | NOT RUNNING |
| burst-reducer pipeline | manual (scripts/tfp.mjs) | NOT RUNNING |

**data/ directory:** Only `data/derived/` exists. No `data/live_v3/`, `data/raw_hot/`, etc. — no live data currently.

---

## 7. Viewpoint Analysis

### 7.1 Forward Viewpoint (What needs to happen)
- **Receiver-only redesign** is the current project focus (parent task t_23ba8d44)
- Pipeline already works: raw → 1s → 30s → 5min with gap barrier
- P4 (book activation) and P5 (retention) are planned but not yet implemented
- Consumer contracts are still forming (5min consumer exists but is recon-only)
- No Gateway integration yet

### 7.2 Recovery Viewpoint (Crash/safety)
- **Strong:** 5-step atomic commit (stage → intent → rename → fsync → checkpoint)
- **Strong:** SHA-256 hash verification at every commit boundary
- **Strong:** Fail-closed on corrupt checkpoint/manifest
- **Strong:** Generation consistency checks prevent stale commits
- **Gap barrier:** 5min buffer reset on non-consecutive input (P3 gap barrier, commit `32389c9`)
- **Startup recovery:** Writer recovery via `startupRecovery()` for rotation writers
- **Recovery module:** `recovery.mjs` reconciles intent records on restart

### 7.3 Adversarial Viewpoint (Edge cases)
- **Stale message detection:** 30s threshold in base-connector
- **Reconnect backoff:** 1s-30s exponential, max 30 attempts
- **Input validation:** `input-validator.mjs` validates trade schemas
- **Quarantine system:** Failed blocks go to quarantine dir with structured reports
- **Verified-missing tracking:** Book gaps tracked separately from trade gaps
- **Checkpoint size bounds:** 256 KiB warn, 1 MiB hard limit
- **Self-test reconnect:** Built-in socket close + reconnect test

### 7.4 Consumer Viewpoint (Who reads the output)
- **5min consumer:** `consumer-5min.mjs` reads committed-only rows, validates schema, supports range queries
- **Downstream consumers:** Not yet defined (open question from P3-C2 recon)
- **No API/endpoint:** All output is file-based (JSONL)
- **Consumer contract:** 5min rows must have quality envelope (source_layer, coverage, finalized, etc.)

### 7.5 State Isolation Viewpoint
- **Namespace isolation:** Each layer (1s/30s/5min) has its own manifest, checkpoint, output dir
- **Market isolation:** Each market processed independently in pipeline
- **Worker isolation:** 4 worker threads, each with own connectors/writers, no shared state
- **Kind isolation:** trades and book_updates tracked separately (P0-1)
- **Run isolation:** Each pipeline run gets unique runId for staging paths

### 7.6 Performance Viewpoint
- **Worker staggering:** 50ms between worker/market startups
- **Flush intervals:** trades 200ms, book 1000ms, liquidations 200ms
- **Health push:** Every 2s from workers to main
- **Stale check:** Every 15s across all writers
- **Market data tick:** 60s default
- **Dependencies:** Minimal (ws + duckdb only)
- **Memory:** In-memory book state per market (BaseConnector._ringBuf = 64K entries)

---

## 8. Open Contract Questions

1. **Consumer contracts undefined:** 5min consumer exists but downstream consumers (dashboard, API, Gateway) not yet specified. This is a blocker for production use.
2. **orderflow_monitor vs fairprice_monitor:** Two separate multi-market WS receivers exist. `start.sh` only runs `fairprice_monitor`. The relationship between these two entry points needs clarification — are they meant to coexist, or is one being replaced?
3. **Raw data path divergence:** `orderflow_monitor` writes to `data/live_v3/`, `fairprice_monitor` writes to `data/raw_hot/`. The pipeline reads from which?
4. **P4 book activation:** #13-#22 columns are still placeholders. P4 plan exists but not implemented.
5. **P5 retention:** Cleanup scripts not yet implemented. Derived data grows without bounds.
6. **Gateway integration:** Not started. How will consumers access the data?
7. **Dashboard.mjs:** Exists but not inspected. Role unclear.

---

## 9. Test Health

- **686/686 tests PASS** (run this session)
- **167 suites**
- **Duration:** ~18.6s
- **Key test areas:** Pipeline (golden, adversarial, horizon, recovery, lock), Rollup (30s, 5min), Consumer, Schema, Connectors, Book state machine, Integration

---

## 10. Key Commit History (recent)

| SHA | Message | Significance |
|-----|---------|-------------|
| 32389c9 | fix: apply 5min alignment at gap barrier | P3 gap barrier fix |
| fc61806 | P3: continue 5min conversion across input gaps | Gap barrier implementation |
| 1f61603 | perf: cache book seed snapshot index during replay | Performance optimization |
| 89863c0 | P4: seed book replay from snapshot archives | Book replay seeding |
| 630198f | P4: #13-#22 activation with real values | Book features activation |
| 709a939 | P3-C3: 5min pipeline wiring | 5min wiring complete |
| 6c589bb | p3-c2: 5min committer + consumer | 5min persistence |

---

## 11. File Inventory (non-vendor, non-test)

| Category | Count | Total Lines |
|----------|-------|------------|
| lib/ (source) | 35 .mjs | ~11,300 |
| lib/burst-reducer/ | 16 .mjs | ~4,200 |
| test/ | 50+ .test.mjs | ~15,700 |
| scripts/ | 20+ .mjs/.sh/.py | ~2,000 |
| docs/ | 40+ .md | ~3,000 |
| Top-level entry | 4 .mjs | ~900 |
| Config | 1 .json | 196 |
| systemd | 1 .service | 15 |
