# C0 Tree Runtime Reconnaissance Report

- Date: 2026-07-15
- Scope: Working tree structure, test coverage, and runtime architecture
- Status: Read-only reconnaissance (no code changes)

## 1. Tree Structure

### 1.1 Source modules (`lib/`)

**Core receiver infrastructure (11 modules, ~2,800 lines):**
- `base-connector.mjs` (542 lines) — base class for all exchange connectors; event emission with ts validation, reconnect scheduling, state machine
- `market-connectors.mjs` (182 lines) — CONNECTOR_CLASSES registry, factory instantiation
- `events.mjs` (46 lines) — event constants
- `health-monitor.mjs` (92 lines) — JSONL health logging per market
- `trade-only-connector.mjs` (23 lines) — base for trade-only connector variants
- `trade-size-buckets.mjs` (29 lines) — trade size bucketing constants

**Exchange connectors (10 modules, ~2,290 lines):**
- `binance-connector.mjs` (359 lines)
- `binance-usdc-connector.mjs` (17 lines)
- `bitfinex-connector.mjs` (156 lines)
- `bitmex-connector.mjs` (257 lines)
- `bitstamp-connector.mjs` (122 lines)
- `bybit-connector.mjs` (217 lines)
- `coinbase-connector.mjs` (207 lines)
- `coinbase-international-connector.mjs` (111 lines)
- `crypto-com-connector.mjs` (139 lines)
- `gemini-connector.mjs` (192 lines)
- `hyperliquid-connector.mjs` (142 lines)
- `kraken-connector.mjs` (192 lines)
- `okx-connector.mjs` (235 lines)

**Book & data processing (7 modules, ~2,200 lines):**
- `book-state-machine.mjs` (358 lines) — P0-0 book state machine, `stateAt`, `snapshotState`
- `book-updates-adapter.mjs` (119 lines) — book envelope parsing
- `full-book.mjs` (318 lines) — full book reconstruction
- `buffered-writer.mjs` (234 lines) — atomic JSONL writer with auto-flush
- `market-data-collector.mjs` (810 lines) — market data ingestion coordinator
- `derivatives-helper.mjs` (269 lines) — derivatives parser
- `fair-price-collector.mjs` (279 lines) — fair price computation
- `replay-book-state.mjs` (137 lines) — older book replay (separate from BookStateMachine)

**Validation (1 module):**
- `config-validator.mjs` (132 lines) — structural config validation (FIX9c, new 2026-07-15)

**Orderflow pipeline (4 modules, ~1,700 lines):**
- `orderflow-worker.mjs` (280 lines) — worker thread market group processor
- `raw-rotation-writer.mjs` (842 lines) — rotated JSONL writer with no-clobber
- `burst-builder.mjs` (241 lines) — burst detection helper

**Burst reducer (14 modules, ~3,200 lines):**
- `burst-reducer/pipeline.mjs` (1028 lines) — main pipeline orchestrator
- `burst-reducer/schema.mjs` (163 lines) — schema validation
- `burst-reducer/input-validator.mjs` (145 lines) — trade input validation
- `burst-reducer/burst-detector.mjs` (98 lines) — burst boundary detection
- `burst-reducer/burst-state-codec.mjs` (213 lines) — burst state checkpoint codec
- `burst-reducer/feature-computer-1s.mjs` (144 lines) — 1s feature computation
- `burst-reducer/output-committer.mjs` (174 lines) — atomic blocking output commit
- `burst-reducer/rollup.mjs` (144 lines) — rollup computation
- `burst-reducer/rollup-5min.mjs` (157 lines) — 5-min rollup
- `burst-reducer/rollup-5min-committer.mjs` (491 lines) — 5-min rollup commit
- `burst-reducer/rollup-output-committer.mjs` (293 lines) — rollup output commit
- `burst-reducer/manifest-manager.mjs` (257 lines) — manifest tracking
- `burst-reducer/consumer-5min.mjs` (229 lines) — 5-min consumer
- `burst-reducer/recovery.mjs` (305 lines) — crash recovery
- `burst-reducer/block-scanner.mjs` (79 lines) — block file scanner
- `burst-reducer/raw-trades-notional-reader.mjs` (161 lines) — trade notional reader
- `burst-reducer/pending-block-manager.mjs` (22 lines) — pending block state
- `burst-reducer/agg-trades-reader.mjs` (47 lines) — agg trades reader

### 1.2 Root-level entrypoints (5 modules, ~1,720 lines)
- `orderflow_monitor.mjs` (305 lines) — multi-worker orchestrator
- `aux_data_collector.mjs` (189 lines) — auxiliary data collection
- `fairprice_monitor.mjs` (201 lines) — fair price monitor
- `dashboard.mjs` (720 lines) — monitoring dashboard

### 1.3 Test files (56 files, 16,685 lines)

**Core/connector tests (12 files):**
- `test/base-connector.test.mjs` (498 lines) — includes 33 FIX9d ts validation tests
- `test/binance-connector-parser.test.mjs` (818 lines)
- `test/binance-sync.test.mjs` (203 lines)
- `test/binance-usdc-connector.test.mjs` (178 lines)
- `test/bitstamp-connector.test.mjs` (64 lines)
- `test/connector-parser.test.mjs` (1153 lines)
- `test/additional-markets-connector.test.mjs` (269 lines)
- `test/gemini-connector.test.mjs` (66 lines)
- `test/hyperliquid-sync.test.mjs` (27 lines)
- `test/market-data-collector.test.mjs` (581 lines)
- `test/derivatives-helper.test.mjs` (228 lines)
- `test/fair-price-collector.test.mjs` (197 lines)

**Book tests (7 files):**
- `test/book-state-machine.test.mjs` (466 lines)
- `test/book-updates-adapter.test.mjs` (306 lines)
- `test/full-book.test.mjs` (214 lines)
- `test/active-book-sync.test.mjs` (133 lines)
- `test/replay-book-state.test.mjs` (149 lines)
- `test/tfp-book-contract-fixture.test.mjs` (1314 lines)
- `test/bybit-topic-routing.test.mjs` (35 lines)

**Orderflow tests (6 files):**
- `test/orderflow-monitor.test.mjs` (364 lines) — startup + entrypoint subprocess tests
- `test/orderflow-worker-raw-only.test.mjs` (414 lines)
- `test/raw-rotation-writer.test.mjs` (358 lines)
- `test/buffered-writer.test.mjs` (136 lines)
- `test/burst-builder.test.mjs` (269 lines)
- `test/parquet-pipeline.test.mjs` (101 lines)

**Config validation tests (1 file):**
- `test/config-validator-runtime.test.mjs` (584 lines) — FIX9c: 46 unit + 10 subprocess tests

**Burst reducer tests (29 files):**
- `test/burst-reducer/pipeline.test.mjs` (540 lines)
- `test/burst-reducer/recovery.test.mjs` (889 lines)
- `test/burst-reducer/horizon.test.mjs` (607 lines)
- `test/burst-reducer/raw-trades-notional-reader.test.mjs` (481 lines)
- `test/burst-reducer/cursor-restart.test.mjs` (401 lines)
- `test/burst-reducer/rollup-5min.test.mjs` (232 lines)
- `test/burst-reducer/rollup.test.mjs` (199 lines)
- `test/burst-reducer/rollup-output-committer.test.mjs` (134 lines)
- `test/burst-reducer/rollup-5min-committer.test.mjs` (258 lines)
- `test/burst-reducer/consumer-5min.test.mjs` (177 lines)
- `test/burst-reducer/feature-computer-1s.test.mjs` (182 lines)
- `test/burst-reducer/finalized-adversarial.test.mjs` (41 lines)
- `test/burst-reducer/golden.test.mjs` (220 lines)
- `test/burst-reducer/input-validator.test.mjs` (148 lines)
- `test/burst-reducer/manifest-manager.test.mjs` (247 lines)
- `test/burst-reducer/output-committer.test.mjs` (166 lines)
- `test/burst-reducer/schema.test.mjs` (125 lines)
- `test/burst-reducer/b3-join.test.mjs` (181 lines)
- `test/burst-reducer/b4-board.test.mjs` (184 lines)
- `test/burst-reducer/b5-checkpoint.test.mjs` (140 lines)
- `test/burst-reducer/b6-inventory.test.mjs` (86 lines)
- `test/burst-reducer/block-scanner.test.mjs` (56 lines)
- `test/burst-reducer/burst-detector.test.mjs` (169 lines)
- `test/burst-reducer/burst-state-codec.test.mjs` (109 lines)
- `test/burst-reducer/checkpoint-size.test.mjs` (181 lines)
- `test/burst-reducer/tfp-lock-integration.test.mjs` (772 lines)
- `test/burst-reducer/p3-c3-wiring.test.mjs` (79 lines)
- `test/burst-reducer/pipeline-rollup-wiring.test.mjs` (98 lines)
- `test/burst-reducer/lock.test.mjs` (207 lines)

**Aux tests (1 file):**
- `test/aux-data-collector.test.mjs` (251 lines)

## 2. Test Coverage Summary

| Suite | Tests (`it()`) | Passing |
|-------|----------------|---------|
| Base connector emit/ts/reconnect | 44 | 44/44 |
| Config validator (unit + subprocess) | 56 | 56/56 |
| Connector parsers (all exchanges) | 52+34+16+12+3+3+1+25+8+20+3 | all |
| Book state machine | 27 | 27/27 |
| Burst reducer (all phases) | ~200 | all |
| Orderflow monitor startup + entrypoint | 19 | 19/19 |
| Orderflow worker | 11 | 11/11 |
| Raw rotation writer | 37 | 37/37 |
|| **Full suite** | **179 suites, 826 `it()`, 774 `tap`** | **774/774 PASS** |

Note: The 826 `it()` calls produce 774 TAP-level pass entries because some `it()` calls appear in comment blocks and are not executed; nested describe blocks and conditional/skip patterns also reduce the final assertion count.

## 3. Runtime Architecture

```
orderflow_monitor.mjs (main thread)
  ├─── HealthMonitor (health.jsonl writer)
  ├─── validateConfig() ← fail-closed entry gate (FIX9c)
  └─── Worker A (binance_spot, binance_perp, ...)
  │      └── orderflow-worker.mjs
  │             ├── market-connectors → connector instance per market
  │             │      └── base-connector.mjs ← ts validation (FIX9d)
  │             ├── raw-rotation-writer.mjs
  │             └── fair-price-collector.mjs
  ├─── Worker B (bybit_perp, bybit_spot, okx_perp, okx_spot)
  ├─── Worker C (coinbase_spot, kraken_spot, bitstamp_spot, gemini_spot)
  └─── Worker D (crypto_com_spot, bitfinex_spot, bitmex_perp,
                 coinbase_international_perp, hyperliquid_perp)
```

### 3.1 Startup Sequence (as of 2026-07-15)
1. Parse CLI flags (`--help`, `--config`, `--seconds`, `--markets`, `--output`, `--selfTestReconnectAfterMs`)
2. Load config from `--config` path (default: `config.v3.json`)
3. **FIX9c**: `validateConfig(config)` — structural validation before any config access
   - On failure: exit 1 with actionable error messages on stderr
   - On success: continue
4. Determine enabled markets (from `--markets` flag or config's enabled markets)
5. Spawn worker threads (A-D) with staggered 50ms delay
   - Each worker gets init message with `{ workerId, markets, configMarkets, configOutput, configTick, outputBase }`
6. Wait for all spawned workers to signal `ready` (60s timeout, fail-closed)
   - **FIX9d**: Workers now validate timestamps before emitting depth/trade events
   - `startupFailed` IPC message or worker exit-before-ready → shutdown all workers, exit 1
7. Start HealthMonitor
8. Listen for SIGTERM/SIGINT → graceful shutdown

### 3.2 Event Flow
1. Worker connects to exchange WS
2. Receives depth diff / trade events
3. Parses via exchange-specific connector
4. **FIX9d**: `_emitDepth` / `_emitTrade` drops events with invalid `ts` (non-number, NaN, Infinity, negative)
5. Valid events written to JSONL via `raw-rotation-writer.mjs`
6. Periodic health stats sent to main thread via IPC

## 4. Key Boundaries (verified current)

| Boundary | Implementation | Status |
|----------|---------------|--------|
| Config structural validation | `lib/config-validator.mjs` (FIX9c) | ✓ fail-closed |
| Invalid config exit | exit 1, actionable stderr, no output created | ✓ verified |
| Timestamp schema | `typeof !== 'number' \|\| !isFinite \|\| < 0` in `_emitDepth`/`_emitTrade` | ✓ fail-closed |
| Exchange connector parsing | 13 connectors, each with `parseDepth`/`parseTrade` | ✓ tested |
| Worker startup ready-wait | 60s timeout, fail-closed on partial ready | ✓ tested |
| Graceful shutdown | SIGTERM/SIGINT → worker shutdown → flush → exit 0 | ✓ tested |
| Reconnect backoff | BaseConnector reconnect scheduling | ✓ tested |

## 5. Allowlist / Secret / Scope Claims

- **No secrets or API keys** in source code. Credentials are loaded from config file (not committed).
- **No allowlist/blocklist** in source code. Market groups are declared in `WORKER_MARKET_GROUPS` (orderflow_monitor.mjs:23-28) and filtered by config's enabled flags.
- **No external network calls** in unit tests. All tests use local mock data.
- **No data written outside** `outputBase` directory. All outputs go to the configured `base_path`.
- **Scope of this recon**: tree only. No runtime profiling or resource usage measurements.
