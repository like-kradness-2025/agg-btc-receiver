# C1: Receiver Specification & Adoption Matrix

- Date: 2026-07-15
- Status: Current as of FIX9c + FIX9d completion
- Verifies: Config boundary, timestamp schema, entrypoint contracts

## 1. Receiver Boundary Contract

The receiver (`orderflow_monitor.mjs`) is a **receive + save only** component. Its contract:

1. **Load configuration** from JSON file (`--config path`)
2. **Validate configuration** structurally before any config access or worker startup
3. **Spawn 4 worker threads** (market groups A-D) to connect to exchange WebSocket streams
4. **Receive depth diffs and trades** via exchange connectors
5. **Save raw JSONL** files to output directory with rotating writers
6. **Shut down gracefully** on SIGTERM/SIGINT or after `--seconds` duration

### 1.1 Fail-Closed Boundaries (established by FIX9c + FIX9d)

| Boundary | Module | Contract | Since |
|----------|--------|----------|-------|
| Config structural validation | `lib/config-validator.mjs` | Invalid config → exit 1 with actionable stderr, no workers spawned, no output created | FIX9c |
| Config file load error | `orderflow_monitor.mjs:72-75` | Missing/unreadable config → exit 1, stderr mentions path | FIX9c |
| Timestamp type/range | `lib/base-connector.mjs:321,332` | Non-number, NaN, Infinity, negative ts → event dropped (no emission) | FIX9d |
| Worker startup timeout | `orderflow_monitor.mjs:229-236` | Partial ready after 60s → shutdown all workers, exit 1 | FIX9d |
| Worker exit before ready | `orderflow_monitor.mjs:185-188` | Worker exits before `ready` → set startupFailed, shutdown | original |
| Worker error before ready | `orderflow_monitor.mjs:177-179` | Worker error before `ready` → set startupFailed | original |

### 1.2 Non-Contract Behaviors (explicitly out of scope)
- No feature computation in receiver
- No authentication/authorization
- No rate limiting
- No data archival or compaction
- No REST API

## 2. Adoption Matrix

### 2.1 Module Adoption

| Module | Type | Lines | Tests | Coverage |
|--------|------|-------|-------|----------|
| `lib/config-validator.mjs` | New (FIX9c) | 132 | 56 (46 unit + 10 subprocess) | All validation paths, full suite pass 56/56 |
| `lib/base-connector.mjs` | Modified (FIX9d) | 542 | 44 (11 original + 33 ts) | Emit paths, reconnect, ts guards, all pass |
|| `orderflow_monitor.mjs` | Modified (FIX9c/d) | 305 | 19 (10 unit + 9 subprocess) | Startup, fail-closed, entrypoint |
| `test/config-validator-runtime.test.mjs` | New (FIX9c) | 584 | N/A (test of config-validator) | All validation + subprocess |
| `test/base-connector.test.mjs` | Modified (FIX9d) | 498 | N/A (test of base-connector) | All emit + ts guard |
| `test/orderflow-monitor.test.mjs` | Modified (FIX9e) | 397 | N/A (test of entrypoint) | Unit + subprocess |

### 2.2 Exchange Connector Matrix

| Connector | File | Lines | Tests |
|-----------|------|-------|-------|
| Binance | `lib/binance-connector.mjs` | 359 | `binance-connector-parser.test.mjs` (818L, 34 tests) |
| Binance USDⓈ | `lib/binance-usdc-connector.mjs` | 17 | `binance-usdc-connector.test.mjs` (178L, 16 tests) |
| Bitfinex | `lib/bitfinex-connector.mjs` | 156 | `connector-parser.test.mjs` (shared, 52 tests) |
| Bitmex | `lib/bitmex-connector.mjs` | 257 | `connector-parser.test.mjs` (shared) |
| Bitstamp | `lib/bitstamp-connector.mjs` | 122 | `bitstamp-connector.test.mjs` (64L, 3 tests) |
| Bybit | `lib/bybit-connector.mjs` | 217 | `bybit-topic-routing.test.mjs` (35L, 3 tests) |
| Coinbase | `lib/coinbase-connector.mjs` | 207 | `connector-parser.test.mjs` (shared) |
| Coinbase International | `lib/coinbase-international-connector.mjs` | 111 | `connector-parser.test.mjs` (shared) |
| Crypto.com | `lib/crypto-com-connector.mjs` | 139 | `additional-markets-connector.test.mjs` (269L, 12 tests) |
| Gemini | `lib/gemini-connector.mjs` | 192 | `gemini-connector.test.mjs` (66L, 3 tests) |
| Hyperliquid | `lib/hyperliquid-connector.mjs` | 142 | `hyperliquid-sync.test.mjs` (27L, 1 test) |
| Kraken | `lib/kraken-connector.mjs` | 192 | `connector-parser.test.mjs` (shared) |
| OKX | `lib/okx-connector.mjs` | 235 | `connector-parser.test.mjs` (shared) |

### 2.3 Data Flow Matrix

```
config.v3.json
  ↓
[FIX9c] validateConfig() ──→ exit 1 if invalid
  ↓
orderflow_monitor.mjs main()
  ↓ spawn 4 workers
Worker A-D (orderflow-worker.mjs)
  ↓ per market
Connector instance (base-connector.mjs subclass)
  ↓ WS message
[FIX9d] _emitDepth / _emitTrade (ts guard) ──→ drop invalid ts
  ↓
raw-rotation-writer.mjs → JSONL files
  ↓
HealthMonitor            ← IPC stats every tick
```

### 2.4 Historical Snapshots

**Before FIX9c (config validation):**
- No structural validation of config.json
- Invalid config would fail at arbitrary later access points with confusing errors
- Workers could start with partial/invalid config

**Before FIX9d (timestamp validation):**
- No validation of `ts` field in `_emitDepth` / `_emitTrade`
- NaN, Infinity, or negative timestamps would be emitted to consumers
- No dropped-event accounting

**After FIX9c + FIX9d (current):**
- Config validated before any worker startup or output creation
- Timestamps validated before emission — malformed values silently dropped
- Both boundaries match the fail-closed contract: no partial work, no ambiguous state

## 3. Verification Checklist

- [x] **Config structural validation**: 46 unit tests + 10 subprocess tests pass
- [x] **Invalid config → exit 1**: verified with 9 distinct invalid config variants
- [x] **Valid config → validation passes**: verified with real `config.v3.json`
- [x] **ts validation → correct events pass**: valid positive ts, ts=0 emit normally
- [x] **ts validation → invalid dropped**: undefined, null, string, NaN, Infinity, -Infinity, negative all dropped silently
- [x] **No partial emission**: invalid ts does not update stats or partial counters
- [x] **No cross-contamination**: one market's invalid ts does not affect other markets
- [x] **Full suite passes**: 774/774 PASS, 0 fail
- [x] **`npm run check`**: PASS (all modules syntax-checked)
- [x] **No secrets/credentials** in source
- [x] **No commits** — working tree only
