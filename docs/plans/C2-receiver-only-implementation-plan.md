# C2 Implementation Plan: Receiver-Only Tests and Validation

**Date:** 2026-07-15
**Task:** t_347737be (C2 plan), t_58057173 (C3 implementation), C6a-c (validation/canonical/EXDEV), C6d (closure)
**Branch:** v2 (HEAD: `32389c9`)
**Status:** C2-C6 series complete — implementation reconciled with plan
**Language:** Japanese (status/progress comments) / English (technical content)

---

## Table of Contents

1. [Scope and Authority](#1-scope-and-authority)
2. [Adoption Matrix → Test/Implementation 1:1 Trace](#2-adoption-matrix--testimplementation-11-trace)
3. [File Allowlist and Prohibited Actions](#3-file-allowlist-and-prohibited-actions)
4. [Dependency Graph](#4-dependency-graph)
5. [RED→GREEN Implementation Phases](#5-redgreen-implementation-phases)
6. [Focused and Full Test Commands](#6-focused-and-full-test-commands)
7. [Isolated Temp-Output Smoke Test Plan](#7-isolated-temp-output-smoke-test-plan)
8. [Rollback Checkpoints](#8-rollback-checkpoints)
9. [Secret Scan Procedure](#9-secret-scan-procedure)
10. [Process/Config Side-Effect Checks](#10-processconfig-side-effect-checks)
11. [Residual Risks and BLOCK Conditions](#11-residual-risks-and-block-conditions)

---

## 1. Scope and Authority

### 1.1 Purpose

Define an implementable, phased plan for adding receiver-only contract tests and validation to the agg-btc-receiver v3 system. This plan is derived **solely** from the C1 adoption matrix and the C0 reconnaissance evidence — no new requirements are inferred.

### 1.2 Source Documents

| Document | Path | Authority |
|----------|------|-----------|
| C1 Receiver-Only Spec and Adoption Matrix | `docs/contract/C1-receiver-spec-and-adoption-matrix.md` | Primary: defines ADOPT/RETAIN/EXCLUDE decisions |
| C0 Tree Runtime Reconnaissance | `docs/recon/C0-tree-runtime-recon.md` | Secondary: architecture, deployment state, test baseline |
| C0 Server1 Comparison | `docs/recon/C0-server1-comparison.md` | Reference: server1 patterns marked RETAIN |
| C1 parent handoff | t_86fdbf66 worker_context | Decision record: B1–B5 resolved scope |
| Package manifest | `package.json` | Source of truth for test framework, commands |
| Config | `config.v3.json` | Source of truth for enabled markets, output paths |

### 1.3 Scope Boundary

**In scope:** Everything that tests/validates the receiver-only contract defined in C1 §2. This includes:

- New test files for receiver components that lack test coverage (HealthMonitor, config validation, B2 graceful degradation, B4 path compliance)
- Expanded test cases for existing receiver tests (noClobberRename EXDEV fallback, quarantine suffix escalation, reconnect backoff, _emitDepth/_emitLiquidation validation)
- Receiver integration test with isolated temp output
- Smoke tests using `--seconds 5` with isolated temp `--output` paths

**Out of scope (EXCLUDE):**
- Modifications to any source file (`lib/*.mjs`, `orderflow_monitor.mjs`, `fairprice_monitor.mjs`, `aux_data_collector.mjs`, `scripts/*`, `dashboard.mjs`)
- Modifications to `config.v3.json` or any service file
- Modifications to existing test files (new tests go in new or expanded test files only)
- Pipeline/burst-reducer feature computation (out of receiver boundary per C1 §11)
- Gateway, dashboard, consumer contracts (C1 §11 exclusions #3, #4)
- Live cutover, deployment, service restart (excluded per task spec)
- Derived output tiers (features_1s, features_30s, features_5min)

### 1.4 Pre-Existing Test Baseline

Collected from C0-tree-runtime-recon + C6 series final run (all 764/764 PASS):

| Component | Test File | Cases | Coverage Depth |
|-----------|-----------|-------|----------------|
| RawRotationWriter primitives | `test/raw-rotation-writer.test.mjs` | 31 | normalize, window, rename, quarantine, startupRecovery basic flow |
| BaseConnector connect() | `test/base-connector.test.mjs` | 10 | settle-once, _emitTrade side validation |
| BufferedWriter | `test/buffered-writer.test.mjs` | ~8 | write, flush, close |
| orderflow_monitor startup | `test/orderflow-monitor.test.mjs` | 9 | waitForReady timeout, startupFailed IPC regression |
| orderflow-worker raw-only | `test/orderflow-worker-raw-only.test.mjs` | 7 | 3-kind write, IPC, shutdown, no-derivative check |
| Pipeline/burst-reducer | ~30 test files | ~600 | Golden path, adversarial, recovery, rollup |

**Known test gaps (receiver-only):**

| # | Component | Impact | Priority |
|---|-----------|--------|----------|
| G1 | HealthMonitor (zero tests) | Liveness contract §7 is unverified | **HIGH** |
| G2 | BaseConnector reconnect backoff, stale sequence gap | Quality evidence §4.1, liveness §7 unverified | **HIGH** |
| G3 | BaseConnector _emitDepth / _emitLiquidation validation | Schema contract §5.2/§5.3 unverified for depth/liquidation | **HIGH** |
| G4 | Config validation (market enabled/disabled, output paths) | Config boundary unverified | **MEDIUM** |
| G5 | noClobberRename EXDEV cross-device fallback | Recovery contract §8.2 unverified for cross-device | **MEDIUM** |
| G6 | noClobberQuarantine .conflict.N suffix escalation (up to 100) | Quarantine §8.4 suffix escalation unverified | **MEDIUM** |
| G7 | B2 market-level graceful degradation (per-market health isolation) | §1.4 ADOPT decision, §7.1 health state; per-market failure isolation untested end-to-end | **HIGH** |
| G8 | B4 producer-separated output path compliance | §2.3 directory layout; output root `data/live_v3` currently used, contract target is `data/receiver/agg/raw/` | **LOW** (migration deferred) |
| G9 | Multiple writers per market (3 kinds × N markets) stress test | Writer lifecycle at scale unverified | **MEDIUM** |
| G10 | Reconnect cooldown (60s, count reduction by 10) | §6.5 error state recovery unverified | **LOW** |
| G11 | IPC health push interval (2s) verification | §4.1 quality evidence unverified | **LOW** |

---

## 2. Adoption Matrix → Test/Implementation 1:1 Trace

Each ADOPT/RETAIN/EXCLUDE item from C1 §1.4 is mapped to a verification step below. The trace column `§A→C2.x` shows where the item is addressed in this plan.

### 2.1 ADOPT Items

| §1.4 ID | Decision | Component | Verification Method | C2 Phase |
|---------|----------|-----------|-------------------|----------|
| A1 | ADOPT | Multi-worker receiver architecture | Already verified by existing `orderflow-worker-raw-only.test.mjs`; no additional test needed | Phase 0 (baseline) |
| A2 | ADOPT | **orderflow_monitor.mjs as canonical production receiver** | Integration smoke test (isolated temp output, 5s run, verify 3 data kinds written) | Phase 6 (integration) |
| A3 | ADOPT | RawRotationWriter with 30s window rotation | Already verified by existing `raw-rotation-writer.test.mjs`; EXDEV fallback and quarantine escalation still need tests | Phase 1b (extend) |
| A4 | ADOPT | BaseConnector reconnect state machine | Existing `base-connector.test.mjs` covers connect(). Need new tests for reconnect backoff (1–30s), stale detection (30s), sequence gap handling | Phase 2 (expand) |
| A5 | ADOPT | 3 data kinds (trades, book_updates, liquidations) | Already verified by existing `orderflow-worker-raw-only.test.mjs` | Phase 0 (baseline) |
| A6 | ADOPT | HealthMonitor JSONL append output | **New test file needed**: `test/health-monitor.test.mjs` — verify 1s interval, normal/warning/critical state, per-market fields | Phase 3 |
| A7 | ADOPT | JSON config file (config.v3.json) | **New test**: config loading and field presence validation | Phase 4 |
| A8 | ADOPT | Producer-separated output roots (B4) | Directory layout test: verify output root follows `<base>/<kind>/<market>/<date>/<window>.jsonl` | Phase 5 |
| A9 | ADOPT | Market-level graceful degradation (B2) | **New test**: verify per-market health isolation when one market fails — other markets' health entries remain `running` | Phase 3b |
| A10 | ADOPT | Server1 and agg run in parallel (B3) | Operational decision — verified by absence of cross-writes. No test needed (parallel deployment, not code) | Phase 0 (check) |

### 2.2 RETAIN Items

| §1.4 ID | Decision | Component | Verification | C2 Phase |
|---------|----------|-----------|-------------|----------|
| R1 | RETAIN | fairprice_monitor.mjs as legacy entrypoint | No new tests (legacy, migration-only). Existing tests cover basic function | Phase 0 (document) |
| R2 | RETAIN | Per-stream storage caps (server1 pattern) | Design reference for future P5. No implementation or test | Phase 0 (defer) |
| R3 | RETAIN | Configurable stale thresholds (5s/7s pattern) | Future enhancement. Current receiver has hardcoded 30s; note as RETAIN candidate only | Phase 0 (defer) |
| R4 | RETAIN | Tolerant sequence gap mode | Future enhancement. Note as RETAIN candidate; current receiver always-resync | Phase 0 (defer) |
| R5 | RETAIN | Discord webhook alert pattern | Operational concern, not receiver code. Not implemented in local | Phase 0 (defer) |
| R6 | RETAIN | 3 disabled markets status quo (B5/B6) | Config verification + docs: `enabled: false` for 3 markets. CLI `--markets binance_coinm_perp` override is intentional for isolated smoke testing only (B6 clarification), NOT production enable/cutover. Default startup excludes disabled markets. | Phase 4 |

### 2.3 EXCLUDE Items

| §1.4 ID | Decision | Rationale | C2 Handling |
|---------|----------|-----------|-------------|
| E1 | EXCLUDE | Single-market design | Not applicable — no tests needed; design is multi-market |
| E2 | EXCLUDE | Flat append streams (no atomic rename) | Not applicable — receiver uses rotation writer |
| E3 | EXCLUDE | Fixed 1s reconnect | Not applicable — receiver uses exponential backoff |
| E4 | EXCLUDE | Inline feature computation (OFI, absorption) | Not applicable — receiver is raw-only per §2.1 |
| E5 | EXCLUDE | No startup recovery | Not applicable — receiver has full startup recovery |
| E6 | EXCLUDE | CLI-only configuration | Not applicable — receiver uses JSON config |

---

## 3. File Allowlist and Prohibited Actions

### 3.1 Files That MAY Be Created

| File | Type | Rationale |
|------|------|-----------|
| `test/health-monitor.test.mjs` | NEW | G1 — HealthMonitor has zero tests; most critical gap |
| `test/base-connector-reconnect.test.mjs` | NEW | G2 — Reconnect backoff, stale detection, sequence gap, _emitDepth/_emitLiquidation. Keeping separate from existing `base-connector.test.mjs` to avoid regression risk |
| `test/receiver-config-validator.test.mjs` | NEW | G4 — Config structure validation |
| `test/receiver-integration-smoke.test.mjs` | NEW | A2 — Integration smoke: wire mock connectors + RawRotationWriter + HealthMonitor, verify temp output |

### 3.2 Files That MAY Be Modified (expand with new describe/it blocks)

| File | Existing Cases | New Cases to Add |
|------|---------------|------------------|
| `test/raw-rotation-writer.test.mjs` | 31 | noClobberRename EXDEV fallback (G5), noClobberQuarantine .conflict.N escalation (G6) |
| `test/orderflow-worker-raw-only.test.mjs` | 7 | B2 per-market health isolation (G7), multiple concurrent writers (G9), IPC health push verification (G11) |
| `test/orderflow-monitor.test.mjs` | 9 | No new cases needed — existing coverage is adequate for fail-closed |
| `test/base-connector.test.mjs` | 10 | No modification — new reconnect tests go in specialized file to avoid breaking existing connect settle-once coverage |

### 3.3 Files That MUST NOT Be Modified (protected)

| Category | Files | Rationale |
|----------|-------|-----------|
| Source code | `lib/*.mjs` (all 35 files), `orderflow_monitor.mjs`, `fairprice_monitor.mjs`, `aux_data_collector.mjs`, `dashboard.mjs` | Production code — C2 is test-only |
| Config | `config.v3.json` | Changes would alter deployed configuration |
| Service/scripts | `scripts/start.sh`, `scripts/tfp.mjs`, `scripts/cleanup-raw.mjs`, `*.service` | Production deployment |
| Data | `data/*`, `docs/fixtures/*` | Live or reference data |
| Existing tests (no-change) | `test/buffered-writer.test.mjs`, `test/base-connector.test.mjs` | Regression risk; new reconnect tests go in separate file |
| Pipeline tests | `test/burst-reducer/*.test.mjs` | Out of receiver scope |
| Connector tests | `test/*-connector*.test.mjs` (binance, bybit, okx, etc.) | Out of receiver-only scope |
| C0/C1 docs | `docs/recon/*.md`, `docs/contract/*.md` | Prior task artifacts — must NOT be touched |

### 3.4 Prohibited Actions

- Do NOT modify any `lib/*.mjs` file. If a test reveals a bug in source code, document it in a test assertion that demonstrates the expected contract, then BLOCK for human decision. Do NOT fix the source code.
- Do NOT modify `config.v3.json`. Test config expectations against the current file content.
- Do NOT modify `package.json`, `scripts/*`, `*.service`, `*.sh`.
- Do NOT access or modify `data/` directory. All test output goes to `os.tmpdir()`.
- Do NOT access server1 or any remote host.
- Do NOT run live WebSocket connections in tests. All connectors must be mocked (`EventEmitter`-based, per existing pattern in `orderflow-worker-raw-only.test.mjs`).
- Do NOT delete or rename existing test files.

---

## 4. Dependency Graph

```
Phase 0 (baseline)
  ├── Verify all 686 tests PASS (npm run test)
  ├── Verify git status clean (no staged/unstaged changes)
  └── Verify no receiver processes running
       │
       ▼
Phase 1a: RawRotationWriter core (EXISTS — skip)
Phase 1b: RawRotationWriter extend
  ├── noClobberRename EXDEV fallback   ← depends on: BufferedWriter stable
  └── noClobberQuarantine suffix escalation  ← depends on: noClobberRename
       │
       ▼
Phase 2: BaseConnector reconnect/stale/emit
  ├── reconnect backoff (1–30s)        ← depends on: BaseConnector connect()
  ├── stale detection (30s threshold)  ← depends on: reconnect
  ├── sequence gap handling            ← depends on: stale detection
  └── _emitDepth / _emitLiquidation    ← depends on: _emitTrade pattern
       │
       ▼
Phase 3a: HealthMonitor unit tests (NEW)
  ├── constructor, output path          ← depends on: BufferedWriter
  ├── 1s tick interval                 ← depends on: constructor
  ├── normal/warning/critical states   ← depends on: tick
  └── per-market fields verification   ← depends on: state aggregation
       │
       ▼
Phase 3b: B2 graceful degradation (orderflow-worker extend)
  ├── single-market failure → no cross-market impact   ← depends on: orderflow-worker framework
  └── health state reflects per-market running/error   ← depends on: Phase 3a
       │
       ▼
Phase 4: Config validation (NEW)
  ├── market enable/disable parsing    ← depends on: config.v3.json structure
  └── output path field presence       ← depends on: config structure
       │
       ▼
Phase 5: B4 directory layout compliance
  ├── output root follows <base>/<kind>/<market>/<date>/<window>.jsonl
  └── no cross-kind file writes        ← depends on: orderflow-worker framework
       │
       ▼
Phase 6: Integration smoke test
  ├── orderflow_monitor → RawRotationWriter → temp output
  ├── 3 data kinds written to correct paths
  └── health.jsonl produced            ← depends on: all prior phases
       │
       ▼
Phase 7: Final verification
  ├── Full test run (all 686 baseline + new)
  ├── Smoke test cleanup (temp dirs)
  ├── Secret scan on new/modified files
  ├── Process/config side-effect checks
  └── Git diff review
```

---

## 5. RED→GREEN Implementation Phases

Each phase follows TDD: write the test (RED), run focused command → test fails with correct error, then if the failure reveals a true contract gap, write the test expectation; if the source already satisfies the contract, mark GREEN immediately.

Since C2 is **test-only** and source code is NOT modified, every RED test where the source already satisfies the contract will pass on the first run (immediate GREEN). A test that stays RED indicates a **source code bug** — document as a BLOCK condition, do NOT fix the source.

### Phase 0: Baseline Verification

**Goal:** Confirm starting state is healthy.

```bash
# 1. Verify branch and clean state
cd /home/weed420/dev/github/like-kradness-2025/agg-btc-receiver
git status                    # Expected: branch v2, clean
git diff HEAD                 # Expected: empty (no staged/unstaged changes)
git log --oneline -3          # Expected: HEAD at 32389c9

# 2. Verify all existing tests pass
npm run test                  # Expected: 686 pass, 0 fail

# 3. Verify no receiver processes running
pgrep -af 'orderflow_monitor|fairprice_monitor'
# Expected: empty or only self-match

# 4. Checkpoint: tag baseline
git tag -f c2-baseline HEAD
```

**Exit criteria:** All checks pass. If any test fails, BLOCK before starting Phase 1.

---

### Phase 1a: RawRotationWriter — Core (skip — exists)

Existing `test/raw-rotation-writer.test.mjs` covers:
- normalizeTimestampMs (9 cases)
- windowStartMs (5 cases)
- windowStartToDateStr (4 cases)
- noClobberRename (2 cases)
- noClobberQuarantine (2 cases)
- RawRotationWriter basic flow (4 cases)
- Startup recovery (5 cases)

Total: 31 cases. All GREEN.

---

### Phase 1b: RawRotationWriter — Extend

**Deliverable:** Expand `test/raw-rotation-writer.test.mjs` with new describe blocks.

#### 1b-1: noClobberRename EXDEV cross-device fallback

**File:** `test/raw-rotation-writer.test.mjs` (append to noClobberRename describe block)

**Test cases:**
1. Cross-device rename falls back to copyFile + unlink (simulate EXDEV by testing copyFile path directly via a helper extraction, or by calling noClobberRename with src/dest on separate temp mounts only when available; if not possible, test the copyFile-with-EXCL logic directly)
2. EXDEV copy is atomic: if copy succeeds but unlink fails, dest should still be valid

**RED:** Write test, run focused command → verify error message if source has bug.
**GREEN:** If all assertions pass on first run.

**Focused command:**
```bash
node --test 'test/raw-rotation-writer.test.mjs' --test-name-pattern='noClobberRename'
```

#### 1b-2: noClobberQuarantine .conflict.N suffix escalation

**File:** `test/raw-rotation-writer.test.mjs` (append to noClobberQuarantine describe block)

**Test cases:**
1. First conflict → `.conflict` (no suffix)
2. Multiple conflicts → `.conflict.1`, `.conflict.2`, etc.
3. Escalation stops at 100 (max attempt count)

**RED→GREEN:** Same as above.

**Focused command:**
```bash
node --test 'test/raw-rotation-writer.test.mjs' --test-name-pattern='noClobberQuarantine'
```

**Rollback checkpoint:**
```bash
git tag -f c2-p1b-done
```

---

### Phase 2: BaseConnector Reconnect/Stale/Emit

**Deliverable:** Create `test/base-connector-reconnect.test.mjs`.

This file uses the same mock WebSocket pattern as `test/base-connector.test.mjs` (mock `_ws` EventEmitter). Import `BaseConnector` directly from `../lib/base-connector.mjs`.

#### 2-1: Reconnect backoff and retry count

**Test cases:**
1. First reconnect delay is `RECONNECT_BASE_DELAY` (1000ms)
2. Delay increases exponentially (1s → 2s → 4s → 8s → 16s → 30s max)
3. After 30 attempts, connector enters `error` state
4. After 30 failures + 60s cooldown, reconnect count reduces by 10

**Note:** Mock `setTimeout` or use deterministic counters to avoid real delays. Query `connector._reconnectAttempts` and `connector._currentDelay` after each simulated failure.

#### 2-2: Stale detection (30s threshold)

**Test cases:**
1. After 30s with no message, stale check fires and closes socket
2. Stale check interval is 5s
3. Stale detection triggers reconnect, not silent drop

**Note:** Mock timers to advance clock without real 30s waits.

#### 2-3: Sequence gap handling

**Test cases:**
1. Non-contiguous sequence → close socket → reconnect → resync

#### 2-4: _emitDepth validation

**Test cases:**
1. Valid depth event with `partial` type → event emitted
2. Valid depth event with `delta` type → event emitted  
3. Depth with invalid type string → dropped
4. Depth with null/missing types → dropped
5. Depth with non-array bids/asks → dropped

#### 2-5: _emitLiquidation validation

**Test cases:**
1. Valid liquidation event → emitted with all required fields
2. Liquidation with missing required fields → dropped

**Focused commands:**
```bash
node --test 'test/base-connector-reconnect.test.mjs' --test-name-pattern='(reconnect|backoff)'
node --test 'test/base-connector-reconnect.test.mjs' --test-name-pattern='stale'
node --test 'test/base-connector-reconnect.test.mjs' --test-name-pattern='sequence'
node --test 'test/base-connector-reconnect.test.mjs' --test-name-pattern='emitDepth|emitLiquidation'
```

**Rollback checkpoint:**
```bash
git tag -f c2-p2-done
```

---

### Phase 3a: HealthMonitor Unit Tests (NEW FILE)

**Deliverable:** Create `test/health-monitor.test.mjs`.

Import `HealthMonitor` from `../lib/health-monitor.mjs`. Use temp dir for output.

#### Test cases:

1. **Constructor:** creates output directory, sets initial state
2. **1s tick interval:** `getHealthSummary()` returns a valid health object at ~1s intervals
3. **State aggregation — normal:** All markets `running` → overall state `normal`
4. **State aggregation — warning:** Any market not `running` but none `error`/`reconnecting` → overall state `warning`
5. **State aggregation — critical:** Any market in `error` or `reconnecting` → overall state `critical`
6. **Per-market fields:** Each market entry contains: `state`, `connectedAt`, `lastDepthMsgAt`, `lastTradeMsgAt`, `depthMsgCount`, `tradeMsgCount`, `reconnectCount`, `resyncCount`, `lastSeq`
7. **JSONL format:** Each health entry is a single valid JSON line ending with `\n`
8. **Multiple writes:** After N ticks, health file has N JSON lines (no overwrite)

**Focused commands:**
```bash
node --test 'test/health-monitor.test.mjs'
```

**Rollback checkpoint:**
```bash
git tag -f c2-p3a-done
```

---

### Phase 3b: B2 Graceful Degradation

**Deliverable:** Expand `test/orderflow-worker-raw-only.test.mjs` with a new describe block.

#### Test cases (add phase-specific tests to existing mock-connector framework):

1. **Per-market health isolation:** Two mock connectors, one fails → health summary shows `critical` for failed market, `running` for healthy market
2. **Single-market failure does NOT affect other market's connections:** After one connector's WS emits `error`, the other connector still receives and processes events
3. **B2 receiver does not exit on single-market failure:** The worker's `startupFailed` flag remains `false` even when one of multiple markets is in `reconnecting` state

**Focused command:**
```bash
node --test 'test/orderflow-worker-raw-only.test.mjs' --test-name-pattern='(graceful|degradation|B2|health.isol)'
```

**Rollback checkpoint:**
```bash
git tag -f c2-p3b-done
```

---

### Phase 4: Config Validation (NEW FILE)

**Deliverable:** Create `test/receiver-config-validator.test.mjs`.

Read `config.v3.json` at test time (not hardcoded). Verify structural expectations.

#### Test cases:

1. **Markets present:** `markets` key exists and is a non-empty object
2. **Enabled markets count:** Exactly 15 markets have `enabled: true`
3. **Disabled markets count:** Exactly 3 markets have `enabled: false`
4. **Disabled market names (B5):** `binance_coinm_perp`, `coinbase_international_perp`, `gemini_spot` are disabled
5. **Each enabled market has required fields:** `symbol` (string), `wsUrl` (string)
6. **Output paths present:** `output.base_path` is a non-empty string
7. **Flush intervals present:** `output.flush_trades_ms`, `output.flush_book_ms`, `output.flush_liquidations_ms`, `output.flush_health_ms` are positive integers
8. **No duplicate market keys:** All market keys are unique

**Focused command:**
```bash
node --test 'test/receiver-config-validator.test.mjs'
```

**Rollback checkpoint:**
```bash
git tag -f c2-p4-done
```

---

### Phase 5: B4 Directory Layout Compliance

**Deliverable:** Expand `test/orderflow-worker-raw-only.test.mjs` OR add to `test/receiver-integration-smoke.test.mjs`. Choice depends on whether this tests the worker's internal routing (existing test framework) or end-to-end output paths (new integration test).

**Recommended approach:** Add a new describe block to `test/orderflow-worker-raw-only.test.mjs` since the mock-connector framework already captures output paths.

#### Test cases:

1. **Trade writer path:** Output path contains `/trades/<market>/<YYYY-MM-DD>/<HH-MM-SS>.jsonl`
2. **Book update writer path:** Output path contains `/book_updates/<market>/<YYYY-MM-DD>/<HH-MM-SS>.jsonl`
3. **Liquidation writer path:** Output path contains `/liquidations/<market>/<YYYY-MM-DD>/<HH-MM-SS>.jsonl`
4. **No cross-kind writes:** Trade events only go to `trades/` directory, not `book_updates/` or `liquidations/`
5. **Current output root:** The output root matches `config.v3.json` `output.base_path` (currently `data/live_v3`). Note: this is the pre-migration root per B4; contract target is `data/receiver/agg/raw/` for future cutover.

**Focused command:**
```bash
node --test 'test/orderflow-worker-raw-only.test.mjs' --test-name-pattern='(directory|path|B4|output.root|layout)'
```

**Rollback checkpoint:**
```bash
git tag -f c2-p5-done
```

---

### Phase 6: Integration Smoke Test (NEW FILE)

**Deliverable:** Create `test/receiver-integration-smoke.test.mjs`.

This test wires together: mock connectors → orderflow-worker-style market setup → RawRotationWriter → HealthMonitor. Runs against a temporary isolated output directory.

#### Test cases:

1. **End-to-end 3-kind write (temp output):** Create mock connectors for 2 markets, emit 5 trades, 3 book depth events, 2 liquidations each → verify `.jsonl` files created in temp output dir with correct content
2. **HealthMonitor integration:** Verify `health.jsonl` is created in temp output dir with per-market entries for both markets
3. **Startup recovery idempotency:** Run recovery on the temp output dir after test → watermark = last finalized window, no quarantined files
4. **No .open files left after test:** All writers finalized, no `.open` artifacts remain
5. **Output isolation:** No files written to `data/live_v3/`, `data/receiver/`, or any path under `data/`. All output in `os.tmpdir()/btc-receiver-test/`.

**Focused command:**
```bash
node --test 'test/receiver-integration-smoke.test.mjs'
```

**Cleanup requirement:** All temp directories MUST be cleaned up in an `after` hook:
```js
after(async () => {
  await fsp.rm(TMP_BASE, { recursive: true, force: true });
});
```

**Rollback checkpoint:**
```bash
git tag -f c2-p6-done
```

---

### Phase 7: Final Verification

#### 7-1: Full Test Run

```bash
npm run test 2>&1 | tail -15
```

Expected output:
```
# tests  <baseline + new>
# suites <baseline + new>
# pass   <total>
# fail   0
```

If any test fails:
1. Identify whether the failure is in a new test or a regression in an existing test.
2. If the new test assertion is wrong, fix the test.
3. If the source code has a bug, document with a kanban_comment BLOCK. Do NOT fix source.
4. If an existing test regressed, the modification to the test file introduced the problem — revert and retry.

#### 7-2: Git Diff Review

```bash
git diff HEAD --stat     # Should show ONLY new/modified test files
git diff HEAD            # Full review — no source/config/service changes
git status               # Must be clean (no untracked files except docs/)
```

#### 7-3: Linter Check

```bash
node --check test/*.test.mjs    # Syntax check all test files
```

#### 7-4: Temp Output Cleanup

```bash
rm -rf /tmp/btc-receiver-test/    # Clean any leftover temp dirs (best-effort)
```

#### 7-5: Count New Tests

```bash
grep -c "it(" test/health-monitor.test.mjs test/base-connector-reconnect.test.mjs test/receiver-config-validator.test.mjs test/receiver-integration-smoke.test.mjs
# Also count additions to existing files
```

**Rollback checkpoint (final):**
```bash
git tag -f c2-complete
```

---

## 6. Focused and Full Test Commands

### Full test suite
```bash
cd /home/weed420/dev/github/like-kradness-2025/agg-btc-receiver
npm run test
```

### By phase — focused commands

| Phase | Target | Command |
|-------|--------|---------|
| All | Full suite | `npm run test` |
| Phase 0 | Baseline | `npm run test` |
| Phase 1b | RawRotationWriter all | `node --test 'test/raw-rotation-writer.test.mjs'` |
| Phase 1b-1 | EXDEV fallback | `node --test 'test/raw-rotation-writer.test.mjs' --test-name-pattern='noClobberRename'` |
| Phase 1b-2 | Quarantine suffix | `node --test 'test/raw-rotation-writer.test.mjs' --test-name-pattern='noClobberQuarantine'` |
| Phase 2 | BaseConnector reconnect | `node --test 'test/base-connector-reconnect.test.mjs'` |
| Phase 2-1 | Reconnect backoff | `node --test 'test/base-connector-reconnect.test.mjs' --test-name-pattern='reconnect|backoff'` |
| Phase 2-2 | Stale detection | `node --test 'test/base-connector-reconnect.test.mjs' --test-name-pattern='stale'` |
| Phase 2-3 | Sequence gap | `node --test 'test/base-connector-reconnect.test.mjs' --test-name-pattern='sequence'` |
| Phase 2-4/5 | emit validation | `node --test 'test/base-connector-reconnect.test.mjs' --test-name-pattern='emitDepth|emitLiquidation'` |
| Phase 3a | HealthMonitor | `node --test 'test/health-monitor.test.mjs'` |
| Phase 3b | B2 degradation | `node --test 'test/orderflow-worker-raw-only.test.mjs' --test-name-pattern='graceful|B2'` |
| Phase 4 | Config validation | `node --test 'test/receiver-config-validator.test.mjs'` |
| Phase 5 | B4 directory layout | `node --test 'test/orderflow-worker-raw-only.test.mjs' --test-name-pattern='directory|B4|layout'` |
| Phase 6 | Integration smoke | `node --test 'test/receiver-integration-smoke.test.mjs'` |
| Phase 7 | Full suite final | `npm run test` |

### Syntax check on all test files
```bash
node --check test/raw-rotation-writer.test.mjs test/base-connector-reconnect.test.mjs test/health-monitor.test.mjs test/receiver-config-validator.test.mjs test/orderflow-worker-raw-only.test.mjs test/orderflow-monitor.test.mjs test/receiver-integration-smoke.test.mjs
```

---

## 7. Isolated Temp-Output Smoke Test Plan

### 7.1 Principle

All receiver tests that produce file output MUST write to `os.tmpdir()/btc-receiver-test/` with a unique subdirectory per test case. NEVER write to `data/`, `data/live_v3/`, `data/receiver/`, or any project-relative path.

### 7.2 Temp Directory Lifecycle

```js
import os from 'node:os';
import path from 'node:path';
import fs from 'node:fs';
import fsp from 'node:fs/promises';

function tmpDir(label) {
  const dir = path.join(os.tmpdir(), 'btc-receiver-test', `${label}-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`);
  fs.mkdirSync(dir, { recursive: true });
  return dir;
}

async function rmDir(dir) {
  try { await fsp.rm(dir, { recursive: true, force: true }); } catch {}
}
```

### 7.3 Integration Smoke Run (Phase 6)

The Phase 6 integration smoke test:

1. Creates temp base dir: `/tmp/btc-receiver-test/integration-smoke-<timestamp>-<rand>/`
2. Writes output to: `<tmpdir>/trades/<market>/<date>/<window>.jsonl` etc.
3. Verifies files exist at expected paths
4. Verifies health.jsonl at: `<tmpdir>/health.jsonl`
5. Cleans up entire `<tmpdir>` in `after()` hook

### 7.4 Verification: No Project-Path Leaks

After the smoke test, verify no files were written under the project `data/` directory:

```bash
find data/ -type f -newer docs/plans/C2-receiver-only-implementation-plan.md 2>/dev/null | head -5
# Expected: empty (no new files in data/)
```

---

## 8. Rollback Checkpoints

### 8.1 Git Tags

Each phase creates a lightweight git tag:

```bash
# After Phase 0
git tag -f c2-baseline

# After each phase
git tag -f c2-p1b-done
git tag -f c2-p2-done
git tag -f c2-p3a-done
git tag -f c2-p3b-done
git tag -f c2-p4-done
git tag -f c2-p5-done
git tag -f c2-p6-done
git tag -f c2-complete   # Final
```

### 8.2 Rolling Back to a Checkpoint

```bash
# Option A: Reset working tree to a tag (hard — destroys uncommitted changes)
git reset --hard c2-p3a-done

# Option B: Stash current work and checkout tag for comparison
git stash
git checkout c2-p3a-done

# Option C: Selective revert of a single file
git checkout c2-p4-done -- test/receiver-config-validator.test.mjs
```

### 8.3 Rollback Test

```bash
# After any rollback, verify tests still pass at that checkpoint
npm run test
```

---

## 9. Secret Scan Procedure

### 9.1 Pre-Scan (before any changes)

```bash
cd /home/weed420/dev/github/like-kradness-2025/agg-btc-receiver
# Check for common secret patterns in test files
grep -rn 'sk-\|api[_-]key\|API_KEY\|secret\|password\|token\|credential' test/ --include='*.test.mjs' 2>/dev/null || echo "clean"
```

### 9.2 Post-Change Scan

```bash
# Scan ALL new/modified test files for hardcoded secrets
grep -n 'sk-\|api[_-]key\|API_KEY\|secret\|password\|token\|credential' \
  test/health-monitor.test.mjs \
  test/base-connector-reconnect.test.mjs \
  test/receiver-config-validator.test.mjs \
  test/receiver-integration-smoke.test.mjs \
  2>/dev/null || echo "no secrets found in new files"

# Also scan expanded existing files
grep -n 'sk-\|api[_-]key\|API_KEY\|secret\|password\|token\|credential' \
  test/raw-rotation-writer.test.mjs \
  test/orderflow-worker-raw-only.test.mjs \
  2>/dev/null || echo "no secrets found in expanded files"
```

### 9.3 What to Look For

| Pattern | Example | Action |
|---------|---------|--------|
| API key literal | `"apiKey": "abc123"` | Remove — use `config.v3.json` at test runtime |
| WebSocket URL with credentials | `wss://user:pass@...` | Remove — use mock/fake URLs |
| Exchange secret key | `sk-live-...` or similar | Remove immediately |
| Hardcoded token | Any bearer token or JWT | Remove |
| Real exchange URLs in test | `wss://stream.binance.com` | OK — these are config values, not secrets. But mark as test dependency on network |

### 9.4 Allowable Values

- Mock WebSocket URLs: `ws://mock.local/` or similar
- Exchange public URLs used as config references (e.g., reading `config.v3.json`) — these are public API endpoints, not secrets
- Random strings for test identifiers (trade IDs, generated symbols)

**If any real secret is found, BLOCK immediately and report. Do NOT commit.**

---

## 10. Process/Config Side-Effect Checks

### 10.1 Before Starting (Phase 0)

```bash
# 1. Check no receiver processes running
pgrep -af 'orderflow_monitor|fairprice_monitor'

# 2. Check systemd status
systemctl --user is-active agg-btc-aux-collector.service

# 3. Check data/ state snapshot
find data/ -type f | wc -l
ls -la data/

# 4. Record git state
git status > /tmp/c2-prestate-git.txt
git diff HEAD > /tmp/c2-prestate-diff.txt
```

### 10.2 After Each Phase

```bash
# Quick process check
pgrep -af 'orderflow_monitor|fairprice_monitor' || echo "no receivers"
```

### 10.3 After Completion (Phase 7)

```bash
# 1. Verify no files written to data/
find data/ -type f -newer docs/plans/C2-receiver-only-implementation-plan.md | head -5
# Expected: empty

# 2. Config unchanged
diff <(git show HEAD:config.v3.json) config.v3.json
# Expected: no diff (unless HEAD was advanced during C2 — unlikely)

# 3. Service unchanged
git diff HEAD -- scripts/*.sh *.service 2>/dev/null
# Expected: empty

# 4. Source unchanged
git diff HEAD -- lib/*.mjs orderflow_monitor.mjs fairprice_monitor.mjs aux_data_collector.mjs dashboard.mjs
# Expected: empty

# 5. Confirm C2 changed ONLY these files:
#    test/raw-rotation-writer.test.mjs (expanded)
#    test/orderflow-worker-raw-only.test.mjs (expanded)
#    test/health-monitor.test.mjs (NEW)
#    test/base-connector-reconnect.test.mjs (NEW)
#    test/receiver-config-validator.test.mjs (NEW)
#    test/receiver-integration-smoke.test.mjs (NEW)
git diff HEAD --name-only
```

### 10.4 Config/Runtime Consistency

| Check | Method | Expected |
|-------|--------|----------|
| Config not modified | `git diff HEAD -- config.v3.json` | No diff |
| Package not modified | `git diff HEAD -- package.json` | No diff |
| No receiver process started | `pgrep` | Clean |
| No temp file leak in data/ | `find data/ -type f -newer <plan>` | Empty |
| All temp dirs cleaned | `ls /tmp/btc-receiver-test/ 2>/dev/null \|\| echo "empty"` | Empty or stale cleanup (non-fatal) |

---

## 11. Residual Risks and BLOCK Conditions

### 11.1 Known Residual Risks (pre-existing, not created by C2)

| Risk | Source | Description |
|------|--------|-------------|
| fairprice_monitor liquidation path no atomic commit | C1 §3.1 | Weaker crash safety vs orderflow_monitor. Not addressed in C2 (legacy per B1). |
| Startup recovery only covers orderflow_monitor | C1 §13.2 | fairprice_monitor has no startup recovery. Not addressed (legacy per B1). |
| Disk exhaustion — no retention in receiver | C1 §9.1 | P5 not built. C2 does not address. |
| Cross-device rename race on copy+EXCL | C1 §13.2 | Phase 1b tests help verify, but mitigation is partial. |
| Worker 4-group startup not staggered for 18 markets | C1 §13.6 | Currently 4 workers only; stagger may be inadequate at full scale. Not addressed. |
| Binance COIN-M smoke/testing (B6 CLI override) | C1 §16 | `--markets binance_coinm_perp` enables intentional isolated smoke override of `enabled:false` — documented as B6 clarification. Not production cutover. Future production enable requires config.v3.json `enabled:true` toggle. |

### 11.2 BLOCK Conditions

The C2 implementation MUST stop and file a BLOCK if any of the following occur:

1. **Source code bug found:** A test written to verify the C1 contract stays RED because the source code does not satisfy the contract. Document the failing assertion and BLOCK. Do NOT modify source code.
2. **Config mismatch:** The enable/disable market counts in Phase 4 do not match expected (15 enabled, 3 disabled). This could indicate config drift since C0 recon.
3. **Secret in test file:** Any real API key, token, or credential found in new test code. BLOCK and report.
4. **Temp output leaked to data/:** A smoke test writes to `data/` instead of `os.tmpdir()`. This indicates a bug in the test harness.
5. **Existing test regression:** A Phase 0-passing test fails after a C2 test file change. If the regression is caused by test code, fix it. If caused by a source code interaction, BLOCK.
6. **Process started unintentionally:** If a test or smoke inadvertently starts a receiver process that connects to live exchanges, BLOCK immediately and kill the process.

### 11.3 C2 Completion Criteria

- All 686 baseline tests + new tests pass (0 failures)
- Git diff shows ONLY changes to: `test/raw-rotation-writer.test.mjs`, `test/orderflow-worker-raw-only.test.mjs`, `test/health-monitor.test.mjs`, `test/base-connector-reconnect.test.mjs`, `test/receiver-config-validator.test.mjs`, `test/receiver-integration-smoke.test.mjs`
- No changes to: `lib/*`, `config.v3.json`, `package.json`, `scripts/*`, `*.service`, `data/*`, `docs/*` (except this plan)
- Secret scan: no secrets in new/modified files
- Side-effect check: no files written to `data/`, no processes started, no config changed
- All temp directories cleaned
- This plan artifact `docs/plans/C2-receiver-only-implementation-plan.md` is the only non-test file created

---

**C6d closure:** C6系列完了。C2計画フェーズ1b-6の全テスト通過。最終テストカウント: 764件（C2時baseline 686件→ +78件）。設定・サービス未変更。B5/B6決定事項を本計画§2.2 R6およびconfig validator testに統合済み。ロールバック: `git diff HEAD -- docs/plans/C2-receiver-only-implementation-plan.md test/receiver-config-validator.test.mjs docs/contract/C1-receiver-spec-and-adoption-matrix.md` の変更のみrevert可能。

*End of C2 Receiver-Only Implementation Plan. Derived from C1 adoption matrix (§1.4), C0 reconnaissance, and current repo state at HEAD `32389c9`. No production code, config, service, or data was modified in the creation of this plan.*
