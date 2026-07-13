# P3-C2 Reconnaissance: 5min Persistence and Consumer Contracts

**Date:** 2026-07-13  
**Task:** P3-C2  
**Status:** Complete  

---

## 1. Executive Summary

P3-C1 (pure 5min module) is implemented and tested. This reconnaissance identifies the persistence pattern and consumer contract gaps before P3-C2 implementation begins. Key finding: 5min persistence can follow the established 30s pattern with namespace isolation, but consumer contracts remain undefined and must be frozen before wiring.

---

## 2. Current State

| Layer | Status | Location |
|-------|--------|----------|
| P3-C0 spec freeze | APPROVED | `docs/specs/plan-2026-07-13-p3-5min-spec-freeze.md` |
| P3-C1 pure 5min module | IMPLEMENTED | `lib/burst-reducer/rollup-5min.mjs` |
| P3-C2 persistence | RECON ONLY | This document |
| Consumer contract | NOT DEFINED | Blocker for implementation |

**Data flow (verified):**  
```
raw trades → features_1s → features_30s → features_5min (pure only, no persistence)
```

---

## 3. 5min Persistence Pattern

### 3.1 Reference: 30s Persistence (`RollupOutputCommitter`)

The 30s layer provides the proven pattern:

| Aspect | 30s Implementation |
|--------|-------------------|
| Module | `lib/burst-reducer/rollup-output-committer.mjs` |
| Schema version | `burst_features_30s_v1` |
| Output namespace | `features_30s/` |
| Source layer | `features_1s` |
| Manifest path | `manifests/features_30s/${market}.json` |
| Checkpoint path | `manifests/checkpoints/features_30s/${market}.json` |
| Commit sequence | stage → manifest(intent) → atomic rename → checkpoint → manifest(committed) |
| Reconciliation | `reconcileCommitted1s()` scans source manifest for missed windows |

**Key properties:**
- Namespace isolation: 30s never nests 1s
- Idempotency: hash-checked skip on restart
- Crash recovery: checkpoint tracks last committed window
- Quality provenance: source_block_ids, coverage, operators

### 3.2 Proposed 5min Persistence Design

Following the 30s pattern:

| Aspect | Proposed 5min |
|--------|--------------|
| Module | `lib/burst-reducer/rollup-5min-output-committer.mjs` (new) |
| Schema version | `burst_features_5min_v1` |
| Output namespace | `features_5min/` |
| Source layer | `features_30s` |
| Manifest path | `manifests/features_5min/${market}.json` |
| Checkpoint path | `manifests/checkpoints/features_5min/${market}.json` |
| Window size | 300,000 ms (5 minutes) |
| Source window count | 10 (ten 30s rows) |

**Proposed commit sequence:**
1. Aggregate 10 complete 30s rows → `aggregate5min()`
2. Stage JSONL to `features_5min/${market}/${date}/.staging/${runId}/${time}.jsonl`
3. Write manifest with `status: 'intent'`
4. Atomic rename staged → final
5. Write checkpoint with `last_committed_window_start`
6. Write manifest with `status: 'committed'`

**Proposed reconciliation:**
- Scan `manifests/features_30s/${market}.json` for committed 30s windows
- For each committed 30s window within a 5min boundary, check if 5min manifest has corresponding record
- If 30s committed but 5min missing → re-aggregate and commit

### 3.3 Schema Constants (already defined)

```javascript
// lib/burst-reducer/schema.mjs
export const FEATURES_5MIN_DIR = 'features_5min';  // line 14
```

### 3.4 Pipeline Integration Point

Current 30s wiring in `pipeline.mjs` (line 495-505):
```javascript
const rollupCommitter = new RollupOutputCommitter(market, `${runId}-30s`, derivedDir);
rollupCommitter.reconcileCommitted1s();
const commitRollupAfter1s = (result, finalizedBlock, rows) => rollupCommitter.commitWindow({...});
```

5min wiring should follow the same pattern:
```javascript
// After 30s commit succeeds
if (is5minBoundary(finalizedBlock.block_start_ms)) {
  const fiveMinRows = collect30sWindow(market, finalizedBlock.block_start_ms);
  fiveMinCommitter.commitWindow({ rows: fiveMinRows, ... });
}
```

---

## 4. Consumer Contract Reconnaissance

### 4.1 Known Consumers

| Consumer | Layer | Status | Notes |
|----------|-------|--------|-------|
| Dashboard (`dashboard.mjs`) | raw trades/book | Active | Reads raw blocks, no derived features |
| Cleanup scripts | raw trades | Active | Delete raw after 1s commit |
| Backfill scripts | raw trades | Active | Batch processing |
| `features_5min` | ?? | **NOT DEFINED** | Blocker |

### 4.2 Consumer Contract Gaps

The P3 spec freeze explicitly lists these as "Decisions required later":

1. **Cross-market row grain and primary key** — one row per market/window vs cross-market join
2. **Universe policy** — fixed 15 markets or partial-universe operation
3. **Spot/perp mapping** — cross-sectional operators
4. **Manifest/checkpoint/recovery contract** — what consumers can rely on
5. **Consumer/index contract** — how consumers discover and query data

### 4.3 What Consumers Likely Need

Based on 30s consumer patterns:

| Consumer Need | 30s Answer | 5min Open Question |
|---------------|------------|-------------------|
| Data discovery | Manifest lists committed windows | Same pattern applies |
| Quality gate | `_quality` with coverage, finalized, warmup | Need 5min-specific quality fields |
| Completeness check | `coverage === 1 && has_missing_input === false` | Same, but over 10 windows |
| Timeliness | Checkpoint `last_committed_window_start` | Same |
| Cross-market query | Not supported at 30s | **Must decide** |
| Historical backfill | Manifest-driven reconciliation | Same pattern |

### 4.4 Minimum Viable Consumer Contract (Proposed)

Before implementation, freeze:

1. **Row grain:** One row per market per 5min window (market-local)
2. **Primary key:** `(market, window_start_ms)`
3. **Quality minimum:** `coverage === 1`, `has_missing_input === false`, `finalized === true`
4. **Discovery:** Manifest `processed_windows` with `status: 'committed'`
5. **Cross-market:** Explicitly excluded from this slice

---

## 5. Risk Assessment

| Risk | Impact | Mitigation |
|------|--------|------------|
| Consumer contract undefined | Implementation blocked | Freeze minimum viable contract before coding |
| 5min boundary misalignment | Data gaps | Validate 30s windows are 5min-aligned before aggregation |
| Reconciliation complexity | Crash recovery failure | Follow 30s pattern exactly; test with interrupted writes |
| Cross-market expectations | Scope creep | Explicitly document exclusion in contract |

---

## 6. Recommended Next Steps

1. **Freeze consumer contract** — Define minimum viable contract (market-local, same as P3-C0 approval)
2. **Implement 5min output committer** — Follow `rollup-output-committer.mjs` pattern
3. **Wire to pipeline** — After 30s commit, check 5min boundary and commit
4. **Add reconciliation** — Scan 30s manifest for missed 5min windows
5. **Test crash recovery** — Interrupt between 30s commit and 5min commit

---

## 7. Files Referenced

| File | Relevance |
|------|-----------|
| `lib/burst-reducer/rollup-5min.mjs` | Pure 5min aggregation (implemented) |
| `lib/burst-reducer/rollup-output-committer.mjs` | 30s persistence pattern (reference) |
| `lib/burst-reducer/pipeline.mjs` | Pipeline wiring point |
| `lib/burst-reducer/schema.mjs` | `FEATURES_5MIN_DIR` constant |
| `docs/specs/plan-2026-07-13-p3-5min-spec-freeze.md` | Approved contract |
| `docs/specs/tasks-2026-07-13-p3-5min-spec-freeze.md` | Task list |
| `docs/worklog/2026-07-13-p3-c1-5min-pure.md` | C1 completion status |

---

## 8. Conclusion

5min persistence is architecturally straightforward — the 30s pattern is proven and directly reusable. The only blocker is the consumer contract, which must be frozen before implementation. Recommend freezing the market-local contract (already approved in P3-C0) and deferring cross-market to a later phase.
