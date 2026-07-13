# B3-B0 Reconnaissance Report: trade+book same-block join and bookSnapshotAt

- Date: 2026-07-12
- Branch: v2
- Status: read-only reconnaissance (no code changes)

## 1. Pipeline Block Reader

### scanBlocks (lib/burst-reducer/block-scanner.mjs:20-54)
- `scanBlocks(dataDir, kind, market, fromMs, toMs)` scans both `trades` and `book_updates` directories
- Returns `BlockInfo[]` sorted ascending by `ms`
- Same 30s boundary structure for both kinds
- Both kinds use identical path: `dataDir/<kind>/<market>/<YYYY-MM-DD>/<HH-MM-SS>.jsonl`
- Block range check: `fileMs < toMs && fileMs + 30000 > fromMs`

### runPipeline entry (lib/burst-reducer/pipeline.mjs:171)
- Signature: `runPipeline({ dataDir, market, fromMs, toMs, runId, outputRoot, finalizedThroughMs, frozenInventory, kind })`
- `kind` parameter defaults to `'trades'`
- Calls `scanBlocks(dataDir, kind, market, effectiveFromMs, toMs)` at L214
- Dispatches to `processBlocks()` which then dispatches based on kind:
  - `kind !== 'trades'` → `processBlocksNonTrade()` (L347-348)
  - `kind === 'trades'` → main processing loop (L351+)

### processBlocksNonTrade (pipeline.mjs:252-340)
- Handles `kind='book_updates'`
- Only ordering checks, gap detection, and horizon validation
- NO content parsing, NO feature computation, NO commit
- No book state machine invocation
- This is the "B0 reconnaissance" target — currently a stub for book_updates

## 2. Trade Aggregation

### validateAndParseTrades (lib/burst-reducer/input-validator.mjs:75-141)
- Parses 30s JSONL trade block
- Each line: `{ ts, side, price, qty, tradeId?, market? }`
- E001-E005 fail-closed validation
- E004: timestamp inversions counted (not thrown)
- §4.2: stable sort by ts ASC, preserving original row order for ties
- Returns: `{ trades, inputSha256, reordered_input, timestamp_inversion_count }`
- Trades are `{ ts, price, qty, side, _idx, tradeId, market }`

### BurstDetector (lib/burst-reducer/burst-detector.mjs)
- `feedTrades(trades)` — feeds sorted trades, detects burst boundaries
- `getClosedBurstsOverlapping(secondTs)` — returns bursts overlapping a 1s bucket
- `getMinimalBurstState()` — serializes for checkpoint persistence
- Burst rules: same side, gap <= 50ms, MAX_BURST_DURATION_MS = 5000ms

### computeFeatures1s (lib/burst-reducer/feature-computer-1s.mjs:19-92)
- Current signature: `{ detector, blockStartMs, tradeTsList, warmup, inputBlockIds, lookupTradedNotional30s }`
- Returns: `Object[]` (30 rows)
- Computes #1-#12 trade-only features
- #13 = null (P1 placeholder), #14 = 0 (P1 placeholder)
- **No bookSnapshotAt parameter in current production code**

### computeFeatures1s working tree diff (per review doc)
- Adds `bookSnapshotAt` and `prevMid` parameters
- Changes return type to `{ rows: Object[], nextPrevMid: number|null }`
- Adds #13/#14 computation using book state
- **Contract violation**: overwrites P1 placeholders #13=null, #14=0
- **Breaking change**: existing callers treat return as `Object[]`

## 3. Book State Lookup

### BookStateMachine (lib/book-state-machine.mjs)
- Pure state machine for `book_updates_v1` envelopes
- `apply(event)` — applies single event with full validation
- `processBlock(events)` — one-shot processing of sorted events
- `stateAt(events, anchor)` — strict `< anchor` lookup (P0-0 §6.1)
- `ordered(events)` — deterministic sort per §13.2
- Exported: `{ BookStateMachine, ordered, processBlock, stateAt }`

### stateAt anchor semantics (book-state-machine.mjs:346-358)
```javascript
export function stateAt(events, anchor) {
  const sm = new BookStateMachine();
  if (!events) return sm.snapshotState();
  const sorted = ordered(events);
  for (const event of sorted) {
    if (event.event_ts_ms >= anchor) break;  // strict < anchor
    sm.apply(event);
  }
  if (sm.quarantined) return { state: null, quarantined: true };
  return sm.snapshotState();
}
```
- Correctly implements P0-0 §6.1: `event_ts_ms < anchor_ts_ms`
- Returns `{ state, quarantined }` — null state when quarantined

### snapshotState (book-state-machine.mjs:265-276)
- Returns `{ seeded, best_bid, best_bid_qty, best_ask, best_ask_qty, mid, last_seq }`
- Masks best fields when not seeded (returns null)

### replayBestBookState (lib/replay-book-state.mjs)
- Different API: `replayBestBookState(bookEvents)` → returns `bookAtTime(ts)` lookup function
- Uses `effective_ts_ms` and `subtype` (snapshot_file/book_update_snapshot/book_update_update)
- Binary search for strict `< ts`
- **Separate from BookStateMachine** — two parallel implementations
- BookStateMachine is the P0-0 contract implementation; replay-book-state is older

## 4. Same-Block Join: Exact Wiring Points

### Current state: NO same-block join exists
The pipeline processes `trades` and `book_updates` as separate `kind` values. There is no code path that reads both for the same `block_start_ms`.

### B3 wiring points needed:

#### A. Dual block scanning (pipeline.mjs)
- `scanBlocks()` is called once per run with a single `kind`
- B3 needs: scan both `trades/<market>/<date>/<time>.jsonl` AND `book_updates/<market>/<date>/<time>.jsonl` for the same `block_start_ms`
- Location: `runPipeline()` or a new orchestrating function

#### B. Book content reading
- `input-validator.mjs` has `validateAndParseTrades()` but no `validateAndParseBookUpdates()`
- Book JSONL parsing is done in `BookStateMachine.processBlock()` via `ordered()` + `apply()`
- B3 needs: read book_updates JSONL file → parse each line as JSON envelope → feed to BookStateMachine
- Missing: a function like `parseBookUpdateBlock(content, blockStartMs)` that returns sorted events

#### C. Book state → bookSnapshotAt generation
- For each of the 30 1-second anchors (`secondTs` in `[blockStartMs, blockStartMs+30000)`):
  - `bookSnapshotAt(secondTs)` = book state at `secondTs` (strict `< secondTs`)
  - This is `stateAt(events, secondTs).state`
- Location: after BookStateMachine processes the block, iterate anchors and call `stateAt()`

#### D. Feature computation with book state
- `computeFeatures1s()` needs `bookSnapshotAt` and `prevMid` parameters
- B3 needs: either modify `computeFeatures1s()` signature (with care for P1 contract) OR add book features in a separate post-processing step
- Review doc recommends: book injection at pipeline orchestration layer, not in feature-computer-1s.mjs

#### E. Commit integration
- `OutputCommitter.commitFinalizedBlock()` needs no structural change
- But `_quality` field needs book metadata (book_status, sequence_status, book_event_count_applied, etc.)
- `rows` must include board candidate columns (new names, not overwriting #13/#14)

### Key join boundary:
```
processBlocks() loop body (pipeline.mjs:389-619):
  1. read candidate block content (trades)
  2. validateAndParseTrades(content, blockStartMs)  ← existing
  3. [NEW] read book_updates block for same block_start_ms
  4. [NEW] parse book events from book_updates JSONL
  5. [NEW] processBlock(bookEvents) → book state decisions
  6. [NEW] if book quarantined → quarantine block (no feature output)
  7. [NEW] if book OK → generate bookSnapshotAt for each secondTs
  8. detector.feedTrades(trades)  ← existing
  9. computeFeatures1s({..., bookSnapshotAt, prevMid})  ← modified call
  10. commitFinalizedBlock(...)  ← existing
```

## 5. Schema Contracts

### book_updates_v1 envelope (P0-0 §4)
```json
{
  "schema_version": "book_updates_v1",
  "market": "binance_spot",
  "type": "snapshot" | "update",
  "event_ts_ms": 1000,
  "seq": 100 | null,
  "prev_seq": null | number,
  "bids": [["100", "2"]],
  "asks": [["101", "3"]],
  "source": {"exchange": "test", "channel": "book"}
}
```

### 1s row output (P1 contract)
- 30 rows per block
- 22 feature fields (#1-#22)
- `_quality` envelope
- #13 = null, #14 = 0 (P1 placeholders)
- Board candidates as new named columns (P0-0 §10)

### _quality additions for book (P0-0 §8)
```json
{
  "book_status": "seeded|unseeded|unsequenced|stale_duplicate|quarantine",
  "sequence_status": "ok|unsequenced|stale_duplicate|gap|malformed",
  "book_event_count_applied": 0,
  "book_event_count_ignored": 0,
  "anchor_rule": "event_ts_ms < anchor_ts_ms"
}
```

## 6. Missing Tests

### No same-block join integration test
- `test/burst-reducer/pipeline.test.mjs` only tests `kind='trades'`
- No test creates both `trades/` and `book_updates/` blocks for the same block_start_ms

### No bookSnapshotAt test
- `computeFeatures1s()` has no test with book state input
- `test/burst-reducer/feature-computer-1s.test.mjs` tests only trade-only features

### No book quarantine → trade isolation test
- P0-0 requires: "book quarantine must not silently commit trade output"
- No test verifies that book quarantine blocks the entire block commit

### Existing book tests (B2 complete):
- `test/book-state-machine.test.mjs`: 27 tests — all B2 book state machine paths
- `test/book-updates-adapter.test.mjs`: adapter tests
- `test/tfp-book-contract-fixture.test.mjs`: 24 tests — independent verifier

## 7. Minimal B3 RED→GREEN Scope

### RED tests to write first:
1. `pipeline-book-join.test.mjs`: same-block trades + book_updates join
   - Happy path: both present, book seeded, features computed with book state
   - Missing book_updates file: block quarantined (not committed)
   - Book quarantine (gap/malformed): block quarantined, no feature output
   - Trade-only regression: #1-#12 unchanged when book is absent/quarantined
2. `feature-computer-book.test.mjs`: bookSnapshotAt integration
   - bookSnapshotAt provided → board candidates computed
   - bookSnapshotAt null (unseeded) → board candidates null
   - prevMid tracking across 30 seconds

### GREEN implementation scope:
1. `parseBookUpdateBlock(content, blockStartMs)` in input-validator or new module
2. Modified `processBlocks()` to read both kinds per block_start_ms
3. `generateBookSnapshots(events, blockStartMs)` → Map<secondTs, bookState>
4. Modified `computeFeatures1s()` call with book state (or post-processing step)
5. Book quarantine propagation to block commit decision
6. Updated `_quality` with book metadata

### Explicitly out of scope (per Phase B plan):
- Board candidate column naming (B4)
- Book-specific quarantine recovery (B5)
- Frozen inventory kind separation (B6)
- Rollup integration (Phase C)

## 8. Risk Assessment

1. **computeFeatures1s return type change**: The working tree diff changes `Object[]` → `{rows, nextPrevMid}`. This breaks all 4 existing call sites (L427, L538, L668, gap handler). Must be coordinated.
2. **Two parallel book implementations**: `BookStateMachine` (P0-0 contract) and `replayBestBookState` (older). B3 must use BookStateMachine, not replay-book-state.
3. **Book quarantine blocking trade output**: P0-0 §9 says trade-only #1-#12 must not be nullified by book issues. But the block commit itself must be quarantined when book is quarantined. This is a "block-level quarantine, not feature-level quarantine" design.
4. **Anchor semantics mismatch risk**: `stateAt()` uses strict `< anchor`. The 1s row covers `[secondTs, secondTs+1000)`. Book events in this range affect the NEXT second's book state, not the current one. Must verify this interaction.
