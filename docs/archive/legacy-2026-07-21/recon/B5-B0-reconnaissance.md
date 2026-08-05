# B5-B0 Reconnaissance Report: Book Quarantine Persistence and Kind-Aware Checkpoint

- Date: 2026-07-12
- Branch: v2
- Status: read-only reconnaissance (no code changes)

## 1. Book Quarantine State Machine

### Quarantine triggers (lib/book-state-machine.mjs)
- **MALFORMED_LEVEL** (L88-100): Non-finite or non-positive price, negative qty
- **SEQUENCE_GAP** (L126-158): `prev_seq !== last_seq` or `seq !== last_seq + 1`
- **CROSSED_BOOK** (L204-216): `best_bid >= best_ask` after state apply
- All three set: `quarantined=true`, `commit=false`, `cursor='retain'`, `book_status='quarantine'`

### Quarantine output surfaces
- `processBlock(events)` → `{ state: null, quality: {...}, decisions: { quarantined: true } }`
- `stateAt(events, anchor)` → `{ state: null, quarantined: true }`
- `BookStateMachine.snapshotDecisions()` → `{ quarantined, commit, cursor, error_code, gap_detected }`
- `BookStateMachine.snapshotQuality()` → `{ book_status, sequence_status, book_event_count_applied, book_event_count_ignored }`

### Quarantine in pipeline (lib/burst-reducer/pipeline.mjs:168-210)
- `loadBookSnapshot()` reads book_updates JSONL → calls `stateAt(events, blockStartMs + 30000)`
- If `stateResult.quarantined` → returns `{ available: false, book_seeded: false }`
- **No quarantine report is written** for book quarantine events
- **No quarantine metadata is persisted** in manifest or checkpoint

## 2. Checkpoint Structure (Current)

### Checkpoint fields (manifest-manager.mjs:187-224)
```javascript
{
  schema_version: 'burst_features_v1',
  last_committed_block_start: number,  // last committed block's start_ms
  pending_block: {                      // N+1 block awaiting commit
    block_start_ms,
    trade_input_sha256,
    auxiliary_input_hashes,
    replay_identity: { market, block_start_ms, input_path },
    reordered_input,
    timestamp_inversion_count,
  },
  open_burst: {                         // minimal burst state for restart
    schemaVersion: 1,
    open: { side, start_ts, end_ts, prints, min_price, max_price, sum_notional, sum_qty },
    nextId: number,
  },
  generation: number,                   // monotonically increasing
  updated_at: string,                   // ISO timestamp
}
```

### Checkpoint path
- `data/derived/burst_features_v1/manifests/checkpoints/${market}.json`
- One checkpoint per market (no kind separation)

### Checkpoint write points
1. `OutputCommitter.commitFinalizedBlock()` (output-committer.mjs:111-131) — atomic write on commit
2. `writeCheckpoint()` (manifest-manager.mjs:187-224) — standalone write (not used in pipeline)

## 3. Kind-Aware Checkpoint Gaps

### Current state: NO kind-aware checkpoint
- Checkpoint is per-market only, no `kind` field
- `runPipeline()` accepts `kind` parameter but checkpoint doesn't store it
- `processBlocksNonTrade()` (pipeline.mjs:309-397) does NOT write checkpoints
- `recovery.mjs` only handles `kind === 'trades'` (L238-248)

### Specific gaps:

#### A. No kind field in checkpoint
- When restarting, cannot determine which kind the checkpoint is for
- `runPipeline()` L256: `if (cp && kind === 'trades')` — checkpoint cursor only used for trades
- For book_updates: checkpoint is ignored, cursor always comes from `--from`

#### B. No book_updates checkpoint persistence
- `processBlocksNonTrade()` advances `pendingBlock` but never writes checkpoint
- On restart, book_updates processing starts from scratch
- No `open_burst` equivalent for book state machine (pure function, no state to persist)

#### C. No book quarantine in checkpoint
- When book is quarantined, block is not committed
- Quarantine state is not persisted — on restart, same block will be reprocessed
- `writeQuarantineReport()` writes to `quarantine/` directory but not to checkpoint

#### D. No cross-kind cursor coordination
- Trades checkpoint cursor and book_updates cursor are independent
- No mechanism to ensure both kinds are synchronized at the same block boundary
- `kind='book_updates'` processing doesn't verify trades checkpoint alignment

## 4. Book Quarantine Persistence Gaps

### Current book quarantine flow
```
loadBookSnapshot() → stateAt(events, anchor) → { state: null, quarantined: true }
    ↓
loadBookSnapshot() returns { available: false, book_seeded: false }
    ↓
computeFeatures1s({ bookSnapshot: { available: false, ... } })
    ↓
Board candidate fields (#23-#26) remain null (no book state)
    ↓
Block committed with trade-only features (no quarantine blockage)
```

### Gap 1: Book quarantine doesn't block block commit
- P0-0 §9: "book quarantine must not silently commit trade output"
- Current: book quarantine → `bookSnapshot.available = false` → board candidates null → block committed
- Expected: book quarantine → entire block quarantined (no feature output)

### Gap 2: No quarantine report for book events
- `writeQuarantineReport()` exists (pipeline.mjs:44-56) but only called for:
  - Missing finalized input (L326-329)
  - Out-of-order blocks (L349-351)
  - Pending block hash mismatch (L426-429)
- Never called for book quarantine triggers (MALFORMED_LEVEL, SEQUENCE_GAP, CROSSED_BOOK)

### Gap 3: No quarantine metadata in manifest
- Manifest records have `status: 'intent' | 'committed' | 'quarantined'`
- But quarantine is for *trade block* processing failures, not book quarantine
- Book quarantine doesn't create manifest records

### Gap 4: No book quarantine recovery
- `reconcileMarketState()` (recovery.mjs:169-275) handles intent/committed records
- Book quarantine is not a manifest state — it's per-block ephemeral
- On restart, book quarantine state is lost

## 5. Existing Test Coverage

### Book quarantine tests (test/book-state-machine.test.mjs)
- 27 tests covering all quarantine triggers
- Tests verify quarantine state machine transitions
- **No integration test** with pipeline

### Checkpoint tests
- `test/burst-reducer/checkpoint-size.test.mjs` — size boundedness
- `test/burst-reducer/cursor-restart.test.mjs` — cursor persistence
- `test/burst-reducer/recovery.test.mjs` — manifest reconciliation
- **No kind-aware checkpoint test**
- **No book_updates checkpoint test**

### Pipeline tests (test/burst-reducer/pipeline.test.mjs)
- Tests `kind='trades'` only
- **No book_updates pipeline test**
- **No book quarantine → block quarantine test**

## 6. B5 Reconnaissance Findings

### What B5 (Book Quarantine Recovery) needs to address:

1. **Checkpoint kind field**: Add `kind` to checkpoint schema to track which kind each checkpoint is for
2. **Book_updates checkpoint persistence**: `processBlocksNonTrade()` must write checkpoint on block advance
3. **Book quarantine persistence**: When book is quarantined, persist quarantine state in checkpoint or manifest
4. **Cross-kind cursor coordination**: Ensure trades and book_updates cursors are synchronized
5. **Quarantine report for book events**: Call `writeQuarantineReport()` on book quarantine triggers
6. **Book quarantine recovery**: `reconcileMarketState()` must handle book quarantine state

### Minimal B5 scope (per Phase B plan):
- Book-specific quarantine recovery
- Kind-aware checkpoint persistence
- Cross-kind cursor synchronization

### Explicitly out of scope (B5):
- Board candidate column naming (B4 — done)
- Same-block join integration (B3 — done)
- Rollup integration (Phase C)
