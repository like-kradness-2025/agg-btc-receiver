# TFP Phase B5 Plan: Book Checkpoint, Verified-Missing, Kind-Aware Recovery

- Date: 2026-07-12
- Branch: `v2`
- Prerequisite: B4 commit `a832090`, independent review 100/100 PASS
- Mode: Kanban PDD / `delegate_task`; profileSession prohibited

## Scope

Add checkpoint persistence, verified-missing tracking, and kind-aware recovery for book_updates processing path.

### In scope

1. **processBlocksNonTrade checkpoint cursor**: on EOF horizon-valid exit, write checkpoint with pending_block cursor so restart can resume. Add checkpoint loading at non-trade path entry.

2. **Verified-missing book block tracking**: persist verified-missing block start times in manifest so restart doesn't re-process known-missing blocks. Add VERIFIED_MISSING tracking to manifest during gap detection in processBlocksNonTrade.

3. **Kind-aware recovery**: make reconcileMarketState (recovery.mjs) handle book_updates checkpoint recovery alongside trades checkpoint. The book checkpoint stores minimal cursor state (pending_block: block_start_ms, input_path).

4. **Test**: add focused tests for book checkpoint write/load, verified-missing persistence, and kind-aware recovery.

### Hard exclusions

- No changes to trades processing path
- No content processing for book_updates blocks (B3's loadBookSnapshot already handles that within trades path)
- No feature computation for book_updates standalone path
- No rollup, Receiver, cron, Gateway, production data

## Acceptance gates

- processBlocksNonTrade writes checkpoint on EOF horizon-valid exit
- processBlocksNonTrade loads checkpoint on entry for cursor resume
- Verified-missing book blocks are recorded in manifest
- recovery.mjs handles book_updates checkpoint
- npm test passes
- Independent reviewer >=95
