# TFP Phase B Plan: Book Contract → Production Wiring

- Date: 2026-07-12
- Branch: `v2`
- Prerequisite: Phase A Gate A 95/100 PASS, commit `a750ddd`
- Mode: PDD strict / delegate_task standard
- Gate: each sub-phase requires parent verification and independent review >=95

## Goal

Integrate the P0-0 book contract into the production TFP pipeline without changing Receiver raw-only behavior, trade-only feature columns, or Phase C rollup scope.

## Non-goals

- No Receiver aggregation or raw-data rewrite/deletion.
- No cron schedule, Gateway, or production output-root execution.
- No rollup implementation/wiring (Phase C).
- Never overwrite existing `#13=null` and `#14=0` placeholders.
- No broad refactor of trade-only pipeline.

## Phase gates

### B0 — reconnaissance and contract lock

Inputs: P0-0 book spec, connector event schema, current `replay-book-state.mjs`, `pipeline.mjs`, existing fixtures/tests.

Output: exact file map, event/envelope mapping, join boundary, and RED tests.

Gate: parent confirms every path/import/callsite; no production edit before B0 evidence.

### B1 — canonical adapter

Implement only connector depth event → `book_updates_v1` envelope:
`event_ts_ms`, `seq`, `prev_seq`, `source`, `schema_version`, bids/asks, market.

Tests: valid snapshot/update, missing fields, numeric normalization, sequence metadata, malformed input fail-closed.

Gate: adapter unit tests + independent fixture verifier + reviewer >=95.

### B2 — BookStateMachine

Implement production state transitions for valid sequence, duplicate, gap, malformed, crossed book, and strict `< anchor` lookup. Keep pure state machine separate from I/O.

Gate: state-machine tests + P0-0 independent verifier comparison + reviewer >=95.

### B3 — pipeline join/wiring

Join same-block trades and book updates; produce `bookSnapshotAt(secondTs)` without changing trade-only #1–#12 behavior. Book quarantine must not silently commit trade output when the contract forbids it.

Gate: integration fixtures for same block, missing book, gap, and trade-only regression + reviewer >=95.

### B4 — board candidate columns

Add only new board-candidate names (e.g. `board_top_depth_ratio`, `board_mid_move_bps`, `_quality` fields). Static and runtime assertions prove `#13=null`, `#14=0`, and #15–#22 placeholders remain unchanged.

### B5 — quarantine and kind-aware recovery

Implement book-specific verified-missing, sequence-gap, malformed, crossed-book quarantine and `kind='book_updates'` checkpoint/manifest/recovery boundedness. `ASSUMED_EMPTY_GAP` remains trade-only.

### B6 — inventory separation

Prove trades/book frozen inventories cannot cross-match. Add negative tests for kind mismatch, market mismatch, block mismatch, and duplicate entries.

### B7 — final verification

Run focused tests, full npm test, node/bash syntax checks, independent verifier, isolated output-root probe (non-production), and final independent reviewer >=95. Only then prepare commit/push.

## Rollback

Each sub-phase is a separate commit. On failed gate, revert only the current uncommitted phase; never reset Phase A commit. Do not touch untracked Phase C `lib/burst-reducer/rollup.mjs`.
