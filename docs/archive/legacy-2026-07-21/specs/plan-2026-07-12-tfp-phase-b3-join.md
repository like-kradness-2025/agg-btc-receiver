# TFP Phase B3 Plan: Same-block Trade + Book Join

- Date: 2026-07-12
- Branch: `v2`
- Prerequisite: B2 commit `612e0e3`, independent review 98/100 PASS
- Mode: Kanban PDD / `delegate_task`; profileSession prohibited

## Scope

B3 wires book state lookup into the existing trade block processing unit without promoting board candidates. The canonical lookup rule is strict `event_ts_ms < anchor_ts_ms`.

### In scope

1. Read/validate the `book_updates` input paired with the same block identity as the trade input.
2. Build a pure `bookSnapshotAt(anchor_ts_ms)` callback/value from `BookStateMachine.stateAt()`.
3. Pass the book snapshot into the existing 1s feature computation path as an internal optional contract.
4. Preserve trade-only output and existing #13/#14 placeholder semantics.
5. Add production-pipeline join tests and an independent verifier that computes the join/anchor expectation without importing production book replay code.

### Out of scope

- `board_*` candidate columns and schema changes (B4)
- populating or changing existing #13/#14 fields
- quarantine/checkpoint/manifest persistence and cursor policy (B5)
- inventory kind separation (B6)
- rollup, Receiver, cron, Gateway, production data

## Acceptance gates

- Same-block trade+book input is joined by explicit block identity, never by wall clock.
- Anchor equality is excluded; only `event_ts_ms < anchor_ts_ms` contributes.
- Missing/malformed/gap/crossed book yields unavailable/quarantine metadata without changing trade-only #1-#12 semantics.
- Trade-only path remains compatible when no book input exists.
- No new board fields are introduced in B3.
- RED→GREEN focused tests, existing regression tests, `npm test`, `node --check`, `git diff --check`.
- Parent re-reads absolute-path files; independent reviewer must score >=95.
