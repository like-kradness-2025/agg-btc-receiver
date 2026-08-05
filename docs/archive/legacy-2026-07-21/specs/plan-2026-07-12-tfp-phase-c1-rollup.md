# TFP Phase C1 Plan: Pure 30s Rollup

- Date: 2026-07-12
- Branch: `v2`
- Base: Phase B complete, final review 100/100, HEAD `06f2069`
- PDD mode: Kanban + delegate_task + parent verification + independent 95-point gate

## Goal

Implement a pure, testable 30-second rollup from complete `features_1s` windows without wiring production persistence yet.

## In scope

- `lib/burst-reducer/rollup.mjs`: 30s pure aggregation only
- `test/burst-reducer/rollup.test.mjs`: independent fixture/oracle tests
- Optional 30s field constants only if required by the contract

## Contract

- Input must contain exactly one aligned row per second for a complete 30s window.
- Duplicate, missing, mixed-market, out-of-order, or unaligned input is rejected/fail-closed.
- Rollup output uses explicit 30s names and operators; it does not rename overlap exposure into direct totals.
- `#13/#14` and `#15-#22` remain excluded/placeholders until P4.
- Empty-valid and missing-input are distinct; quality provenance records coverage and source window.
- Existing `features_1s` rows and trade-only contracts are untouched.

## Explicitly out of scope

- 5min aggregation
- pipeline wiring
- output-committer persistence
- manifest/checkpoint/recovery
- Receiver/raw, cron, Gateway
- #13/#14 activation or #15-#22 real values

## Acceptance

- Focused C1 tests GREEN.
- Full existing suite GREEN.
- Node syntax and diff checks PASS.
- Independent verifier uses hand-computed oracles, not production output as expected data.
- Independent review >=95.
