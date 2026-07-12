# TFP Phase B6 Plan: Frozen Inventory Kind Separation

- Date: 2026-07-12
- Branch: `v2`
- Prerequisite: B5 commit `b43c486`, independent review 98/100 PASS
- Scope: frozen inventory validation and kind separation only

## Goal

Prove and harden that `trades` and `book_updates` frozen inventory entries are validated and looked up independently, with no cross-kind fallback or collision.

## In scope

1. Direct tests for path-kind mismatch rejection.
2. Multi-market + multi-kind inventory cross-reference tests.
3. Direct tests that book_updates checkpoint corruption is reported as `corrupt-checkpoint`.
4. Regression tests for kind-qualified checkpoint lookup and no trades fallback.

## Out of scope

- Receiver/raw ingestion
- rollup
- quarantine semantics changes
- board columns
- output schema
- cron/Gateway

## Acceptance

- Focused B6 tests GREEN.
- Existing horizon/inventory tests GREEN.
- Full `npm test` GREEN.
- `node --check` and `git diff --check` PASS.
- Independent reviewer score >=95.
