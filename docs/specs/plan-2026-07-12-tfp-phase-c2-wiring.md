# TFP Phase C2 Plan: 30s Rollup Wiring and Isolated Output

- Date: 2026-07-12
- Base: C1 commit `48d9997`, independent review 100/100
- PDD: Kanban + delegate_task + parent verification + independent 95 gate

## Goal

Wire the pure C1 `aggregate30s` into the trade pipeline only after durable 1s commit, writing an isolated `features_30s` shard namespace without changing 1s artifacts.

## In scope

- `pipeline.mjs`: call 30s path after successful 1s commit for complete 30-row windows.
- New 30s-only writer/committer module with explicit `features_30s` root and separate manifest/checkpoint namespace.
- Tests for normal, gap/missing-quality, EOF partial, restart/idempotency, and isolated output-root.

## Contract

- 1s commit is authoritative and must succeed before 30s output is written.
- 30s output is under `<derivedRoot>/features_30s`; it never nests `features_1s`.
- 30s manifest/checkpoint are separate from 1s market state.
- C1 fail-closed rules remain active; partial/gap/missing/EOF windows do not produce 30s rows.
- `--output-root` is the only root override; default production root is not touched by isolated tests.
- book_updates simplified path remains excluded.

## Out of scope

- 5min aggregation
- Receiver/raw, cron, Gateway
- changing 1s schema/output/checkpoint semantics
- changing #13/#14 or #15-#22
- consumer/dashboard contract

## Gate

Focused C2 tests, relevant existing tests, full npm test, syntax/diff checks, isolated-root probe with byte-identical 1s snapshot, independent 95-point review, then commit/push.
