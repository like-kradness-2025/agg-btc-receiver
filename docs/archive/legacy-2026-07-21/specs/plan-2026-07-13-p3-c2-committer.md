# P3-C2 Implementation Plan: 5min Committer + Recovery + Consumer

- Date: 2026-07-13
- Base: P3-C1 commit `3cf2fdc`, P3-C2 contract approved.
- Scope: dedicated 5min committer, recovery, consumer module.
- Allowed files: `lib/burst-reducer/rollup-5min-committer.mjs`, `test/burst-reducer/rollup-5min-committer.test.mjs`, `lib/burst-reducer/consumer-5min.mjs`, `test/burst-reducer/consumer-5min.test.mjs`, updates to `docs/specs/plan-2026-07-13-p3-c2-persistence-freeze.md`, `docs/specs/tasks-2026-07-13-p3-c2-persistence-freeze.md`, `docs/worklog/2026-07-13-p3-c2-persistence-freeze.md`.
- Pipeline wiring, 30s manifest changes, Receiver/raw/cron/Gateway: **excluded**.

## Committer contract

- `<derivedRoot>/features_5min/<market>/<date>/<time>.jsonl`
- Staging: `<...>/.staging/<runId>/<time>.jsonl`
- Collision key: `burst_features_5min_v1:<market>:<window_start_ms>:<source_window_hash>`
- Hash conflict → quarantine, no silent overwrite
- Atomic rename + fsync directory after each commit
- Verify renamed output hash against staged hash

## Recovery contract

- Orphan `.staging/` scan at startup
- Source-referenced reconciliation (scan committed 30s manifest for complete windows)
- intent → committed promote
- Output hash mismatch → quarantine

## Consumer contract

- Manifest committed-only reader
- Row validation (10 inputs, coverage, finalized, no missing)
- Range query by `(market, from_ms, to_ms)`
- Hash integrity check
- Diagnostic status (blocked/quarantined → no data row)

## State isolation

- Never read/write 1s/30s manifest/checkpoint/output
- No promotion of legacy cross-market fields

## Tests

- Committer: normal commit, staging crash, hash conflict quarantine, recovery promote, idempotency, empty-valid, missing reject
- Consumer: committed-only filter, row validation, range query, hash check, status diagnostics
