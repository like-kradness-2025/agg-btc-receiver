# Handoff: Burst Reducer DS4 Flash Implementation (2026-07-10)

## Status
**Design / implementation plan review gate: PASS (97/100).**

This is a documentation handoff only. **No production reducer code was implemented in this work.**

## Canonical documents

Implementation must follow all three documents together:

1. `docs/specs/specify-2026-07-09-burst-features.md`
2. `docs/specs/design-2026-07-10-burst-reducer.md`
3. `docs/specs/plan-2026-07-10-burst-reducer.md`

The plan is the task-level implementation source. The feature specification is the schema/semantic authority when wording differs.

## Locked architecture

- Receiver remains **receive + save only**. Do not alter Receiver and do not delete/move raw inputs.
- Reducer reads raw `trades` and writes only beneath `data/derived/burst_features_v1/` (or the explicit validation-only `--output-root`).
- Output is 30-row 1s JSONL block shards.
- One-block lag is mandatory:
  1. validate candidate N+1 raw input,
  2. validate finalized N's required #12 auxiliary coverage,
  3. on either failure quarantine only and preserve detector/checkpoint/manifest pre-feed state,
  4. capture pre-feed N+1 detector state,
  5. feed all N+1 trades,
  6. compute N for the first time,
  7. commit N once while persisting N+1 as pending.
- EOF is the only path that permits `nextPendingBlock=null`.
- Commit is staged-file → intent manifest → same staged file atomic rename → checkpoint → committed manifest, with file and directory fsync requirements.
- `BurstBuilder` private state is accessible only through `burst-state-codec.mjs` in the new reducer implementation.

## Feature contracts

- 22 logical feature columns; physical JSON row has 25 top-level keys: `ts`, `market`, 22 features, `_quality`.
- P1 values: #13 `null`; #14 `0`; #15–#22 `0`.
- #12 denominator per 1s row is `[second_ts - 30000, second_ts)`.
- Required agg source columns: `ts`, `volume`, `vwap`; missing/incomplete coverage is E007 and must fail closed even for zero-burst seconds.
- A pending block may have `auxiliary_input_hashes: {}`. Actual hashes are obtained only when that block is finalized and are recorded in the manifest.

## Implementation order

Use plan tasks in order. Do not skip their tests:

1. fixtures and schema;
2. validator/scanner;
3. detector + `burst-state-codec`;
4. FeatureComputer and agg reader;
5. OutputCommitter;
6. one-block-lag pipeline/recovery;
7. CLI;
8. golden tests;
9. isolated real-data validation.

## Verification required before claiming implementation complete

- All `node --test test/burst-reducer/*.test.mjs` tests pass.
- `git diff --check -- docs/specs` is clean for the documentation handoff.
- Run Task 11 only with a validation-only output root under `data/derived/burst_features_v1_validation/<run_id>/`; never delete or overwrite canonical derived output for validation.
- Verify two consecutive raw blocks dynamically; do not hardcode a date/time.
- Confirm normal commit, EOF commit, crash-after-data-rename recovery, N+1 invalid preservation, and E007 auxiliary failure paths.

## Review evidence

- Independent delegated review: **97/100 PASS**, P0 none, P1 none.
- The review originally requested gpt-5.6-terra. Codex MCP could not run that model because the installed Codex client rejected it as too old; the delivered independent review was actually run by `deepseek-v4-pro`, not terra.
- Parent static verification after review: no `tmpOutput`, no legacy ISO-start variable marker, no `detector._market`; market getter and pending empty-aux contract present; `git diff --check -- docs/specs` exit 0.

## Scope note

The working tree also contains unrelated pre-existing documentation changes under `docs/specs/` dated 2026-07-08. Do not mix them into a reducer implementation change without separate review.
