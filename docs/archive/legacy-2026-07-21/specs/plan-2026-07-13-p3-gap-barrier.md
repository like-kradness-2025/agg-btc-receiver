# PDD Plan: 5min Gap Barrier / Fail-Soft Recovery

- Date: 2026-07-13
- Branch: `v2`
- Base: `1f61603`
- Mode: PDD, parent verification, independent 95-point review

## Problem

1s and 30s outputs are committed successfully, but the inline 5min buffer throws `E_FIVEMIN_UNALIGNED_BUFFER` when a long input gap occurs. This aborts the whole run and prevents later complete windows from being processed.

## Goal

Continue 1s/30s/5min processing across data gaps without fabricating missing data.

## Contract

- Complete 5min windows only are emitted to `features_5min`.
- Missing/partial windows are never zero-filled by default.
- A gap barrier clears the incomplete 5min buffer and resumes at the next 5min-aligned complete window.
- Gap ranges are persisted in a dedicated market manifest section (`gaps`), with source layer, start/end, reason, and recovery status.
- Existing valid 5min rows retain `coverage=1`, `has_missing_input=false`, and `finalized=true`.
- 1s/30s commits are not rolled back because 5min cannot be formed.
- Recovery groups source rows by expected 5min timestamp, not arbitrary groups of ten.
- Consumer/status can distinguish missing windows from committed windows.

## Implementation slices

1. Add gap-barrier state to `pipeline.mjs`: detect non-consecutive buffer, record gap, reset buffer, continue without throw.
2. Add append-only/idempotent gap record support to 5min manifest committer.
3. Change `reconcileCommitted30s()` to timestamp-bucket grouping; skip incomplete buckets and record gaps.
4. Add focused tests: gap before 5min boundary, gap after complete window, recovery across gap, no zero-fill, later window commits.
5. Run full suite, isolated live conversion, inspect raw/output/gap manifest.
6. Independent adversarial review >=95; fix/re-review if needed.

## Out of scope

- automatic zero-fill / `verified-empty` policy
- changing Receiver/raw data
- changing 1s/30s quality semantics
- cron/Gateway changes
- cross-market aggregation
