# TFP Phase B Tasks

## B0 Reconnaissance

- [ ] Read P0-0 spec and identify exact envelope/state/join contracts.
- [ ] Trace connector → raw `book_updates` event paths with real source lines.
- [ ] Trace `tfp.mjs` → `pipeline.mjs` → feature/commit/recovery callsites.
- [ ] Confirm existing independent verifier cases and missing RED cases.
- [ ] Record researcher evidence and parent confirmations.

## B1 Adapter

- [ ] Add pure adapter module at an existing connector-compatible path.
- [ ] Add RED fixtures before production implementation.
- [ ] Cover valid snapshot/update, malformed, missing sequence, numeric fields, and source/schema metadata.
- [ ] Parent runs focused + full tests.
- [ ] Independent reviewer score >=95; otherwise FIX and repeat.

## B2 State machine

- [ ] Add pure state machine only; no file writes.
- [ ] Cover strict anchor, duplicate, gap, malformed, crossed book, and fail-closed transitions.
- [ ] Compare against independent verifier, not production-derived expected values.
- [ ] Parent verification + reviewer >=95.

## B3 Join/wiring

- [ ] Add same-block join with trade-only regression fixture.
- [ ] Preserve #1–#12 and existing placeholder columns.
- [ ] Prove book quarantine behavior and cursor/commit effects.
- [ ] Parent verification + reviewer >=95.

## B4–B6 Contract completion

- [ ] Add board candidate columns under new names only.
- [ ] Implement kind-aware quarantine/checkpoint/recovery.
- [ ] Test inventory separation and bounded state.
- [ ] Parent verification + reviewer >=95.

## B7 Final gate

- [ ] Focused tests PASS.
- [ ] Full `npm test` PASS.
- [ ] Syntax/diff/static negative probes PASS.
- [ ] Isolated non-production output-root probe PASS.
- [ ] Independent reviewer >=95.
- [ ] Commit/push only after all evidence is present.
