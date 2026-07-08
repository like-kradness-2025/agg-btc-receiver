# Burst Implementation Bridge Plan

**Status:** Complete through slice 4 closeout  
**Track:** burst feature contract sequence  
**Prerequisite:** `docs/burst-spec-review-gate.md` = PASS  
**Governing contracts:**
- `docs/burst-formation-contract.md`
- `docs/same-price-burst-contract.md`
- `docs/multilevel-burst-contract.md`
- `docs/burst-summary-contract.md`
- `docs/burst-book-validation-contract.md`
- `docs/1s-burst-feature-schema.md`

---

## 1. Purpose

This document freezes the minimum implementation order after spec PASS.

It answers:
- what existing code is reused
- what exact responsibilities are split where
- what the first vertical slice is
- what verification package is required before moving to the next slice

This is not a new design contract. It is the implementation bridge from fixed burst specs to code.

---

## 2. Core reuse decision

**Reuse the existing accumulator path. Do not rewrite the pipeline.**

Implementation should adapt existing aggregation/replay flow so burst features are added as derived 1s columns.

Default posture:
- reuse existing trade replay ordering
- reuse existing 1s bucketing
- reuse existing best bid / ask tracking and existing at-touch / through / depletion / replenish state
- add burst-specific formation and summary state as a constrained extension

Do not create a parallel standalone burst pipeline unless the existing accumulator proves structurally incapable.

---

## 3. Responsibility split

### 3.1 Replay / feed ordering layer
Responsibility:
- preserve deterministic trade ordering (`ts`, then stable arrival order)
- feed trades/depth into the existing accumulator path

Likely reuse target:
- current replay / batch runner and feed loop

### 3.2 Burst formation layer
Responsibility:
- maintain Phase 1 open-burst state
- close bursts on market/side/gap/duration rules
- expose closed burst primitives for downstream summaries

Must not:
- depend on book state
- redefine bucket-local run/gap features

### 3.3 Same-price / multilevel characterization layer
Responsibility:
- derive same-price sub-runs inside already-formed bursts
- classify multilevel bursts
- compute/attach multilevel span using configured `tick_size`

Must not:
- re-form bursts
- use loose proximity bands

### 3.4 1s summary layer
Responsibility:
- compute overlap-based burst summaries
- compute bucket-local run/flip/gap features
- emit nullable semantics exactly as frozen in schema

### 3.5 Book-validation layer
Responsibility:
- annotate bucket-local burst-associated prints with at-touch / through classification when book state is classifiable
- accumulate ratio denominators/numerators
- accumulate depletion / replenish co-occurrence counts

Must not:
- feed back into burst formation
- silently coerce NULL to 0

### 3.6 Writer / output layer
Responsibility:
- append the new burst columns to the existing 1s output row shape
- preserve nullability at storage layer
- keep current output keying (`ts`, `market`) unchanged

---

## 4. Initial vertical slice

### Recommended first slice
**trade-only burst spine:**

`deterministic trade replay -> Phase 1 burst formation -> same-price + multilevel characterization -> overlap-based 1s burst summaries -> output rows`

Included in slice 1:
- `burst_count_1s`
- `max_burst_notional_1s`
- `max_burst_prints_1s`
- `max_burst_duration_ms_1s`
- `same_price_burst_count_1s`
- `same_price_burst_max_len_1s`
- `same_price_burst_notional_1s`
- `multilevel_burst_count_1s`
- `multilevel_burst_max_span_ticks_1s`
- `multilevel_burst_notional_1s`
- `buy_burst_notional_1s`
- `sell_burst_notional_1s`
- `burst_delta_notional_1s`
- `largest_burst_share_notional_1s`

Explicitly deferred from slice 1:
- `max_same_side_run_prints_1s`
- `side_flip_count_1s`
- `same_side_gap_ms_min_1s`
- `same_side_gap_ms_p25_1s`
- all book-aware validation fields

Rationale:
- smallest executable path using already-fixed burst contracts
- proves burst formation, overlap semantics, and schema plumbing first
- keeps book-aware complexity out of the first end-to-end slice

**Implementation status:** done

---

## 5. Slice sequence after the spine

### Slice 2 — bucket-local print-structure features
Add:
- `max_same_side_run_prints_1s`
- `side_flip_count_1s`
- `same_side_gap_ms_min_1s`
- `same_side_gap_ms_p25_1s`

**Implementation status:** done

### Slice 3 — book-aware validation features
Add:
- `burst_at_touch_ratio_1s`
- `burst_through_ratio_1s`
- `burst_depletion_count_1s`
- `burst_replenish_after_touch_count_1s`

**Implementation status:** done

### Slice 4 — cleanup / reconciliation
Add:
- explicit reconciliation note with pre-existing v2 trade-level at-touch/through fields
- any sink-level nullability fixes or adapter cleanup

**Implementation status:** closed via `docs/burst-slice4-reconciliation-note.md`; no sink-level nullability fix required from current evidence

---

## 6. Verification package required per slice

Every slice must provide all four:
1. fixture or deterministic replay input
2. command that runs the slice end-to-end
3. emitted row sample or row-count evidence
4. deterministic test that asserts the expected output shape/values

### Slice 1 minimum verification
Must prove:
- deterministic burst formation on a fixed trade sequence
- cross-second overlap behavior for burst-derived summaries
- same-price and multilevel coexistence on one controlled fixture
- `multilevel_burst_max_span_ticks_1s` uses configured `tick_size`

**Observed evidence:** covered in `test/feature-accumulator-burst-slice1.test.mjs`

### Slice 2 minimum verification
Must prove:
- bucket-local run/gap features do not leak across second boundaries
- empty same-side gap sample emits NULL

**Observed evidence:** covered in `test/feature-accumulator-burst-slice1.test.mjs`

### Slice 3 minimum verification
Must prove:
- classified-only denominator behavior for book-aware ratios
- `NULL` vs `0` behavior for book-aware count fields
- at-touch / through mutual exclusivity on classified prints

**Observed evidence:** covered in `test/feature-accumulator-burst-slice1.test.mjs` and independent review follow-up fixes

### Slice 4 minimum verification
Must prove:
- legacy global trade/book fields remain distinct from new burst validation fields
- reconciliation is documented explicitly
- no sink currently coerces nullable burst validation fields to zero

**Observed evidence:** `docs/burst-slice4-reconciliation-note.md`, schema/code reconciliation pass, and current JSONL row assembly preserving `null`

---

## 7. Guardrails

- Do not jump from spec PASS to full book-aware implementation in one change.
- Do not rewrite existing accumulator math if an additive state extension is sufficient.
- Do not collapse overlap-based semantics and bucket-local semantics into one generic helper if it blurs contract behavior.
- Do not silently materialize nullable fields as zeroes in sinks lacking null support.
- Do not implement the alternate rejected multilevel span semantics.

---

## 8. First coding target

The first implementation target should be narrow enough to be testable with one deterministic fixture and one end-to-end replay command.

Recommended first target:
- add Phase 1 burst formation state and close-burst primitives
- emit the 14 overlap-based burst fields from slice 1 into the existing 1s row path
- add one focused deterministic test fixture covering:
  - same-side short-gap burst
  - same-price sub-run
  - multilevel burst with configured `tick_size`
  - second-boundary overlap duplication semantics

**Actual outcome:** delivered as planned; later expanded in the same test file to cover slices 2 and 3.

---

## 9. Exit check

This bridge plan is acceptable only if:
- it clearly reuses the existing accumulator path
- the first vertical slice is smaller than the full burst feature set
- verification is required per slice, not deferred to the end
- the implementation order respects the frozen contracts and review-gate PASS

Current status:
- slices 1–4: complete / closed
- implementation bridge: complete
- next work should happen outside this bridge plan (integration validation, replay validation, downstream consumer adoption, or broader end-to-end data verification)
