# Burst Spec Review Gate

**Status:** PASS (after blocker fix)  
**Track:** burst feature contract sequence  
**Reviewed on:** 2026-07-04

---

## Reviewed doc set

- `docs/burst-feature-contract-plan.md`
- `docs/burst-formation-contract.md`
- `docs/same-price-burst-contract.md`
- `docs/multilevel-burst-contract.md`
- `docs/burst-summary-contract.md`
- `docs/burst-book-validation-contract.md`
- `docs/1s-burst-feature-schema.md`

---

## What is now consistent

1. **Formation vs downstream characterization is cleanly separated**
   - Phase 1 burst formation is trade-stream only.
   - same-price, multilevel, summary, and book-aware metrics are all downstream layers.

2. **Overlap semantics are fixed for burst-derived 1s summaries**
   - burst / same-price / multilevel / directional / concentration overlap-based summaries all use the same second-overlap model.

3. **Bucket-local semantics are fixed where needed**
   - run / flip / gap features are explicitly intra-second.
   - book-aware ratio and event-count features are explicitly bucket-local.

4. **NULL vs zero is mostly fixed**
   - trade-only count/sum/max fields default to `0`
   - empty gap samples emit `NULL`
   - book-aware ratio fields emit `NULL` on empty classified denominator
   - book-aware count fields emit `NULL` when relevant book observation is unavailable

5. **Non-claims are consistent across docs**
   - no contract claims true parent-order recovery
   - no contract treats validation fields as formation inputs

---

## Resolved blocker

### B1. Multilevel `span_ticks` contract freeze — RESOLVED
**Resolution applied:**
- `docs/multilevel-burst-contract.md` now defines `span_ticks` as `(burst_max_price - burst_min_price) / tick_size`
- `tick_size` is configured market/venue metadata, not inferred from the burst tape
- `docs/1s-burst-feature-schema.md` was updated to inherit the frozen meaning

**Result:**
The burst contract set no longer has competing definitions for `multilevel_burst_max_span_ticks_1s`.

---

## Non-blocking observations

1. `burst-book-validation-contract.md` uses bucket-local burst-associated prints for ratio denominators. This is internally coherent and should remain separate from overlap-style burst summaries.
2. `largest_burst_share_notional_1s` denominator is clearly fixed and does not currently conflict with any other document.
3. same-price and multilevel coexistence is adequately documented and not currently contradictory.

---

## Gate decision

**Decision: PASS**

Reason:
- the previously blocking multilevel `span_ticks` ambiguity has been removed
- the remaining observations are non-blocking and can be handled during implementation or follow-up cleanup without changing core contract meaning

---

## Follow-up tasks (non-blocking)

1. Add an explicit normative section for the four core burst 1s fields (`burst_count_1s`, `max_burst_notional_1s`, `max_burst_prints_1s`, `max_burst_duration_ms_1s`) in either Phase 1 or Phase 4 docs to remove any remaining inference burden on implementers.
2. Add a reconciliation note between pre-existing v2 trade-level at-touch/through columns and the new burst-level validation columns.
3. Clean stale resolved items out of `docs/worklog/open-questions.md` and add Phase 5 decisions to `docs/worklog/decision-log.md`.
