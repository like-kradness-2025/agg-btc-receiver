# Burst Formation Contract

**Status:** Draft v1  
**Track:** burst feature contract sequence  
**Governing plan:** `docs/burst-feature-contract-plan.md`

---

## 1. Purpose

This contract defines what counts as one **burst** for the burst-feature family.

A burst is intended to represent:

> a short-horizon same-side aggressive flow burst inferred from public trade tape

A burst is **not** defined as:

> a true same-parent order identity

This distinction is normative and must be preserved in code comments, docs, and downstream interpretation.

---

## 2. Inputs

Burst formation uses **trade stream only**.

Required trade fields:
- `market`
- `side`
- `ts`
- `price`
- `qty`
- `tradeId` (optional for uniqueness / diagnostics, not required for burst semantics)

Burst formation **must not** depend on:
- book state
- at-touch / through classification
- depletion / replenish signals
- inferred order identity

Book-aware metrics are handled later by `docs/burst-book-validation-contract.md`.

---

## 3. Canonical definition

A burst is a maximal contiguous run of trade prints satisfying all of the following:

1. same `market`
2. same `side`
3. adjacent print-to-print time gap does not exceed `gap_threshold_ms`
4. total burst duration does not exceed `max_burst_duration_ms`

The burst is formed on the trade stream in timestamp order.

---

## 4. Trade ordering

### 4.1 Primary ordering
Burst formation must process trades in ascending order of:
1. `ts`
2. stable arrival order within equal `ts`

### 4.2 Equal-timestamp prints
When multiple trades have the same `ts`, they must preserve the deterministic order produced by the replay input stream.

The implementation must not re-sort equal-`ts` trades using price, qty, or side.

### 4.3 Scope of formation
Burst formation must run on the **full ordered trade stream first**, then 1s summaries are derived from formed bursts.

Burst formation must **not** be recomputed independently inside each 1s bucket.

Rationale:
- recomputing inside each 1s bucket would make burst semantics depend on bucket boundaries
- global stream-first formation preserves deterministic burst identity before 1s collapse

---

## 5. Formation state

At any point, the burst builder maintains at most one open burst per `market`.

An open burst state must include at minimum:
- `market`
- `side`
- `start_ts`
- `end_ts`
- `print_count`
- `sum_qty`
- `sum_notional`
- `first_price`
- `last_price`
- `min_price`
- `max_price`

Additional internal fields are allowed if they do not change normative semantics.

---

## 6. Split rules

A currently open burst must be closed before the next trade is appended if any of the following is true.

### 6.1 Market change
If `next.market != open.market`, close the open burst.

### 6.2 Side change
If `next.side != open.side`, close the open burst.

### 6.3 Gap threshold exceeded
Let:
- `gap_ms = next.ts - open.end_ts`

If `gap_ms > gap_threshold_ms`, close the open burst.

### 6.4 Max duration exceeded
Let the candidate extended duration be:
- `candidate_duration_ms = next.ts - open.start_ts`

If `candidate_duration_ms > max_burst_duration_ms`, close the open burst before appending `next`.

### 6.5 Non-monotone input
If a trade arrives with `next.ts < open.end_ts`, this violates ordered-input assumptions.

For v1 contract, replay input must be sorted before formation. Non-monotone input handling belongs to upstream replay guarantees and is out of scope for burst semantics.

---

## 7. No split by price continuity

Price continuity must **not** be a split rule in v1.

Specifically, a burst must not be split merely because:
- price changed
- price repeated
- price moved by multiple ticks

Rationale:
- same aggressive sweep can span multiple price levels
- same-price repetition and multilevel sweep are downstream burst characterizations, not formation rules

---

## 8. Threshold scope

### 8.1 `gap_threshold_ms`
For v1, `gap_threshold_ms` is defined **per venue**.

Interpretation:
- all markets from the same venue share the same gap threshold in v1
- per-market tuning is deferred
- one global threshold across all venues is rejected for v1 because tape fragmentation and timestamp behavior differ materially by venue

### 8.2 `max_burst_duration_ms`
For v1, `max_burst_duration_ms` is global across venues unless later evidence forces venue overrides.

Rationale:
- this rule exists primarily to prevent over-merging long same-side sequences
- a common upper cap is simpler and more interpretable than venue-specific caps at first pass

### 8.3 Threshold values
This contract defines the semantics and scope of thresholds, not their final numeric values.

Numeric values must be fixed in implementation/config documentation later and must respect this scope:
- `gap_threshold_ms`: per venue
- `max_burst_duration_ms`: global default

---

## 9. Burst completion semantics

A burst is considered closed when any split rule triggers or when the trade stream ends.

Once closed, its summary becomes immutable for downstream 1s aggregation.

The implementation must not merge two already closed bursts retroactively.

---

## 10. 1-second aggregation semantics

### 10.1 Formation first, summarize second
1s burst features must be computed from already-formed bursts.

### 10.2 Overlap rule
A burst contributes to every 1s bucket whose interval overlaps the burst interval.

Use half-open second buckets:
- second bucket = `[bucket_start_ts, bucket_start_ts + 1000)`

Use closed burst interval over observed prints:
- burst interval = `[start_ts, end_ts]`

A burst overlaps a bucket if:
- `start_ts < bucket_end_ts`
- and `end_ts >= bucket_start_ts`

### 10.3 Per-second summaries from overlapping bursts
For v1 summary semantics:
- a burst may contribute to more than one 1s row if it crosses a second boundary
- the same burst identity is preserved conceptually; per-second features are overlap-based summaries, not re-formed sub-bursts

### 10.4 Consequence
This means counts like `burst_count_1s` are counts of bursts overlapping the second, not necessarily bursts that both started and ended inside the second.

This is intentional and must be documented in downstream schema notes.

---

## 11. Required derived burst primitives

Every closed burst must make available at least these primitives for downstream feature computation:
- `burst_start_ts`
- `burst_end_ts`
- `burst_duration_ms`
- `burst_side`
- `burst_print_count`
- `burst_qty`
- `burst_notional`
- `burst_first_price`
- `burst_last_price`
- `burst_min_price`
- `burst_max_price`

Later contracts may derive same-price, multilevel, directional, concentration, and book-validation features from these primitives plus additional annotations.

---

## 12. Null / zero semantics at formation layer

Formation itself does not emit `NULL` metrics.

A trade either participates in a burst or starts a new burst.

Null / zero policy applies later at the 1s feature summary layer, not at burst formation.

---

## 13. Explicit non-claims

This contract does **not** claim that a burst equals:
- one order
- one participant
- one parent execution algorithm

A burst is only a deterministic grouping of short-horizon same-side public prints.

---

## 14. Implementation constraints

Implementations following this contract must:
- process trades deterministically
- avoid price-based split logic in v1
- avoid book-dependent formation in v1
- preserve cross-second burst identity for downstream summaries

Implementations must not:
- rename burst features as order-size features
- recompute burst identity independently per 1s bucket
- silently switch threshold scope from per-venue to global or per-market

---

## 15. Open follow-ups handed to later contracts

Deferred to later contracts:
- exact same-price semantics
- exact multilevel sweep semantics
- formulas for directional / concentration / timing summaries
- at-touch / through / depletion / replenish attachment to bursts
- exact null/zero semantics for book-aware fields
- final threshold numeric values

---

## 16. Exit check

This contract is acceptable only if all of the following are true:
- burst semantics are clearly separated from parent-order identity
- split rules are deterministic
- price is not used as a formation split rule
- threshold scope is explicit
- 1s summaries are explicitly defined as derived from stream-formed bursts, not bucket-local reformation
