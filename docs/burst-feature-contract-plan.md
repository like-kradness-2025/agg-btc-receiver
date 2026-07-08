# Burst Feature Contract Plan

> **For Hermes:** Follow this plan sequentially. Do not skip ahead. Close one contract at a time, save it to docs, then spot-review for consistency before moving to the next contract.

**Goal:** Add the full burst / cluster feature family to the 1s pipeline without drifting in meaning, by fixing semantics first and only then implementing.

**Architecture:** Treat this as a contract-first design track. We are **not** trying to prove same parent order. We are designing **aggressive flow burst characterization** from public trade data (`price`, `qty`, `side`, `ts`, `tradeId`) plus optional book-validation context. All features are grouped by semantic role and specified one contract at a time.

**Tech Stack:** Node.js receiver pipeline, `lib/feature-accumulator.mjs`, JSONL 1s features, existing raw trade/depth/snapshot streams.

---

## Fixed scope

The following feature families are in scope and assumed to be added unless explicitly removed later by review:

### A. Burst core
- `burst_count_1s`
- `max_burst_notional_1s`
- `max_burst_prints_1s`
- `max_burst_duration_ms_1s`

### B. Same-price burst
- `same_price_burst_count_1s`
- `same_price_burst_max_len_1s`
- `same_price_burst_notional_1s`

### C. Multilevel sweep
- `multilevel_burst_count_1s`
- `multilevel_burst_max_span_ticks_1s`
- `multilevel_burst_notional_1s`

### D. Directional burst
- `buy_burst_notional_1s`
- `sell_burst_notional_1s`
- `burst_delta_notional_1s`

### E. Concentration / run structure
- `largest_burst_share_notional_1s`
- `max_same_side_run_prints_1s`
- `side_flip_count_1s`

### F. Gap / timing texture
- `same_side_gap_ms_min_1s`
- `same_side_gap_ms_p25_1s`

### G. Book-aware validation
- `burst_at_touch_ratio_1s`
- `burst_through_ratio_1s`
- `burst_depletion_count_1s`
- `burst_replenish_after_touch_count_1s`

---

## Non-goals

- Do **not** define these as true parent-order reconstruction.
- Do **not** use `order_*` naming.
- Do **not** let book-validation rules leak into burst formation rules unless explicitly decided later.
- Do **not** optimize per-venue thresholds before base semantics are fixed.
- Do **not** start implementation before the contract sequence below is closed.

---

## Canonical interpretation

All burst features are intended to describe:

> short-horizon same-side aggressive flow bursts inferred from public trade tape

not:

> true same-parent order identity

Use `burst` / `cluster` / `*_est` semantics consistently in docs and code comments.

---

## Sequential contract closure order

### Phase 1 — Burst formation contract (first priority)
**Question:** What exactly counts as one burst?

Must fix:
- required grouping keys (`market`, `side`)
- time-nearness rule
- split conditions
- max duration rule
- handling at 1s boundaries
- whether formation is global then summarized into 1s, or formed independently inside each 1s bucket
- whether thresholds are global, per venue, or per market in v1

**Deliverable:** `docs/burst-formation-contract.md`

**Exit criteria:**
- impossible to confuse burst with order identity
- split conditions are explicit and deterministic
- 1s aggregation semantics are explicit

---

### Phase 2 — Same-price burst contract
**Question:** What exactly is “same-price burst” and how is it counted?

Must fix:
- whether same-price requires same side (expected: yes)
- whether burst formation is inherited from Phase 1 or recomputed
- whether price equality is exact raw price or normalized tick-aligned price
- what `same_price_burst_notional_1s` sums over
- relation between `same_price_burst_max_len_1s` and ordinary burst print count

**Deliverable:** `docs/same-price-burst-contract.md`

**Exit criteria:**
- no ambiguity about exact equality vs normalized equality
- count / max_len / notional are all normatively defined

---

### Phase 3 — Multilevel sweep contract
**Question:** What qualifies as a multilevel sweep?

Must fix:
- whether price span is measured in raw ticks or normalized ticks
- whether `multilevel` means strictly more than one distinct price level
- how `max_span_ticks` is computed
- whether `multilevel_burst_notional_1s` refers to all multilevel bursts or only the max-span burst
- relation to same-price bursts (must be clearly distinct)

**Deliverable:** `docs/multilevel-burst-contract.md`

**Exit criteria:**
- same-price and multilevel are mutually intelligible and non-contradictory
- span semantics are deterministic

---

### Phase 4 — Directional / concentration / timing contract
**Question:** How are burst summaries collapsed into 1s directional and structure features?

Must fix:
- definition of `buy_burst_notional_1s` / `sell_burst_notional_1s`
- exact formula for `burst_delta_notional_1s`
- exact denominator and range for `largest_burst_share_notional_1s`
- meaning of `max_same_side_run_prints_1s`
- meaning of `side_flip_count_1s`
- exact sample set for `same_side_gap_ms_min_1s` and `same_side_gap_ms_p25_1s`
- null vs zero rules when too few events exist

**Deliverable:** `docs/burst-summary-contract.md`

**Exit criteria:**
- all summary features have explicit formulas
- sparse-data semantics are fixed

---

### Phase 5 — Book-aware validation contract
**Question:** How do at-touch / through / depletion / replenish interact with bursts?

Must fix:
- whether these are formation inputs or validation summaries (default expectation: validation only)
- exact numerator/denominator for `burst_at_touch_ratio_1s`
- exact numerator/denominator for `burst_through_ratio_1s`
- what event increments `burst_depletion_count_1s`
- what event increments `burst_replenish_after_touch_count_1s`
- null vs zero when book state is missing/unsynchronized
- synchronized-book prerequisite if needed

**Deliverable:** `docs/burst-book-validation-contract.md`

**Exit criteria:**
- all book-aware metrics are clearly separated from burst formation
- missing-book semantics are explicit

---

### Phase 6 — Final schema writeup
**Question:** How do all burst features join the existing 1s schema?

Must fix:
- exact column names
- ordering in schema doc
- types
- null / zero policy
- naming notes and interpretation notes
- migration note for downstream consumers

**Deliverable:** `docs/1s-burst-feature-schema.md`

**Exit criteria:**
- complete column table exists
- every feature traces back to a closed prior contract

---

## Working rules

1. One contract at a time.
2. After each contract doc is written, do a spot-review against adjacent docs before moving on.
3. If a later contract would force reinterpretation of an earlier one, stop and revise the earlier contract explicitly.
4. Use deterministic language: “must”, “must not”, “defined as”. Avoid suggestive prose in normative sections.
5. Keep “same parent order” language out of normative definitions.
6. Prefer `burst` terminology in exported columns. Use `cluster` only if needed internally.

---

## Open questions to resolve during the sequence

- Should burst formation be run on the full stream first, then summarized into 1s rows?
- Should `gap_threshold_ms` be fixed globally in v1 or delegated per venue?
- Should 1s summaries use only bursts that start inside the second, end inside the second, or overlap the second?
- Are same-price comparisons done on exact raw price or canonical tick-normalized price?
- For multilevel sweep, is span based on distinct touched prices or first-to-last price distance?
- For gap-percentile features, what is the behavior when the sample size is 0 or 1?
- For book-aware ratios, should missing book imply `NULL` or `0`?

---

## Immediate next step

Start with **Phase 1 — Burst formation contract** and do not define any feature formulas that depend on it until the contract is saved.
