# Same-Price Burst Contract

**Status:** Draft v1  
**Track:** burst feature contract sequence  
**Governing plan:** `docs/burst-feature-contract-plan.md`  
**Depends on:** `docs/burst-formation-contract.md`

---

## 1. Purpose

This contract defines the semantics of the same-price burst feature family:
- `same_price_burst_count_1s`
- `same_price_burst_max_len_1s`
- `same_price_burst_notional_1s`

These features are intended to describe:

> repeated same-side execution concentrated at one price level within already-formed bursts

These features are **not** intended to mean:
- same parent order identity
- one queue event
- one participant

---

## 2. Dependency on burst formation

Same-price burst logic is a **downstream characterization layer** built on top of Phase 1 burst formation.

It must:
- inherit Phase 1 burst identity
- operate only within already-formed bursts
- never recompute or replace Phase 1 burst boundaries

Therefore, same-price burst detection must not join prints that belong to different Phase 1 bursts.

---

## 3. Canonical definition

A same-price burst is a maximal contiguous run of prints inside one already-formed burst such that:

1. all prints belong to the same Phase 1 burst
2. all prints have the same `side` (already guaranteed by Phase 1 burst membership)
3. all prints have the same canonical price key

A same-price burst is therefore a **sub-run inside one Phase 1 burst**.

---

## 4. Price equality semantics

### 4.1 Canonical price key
Same-price equality must be evaluated on a **canonical normalized price key**, not on raw string formatting.

Interpretation:
- prices that are numerically equal after venue-consistent normalization must be treated as the same price
- differences caused only by formatting representation must not split same-price runs

Examples:
- `100000`, `100000.0`, and `100000.000` must map to the same canonical price key
- `100000.0` and `100000.5` must map to different canonical price keys

### 4.2 Tick alignment
For v1, canonical price normalization must be numeric and deterministic, but this contract does **not** require venue tick-size quantization as a split rule beyond the upstream canonical price parsing already used by the system.

In other words:
- same-price means numerically equal normalized price
- not “same venue tick bucket after additional rounding”

If later infrastructure introduces a stricter venue tick canonicalizer, it must preserve existing equal-price semantics or explicitly revise this contract.

---

## 5. Detection order

Within each already-formed Phase 1 burst, prints must be processed in burst order:
1. ascending `ts`
2. stable original order within equal `ts`

A same-price burst begins when:
- a Phase 1 burst starts, or
- the canonical price key changes from the previous print inside that burst

A same-price burst ends when:
- the canonical price key changes, or
- the enclosing Phase 1 burst ends

---

## 6. Required same-price primitives

Each same-price burst sub-run must expose at minimum:
- `parent_burst_id` (conceptual identity; implementation may use any deterministic internal handle)
- `same_price_key`
- `same_price_start_ts`
- `same_price_end_ts`
- `same_price_print_count`
- `same_price_qty`
- `same_price_notional`

These primitives are used only for downstream 1s summaries.

---

## 7. Summary feature semantics

### 7.1 `same_price_burst_count_1s`
Definition:

> count of same-price bursts whose interval overlaps the 1s bucket

This follows the same overlap model as Phase 1 burst summaries.

If a same-price burst crosses a 1s boundary, it may contribute to more than one 1s row.

### 7.2 `same_price_burst_max_len_1s`
Definition:

> maximum `same_price_print_count` among same-price bursts overlapping the 1s bucket

Interpretation:
- this is a maximum print-run length
- not a count of price levels
- not a duration metric

### 7.3 `same_price_burst_notional_1s`
Definition:

> sum of `same_price_notional` over all same-price bursts overlapping the 1s bucket

Interpretation:
- this is not only the max same-price burst notional
- this is the total notional carried by all same-price sub-runs that overlap the second

---

## 8. Overlap semantics

Use the same overlap model as `docs/burst-formation-contract.md`.

Let:
- 1s bucket = `[bucket_start_ts, bucket_start_ts + 1000)`
- same-price burst interval = `[same_price_start_ts, same_price_end_ts]`

A same-price burst overlaps a bucket if:
- `same_price_start_ts < bucket_end_ts`
- and `same_price_end_ts >= bucket_start_ts`

This means a same-price burst may contribute to multiple 1s rows if it crosses a second boundary.

---

## 9. Relationship to ordinary burst features

These same-price features must be interpreted as a refinement of Phase 1 burst structure.

They are not replacements for:
- `burst_count_1s`
- `max_burst_prints_1s`
- `max_burst_notional_1s`

Key distinctions:
- `burst_count_1s` counts Phase 1 bursts
- `same_price_burst_count_1s` counts same-price sub-runs inside those bursts
- `max_burst_prints_1s` measures the longest whole-burst print count
- `same_price_burst_max_len_1s` measures the longest equal-price sub-run print count

Therefore:
- `same_price_burst_count_1s` may be greater than `burst_count_1s`
- `same_price_burst_max_len_1s` must be less than or equal to the parent burst print count for any contributing run

---

## 10. Null / zero semantics

At the 1s summary layer:
- if no same-price bursts overlap the second, then
  - `same_price_burst_count_1s = 0`
  - `same_price_burst_max_len_1s = 0`
  - `same_price_burst_notional_1s = 0`

These features do not use `NULL` in v1 because they depend only on trade prints and deterministic burst decomposition, not on optional book state.

---

## 11. No cross-burst merging

A same-price burst must not:
- merge across different Phase 1 bursts
- merge across side changes
- merge across market changes
- merge across Phase 1 split boundaries even if price repeats

Example:
- buy at 100000
- long gap
- buy at 100000

If Phase 1 split the enclosing burst due to the long gap, these must be treated as two separate same-price bursts.

---

## 12. Explicit non-claims

This contract does **not** claim that a same-price burst means:
- the same resting queue was continuously hit by one order
- one participant repeatedly crossed the spread
- one iceberg execution slice

It only claims deterministic repeated same-price execution inside already-formed same-side bursts.

---

## 13. Implementation constraints

Implementations following this contract must:
- derive same-price runs only after Phase 1 burst formation
- use deterministic canonical price equality
- preserve overlap-based 1s summary semantics

Implementations must not:
- use book state to define same-price runs
- reinterpret same-price runs as parent-order identity
- replace numeric equality with loose proximity bands

---

## 14. Follow-ups handed to later contracts

Deferred to later contracts:
- relation between same-price and multilevel sweep when a Phase 1 burst contains both patterns
- whether later validation layers should annotate same-price bursts with at-touch / depletion context
- final schema placement and type table

---

## 15. Exit check

This contract is acceptable only if all of the following are true:
- same-price semantics are clearly downstream of Phase 1 burst formation
- price equality is deterministic and formatting-insensitive
- count / max_len / notional summaries are explicit
- overlap semantics are explicit
- no wording implies same parent-order identity
