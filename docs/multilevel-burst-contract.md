# Multilevel Burst Contract

**Status:** Draft v1  
**Track:** burst feature contract sequence  
**Governing plan:** `docs/burst-feature-contract-plan.md`  
**Depends on:** `docs/burst-formation-contract.md`, `docs/same-price-burst-contract.md`

---

## 1. Purpose

This contract defines the semantics of the multilevel burst feature family:
- `multilevel_burst_count_1s`
- `multilevel_burst_max_span_ticks_1s`
- `multilevel_burst_notional_1s`

These features are intended to describe:

> already-formed bursts whose execution traverses more than one distinct price level

These features are **not** intended to mean:
- proven same-parent order identity
- guaranteed ladder sweep of the full displayed queue
- one participant crossing multiple levels alone

---

## 2. Dependency on burst formation

Multilevel burst logic is a downstream characterization layer built on top of Phase 1 burst formation.

It must:
- inherit Phase 1 burst identity
- operate only on already-formed bursts
- never recompute or replace Phase 1 burst boundaries

A multilevel burst is therefore a property of a Phase 1 burst, not a separate independently formed burst family.

---

## 3. Canonical definition

A Phase 1 burst is classified as **multilevel** if it contains prints at **two or more distinct canonical price keys**.

Equivalently:
- let `distinct_price_count` be the number of distinct canonical price keys touched by one Phase 1 burst
- the burst is multilevel iff `distinct_price_count >= 2`

If `distinct_price_count == 1`, the burst is not multilevel.

---

## 4. Price-key semantics

Multilevel classification must use the same canonical price equality semantics as `docs/same-price-burst-contract.md`.

Therefore:
- distinct levels are measured using canonical normalized numeric price keys
- raw formatting differences do not create false distinct levels
- loose proximity buckets are not allowed in v1

---

## 5. Required multilevel primitives

Each Phase 1 burst must expose at minimum:
- `distinct_price_count`
- ordered list or equivalent set of touched canonical price keys
- `burst_min_price`
- `burst_max_price`
- `tick_size` (from market/venue configuration at summary time, not inferred from burst tape)
- `burst_notional`
- `burst_side`
- `burst_start_ts`
- `burst_end_ts`

Additional internal helper state is allowed if it does not change normative semantics.

---

## 6. Span semantics

### 6.1 `span_ticks`
For a multilevel burst, define:

> `span_ticks` = `(burst_max_price - burst_min_price) / tick_size`

Where:
- `burst_min_price` and `burst_max_price` are the canonical normalized burst-level min/max prices
- `tick_size` is the configured minimum price increment for the market (or venue-level default if market-specific config is not yet available)

### 6.2 Special cases
- if `distinct_price_count < 2`, `span_ticks = 0` by definition
- if `tick_size` is missing or invalid for a market, multilevel classification may still exist conceptually, but `span_ticks` is not implementation-ready for that market until configuration is fixed; this is a configuration blocker, not a semantic ambiguity
- implementations must emit integer tick span only when `(burst_max_price - burst_min_price)` is an exact multiple of `tick_size`; otherwise upstream price canonicalization / tick configuration is inconsistent and must be corrected rather than rounded silently

### 6.3 Interpretation
This definition intentionally measures the **full min-to-max burst price span in configured tick units**, not:
- net first-to-last displacement
- count of all distinct prices touched

Therefore:
- a burst touching `100000 -> 100020` on a `0.01` market spans many ticks, not `1`
- revisits to prior prices do not increase `span_ticks` beyond the min/max range
- non-monotone bursts still preserve their full traversed price range through min/max span

---

## 7. Summary feature semantics

### 7.1 `multilevel_burst_count_1s`
Definition:

> count of multilevel Phase 1 bursts whose interval overlaps the 1s bucket

This uses the same overlap model as Phase 1 burst summaries.

### 7.2 `multilevel_burst_max_span_ticks_1s`
Definition:

> maximum `span_ticks` among multilevel bursts overlapping the 1s bucket

Interpretation:
- this is a maximum traversed span
- not a count of distinct prices over the full second
- not a notional-weighted span

### 7.3 `multilevel_burst_notional_1s`
Definition:

> sum of `burst_notional` over all multilevel bursts overlapping the 1s bucket

Interpretation:
- this is not only the notional of the widest-span burst
- this is the total notional carried by all overlapping multilevel bursts

---

## 8. Overlap semantics

Use the same overlap model as `docs/burst-formation-contract.md`.

Let:
- 1s bucket = `[bucket_start_ts, bucket_start_ts + 1000)`
- multilevel burst interval = `[burst_start_ts, burst_end_ts]`

A multilevel burst overlaps a bucket if:
- `burst_start_ts < bucket_end_ts`
- and `burst_end_ts >= bucket_start_ts`

This means a multilevel burst may contribute to more than one 1s row if it crosses a second boundary.

---

## 9. Relationship to same-price bursts

Same-price and multilevel features characterize different aspects of Phase 1 burst structure.

- same-price focuses on equal-price sub-runs inside a burst
- multilevel focuses on whether the whole burst traversed multiple price levels

These are not contradictory.

A single Phase 1 burst may:
- contain same-price sub-runs
- also be multilevel overall

Example:
- buy @ 100000
- buy @ 100000
- buy @ 100001
- buy @ 100001

This burst contains:
- two same-price sub-runs
- one multilevel burst with `distinct_price_count = 2`

---

## 10. Null / zero semantics

At the 1s summary layer:
- if no multilevel bursts overlap the second, then
  - `multilevel_burst_count_1s = 0`
  - `multilevel_burst_max_span_ticks_1s = 0`
  - `multilevel_burst_notional_1s = 0`

These features do not use `NULL` in v1 because they depend only on deterministic burst decomposition and price-key semantics from trade data.

---

## 11. No cross-burst merging

A multilevel burst classification must not:
- merge across different Phase 1 bursts
- merge across side changes
- merge across market changes
- merge across Phase 1 split boundaries even if price ladders continue later

Example:
- buy @ 100000
- buy @ 100001
- long gap
- buy @ 100002

If Phase 1 splits after the long gap, the latter print must not be merged into the earlier multilevel burst.

---

## 12. Explicit non-claims

This contract does **not** claim that a multilevel burst means:
- one order definitely swept every intermediate resting level
- all traversed prices were consumed by one participant
- the full visible ladder path is recoverable from trades alone

It only claims deterministic multi-price execution within one already-formed Phase 1 burst.

---

## 13. Implementation constraints

Implementations following this contract must:
- derive multilevel classification only after Phase 1 burst formation
- reuse the same canonical price semantics as same-price features
- compute `multilevel_burst_notional_1s` from all overlapping multilevel bursts
- preserve overlap-based 1s summary semantics
- compute `span_ticks` using configured `tick_size`, not inferred in-burst step size

Implementations must not:
- reinterpret multilevel bursts as parent-order identity
- use loose proximity bands instead of canonical price keys
- redefine `max_span_ticks` as distinct-price-count or first-to-last inferred-step span without explicit contract revision

---

## 14. Follow-ups handed to later contracts

Deferred to later contracts:
- interaction between multilevel bursts and book-aware validation metrics
- whether later analytics want a separate `distinct_price_count_max_1s`
- final schema placement and type table

---

## 15. Exit check

This contract is acceptable only if all of the following are true:
- multilevel is clearly downstream of Phase 1 burst formation
- price-key semantics are consistent with same-price semantics
- `span_ticks` is explicitly defined
- `multilevel_burst_notional_1s` is explicitly defined as sum across all overlapping multilevel bursts
- no wording implies true parent-order recovery
