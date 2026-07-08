# Burst Summary Contract

**Status:** Draft v1  
**Track:** burst feature contract sequence  
**Governing plan:** `docs/burst-feature-contract-plan.md`  
**Depends on:** `docs/burst-formation-contract.md`, `docs/same-price-burst-contract.md`, `docs/multilevel-burst-contract.md`

---

## 1. Purpose

This contract defines the semantics of the following feature families:

### Directional burst features
- `buy_burst_notional_1s`
- `sell_burst_notional_1s`
- `burst_delta_notional_1s`

### Concentration / run-structure features
- `largest_burst_share_notional_1s`
- `max_same_side_run_prints_1s`
- `side_flip_count_1s`

### Timing texture features
- `same_side_gap_ms_min_1s`
- `same_side_gap_ms_p25_1s`

These features are intended to describe:
- how much notional arrives in burst form by side
- how concentrated burst notional is within a second
- how prints alternate or persist by side inside a second
- how tightly packed same-side print sequences are inside a second

These features are **not** intended to mean:
- same parent-order identity
- participant identity
- causal attribution of price move to one actor

---

## 2. Two computational layers

This contract intentionally contains **two different summary layers**.

### 2.1 Burst-overlap summary layer
These features are computed from already-formed Phase 1 bursts using the overlap model from `docs/burst-formation-contract.md`:
- `buy_burst_notional_1s`
- `sell_burst_notional_1s`
- `burst_delta_notional_1s`
- `largest_burst_share_notional_1s`

These inherit the cross-second overlap semantics of Phase 1.

### 2.2 Intra-second print-structure layer
These features are computed only from prints whose timestamps fall inside the current 1s bucket:
- `max_same_side_run_prints_1s`
- `side_flip_count_1s`
- `same_side_gap_ms_min_1s`
- `same_side_gap_ms_p25_1s`

These do **not** inherit cross-second overlap semantics.

This distinction is normative.

---

## 3. Burst-overlap directional features

Use the same overlap model as Phase 1:
- 1s bucket = `[bucket_start_ts, bucket_start_ts + 1000)`
- burst interval = `[burst_start_ts, burst_end_ts]`
- a burst overlaps a bucket if:
  - `burst_start_ts < bucket_end_ts`
  - and `burst_end_ts >= bucket_start_ts`

### 3.1 `buy_burst_notional_1s`
Definition:

> sum of `burst_notional` over all overlapping bursts with `burst_side = buy`

If no buy-side bursts overlap the second:
- `buy_burst_notional_1s = 0`

### 3.2 `sell_burst_notional_1s`
Definition:

> sum of `burst_notional` over all overlapping bursts with `burst_side = sell`

If no sell-side bursts overlap the second:
- `sell_burst_notional_1s = 0`

### 3.3 `burst_delta_notional_1s`
Definition:

> `buy_burst_notional_1s - sell_burst_notional_1s`

Interpretation:
- positive values indicate buy-side burst dominance
- negative values indicate sell-side burst dominance
- zero means balanced or absent burst-side notional

---

## 4. Burst-overlap concentration feature

### 4.1 `largest_burst_share_notional_1s`
Definition:

Let:
- `total_burst_notional_1s = sum(burst_notional)` over all overlapping bursts
- `max_burst_notional_1s_overlap = max(burst_notional)` over all overlapping bursts

Then:
- if no bursts overlap the second, `largest_burst_share_notional_1s = 0`
- otherwise, `largest_burst_share_notional_1s = max_burst_notional_1s_overlap / total_burst_notional_1s`

### 4.2 Range
- if bursts overlap the second, range is `(0, 1]`
- if no bursts overlap the second, value is `0`

### 4.3 Interpretation
This is a **within-burst-notional concentration metric**.

The denominator is:
- total overlapping burst notional

The denominator is **not**:
- total trade notional for the second

Therefore:
- `largest_burst_share_notional_1s = 1.0` is correct when exactly one burst overlaps the second

---

## 5. Intra-second print ordering

The following features are computed from the set of trade prints whose `ts` falls inside the 1s bucket.

Within the bucket, prints must be processed in deterministic order:
1. ascending `ts`
2. stable original order within equal `ts`

No print outside the bucket contributes to these intra-second features.

---

## 6. Intra-second same-side run feature

### 6.1 Same-side run definition
Inside one 1s bucket, a same-side run is a maximal contiguous sequence of prints such that all prints in the sequence have the same side.

A same-side run is broken only by:
- a side change, or
- the 1s bucket boundary

It is **not** broken by:
- time gap magnitude
- price change
- burst boundary

### 6.2 `max_same_side_run_prints_1s`
Definition:

> maximum run length in prints across all same-side runs inside the 1s bucket

If the bucket contains:
- 0 prints → `max_same_side_run_prints_1s = 0`
- 1 print → `max_same_side_run_prints_1s = 1`

### 6.3 Relationship to Phase 1 bursts
This feature is not identical to `max_burst_prints_1s`.

Differences:
- `max_burst_prints_1s` respects Phase 1 gap and duration split rules
- `max_same_side_run_prints_1s` ignores burst split rules and only tracks intra-second side persistence

Therefore:
- `max_same_side_run_prints_1s` may be greater than `max_burst_prints_1s`
- this is expected and not contradictory

---

## 7. Intra-second side alternation feature

### 7.1 `side_flip_count_1s`
Definition:

> count of adjacent print pairs inside the 1s bucket for which `side_i != side_{i+1}`

Only consecutive prints fully inside the bucket are considered.

If the bucket contains:
- 0 or 1 print → `side_flip_count_1s = 0`

### 7.2 Boundary rule
A flip across a second boundary is not counted in either second.

This feature is strictly intra-second.

---

## 8. Intra-second same-side gap sample

### 8.1 Sample definition
Inside one 1s bucket, define the same-side consecutive-pair sample `G` as follows.

For each adjacent print pair `(p_i, p_{i+1})` inside the bucket:
- if `p_i.side == p_{i+1}.side`, include:
  - `gap_ms = p_{i+1}.ts - p_i.ts`
- otherwise do not include a sample

Important:
- the pair must be adjacent in bucket order
- two same-side prints separated by an opposite-side print do **not** form a same-side gap sample

### 8.2 Sample properties
- all `gap_ms` values are `>= 0`
- the sample may be empty
- the sample is bucket-local and does not cross second boundaries

---

## 9. Intra-second timing texture features

### 9.1 `same_side_gap_ms_min_1s`
Definition:
- if `G` is empty, `same_side_gap_ms_min_1s = NULL`
- otherwise, `same_side_gap_ms_min_1s = min(G)`

### 9.2 `same_side_gap_ms_p25_1s`
Definition:
- if `G` is empty, `same_side_gap_ms_p25_1s = NULL`
- otherwise, `same_side_gap_ms_p25_1s = p25(G)` using linear interpolation

### 9.3 Percentile method
Use standard linear interpolation over the sorted sample.

This contract does not require a special minimum sample size beyond the empty-sample rule.

Therefore:
- if `|G| = 1`, then `same_side_gap_ms_p25_1s` equals that sole sample value

### 9.4 Interpretation
These are timing-texture statistics, not event counters.

Therefore:
- `NULL` means “no same-side consecutive-pair sample exists”
- `0` means a real observed zero-gap pair exists

---

## 10. Null / zero semantics

### 10.1 Burst-overlap features
If no bursts overlap the second:
- `buy_burst_notional_1s = 0`
- `sell_burst_notional_1s = 0`
- `burst_delta_notional_1s = 0`
- `largest_burst_share_notional_1s = 0`

### 10.2 Intra-second run / flip features
If the bucket contains no prints:
- `max_same_side_run_prints_1s = 0`
- `side_flip_count_1s = 0`

If the bucket contains one print:
- `max_same_side_run_prints_1s = 1`
- `side_flip_count_1s = 0`

### 10.3 Intra-second gap features
If `G` is empty:
- `same_side_gap_ms_min_1s = NULL`
- `same_side_gap_ms_p25_1s = NULL`

---

## 11. Explicit non-claims

This contract does **not** claim that:
- a long same-side run equals one parent order
- low same-side gap values imply one participant
- concentration in one burst proves one actor dominated the tape

These are descriptive summary features only.

---

## 12. Implementation constraints

Implementations following this contract must:
- separate burst-overlap and intra-second computations
- preserve Phase 1 overlap semantics for burst-overlap features
- preserve bucket-local semantics for run/gap features
- return `NULL` rather than `0` for empty same-side gap samples

Implementations must not:
- recompute Phase 1 bursts inside each second for directional/concentration metrics
- let cross-second pairs influence `side_flip_count_1s`
- let cross-second pairs influence same-side gap samples

---

## 13. Follow-ups handed to later contracts

Deferred to later contracts:
- interaction between these summary features and book-aware validation metrics
- final schema placement and column type table
- whether a separate `total_burst_notional_1s` helper column is worth materializing

---

## 14. Exit check

This contract is acceptable only if all of the following are true:
- burst-overlap features clearly inherit Phase 1 overlap semantics
- run/gap features clearly remain intra-second only
- `largest_burst_share_notional_1s` denominator is explicit
- `same_side_gap_ms_min_1s` and `same_side_gap_ms_p25_1s` have deterministic NULL semantics
- no wording implies same parent-order recovery
