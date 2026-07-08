# 1s Burst Feature Schema

**Status:** Draft v1  
**Track:** burst feature contract sequence  
**Governing plan:** `docs/burst-feature-contract-plan.md`  
**Depends on:**
- `docs/burst-formation-contract.md`
- `docs/same-price-burst-contract.md`
- `docs/multilevel-burst-contract.md`
- `docs/burst-summary-contract.md`
- `docs/burst-book-validation-contract.md`

---

## 1. Purpose

This document defines the 1-second row schema for burst-related features.

It is a schema-layer summary of the burst contracts. If any field meaning here conflicts with the contract documents above, the contract documents win and this schema document must be updated.

---

## 2. Scope

This schema adds the following burst-related 1s columns:

### Core burst
- `burst_count_1s`
- `max_burst_notional_1s`
- `max_burst_prints_1s`
- `max_burst_duration_ms_1s`

### Same-price burst
- `same_price_burst_count_1s`
- `same_price_burst_max_len_1s`
- `same_price_burst_notional_1s`

### Multilevel burst
- `multilevel_burst_count_1s`
- `multilevel_burst_max_span_ticks_1s`
- `multilevel_burst_notional_1s`

### Directional burst
- `buy_burst_notional_1s`
- `sell_burst_notional_1s`
- `burst_delta_notional_1s`

### Concentration / run / timing
- `largest_burst_share_notional_1s`
- `max_same_side_run_prints_1s`
- `side_flip_count_1s`
- `same_side_gap_ms_min_1s`
- `same_side_gap_ms_p25_1s`

### Book-aware validation
- `burst_at_touch_ratio_1s`
- `burst_through_ratio_1s`
- `burst_depletion_count_1s`
- `burst_replenish_after_touch_count_1s`

---

## 3. Conventions

### 3.1 Second bucket interval
Every row describes one 1s bucket:
- `[second_ts, second_ts + 1000)`

### 3.2 Overlap-based vs bucket-local columns
Two semantics exist in this schema.

#### Overlap-based burst summaries
These are computed from already-formed bursts using overlap semantics:
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

#### Bucket-local print summaries
These are computed only from prints/events inside the current second:
- `max_same_side_run_prints_1s`
- `side_flip_count_1s`
- `same_side_gap_ms_min_1s`
- `same_side_gap_ms_p25_1s`
- `burst_at_touch_ratio_1s`
- `burst_through_ratio_1s`
- `burst_depletion_count_1s`
- `burst_replenish_after_touch_count_1s`

### 3.3 Null / zero policy
- Pure trade-derived count/sum/max features use `0` when nothing qualifies
- Timing-gap features use `NULL` when the sample is empty
- Book-aware ratio features use `NULL` when the classified denominator is empty
- Book-aware count features use `NULL` when book state is unavailable for the relevant event detection

---

## 4. Column table

| Column | Type | Nullable | Semantics |
|---|---|---:|---|
| `burst_count_1s` | BIGINT | no | Count of Phase 1 bursts overlapping the second |
| `max_burst_notional_1s` | DOUBLE | no | Maximum burst notional among Phase 1 bursts overlapping the second |
| `max_burst_prints_1s` | BIGINT | no | Maximum burst print count among Phase 1 bursts overlapping the second |
| `max_burst_duration_ms_1s` | DOUBLE | no | Maximum burst duration in milliseconds among Phase 1 bursts overlapping the second |
| `same_price_burst_count_1s` | BIGINT | no | Count of same-price sub-runs overlapping the second |
| `same_price_burst_max_len_1s` | BIGINT | no | Maximum print-run length among same-price sub-runs overlapping the second |
| `same_price_burst_notional_1s` | DOUBLE | no | Sum of same-price sub-run notional across all same-price sub-runs overlapping the second |
| `multilevel_burst_count_1s` | BIGINT | no | Count of multilevel Phase 1 bursts overlapping the second |
| `multilevel_burst_max_span_ticks_1s` | BIGINT | no | Maximum multilevel burst span in ticks among multilevel bursts overlapping the second |
| `multilevel_burst_notional_1s` | DOUBLE | no | Sum of burst notional across all multilevel bursts overlapping the second |
| `buy_burst_notional_1s` | DOUBLE | no | Sum of burst notional across buy-side bursts overlapping the second |
| `sell_burst_notional_1s` | DOUBLE | no | Sum of burst notional across sell-side bursts overlapping the second |
| `burst_delta_notional_1s` | DOUBLE | no | `buy_burst_notional_1s - sell_burst_notional_1s` |
| `largest_burst_share_notional_1s` | DOUBLE | no | Largest overlapping burst notional divided by total overlapping burst notional in the second |
| `max_same_side_run_prints_1s` | BIGINT | no | Longest contiguous same-side print run fully inside the second |
| `side_flip_count_1s` | BIGINT | no | Count of adjacent print pairs inside the second whose sides differ |
| `same_side_gap_ms_min_1s` | DOUBLE | yes | Minimum gap in ms across adjacent same-side print pairs inside the second |
| `same_side_gap_ms_p25_1s` | DOUBLE | yes | 25th percentile gap in ms across adjacent same-side print pairs inside the second |
| `burst_at_touch_ratio_1s` | DOUBLE | yes | Notional share of classifiable burst-associated prints inside the second that are at-touch |
| `burst_through_ratio_1s` | DOUBLE | yes | Notional share of classifiable burst-associated prints inside the second that are through |
| `burst_depletion_count_1s` | BIGINT | yes | Count of best-level depletion events in the second that co-occur with at-touch burst activity |
| `burst_replenish_after_touch_count_1s` | BIGINT | yes | Count of best-level replenishment events in the second that co-occur with at-touch burst activity |

---

## 5. Column notes

### 5.1 `burst_count_1s`
This is an overlap count, not a count of bursts that both start and end inside the second.

### 5.2 `max_burst_*_1s`
The `max_*` burst core fields are maxima over overlapping Phase 1 bursts, not maxima over raw trades.

### 5.3 Same-price vs multilevel
A second may simultaneously have:
- non-zero same-price fields, and
- non-zero multilevel fields

This is expected because same-price sub-runs can exist inside multilevel bursts.

### 5.4 `largest_burst_share_notional_1s`
This is a within-burst-notional concentration metric. The denominator is overlapping burst notional only, not all trade notional in the second.

### 5.5 Run / gap fields
Run and gap fields are bucket-local. They do not use burst overlap replication across second boundaries.

### 5.6 Book-aware ratio fields
The denominator uses only classifiable burst-associated notional. If the classified denominator is zero, the ratio is `NULL`.

### 5.7 Book-aware count fields
A value of `0` means the book state was available and no qualifying event occurred. `NULL` means the relevant book-event observation was unavailable.

---

## 6. Suggested physical types

Recommended physical representation if the sink supports standard SQL-like types:

- count columns → `BIGINT`
- notional / duration / ratio / percentile columns → `DOUBLE`
- nullable fields remain nullable at storage layer

If a sink lacks nullable numeric support, the adapter must preserve null semantics explicitly (for example via Arrow/Parquet null bitmaps) and must not silently coerce `NULL` to `0`.

---

## 7. Validation checklist for implementation

Before treating this schema as implementation-ready, confirm:
- overlap-based columns actually use already-formed bursts
- bucket-local columns do not leak cross-second adjacency
- same-price and multilevel reuse the same canonical price semantics
- `largest_burst_share_notional_1s` denominator matches the contract
- gap fields emit `NULL` on empty samples
- book-aware ratio fields emit `NULL` on empty classified denominator
- book-aware count fields distinguish unavailable book state from observed zero events

---

## 8. Multilevel span note

`multilevel_burst_max_span_ticks_1s` inherits the finalized multilevel contract meaning:
- burst span is measured as `(burst_max_price - burst_min_price) / tick_size`
- `tick_size` is taken from configured market/venue tick metadata, not inferred from the observed burst tape

This keeps `span_ticks` interpretable and comparable across bursts on the same market.

---

## 9. Exit check

This schema document is acceptable only if:
- every burst-related field has a clear type and nullability
- overlap-based vs bucket-local semantics are explicit
- null vs zero rules are explicit
- any remaining blocker is listed explicitly rather than hidden
