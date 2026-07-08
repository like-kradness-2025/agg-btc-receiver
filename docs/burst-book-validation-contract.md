# Burst Book Validation Contract

**Status:** Draft v1  
**Track:** burst feature contract sequence  
**Governing plan:** `docs/burst-feature-contract-plan.md`  
**Depends on:** `docs/burst-formation-contract.md`, `docs/same-price-burst-contract.md`, `docs/multilevel-burst-contract.md`, `docs/burst-summary-contract.md`

---

## 1. Purpose

This contract defines the semantics of the following book-aware validation features:
- `burst_at_touch_ratio_1s`
- `burst_through_ratio_1s`
- `burst_depletion_count_1s`
- `burst_replenish_after_touch_count_1s`

These features are intended to describe:
- whether burst-associated prints occurred at the displayed best opposite-side touch
- whether burst-associated prints extended through the displayed best opposite-side touch
- whether burst activity co-occurred with best-level depletion
- whether burst activity co-occurred with best-level replenishment after touch interaction

These features are **not** intended to mean:
- same parent-order identity
- causal proof that one burst alone moved the book
- perfect synchronized-book reconstruction from trades alone

---

## 2. Role in the system

Book-aware burst metrics are **post-formation validation summaries**.

They must:
- be computed only after Phase 1 burst formation has already assigned burst identity
- never participate in burst formation itself
- never split, merge, or redefine Phase 1 bursts

This separation is normative.

---

## 3. Required upstream state

These metrics require:
- burst membership for trade prints (from Phase 1 and downstream burst layers)
- trade-side and trade-price information
- best bid / best ask state from depth processing
- existing best-level depletion / replenishment detection state

If best bid / best ask state is unavailable, these metrics may become partially or wholly unobservable.

---

## 4. Classification vocabulary

### 4.1 At-touch trade
A burst-associated trade is **at-touch** if:
- for a buy-side trade: `trade.price == best_ask_price` at the classification point
- for a sell-side trade: `trade.price == best_bid_price` at the classification point

### 4.2 Through trade
A burst-associated trade is **through** if:
- for a buy-side trade: `trade.price > best_ask_price` at the classification point
- for a sell-side trade: `trade.price < best_bid_price` at the classification point

### 4.3 Neither
A burst-associated trade may be neither at-touch nor through if the visible best state does not support either classification or if price/book alignment is otherwise absent.

### 4.4 Mutual exclusivity
A single burst-associated trade must not be classified as both at-touch and through simultaneously.

---

## 5. Classification point and caveat

For v1, at-touch / through classification uses the best bid / ask state available at the trade classification point in the live accumulator.

This is a practical approximation, not a perfect synchronized-book reconstruction.

Therefore:
- book-aware ratios are descriptive validation signals
- they may include mismatch caused by stale or asynchronous book updates
- they must not be interpreted as exact queue-path reconstruction

---

## 6. Burst-associated trade set per second

For each 1s bucket, define the burst-associated trade set as:

> all trade prints that both:
> 1. belong to a Phase 1 burst, and
> 2. have `ts` inside the 1s bucket `[bucket_start_ts, bucket_start_ts + 1000)`

This trade set is **bucket-local**.

Unlike burst-overlap summaries, these ratio features are based on the prints observed inside the second, not on whole-burst overlap replication across seconds.

---

## 7. Ratio denominator policy

For ratio features, define:
- `N_total_classified` = total notional of burst-associated trades in the bucket for which best-state classification is available
- `N_total_unclassified` = total notional of burst-associated trades in the bucket for which best-state classification is unavailable

Ratios must use `N_total_classified` as the denominator.

This means:
- partially missing book coverage does not force the ratio to zero
- unclassifiable burst-associated notional is excluded from the ratio denominator

This is normative for v1.

---

## 8. `burst_at_touch_ratio_1s`

Definition:
- let `N_at_touch` = total notional of burst-associated trades in the bucket classified as at-touch
- let denominator = `N_total_classified`

Then:
- if `N_total_classified = 0`, `burst_at_touch_ratio_1s = NULL`
- otherwise, `burst_at_touch_ratio_1s = N_at_touch / N_total_classified`

### Interpretation
- range: `[0, 1]`
- `0` means classified burst-associated prints existed, but none were at-touch
- `NULL` means the ratio is unobservable because no burst-associated trade in the bucket had usable best-state classification

---

## 9. `burst_through_ratio_1s`

Definition:
- let `N_through` = total notional of burst-associated trades in the bucket classified as through
- let denominator = `N_total_classified`

Then:
- if `N_total_classified = 0`, `burst_through_ratio_1s = NULL`
- otherwise, `burst_through_ratio_1s = N_through / N_total_classified`

### Interpretation
- range: `[0, 1]`
- `0` means classified burst-associated prints existed, but none were through
- `NULL` means the ratio is unobservable because no burst-associated trade in the bucket had usable best-state classification

### Relation to at-touch
Because at-touch and through are mutually exclusive on each classified trade:
- `burst_at_touch_ratio_1s + burst_through_ratio_1s <= 1`

The remaining share represents classified burst-associated trades that were neither at-touch nor through.

---

## 10. `burst_depletion_count_1s`

### 10.1 Purpose
This feature counts best-level depletion events that co-occur with burst activity in the same second.

### 10.2 Event rule
A depletion event increments `burst_depletion_count_1s` if both are true inside the same 1s bucket:
1. a best-level depletion event is detected by the existing depth-state logic
2. at least one burst-associated trade in that bucket is classified as at-touch

### 10.3 Semantics
This is a **co-occurrence count**, not a causal claim.

It does not prove:
- one burst alone caused the depletion
- the depleted queue belonged to one participant

### 10.4 Missing-book rule
- if book state is unavailable for depletion detection in the bucket, `burst_depletion_count_1s = NULL`
- if book state is available and no qualifying event occurs, `burst_depletion_count_1s = 0`

---

## 11. `burst_replenish_after_touch_count_1s`

### 11.1 Purpose
This feature counts best-level replenishment events that co-occur with burst touch interaction in the same second.

### 11.2 Event rule
A replenishment event increments `burst_replenish_after_touch_count_1s` if both are true inside the same 1s bucket:
1. a best-level replenishment event is detected by the existing depth-state logic
2. at least one burst-associated trade in that bucket is classified as at-touch

### 11.3 Semantics
This is a **co-occurrence count**, not proof that the replenishment was a response to one specific burst.

### 11.4 Missing-book rule
- if book state is unavailable for replenishment detection in the bucket, `burst_replenish_after_touch_count_1s = NULL`
- if book state is available and no qualifying event occurs, `burst_replenish_after_touch_count_1s = 0`

---

## 12. Partial-book coverage policy

### 12.1 Ratio features
For ratio features:
- use only classified burst-associated notional in the denominator
- exclude unclassifiable burst-associated notional
- emit `NULL` only if the classified denominator is zero

### 12.2 Event-count features
For depletion / replenish count features:
- if the bucket does not have usable book state for the relevant event detection logic, emit `NULL`
- otherwise emit integer event counts, possibly zero

This is intentionally more conservative than the ratio policy.

---

## 13. Null / zero semantics summary

### 13.1 Ratio features
- no burst-associated trades in bucket → `NULL`
- burst-associated trades exist but none classifiable → `NULL`
- classifiable burst-associated trades exist but numerator is zero → `0`

### 13.2 Event-count features
- book unavailable for event detection → `NULL`
- book available, no qualifying event → `0`
- book available, one or more qualifying events → positive integer

---

## 14. Relationship to other burst features

These metrics validate or contextualize burst behavior; they do not redefine earlier burst features.

Therefore:
- they must not alter `burst_count_1s`
- they must not alter same-price or multilevel classification
- they must not alter burst-overlap directional or concentration summaries

---

## 15. Explicit non-claims

This contract does **not** claim that:
- a high `burst_at_touch_ratio_1s` proves one parent order
- a high `burst_through_ratio_1s` proves one sweep consumed every level in sequence
- `burst_depletion_count_1s` proves burst-caused depletion
- `burst_replenish_after_touch_count_1s` proves replenishment was a direct reaction to one burst

These are validation/context features only.

---

## 16. Implementation constraints

Implementations following this contract must:
- compute these features only after burst membership is known
- keep ratio features bucket-local to burst-associated prints inside the current second
- keep event-count features tied to existing depth-event detection state
- preserve NULL-vs-zero semantics exactly as defined here

Implementations must not:
- use these features as burst-formation inputs
- silently replace NULL with 0 for unobservable buckets
- reinterpret event co-occurrence as causal proof

---

## 17. Follow-ups handed to later contracts

Deferred to later contracts:
- final schema placement and column type table
- whether to materialize helper diagnostics such as classified-vs-unclassified burst notional
- whether later synchronized-book infrastructure should revise classification semantics

---

## 18. Exit check

This contract is acceptable only if all of the following are true:
- book-aware metrics are clearly post-formation validation summaries
- ratio denominators are explicit
- depletion/replenish counts are explicit co-occurrence rules
- NULL-vs-zero behavior is deterministic
- no wording implies same parent-order recovery or causal proof
