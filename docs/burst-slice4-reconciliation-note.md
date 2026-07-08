# Burst Slice 4 Reconciliation Note

**Status:** Draft v1  
**Track:** burst feature implementation bridge  
**Scope:** cleanup / reconciliation after slices 1–3  

---

## 1. Purpose

This note closes the remaining bridge-plan item for slice 4: explicitly reconcile the newly added burst book-validation fields with the pre-existing trade-level book fields already emitted in the v2 1s row.

This document is descriptive only. It does not redefine any contract.

---

## 2. Legacy trade-level fields vs new burst-level fields

The existing 1s row already emits global trade-level / book-level columns from `lib/feature-accumulator.mjs`:

- `trade_at_touch_qty`
- `trade_through_qty`
- `best_deplete_count`
- `best_replenish_count`

Slices 1–3 added burst-specific fields:

- `burst_at_touch_ratio_1s`
- `burst_through_ratio_1s`
- `burst_depletion_count_1s`
- `burst_replenish_after_touch_count_1s`

These fields are **not duplicates**. They intentionally answer different questions.

---

## 3. Field-by-field reconciliation

| Legacy field | New field | Why both exist |
|---|---|---|
| `trade_at_touch_qty` | `burst_at_touch_ratio_1s` | Legacy field is a **global qty sum** over all trades classified at-touch in the bucket. New field is a **burst-associated notional ratio** over the classifiable burst subset only. Different scope, unit, and denominator. |
| `trade_through_qty` | `burst_through_ratio_1s` | Legacy field is a **global qty sum**. New field is a **burst-associated notional ratio**. They are not interchangeable. |
| `best_deplete_count` | `burst_depletion_count_1s` | Legacy field counts **all observed best-level depletion events** in the bucket. New field counts only the subset that **co-occur with at-touch burst activity**. |
| `best_replenish_count` | `burst_replenish_after_touch_count_1s` | Legacy field counts **all observed replenishment events**. New field counts only the subset that **co-occur with at-touch burst activity**. |

---

## 4. Scope differences that downstream users must not miss

### 4.1 Global vs burst-associated
- Legacy `trade_*` fields summarize all classified trades in the bucket.
- New `burst_*` validation fields summarize only burst-associated prints / events.

### 4.2 Qty / count vs ratio
- `trade_at_touch_qty` and `trade_through_qty` are raw quantities.
- `burst_at_touch_ratio_1s` and `burst_through_ratio_1s` are ratios over classifiable burst-associated **notional**.

### 4.3 Event count vs co-occurrence count
- `best_deplete_count` and `best_replenish_count` are unconditional book-event counts.
- `burst_depletion_count_1s` and `burst_replenish_after_touch_count_1s` are conditioned on at-touch burst activity in the same bucket.

### 4.4 Nullability
- Legacy fields are non-null numeric outputs in the current 1s row.
- New burst validation fields preserve contract nullability:
  - ratios: `NULL` when classified burst denominator is empty
  - counts: `NULL` when book state is unavailable for the relevant observation

---

## 5. Current code-state check

Confirmed in `lib/feature-accumulator.mjs`:
- legacy fields remain emitted unchanged in the main row:
  - `trade_at_touch_qty`
  - `trade_through_qty`
  - `best_deplete_count`
  - `best_replenish_count`
- new burst validation fields are emitted in addition, not as renames or replacements
- sink path preserves `null` values for the new nullable burst validation columns

No adapter-layer nullability fix was required during this slice sequence.

---

## 6. Practical reading guide for downstream analysis

Use legacy fields when asking:
- how much total trade volume printed at the touch?
- how many total depletion/replenish events occurred?

Use new burst fields when asking:
- what share of burst-associated notional was at-touch / through?
- how often did best-level deplete or replenish in seconds where burst-at-touch activity was present?

Do **not** compare raw magnitudes directly across these families without normalizing for scope and unit.

---

## 7. Slice 4 close decision

Slice 4 required items from `docs/burst-implementation-bridge-plan.md`:
- explicit reconciliation note with pre-existing v2 trade-level at-touch/through fields
- any sink-level nullability fixes or adapter cleanup

Status:
- reconciliation note: **done** (this document)
- sink-level nullability fix: **not required** based on current verification

**Slice 4 can be treated as closed unless a downstream sink later proves unable to preserve nullable numeric fields.**
