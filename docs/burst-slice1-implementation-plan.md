# Burst Slice 1 Implementation Plan

**Status:** Draft v1  
**Track:** burst feature implementation bridge  
**Governing bridge plan:** `docs/burst-implementation-bridge-plan.md`  
**Target slice:** trade-only burst spine

---

## 1. Purpose

This document freezes the smallest implementation target for slice 1.

Scope is limited to the 14 overlap-based trade-only burst fields:
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

No bucket-local run/gap fields and no book-aware validation fields are included in slice 1.

---

## 2. Evidence-led target files

### Primary implementation target
- `lib/feature-accumulator.mjs`

**Why:**
- owns `feedTrade()` where trade stream ordering is already processed
- owns `feedSecond()` where the 1s output row is assembled
- already maintains per-market state (`_tradeAccums`, `_flow`, `_lastL1`, ring buffers)
- is the narrowest place to add burst state without pipeline fork

### Likely secondary targets
- test file(s) under repo test harness for deterministic replay / row assertions
- possibly config surface if `tick_size` lookup is externalized instead of hardcoded default

### Explicit non-targets for slice 1
- writer/storage redesign
- connector parsing changes
- depth diff logic
- at-touch / through / depletion / replenish logic

---

## 3. Smallest first diff

### 3.1 Add per-market burst state
Inside `FeatureAccumulator`, add a new per-market state container separate from `_tradeAccums` and `_flow`.

Required responsibilities:
- maintain one current open burst per market
- retain recently closed bursts long enough to summarize overlapping seconds
- carry Phase 1 burst primitives needed by slice 1 summaries

Suggested shape:
- `_burstState: Map<market, BurstState>`
- `BurstState.openBurst`
- `BurstState.closedBursts`

### 3.2 Extend `feedTrade()`
When each trade arrives:
- normalize side / numeric price / numeric qty / notional
- update existing open burst or close/start according to Phase 1 rules:
  - same market
  - same side
  - gap within threshold
  - duration within threshold
- update burst primitives:
  - start/end ts
  - print count
  - total qty / notional
  - min/max price
  - same-price sub-run decomposition inputs
  - distinct price tracking for multilevel classification

### 3.3 Add burst finalization helpers
Introduce small internal helpers rather than inlining everything into `feedTrade()`.

Suggested helpers:
- `_getOrCreateBurstState(market)`
- `_normalizeTradeSide(rawSide)`
- `_startBurst(market, tradeNorm)`
- `_appendTradeToBurst(burst, tradeNorm)`
- `_closeBurst(market, reason)`
- `_summarizeBurstForSecond(second, burst)` or equivalent aggregation helper

### 3.4 Extend `feedSecond()`
At row build time:
- find all closed/open bursts whose interval overlaps the current second
- derive the 14 slice-1 fields
- append them to the row object with contract-frozen semantics
- keep zero defaults for non-overlapping cases

### 3.5 Cleanup policy
After rows flush:
- remove bursts whose end time is older than the latest fully flushed second and can no longer overlap future rows
- preserve any still-open burst across seconds

---

## 4. Config / defaults for slice 1

Slice 1 needs exactly these runtime knobs:
- `gap_threshold_ms` (already contract-fixed per venue)
- `max_burst_duration_ms` (contract-fixed global)
- `tick_size` (for multilevel span)

Implementation rule for first slice:
- if current code already has a config access pattern, reuse it
- if not, introduce the smallest local lookup possible with explicit TODO for later config plumbing
- do not redesign the global config system just to add burst settings

---

## 5. Normative formulas that must be explicit in code comments

Because review found these easy to leave implicit, slice 1 implementation should comment the formulas directly near the code:
- `burst_count_1s` = count of Phase 1 bursts overlapping the second
- `max_burst_notional_1s` = max `burst_notional` over overlapping bursts
- `max_burst_prints_1s` = max `burst_print_count` over overlapping bursts
- `max_burst_duration_ms_1s` = max `burst_duration_ms` over overlapping bursts
- `same_price_burst_count_1s` = count of overlapping same-price sub-runs
- `same_price_burst_max_len_1s` = max same-price sub-run print length over overlaps
- `same_price_burst_notional_1s` = sum of overlapping same-price sub-run notional
- `multilevel_burst_count_1s` = count of overlapping multilevel bursts
- `multilevel_burst_max_span_ticks_1s` = max multilevel span across overlapping multilevel bursts
- `multilevel_burst_notional_1s` = sum of notional across overlapping multilevel bursts
- `buy_burst_notional_1s` / `sell_burst_notional_1s` = side-filtered overlap sums
- `burst_delta_notional_1s` = buy minus sell burst notional
- `largest_burst_share_notional_1s` = max overlapping burst notional / total overlapping burst notional, else 0

---

## 6. First test fixture requirements

One deterministic fixture should cover all of these in one sequence if possible:
- same-side short-gap burst formation
- burst split on side change
- same-price repeated sub-run
- multilevel burst with known `tick_size`
- cross-second overlap duplication semantics
- one second with no bursts to confirm zero-fill behavior

The fixture does not need depth/book events for slice 1.

---

## 7. Verification package for slice 1

Before calling slice 1 done, provide:
1. fixture input path or inline deterministic sequence
2. run command
3. sample emitted rows showing burst columns
4. deterministic automated test assertions for the 14 fields above

---

## 8. Risks to avoid

- mixing slice 2 bucket-local run/gap logic into slice 1
- mixing slice 3 book-aware logic into slice 1
- storing burst state inside `_flow` (wrong dependency direction)
- silently choosing the rejected multilevel span semantics
- forgetting cleanup for closed bursts that can no longer overlap future seconds

---

## 9. Ready-to-code entry point

The first coding task after this plan should be:

> Modify `lib/feature-accumulator.mjs` to add internal burst state and emit the 14 overlap-based slice-1 burst columns in `feedSecond()`, backed by one deterministic replay test fixture.

This is the smallest end-to-end implementation target consistent with the frozen contracts.
