# PDD Plan: P4 Book/Liquidation Activation

- Date: 2026-07-13
- Branch: `v2`
- Base: P3-C3 commit `709a939`, independent review 97/100 PASS, 686/686 tests PASS
- Mode: PDD / delegate_task / independent 95-point gate

## Scope

Activate #13 `burst_notional_vs_top_depth`, #14 `burst_mid_move_bps_1s`, and #15-#22 research columns with real computed values. P1-P3 used placeholders (null/0 + quality phase labels); P4 fills them.

## Current state

- TFP Phase B (book contract + board columns) is complete.
- `feature-computer-1s.mjs` computes #1-#12 and `board_*` columns from `bookSnapshot`.
- #13 = null (P1_book_null), #14 = 0 (P1_book_zero), #15-#22 = 0 (P1_placeholder).
- `replay-book-state.mjs` exists with `replayBestBookState`.
- Board columns (`board_top_depth_ratio`, `board_vs_30s`, etc.) are already computed.

## What P4 changes

### A. #13/#14 activation (feature-computer-1s.mjs)
- When `bookSnapshot.available && bookSnapshot.state.seeded && best_bid != null && best_ask != null`:
  - #13 `burst_notional_vs_top_depth` = `board_top_depth_ratio` (burst_notional / top_depth)
  - #14 `burst_mid_move_bps_1s` = `board_mid_move_bps_1s` (mid move in bps)
- When book unavailable/unseeded: #13 stays null, #14 stays 0 (existing contract)

### B. #15-#22 research/outlier columns (feature-computer-1s.mjs)
Compute from available burst detector data:
- #15 `same_price_burst_max_len_1s`: max `burst_print_count` among same-price bursts
- #16 `same_price_burst_notional_1s`: sum of `burst_notional` among same-price bursts
- #17 `multilevel_burst_max_span_ticks_1s`: max `tick_span` among multilevel bursts
- #18 `multilevel_burst_max_span_bps_1s`: max `bps_span` among multilevel bursts
- #19 `multilevel_burst_notional_1s`: sum of `burst_notional` among multilevel bursts
- #20 `same_price_absorption_ratio_1s`: same_price_notional / total_notional (0 if total=0)
- #21 `burst_delta_notional_1s`: buy_notional - sell_notional
- #22 `outlier_trade_flag_1s`: 1 if any burst exceeds 5x the mean burst notional, else 0

### C. Quality phase labels
- Remove `phase: "P1_placeholder"`, `phase: "P1_book_null"`, `phase: "P1_book_zero"` from quality output
- All 22 columns now carry real values

### D. 30s/5min propagation
- 30s rollup inherits #13/#14 real values automatically via 30s aggregation
- 5min rollup inherits automatically via aggregation

## Files to change

1. `lib/burst-reducer/feature-computer-1s.mjs` — core computation for #13-#22
2. `lib/burst-reducer/schema.mjs` — (possibly) remove placeholder phase labels if stored there
3. `test/burst-reducer/feature-computer-1s.test.mjs` — add P4 activation fixture tests
4. `test/burst-reducer/p3-c3-wiring.test.mjs` — update if quality expectations change

## TDD fixtures

- book-seeded: bookSnapshot with all fields populated → #13 not null, #14 not 0
- book-unseeded: bookSnapshot null → #13=null, #14=0
- all 15-#22 computed = expected per second
- outlier present (5x mean) → #22=1
- no outlier → #22=0

## Verification

- `node --test test/burst-reducer/feature-computer-1s.test.mjs`
- `npm test` full suite
- `node --check` on all changed files
- `git diff --check`
- Independent review >=95

## Out of scope

- Liquidation column activation (separate phase)
- Retention/cleanup (P5)
- Cross-market 5min
- Consumer/dashboard changes
- Receiver/cron/Gateway changes
