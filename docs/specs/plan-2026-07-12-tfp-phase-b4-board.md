# TFP Phase B4 Plan: Board Candidate Columns

- Date: 2026-07-12
- Branch: `v2`
- Prerequisite: B3 commit `858e3de`, independent review 98/100 PASS
- Mode: Kanban PDD / `delegate_task`; profileSession prohibited

## Scope

Add spec §10 board candidate columns to the 1s feature schema and compute them from the B3-wired `bookSnapshot` without modifying existing #13/#14 placeholders.

### In scope

1. Add `BOARD_FIELDS` array and `BOARD_CANDIDATE_FIELDS` set in `schema.mjs`.
2. Append board columns to `FEATURE_1S_FIELDS` (after #22).
3. Update `createBaseRow()` to initialize board columns (P1 = null for no-observation).
4. Compute board columns in `feature-computer-1s.mjs` from `bookSnapshot.state`.
5. Columns per §10:
   - `board_top_depth_ratio` — burst_notional_1s ÷ (best_bid * best_bid_qty + best_ask * best_ask_qty) ; null if depth ≤ 0
   - `board_mid_move_bps_1s` — (mid - prior_mid) / prior_mid × 10000; null if prior_mid null
   - `board_vs_30s` — burst_notional_1s ÷ traded_notional_30s; null if denom ≤ 0 (already computed as #12, but board_ prefix)
   - `board_vs_depth` — alias for `board_top_depth_ratio` per §10.4
6. Wire into existing independent verifier (`tfp-book-contract-fixture.test.mjs`) expected values from fixture board_candidates_at_1000/2000.

### Out of scope

- #13 `burst_notional_vs_top_depth` — remains null for no-book P1 contract
- #14 `burst_mid_move_bps_1s` — remains 0 for P1 contract
- B5 quarantine/checkpoint/manifest/cursor policy
- B6 inventory kind separation
- rollup, Receiver, cron, Gateway, production data

## Acceptance gates

- Board columns present in schema and base row.
- Book-unavailable rows have null board columns.
- Book-quarantined rows have null board columns.
- #13 remains null, #14 remains 0.
- Independent verifier expected values match fixture.
- Same-block strict anchor rules inherited from B3.
- RED→GREEN tests, npm test, node --check, git diff --check.
- Parent re-read; independent reviewer >=95.
