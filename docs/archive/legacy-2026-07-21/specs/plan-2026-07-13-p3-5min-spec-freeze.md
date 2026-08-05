# P3 5min Summary Specification Freeze

- Date: 2026-07-13
- Base: Phase C complete, HEAD `d110b56`
- Status: **APPROVED for market-local pure module**; cross-market calculations remain deferred.

## Approved contract

- Row grain: one row per market per 5-minute window.
- Primary key: `(market, window_start_ms)`.
- Input: exactly ten consecutive aligned `features_30s` rows for the same market.
- Cross-market: not included in this first slice; a later approved phase may join market-local rows.
- Universe: no fixed 15-market requirement in this slice.
- Partial market universe: irrelevant to market-local computation; missing 30s input is fail-closed.
- Empty: valid only when all ten 30s inputs are valid empty rows.
- Missing/not-yet-arrived/verified-missing: reject or block; never zero-fill.
- Output namespace: `features_5min` pure row only; no persistence in this slice.
- Quality: preserve source layer/window count, coverage, empty/missing provenance, finalized state, and market/window identity.

## Decisions required later

- Cross-market row grain and primary key.
- Fixed universe / partial-universe policy for cross-market joins.
- Spot/perp mapping and cross-sectional operators.
- 5min manifest/checkpoint/recovery and consumer/index contract.

## Why HOLD

The repository defines `features_5min` as a future dataset, but the cross-market field set, consumer contract, and recovery namespace remain unapproved. Older documents contain draft indicators and must not be promoted silently.

1. Row grain and primary key: one row per market/window, one cross-market row/window, or both.
2. Input alignment: ten 30s windows, market universe, and timestamp key.
3. Universe: fixed 15 markets or partial-universe operation.
4. Spot/perp mapping and cross-market sample rules.
5. Empty, missing, not-yet-arrived, and verified-missing behavior.
6. Operator matrix and minimum output fields; direct vs rollup vs recompute names.
7. Quality schema: per-market coverage, active/inactive counts, missing list, sample size, finalized state, zero-denominator/std behavior.
8. Manifest/checkpoint/recovery and consumer/index contract.

## Safe implementation boundary after approval

Pure module + deterministic fixtures only. No pipeline wiring, persistence, manifest/checkpoint, consumer, Receiver/raw, cron, or Gateway changes until the pure contract passes its own 95-point gate.

## Existing facts

- `features_1s` and `features_30s` are implemented and verified.
- `features_5min` constant exists but no producer/committer/pipeline/recovery exists.
- Empty is valid; missing is fail-closed in existing layers.
