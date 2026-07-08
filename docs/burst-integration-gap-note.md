# Burst Integration Gap Note

**Status:** blocker identified  
**Scope:** post-bridge real-data validation readiness

---

## 1. What was attempted

The next planned step after the implementation bridge was a small real-data replay / integration validation.

Before running that, the current repo was checked for:
- existing `data/1s_features/...` outputs to inspect
- the live/replay entrypoint that should emit 1s rows
- the parquet aggregation script that should carry the new burst columns downstream

---

## 2. Blocker found

### 2.1 Live/replay path is not using `FeatureAccumulator`

Observed in `orderflow_monitor.mjs`:
- imports `FeatureComputer`
- constructs `const featureComputer = new FeatureComputer();`
- no observed import or construction of `FeatureAccumulator`

Observed repo-wide search result:
- `FeatureAccumulator` is currently referenced in tests and its own implementation file only
- no production entrypoint currently instantiates it

**Implication:** the burst feature implementation exists in `lib/feature-accumulator.mjs`, but the live pipeline is still writing the older feature shape through `FeatureComputer`.

### 2.2 Downstream parquet aggregation script still uses the old schema

Observed in `scripts/aggregate-1s.mjs`:
- `ALL_COLS` stops at the pre-burst schema
- no burst columns are present in the enumerated projection

**Implication:** even if burst-aware 1s JSONL were emitted, the current aggregation script would drop those columns unless it is updated.

### 2.3 No existing `data/1s_features/...` files were found for direct inspection

Observed under the repo data directory:
- many raw trade/depth/snapshot JSONL files exist in `data/raw_hot/...`
- no existing `data/1s_features/*.jsonl` or `data/agg/*.parquet` outputs were present to inspect directly

**Implication:** there is no already-materialized burst-aware output artifact available for quick spot-checking.

---

## 3. What this means

The bridge task is complete, but the repo has not yet crossed the next boundary:

**implemented feature logic** → **wired production/replay output path**

So the correct next task is not “inspect real burst rows” yet.
The correct next task is:

1. decide the cutover path from `FeatureComputer` to `FeatureAccumulator`
2. update the 1s aggregation / export scripts to include the burst columns
3. only then run real-data replay validation

---

## 4. Recommended next concrete step

Create a narrow integration plan that answers:
- where `FeatureAccumulator` replaces or complements `FeatureComputer`
- what output path becomes authoritative for `data/1s_features/...`
- how `scripts/aggregate-1s.mjs` should be updated so burst columns survive into parquet
- what one-market / one-window replay command will validate the cutover

Until that cutover is designed and implemented, real-data burst validation remains blocked.
