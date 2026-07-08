# Burst Integration Cutover Plan

**Status:** draft v1  
**Scope:** wire `FeatureAccumulator` into the real output path after the completed burst implementation bridge

---

## 1. Goal

Promote the burst-capable `FeatureAccumulator` from unit-tested library code to the authoritative 1s feature producer for live / replay output.

Success means:
- raw trade/depth input can flow into `FeatureAccumulator`
- `data/1s_features/...` rows include the burst columns
- `scripts/aggregate-1s.mjs` preserves those burst columns into parquet
- one real-data replay / smoke path proves the cutover on a small window

---

## 2. Current state

### What is already done
- `lib/feature-accumulator.mjs` implements the burst feature bridge through slice 4
- deterministic tests pass
- docs/spec/reconciliation/handoff are synchronized

### What is not yet wired
- `orderflow_monitor.mjs` still emits feature rows through `FeatureComputer`
- no production entrypoint currently instantiates `FeatureAccumulator`
- `scripts/aggregate-1s.mjs` still enumerates the pre-burst schema

---

## 3. Recommended cutover posture

### 3.1 Do not extend `FeatureComputer`
`FeatureComputer` is too small and semantically too different from `FeatureAccumulator`.
It only computes a thin best-bid/best-ask snapshot + aggregated trade totals. Trying to evolve it into the burst-capable feature engine would create duplicate responsibilities.

**Decision:** make `FeatureAccumulator` the authoritative 1s feature row producer for the burst-aware pipeline.

### 3.2 Keep the cutover additive at first
Do not rip out every old output path in one change.

Initial cutover should:
- keep raw trade/depth writers unchanged
- keep aggregated trade writers unchanged
- keep book snapshot writers unchanged
- add / switch only the 1s feature row production path

That gives the smallest blast radius.

---

## 4. Proposed implementation slices

### Slice A — live path wiring in `orderflow_monitor.mjs`

#### A1. Instantiate one `FeatureAccumulator`
Recommended output base for 1s rows:
- `path.join(outputBase, '1s_features')`

#### A2. Feed raw events directly
At connector event hooks:
- on `trade` → call `featureAccumulator.feedTrade(market, tradeEvent)`
- on `depth` → call `featureAccumulator.feedDepth(market, depthEvent, connector.book.getMid())` (or equivalent authoritative mid available at that moment)

#### A3. Flush at the existing 1s cadence
At the same 1s loop where aggregated trade rows are flushed:
- call `featureAccumulator.feedSecond(market, nowSecondTs, book)` for each writable market

#### A4. Choose authoritative 1s output path
Prefer:
- `FeatureAccumulator` writes directly to dated JSONL under `data/1s_features/<date>/<market>.jsonl`

Do **not** continue writing `features.jsonl` as the authoritative analytical output if it is still tied to the old `FeatureComputer` shape.

#### A5. Transitional compatibility
For one transitional step, the repo may keep `features.jsonl` if something else depends on it, but it should be treated as legacy.

---

### Slice B — downstream schema propagation

Update `scripts/aggregate-1s.mjs`:
- extend `ALL_COLS` to the burst-aware schema
- add integer typing for new count-like columns
- preserve nullable numeric columns for burst validation ratios/gap metrics

At minimum add these columns:
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
- `max_same_side_run_prints_1s`
- `side_flip_count_1s`
- `same_side_gap_ms_min_1s`
- `same_side_gap_ms_p25_1s`
- `burst_at_touch_ratio_1s`
- `burst_through_ratio_1s`
- `burst_depletion_count_1s`
- `burst_replenish_after_touch_count_1s`

This step is mandatory before parquet-level validation.

---

### Slice C — one-market smoke validation

Use existing raw_hot data for one market and one short time window.

Recommended first target:
- `binance_spot` or `binance_perp`

Validation objective:
- confirm `data/1s_features/<date>/<market>.jsonl` is created
- confirm at least one row contains the burst columns
- confirm nullable fields remain `null` when expected
- run `scripts/aggregate-1s.mjs` and confirm parquet contains the burst columns too

---

## 5. Minimal acceptance checklist

### For live wiring
- [ ] `FeatureAccumulator` is instantiated in a production entrypoint
- [ ] trade events feed into it
- [ ] depth events feed into it
- [ ] second flushes call `feedSecond()`
- [ ] dated `1s_features/...` files are written

### For schema propagation
- [ ] `scripts/aggregate-1s.mjs` includes burst columns in `ALL_COLS`
- [ ] count-like burst columns have integer types where appropriate
- [ ] nullable burst columns survive JSONL → parquet without zero-coercion

### For smoke validation
- [ ] one market produces burst-aware JSONL rows
- [ ] one parquet file contains the burst columns
- [ ] one sampled row is inspected manually

---

## 6. Recommended first coding target

Start with **Slice A only**:
- wire `FeatureAccumulator` into `orderflow_monitor.mjs`
- produce dated `1s_features/...` JSONL
- do not touch aggregation/parquet until the JSONL path is proven

Reason:
- smallest real cutover
- easiest to verify
- avoids mixing runtime wiring and downstream schema edits in one change

---

## 7. First replay/smoke command after wiring

After Slice A lands, use a short bounded run such as:

```bash
node orderflow_monitor.mjs \
  --config config.v3.json \
  --seconds 5 \
  --markets binance_spot \
  --output data/live_burst_smoke
```

Then inspect:
- `data/live_burst_smoke/1s_features/<date>/binance_spot.jsonl`
- confirm the burst fields exist on emitted rows

After Slice B lands:

```bash
node scripts/aggregate-1s.mjs --cutoff=<recent_ts>
```

and inspect the resulting parquet schema / sampled rows.

---

## 8. Non-goals for the first cutover

Do not in the first integration change:
- redesign the raw writer layout
- migrate every downstream consumer
- remove `FeatureComputer` everywhere without evidence
- combine live wiring + full downstream consumer refactor + historical backfill into one patch

Keep the cutover narrow and verifiable.
