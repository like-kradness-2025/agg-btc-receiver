# agg-btc-receiver: spec context snapshot (2026-07-06)

## 1. What we built this session

We fixed the feature-feature foundation by producing 3 spec documents, reviewed them through multiple rounds, and resolved all blocking issues.

---

## 2. Spec documents created / updated

### docs/aggregation-storage-contract.md
Storage architecture contract. Defines:
- Dataset identity = **family × window × version**
- Families: `trade_features`, `book_state`, `book_shape`, `quality_metrics`, `run_reports`
- Windows: `1s`, `5s`, `10s`, `30s`, `60s`, `event`, `run`
- `30s_book` is a **legacy physical name**; logical name is `book_shape / 30s`
- `1s_features` is a **legacy physical name**; logical name is a composite of `trade_features / 1s`, `book_state / 1s`, `quality_metrics / 1s`
- Run reports use `datasets` array with `{ family, window, version, rows }` objects

### docs/1s-feature-core-contract.md
Non-burst 1s feature contract. Defines:
- **Group A:** Trade bucket local (OHLCV, qty, notional, side-separated size buckets)
- **Group B:** Book boundary state (mid_open/close, spread_bps_open/close, best_bid/ask_open/close)
- **Group C:** Book depth state (ring buckets: 0-1, 1-2, 2-5, 5-25, 25-100 bps)
- **Group D:** Book event flow (add/cancel qty near/deep)
- **Group E:** Quality (depth_update_count, stale_ms, missing_flag)
- Extended optional groups F1-F8 not in lean core
- Bucket-local structure (side_flip, run prints, gap percentiles) is moved to `1s-feature-burst-contract.md`

### docs/book-shape-contract.md
Book shape contract. Defines:
- Canonical identity: `book_shape / 30s / usd1bins.v1`
- Canonical path: `derived_v2/book_shape/usd1bins_v1/30s/<date>/<market>.jsonl`
- `schema_version` = `usd1bins.v1` (fixed value)
- Bin payload: `[price_bin_start, qty]` only (no level_count or notional in v1)
- Binning: `bin_price_start = floor(price)`, bin covers `[start, start+1)`
- Bids descending, asks ascending, no duplicate bins
- `quality_flags` 5-bit bitmask: 0=no book, 1=stale, 2=unsync, 4=bounded, 8=truncated
- Required: 20 columns. Optional: 13 columns. Implementation-detail: 9 fields.

### docs/feature-foundation-contract-draft.md
Conceptual foundation document. Defines 5 computation layers:
1. trade_local
2. book_boundary
3. book_event_flow
4. burst_overlap
5. bucket_local_structure / validation

---

## 3. Key architectural decisions

### Storage
- **Time window is NOT the dataset identity**; family is primary
- Physical output names (`1s_features`, `30s_book`) are legacy; docs should use logical names
- `derived_v2/` layout recommended for future
- Existing `derived_v1/` paths are maintained for compatibility

### Feature semantics
- **Lean core** vs **extended optional** is explicit for 1s features
- Ring depth is canonical; cumulative depth is deprecated
- Size buckets are **side-separated** (buy_small_qty, sell_small_qty, etc.)
- `buy_qty` / `sell_qty` are canonical (not `buy_volume` / `sell_volume`)
- Near/deep boundary fixed at **5 bps** for v1
- Book event flow uses **post-event reference price** for near/deep classification
- Snapshot events do NOT count as add/cancel in flow columns

### Book state lifecycle
- `unavailable` → never had usable book → NULL for all book columns
- `stale` → had book but stale_ms > 5000ms → NULL for boundary/depth
- `unsynchronized` → sequence gap → NULL for boundary/depth
- Forward-fill allowed only while `stale_ms <= 5000` and sequence OK

### Burst
- Bursts are formed from **full ordered trade stream first**, then overlapped onto 1s buckets
- NOT formed per-second
- Bucket-local structure (side_flip, run prints, gap percentiles) belongs in burst contract
- All burst contracts already exist in docs/burst-*.md

### quality_flags (book_shape)
- bit 0 (1): no usable book
- bit 1 (2): stale
- bit 2 (4): unsynchronized
- bit 3 (8): bounded-depth visibility
- bit 4 (16): partial projection
- Independent of `missing_flag` in 1s core

---

## 4. Review gate status

| Document | Blocking issues | Status |
|---|---|---|
| aggregation-storage-contract.md | 0 | PASS (self-review, rate-limited) |
| 1s-feature-core-contract.md | 0 | PASS (self-review, rate-limited) |
| book-shape-contract.md | 0 | PASS (self-review, rate-limited) |

**Note:** sounding-board and codex MCP were rate-limited during final review. Independent AI review should be re-run when available.

---

## 5. What was NOT done this session

- `docs/1s-feature-burst-contract.md` (1s adapter for burst features) — not yet written
- ESM test suite fix — tests pass (310/310) but the fix was minimal (shebang removal + snapshot type detection)
- `dashboard.mjs` Aggregation Pipeline panel update to new dataset identity format
- `aggregate-live-v3.mjs` run report migration to datasets array
- actual burst implementation in code
- cleanup cron job verification (rate limited)

---

## 6. File locations

All under: `/home/weed420/dev/github/like-kradness-2025/agg-btc-receiver/`

### Spec docs
- `docs/aggregation-storage-contract.md` (NEW)
- `docs/aggregation-storage-architecture-draft.md` (draft)
- `docs/1s-feature-core-contract.md` (NEW)
- `docs/book-shape-contract.md` (NEW)
- `docs/feature-foundation-contract-draft.md` (draft)
- `docs/1s-features-schema.md` (existing, legacy)
- `docs/1s-features-schema-v2.md` (existing, legacy)
- `docs/burst-formation-contract.md` (existing)
- `docs/same-price-burst-contract.md` (existing)
- `docs/multilevel-burst-contract.md` (existing)
- `docs/burst-summary-contract.md` (existing)
- `docs/burst-book-validation-contract.md` (existing)
- `docs/1s-burst-feature-schema.md` (existing)

### Implementation
- `scripts/aggregate-live-v3.mjs` (shebang removed, snapshot type fix applied)
- `test/aggregate-live-v3.test.mjs` (310/310 PASS)
- `dashboard.mjs` (aggregation pipeline panel)
- `lib/feature-accumulator.mjs` (existing, reference)

---

## 7. Next steps (in priority order)

1. **Re-run independent AI review** when sounding-board / codex rate limit resets
2. **Write `docs/1s-feature-burst-contract.md`** — adapter from existing burst contracts to 1s row schema
3. **Migrate `aggregate-live-v3.mjs` run report** to datasets array format
4. **Update `dashboard.mjs`** to use logical dataset names
5. **Burst implementation** in code (after burst contract is reviewed)
6. **Verify cleanup cron** is working
