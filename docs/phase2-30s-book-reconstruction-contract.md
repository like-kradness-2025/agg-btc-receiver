# Phase2 30s_book reconstruction contract

**Status:** draft v3 for review  
**Scope:** deterministic 30s book reconstruction from `data/live_v3` finalized raw windows  
**Depends on:** `docs/phase2-live-v3-raw-schema-contract.md`, `docs/book-coverage-tiers.md`  
**Output:** `data/30s_book/<YYYY-MM-DD>/<market>.jsonl`

---

## 1. Purpose

This contract defines how the phase2 downstream aggregator reconstructs the visible order book from finalized `live_v3` raw windows and emits one macro liquidity row per 30-second window per market.

`30s_book` is the **macro book-shape layer**. It is not a tick-level feature stream and does not claim complete hidden exchange depth.

---

## 2. Input scope

### 2.1 Included raw-window inputs

Read only finalized `.jsonl` files from:

```text
data/live_v3/snapshots/<market>/<YYYY-MM-DD>/<HH-MM-SS>.jsonl
data/live_v3/book_updates/<market>/<YYYY-MM-DD>/<HH-MM-SS>.jsonl
```

All row parsing and validation follows `docs/phase2-live-v3-raw-schema-contract.md`.

### 2.2 Excluded inputs

Do not use these files in phase2a `30s_book` reconstruction:

```text
data/live_v3/book/<market>.jsonl
data/live_v3/trades/<market>.jsonl
data/live_v3/trades/<market>/<date>/<window>.jsonl
data/live_v3/liquidations/<market>/<date>/<window>.jsonl
```

Rationale:

- flat `book/<market>.jsonl` is a side-output / legacy periodic book stream, not part of the fixed raw-window input set
- trade and liquidation streams do not mutate book state
- adding extra anchor sources would make manifest and replay semantics ambiguous

### 2.3 Invocation inputs

The invocation/manifest contract supplies:

- fixed manifest file set
- requested markets
- market -> exchange metadata
- aligned `emit_start_ms` and `emit_end_ms`

This contract requires `emit_start_ms % 30000 == 0` and `emit_end_ms % 30000 == 0`. All anchor discovery and replay are limited to the fixed manifest. Output `exchange` comes from invocation market metadata, not from arbitrary row exchange values.

### 2.4 File eligibility

- Read files ending exactly in `.jsonl`.
- Never read `.jsonl.open`.
- Never read `.processing`, `.processed`, `.conflict`, or quarantine paths.
- In live mode, skip current and previous 30s windows; invocation contract owns the exact skip rule.

---

## 3. Time and range semantics

30s windows are UTC epoch aligned:

```text
window_start_ms = floor(ts_ms / 30000) * 30000
window_end_ms   = window_start_ms + 30000
window interval = [window_start_ms, window_end_ms)
```

Emit range is half-open:

```text
[emit_start_ms, emit_end_ms)
```

Emit one row for every market/window where:

```text
emit_start_ms <= window_start_ms < emit_end_ms
```

Output partition date is derived from `window_start_ms` in UTC.

---

## 4. Replay start / warm-up

A 30s row must be based on a real book anchor, not an arbitrary fixed warm-up length.

For each market, before emitting the first requested window, the replay must:

1. find the latest eligible snapshot whose complete ordering key (§5) is before the first emitted window start
2. initialize the book from that snapshot
3. replay every snapshot/update event in the fixed manifest whose complete ordering key is after the selected anchor key and whose `effective_ts_ms < first_window_end_ms`

If no eligible snapshot exists before `emit_start_ms`, the first and later rows emit with `book_available=false` until an eligible snapshot appears inside the replay stream.

This avoids the invalid assumption that `emit_start_ms - 30000` is enough warm-up.

---

## 5. Deterministic event ordering

Normalize rows via the raw schema contract, then sort by:

1. `effective_ts_ms` ascending
2. subtype priority:
   - `snapshot_file` = 0
   - `book_update_snapshot` = 1
   - `book_update_update` = 2
3. `file_path` lexical ascending
4. `line_no` ascending

Rows at exactly `window_end_ms` belong to the next window.

---

## 6. Book state

Maintain one state per market:

```text
bids: Map<canonical_price_decimal, canonical_qty_decimal>
asks: Map<canonical_price_decimal, canonical_qty_decimal>
book_state: UNSEEDED | SEEDED | INVALIDATED
last_seq: integer | null
seq_status: unavailable | unverified | verified_monotonic
anchor_epoch_seq_decrease_seen: boolean
anchor_ts_ms: integer | null
last_book_event_ts_ms: integer | null
invalid_reason: string | null
```

Best levels use numeric decimal ordering:

```text
best_bid = max(bid prices with qty > 0)
best_ask = min(ask prices with qty > 0)
```

A usable book is:

```text
book_state == SEEDED
bid side non-empty
ask side non-empty
best_bid < best_ask
```

---

## 7. Snapshot handling

### 7.1 Eligible snapshot sources

A snapshot can come from:

1. `snapshots/<market>/<date>/<window>.jsonl`
2. `book_updates/<market>/<date>/<window>.jsonl` rows with `type: "snapshot"`

### 7.2 Eligibility

A snapshot is eligible iff:

1. raw schema validation passes
2. both `bids` and `asks` are non-empty
3. every level is valid
4. applying positive-qty levels yields `best_bid < best_ask`

`bidLevelCount` / `askLevelCount` mismatch is a quality warning, not a rejection.

Non-eligible snapshots are atomic no-ops: they do not mutate book state, sequence state, anchor timestamp, or last book event timestamp.

### 7.3 Application

Applying a snapshot:

1. clears both sides
2. inserts all positive-qty levels
3. resolves duplicate prices by **last-wins within the row**
4. drops levels whose qty normalizes to `0`
5. sets `book_state = SEEDED`
6. sets `anchor_ts_ms = snapshot.effective_ts_ms`
7. sets `last_book_event_ts_ms = snapshot.effective_ts_ms`
8. sets `last_seq = snapshot.seq` if non-null else `null`
9. sets `seq_status = unavailable` if no seq else `unverified`
10. sets `anchor_epoch_seq_decrease_seen = false`
11. clears `invalid_reason`

A `book_updates.type == "snapshot"` row may re-seed an invalidated book mid-replay.

---

## 8. Update handling

Only `book_updates.type == "update"` rows are incremental diffs.

For each update row:

1. validate the whole row first and report raw-invalid rows per the raw contract
2. if invalid, drop atomically; do not mutate book state; raw-invalid rows are reported only in the run report, not row-local fields
3. if `book_state != SEEDED`, skip the update atomically and do not mutate persistent state; increment `window_skip_count` when the skipped update is inside the emitted window
4. resolve duplicate prices by last-wins within each side of the row
4. apply bid changes:
   - qty `0` deletes level
   - qty `> 0` sets level
5. apply ask changes with same rules
6. set `last_book_event_ts_ms = update.effective_ts_ms` if mutation is not stale-dropped
7. after mutation, if either side is empty or `best_bid >= best_ask`, set:

```text
book_state = INVALIDATED
invalid_reason = crossed_or_empty_book
```

Once invalidated, the book remains unavailable until a later eligible snapshot re-seeds it.

---

## 9. Sequence handling

`live_v3` does not provide universal lineage. `prevSeq` is generally absent, some venues omit `seq`, and Kraken-like fields may decrease in persisted samples. Therefore seq is a quality signal, not proof of exchange-grade synchronization.

### 9.1 Initial values

```text
last_seq = null
seq_status = unavailable
```

### 9.2 Update-row sequence rules

Sequence checks apply only to `book_updates.type == "update"` rows and run before mutation. Snapshot rows initialize `last_seq` and `seq_status` by §7.3.

Within one anchor epoch, `seq_status=unverified` is sticky after a decrease. This is represented by `anchor_epoch_seq_decrease_seen=true`. A later increase does not restore `verified_monotonic` until the next eligible snapshot resets the flag.

| condition | action |
|---|---|
| seq absent/null | preserve `last_seq`; preserve `seq_status`; add `seq_unavailable` |
| `last_seq == null` and seq integer | set `last_seq=seq`; set `seq_status=verified_monotonic` for monotonic-capable markets, else `unverified` |
| seq integer and `seq > last_seq` | if `seq > last_seq + 1`, add `seq_jump_observed`; set `last_seq=seq`; if `anchor_epoch_seq_decrease_seen=false`, set status by market capability; otherwise keep `seq_status=unverified` |
| seq integer and `seq == last_seq` | atomically skip update mutation; add `stale_seq_drop` |
| seq integer and `seq < last_seq` | keep `last_seq`; set `anchor_epoch_seq_decrease_seen=true`; set `seq_status=unverified`; add `seq_decrease_observed`; apply row unless it otherwise invalidates book |

### 9.3 Monotonic-capable markets

```text
binance_perp
binance_perp_btcusdc
binance_spot
binance_spot_usdc
bybit_perp
bybit_spot
coinbase_spot
okx_perp
okx_spot
```

All other markets use `unavailable` or `unverified` status.

---

## 10. Window close and row emission

At each window close:

```text
process all events with effective_ts_ms < window_end_ms
then emit exactly one row
```

A row is emitted even when book is unavailable.

`book_available=true` iff usable book condition from §6 holds.

Canonical `invalid_reason` values:

```text
null
no_snapshot_anchor
crossed_or_empty_book
unsynchronized
```

Reason precedence:

1. if no eligible snapshot has ever been accepted: `no_snapshot_anchor`
2. else if state is `INVALIDATED`: current `invalid_reason`
3. else if state is not `SEEDED`: `unsynchronized`
4. else `null`

---

## 11. Output schema

Each row is a JSON object serialized in this exact field order:

| field | type / rule |
|---|---|
| `schema_version` | string, `"30s_book.v1"` |
| `market` | string |
| `exchange` | string |
| `coverage_tier` | string |
| `window_start_ms` | integer |
| `window_end_ms` | integer |
| `book_available` | boolean |
| `book_state` | string |
| `invalid_reason` | string or null |
| `seq_status` | string |
| `anchor_ts_ms` | integer or null |
| `last_book_event_ts_ms` | integer or null |
| `last_seq` | integer or null |
| `book_age_ms` | integer or null, `window_end_ms - last_book_event_ts_ms` |
| `best_bid` | number or null |
| `best_ask` | number or null |
| `mid` | number or null |
| `spread` | number or null |
| `spread_bps` | number or null |
| `bid_level_count` | integer |
| `ask_level_count` | integer |
| `bid_qty_total` | number |
| `ask_qty_total` | number |
| `bid_notional_total` | number |
| `ask_notional_total` | number |
| `imbalance_qty` | number or null |
| `bid_bins_1usd` | array, §12 |
| `ask_bins_1usd` | array, §12 |
| `missing_flag` | integer |
| `quality_flags` | array<string>, sorted lexically |
| `window_update_count` | integer |
| `window_snapshot_count` | integer |
| `window_skip_count` | integer |

No wall-clock field is included. Invocation/run reports may record write time separately.

### 11.1 Field formulas and unavailable values

All formulas use exact unrounded decimals internally; §13 rounding applies only to final emitted values.

When `book_available=false`:

- `best_bid`, `best_ask`, `mid`, `spread`, `spread_bps`, `imbalance_qty`, `book_age_ms`, and notional totals are `null`
- level counts, qty totals, bin counts, window counters are integers/numbers and use `0` when no usable state exists
- bin arrays are `[]`

When `book_available=true`:

```text
best_bid = max bid price
best_ask = min ask price
mid = (best_bid + best_ask) / 2
spread = best_ask - best_bid
spread_bps = (spread / mid) * 10000
bid_qty_total = sum(qty over bids)
ask_qty_total = sum(qty over asks)
bid_notional_total = sum(price * qty over bids)
ask_notional_total = sum(price * qty over asks)
imbalance_qty = (bid_qty_total - ask_qty_total) / (bid_qty_total + ask_qty_total)
book_age_ms = window_end_ms - last_book_event_ts_ms
```

If denominator for `imbalance_qty` is zero, emit `null`.

---

## 12. Price binning

`30s_book` emits aggregated absolute $1 bins.

Each bin element is positional:

```text
[bin_price_int, qty_number, notional_number, level_count_int]
```

Bid bin rule:

```text
bin_price_int = floor(price)
```

Ask bin rule:

```text
bin_price_int = ceil(price)
```

For every positive-qty visible level:

```text
qty += level.qty
notional += level.price * level.qty
level_count += 1
```

Sort order:

- `bid_bins_1usd`: `bin_price_int` descending
- `ask_bins_1usd`: `bin_price_int` ascending

---

## 13. Numeric serialization

All emitted scalar and bin numeric values are JSON numbers.

Canonical rules:

1. use exact decimal arithmetic internally
2. round derived decimal values to 8 decimal places
3. rounding mode: half-away-from-zero
4. strip trailing zeros after decimal point
5. serialize `-0` as `0`
6. never emit exponent notation
7. integer fields emit as JSON integers

Fields using this decimal policy include:

```text
best_bid, best_ask, mid, spread, spread_bps,
bid_qty_total, ask_qty_total,
bid_notional_total, ask_notional_total,
imbalance_qty, bin qty, bin notional
```

---

## 14. Missing and quality flags

Flags are window-local.

### 14.1 missing_flag bits

| bit | value | meaning |
|---|---:|---|
| 0 | 1 | no usable book event applied inside this window |
| 1 | 2 | book unavailable at close |
| 2 | 4 | no eligible snapshot anchor has ever been accepted |

### 14.2 quality_flags

Known flags and exact emit rules:

- `crossed_or_empty_book`: add when an applied update or eligible snapshot produces empty side or crossed book; if this happens at close, `book_available=false`.
- `level_count_mismatch`: add for a snapshot in this window whose declared level count differs from array length.
- `non_eligible_snapshot`: add for a snapshot in this window that fails eligibility.
- `seq_decrease_observed`: add for update sequence decrease in this window.
- `seq_jump_observed`: add for update sequence jump in this window.
- `seq_unavailable`: add when a valid event in this window lacks usable seq.
- `stale_seq_drop`: add when a same-seq update is stale-dropped in this window.
- `unknown_coverage_tier`: add when market is absent from coverage map.

Known flag vocabulary:

```text
book_age_gt_30s
book_age_gt_120s
crossed_or_empty_book
level_count_mismatch
non_eligible_snapshot
seq_decrease_observed
seq_jump_observed
seq_unavailable
stale_seq_drop
unknown_coverage_tier
```

`book_age_gt_120s` implies `book_age_gt_30s`; both may appear. Age flags use strict `>` comparisons when `book_age_ms` is non-null.

Empty `bids:[]` and `asks:[]` updates are valid no-op updates. When `book_state==SEEDED`, they set `last_book_event_ts_ms`, increment `window_update_count`, and clear missing bit 0 for that window.

Window counters reset at every window start and exclude warm-up events before `emit_start_ms`:

- `window_update_count`: valid update rows in the window that are not stale-dropped and are applied or accepted as no-op while `book_state==SEEDED`
- `window_snapshot_count`: eligible snapshot rows in the window that re-seed state
- `window_skip_count`: stale-dropped updates plus updates skipped because state is not `SEEDED`; raw-invalid rows are counted in the run report, not here

`missing_flag` bit 0 is clear only if `window_update_count + window_snapshot_count > 0`; otherwise set.

---

## 15. Coverage tier

Use the exact mapping from `docs/book-coverage-tiers.md`:

```json
{
  "coinbase_spot": "tier_a_full_book_like",
  "bitmex_perp": "tier_a_full_book_like",
  "binance_spot": "tier_a_full_book_like",
  "binance_spot_usdc": "tier_a_full_book_like",
  "kraken_spot": "tier_a_full_book_like",
  "binance_perp": "tier_b_snapshot_limited_mid_depth",
  "binance_perp_btcusdc": "tier_b_snapshot_limited_mid_depth",
  "bybit_perp": "tier_b_snapshot_limited_mid_depth",
  "okx_perp": "tier_c_bounded_depth_near_book",
  "okx_spot": "tier_c_bounded_depth_near_book",
  "bybit_spot": "tier_c_bounded_depth_near_book",
  "bitstamp_spot": "tier_c_bounded_depth_near_book",
  "bitfinex_spot": "tier_c_bounded_depth_near_book",
  "crypto_com_spot": "tier_c_bounded_depth_near_book",
  "hyperliquid_perp": "tier_c_bounded_depth_near_book"
}
```

Unknown market:

- `coverage_tier="unknown"`
- add `unknown_coverage_tier`
- do not fail the run solely for unknown tier

---

## 16. Determinism

A fixed manifest + fixed emit range must produce identical rows.

Requirements:

1. input file set is fixed by invocation manifest
2. row ordering follows §5
3. output partition is UTC date of `window_start_ms`
4. rows are sorted by `window_start_ms` ascending
5. no `Date.now()` / mtime / filesystem ordering / locale formatting
6. JSON field order follows §11
7. bin ordering follows §12
8. no append-in-place; invocation contract must write complete output atomically

---

## 17. Conformance fixtures

Implementation must include fixtures for:

1. no snapshot before emit range → `no_snapshot_anchor`
2. snapshot before emit range + updates before first window → first row includes replayed updates
3. update exactly at `window_end_ms` belongs to next window
4. embedded `book_updates.type="snapshot"` re-seeds state
5. raw-invalid update is omitted before reconstruction and does not affect row-local counters
6. duplicate price in one row is last-wins
7. Bitfinex empty-string qty deletes level
8. crossed book invalidates until next snapshot
9. seq absent/null does not invalidate
10. seq decrease records quality flag
11. bins use bid-floor / ask-ceil and deterministic sort
12. numeric serialization never emits exponent or `-0`

---

## 18. Non-goals

This contract does not define:

- 1s feature replay
- trade aggregation
- liquidation aggregation
- marker / cleanup lifecycle
- parquet conversion
- live tail scheduling
- flat `data/live_v3/book/<market>.jsonl` as input
