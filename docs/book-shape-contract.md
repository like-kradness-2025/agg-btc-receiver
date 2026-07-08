# Book Shape Contract

**Status:** draft v1  
**Logical identity:** `book_shape / 30s / usd1bins.v1`  
**Canonical path recommendation:** `derived_v2/book_shape/usd1bins_v1/30s/<date>/<market>.jsonl`  
**Canonical schema_version value:** `usd1bins.v1`  
**Purpose:** 現在の `30s_book` を、単なる 30 秒集約ではなく、**macro liquidity shape snapshot** として契約化する。  
**Storage relation:** 現在の `30s_book` physical output は legacy name として許容。論理名は `book_shape / 30s` とする。

---

## 1. Definition

`book_shape` は window 終端時点の book state を price-bin で圧縮した macro liquidity shape dataset である。

これは:
- tick-level stream ではない
- diff replay log ではない
- queue reconstruction dataset ではない

本質は:
- 価格帯ごとの板量
- best bid / ask
- spread / mid
- coverage tier
- その window で観測できた book availability

を、下流が macro な流動性形状として読めるようにすることにある。

---

## 2. Family / window / version

### 2.1 Family
- `book_shape`

### 2.2 Window
- current first view: `30s`

### 2.3 Version
- current first contract: `usd1bins.v1`

### 2.4 Rule
30 秒は family identity ではない。

つまり:
- `book_shape / 30s / usd1bins.v1`
- `book_shape / 60s / usd1bins.v1`

は同 family の別 view である。

---

## 3. Row semantics

### 3.1 Row unit
1 row = `market × window`

For current view:
- window = `[window_start_ms, window_end_ms)`
- `window_end_ms = window_start_ms + 30000`

### 3.2 Snapshot timing
book_shape row is a **window-end projection**.

Canonical meaning:
- the row represents the latest usable book state available for that market within the window, interpreted as the shape for that window end

### 3.3 Not a per-event reconstruction log
A row does not claim to preserve every depth event. It preserves the projected shape only.

---

## 4. Availability semantics

### 4.1 One row per market × window
- emit one row per market × window even when usable book state is unavailable

### 4.2 `book_available`
`book_available = true` means:
- there was usable book state for that market at some point within the window
- required top-of-book and bin projection fields can be interpreted (may be stale per 4.3)

`book_available = false` means:
- no usable book state existed for that row
- structural fields must follow null/empty rules

### 4.3 Stale handling (resolved for v1)
A row where usable book existed but the **last usable state is stale relative to window_end** is treated as:

- `book_available = true` (because usable state existed in the window)
- `quality_flags` bit 1 must be set
- **shape fields are emitted as non-NULL** using the latest usable state
- downstream consumers must check quality_flags before interpreting stale shape fields

This decision means:
- stale does NOT force NULL
- stale does NOT force `book_available = false`
- stale is a quality annotation, not an erasure condition

### 4.4 Coverage tier is separate from availability
A market can have:
- `coverage_tier = tier_c_bounded_depth_near_book`
- `book_available = true`

This means the row is available but only within bounded exchange visibility.

---

## 5. Required columns

These are required for `book_shape / 30s / usd1bins.v1`.

| Column | Type | Semantics |
|---|---|---|
| `schema_version` | STRING | fixed value `usd1bins.v1` |
| `market` | STRING | market key |
| `window_start_ms` | BIGINT | inclusive window start |
| `window_end_ms` | BIGINT | exclusive window end |
| `coverage_tier` | STRING | tier metadata from market coverage contract |
| `book_available` | BOOLEAN | whether usable book state exists in window |
| `best_bid` | DOUBLE | best bid price (NULL if not available) |
| `best_ask` | DOUBLE | best ask price (NULL if not available) |
| `mid` | DOUBLE | midpoint price (NULL if not available) |
| `spread` | DOUBLE | absolute spread (NULL if not available) |
| `spread_bps` | DOUBLE | spread in bps (NULL if not available) |
| `bid_level_count` | BIGINT | count of visible bid price levels used in shape projection (0 if none) |
| `ask_level_count` | BIGINT | count of visible ask price levels used in shape projection (0 if none) |
| `bid_qty_total` | DOUBLE | total visible bid qty included in row (0 if none) |
| `ask_qty_total` | DOUBLE | total visible ask qty included in row (0 if none) |
| `bid_bins` | JSON/ARRAY | descending array of `[price_bin_start, qty]` (empty if none) |
| `ask_bins` | JSON/ARRAY | ascending array of `[price_bin_start, qty]` (empty if none) |
| `quality_flags` | BIGINT | bitmask (see 5.1 for bit definitions) |
| `window_update_count` | BIGINT | count of depth update events seen in window |
| `window_snapshot_count` | BIGINT | count of snapshot events seen in window |

### 5.1 `quality_flags` bit definitions

| Bit | Value | Meaning |
|---|---|---|
| 0 | 1 | no usable book state existed in this window |
| 1 | 2 | book state is stale relative to window_end (threshold: stale_ms > 5000ms, same as 1s-feature-core §4.3) |
| 2 | 4 | sequence integrity is broken (unsynchronized) |
| 3 | 8 | bounded-depth visibility (coverage_tier = tier_c or equivalent) |
| 4 | 16 | partial projection (bin range truncated, not full visible depth) |

Rules:
- `quality_flags = 0` means no issues detected
- multiple bits may be set simultaneously
- if `book_available = false`, bit 0 must be set
- `quality_flags` is independent of `missing_flag` in 1s-feature-core; they are not guaranteed to share bit semantics

---

## 6. Optional columns

Valid extensions but not required for v1 core acceptance:

| Column | Type | Semantics |
|---|---|---|
| `exchange` | STRING | venue / exchange identifier |
| `book_age_ms` | BIGINT | age of last usable book state relative to window end |
| `book_state` | STRING | coarse state enum (e.g. usable/stale/missing) |
| `invalid_reason` | STRING | explanation when `book_available=false` |
| `bid_notional_total` | DOUBLE | optional sum of bid bin notional |
| `ask_notional_total` | DOUBLE | optional sum of ask bin notional |
| `imbalance_total` | DOUBLE | optional aggregate imbalance |
| `missing_flag` | BIGINT | optional compatibility flag; not guaranteed to share bit semantics with `quality_flags` |
| `window_skip_count` | BIGINT | optional skipped / ignored event count |
| `anchor_ts_ms` | BIGINT | snapshot anchor timestamp if reconstruction exposes it |
| `last_book_event_ts_ms` | BIGINT | last event ts contributing to row |
| `last_seq` | STRING/DOUBLE | last known sequence reference if meaningful |
| `seq_status` | STRING | coarse sequence integrity status |

---

## 7. Implementation-detail fields (non-canonical)

Implementation detail unless explicitly promoted later:
- raw `stream` names
- raw connector-specific timestamp duplicates
- `snapshot_ts` duplicates when equivalent to anchor metadata
- hard-coded binning constants serialized into each row
- redundant booleans that restate `book_available`
- redundant count fields that can be derived from bin arrays

These may exist in temporary outputs but are not part of the canonical contract.

---

## 8. Binning semantics

### 8.1 Default binning
Current canonical first view:
- price bins are `$1` wide
- `bin_price_start = floor(price)` for both sides
- each bin covers `[bin_price_start, bin_price_start + 1)`
- bids aggregate by `bin_price_start` and are serialized in descending order
- asks aggregate by `bin_price_start` and are serialized in ascending order
- duplicate bin keys are forbidden within one row side
- qty unit is BTC qty

### 8.2 Bounded range
Current implementation may bound output around mid.
This is acceptable if documented by version and if downstream does not mistake bounded visibility for full-book visibility.

### 8.3 Bin representation
For `usd1bins.v1`, each bin element is fixed as:
- `[price_bin_start, qty]`

Rules:
- `price_bin_start` is numeric lower bound of the bin
- `qty` is aggregated BTC qty in that bin
- no `level_count` or `notional` payload is included in v1
- empty array means no visible levels were projected for that side in the row

Consumers must not infer richer payload fields in `usd1bins.v1`.

---

## 9. Null / empty rules

### 9.1 When `book_available = false`
- `best_bid`, `best_ask`, `mid`, `spread`, `spread_bps` → NULL
- `bid_level_count`, `ask_level_count` → 0
- `bid_qty_total`, `ask_qty_total` → 0
- `bid_bins`, `ask_bins` → empty arrays
- `quality_flags` bit 0 must be set

### 9.2 When `book_available = true`
- scalar shape fields should be non-NULL if derivable from usable state
- scalar shape fields may be stale (quality_flags bit 1 set) but still non-NULL
- count / qty totals may be 0 only if visible shape is genuinely empty

### 9.3 Coverage tier does not force NULL
A bounded-depth venue may still emit valid non-NULL shape fields if the visible book state is usable.

---

## 10. Coverage-tier interpretation

`coverage_tier` is mandatory because different markets expose different amounts of visible book.

Downstream must not compare:
- breadth
- total visible qty
- far-book shape

across markets as if all venues exposed identical depth.

Interpretation rule:
- `book_shape` is always tier-aware
- `tier_c_bounded_depth_near_book` rows are macro proxies, not far-book truth

---

## 11. Path recommendation

### 11.1 Logical recommendation
Canonical recommended logical layout:
- `derived_v2/book_shape/usd1bins_v1/30s/<date>/<market>.jsonl`

### 11.2 Legacy compatibility
Current `derived_v1/30s_book/...` is allowed as a physical compatibility path.

But in docs and future run reports, this dataset should be referred to as:
- `book_shape / 30s / usd1bins.v1`

---

## 12. Relation to current implementation

### 12.1 What is canonical now
Canonical enough to keep:
- macro liquidity-shape purpose
- per-market per-window rows
- best bid / ask / mid / spread
- price-bin arrays
- coverage tier
- bounded depth interpretation

### 12.2 What is implementation detail for now
Not yet canonical:
- exact internal reconstruction state machine exposure
- exact seq fields
- exact anchor bookkeeping
- richer alternative bin payloads beyond `[price_bin_start, qty]`
- exact stale / invalid flag vocabulary (beyond quality_flags bits)

---

## 13. Open questions

1. Is `exchange` required or merely convenient redundancy?
2. Should `book_age_ms` be required in v1?
3. When should the two live implementations converge to one canonical writer?
4. Is `book_state` needed if `book_available + quality_flags` already exist?
5. How should non-USD or non-$1 bin variants be versioned?

---

## 14. Exit check

This document is acceptable only if:
- `30s_book` is reinterpreted as logical `book_shape / 30s`
- required columns are distinguished from optional ones
- stale handling is explicit: stale rows emit non-NULL shape fields with quality_flags bit 1
- availability semantics are explicit
- coverage-tier interpretation is explicit
- canonical semantics are separated from implementation detail
- quality_flags bit table is properly nested under the required columns section
