# 1s Feature Core Contract

**Status:** draft v1  
**Scope:** 非 burst 系 1 秒特徴量の意味・命名・NULL/0 ルールを固定する。  
**Out of scope:** burst overlap, bucket-local burst validation（→ `docs/1s-feature-burst-contract.md` に分離）  
**Governing foundation:** `docs/feature-foundation-contract-draft.md`  
**Storage relation:** 本 contract の列群は論理的には `trade_features / 1s / core.v1`、`book_state / 1s / core.v1`、`quality_metrics / 1s / core.v1` にまたがる。現状の physical output では `1s_features` row に混在してよい。
**Scope clarification vs foundation draft:** `docs/feature-foundation-contract-draft.md` では bucket-local structure を core 候補に含めていたが、v1 では `max_same_side_run_prints_1s` / `side_flip_count_1s` / `same_side_gap_ms_*` を `docs/1s-feature-burst-contract.md` 側へ移管する。理由は、burst-associated structure / validation と一緒に review した方が意味境界が割れにくいため。

---

## 1. 時間基準

| 項目 | 値 |
|------|-----|
| 1 行 | `market × 1 second` |
| second bucket | `[ts, ts + 1000)` |
| `ts` | epoch ms を秒 floor した値 |
| 時刻基準 | 原則 `exchange_event_time`。exchange 時刻がない venue のみ `recv_time` フォールバック |
| boundary state | `*_open` = 秒開始時点の forward-filled 最新 book state。`*_close` = 秒終了時点 |
| 出力単位 | absolute price = native quote, relative = `*_bps`, flow = BTC qty (`*_qty`), depth = BTC qty |

---

## 2. Canonical column groups

本 contract は burst 系を除く 1s core を、次の 5 群で定義する。

1. Trade bucket local
2. Book boundary state
3. Book depth state
4. Book event flow
5. Quality / observability

---

## 3. Group A — Trade bucket local

その 1 秒の trade だけで閉じる特徴量群。

### 3.1 Required core columns

| Column | Type | Semantics | Null/Zero |
|---|---|---|---|
| `ts` | BIGINT | second start epoch ms | never NULL |
| `market` | STRING | market key | never NULL |
| `open` | DOUBLE | first trade price in bucket | NULL when trade_count=0 |
| `high` | DOUBLE | max trade price in bucket | NULL when trade_count=0 |
| `low` | DOUBLE | min trade price in bucket | NULL when trade_count=0 |
| `close` | DOUBLE | last trade price in bucket | NULL when trade_count=0 |
| `vwap` | DOUBLE | `Σ(price×qty) / Σqty` | NULL when trade_count=0 |
| `trade_count` | BIGINT | number of trade prints in bucket | 0 when no trades |
| `buy_qty` | DOUBLE | buy-side BTC qty | 0 |
| `sell_qty` | DOUBLE | sell-side BTC qty | 0 |
| `buy_notional` | DOUBLE | buy-side quote notional | 0 |
| `sell_notional` | DOUBLE | sell-side quote notional | 0 |
| `delta_notional` | DOUBLE | `buy_notional - sell_notional` | 0 |

### 3.2 Size bucket columns

Canonical size bucket semantics are **side-separated**. Total-only buckets are not canonical.

Thresholds:
- small: notional < $1,000
- medium: $1,000 ≤ notional < $10,000
- large: notional ≥ $10,000

Required columns:

| Column | Type | Semantics |
|---|---|---|
| `buy_small_qty` | DOUBLE | buy qty, small notional |
| `buy_medium_qty` | DOUBLE | buy qty, medium notional |
| `buy_large_qty` | DOUBLE | buy qty, large notional |
| `buy_small_count` | BIGINT | buy trade count, small notional |
| `buy_medium_count` | BIGINT | buy trade count, medium notional |
| `buy_large_count` | BIGINT | buy trade count, large notional |
| `sell_small_qty` | DOUBLE | sell qty, small notional |
| `sell_medium_qty` | DOUBLE | sell qty, medium notional |
| `sell_large_qty` | DOUBLE | sell qty, large notional |
| `sell_small_count` | BIGINT | sell trade count, small notional |
| `sell_medium_count` | BIGINT | sell trade count, medium notional |
| `sell_large_count` | BIGINT | sell trade count, large notional |

All size bucket columns use `0` when no qualifying trades exist.

### 3.3 Naming decisions

Resolved:
- `buy_qty` / `sell_qty` are canonical.
- legacy names such as `buy_volume` / `sell_volume` are non-canonical.
- side-separated size buckets are canonical.
- total-only bucket names such as `small_volume` / `medium_volume` / `large_volume` are non-canonical.

---

## 4. Group B — Book boundary state

秒開始 / 秒終了時点の top-of-book state を表す。

### 4.1 Required core columns

| Column | Type | Semantics | Null/Zero |
|---|---|---|---|
| `mid_open` | DOUBLE | mid at second start | NULL when book unavailable |
| `mid_close` | DOUBLE | mid at second end | NULL when book unavailable |
| `spread_bps_open` | DOUBLE | spread bps at second start | NULL when book unavailable |
| `spread_bps_close` | DOUBLE | spread bps at second end | NULL when book unavailable |
| `best_bid_open` | DOUBLE | best bid price at second start | NULL when book unavailable |
| `best_ask_open` | DOUBLE | best ask price at second start | NULL when book unavailable |
| `best_bid_close` | DOUBLE | best bid price at second end | NULL when book unavailable |
| `best_ask_close` | DOUBLE | best ask price at second end | NULL when book unavailable |

### 4.2 Rule
`*_open` / `*_close` are boundary-state columns, not event-flow columns.
They may be forward-filled from prior usable book state, subject to stale / missing rules.

### 4.3 Usable / stale / unsynchronized semantics
For v1, define:
- `last_usable_depth_update_ts_ms` = timestamp of the latest depth update or snapshot that produced a usable book state
- `stale_ms(boundary_ts) = boundary_ts - last_usable_depth_update_ts_ms`
- `stale_threshold_ms = 5000`

Interpretation rules:
- if no usable book state has ever been established, state is `unavailable`
- if usable book exists but `stale_ms > stale_threshold_ms`, state is `stale`
- if connector / replay state marks sequence integrity broken and no repaired snapshot has re-established a usable book, state is `unsynchronized`

NULL rules for boundary/depth columns:
- unavailable → NULL
- stale → NULL
- unsynchronized → NULL

Forward-fill is allowed only while `stale_ms <= stale_threshold_ms` and sequence integrity remains usable.

---

## 5. Group C — Book depth state

秒終了時点の ring-bucketed depth state を表す。

### 5.1 Canonical ring definition

Inner rings:
- `0-1 bps`
- `1-2 bps`
- `2-5 bps`

Outer rings:
- `5-25 bps`
- `25-100 bps`

Reference point:
- inner rings use best bid / best ask
- outer rings use mid

### 5.2 Required core columns

| Column | Type | Null/Zero |
|---|---|---|
| `bid_0_1bps` | DOUBLE | NULL when book unavailable |
| `bid_1_2bps` | DOUBLE | NULL |
| `bid_2_5bps` | DOUBLE | NULL |
| `bid_5_25bps` | DOUBLE | NULL |
| `bid_25_100bps` | DOUBLE | NULL |
| `ask_0_1bps` | DOUBLE | NULL |
| `ask_1_2bps` | DOUBLE | NULL |
| `ask_2_5bps` | DOUBLE | NULL |
| `ask_5_25bps` | DOUBLE | NULL |
| `ask_25_100bps` | DOUBLE | NULL |

### 5.3 Naming decision
Ring buckets are canonical. Old cumulative depth buckets are non-canonical.

### 5.4 Exact interval contract

All rings are half-open intervals except the outermost, which is closed on the right:

| Ring | Interval | Reference point |
|---|---|---|
| 0-1 bps | `[0, 1)` | best bid / best ask |
| 1-2 bps | `[1, 2)` | best bid / best ask |
| 2-5 bps | `[2, 5)` | best bid / best ask |
| 5-25 bps | `[5, 25)` | mid |
| 25-100 bps | `[25, 100]` | mid |

Distance formula:
- inner ring (bid side): `distance_bps = (best_bid - price_level) / best_bid * 10000`
- inner ring (ask side): `distance_bps = (price_level - best_ask) / best_ask * 10000`
- outer ring (bid side): `distance_bps = (mid - price_level) / mid * 10000`
- outer ring (ask side): `distance_bps = (price_level - mid) / mid * 10000`

Boundary at 5 bps:
- levels within `[2, 5)` bps from best go to ring `2_5bps`
- levels within `[5, 25)` bps from mid go to ring `5_25bps`
- no overlap or gap exists between inner and outer rings because the reference point shifts at the 5 bps boundary

Book state timing:
- ring depth is computed from the book state at second end (`*_close` state)
- if book state is stale / unsynchronized / unavailable, all ring columns are NULL

Crossed / invalid book:
- if best_bid >= best_ask (crossed book), ring depth columns are NULL for that second
- if best_bid or best_ask is zero or negative, ring depth columns are NULL

### 5.5 Mid NULL handling
- if mid is NULL (book unavailable), outer ring columns are NULL
- inner ring columns also require best bid / best ask, so they are NULL as well

---

## 6. Group D — Book event flow

その 1 秒に入った depth diff event から積み上げる flow 特徴量群。

### 6.1 Required core columns

| Column | Type | Semantics | Null/Zero |
|---|---|---|---|
| `bid_add_qty_near` | DOUBLE | bid add qty within near zone | 0 |
| `bid_cancel_qty_near` | DOUBLE | bid cancel qty within near zone | 0 |
| `ask_add_qty_near` | DOUBLE | ask add qty within near zone | 0 |
| `ask_cancel_qty_near` | DOUBLE | ask cancel qty within near zone | 0 |
| `bid_add_qty_deep` | DOUBLE | bid add qty beyond near zone | 0 |
| `bid_cancel_qty_deep` | DOUBLE | bid cancel qty beyond near zone | 0 |
| `ask_add_qty_deep` | DOUBLE | ask add qty beyond near zone | 0 |
| `ask_cancel_qty_deep` | DOUBLE | ask cancel qty beyond near zone | 0 |

### 6.2 Deterministic formula

For each depth diff event within the 1s bucket:

1. **Classify the event:**
   - `before_qty` = quantity at this price level before the event
   - `after_qty` = quantity at this price level after the event
   - if level is new (did not exist before): `before_qty = 0`
   - if level is deleted (does not exist after): `after_qty = 0`

2. **Compute delta:**
   - `add_qty = max(after_qty - before_qty, 0)`
   - `cancel_qty = max(before_qty - after_qty, 0)`
   - a level where qty is unchanged contributes 0 to both
   - a level where qty increases contributes only to add
   - a level where qty decreases contributes only to cancel

3. **Determine near / deep zone:**
   - reference price = best bid (for bid-side levels) or best ask (for ask-side levels)
   - `distance_bps = abs(price_level - reference_price) / reference_price * 10000`
   - near: `distance_bps <= 5`
   - deep: `distance_bps > 5`
   - reference price is taken from the book state **after** the event is applied
   - if reference price is zero or unavailable after the event, the event is excluded from both near and deep counters

4. **Accumulate:**
   - add the computed `add_qty` or `cancel_qty` to the appropriate near/deep column

5. **Snapshot events:**
   - snapshot events are NOT counted as add or cancel
   - they reset the book state but do not increment flow counters
   - only diff events (quantity changes on existing or new levels) contribute to flow columns

6. **Stale / unsynchronized events:**
   - if the book state is unsynchronized (sequence gap without repair), diff events in that state are excluded from flow counters
   - they are still counted in `depth_update_count`

7. **Crossed book:**
   - if the event produces a crossed book (best_bid >= best_ask), the event is still classified and accumulated
   - downstream consumers should check quality flags for crossed-book conditions

### 6.3 Near / deep boundary
Current canonical split:
- near = within 5 bps (inclusive)
- deep = beyond 5 bps

This boundary is fixed for v1 and applied after each event using the post-event reference price.

This is accepted as v1 contract behavior.

### 6.3 Extended optional flow-count variants
The following are optional and not part of lean core:
- `bid_add_cnt_near`
- `bid_cancel_cnt_near`
- `ask_add_cnt_near`
- `ask_cancel_cnt_near`
- `bid_add_cnt_deep`
- `bid_cancel_cnt_deep`
- `ask_add_cnt_deep`
- `ask_cancel_cnt_deep`

---

## 7. Group E — Quality / observability

### 7.1 Required core columns

| Column | Type | Semantics | Null/Zero |
|---|---|---|---|
| `depth_update_count` | BIGINT | number of depth diff events in this second | 0 |
| `stale_ms` | BIGINT | ms since last usable depth update | never NULL (see 7.3 for sentinel) |
| `missing_flag` | BIGINT | bitmask summarizing missing inputs/state | never NULL |

### 7.2 `missing_flag` bits

Recommended v1 encoding:
- `1` = no trade events in this second
- `2` = no depth update events in this second
- `4` = book state unavailable

Multiple bits may be set simultaneously.

Recommended interaction with book state:
- if boundary/depth columns are NULL because state is unavailable, stale, or unsynchronized, bit `4` must be set
- `stale_ms` is still emitted numerically even when bit `4` is set

### 7.3 `stale_ms` sentinel rule (resolved for v1)
- `stale_ms = 0` is a **valid measurement** meaning "last usable depth update happened during or immediately before this second boundary"
- there is NO sentinel for "never initialized". Instead:
  - if no usable depth timestamp has ever been established, `stale_ms = 0` is emitted AND `missing_flag` bit 4 must be set
  - downstream consumers MUST check `missing_flag & 4` to distinguish "fresh" from "never initialized"
- this is a contract-level rule; implementation must emit `stale_ms` with the computed value at all times

---

## 8. Extended optional columns (not lean core)

The following groups are valid but optional.

### 8.1 Best queue dynamics
- `best_bid_size_open_qty`
- `best_bid_size_close_qty`
- `best_ask_size_open_qty`
- `best_ask_size_close_qty`
- `best_bid_atouch_add_qty`
- `best_bid_atouch_cancel_qty`
- `best_ask_atouch_add_qty`
- `best_ask_atouch_cancel_qty`
- `best_bid_atouch_trade_qty`
- `best_ask_atouch_trade_qty`
- `best_bid_price_move_out_count`
- `best_ask_price_move_out_count`
- `best_replenish_count`

### 8.2 Imbalance by ring
- `imbalance_0_1bps`
- `imbalance_1_2bps`
- `imbalance_2_5bps`
- `imbalance_5_25bps`
- `imbalance_25_100bps`

### 8.3 Microprice
- `microprice_close`

### 8.4 Cross-venue
- `premium_to_ref_bps`
- `basis_to_ref_bps`

### 8.5 Rolling window
- `realized_vol_10s`
- `cvd_10s`
- `cvd_30s`
- `adverse_selection_bps`

### 8.6 Trade at-touch / through
- `trade_at_touch_qty`
- `trade_through_qty`

### 8.7 Miscellaneous / unresolved legacy
- `exchange_to_recv_lag_ms_avg`
- `crr`
- `tmr`
- `rvz`
- `best_deplete_count`
- `spread_widen_count`
- `replenish_lag_ms`

---

## 9. NULL / 0 rules

### 9.1 Trade local
- count/sum columns use `0` when nothing qualifies
- price summary columns (`open/high/low/close/vwap`) use `NULL` when `trade_count = 0`

### 9.2 Book-derived boundary/depth state
- use `NULL` when book state is unavailable / stale / unsynchronized

### 9.3 Event flow
- flow/counter columns use `0` when no qualifying event is observed
- they do not use `NULL` merely because the second is quiet

### 9.4 Empty-sample statistics
- any percentile / min / avg based on an empty sample uses `NULL`

---

## 10. Canonical split: lean core vs extended optional

### 10.1 Lean core
Lean core includes only:
- Group A required columns
- Group B required columns
- Group C required columns
- Group D required qty-flow columns
- Group E required columns

### 10.2 Extended optional
Everything in section 8 is extended optional.

This split is normative for planning and implementation priority.

---

## 11. V1 decisions and deferred items

Resolved for v1:

1. partial-book / bounded-depth venues emit ring depth as **visible-book-only values**, not forced NULL, as long as a usable visible book state exists
2. best queue dynamics remain **extended optional**, not lean core
3. near/deep boundary remains fixed at **5 bps** for v1
4. `best_deplete_count` remains **extended optional** alongside queue dynamics
5. `crr` / `tmr` are **not part of v1 core contract**
6. `rvz` is **not part of v1 core contract**
7. `adverse_selection_bps` remains **extended optional**, outside v1 core freeze

Deferred beyond v1 core:
- exact formulas for `crr` / `tmr`
- whether `rvz` gets a future explicit contract or is dropped
- whether best queue dynamics should ever be promoted into lean core

---

## 12. Exit check

This document is acceptable only if:
- non-burst 1s columns are grouped by meaning, not by implementation accident
- naming is canonicalized for qty / size buckets
- ring depth is preferred over legacy cumulative depth
- lean core vs extended optional is explicit
- null / zero rules are explicit
