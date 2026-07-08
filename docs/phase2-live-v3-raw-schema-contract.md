# Phase2 live_v3 raw schema contract

**Status:** draft for review  
**Scope:** phase2 downstream aggregation input contract  
**Authoritative input root:** `data/live_v3`

---

## 1. Purpose

This contract fixes the raw JSONL shapes and normalization rules consumed by the phase2 downstream aggregator.

The receiver is already responsible only for receiving and writing finalized raw windows. The downstream aggregator must read only finalized `.jsonl` windows and derive:

- `data/1s_features/<date>/<market>.jsonl`
- `data/30s_book/<date>/<market>.jsonl`

This document defines **what a valid input row is**. It does not define book reconstruction or feature formulas; those are separate contracts.

---

## 2. Authoritative layout

The phase2 aggregator reads finalized raw-window files under:

```text
data/live_v3/trades/<market>/<YYYY-MM-DD>/<HH-MM-SS>.jsonl
data/live_v3/book_updates/<market>/<YYYY-MM-DD>/<HH-MM-SS>.jsonl
data/live_v3/snapshots/<market>/<YYYY-MM-DD>/<HH-MM-SS>.jsonl
data/live_v3/liquidations/<market>/<YYYY-MM-DD>/<HH-MM-SS>.jsonl
```

The following flat legacy/side-output files may exist under `data/live_v3` but are **not** part of this raw-window contract:

```text
data/live_v3/trades/<market>.jsonl        # aggregated trade bars, not raw trade prints
data/live_v3/book/<market>.jsonl          # periodic full-book snapshots, not 30s rotated raw windows
data/live_v3/health.jsonl
data/live_v3/derivatives/<market>.jsonl
data/live_v3/ticker/<market>.jsonl
data/live_v3/ohlcv/<market>.jsonl
data/live_v3/premium/<name>.jsonl
data/live_v3/lsratio/<market>.jsonl
data/live_v3/takervol/<market>.jsonl
```

A later reconstruction contract may choose to use `data/live_v3/book/<market>.jsonl` as an additional anchor source, but this raw-window schema contract covers the rotated `snapshots/<market>/<date>/<window>.jsonl` input only.

Rules:

1. Read only files ending exactly in `.jsonl`.
2. Never read `.jsonl.open`.
3. Never read `.processing`, `.processed`, `.conflict`, or files under quarantine paths as primary input.
4. The file path provides `kind`, `market`, `date`, and `window_start_label`.
5. The JSON row's `market`, when present, must equal the path market. If it differs, the row is invalid.
6. The date / file name are routing metadata. Event time still comes from row timestamp fields.

---

## 3. Common row rules

### 3.1 Encoding

- JSONL
- UTF-8
- one JSON object per line
- blank lines ignored
- invalid JSON line: row invalid, counted in run report, skipped

### 3.2 Common normalized fields

Every accepted row is normalized internally to:

| normalized field | source | rule |
|---|---|---|
| `kind` | path | one of `trade`, `book_update`, `snapshot`, `liquidation` |
| `market` | row/path | must match path market |
| `exchange` | row or market mapping | if absent, derive from market prefix before first `_` |
| `event_ts_ms` | row `ts` | required integer milliseconds after timestamp normalization |
| `recv_ts_ms` | row `recvTs` if present | integer milliseconds or `null`; metadata only |
| `source_ts_ms` | row `source_ts` or `sourceTs` if present | integer milliseconds or `null`; metadata only |
| `effective_ts_ms` | `event_ts_ms` | no fallback in phase2a raw schema |
| `file_path` | path | lexical path string |
| `line_no` | scanner | 1-indexed line number |

### 3.3 Timestamp normalization

Accepted timestamp inputs:

- Required timestamp fields must be JSON numbers. Numeric strings are invalid.
- Optional timestamp fields may be absent or explicit `null`; absent/null normalize to `null`.
- Non-null timestamp values must be finite JSON numbers. `NaN`, `Infinity`, and non-number non-null values are invalid.
- fractional milliseconds are floored to integer milliseconds.

Unit detection:

| numeric range | interpreted as | conversion |
|---:|---|---:|
| `1e11 <= abs(ts) < 1e14` | milliseconds | `floor(ts)` |
| `1e14 <= abs(ts) < 1e17` | microseconds | `floor(ts / 1000)` |
| `1e17 <= abs(ts) < 1e20` | nanoseconds | `floor(ts / 1_000_000)` |
| `1e9 <= abs(ts) < 1e11` | seconds | `floor(ts * 1000)` |
| otherwise | invalid | skip row |

For current `live_v3`, observed rows use millisecond `ts`, except some feeds may produce fractional millisecond numbers for book updates. Those are floored.

### 3.4 Decimal grammar

This section applies to every field whose type is `decimal`, including scalar trade/liquidation fields and book level arrays.

Accepted decimal values:

- JSON number: finite only
- JSON string matching:

```regex
^-?(?:0|[1-9][0-9]*)(?:\.[0-9]+)?$
```

Rejected:

- empty string
- whitespace-padded string
- scientific notation string such as `"1e-8"`
- `NaN`, `Infinity`, `-Infinity`
- negative quantity
- non-positive price

Exception for book level quantities only:

- Empty string `""` is accepted only as a level quantity delete sentinel and normalizes to quantity `0`.
- Empty string is never valid for price or scalar decimal fields.

Parsing rule:

- Parse as exact base-10 decimal for validation and arithmetic inputs.
- Do not round at schema-normalization time.
- Derived outputs may later choose their own numeric storage/rounding contract.

Quantity `0` is valid for depth/book levels and means deletion when applying updates. This includes `""` for level quantity on venues such as Bitfinex.

### 3.5 Market and exchange normalization

`market` is canonical and must match path directory.

`exchange` normalization:

1. If row contains `exchange`, it must be a non-empty string and is used.
2. For trade, book_update, and snapshot rows only: if row omits `exchange`, derive from `market` as the substring before the first `_`.
   - `binance_spot` -> `binance`
   - `binance_perp_btcusdc` -> `binance`
   - `hyperliquid_perp` -> `hyperliquid`
3. Liquidation rows must carry explicit `exchange`; missing liquidation `exchange` is invalid.
4. If no `_` exists in `market`, row is invalid unless row exchange is present.

---

## 4. Trade rows

Path:

```text
data/live_v3/trades/<market>/<date>/<HH-MM-SS>.jsonl
```

Observed example:

```json
{"market":"binance_perp","price":62966.5,"qty":0.01,"side":"buy","ts":1783206899658,"tradeId":"7867835202"}
```

### 4.1 Required fields

| field | type | rule |
|---|---|---|
| `market` | string | must match path market |
| `price` | decimal | positive |
| `qty` | decimal | positive |
| `side` | string | normalized by §4.3 side normalization |
| `ts` | number | valid timestamp |


### 4.2 Optional fields

| field | type | rule |
|---|---|---|
| `tradeId` | string or null or absent | missing normalizes to `null`; explicit null valid |
| `exchange` | string | if absent derive from market |
| `recvTs` | number | if present valid timestamp |
| `sourceTs` | number | metadata only; does not replace required `ts` |

### 4.3 Side normalization

Accepted side values:

| input | normalized |
|---|---|
| `buy` | `buy` |
| `sell` | `sell` |
| `BUY` | `buy` |
| `SELL` | `sell` |

All other values are invalid for phase2a.

Important: side means **aggressor side / taker side as already normalized by the connector**, not maker side.

### 4.4 Trade count meaning

Any accepted trade row contributes one print. Therefore:

- `trade_print_count += 1`
- `buy_print_count += 1` only for normalized buy
- `sell_print_count += 1` only for normalized sell

This is not parent-order count and not swept-level count.

---

## 5. Book update rows

Path:

```text
data/live_v3/book_updates/<market>/<date>/<HH-MM-SS>.jsonl
```

Observed examples:

```json
{"market":"binance_spot","type":"update","bids":[["62872.23000000","0.02632000"]],"asks":[],"ts":1783215870015,"seq":96958307332}
```

```json
{"market":"bitstamp_spot","type":"update","bids":[["62635.13","0.09556019"]],"asks":[["62807.68","0.50000000"]],"ts":1783215870055.68,"seq":null}
```

### 5.1 Required fields

| field | type | rule |
|---|---|---|
| `market` | string | must match path market |
| `type` | string | must be `update` or `snapshot`; preserve subtype for reconstruction |
| `bids` | array | array of `[price, qty]` levels; may be empty |
| `asks` | array | array of `[price, qty]` levels; may be empty |
| `ts` | number | valid timestamp after flooring |

For `type: "update"`: at least one of `bids` or `asks` may be non-empty in practice, but both empty is accepted and treated as a no-op quality event.

For `type: "snapshot"`: the row is a full visible book snapshot embedded in the rotated `book_updates` stream, commonly emitted during initial connect/reconnect for some venues. It is accepted by this raw schema and must retain `book_update_subtype = "snapshot"` for the reconstruction contract. A `snapshot` subtype must contain non-empty `bids` and non-empty `asks` to be anchor-eligible; if either side is empty it remains parseable but is not a usable anchor.

### 5.2 Optional / nullable fields

| field | type | rule |
|---|---|---|
| `seq` | integer or null or absent | if present non-null, must be non-negative safe integer |
| `prevSeq` | integer or null or absent | if present non-null, must be non-negative safe integer |
| `exchange` | string | if absent derive from market |
| `recvTs` | number | if present valid timestamp |

Nullability rule:

- `seq: null` is valid and means sequence unavailable.
- `prevSeq: null` is valid and means no previous sequence available.
- Absent `seq` / `prevSeq` are normalized to `null`.
- A non-null non-integer `seq` / `prevSeq` invalidates the row.

### 5.3 Level array grammar

Each level must be a 2-element array:

```json
[price, qty]
```

Rules:

- `price` accepted decimal and `> 0`; empty string invalid
- `qty` accepted decimal and `>= 0`; empty string accepted as delete sentinel and normalized to `0`
- `qty == 0` means delete that price level from the side during reconstruction
- malformed level invalidates the whole row for phase2a

---

## 6. Snapshot rows

Path:

```text
data/live_v3/snapshots/<market>/<date>/<HH-MM-SS>.jsonl
```

Observed example:

```json
{
  "market":"binance_perp",
  "ts":1783207501625,
  "seq":10969073657196,
  "bids":[["63048.90","3.720"]],
  "asks":[["63049.00","1.234"]],
  "bidLevelCount":1657,
  "askLevelCount":1752
}
```

### 6.1 Required fields

| field | type | rule |
|---|---|---|
| `market` | string | must match path market |
| `ts` | number | snapshot capture time, valid timestamp |
| `bids` | array | full visible bid side; every level must follow §5.3 level grammar |
| `asks` | array | full visible ask side; every level must follow §5.3 level grammar |
| `bidLevelCount` | integer | non-negative safe integer; mismatch with `bids.length` is quality warning |
| `askLevelCount` | integer | non-negative safe integer; mismatch with `asks.length` is quality warning |

### 6.2 Optional / nullable fields

| field | type | rule |
|---|---|---|
| `seq` | integer or null or absent | if present non-null, non-negative safe integer |
| `exchange` | string | if absent derive from market |
| `recvTs` | number | if present valid timestamp |

### 6.3 Snapshot time semantics

Current `live_v3` snapshot rows use `ts` as **receiver capture/write time** of the visible in-memory book snapshot. It is the event time for snapshot rows.

If future rows add both `sourceTs` and `recvTs`, phase2a will still use `ts` as snapshot effective time unless a later contract explicitly changes it.

### 6.4 Snapshot application semantics

Snapshot rows are valid inputs for book reconstruction. The reconstruction contract will define how they are applied. For raw schema purposes:

- `bids` / `asks` are the complete visible book sides at capture time.
- A valid snapshot must contain at least one bid and one ask.
- Empty side invalidates the snapshot row for 30s_book reconstruction, but row is still counted in raw quality report.

---

## 7. Liquidation rows

Path:

```text
data/live_v3/liquidations/<market>/<date>/<HH-MM-SS>.jsonl
```

Observed example:

```json
{"ts":1783206887471,"market":"bybit_perp","exchange":"bybit","symbol":"BTCUSDT","side":"buy","price":62653.2,"qty":0.007,"notional":438.5724,"raw_type":"liquidation","trade_id":null,"source_ts":1783206887198}
```

### 7.1 Required fields

| field | type | rule |
|---|---|---|
| `ts` | number | receiver-side normalized timestamp, valid |
| `market` | string | must match path market |
| `exchange` | string | non-empty |
| `symbol` | string | non-empty |
| `side` | string | normalized by §4.3 side normalization |
| `price` | decimal | positive |
| `qty` | decimal | positive |
| `notional` | decimal | non-negative; mismatch with `price * qty` is a quality warning, not invalid |
| `raw_type` | string | must be `liquidation` |

### 7.2 Liquidation notional tolerance

For quality reporting, compute:

```text
notional_abs_error = abs(notional - price * qty)
notional_tolerance = max(1e-8, abs(price * qty) * 1e-8)
```

If `notional_abs_error > notional_tolerance`, accept the row but record quality warning `notional_mismatch`.

### 7.3 Optional / nullable fields

| field | type | rule |
|---|---|---|
| `trade_id` | string or null | null valid |
| `source_ts` | number or null or absent | exchange/source timestamp; missing/null normalize to `null`; non-null must be valid timestamp |
| `recvTs` | number | if present valid timestamp |

### 7.4 Current phase2 handling

Liquidations are valid raw inputs but do not directly drive 1s feature replay or 30s_book reconstruction in the first phase2 slice unless explicitly requested by a later aggregation contract.

If liquidations are included in a deterministic multi-stream merge, their stream priority is after trade:

```text
snapshot < book_update < trade < liquidation
```

Lower number sorts earlier if a numeric priority is used.

---

## 8. Deterministic row ordering key

For any batch that needs deterministic ordering, accepted rows are sorted by:

1. `effective_ts_ms` ascending
2. stream priority ascending:
   - snapshot: 0
   - book_update: 1
   - trade: 2
   - liquidation: 3
3. `file_path` lexical ascending
4. `line_no` ascending

Rows with invalid timestamp are skipped before ordering and counted in the run report.

Duplicate JSON object keys are invalid. Parsers used by phase2 must detect duplicates before normal object materialization. Unknown fields are allowed unless a section explicitly prohibits them; unknown fields do not affect normalized values.

This means per-file append order is preserved only as final tie-breaker, not as primary time ordering.

---

## 9. Invalid row policy

Invalid row handling is deterministic:

1. Validate in the precedence order below and report only the first failing reason.
2. Skip the row for derived feature computation.
3. Count it in a run report with:
   - `kind`
   - `market`
   - `file_path`
   - `line_no`
   - `reason`
4. Do not abort the whole batch unless invalid row rate exceeds a later invocation-contract threshold.
5. Do not write partially-normalized rows to derived outputs.

Validation precedence and canonical invalid reasons:

1. `invalid_json` — line is not a JSON object, has duplicate object keys, or cannot be parsed
2. `market_mismatch` — row `market` missing or differs from path market
3. `missing_required_field` — any kind-specific required field is absent, including `type`, `raw_type`, liquidation `exchange`, or liquidation `symbol`
4. `invalid_type` — present row `type` / `raw_type` violates the kind-specific constant
5. `invalid_timestamp` — required `ts` or non-null optional timestamp field has invalid value
6. `invalid_exchange` — present exchange is empty/non-string, derived exchange is impossible for non-liquidation rows, or exchange otherwise fails normalization
7. `invalid_symbol` — present symbol is empty/non-string
8. `invalid_decimal` — scalar decimal field violates §3.4
9. `invalid_side` — side violates §4.3
10. `invalid_sequence` — non-null seq/prevSeq is not a non-negative safe integer
11. `invalid_level_shape` — book level is not a 2-element array or violates price/qty grammar
12. `invalid_level_count` — bidLevelCount/askLevelCount not a non-negative safe integer
13. `empty_snapshot_side` — accepted JSON snapshot has zero bids or zero asks
14. `invalid_field` — catch-all for any field-level validation failure not covered above

Missing-field rule is intentionally before invalid-value rules. Example: missing `raw_type` is `missing_required_field`; present `raw_type: "foo"` is `invalid_type`. Missing liquidation `exchange` is `missing_required_field`; present `exchange: ""` is `invalid_exchange`.

---

## 10. Empirical evidence checked

Observed with a local scan of finalized `data/live_v3` files:

- `trades`: fields `market, price, qty, side, tradeId, ts`
- `book_updates`: fields `market, type, bids, asks, ts`, sometimes `seq`; some venues omit `seq`; some emit `seq: null`; some `ts` values are fractional milliseconds; Bitfinex can emit level quantity `""` as delete sentinel; rare `type: "snapshot"` full-ladder rows exist in rotated book_updates and must be accepted
- `snapshots`: fields `market, ts, bids, asks, bidLevelCount, askLevelCount`, optional `seq`
- `liquidations`: fields `ts, market, exchange, symbol, side, price, qty, notional, raw_type, trade_id, source_ts`
- top-level `trades/<market>.jsonl` and `book/<market>.jsonl` exist but are not raw-window inputs for this contract

Representative command:

```bash
python3 - <<'PY'
# scan data/live_v3/<kind>/<market>/<date>/<HH-MM-SS>.jsonl
# collect key sets and JSON value types
PY
```

---

## 11. Non-goals

This contract does not define:

- how to reconstruct a book from snapshots and updates
- 1s feature formulas
- 30s_book bucket formulas
- output schema for derived datasets
- marker / cleanup policy
- daemon scheduling

Those are separate phase2 contracts.
