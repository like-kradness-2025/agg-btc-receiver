# phase2 live_v3 raw schema contract — outline (proposed)

## Purpose
Authoritative schema contract for the **raw** replay-input data under `data/live_v3/`.
This document lists the normative blockers that MUST be resolved before any phase2a batch replay
implementation can consume live_v3 raw data without guessing.

## Recovery note
Prior phase2a raw schema doc (`docs/v2-phase2a-raw-event-schema.md`, Codex review **31/100**)
was written against the old `data/raw_hot` flat layout.  That file no longer exists on disk
(likely removed when moving to the live_v3 30-second rotation layout).
The seven blocking findings from that review are preserved below and re-targeted at the live_v3 shape.

---

## 1. Authoritative input layout (live_v3)

Only these are **raw replay input**.  Everything else under `live_v3/` is post-aggregation output
and is **not** part of the raw contract.

```
data/live_v3/trades/<market>/<YYYY-MM-DD>/<HH-MM-SS>.jsonl       ← raw trade prints
data/live_v3/book_updates/<market>/<YYYY-MM-DD>/<HH-MM-SS>.jsonl  ← raw incremental depth diffs
data/live_v3/book/<market>.jsonl                                   ← periodic full-book snapshots (30 s)
```

`.open` / in-progress files are excluded; only finalized `.jsonl` files are eligible for replay.

---

## 2. BLOCKER 1 — Timestamp contract

### 2a. Trade timestamp
- Field: `ts` (JSON number, epoch ms, integer observed — fractional ms possible in edge cases)
- Semantics: exchange event time
- Normalization: `Math.floor(ts)` — reject non-finite / null / absent

### 2b. Book-update timestamp
- Field: `ts` (JSON number, epoch ms, integer observed)
- Semantics: exchange event time (connector-origin; no explicit provenance flag in current schema)
- Normalization: `Math.floor(ts)` — reject non-finite / null / absent

### 2c. Book-snapshot timestamp
- Field: `ts` (JSON number, epoch ms)
- Semantics: receiver-side capture time (wall clock, NOT venue event time)
- Normalization: `Math.floor(ts)`
- **CRITICAL**: Phase2a MUST NOT treat snapshot.ts as event time — it is a receiver-local wall clock.
  Book-reconstruction contracts must use event-time from depth updates for ordering, and
  treat snapshot as a state anchor only.

### 2d. Fractional-ms policy
- Accept fractional-ms values that parse to finite `Number`
- Normalize with `Math.floor` (truncate toward zero for negative — but negative ts is row-invalid anyway)
- Reject: NaN, ±Infinity, null, absent, non-numeric strings

---

## 3. BLOCKER 2 — seq / nullability contract

### 3a. Trade rows
- No `seq` field in live_v3 raw trades.
- `tradeId` is optional (string or absent/null).  Not present on all venues (e.g., Bitfinex may not
  include it).  Phase2a MUST tolerate missing `tradeId`.

### 3b. Book-update rows
- `seq` : integer number when the venue provides sequencing (Binance, Bybit, OKX).
  **Absent** (field missing entirely) for venues that do not provide per-event sequence
  IDs (e.g., Bitfinex in currently observed output).
- `prevSeq` : **NOT present** in live_v3 book_updates (unlike the old v2 `raw_hot/depth` format).
  Gap detection MUST NOT rely on `prevSeq`; it can only use `seq` monotonicity within a market.

### 3c. Book-snapshot rows
- `seq` : integer number, same venue semantics as book-update `seq`.
  May be absent for venues without sequencing.

### 3d. Nullability rules (all streams)
- Any row where a **required** field is `null` / absent → row-invalid.
- `seq` is conditionally required (venue-dependent); absence alone is not a validity failure.

---

## 4. BLOCKER 3 — Decimal grammar contract

### 4a. Trade prices / quantities
- `price` and `qty` are JSON numbers (e.g., `62886.29`, `0.00047`)
- Always finite JS `Number`.  Reject NaN/Infinity.
- Phase2a replay MUST parse with deterministic decimal semantics.
  - Recommendation: normalize to string with fixed significant digits, then to BigInt or
    fixed-point via the configured tick/step size for the market.
  - Minimum: reject non-finite values; `Number.isFinite()` guard.

### 4b. Book-update prices / quantities
- `bids` / `asks` entries are `[price_string, qty_string]` — **both are decimal strings**,
  NOT JSON numbers (observed across all venues: Binance, Bitfinex, OKX, Bybit, Kraken).
  - Examples: `["62886.28000000","0.73607000"]`, `["62945",""]`
- Parser MUST explicitly convert these strings; implicit JS coercion is forbidden.

### 4c. Delete sentinel (Bitfinex critical)
- Bitfinex uses `""` (empty string) for qty to signal "delete this price level".
- Other venues use `"0.00000000"` (numeric-zero string) for deletion.
- **Normative rule**: qty strings that parse to `0` (including `""`, `"0"`, `"0.0"`, `"0.00000000"`,
  `"-0"`, leading/trailing whitespace) are delete sentinels.
  - Empty string `""` → delete.
  - `Number(qty_string) === 0` after trimming → delete.
  - Negative qty → row-invalid (not a delete — reject the entire tuple).

### 4d. Book-snapshot prices / quantities
- Same string convention as book-updates: `[price_string, qty_string]`.
- Snapshots are full-ladder; empty-string qty should not appear in snapshot output but MUST
  be handled if present (treat as delete at that level).

---

## 5. BLOCKER 4 — Side normalization

### 5a. Trade side
- Exact JSON string domain: `"buy"` | `"sell"`
- Not case-insensitive.  Not "BUY" / "Buy" / "B" / "S" / "bid" / "ask".
- Rows with any other value (including null/absent) are **row-invalid** for trade aggregation.
  Phase2a MUST NOT attempt to infer side from price-vs-best-bid/ask or any heuristic.

### 5b. Liquidation / other streams
- If liquidation rows carry `side`, the same strict domain applies.
- Streams without trade-directional semantics (book, health, derivatives, ohlcv, etc.)
  are not subject to side normalization.

---

## 6. BLOCKER 5 — Exchange derivation

### Rule
- Canonical exchange = everything before the last `_` in `market`.
- Known exceptions:
  - `binance_perp_btcusdc` → exchange = `binance` (not `binance_perp`)
  - `binance_spot_usdc` → exchange = `binance` (not `binance_spot`)
  - `coinbase_international_perp` → exchange = `coinbase` (not `coinbase_international`)
- Phase2a MUST apply these special cases exactly as `lib/fair-price-collector.mjs::exchangeFromMarket()`
  does.
- If a row already carries an `exchange` field (book snapshots), the value MUST match the
  derived exchange.  Mismatch → row-invalid.

---

## 7. BLOCKER 6 — Invalid-row policy

### Three-tier disposition

| Tier | Condition | Action |
|------|-----------|--------|
| **FATAL** | File not valid JSONL, UTF-8 corruption, unparseable line | Abort the entire batch run |
| **ROW-INVALID** | Missing required field, invalid side, non-finite timestamp, negative qty, malformed bids/asks tuple, exchange mismatch | Skip the row, increment `invalid_row_count` in manifest, continue |
| **QUALITY-WARN** | Duplicate tradeId within same market×second, timestamp out of expected range, unexpected fields | Emit warning to manifest, process row normally |

### Accounting
- Every phase2a batch run MUST emit:
  - `total_rows_read`
  - `total_rows_valid`
  - `invalid_row_count` (per stream, per market, per reason category)
  - `duplicate_trade_id_count`

---

## 8. BLOCKER 7 — Snapshot anchor eligibility

Carried forward from prior review:

A book snapshot row is an anchor candidate for synchronized book reconstruction **iff ALL** of:
1. `bids` array is non-empty AND `asks` array is non-empty
2. `bidLevelCount == bids.length` AND `askLevelCount == asks.length`
3. No malformed price/qty tuples in either ladder
4. `ts` is finite and positive

Snapshots that fail any of these conditions are NOT usable as reconstruction anchors.
They are preserved in raw data but skipped by the replay engine.
Startup snapshots that are one-sided (empty bids or empty asks) are the canonical case of
non-usable anchors.

---

## 9. Open / unresolved

1. **Liquidation raw data**:  No `liquidations/` directory observed under live_v3.
   If liquidations exist in a separate path or are needed for phase2a, the path and schema
   must be discovered and added to this contract.

2. **book vs book_updates relationship**:  `book/<market>.jsonl` flat files are periodic
   (30-second) full-book snapshots.  `book_updates/<market>/<date>/<HH-MM-SS>.jsonl` nested
   files are raw incremental diffs.  The reconstruction contract must define whether snapshots
   from `book/` or bootstrap snapshots from the start of a `book_updates/` time-slice are
   used as anchors (or both).

3. **No `prevSeq` in live_v3 book_updates**:  Gap detection must be redesigned around
   `seq`-only monotonicity tracking rather than the `prevSeq` lineage chain that existed
   in the old `raw_hot/depth` format.

4. **Bitfinex qty precision**:  Bitfinex appears to use integer-like price strings
   (`"62945"` vs `"62945.00000000"`).  Decimal normalization must handle variable precision
   across venues without introducing false inequality.

---

## 10. Review checklist (for re-review gate)

Prior Codex review found these high-severity gaps (score 31/100).  This outline addresses:

| # | Prior blocker | Addressed in section | Status |
|---|---------------|---------------------|--------|
| 1 | Time mapping wrong (all streams uniform) | §2 (per-stream) | ✅ |
| 2 | Fractional ms normalization undefined | §2d | ✅ |
| 3 | Snapshot unconditional anchor | §8 (eligibility) | ✅ |
| 4 | Depth delete semantics (Bitfinex `""`) | §4c | ✅ |
| 5 | Side normalization ambiguous | §5 | ✅ |
| 6 | Validation disposition undefined | §7 (three-tier) | ✅ |
| 7 | Liquidation path mismatch | §9 (open) | ⚠️ (deferred) |

New live_v3-specific gaps to close before review:

| # | Gap | Section |
|---|-----|---------|
| A | No `prevSeq` — how to detect gaps | §3b, §9.3 |
| B | Decimal strings vs numbers (two grammars) | §4a, §4b |
| C | `book/` vs `book_updates/` anchor source | §9.2 |
| D | Missing `tradeId` tolerance | §3a |
| E | Bitfinex integer-like price strings | §9.4 |

---

## Next step (not part of this outline)
1. Promote this outline to a full normative contract: `docs/phase2-live-v3-raw-schema-contract.md`
2. Validate against 100+ actual rows of each stream×venue from live_v3
3. Codex review gate (target ≥ 95/100)
