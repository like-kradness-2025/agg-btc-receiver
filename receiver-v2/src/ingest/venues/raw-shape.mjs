/**
 * Set 7b: the v1 raw record shapes, shared by the venue adapters.
 *
 * Reception writes the canonical raw from the socket boundary (Set 7a: `src/raw.mjs`, wired in
 * `main.mjs`). The adapter is the only layer that can read the venue's bytes, so it is the adapter
 * that turns one received frame into the raw record(s) it carries (`rawEventFor`). Set 7a did that
 * for Binance's depth frames; Set 7b adds every other market's trade and book frames, matching the
 * payloads the running v1 store actually holds (measured 2026-10-10 from
 * `~/Tool/agg-btc-receiver/data/sqlite/<market>.sqlite`).
 *
 * The v1 payload is the emitted event verbatim plus worker metadata (`buildRawDbEnvelope`), so the
 * shapes below are the venue's normalized events field for field. Three record kinds share the
 * envelope the writer expects (`{ stream, event_ts_ms, source_event_ts_ms, source_event_time_known,
 * source_id, payload }`); `payload.market` is v1's own key and must be present.
 */

/** v1's trade record: `{market, price, qty, side, ts, tradeId}` (+ any per-source metadata first). */
export function rawTrade({ market, price, qty, side, ts, tradeId, extra = null }) {
  // v1 always stringified the trade id (`String(event.t)`, `String(t.i)`, ...), so the payload's
  // `tradeId` and the envelope's `source_id` are the same string.
  const id = String(tradeId);
  return {
    stream: 'trades',
    event_ts_ms: ts,
    source_event_ts_ms: ts,
    source_event_time_known: true,
    source_id: id,
    payload: { ...(extra ?? {}), market, price, qty, side, ts, tradeId: id },
  };
}

/**
 * v1's depth record. `stream` is `book_updates` for a depth frame or snapshot, and `snapshots` for
 * the second write of a snapshot (v1 wrote a snapshot to both - `orderflow-worker.mjs:472-486`).
 */
export function rawBook({ market, stream = 'book_updates', payload, event_ts_ms, source_event_ts_ms = null, source_event_time_known = false }) {
  return {
    stream,
    event_ts_ms,
    source_event_ts_ms,
    source_event_time_known,
    source_id: null,
    payload: { market, ...payload },
  };
}

/** v1's liquidation record: `{market, exchange, symbol, side, price, qty, notional, raw_type, trade_id, source_ts, ts}`. */
export function rawLiquidation({ market, exchange, symbol, side, price, qty, notional, raw_type, trade_id = null, source_ts = null, ts }) {
  return {
    stream: 'liquidations',
    event_ts_ms: ts,
    source_event_ts_ms: source_ts,
    source_event_time_known: source_ts !== null,
    source_id: null,
    payload: { market, exchange, symbol, side, price, qty, notional: notional ?? price * qty, raw_type, trade_id: trade_id ?? null, source_ts, ts },
  };
}

/**
 * The `rawEventFor` return contract: null when a frame carries no record, the record itself when it
 * carries exactly one (the common case, and what Set 7a already returned for a depth frame), and an
 * array when the frame carries several (a trade frame with many trades, or a snapshot written to
 * both `book_updates` and `snapshots`).
 */
export function collapse(records) {
  if (records.length === 0) return null;
  return records.length === 1 ? records[0] : records;
}

