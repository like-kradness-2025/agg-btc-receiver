// lib/burst-reducer/burst-state-codec.mjs — BurstBuilder internal state serializer/restorer
// Only file allowed to access BurstBuilder privates (_open, _closedBursts, _nextId)
// Follows plan Task 5b

// BurstBuilder actual internal field shapes (from lib/burst-builder.mjs):
//
//   this._open = {
//     side:          'buy' | 'sell',
//     start_ts:      number,
//     end_ts:        number,
//     prints:        [{ ts, price, qty, side, ...trade }],
//     min_price:     number,
//     max_price:     number,
//     sum_notional:  number,
//     sum_qty:       number,
//   }
//
//   this._closedBursts = [{
//     burst_id:             string,
//     market:               string,
//     side:                 'buy' | 'sell',
//     burst_notional:       number,
//     burst_print_count:    number,
//     burst_duration_ms:    number,
//     burst_start_ts:       number,
//     burst_end_ts:         number,
//     min_price:            number,
//     max_price:            number,
//     distinct_price_count: number,
//     span_ticks:           number,
//     same_price_runs:      SamePriceRun[],
//     prints:               TradePrint[],
//   this._nextId: number

const SCHEMA_VERSION = 1;

/**
 * Serialize BurstBuilder state to a JSON-safe deep clone (FULL — includes closedBursts).
 * Used for in-memory snapshots only. NOT for checkpoint persistence.
 * @param {import('../burst-builder.mjs').BurstBuilder} builder
 * @returns {{ schemaVersion: number, open: Object|null, closedBursts: Object[], nextId: number }}
 */
export function serializeBurstBuilderState(builder) {
  return {
    schemaVersion: SCHEMA_VERSION,
    open: builder._open ? deepCloneOpen(builder._open) : null,
    closedBursts: builder._closedBursts.map(b => deepCloneClosedBurst(b)),
    nextId: builder._nextId,
  };
}

/**
 * Serialize MINIMAL state for checkpoint persistence (P1-1).
 * Excludes closedBursts, prints, same_price_runs arrays.
 * Only `open` (with prints — required for same_price_runs on burst close) + `nextId`.
 * @param {import('../burst-builder.mjs').BurstBuilder} builder
 * @returns {{ schemaVersion: number, open: Object|null, nextId: number }}
 */
export function serializeMinimalBurstState(builder) {
  return {
    schemaVersion: SCHEMA_VERSION,
    open: builder._open ? deepCloneOpen(builder._open) : null,
    nextId: builder._nextId,
  };
}

/**
 * Restore BurstBuilder state from a serialized snapshot.
 * @param {import('../burst-builder.mjs').BurstBuilder} builder
 * @param {{ schemaVersion: number, open: Object|null, closedBursts: Object[], nextId: number }} state
 * @throws {Error} E020 on schema mismatch or malformed state
 */
export function restoreBurstBuilderState(builder, state) {
  if (!state || typeof state !== 'object') {
    throw new Error('E020: burst state codec: state is not an object');
  }
  if (state.schemaVersion !== SCHEMA_VERSION) {
    throw new Error(`E020: burst state codec: schema version mismatch: expected ${SCHEMA_VERSION}, got ${state.schemaVersion}`);
  }
  if (!Array.isArray(state.closedBursts)) {
    throw new Error('E020: burst state codec: closedBursts is not an array');
  }
  if (typeof state.nextId !== 'number' || !isFinite(state.nextId) || state.nextId < 1) {
    throw new Error(`E020: burst state codec: invalid nextId: ${state.nextId}`);
  }

  // Restore _open (nullable)
  if (state.open !== null && state.open !== undefined) {
    if (!state.open.side || !state.open.prints) {
      throw new Error('E020: burst state codec: open state missing required fields');
    }
    builder._open = deepCloneOpen(state.open);
  } else {
    builder._open = null;
  }

  // Restore _closedBursts
  builder._closedBursts = state.closedBursts.map(b => {
    validateClosedBurst(b);
    return deepCloneClosedBurst(b);
  });

  // Restore _nextId
  builder._nextId = state.nextId;
}

/**
 * Restore BurstBuilder from MINIMAL checkpoint state (P1-1).
 * Only restores `_open` and `_nextId`. `_closedBursts` starts empty —
 * the caller MUST re-feed trades to rebuild closed bursts.
 * @param {import('../burst-builder.mjs').BurstBuilder} builder
 * @param {{ schemaVersion: number, open: Object|null, nextId: number }} state
 * @throws {Error} E020 on schema mismatch or malformed state
 */
export function restoreMinimalBurstState(builder, state) {
  if (!state || typeof state !== 'object') {
    throw new Error('E020: burst state codec: minimal state is not an object');
  }
  if (state.schemaVersion !== SCHEMA_VERSION) {
    throw new Error(`E020: burst state codec: schema version mismatch: expected ${SCHEMA_VERSION}, got ${state.schemaVersion}`);
  }
  if (typeof state.nextId !== 'number' || !isFinite(state.nextId) || state.nextId < 1) {
    throw new Error(`E020: burst state codec: invalid nextId: ${state.nextId}`);
  }

  // Restore _open (nullable)
  if (state.open !== null && state.open !== undefined) {
    if (!state.open.side || !state.open.prints) {
      throw new Error('E020: burst state codec: minimal open state missing required fields');
    }
    builder._open = deepCloneOpen(state.open);
  } else {
    builder._open = null;
  }

  // _closedBursts always starts empty on minimal restore
  builder._closedBursts = [];

  // Restore _nextId
  builder._nextId = state.nextId;
}

/**
 * Get a deep-cloned snapshot of the closed bursts array.
 * All planned files must use this API; never read _closedBursts directly.
 * @param {import('../burst-builder.mjs').BurstBuilder} builder
 * @returns {Object[]} deep-cloned closed bursts
 */
export function getClosedBurstsSnapshot(builder) {
  return builder._closedBursts.map(b => deepCloneClosedBurst(b));
}

/**
 * Validate a closed burst shape is safe to deep-clone.
 * @param {Object} b
 * @throws {Error} E020 on malformed burst
 */
function validateClosedBurst(b) {
  if (!b || typeof b !== 'object') {
    throw new Error('E020: burst state codec: closed burst is not an object');
  }
  const requiredNumbers = ['burst_notional', 'burst_print_count', 'burst_duration_ms',
    'burst_start_ts', 'burst_end_ts', 'min_price', 'max_price',
    'distinct_price_count', 'span_ticks'];
  for (const key of requiredNumbers) {
    if (typeof b[key] !== 'number' || !isFinite(b[key])) {
      throw new Error(`E020: burst state codec: invalid or missing field "${key}" in closed burst`);
    }
  }
  if (typeof b.burst_id !== 'string' || !b.burst_id) {
    throw new Error('E020: burst state codec: missing burst_id');
  }
  if (!Array.isArray(b.same_price_runs)) {
    throw new Error('E020: burst state codec: same_price_runs is not an array');
  }
  if (!Array.isArray(b.prints)) {
    throw new Error('E020: burst state codec: prints is not an array');
  }
}

// ── Private deep-clone helpers ──

function deepCloneOpen(o) {
  return {
    side: o.side,
    start_ts: o.start_ts,
    end_ts: o.end_ts,
    prints: o.prints.map(p => ({ ...p })),
    min_price: o.min_price,
    max_price: o.max_price,
    sum_notional: o.sum_notional,
    sum_qty: o.sum_qty,
  };
}

function deepCloneClosedBurst(b) {
  return {
    burst_id: b.burst_id,
    market: b.market,
    side: b.side,
    burst_notional: b.burst_notional,
    burst_print_count: b.burst_print_count,
    burst_duration_ms: b.burst_duration_ms,
    burst_start_ts: b.burst_start_ts,
    burst_end_ts: b.burst_end_ts,
    min_price: b.min_price,
    max_price: b.max_price,
    distinct_price_count: b.distinct_price_count,
    span_ticks: b.span_ticks,
    same_price_runs: b.same_price_runs.map(r => ({ ...r })),
    prints: b.prints.map(p => ({ ...p })),
  };
}
