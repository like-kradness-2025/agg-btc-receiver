// lib/burst-reducer/rollup.mjs — pure 30s aggregation from complete 1s features

const ROLLUP_FIELD_OPERATORS = Object.freeze({
  burst_count_mean_30s: 'mean',
  burst_count_max_30s: 'max',
  burst_notional_overlap_sum_30s: 'sum',
  burst_notional_overlap_max_30s: 'max',
  burst_notional_overlap_p95_30s: 'p95',
  max_burst_notional_max_30s: 'max',
  max_burst_notional_mean_30s: 'mean',
  max_burst_prints_max_30s: 'max',
  max_burst_duration_max_30s: 'max',
  trade_count_30s: 'sum',
  buy_trade_count_30s: 'sum',
  sell_trade_count_30s: 'sum',
  traded_qty_30s: 'sum',
  traded_notional_30s: 'sum',
  buy_qty_30s: 'sum',
  sell_qty_30s: 'sum',
  buy_notional_30s: 'sum',
  sell_notional_30s: 'sum',
  signed_volume_30s: 'sum',
  signed_notional_30s: 'sum',
  max_trade_notional_30s: 'max',
  large_trade_count_30s: 'sum',
  large_trade_notional_30s: 'sum',
  trade_imbalance_qty_30s: 'recompute',
  trade_imbalance_notional_30s: 'recompute',
  mean_trade_notional_30s: 'recompute',
  large_trade_notional_share_30s: 'recompute',
  trade_open_30s: 'first',
  trade_high_30s: 'max',
  trade_low_30s: 'min',
  trade_close_30s: 'last',
  ofi_30s: 'sum',
  bid_add_qty_30s: 'sum',
  bid_cancel_qty_30s: 'sum',
  ask_add_qty_30s: 'sum',
  ask_cancel_qty_30s: 'sum',
  replenishment_qty_30s: 'sum',
  pulling_qty_30s: 'sum',
});

export const ROLLUP_FIELDS = Object.freeze(Object.keys(ROLLUP_FIELD_OPERATORS));
export { ROLLUP_FIELD_OPERATORS };

const INPUT_FIELDS = Object.freeze({
  burst_count_mean_30s: 'burst_count_1s',
  burst_count_max_30s: 'burst_count_1s',
  burst_notional_overlap_sum_30s: 'total_burst_notional_1s',
  burst_notional_overlap_max_30s: 'total_burst_notional_1s',
  burst_notional_overlap_p95_30s: 'total_burst_notional_1s',
  max_burst_notional_max_30s: 'max_burst_notional_1s',
  max_burst_notional_mean_30s: 'max_burst_notional_1s',
  max_burst_prints_max_30s: 'max_burst_prints_1s',
  max_burst_duration_max_30s: 'max_burst_duration_ms_1s',
  trade_count_30s: 'trade_count_1s',
  buy_trade_count_30s: 'buy_trade_count_1s',
  sell_trade_count_30s: 'sell_trade_count_1s',
  traded_qty_30s: 'traded_qty_1s',
  traded_notional_30s: 'traded_notional_1s',
  buy_qty_30s: 'buy_qty_1s',
  sell_qty_30s: 'sell_qty_1s',
  buy_notional_30s: 'buy_notional_1s',
  sell_notional_30s: 'sell_notional_1s',
  signed_volume_30s: 'signed_volume_1s',
  signed_notional_30s: 'signed_notional_1s',
  max_trade_notional_30s: 'max_trade_notional_1s',
  large_trade_count_30s: 'large_trade_count_1s',
  large_trade_notional_30s: 'large_trade_notional_1s',
  trade_open_30s: 'trade_open_1s',
  trade_high_30s: 'trade_high_1s',
  trade_low_30s: 'trade_low_1s',
  trade_close_30s: 'trade_close_1s',
  ofi_30s: 'ofi_1s',
  bid_add_qty_30s: 'bid_add_qty_1s',
  bid_cancel_qty_30s: 'bid_cancel_qty_1s',
  ask_add_qty_30s: 'ask_add_qty_1s',
  ask_cancel_qty_30s: 'ask_cancel_qty_1s',
  replenishment_qty_30s: 'replenishment_qty_1s',
  pulling_qty_30s: 'pulling_qty_1s',
});
const P0_INPUT_FIELDS = new Set([
  'trade_count_1s', 'buy_trade_count_1s', 'sell_trade_count_1s',
  'traded_qty_1s', 'traded_notional_1s', 'buy_qty_1s', 'sell_qty_1s',
  'buy_notional_1s', 'sell_notional_1s', 'signed_volume_1s', 'signed_notional_1s',
  'max_trade_notional_1s', 'large_trade_count_1s', 'large_trade_notional_1s',
]);
const P1_INPUT_FIELDS = new Set(['ofi_1s', 'bid_add_qty_1s', 'bid_cancel_qty_1s', 'ask_add_qty_1s', 'ask_cancel_qty_1s', 'replenishment_qty_1s', 'pulling_qty_1s']);
const OHLC_INPUT_FIELDS = new Set(['trade_open_1s', 'trade_high_1s', 'trade_low_1s', 'trade_close_1s']);

function error(code, message) {
  const err = new Error(message);
  err.code = code;
  return err;
}

function numericValues(rows, field) {
  return rows.map((row) => row[field]).filter((value) => Number.isFinite(value));
}

function percentile95(values) {
  if (values.length === 0) return null;
  const sorted = [...values].sort((a, b) => a - b);
  return sorted[Math.max(0, Math.ceil(sorted.length * 0.95) - 1)];
}

function aggregate(values, op) {
  if (op === 'first') return values.length > 0 ? values[0] : null;
  if (op === 'last') return values.length > 0 ? values[values.length - 1] : null;
  if (op === 'sum') return values.reduce((sum, value) => sum + value, 0);
  if (op === 'mean') return values.length > 0 ? values.reduce((sum, value) => sum + value, 0) / values.length : null;
  if (op === 'max') return values.length > 0 ? Math.max(...values) : null;
  if (op === 'min') return values.length > 0 ? Math.min(...values) : null;
  if (op === 'p95') return percentile95(values);
  if (op === 'recompute') return null;
  throw error('E_ROLLUP_OPERATOR', `unsupported operator: ${op}`);
}

function validateWindow(rows) {
  if (!Array.isArray(rows) || rows.length === 0) {
    throw error('E_ROLLUP_MISSING_INPUT', '30s input window is missing');
  }
  if (rows.length !== 30) {
    throw error('E_ROLLUP_PARTIAL_WINDOW', `expected 30 rows, got ${rows.length}`);
  }

  const first = rows[0];
  if (!first || typeof first.ts !== 'number' || first.ts % 30_000 !== 0) {
    throw error('E_ROLLUP_UNALIGNED', '30s window start must be 30s aligned');
  }
  const market = first.market;
  if (typeof market !== 'string' || market.length === 0) {
    throw error('E_ROLLUP_INVALID_MARKET', '30s window market is required');
  }
  const requiredFields = [...new Set(Object.values(INPUT_FIELDS))];
  for (const row of rows) {
    const quality = row?._quality;
    const status = quality?.input_status;
    if (row?._quality?.finalized !== true) {
      throw error('E_ROLLUP_INVALID_INPUT_STATUS', 'input row must have finalized=true');
    }
    if (typeof status === 'string' && /missing|quarantine|corrupt|blocked/i.test(status)) {
      throw error('E_ROLLUP_INVALID_INPUT_STATUS', `input quality status is not rollupable: ${status}`);
    }
    for (const field of requiredFields) {
      const legacyWithoutP0 = P0_INPUT_FIELDS.has(field)
        && row?.[field] === undefined
        && row?._quality?.feature_schema_version === undefined;
      if (legacyWithoutP0) continue;
      if (P1_INPUT_FIELDS.has(field) && row?.[field] === undefined && row?._quality?.feature_schema_version === undefined) continue;
      if (P1_INPUT_FIELDS.has(field) && row?.[field] === null) continue;
      if (OHLC_INPUT_FIELDS.has(field) && (row?.[field] === undefined || row?.[field] === null)) continue;
      if (field === 'max_trade_notional_1s' && row?.[field] === null) continue;
      if (typeof row?.[field] !== 'number' || !Number.isFinite(row[field])) {
        throw error('E_ROLLUP_INVALID_FEATURE', `required feature is not finite: ${field}`);
      }
    }
  }
  const seen = new Set();
  for (let i = 0; i < rows.length; i += 1) {
    const row = rows[i];
    if (!row || row.market !== market) {
      throw error('E_ROLLUP_MIXED_MARKET', '30s window contains multiple markets');
    }
    if (typeof row.ts !== 'number' || row.ts % 1000 !== 0) {
      throw error('E_ROLLUP_UNALIGNED', 'input timestamps must be second aligned');
    }
    if (seen.has(row.ts)) throw error('E_ROLLUP_DUPLICATE', `duplicate timestamp: ${row.ts}`);
    seen.add(row.ts);
    if (i > 0 && row.ts < rows[i - 1].ts) {
      throw error('E_ROLLUP_OUT_OF_ORDER', 'input rows must be timestamp ordered');
    }
  }
  for (let i = 0; i < rows.length; i += 1) {
    const expected = first.ts + i * 1000;
    if (rows[i].ts !== expected) throw error('E_ROLLUP_MISSING_SECOND', `missing second at ${expected}`);
  }
}

function qualityFor(rows, start, empty) {
  const sourceIds = [...new Set(rows.flatMap((row) => row._quality?.input_block_ids || []))];
  return {
    phase: 'P2',
    operator: 'rollup',
    source_layer: 'features_1s',
    finalized: rows.every((row) => row._quality?.finalized === true),
    source_window_count: rows.length,
    source_window_start_ms: start,
    source_window_end_ms: start + 30_000,
    coverage: 1,
    coverage_seconds: 30,
    expected_seconds: 30,
    input_status: empty ? 'arrived-empty-valid' : 'arrived-valid',
    has_empty_input: empty,
    has_missing_input: false,
    input_block_ids: sourceIds,
    warmup: rows.some((row) => row._quality?.warmup === true),
    placeholder_policy: 'P4_only_for_book_and_research',
    operators: ROLLUP_FIELD_OPERATORS,
  };
}

/** Aggregate one complete, aligned 30-second features_1s window. */
export function aggregate30s(rows) {
  validateWindow(rows);
  const start = rows[0].ts;
  const empty = rows.every((row) => {
    const noTrades = row._quality?.trade_count_this_second === 0 || row._quality?.empty_block === true;
    return noTrades && Object.values(INPUT_FIELDS).every((field) => (
      OHLC_INPUT_FIELDS.has(field) || P1_INPUT_FIELDS.has(field) || row[field] === 0
    ));
  });
  const result = { ts: start, market: rows[0].market, _quality: qualityFor(rows, start, empty) };

  for (const field of ROLLUP_FIELDS) {
    const values = numericValues(rows, INPUT_FIELDS[field]);
    if (ROLLUP_FIELD_OPERATORS[field] !== 'recompute') {
      result[field] = aggregate(values, ROLLUP_FIELD_OPERATORS[field]);
    }
  }
  result.trade_imbalance_qty_30s = result.traded_qty_30s > 0
    ? result.signed_volume_30s / result.traded_qty_30s : 0;
  result.trade_imbalance_notional_30s = result.traded_notional_30s > 0
    ? result.signed_notional_30s / result.traded_notional_30s : 0;
  result.mean_trade_notional_30s = result.trade_count_30s > 0
    ? result.traded_notional_30s / result.trade_count_30s : null;
  result.large_trade_notional_share_30s = result.traded_notional_30s > 0
    ? result.large_trade_notional_30s / result.traded_notional_30s : 0;
  return [result];
}
