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
});

function error(code, message) {
  const err = new Error(message);
  err.code = code;
  return err;
}

function numericValues(rows, field) {
  return rows.map((row) => row[field]);
}

function percentile95(values) {
  const sorted = [...values].sort((a, b) => a - b);
  return sorted[Math.max(0, Math.ceil(sorted.length * 0.95) - 1)];
}

function aggregate(values, op) {
  if (op === 'sum') return values.reduce((sum, value) => sum + value, 0);
  if (op === 'mean') return values.reduce((sum, value) => sum + value, 0) / values.length;
  if (op === 'max') return Math.max(...values);
  if (op === 'p95') return percentile95(values);
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
    const status = row?._quality?.input_status;
    if (typeof status === 'string' && /missing|quarantine|corrupt|blocked/i.test(status)) {
      throw error('E_ROLLUP_INVALID_INPUT_STATUS', `input quality status is not rollupable: ${status}`);
    }
    for (const field of requiredFields) {
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
    return noTrades && Object.values(INPUT_FIELDS).every((field) => row[field] === 0);
  });
  const result = { ts: start, market: rows[0].market, _quality: qualityFor(rows, start, empty) };

  for (const field of ROLLUP_FIELDS) {
    const values = numericValues(rows, INPUT_FIELDS[field]);
    result[field] = aggregate(values, ROLLUP_FIELD_OPERATORS[field]);
  }
  return [result];
}
