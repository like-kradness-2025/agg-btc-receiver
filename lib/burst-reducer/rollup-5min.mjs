// lib/burst-reducer/rollup-5min.mjs — pure 5min aggregation from complete 30s rows
// P3-C1: market-local summary only; cross-market joins deferred.

const FIVEMIN_FIELD_OPERATORS = Object.freeze({
  burst_count_mean_5min: 'mean',
  burst_count_max_5min: 'max',
  burst_notional_overlap_sum_5min: 'sum',
  burst_notional_overlap_max_5min: 'max',
  burst_notional_overlap_p95_5min: 'p95',
  max_burst_notional_max_5min: 'max',
  max_burst_notional_mean_5min: 'mean',
  max_burst_prints_max_5min: 'max',
  max_burst_duration_max_5min: 'max',
});

export const FIVEMIN_FIELDS = Object.freeze(Object.keys(FIVEMIN_FIELD_OPERATORS));
export { FIVEMIN_FIELD_OPERATORS };

const INPUT_FIELDS = Object.freeze({
  burst_count_mean_5min: 'burst_count_mean_30s',
  burst_count_max_5min: 'burst_count_max_30s',
  burst_notional_overlap_sum_5min: 'burst_notional_overlap_sum_30s',
  burst_notional_overlap_max_5min: 'burst_notional_overlap_max_30s',
  burst_notional_overlap_p95_5min: 'burst_notional_overlap_p95_30s',
  max_burst_notional_max_5min: 'max_burst_notional_max_30s',
  max_burst_notional_mean_5min: 'max_burst_notional_mean_30s',
  max_burst_prints_max_5min: 'max_burst_prints_max_30s',
  max_burst_duration_max_5min: 'max_burst_duration_max_30s',
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
  throw error('E_5MIN_OPERATOR', `unsupported operator: ${op}`);
}

function validateWindow(rows) {
  if (!Array.isArray(rows) || rows.length === 0) {
    throw error('E_5MIN_MISSING_INPUT', '5min input window is missing');
  }
  if (rows.length !== 10) {
    throw error('E_5MIN_PARTIAL_WINDOW', `expected 10 rows, got ${rows.length}`);
  }

  const first = rows[0];
  if (!first || typeof first.ts !== 'number' || first.ts % 300_000 !== 0) {
    throw error('E_5MIN_UNALIGNED', '5min window start must be 5min aligned');
  }
  const market = first.market;
  if (typeof market !== 'string' || market.length === 0) {
    throw error('E_5MIN_INVALID_MARKET', '5min window market is required');
  }
  const requiredFields = [...new Set(Object.values(INPUT_FIELDS))];
  for (const row of rows) {
    const quality = row?._quality;
    if (!quality || typeof quality !== 'object') {
      throw error('E_5MIN_INVALID_QUALITY', 'features_30s quality is required');
    }
    if (quality.source_layer !== 'features_30s') {
      throw error('E_5MIN_INVALID_QUALITY', 'source_layer must be features_30s');
    }
    if (quality.input_status !== 'arrived-valid' && quality.input_status !== 'arrived-empty-valid') {
      throw error('E_5MIN_INVALID_INPUT_STATUS', `input quality status is not rollupable: ${quality.input_status}`);
    }
    if (quality.has_missing_input !== false) {
      throw error('E_5MIN_INVALID_QUALITY', 'has_missing_input must be false');
    }
    if (quality.coverage !== 1 || quality.coverage_seconds !== 30 || quality.expected_seconds !== 30) {
      throw error('E_5MIN_INVALID_QUALITY', 'features_30s coverage must be complete');
    }
    if (typeof quality.finalized !== 'boolean') {
      throw error('E_5MIN_INVALID_QUALITY', 'finalized quality is required');
    }
    for (const field of requiredFields) {
      if (typeof row?.[field] !== 'number' || !Number.isFinite(row[field])) {
        throw error('E_5MIN_INVALID_FEATURE', `required feature is not finite: ${field}`);
      }
    }
  }
  const seen = new Set();
  for (let i = 0; i < rows.length; i += 1) {
    const row = rows[i];
    if (!row || row.market !== market) {
      throw error('E_5MIN_MIXED_MARKET', '5min window contains multiple markets');
    }
    if (typeof row.ts !== 'number' || row.ts % 30_000 !== 0) {
      throw error('E_5MIN_UNALIGNED', 'input timestamps must be 30s aligned');
    }
    if (seen.has(row.ts)) throw error('E_5MIN_DUPLICATE', `duplicate timestamp: ${row.ts}`);
    seen.add(row.ts);
    if (i > 0 && row.ts < rows[i - 1].ts) {
      throw error('E_5MIN_OUT_OF_ORDER', 'input rows must be timestamp ordered');
    }
  }
  for (let i = 0; i < rows.length; i += 1) {
    const expected = first.ts + i * 30_000;
    if (rows[i].ts !== expected) throw error('E_5MIN_MISSING_WINDOW', `missing 30s window at ${expected}`);
  }
}

function qualityFor(rows, start, empty) {
  const sourceIds = [...new Set(rows.flatMap((row) => row._quality?.input_block_ids || []))];
  return {
    phase: 'P3',
    operator: 'rollup',
    source_layer: 'features_30s',
    source_window_count: rows.length,
    source_window_start_ms: start,
    source_window_end_ms: start + 300_000,
    coverage: 1,
    coverage_seconds: 300,
    expected_seconds: 300,
    input_status: empty ? 'arrived-empty-valid' : 'arrived-valid',
    has_empty_input: empty,
    has_missing_input: false,
    finalized: rows.every((row) => row._quality.finalized === true),
    input_block_ids: sourceIds,
    warmup: rows.some((row) => row._quality?.warmup === true),
    placeholder_policy: 'P4_only_for_book_and_research',
    operators: FIVEMIN_FIELD_OPERATORS,
  };
}

/** Aggregate ten complete, aligned, same-market 30s features rows into one 5min summary. */
export function aggregate5min(rows) {
  validateWindow(rows);
  const start = rows[0].ts;
  const empty = rows.every((row) => {
    const noActivity = row._quality?.has_empty_input === true
      || row._quality?.input_status === 'arrived-empty-valid';
    return noActivity && Object.values(INPUT_FIELDS).every((field) => row[field] === 0);
  });
  const result = { ts: start, market: rows[0].market, _quality: qualityFor(rows, start, empty) };

  for (const field of FIVEMIN_FIELDS) {
    const values = numericValues(rows, INPUT_FIELDS[field]);
    result[field] = aggregate(values, FIVEMIN_FIELD_OPERATORS[field]);
  }
  return [result];
}
