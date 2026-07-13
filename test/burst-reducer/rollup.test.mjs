// test/burst-reducer/rollup.test.mjs — independent C1 pure 30s rollup fixtures
import assert from 'node:assert/strict';
import { describe, it } from 'node:test';
import * as rollup from '../../lib/burst-reducer/rollup.mjs';

const {
  aggregate30s,
  ROLLUP_FIELDS,
  ROLLUP_FIELD_OPERATORS,
} = rollup;

const START = 30_000;
const MARKET = 'binance_spot';
const SOURCE_BLOCK = 'fixture-000030';

function makeRows({ start = START, market = MARKET, empty = false } = {}) {
  return Array.from({ length: 30 }, (_, i) => ({
    ts: start + i * 1000,
    market,
    burst_count_1s: empty ? 0 : (i % 5),
    total_burst_notional_1s: empty ? 0 : i + 1,
    max_burst_notional_1s: empty ? 0 : (i === 12 ? 91 : i + 1),
    max_burst_prints_1s: empty ? 0 : (i === 7 ? 8 : i % 3),
    max_burst_duration_ms_1s: empty ? 0 : (i === 18 ? 275 : i * 10),
    _quality: {
      source_layer: 'features_1s',
      finalized: true,
      input_block_ids: [SOURCE_BLOCK],
      empty_block: empty,
      warmup: false,
    },
  }));
}

function expectError(fn, code) {
  assert.throws(fn, (error) => {
    assert.equal(error.code, code);
    return true;
  });
}

describe('C1 pure 30s rollup', () => {
  it('exposes the approved P2 rollup-only field/operator matrix', () => {
    assert.deepEqual(ROLLUP_FIELD_OPERATORS, {
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
    assert.deepEqual(ROLLUP_FIELDS, Object.keys(ROLLUP_FIELD_OPERATORS));
  });

  it('computes hand-derived mean, max, sum, and nearest-rank p95 values', () => {
    const rows = makeRows();
    // Use distinct boundary values so the nearest-rank p95 oracle is identifiable.
    rows[27].total_burst_notional_1s = 90;
    rows[28].total_burst_notional_1s = 100;
    rows[29].total_burst_notional_1s = 101;

    const [result] = aggregate30s(rows);

    assert.equal(result.ts, START);
    assert.equal(result.market, MARKET);
    assert.equal(result.burst_count_mean_30s, 2);
    assert.equal(result.burst_count_max_30s, 4);
    assert.equal(result.burst_notional_overlap_sum_30s, 669);
    assert.equal(result.burst_notional_overlap_max_30s, 101);
    assert.equal(result.burst_notional_overlap_p95_30s, 100);
    assert.equal(result.max_burst_notional_max_30s, 91);
    assert.equal(result.max_burst_notional_mean_30s, 18.1);
    assert.equal(result.max_burst_prints_max_30s, 8);
    assert.equal(result.max_burst_duration_max_30s, 290);
  });

  it('keeps overlap exposure explicitly separate and emits no direct/recompute masquerade', () => {
    const [result] = aggregate30s(makeRows());
    assert.ok('burst_notional_overlap_sum_30s' in result);
    assert.ok(!('burst_notional_sum_30s' in result));
    assert.ok(!('burst_unique_count_30s' in result));
    assert.ok(!('burst_notional_vs_traded_notional_30s' in result));
    assert.ok(!('burst_imbalance_ratio_30s' in result));
    assert.ok(!('largest_burst_share_30s' in result));
  });

  it('does not emit #13/#14 or #15-#22 before P4', () => {
    const [result] = aggregate30s(makeRows());
    const excluded = [
      'burst_notional_vs_top_depth',
      'burst_mid_move_bps_1s',
      'same_price_burst_max_len_1s',
      'same_price_burst_notional_1s',
      'multilevel_burst_max_span_ticks_1s',
      'multilevel_burst_max_span_bps_1s',
      'multilevel_burst_notional_1s',
      'same_price_absorption_ratio_1s',
      'burst_delta_notional_1s',
      'outlier_trade_flag_1s',
    ];
    for (const field of excluded) assert.ok(!(field in result), field);
    assert.equal(result._quality.phase, 'P2');
    assert.equal(result._quality.placeholder_policy, 'P4_only_for_book_and_research');
  });

  it('propagates complete-window quality and source provenance', () => {
    const [result] = aggregate30s(makeRows());
    assert.deepEqual(result._quality, {
      phase: 'P2',
      operator: 'rollup',
      source_layer: 'features_1s',
      finalized: true,
      source_window_count: 30,
      source_window_start_ms: START,
      source_window_end_ms: START + 30_000,
      coverage: 1,
      coverage_seconds: 30,
      expected_seconds: 30,
      input_status: 'arrived-valid',
      has_empty_input: false,
      has_missing_input: false,
      input_block_ids: [SOURCE_BLOCK],
      warmup: false,
      placeholder_policy: 'P4_only_for_book_and_research',
      operators: ROLLUP_FIELD_OPERATORS,
    });
  });

  it('emits an empty-valid window while distinguishing it from missing input', () => {
    const [result] = aggregate30s(makeRows({ empty: true }));
    assert.equal(result.input_status, undefined);
    assert.equal(result._quality.input_status, 'arrived-empty-valid');
    assert.equal(result._quality.has_empty_input, true);
    assert.equal(result._quality.has_missing_input, false);
    assert.equal(result.burst_notional_overlap_sum_30s, 0);
    assert.equal(result.burst_count_mean_30s, 0);
    expectError(() => aggregate30s([]), 'E_ROLLUP_MISSING_INPUT');
    expectError(() => aggregate30s(null), 'E_ROLLUP_MISSING_INPUT');
  });

  it('fails closed on invalid required fields and missing-quality status', () => {
    const missingField = makeRows();
    delete missingField[4].total_burst_notional_1s;
    expectError(() => aggregate30s(missingField), 'E_ROLLUP_INVALID_FEATURE');

    const invalidNumber = makeRows();
    invalidNumber[4].max_burst_prints_1s = Number.NaN;
    expectError(() => aggregate30s(invalidNumber), 'E_ROLLUP_INVALID_FEATURE');

    const missingQuality = makeRows();
    missingQuality[0]._quality.input_status = 'verified-missing';
    expectError(() => aggregate30s(missingQuality), 'E_ROLLUP_INVALID_INPUT_STATUS');
  });

  it('rejects partial, missing, duplicate, out-of-order, unaligned, and mixed-market windows', () => {
    expectError(() => aggregate30s(makeRows().slice(0, 29)), 'E_ROLLUP_PARTIAL_WINDOW');

    const missingSecond = makeRows();
    for (let i = 10; i < missingSecond.length; i += 1) missingSecond[i].ts += 1000;
    expectError(() => aggregate30s(missingSecond), 'E_ROLLUP_MISSING_SECOND');

    const duplicate = makeRows();
    duplicate[10].ts = duplicate[9].ts;
    expectError(() => aggregate30s(duplicate), 'E_ROLLUP_DUPLICATE');

    const outOfOrder = makeRows();
    [outOfOrder[10], outOfOrder[11]] = [outOfOrder[11], outOfOrder[10]];
    expectError(() => aggregate30s(outOfOrder), 'E_ROLLUP_OUT_OF_ORDER');

    expectError(() => aggregate30s(makeRows({ start: 1000 })), 'E_ROLLUP_UNALIGNED');

    const mixedMarket = makeRows();
    mixedMarket[15].market = 'binance_perp';
    expectError(() => aggregate30s(mixedMarket), 'E_ROLLUP_MIXED_MARKET');
  });

  it('returns only one 30s row and has no 5min implementation in C1', () => {
    const output = aggregate30s(makeRows());
    assert.equal(output.length, 1);
    assert.equal('aggregate5min' in rollup, false);
    assert.deepEqual(Object.keys(output[0]).sort(), [
      '_quality',
      'burst_count_max_30s',
      'burst_count_mean_30s',
      'burst_notional_overlap_max_30s',
      'burst_notional_overlap_p95_30s',
      'burst_notional_overlap_sum_30s',
      'max_burst_duration_max_30s',
      'max_burst_notional_max_30s',
      'max_burst_notional_mean_30s',
      'max_burst_prints_max_30s',
      'market',
      'ts',
    ].sort());
  });
});
