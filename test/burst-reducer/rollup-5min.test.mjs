// test/burst-reducer/rollup-5min.test.mjs — P3-C1 pure 5min summary fixtures
import assert from 'node:assert/strict';
import { describe, it } from 'node:test';
import * as rollup5min from '../../lib/burst-reducer/rollup-5min.mjs';

const {
  aggregate5min,
  FIVEMIN_FIELDS,
  FIVEMIN_FIELD_OPERATORS,
} = rollup5min;

const START = 300_000; // 5min-aligned: 300s = 5min
const MARKET = 'binance_spot';
const SOURCE_BLOCK = 'fixture-5min-000';

function makeRows({ start = START, market = MARKET, empty = false } = {}) {
  return Array.from({ length: 10 }, (_, i) => ({
    ts: start + i * 30_000,
    market,
    burst_count_mean_30s: empty ? 0 : i + 1,
    burst_count_max_30s: empty ? 0 : (i % 4) + 1,
    burst_notional_overlap_sum_30s: empty ? 0 : (i + 1) * 10,
    burst_notional_overlap_max_30s: empty ? 0 : (i === 9 ? 200 : (i + 1) * 10),
    burst_notional_overlap_p95_30s: empty ? 0 : (i + 1) * 5,
    max_burst_notional_max_30s: empty ? 0 : (i === 5 ? 500 : (i + 1) * 20),
    max_burst_notional_mean_30s: empty ? 0 : i + 0.5,
    max_burst_prints_max_30s: empty ? 0 : (i % 3) + 1,
    max_burst_duration_max_30s: empty ? 0 : (i === 7 ? 999 : i * 50),
    trade_open_30s: empty ? null : 10_000 + i,
    trade_high_30s: empty ? null : 10_000 + i + 0.5,
    trade_low_30s: empty ? null : 9_999.5 + i,
    trade_close_30s: empty ? null : 10_000 + i + 0.25,
    trade_count_30s: empty ? 0 : (i + 1) * 30,
    buy_trade_count_30s: empty ? 0 : i * 30,
    sell_trade_count_30s: empty ? 0 : 30,
    traded_qty_30s: empty ? 0 : (i + 1) * 60,
    traded_notional_30s: empty ? 0 : (i + 1) * 300,
    buy_qty_30s: empty ? 0 : (i + 1) * 30,
    sell_qty_30s: empty ? 0 : (i + 1) * 30,
    buy_notional_30s: empty ? 0 : (i + 1) * 180,
    sell_notional_30s: empty ? 0 : (i + 1) * 120,
    signed_volume_30s: empty ? 0 : 0,
    signed_notional_30s: empty ? 0 : (i + 1) * 60,
    max_trade_notional_30s: empty ? 0 : (i + 1) * 10,
    large_trade_count_30s: empty ? 0 : i % 2,
    large_trade_notional_30s: empty ? 0 : (i % 2) * (i + 1) * 20,
    _quality: {
      source_layer: 'features_30s',
      input_block_ids: [SOURCE_BLOCK],
      input_status: empty ? 'arrived-empty-valid' : 'arrived-valid',
      has_empty_input: empty,
      has_missing_input: false,
      coverage: 1,
      coverage_seconds: 30,
      expected_seconds: 30,
      finalized: true,
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

describe('P3-C1 pure 5min summary', () => {
  it('exposes the approved P3 field/operator matrix', () => {
    assert.deepEqual(FIVEMIN_FIELD_OPERATORS, {
      burst_count_mean_5min: 'mean',
      burst_count_max_5min: 'max',
      burst_notional_overlap_sum_5min: 'sum',
      burst_notional_overlap_max_5min: 'max',
      burst_notional_overlap_p95_5min: 'p95',
      max_burst_notional_max_5min: 'max',
      max_burst_notional_mean_5min: 'mean',
      max_burst_prints_max_5min: 'max',
      max_burst_duration_max_5min: 'max',
      trade_count_5min: 'sum',
      buy_trade_count_5min: 'sum',
      sell_trade_count_5min: 'sum',
      traded_qty_5min: 'sum',
      traded_notional_5min: 'sum',
      buy_qty_5min: 'sum',
      sell_qty_5min: 'sum',
      buy_notional_5min: 'sum',
      sell_notional_5min: 'sum',
      signed_volume_5min: 'sum',
      signed_notional_5min: 'sum',
      max_trade_notional_5min: 'max',
      large_trade_count_5min: 'sum',
      large_trade_notional_5min: 'sum',
      trade_imbalance_qty_5min: 'recompute',
      trade_imbalance_notional_5min: 'recompute',
      mean_trade_notional_5min: 'recompute',
      large_trade_notional_share_5min: 'recompute',
      trade_open_5min: 'first',
      trade_high_5min: 'max',
      trade_low_5min: 'min',
      trade_close_5min: 'last',
      ofi_5min: 'sum', bid_add_qty_5min: 'sum', bid_cancel_qty_5min: 'sum',
      ask_add_qty_5min: 'sum', ask_cancel_qty_5min: 'sum',
      replenishment_qty_5min: 'sum', pulling_qty_5min: 'sum',
    });
    assert.deepEqual(FIVEMIN_FIELDS, Object.keys(FIVEMIN_FIELD_OPERATORS));
  });

  it('computes hand-derived mean, max, sum, and nearest-rank p95 values from 10 30s rows', () => {
    const rows = makeRows();
    const [result] = aggregate5min(rows);

    assert.equal(result.ts, START);
    assert.equal(result.market, MARKET);
    // burst_count_mean_30s values: 1,2,3,4,5,6,7,8,9,10 → mean=5.5
    assert.equal(result.burst_count_mean_5min, 5.5);
    // burst_count_max_30s values: 1,2,3,4,1,2,3,4,1,2 → max=4
    assert.equal(result.burst_count_max_5min, 4);
    // burst_notional_overlap_sum_30s: 10,20,30,40,50,60,70,80,90,100 → sum=550
    assert.equal(result.burst_notional_overlap_sum_5min, 550);
    // burst_notional_overlap_max_30s: 10,20,30,40,50,60,70,80,90,200 → max=200
    assert.equal(result.burst_notional_overlap_max_5min, 200);
    // burst_notional_overlap_p95_30s: 5,10,15,20,25,30,35,40,45,50 → rank ceil(10*0.95)=10 → idx=9 → 50
    assert.equal(result.burst_notional_overlap_p95_5min, 50);
    // max_burst_notional_max_30s: 20,40,60,80,100,500,140,160,180,200 → max=500
    assert.equal(result.max_burst_notional_max_5min, 500);
    // max_burst_notional_mean_30s: 0.5,1.5,2.5,3.5,4.5,5.5,6.5,7.5,8.5,9.5 → mean=5.0
    assert.equal(result.max_burst_notional_mean_5min, 5);
    // max_burst_prints_max_30s: 1,2,3,1,2,3,1,2,3,1 → max=3
    assert.equal(result.max_burst_prints_max_5min, 3);
    // max_burst_duration_max_30s: 0,50,100,150,200,250,300,999,400,450 → max=999
    assert.equal(result.max_burst_duration_max_5min, 999);
    assert.equal(result.trade_open_5min, 10_000);
    assert.equal(result.trade_high_5min, 10_009.5);
    assert.equal(result.trade_low_5min, 9_999.5);
    assert.equal(result.trade_close_5min, 10_009.25);
  });

  it('keeps overlap exposure explicitly separate and emits no direct/recompute masquerade', () => {
    const [result] = aggregate5min(makeRows());
    assert.ok('burst_notional_overlap_sum_5min' in result);
    assert.ok(!('burst_notional_sum_5min' in result));
    assert.ok(!('burst_unique_count_5min' in result));
    assert.ok(!('burst_notional_vs_traded_notional_5min' in result));
    assert.ok(!('burst_imbalance_ratio_5min' in result));
    assert.ok(!('largest_burst_share_5min' in result));
  });

  it('does not emit book-dependent or research fields before P4', () => {
    const [result] = aggregate5min(makeRows());
    const excluded = [
      'burst_notional_vs_top_depth_5min',
      'burst_mid_move_bps_5min',
      'same_price_burst_max_len_5min',
      'same_price_burst_notional_5min',
      'multilevel_burst_max_span_ticks_5min',
      'multilevel_burst_max_span_bps_5min',
      'multilevel_burst_notional_5min',
      'same_price_absorption_ratio_5min',
      'burst_delta_notional_5min',
      'outlier_trade_flag_5min',
    ];
    for (const field of excluded) assert.ok(!(field in result), field);
    assert.equal(result._quality.phase, 'P3');
    assert.equal(result._quality.placeholder_policy, 'P4_only_for_book_and_research');
  });

  it('propagates complete-window quality and source provenance', () => {
    const [result] = aggregate5min(makeRows());
    assert.deepEqual(result._quality, {
      phase: 'P3',
      operator: 'rollup',
      source_layer: 'features_30s',
      source_window_count: 10,
      source_window_start_ms: START,
      source_window_end_ms: START + 300_000,
      coverage: 1,
      coverage_seconds: 300,
      expected_seconds: 300,
      input_status: 'arrived-valid',
      has_empty_input: false,
      has_missing_input: false,
      finalized: true,
      input_block_ids: [SOURCE_BLOCK],
      warmup: false,
      placeholder_policy: 'P4_only_for_book_and_research',
      operators: FIVEMIN_FIELD_OPERATORS,
    });
  });

  it('fails closed on missing or contradictory input provenance', () => {
    const cases = [
      ['missing quality', (rows) => { delete rows[0]._quality; }],
      ['wrong source layer', (rows) => { rows[0]._quality.source_layer = 'features_1s'; }],
      ['not-yet-arrived', (rows) => { rows[0]._quality.input_status = 'not-yet-arrived'; }],
      ['partial coverage', (rows) => { rows[0]._quality.coverage = 0.9; }],
      ['missing input flag', (rows) => { rows[0]._quality.has_missing_input = true; }],
      ['missing finalized', (rows) => { delete rows[0]._quality.finalized; }],
    ];
    for (const [label, mutate] of cases) {
      const rows = makeRows();
      mutate(rows);
      assert.throws(() => aggregate5min(rows), undefined, label);
    }
  });

  it('emits an empty-valid window while distinguishing it from missing input', () => {
    const [result] = aggregate5min(makeRows({ empty: true }));
    assert.equal(result._quality.input_status, 'arrived-empty-valid');
    assert.equal(result._quality.has_empty_input, true);
    assert.equal(result._quality.has_missing_input, false);
    assert.equal(result.burst_notional_overlap_sum_5min, 0);
    assert.equal(result.burst_count_mean_5min, 0);
    expectError(() => aggregate5min([]), 'E_5MIN_MISSING_INPUT');
    expectError(() => aggregate5min(null), 'E_5MIN_MISSING_INPUT');
  });

  it('fails closed on invalid required fields and missing-quality status', () => {
    const missingField = makeRows();
    delete missingField[4].burst_notional_overlap_sum_30s;
    expectError(() => aggregate5min(missingField), 'E_5MIN_INVALID_FEATURE');

    const invalidNumber = makeRows();
    invalidNumber[4].max_burst_prints_max_30s = Number.NaN;
    expectError(() => aggregate5min(invalidNumber), 'E_5MIN_INVALID_FEATURE');

    const missingQuality = makeRows();
    missingQuality[0]._quality.input_status = 'verified-missing';
    expectError(() => aggregate5min(missingQuality), 'E_5MIN_INVALID_INPUT_STATUS');
  });

  it('rejects partial, missing-window, duplicate, out-of-order, unaligned, and mixed-market windows', () => {
    // Partial: 9 instead of 10
    expectError(() => aggregate5min(makeRows().slice(0, 9)), 'E_5MIN_PARTIAL_WINDOW');

    // Missing window: shift one row to break consecutive 30s alignment
    const missingWindow = makeRows();
    for (let i = 5; i < missingWindow.length; i += 1) missingWindow[i].ts += 30_000;
    expectError(() => aggregate5min(missingWindow), 'E_5MIN_MISSING_WINDOW');

    // Duplicate timestamp
    const duplicate = makeRows();
    duplicate[3].ts = duplicate[2].ts;
    expectError(() => aggregate5min(duplicate), 'E_5MIN_DUPLICATE');

    // Out of order
    const outOfOrder = makeRows();
    [outOfOrder[3], outOfOrder[4]] = [outOfOrder[4], outOfOrder[3]];
    expectError(() => aggregate5min(outOfOrder), 'E_5MIN_OUT_OF_ORDER');

    // Not 5min-aligned start (30s boundary only)
    expectError(() => aggregate5min(makeRows({ start: 30_000 })), 'E_5MIN_UNALIGNED');

    // Mixed market
    const mixedMarket = makeRows();
    mixedMarket[3].market = 'binance_perp';
    expectError(() => aggregate5min(mixedMarket), 'E_5MIN_MIXED_MARKET');
  });

  it('returns only one 5min row and has no 5min implementation in C1 30s rollup', () => {
    const output = aggregate5min(makeRows());
    assert.equal(output.length, 1);
    assert.deepEqual(Object.keys(output[0]).sort(), [
      '_quality',
      'ask_add_qty_5min', 'ask_cancel_qty_5min', 'bid_add_qty_5min', 'bid_cancel_qty_5min',
      'burst_count_max_5min',
      'burst_count_mean_5min',
      'burst_notional_overlap_max_5min',
      'burst_notional_overlap_p95_5min',
      'burst_notional_overlap_sum_5min',
      'max_burst_duration_max_5min',
      'max_burst_notional_max_5min',
      'max_burst_notional_mean_5min',
      'max_burst_prints_max_5min',
      'trade_count_5min',
      'buy_trade_count_5min',
      'sell_trade_count_5min',
      'traded_qty_5min',
      'traded_notional_5min',
      'buy_qty_5min',
      'sell_qty_5min',
      'buy_notional_5min',
      'sell_notional_5min',
      'signed_volume_5min',
      'signed_notional_5min',
      'max_trade_notional_5min',
      'large_trade_count_5min',
      'large_trade_notional_5min',
      'trade_imbalance_qty_5min',
      'trade_imbalance_notional_5min',
      'mean_trade_notional_5min',
      'large_trade_notional_share_5min',
      'trade_open_5min',
      'trade_high_5min',
      'trade_low_5min',
      'trade_close_5min',
      'ofi_5min', 'pulling_qty_5min', 'replenishment_qty_5min',
      'market',
      'ts',
    ].sort());
  });
});
