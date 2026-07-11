// test/burst-reducer/schema.test.mjs — Schema contract tests
import assert from 'node:assert/strict';
import { describe, it } from 'node:test';
import {
  SCHEMA_VERSION,
  GAP_THRESHOLD_MS,
  MAX_BURST_DURATION_MS,
  FEATURE_1S_FIELDS,
  ROW_ENVELOPE_FIELDS,
  createBaseRow,
  PHASE1_FIELDS,
  BOOK_DEPENDENT_FIELDS,
  RESEARCH_FIELDS,
  MONITORING_FIELDS,
  floorToSecond,
  generateSeconds,
  getTickSize,
} from '../../lib/burst-reducer/schema.mjs';

describe('Schema / contract', () => {
  it('SCHEMA_VERSION is burst_features_v1', () => {
    assert.equal(SCHEMA_VERSION, 'burst_features_v1');
  });

  it('fixed burst detection parameters', () => {
    assert.equal(GAP_THRESHOLD_MS, 50);
    assert.equal(MAX_BURST_DURATION_MS, 5000);
  });

  it('FEATURE_1S_FIELDS has exactly 22 logical feature columns', () => {
    assert.equal(FEATURE_1S_FIELDS.length, 22);
  });

  it('ROW_ENVELOPE_FIELDS are ts, market, _quality', () => {
    assert.deepEqual(ROW_ENVELOPE_FIELDS, ['ts', 'market', '_quality']);
  });

  it('createBaseRow produces 25 physical JSON top-level keys', () => {
    const row = createBaseRow(1000, 'test', {
      book_seeded: false,
      trade_count_this_second: 0,
      warmup: true,
      input_block_ids: [],
    });
    assert.equal(Object.keys(row).length, 25);
  });

  it('createBaseRow has correct P1 contract values', () => {
    const row = createBaseRow(1000, 'test', {
      book_seeded: false,
      trade_count_this_second: 0,
      warmup: true,
      input_block_ids: [],
    });
    // #13 = null
    assert.equal(row.burst_notional_vs_top_depth, null);
    // #14 = 0 (not null)
    assert.equal(row.burst_mid_move_bps_1s, 0);
    // #15-#22 = 0
    assert.equal(row.same_price_burst_max_len_1s, 0);
    assert.equal(row.same_price_burst_notional_1s, 0);
    assert.equal(row.multilevel_burst_max_span_ticks_1s, 0);
    assert.equal(row.multilevel_burst_max_span_bps_1s, 0);
    assert.equal(row.multilevel_burst_notional_1s, 0);
    assert.equal(row.same_price_absorption_ratio_1s, 0);
    assert.equal(row.burst_delta_notional_1s, 0);
    assert.equal(row.outlier_trade_flag_1s, 0);
    // #1-#12 = 0
    assert.equal(row.burst_count_1s, 0);
    assert.equal(row.total_burst_notional_1s, 0);
    assert.equal(row.burst_notional_vs_30s_traded_notional, 0);
  });

  it('createBaseRow includes ts, market, _quality', () => {
    const row = createBaseRow(5000, 'binance_spot', {
      book_seeded: false,
      trade_count_this_second: 3,
      warmup: false,
      input_block_ids: ['0'],
    });
    assert.equal(row.ts, 5000);
    assert.equal(row.market, 'binance_spot');
    assert.equal(row._quality.book_seeded, false);
    assert.equal(row._quality.trade_count_this_second, 3);
    assert.equal(row._quality.warmup, false);
    assert.deepEqual(row._quality.input_block_ids, ['0']);
  });

  it('field set sizes are correct', () => {
    assert.equal(PHASE1_FIELDS.size, 12);
    assert.equal(BOOK_DEPENDENT_FIELDS.size, 2);
    assert.equal(RESEARCH_FIELDS.size, 7);
    assert.equal(MONITORING_FIELDS.size, 1);
  });

  it('floorToSecond works', () => {
    assert.equal(floorToSecond(0), 0);
    assert.equal(floorToSecond(500), 0);
    assert.equal(floorToSecond(1000), 1000);
    assert.equal(floorToSecond(1500), 1000);
    assert.equal(floorToSecond(29999), 29000);
  });

  it('generateSeconds produces 30 timestamps', () => {
    const gen = generateSeconds(0);
    const results = [...gen];
    assert.equal(results.length, 30);
    assert.equal(results[0], 0);
    assert.equal(results[29], 29000);
  });

  it('generateSeconds works from non-zero start', () => {
    const gen = generateSeconds(30000);
    const results = [...gen];
    assert.equal(results.length, 30);
    assert.equal(results[0], 30000);
    assert.equal(results[29], 59000);
  });

  it('getTickSize returns correct values', () => {
    assert.equal(getTickSize('binance_spot'), 0.01);
    assert.equal(getTickSize('binance_perp'), 1.0);
    assert.equal(getTickSize('unknown_market'), null);
  });
});
