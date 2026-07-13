// test/burst-reducer/b4-board.test.mjs — B4 board candidate column tests
// Independent verifier + production integration.

import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { createBaseRow, FEATURE_1S_FIELDS, BOARD_FIELDS, BOARD_CANDIDATE_FIELDS } from '../../lib/burst-reducer/schema.mjs';
import { computeFeatures1s } from '../../lib/burst-reducer/feature-computer-1s.mjs';
import { BurstDetector } from '../../lib/burst-reducer/burst-detector.mjs';

const zeroLookup = new Map(Array.from({ length: 30 }, (_, i) => [i * 1000, 0]));

describe('B4: Schema contract — board fields', () => {
  it('BOARD_FIELDS array has 4 entries', () => {
    assert.equal(BOARD_FIELDS.length, 4);
    assert.deepEqual(BOARD_FIELDS, ['board_top_depth_ratio', 'board_mid_move_bps_1s', 'board_vs_30s', 'board_vs_depth']);
  });

  it('BOARD_CANDIDATE_FIELDS Set contains all 4 board fields', () => {
    assert.equal(BOARD_CANDIDATE_FIELDS.size, 4);
    for (const f of BOARD_FIELDS) {
      assert.ok(BOARD_CANDIDATE_FIELDS.has(f));
    }
  });

  it('board fields are in FEATURE_1S_FIELDS after #22', () => {
    const idx = FEATURE_1S_FIELDS.indexOf('outlier_trade_flag_1s');
    assert.ok(idx >= 0);
    assert.equal(FEATURE_1S_FIELDS[idx + 1], 'board_top_depth_ratio');
    assert.equal(FEATURE_1S_FIELDS[idx + 2], 'board_mid_move_bps_1s');
    assert.equal(FEATURE_1S_FIELDS[idx + 3], 'board_vs_30s');
    assert.equal(FEATURE_1S_FIELDS[idx + 4], 'board_vs_depth');
  });

  it('createBaseRow initializes board fields to null', () => {
    const row = createBaseRow(1000, 'test', { book_seeded: false, trade_count_this_second: 0, warmup: true, input_block_ids: [] });
    assert.equal(row.board_top_depth_ratio, null);
    assert.equal(row.board_mid_move_bps_1s, null);
    assert.equal(row.board_vs_30s, null);
    assert.equal(row.board_vs_depth, null);
  });

  it('#13 remains null and #14 remains 0', () => {
    const row = createBaseRow(1000, 'test', { book_seeded: false, trade_count_this_second: 0, warmup: true, input_block_ids: [] });
    assert.equal(row.burst_notional_vs_top_depth, null);
    assert.equal(row.burst_mid_move_bps_1s, 0);
  });
});

describe('B4: Board candidate computation', () => {
  it('no bookSnapshot yields null board columns', () => {
    const detector = new BurstDetector('test');
    detector.feedTrades([{ ts: 1000, side: 'buy', price: 100, qty: 1 }]);
    detector.flushAll();

    const rows = computeFeatures1s({
      detector,
      blockStartMs: 0,
      tradeTsList: [1000],
      warmup: false,
      inputBlockIds: ['test'],
      lookupTradedNotional30s: zeroLookup,
    });

    for (const row of rows) {
      assert.equal(row.board_top_depth_ratio, null);
      assert.equal(row.board_mid_move_bps_1s, null);
      assert.equal(row.board_vs_30s, null);
      assert.equal(row.board_vs_depth, null);
    }
  });

  it('bookSnapshot with available seeded state computes board_top_depth_ratio', () => {
    const detector = new BurstDetector('test');
    detector.feedTrades([{ ts: 1000, side: 'buy', price: 100, qty: 1 }]);
    detector.flushAll();

    const bookSnapshot = {
      available: true,
      book_seeded: true,
      state: { seeded: true, best_bid: 100, best_bid_qty: 2, best_ask: 101, best_ask_qty: 3, mid: 100.5, last_seq: 10 },
    };

    const rows = computeFeatures1s({
      detector,
      blockStartMs: 0,
      tradeTsList: [1000],
      warmup: false,
      inputBlockIds: ['test'],
      lookupTradedNotional30s: zeroLookup,
      bookSnapshot,
    });

    // Row at ts=1000: totalNotional=100 (1*100), topDepth=100*2+101*3=503
    // board_top_depth_ratio = 100/503 ≈ 0.1988
    const row1 = rows[1]; // secondTs=1000
    assert.equal(row1.ts, 1000);
    assert.ok(row1.board_top_depth_ratio > 0.19 && row1.board_top_depth_ratio < 0.20, `ratio=${row1.board_top_depth_ratio}`);
    // board_vs_depth is alias
    assert.equal(row1.board_vs_depth, row1.board_top_depth_ratio);
    // board_vs_30s = 100/0 = null (denom=0)
    assert.equal(row1.board_vs_30s, null);
    // board_mid_move_bps_1s = null (no prior cross-block state in B4)
    assert.equal(row1.board_mid_move_bps_1s, null);
  });

  it('bookSnapshot unavailable yields null board columns', () => {
    const detector = new BurstDetector('test');
    detector.feedTrades([{ ts: 1000, side: 'buy', price: 100, qty: 1 }]);
    detector.flushAll();

    const bookSnapshot = { available: false, book_seeded: false };

    const rows = computeFeatures1s({
      detector,
      blockStartMs: 0,
      tradeTsList: [1000],
      warmup: false,
      inputBlockIds: ['test'],
      lookupTradedNotional30s: zeroLookup,
      bookSnapshot,
    });

    for (const row of rows) {
      assert.equal(row.board_top_depth_ratio, null);
      assert.equal(row.board_mid_move_bps_1s, null);
    }
  });

  it('book_seeded=false yields null board columns', () => {
    const detector = new BurstDetector('test');
    detector.feedTrades([{ ts: 1000, side: 'buy', price: 100, qty: 1 }]);
    detector.flushAll();

    const bookSnapshot = { available: true, book_seeded: false, state: { seeded: false, best_bid: null, best_ask: null } };

    const rows = computeFeatures1s({
      detector,
      blockStartMs: 0,
      tradeTsList: [1000],
      warmup: false,
      inputBlockIds: ['test'],
      lookupTradedNotional30s: zeroLookup,
      bookSnapshot,
    });

    for (const row of rows) {
      assert.equal(row.board_top_depth_ratio, null);
      assert.equal(row.board_vs_depth, null);
    }
  });

  it('#13 set when bookSnapshot provided with seeded state', () => {
    const detector = new BurstDetector('test');
    detector.feedTrades([{ ts: 1000, side: 'buy', price: 100, qty: 1 }]);
    detector.flushAll();

    const bookSnapshot = {
      available: true,
      book_seeded: true,
      state: { seeded: true, best_bid: 100, best_bid_qty: 2, best_ask: 101, best_ask_qty: 3, mid: 100.5 },
    };

    const rows = computeFeatures1s({
      detector,
      blockStartMs: 0,
      tradeTsList: [1000],
      warmup: false,
      inputBlockIds: ['test'],
      lookupTradedNotional30s: zeroLookup,
      bookSnapshot,
    });

    for (const row of rows) {
      // #13 = board_top_depth_ratio when book is seeded
      if (row.burst_count_1s > 0) {
        assert.equal(row.burst_notional_vs_top_depth, row.board_top_depth_ratio, `#13 should equal board_top_depth_ratio at ts ${row.ts}`);
      } else {
        assert.equal(row.burst_notional_vs_top_depth, null, `#13 should be null at ts ${row.ts} (no burst)`);
      }
      // #14 stays 0 (no cross-block mid state at row level)
      assert.equal(row.burst_mid_move_bps_1s, 0, `#14 should be 0 at ts ${row.ts}`);
    }
  });
});
