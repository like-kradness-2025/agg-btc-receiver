// test/burst-reducer/feature-computer-1s.test.mjs — FeatureComputer 1s tests
import assert from 'node:assert/strict';
import { describe, it } from 'node:test';
import { BurstDetector } from '../../lib/burst-reducer/burst-detector.mjs';
import { computeFeatures1s } from '../../lib/burst-reducer/feature-computer-1s.mjs';

// Helper: complete 30-key zero-denominator lookup for valid-path tests
const zeroLookup = new Map(Array.from({length: 30}, (_, i) => [i * 1000, 0]));

describe('FeatureComputer 1s', () => {
  it('returns 30 rows for a block', () => {
    const bd = new BurstDetector('test');
    bd.feedTrades([{ ts: 1000, side: 'buy', price: 100, qty: 1 }]);
    bd.flushAll();

    const rows = computeFeatures1s({
      detector: bd,
      blockStartMs: 0,
      tradeTsList: [1000],
      warmup: true,
      inputBlockIds: ['0'],
      lookupTradedNotional30s: zeroLookup,
    });
    assert.equal(rows.length, 30); // 30秒 = 30 rows
  });

  it('zero-fills seconds with no bursts', () => {
    const bd = new BurstDetector('test');
    const rows = computeFeatures1s({
      detector: bd,
      blockStartMs: 0,
      tradeTsList: [],
      warmup: true,
      inputBlockIds: ['0'],
      lookupTradedNotional30s: zeroLookup,
    });
    for (const row of rows) {
      assert.equal(row.burst_count_1s, 0);
      assert.equal(row.total_burst_notional_1s, 0);
    }
  });

  it('computes correct features for one burst', () => {
    const bd = new BurstDetector('test');
    bd.feedTrades([
      { ts: 500, side: 'buy', price: 100, qty: 1 },
      { ts: 520, side: 'buy', price: 100, qty: 2 },
    ]);
    bd.flushAll();

    const rows = computeFeatures1s({
      detector: bd,
      blockStartMs: 0,
      tradeTsList: [500, 520],
      warmup: true,
      inputBlockIds: ['0'],
      lookupTradedNotional30s: zeroLookup,
    });

    const row0 = rows[0]; // secondTs = 0
    assert.equal(row0.burst_count_1s, 1); // burst [500,520] overlaps [0,1000)
    assert.equal(row0.total_burst_notional_1s, 300);
    assert.equal(row0.max_burst_notional_1s, 300);
    assert.equal(row0.buy_burst_notional_1s, 300);
    assert.equal(row0.sell_burst_notional_1s, 0);
    assert.equal(row0.burst_imbalance_ratio_1s.toFixed(4), '1.0000'); // all buy (approx 1.0)
    assert.equal(row0.same_price_burst_count_1s, 1);
    assert.equal(row0.multilevel_burst_count_1s, 0);

    const row1 = rows[1]; // secondTs = 1000
    assert.equal(row1.burst_count_1s, 0); // burst ended at 520, no overlap with [1000,2000)
  });

  it('warmup flag is set', () => {
    const bd = new BurstDetector('test');
    const rows = computeFeatures1s({
      detector: bd, blockStartMs: 0, tradeTsList: [], warmup: true, inputBlockIds: ['0'],
      lookupTradedNotional30s: zeroLookup,
    });
    for (const row of rows) {
      assert.equal(row._quality.warmup, true);
    }
  });

  it('uses full replayed book levels for multi-level dollar depth', () => {
    const bd = new BurstDetector('test');
    const rows = computeFeatures1s({
      detector: bd,
      blockStartMs: 0,
      tradeTsList: [],
      warmup: false,
      inputBlockIds: ['0'],
      lookupTradedNotional30s: zeroLookup,
      bookSnapshot: {
        available: true,
        book_seeded: true,
        state: {
          seeded: true,
          best_bid: 100,
          best_bid_qty: 2,
          best_ask: 101,
          best_ask_qty: 3,
          mid: 100.5,
          bids: [[100, 2], [99.5, 4], [0, 999]],
          asks: [[101, 3], [101.5, 5], [2000, 999]],
        },
      },
    });

    // All normal levels are inside both windows; invalidly distant levels are not.
    assert.equal(rows[0].book_bid_depth_100, 100 * 2 + 99.5 * 4);
    assert.equal(rows[0].book_ask_depth_100, 101 * 3 + 101.5 * 5);
    assert.equal(rows[0].book_bid_depth_1000, rows[0].book_bid_depth_100);
    assert.equal(rows[0].book_ask_depth_1000, rows[0].book_ask_depth_100);
  });

  it('uses the strict pre-second book state without lookahead', () => {
    const bd = new BurstDetector('test');
    const statesBySecond = new Map([
      [0, { seeded: true, best_bid: 100, best_bid_qty: 1, best_ask: 101, best_ask_qty: 1 }],
      [1000, { seeded: true, best_bid: 102, best_bid_qty: 1, best_ask: 103, best_ask_qty: 1 }],
    ]);
    const rows = computeFeatures1s({
      detector: bd,
      blockStartMs: 0,
      tradeTsList: [],
      warmup: false,
      inputBlockIds: ['0'],
      lookupTradedNotional30s: zeroLookup,
      bookSnapshot: { available: true, book_seeded: true, statesBySecond },
    });
    assert.equal(rows[0].book_mid_price, 100.5);
    assert.equal(rows[1].book_mid_price, 102.5);
    assert.equal(rows[2].book_mid_price, null);
  });

  it('invariants hold', () => {
    const bd = new BurstDetector('test');
    bd.feedTrades([
      { ts: 500, side: 'buy', price: 100, qty: 1 },
      { ts: 540, side: 'sell', price: 101, qty: 1 },
    ]);
    bd.flushAll();
    const rows = computeFeatures1s({
      detector: bd, blockStartMs: 0, tradeTsList: [500, 540], warmup: false, inputBlockIds: ['0'],
      lookupTradedNotional30s: zeroLookup,
    });
    for (const row of rows) {
      // burst_count = same_price + multilevel
      assert.equal(row.burst_count_1s, row.same_price_burst_count_1s + row.multilevel_burst_count_1s);
      // total = buy + sell
      assert.ok(Math.abs(row.total_burst_notional_1s - (row.buy_burst_notional_1s + row.sell_burst_notional_1s)) < 0.01);
      // imbalance in [-1, 1]
      assert.ok(row.burst_imbalance_ratio_1s >= -1.0 && row.burst_imbalance_ratio_1s <= 1.0);
      // share in [0, 1]
      assert.ok(row.largest_burst_share_notional_1s >= 0 && row.largest_burst_share_notional_1s <= 1.0);
      // book-dependent: #13=null, #14=0 per P1 contract
      assert.equal(row.burst_notional_vs_top_depth, null);
      assert.equal(row.burst_mid_move_bps_1s, 0);
      // P4: #15 = max burst_print_count among same-price bursts
      assert.ok(row.same_price_burst_max_len_1s >= (row.same_price_burst_count_1s > 0 ? 1 : 0),
        `#15 >= 1 when same-price bursts exist: ${row.same_price_burst_max_len_1s} / ${row.same_price_burst_count_1s}`);
      // #16 = same_price_notional >= 0
      assert.ok(row.same_price_burst_notional_1s >= 0);
      // #17 = multilevel span (0 when no multilevel)
      assert.ok(row.multilevel_burst_max_span_ticks_1s >= 0);
      // #18 = bps span (0 when no multilevel)
      assert.ok(row.multilevel_burst_max_span_bps_1s >= 0);
      // #19 = multilevel notional >= 0
      assert.ok(row.multilevel_burst_notional_1s >= 0);
      // #20 = absorption ratio in [0, 1]
      assert.ok(row.same_price_absorption_ratio_1s >= 0 && row.same_price_absorption_ratio_1s <= 1.0);
      // #21 = delta = buy - sell
      assert.equal(row.burst_delta_notional_1s, row.buy_burst_notional_1s - row.sell_burst_notional_1s);
      // #22 = outlier flag (0 or 1)
      assert.ok(row.outlier_trade_flag_1s === 0 || row.outlier_trade_flag_1s === 1);
    }
  });

  it('#12 — burst_notional_vs_30s computed with nonzero denominator', () => {
    const bd = new BurstDetector('test');
    bd.feedTrades([{ ts: 500, side: 'buy', price: 100, qty: 1 }]);
    bd.flushAll();

    const lookup = new Map(Array.from({length: 30}, (_, i) => [i * 1000, 10000]));
    const rows = computeFeatures1s({
      detector: bd, blockStartMs: 0, tradeTsList: [500], warmup: false, inputBlockIds: ['test'],
      lookupTradedNotional30s: lookup,
    });
    const row0 = rows[0];
    assert.equal(row0.total_burst_notional_1s, 100);
    assert.equal(row0.burst_notional_vs_30s_traded_notional, 100 / 10000);
  });

  it('#12 — zero denominator with valid aux input => output 0', () => {
    const bd = new BurstDetector('test');
    bd.feedTrades([{ ts: 500, side: 'buy', price: 100, qty: 1 }]);
    bd.flushAll();

    const lookup = new Map(Array.from({length: 30}, (_, i) => [i * 1000, 0]));
    const rows = computeFeatures1s({
      detector: bd, blockStartMs: 0, tradeTsList: [500], warmup: false, inputBlockIds: ['test'],
      lookupTradedNotional30s: lookup,
    });
    const row0 = rows[0];
    assert.equal(row0.burst_notional_vs_30s_traded_notional, 0);
  });

  it('#12 — missing lookupTradedNotional30s throws E007 (fail closed)', () => {
    const bd = new BurstDetector('test');
    bd.feedTrades([{ ts: 500, side: 'buy', price: 100, qty: 1 }]);
    bd.flushAll();
    // Intentionally omit lookupTradedNotional30s
    assert.throws(() => {
      computeFeatures1s({
        detector: bd, blockStartMs: 0, tradeTsList: [500], warmup: false, inputBlockIds: ['test'],
      });
    }, /E007/);
  });

  it('#12 — incomplete map throws E007 even with zero bursts', () => {
    const bd = new BurstDetector('test');
    bd.flushAll(); // no trades — all 30 rows will have zero bursts

    // Only 29 keys, missing the last second
    const incompleteLookup = new Map(Array.from({length: 29}, (_, i) => [i * 1000, 0]));
    assert.throws(() => {
      computeFeatures1s({
        detector: bd, blockStartMs: 0, tradeTsList: [], warmup: false, inputBlockIds: ['test'],
        lookupTradedNotional30s: incompleteLookup,
      });
    }, /E007/);
  });

  it('computes Phase 0 raw-trade flow and strict-past RV features', () => {
    const trades = [
      { ts: 500, side: 'buy', price: 100, qty: 1 },
      { ts: 700, side: 'sell', price: 101, qty: 2 },
      { ts: 900, side: 'buy', price: 100, qty: 3 },
    ];
    const bd = new BurstDetector('test');
    bd.feedTrades(trades);
    bd.flushAll();

    const rows = computeFeatures1s({
      detector: bd,
      blockStartMs: 0,
      tradeTsList: trades.map((trade) => trade.ts),
      tradeRecords: trades,
      tradeHistory: trades,
      warmup: false,
      inputBlockIds: ['p0'],
      lookupTradedNotional30s: zeroLookup,
      largeTradeNotionalThreshold: 200,
      largeTradeThresholdVersion: 'test-200-v1',
    });

    const row0 = rows[0];
    assert.equal(row0.trade_open_1s, 100);
    assert.equal(row0.trade_high_1s, 101);
    assert.equal(row0.trade_low_1s, 100);
    assert.equal(row0.trade_close_1s, 100);
    assert.equal(row0.trade_count_1s, 3);
    assert.equal(row0.buy_trade_count_1s, 2);
    assert.equal(row0.sell_trade_count_1s, 1);
    assert.equal(row0.traded_qty_1s, 6);
    assert.equal(row0.traded_notional_1s, 602);
    assert.equal(row0.signed_volume_1s, 2);
    assert.equal(row0.trade_imbalance_qty_1s, 1 / 3);
    assert.equal(row0.large_trade_count_1s, 2);
    assert.equal(row0.large_trade_notional_1s, 502);
    assert.equal(row0.large_trade_notional_share_1s, 502 / 602);
    assert.equal(row0.mean_interarrival_ms_1s, 200);
    assert.equal(row0.median_interarrival_ms_1s, 200);
    assert.equal(row0.p95_interarrival_ms_1s, 200);
    assert.equal(row0.side_flip_count_1s, 2);
    assert.equal(row0.realized_vol_10s, null);
    assert.equal(row0.realized_vol_60s, null);
    assert.equal(row0._quality.trade_feature_source, 'raw_trades');
    assert.equal(row0._quality.large_trade_threshold_version, 'test-200-v1');

    const row1 = rows[1];
    assert.ok(Number.isFinite(row1.realized_vol_10s));
    assert.ok(Number.isFinite(row1.realized_vol_60s));
    assert.equal(row1.trade_count_1s, 0);
    assert.equal(row1.trade_open_1s, null);
    assert.equal(row1.trade_high_1s, null);
    assert.equal(row1.trade_low_1s, null);
    assert.equal(row1.trade_close_1s, null);
    assert.equal(row1.traded_notional_1s, 0);
  });
});
