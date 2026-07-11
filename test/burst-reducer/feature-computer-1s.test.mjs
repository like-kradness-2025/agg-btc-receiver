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
      // research: #15-#22 = 0 per P1 contract
      assert.equal(row.same_price_burst_max_len_1s, 0);
      assert.equal(row.outlier_trade_flag_1s, 0);
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
});
