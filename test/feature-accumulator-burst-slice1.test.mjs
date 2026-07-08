import { describe, it } from 'node:test';
import assert from 'node:assert';
import { FeatureAccumulator } from '../lib/feature-accumulator.mjs';

class StubBook {
  constructor(ts) {
    this._ts = ts;
    this.bids = new Map();
    this.asks = new Map();
  }
  getMid() { return null; }
  getBestBid() { return null; }
  getBestAsk() { return null; }
  isEmpty() { return true; }
}

describe('FeatureAccumulator burst slice 1', () => {
  it('emits overlap-based burst fields for same-price and multilevel bursts', () => {
    const acc = new FeatureAccumulator('/tmp/burst-slice1-test', {
      burstGapThresholdMs: 100,
      burstMaxDurationMs: 1000,
      burstTickSizeByMarket: { test_market: 1 },
    });

    // Burst A: same-side short-gap, same-price sub-run then multilevel extension, crosses second boundary.
    acc.feedTrade('test_market', { ts: 950, price: 100, qty: 1, side: 'buy' });
    acc.feedTrade('test_market', { ts: 980, price: 100, qty: 2, side: 'buy' });
    acc.feedTrade('test_market', { ts: 1010, price: 101, qty: 1, side: 'buy' });

    // Side change splits burst.
    acc.feedTrade('test_market', { ts: 1080, price: 99, qty: 1, side: 'sell' });

    const book0 = new StubBook(999);
    const book1 = new StubBook(1999);
    acc.feedSecond('test_market', 0, book0);
    acc.feedSecond('test_market', 1000, book1);

    const rows = acc._buffers.get('test_market');
    assert.ok(rows, 'expected buffered rows for test_market');

    const row0 = rows.get(0);
    const row1 = rows.get(1000);
    assert.ok(row0, 'expected second-0 row');
    assert.ok(row1, 'expected second-1000 row');

    // Second 0: only Burst A overlaps.
    assert.strictEqual(row0.burst_count_1s, 1);
    assert.strictEqual(row0.max_burst_notional_1s, 401);
    assert.strictEqual(row0.max_burst_prints_1s, 3);
    assert.strictEqual(row0.max_burst_duration_ms_1s, 60);
    assert.strictEqual(row0.same_price_burst_count_1s, 1);
    assert.strictEqual(row0.same_price_burst_max_len_1s, 2);
    assert.strictEqual(row0.same_price_burst_notional_1s, 300);
    assert.strictEqual(row0.multilevel_burst_count_1s, 1);
    assert.strictEqual(row0.multilevel_burst_max_span_ticks_1s, 1);
    assert.strictEqual(row0.multilevel_burst_notional_1s, 401);
    assert.strictEqual(row0.buy_burst_notional_1s, 401);
    assert.strictEqual(row0.sell_burst_notional_1s, 0);
    assert.strictEqual(row0.burst_delta_notional_1s, 401);
    assert.strictEqual(row0.largest_burst_share_notional_1s, 1);

    // Second 1: Burst A overlaps and Burst B starts in this second.
    assert.strictEqual(row1.burst_count_1s, 2);
    assert.strictEqual(row1.max_burst_notional_1s, 401);
    assert.strictEqual(row1.max_burst_prints_1s, 3);
    assert.strictEqual(row1.max_burst_duration_ms_1s, 60);
    assert.strictEqual(row1.same_price_burst_count_1s, 2);
    assert.strictEqual(row1.same_price_burst_max_len_1s, 1);
    assert.strictEqual(row1.same_price_burst_notional_1s, 200);
    assert.strictEqual(row1.multilevel_burst_count_1s, 1);
    assert.strictEqual(row1.multilevel_burst_max_span_ticks_1s, 1);
    assert.strictEqual(row1.multilevel_burst_notional_1s, 401);
    assert.strictEqual(row1.buy_burst_notional_1s, 401);
    assert.strictEqual(row1.sell_burst_notional_1s, 99);
    assert.strictEqual(row1.burst_delta_notional_1s, 302);
    assert.strictEqual(row1.largest_burst_share_notional_1s, 401 / 500);
  });

  it('emits bucket-local run, flip, and same-side gap fields without leaking across seconds', () => {
    const acc = new FeatureAccumulator('/tmp/burst-slice2-test');

    // second 0: buy,buy,sell,sell,buy => runs [2,2,1], flips=2, same-side gaps=[20,20]
    acc.feedTrade('test_market', { ts: 100, price: 100, qty: 1, side: 'buy' });
    acc.feedTrade('test_market', { ts: 120, price: 100, qty: 1, side: 'buy' });
    acc.feedTrade('test_market', { ts: 140, price: 100, qty: 1, side: 'sell' });
    acc.feedTrade('test_market', { ts: 160, price: 100, qty: 1, side: 'sell' });
    acc.feedTrade('test_market', { ts: 190, price: 100, qty: 1, side: 'buy' });

    // cross-second continuation should NOT leak back into second 0
    acc.feedTrade('test_market', { ts: 1010, price: 101, qty: 1, side: 'buy' });

    const book0 = new StubBook(999);
    const book1 = new StubBook(1999);
    acc.feedSecond('test_market', 0, book0);
    acc.feedSecond('test_market', 1000, book1);

    const rows = acc._buffers.get('test_market');
    const row0 = rows.get(0);
    const row1 = rows.get(1000);

    assert.strictEqual(row0.max_same_side_run_prints_1s, 2);
    assert.strictEqual(row0.side_flip_count_1s, 2);
    assert.strictEqual(row0.same_side_gap_ms_min_1s, 20);
    assert.strictEqual(row0.same_side_gap_ms_p25_1s, 20);

    assert.strictEqual(row1.max_same_side_run_prints_1s, 1);
    assert.strictEqual(row1.side_flip_count_1s, 0);
    assert.strictEqual(row1.same_side_gap_ms_min_1s, null);
    assert.strictEqual(row1.same_side_gap_ms_p25_1s, null);
  });

  it('emits burst book-validation ratios and co-occurrence counts with null handling', () => {
    const acc = new FeatureAccumulator('/tmp/burst-slice3-test');

    // Establish best bid/ask for second 0.
    acc.feedDepth('test_market', { ts: 10, bids: [[99, 1]], asks: [[101, 1], [102, 1]] }, 100);
    // At-touch buy at best ask.
    acc.feedTrade('test_market', { ts: 100, price: 101, qty: 1, side: 'buy' });
    // Through buy beyond best ask per contract semantics.
    acc.feedTrade('test_market', { ts: 120, price: 102, qty: 2, side: 'buy' });
    // Deplete ask 101 so best ask moves to 102.
    acc.feedDepth('test_market', { ts: 150, asks: [[101, 0]] }, 100.5);
    // Remove the remaining ask so depleted state is marked.
    acc.feedDepth('test_market', { ts: 160, asks: [[102, 0]] }, 99.5);
    // Replenish ask 101.
    acc.feedDepth('test_market', { ts: 180, asks: [[101, 1]] }, 100);

    // Second 1: burst-associated trade exists but no usable book state.
    acc.feedTrade('other_market', { ts: 1050, price: 200, qty: 1, side: 'buy' });

    const book0 = new StubBook(999);
    const book1 = new StubBook(1999);
    acc.feedSecond('test_market', 0, book0);
    acc.feedSecond('other_market', 1000, book1);

    const rowsA = acc._buffers.get('test_market');
    const row0 = rowsA.get(0);
    const rowsB = acc._buffers.get('other_market');
    const row1 = rowsB.get(1000);

    assert.strictEqual(row0.burst_at_touch_ratio_1s, 101 / 305);
    assert.strictEqual(row0.burst_through_ratio_1s, 204 / 305);
    assert.strictEqual(row0.burst_depletion_count_1s, 1);
    assert.strictEqual(row0.burst_replenish_after_touch_count_1s, 1);

    assert.strictEqual(row1.burst_at_touch_ratio_1s, null);
    assert.strictEqual(row1.burst_through_ratio_1s, null);
    assert.strictEqual(row1.burst_depletion_count_1s, null);
    assert.strictEqual(row1.burst_replenish_after_touch_count_1s, null);
  });

  it('keeps slice-3 classification side-aware and supports string-priced depth fixtures', () => {
    const acc = new FeatureAccumulator('/tmp/burst-slice3-sideaware-test');

    // Book uses string prices/qtys, like real exchange payloads.
    acc.feedDepth('test_market', { ts: 10, bids: [['99', '1']], asks: [['101', '1'], ['102', '1']] }, 100);
    // Wrong-side price alignment should NOT count as at-touch for a buy.
    acc.feedTrade('test_market', { ts: 100, price: 99, qty: 1, side: 'buy' });
    // Proper at-touch buy.
    acc.feedTrade('test_market', { ts: 120, price: 101, qty: 1, side: 'buy' });
    // Proper through buy.
    acc.feedTrade('test_market', { ts: 140, price: 102, qty: 1, side: 'buy' });
    // Force depletion then replenish using string keys.
    acc.feedDepth('test_market', { ts: 150, asks: [['101', '0']] }, 100.5);
    acc.feedDepth('test_market', { ts: 160, asks: [['102', '0']] }, 99.5);
    acc.feedDepth('test_market', { ts: 180, asks: [['101', '1']] }, 100);

    acc.feedSecond('test_market', 0, new StubBook(999));
    const row = acc._buffers.get('test_market').get(0);

    // Denominator is all side-relevantly classifiable buy prints with best ask present.
    assert.strictEqual(row.burst_at_touch_ratio_1s, 101 / 302);
    assert.strictEqual(row.burst_through_ratio_1s, 102 / 302);
    assert.strictEqual(row.burst_depletion_count_1s, 1);
    assert.strictEqual(row.burst_replenish_after_touch_count_1s, 1);
  });
});
