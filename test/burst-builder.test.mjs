import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import { BurstBuilder } from '../lib/burst-builder.mjs';

describe('BurstBuilder', () => {
  it('forms a single-print burst for a lone trade', () => {
    const bb = new BurstBuilder({ market: 'test' });
    bb.feedTrade({ ts: 1000, side: 'buy', price: 100, qty: 2 });
    bb.flushAll();

    const bursts = bb.getClosedBurstsOverlapping(1000);
    assert.equal(bursts.length, 1);
    const b = bursts[0];
    assert.equal(b.market, 'test');
    assert.equal(b.side, 'buy');
    assert.equal(b.burst_notional, 200);
    assert.equal(b.burst_print_count, 1);
    assert.equal(b.burst_duration_ms, 0);
    assert.equal(b.burst_start_ts, 1000);
    assert.equal(b.burst_end_ts, 1000);
    assert.equal(b.min_price, 100);
    assert.equal(b.max_price, 100);
    assert.equal(b.distinct_price_count, 1);
    assert.equal(b.span_ticks, 0);
    assert.equal(b.same_price_runs.length, 1);
    assert.equal(b.same_price_runs[0].same_price_print_count, 1);
  });

  it('merges two trades within gap threshold into one burst', () => {
    const bb = new BurstBuilder({ market: 'test', gap_threshold_ms: 5 });
    bb.feedTrade({ ts: 1000, side: 'buy', price: 100, qty: 1 });
    bb.feedTrade({ ts: 1003, side: 'buy', price: 101, qty: 2 });
    bb.flushAll();

    const bursts = bb.getClosedBurstsOverlapping(1000);
    assert.equal(bursts.length, 1);
    const b = bursts[0];
    assert.equal(b.burst_print_count, 2);
    assert.equal(b.burst_notional, 100 * 1 + 101 * 2);
    assert.equal(b.burst_duration_ms, 3);
    assert.equal(b.min_price, 100);
    assert.equal(b.max_price, 101);
    assert.equal(b.distinct_price_count, 2);
    assert.equal(b.span_ticks, Math.round(1 / 0.01)); // 100 ticks
    assert.equal(b.same_price_runs.length, 2); // two distinct prices
  });

  it('splits on gap threshold exceeded: separate bursts', () => {
    const bb = new BurstBuilder({ market: 'test', gap_threshold_ms: 5 });
    bb.feedTrade({ ts: 1000, side: 'buy', price: 100, qty: 1 });
    bb.feedTrade({ ts: 1010, side: 'buy', price: 101, qty: 2 }); // gap=10 > 5
    bb.flushAll();

    // Two closed bursts, both overlap [1000, 2000)
    const bursts0 = bb.getClosedBurstsOverlapping(1000);
    assert.equal(bursts0.length, 2);
    // first burst: 1 print
    assert.equal(bursts0[0].burst_print_count, 1);
    assert.equal(bursts0[0].burst_notional, 100);
    // second burst: 1 print
    assert.equal(bursts0[1].burst_print_count, 1);
    assert.equal(bursts0[1].burst_notional, 202);
  });

  it('splits on side change into separate bursts', () => {
    const bb = new BurstBuilder({ market: 'test', gap_threshold_ms: 10 });
    bb.feedTrade({ ts: 1000, side: 'buy', price: 100, qty: 1 });
    bb.feedTrade({ ts: 1002, side: 'sell', price: 101, qty: 2 });
    bb.flushAll();

    // Both bursts overlap [1000, 2000)
    const all = bb.getClosedBurstsOverlapping(1000);
    assert.equal(all.length, 2);
    // buy burst
    const buyBurst = all.find(b => b.side === 'buy');
    assert.ok(buyBurst);
    assert.equal(buyBurst.burst_print_count, 1);
    assert.equal(buyBurst.burst_notional, 100);
    // sell burst
    const sellBurst = all.find(b => b.side === 'sell');
    assert.ok(sellBurst);
    assert.equal(sellBurst.burst_print_count, 1);
    assert.equal(sellBurst.burst_notional, 202);
  });

  it('splits on max duration exceeded into separate bursts', () => {
    const bb = new BurstBuilder({ market: 'test', gap_threshold_ms: 100, max_burst_duration_ms: 10 });
    bb.feedTrade({ ts: 1000, side: 'buy', price: 100, qty: 1 });
    // gap=3 (within threshold) but candidate duration = 1015-1000 = 15 > 10
    bb.feedTrade({ ts: 1015, side: 'buy', price: 101, qty: 2 });
    bb.flushAll();

    // Two bursts, both overlap [1000, 2000)
    const all = bb.getClosedBurstsOverlapping(1000);
    assert.equal(all.length, 2);

    const first = all.find(b => b.burst_start_ts === 1000);
    assert.ok(first);
    assert.equal(first.burst_print_count, 1);

    const second = all.find(b => b.burst_start_ts === 1015);
    assert.ok(second);
    assert.equal(second.burst_print_count, 1);
  });

  it('overlap: burst crosses second boundary and appears in both buckets', () => {
    const bb = new BurstBuilder({ market: 'test', gap_threshold_ms: 1000, max_burst_duration_ms: 2000 });
    // Two trades 900ms apart within gap threshold — one burst crosses t=1000
    bb.feedTrade({ ts: 700, side: 'buy', price: 100, qty: 1 });
    bb.feedTrade({ ts: 1600, side: 'buy', price: 101, qty: 2 });
    bb.flushAll();

    // Bucket [0, 1000) — burst starts at 700 < 1000, ends at 1600 >= 0 → overlaps
    const b0 = bb.getClosedBurstsOverlapping(0);
    assert.equal(b0.length, 1);
    const bid = b0[0].burst_id;

    // Bucket [1000, 2000) — same burst overlaps
    const b1 = bb.getClosedBurstsOverlapping(1000);
    assert.equal(b1.length, 1);
    assert.equal(b1[0].burst_id, bid);
  });

  it('overlap: burst entirely before bucket is excluded', () => {
    const bb = new BurstBuilder({ market: 'test', gap_threshold_ms: 5 });
    bb.feedTrade({ ts: 100, side: 'buy', price: 100, qty: 1 });
    bb.feedTrade({ ts: 103, side: 'buy', price: 100, qty: 1 });
    bb.flushAll();

    // Bucket [1000, 2000): burst_end_ts=103 < 1000 → no overlap
    const b = bb.getClosedBurstsOverlapping(1000);
    assert.equal(b.length, 0);
  });

  it('overlap: burst entirely after bucket is excluded', () => {
    const bb = new BurstBuilder({ market: 'test', gap_threshold_ms: 5 });
    bb.feedTrade({ ts: 2100, side: 'buy', price: 100, qty: 1 });
    bb.feedTrade({ ts: 2103, side: 'buy', price: 100, qty: 1 });
    bb.flushAll();

    // Bucket [0, 1000): burst_start_ts=2100 >= 1000 → no overlap
    const b = bb.getClosedBurstsOverlapping(0);
    assert.equal(b.length, 0);
  });

  it('same-price sub-runs: detects contiguous equal-price sequences', () => {
    const bb = new BurstBuilder({ market: 'test', gap_threshold_ms: 5, max_burst_duration_ms: 1000 });
    // price pattern: 100, 100, 101, 101, 102
    bb.feedTrade({ ts: 1000, side: 'buy', price: 100, qty: 1 });
    bb.feedTrade({ ts: 1001, side: 'buy', price: 100, qty: 2 });
    bb.feedTrade({ ts: 1002, side: 'buy', price: 101, qty: 3 });
    bb.feedTrade({ ts: 1003, side: 'buy', price: 101, qty: 4 });
    bb.feedTrade({ ts: 1004, side: 'buy', price: 102, qty: 5 });
    bb.flushAll();

    const bursts = bb.getClosedBurstsOverlapping(1000);
    assert.equal(bursts.length, 1);
    const b = bursts[0];
    assert.equal(b.burst_print_count, 5);
    assert.equal(b.distinct_price_count, 3);

    // 3 same-price sub-runs
    assert.equal(b.same_price_runs.length, 3);
    assert.deepStrictEqual(b.same_price_runs.map(sp => sp.same_price_key), [100, 101, 102]);
    assert.deepStrictEqual(b.same_price_runs.map(sp => sp.same_price_print_count), [2, 2, 1]);
    assert.deepStrictEqual(b.same_price_runs.map(sp => sp.same_price_notional),
      [100 * 1 + 100 * 2, 101 * 3 + 101 * 4, 102 * 5]);
  });

  it('multilevel: burst with distinct_price_count >= 2 is multilevel', () => {
    const bb = new BurstBuilder({ market: 'test', gap_threshold_ms: 5, tick_size: 0.01 });
    bb.feedTrade({ ts: 1000, side: 'buy', price: 100.00, qty: 1 });
    bb.feedTrade({ ts: 1001, side: 'buy', price: 100.05, qty: 1 });
    bb.flushAll();

    const bursts = bb.getClosedBurstsOverlapping(1000);
    assert.equal(bursts.length, 1);
    const b = bursts[0];
    assert.equal(b.distinct_price_count, 2);
    // span_ticks = round((100.05 - 100.00) / 0.01) = 5
    assert.equal(b.span_ticks, 5);
  });

  it('multilevel: single-price burst is NOT multilevel', () => {
    const bb = new BurstBuilder({ market: 'test', gap_threshold_ms: 5 });
    bb.feedTrade({ ts: 1000, side: 'buy', price: 100, qty: 1 });
    bb.feedTrade({ ts: 1001, side: 'buy', price: 100, qty: 1 });
    bb.flushAll();

    const bursts = bb.getClosedBurstsOverlapping(1000);
    assert.equal(bursts.length, 1);
    assert.equal(bursts[0].distinct_price_count, 1);
    assert.equal(bursts[0].span_ticks, 0);
  });

  it('getOpenBursts returns the current open burst', () => {
    const bb = new BurstBuilder({ market: 'test' });
    bb.feedTrade({ ts: 1000, side: 'buy', price: 100, qty: 1 });

    const open = bb.getOpenBursts();
    assert.equal(open.length, 1);
    assert.equal(open[0].side, 'buy');
    assert.equal(open[0].burst_print_count, 1);
    assert.equal(open[0].burst_notional, 100);

    bb.feedTrade({ ts: 1002, side: 'buy', price: 100, qty: 1 });
    const open2 = bb.getOpenBursts();
    assert.equal(open2.length, 1);
    assert.equal(open2[0].burst_print_count, 2);

    bb.flushAll();
    assert.equal(bb.getOpenBursts().length, 0);
  });

  it('flushAll closes open burst and is idempotent', () => {
    const bb = new BurstBuilder({ market: 'test' });
    bb.feedTrade({ ts: 1000, side: 'buy', price: 100, qty: 1 });
    bb.flushAll();
    bb.flushAll(); // idempotent

    const bursts = bb.getClosedBurstsOverlapping(1000);
    assert.equal(bursts.length, 1);
    assert.equal(bb.getOpenBursts().length, 0);
  });

  it('per-market isolation: separate builders do not interfere', () => {
    const bbA = new BurstBuilder({ market: 'binance' });
    const bbB = new BurstBuilder({ market: 'coinbase' });

    bbA.feedTrade({ ts: 1000, side: 'buy', price: 100, qty: 1 });
    bbB.feedTrade({ ts: 1000, side: 'sell', price: 200, qty: 2 });

    bbA.flushAll();
    bbB.flushAll();

    const a = bbA.getClosedBurstsOverlapping(1000);
    assert.equal(a.length, 1);
    assert.equal(a[0].market, 'binance');
    assert.equal(a[0].side, 'buy');

    const b = bbB.getClosedBurstsOverlapping(1000);
    assert.equal(b.length, 1);
    assert.equal(b[0].market, 'coinbase');
    assert.equal(b[0].side, 'sell');
  });

  it('prints array preserves full trade detail', () => {
    const bb = new BurstBuilder({ market: 'test', gap_threshold_ms: 5 });
    bb.feedTrade({ ts: 1000, side: 'buy', price: 100, qty: 1.5 });
    bb.feedTrade({ ts: 1002, side: 'buy', price: 101, qty: 2.5 });
    bb.flushAll();

    const bursts = bb.getClosedBurstsOverlapping(1000);
    assert.equal(bursts.length, 1);
    const b = bursts[0];
    assert.equal(b.prints.length, 2);
    assert.deepStrictEqual(b.prints[0], { ts: 1000, side: 'buy', price: 100, qty: 1.5 });
    assert.deepStrictEqual(b.prints[1], { ts: 1002, side: 'buy', price: 101, qty: 2.5 });
  });

  it('no-op with zero trades returns empty arrays', () => {
    const bb = new BurstBuilder({ market: 'test' });
    assert.equal(bb.getClosedBurstsOverlapping(0).length, 0);
    assert.equal(bb.getOpenBursts().length, 0);
    bb.flushAll();
    assert.equal(bb.getClosedBurstsOverlapping(0).length, 0);
  });
});
