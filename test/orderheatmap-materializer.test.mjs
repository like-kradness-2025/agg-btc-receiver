import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { materializeOrderHeatmapRow } from '../lib/orderheatmap-materializer.mjs';

describe('market OrderHeatmap materializer', () => {
  it('aggregates strict snapshot levels into absolute $1 BTC buckets', () => {
    const row = materializeOrderHeatmapRow({
      schema_version: 'book_snapshot_1s_v2', market: 'okx_perp', ts: 1000,
      finalized: true, seeded: true, gap: false, crossed: false, stale: false,
      book_status: 'seeded', best_bid: 50000, best_ask: 50010, mid: 50005,
      bid_prices: [50000.4, 50000.9], bid_qtys: [0.1, 0.2],
      ask_prices: [50010.2], ask_qtys: [0.3],
    });
    assert.deepEqual(row.bid_prices, [50000]);
    assert.deepEqual(row.bid_qtys_btc, [0.3]);
    assert.deepEqual(row.ask_prices, [50010]);
    assert.deepEqual(row.ask_qtys_btc, [0.3]);
    assert.equal(row.coverage_ratio, 1);
  });

  it('keeps an invalid second as an explicit gap instead of zero liquidity', () => {
    const row = materializeOrderHeatmapRow({
      market: 'binance_spot', ts: 2000, finalized: false, seeded: false,
      gap: true, crossed: false, stale: false, book_status: 'quarantine',
      bid_prices: [50000], bid_qtys: [2], ask_prices: [50010], ask_qtys: [2],
    });
    assert.equal(row.finalized, false);
    assert.equal(row.gap, true);
    assert.deepEqual(row.bid_prices, []);
    assert.equal(row.coverage_ratio, 0);
  });

  it('limits displayed depth to $10,000 from that second\'s mid price', () => {
    const row = materializeOrderHeatmapRow({
      market: 'binance_perp', ts: 3000, finalized: true, seeded: true,
      gap: false, crossed: false, stale: false, book_status: 'seeded',
      best_bid: 50000, best_ask: 50010, mid: 50005,
      bid_prices: [40005, 40004, 50000], bid_qtys: [1, 2, 3],
      ask_prices: [60005, 60006, 50010], ask_qtys: [4, 5, 6],
    });
    assert.deepEqual(row.bid_prices, [40005, 50000]);
    assert.deepEqual(row.bid_qtys_btc, [1, 3]);
    assert.deepEqual(row.ask_prices, [50010, 60005]);
    assert.deepEqual(row.ask_qtys_btc, [6, 4]);
    assert.equal(row.depth_limit_usd, 10000);
  });

  it('keeps rounded bucket prices inside the displayed depth limit', () => {
    const row = materializeOrderHeatmapRow({
      market: 'binance_perp', ts: 4000, finalized: true, seeded: true,
      gap: false, crossed: false, stale: false, book_status: 'seeded',
      best_bid: 65300, best_ask: 65320, mid: 65310.05,
      bid_prices: [55310.9, 55310.0], bid_qtys: [1, 2],
      ask_prices: [75310.0], ask_qtys: [3],
    });
    assert.deepEqual(row.bid_prices, []);
    assert.deepEqual(row.bid_qtys_btc, []);
    assert.deepEqual(row.ask_prices, [75310]);
  });
});
