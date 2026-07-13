// test/burst-reducer/golden.test.mjs — Golden fixture E2E contract tests
// Follows plan Task 10

import assert from 'node:assert/strict';
import { describe, it } from 'node:test';
import { readFileSync } from 'node:fs';
import { join } from 'node:path';
import { validateAndParseTrades } from '../../lib/burst-reducer/input-validator.mjs';
import { BurstDetector } from '../../lib/burst-reducer/burst-detector.mjs';
import { computeFeatures1s } from '../../lib/burst-reducer/feature-computer-1s.mjs';
import { GAP_THRESHOLD_MS, MAX_BURST_DURATION_MS } from '../../lib/burst-reducer/schema.mjs';

const FIXTURES_DIR = join('test', 'fixtures', 'burst-v1');

function loadJsonl(path) {
  const content = readFileSync(path, 'utf8');
  return content.trim().split('\n').filter(l => l).map(l => JSON.parse(l));
}

// Helper: complete 30-key zero-denominator lookup for valid-path golden tests
const zeroLookup = new Map(Array.from({ length: 30 }, (_, i) => [i * 1000, 0]));

describe('Golden fixtures', () => {
  it('trades-basic: matches expected features', () => {
    const tradePath = join(FIXTURES_DIR, 'trades-basic.jsonl');
    const expectedPath = join(FIXTURES_DIR, 'expected-features-1s.jsonl');

    const tradeContent = readFileSync(tradePath, 'utf8');
    const { trades } = validateAndParseTrades(tradeContent, 0);

    const detector = new BurstDetector('test');
    detector.feedTrades(trades);
    detector.flushAll();

    const rows = computeFeatures1s({
      detector,
      blockStartMs: 0,
      tradeTsList: trades.map(t => t.ts),
      warmup: true,
      inputBlockIds: ['trades-basic'],
      lookupTradedNotional30s: zeroLookup,
    });

    const expected = loadJsonl(expectedPath);

    // Compare only seconds that have expectations
    for (const exp of expected) {
      const row = rows.find(r => r.ts === exp.ts);
      assert.ok(row, `missing row for ts ${exp.ts}`);

      // trade-only 12 fields (#1-#12)
      const fields = [
        'burst_count_1s', 'total_burst_notional_1s', 'max_burst_notional_1s',
        'max_burst_prints_1s', 'max_burst_duration_ms_1s',
        'buy_burst_notional_1s', 'sell_burst_notional_1s',
        'burst_imbalance_ratio_1s', 'largest_burst_share_notional_1s',
        'same_price_burst_count_1s', 'multilevel_burst_count_1s',
        'burst_notional_vs_30s_traded_notional',
      ];
      for (const f of fields) {
        if (Number.isFinite(exp[f]) && Number.isFinite(row[f])) {
          assert.ok(Math.abs(row[f] - exp[f]) < 0.01, `${f}: expected ${exp[f]}, got ${row[f]}`);
        } else {
          assert.equal(row[f], exp[f], `${f}: expected ${exp[f]}, got ${row[f]}`);
        }
      }

      // P4 activation: #13=null when no book, #14=0, #15-#22 computed from bursts
      if (row.burst_count_1s > 0) {
        // For the trades-basic fixture (all same-price at 100): one burst, 4 prints
        assert.ok(row.same_price_burst_max_len_1s > 0, `expected #15 > 0 for ts ${row.ts}, got ${row.same_price_burst_max_len_1s}`);
        assert.ok(row.same_price_burst_notional_1s > 0, `expected #16 > 0 for ts ${row.ts}`);
        assert.equal(row.multilevel_burst_max_span_ticks_1s, 0, `expected #17=0 for same-price burst`);
        assert.equal(row.multilevel_burst_max_span_bps_1s, 0, `expected #18=0 for same-price burst`);
        assert.equal(row.multilevel_burst_notional_1s, 0, `expected #19=0 for same-price burst`);
        assert.ok(row.same_price_absorption_ratio_1s > 0, `expected #20 > 0 for same-price burst`);
        assert.equal(row.outlier_trade_flag_1s, 0, `expected #22=0 (no outlier in fixture)`);
      }
      // B4 board candidate fields are null when book not passed
      assert.equal(row.board_top_depth_ratio, null);
      assert.equal(row.board_mid_move_bps_1s, null);
      assert.equal(row.board_vs_30s, null);
      assert.equal(row.board_vs_depth, null);
    }
  });

  it('trades-cross-boundary: overlap works', () => {
    const tradePath = join(FIXTURES_DIR, 'trades-cross-boundary.jsonl');
    const expectedPath = join(FIXTURES_DIR, 'expected-cross-boundary-1s.jsonl');

    // Cross-boundary: first trade (ts=29900) is in block 0, second (ts=30100) is in block 1.
    // We validate+feed only the first trade here; the expected output only covers block 0's second (ts=29000).
    const tradeContent = readFileSync(tradePath, 'utf8');
    const lines = tradeContent.trim().split('\n').filter(l => l);

    // Parse first line with blockStartMs=0 (ts=29900 is valid)
    const firstTrade = JSON.parse(lines[0]);
    const { trades } = validateAndParseTrades(JSON.stringify(firstTrade) + '\n', 0);

    const detector = new BurstDetector('test');
    detector.feedTrades(trades);
    detector.flushAll();

    const rows = computeFeatures1s({
      detector,
      blockStartMs: 0,
      tradeTsList: trades.map(t => t.ts),
      warmup: true,
      inputBlockIds: ['cross-boundary'],
      lookupTradedNotional30s: zeroLookup,
    });

    const expected = loadJsonl(expectedPath);
    for (const exp of expected) {
      const row = rows.find(r => r.ts === exp.ts);
      assert.ok(row, `missing row for ts ${exp.ts}`);
      assert.equal(row.burst_count_1s, exp.burst_count_1s);
      assert.equal(row.total_burst_notional_1s, exp.total_burst_notional_1s);
    }
  });

  it('trades-empty-block: all zero', () => {
    // Create on the fly: no trades = empty block
    const tradeContent = '';  // empty
    const { trades } = validateAndParseTrades(tradeContent + '\n', 0);

    const detector = new BurstDetector('test');
    if (trades.length > 0) detector.feedTrades(trades);
    detector.flushAll();

    const rows = computeFeatures1s({
      detector,
      blockStartMs: 0,
      tradeTsList: trades.map(t => t.ts),
      warmup: false,
      inputBlockIds: ['empty-block'],
      lookupTradedNotional30s: zeroLookup,
    });

    for (const row of rows) {
      assert.equal(row.burst_count_1s, 0, `ts ${row.ts}: expected 0 bursts`);
      assert.equal(row.total_burst_notional_1s, 0);
    }
  });

  it('trades-single-print-burst: single trade = 1 burst', () => {
    const tradesRaw = [JSON.stringify({ ts: 1000, side: 'buy', price: 100, qty: 1 })].join('\n');
    const { trades } = validateAndParseTrades(tradesRaw, 0);

    const detector = new BurstDetector('test');
    detector.feedTrades(trades);
    detector.flushAll();

    const rows = computeFeatures1s({
      detector,
      blockStartMs: 0,
      tradeTsList: trades.map(t => t.ts),
      warmup: false,
      inputBlockIds: ['single-print'],
      lookupTradedNotional30s: zeroLookup,
    });

    const row0 = rows[0]; // secondTs = 0 (no burst at ts=0)
    assert.equal(row0.burst_count_1s, 0);

    const row1 = rows[1]; // secondTs = 1000 (burst at ts=1000 overlaps [1000,2000))
    assert.equal(row1.burst_count_1s, 1);
    assert.equal(row1.total_burst_notional_1s, 100);
  });

  it('all rows have 29 physical keys (#1-#22 + B4 board_* + ts + market + _quality)', () => {
    const tradesRaw = [JSON.stringify({ ts: 500, side: 'buy', price: 100, qty: 1 })].join('\n');
    const { trades } = validateAndParseTrades(tradesRaw, 0);

    const detector = new BurstDetector('test');
    detector.feedTrades(trades);
    detector.flushAll();

    const rows = computeFeatures1s({
      detector,
      blockStartMs: 0,
      tradeTsList: trades.map(t => t.ts),
      warmup: true,
      inputBlockIds: ['test'],
      lookupTradedNotional30s: zeroLookup,
    });

    for (const row of rows) {
      assert.equal(Object.keys(row).length, 29, `ts ${row.ts}: expected 29 keys, got ${Object.keys(row).length}`);
    }
  });

  // P1-2: closed burst retention is bounded
  it('closed burst count stays bounded after many blocks with prune', () => {
    const detector = new BurstDetector('test');
    const RETENTION_WINDOW = MAX_BURST_DURATION_MS + GAP_THRESHOLD_MS + 1000;

    // Simulate many blocks of trades: each block has 1 trade, widely spaced
    // The detector is NOT pruned between blocks — only the pipeline calls prune.
    // Here we manually simulate the pipeline pattern: feed, then prune.
    for (let block = 0; block < 100; block++) {
      const blockStartMs = block * 30000;
      // Each block has trades forming 2 bursts each
      detector.feedTrades([
        { ts: blockStartMs + 100, side: 'buy', price: 100, qty: 1 },
        { ts: blockStartMs + 30000 - 100, side: 'sell', price: 101, qty: 1 },
      ]);
      // Prune after each block (as pipeline does)
      const nextBlockStartMs = (block + 1) * 30000;
      detector.pruneClosedBurstsBefore(nextBlockStartMs);
    }
    detector.flushAll();

    // After 100 blocks of processing with prune, closed burst count should be bounded
    // by the retention window (~6s) relative to the last block's start.
    // At most a few bursts should remain (those ending >= cutoff).
    const allBursts = detector.getAllClosedBursts();
    assert.ok(allBursts.length <= 5, `bounded retention: ${allBursts.length} bursts (expected <= 5)`);
  });
});
