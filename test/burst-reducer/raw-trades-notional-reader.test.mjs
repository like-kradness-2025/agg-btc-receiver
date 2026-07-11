// test/burst-reducer/raw-trades-notional-reader.test.mjs — TDD for raw trades notional reader
import assert from 'node:assert/strict';
import { describe, it, before, after } from 'node:test';
import { existsSync, mkdirSync, writeFileSync, rmSync } from 'node:fs';
import { join } from 'node:path';
import {
  buildRawTradedNotionalLookup,
  validateRawTradeLookback,
} from '../../lib/burst-reducer/raw-trades-notional-reader.mjs';

const TEST_DIR = join('test', 'fixtures', 'burst-v1', 'tmp-raw-reader');
const MARKET = 'test_raw_reader';

function makeTradeBlock(trades) {
  return trades.map(t => JSON.stringify(t)).join('\n') + '\n';
}

function tradeBlockPath(market, dateStr, timeStr) {
  return join(TEST_DIR, 'trades', market, dateStr, `${timeStr}.jsonl`);
}

describe('raw-trades-notional-reader', () => {
  before(() => {
    rmSync(TEST_DIR, { recursive: true, force: true });
    mkdirSync(join(TEST_DIR, 'trades', MARKET, '1970-01-01'), { recursive: true });
    mkdirSync(join(TEST_DIR, 'trades', MARKET, '1969-12-31'), { recursive: true });
  });

  after(() => {
    rmSync(TEST_DIR, { recursive: true, force: true });
  });

  // ── Boundary: inclusive start, exclusive end ──
  describe('boundary trades', () => {
    before(() => {
      // Lookback block [0, 30000): trade at ts=0 (inclusive)
      writeFileSync(tradeBlockPath(MARKET, '1970-01-01', '00-00-00'),
        makeTradeBlock([
          { ts: 0, side: 'buy', price: 100, qty: 1 },    // at boundary start
          { ts: 10000, side: 'sell', price: 200, qty: 2 }, // mid-block
          { ts: 29999, side: 'buy', price: 300, qty: 3 }, // just before block end
        ]));

      // Target block [30000, 60000): trade at ts=30000 (inclusive)
      writeFileSync(tradeBlockPath(MARKET, '1970-01-01', '00-00-30'),
        makeTradeBlock([
          { ts: 30000, side: 'sell', price: 400, qty: 1 }, // at target start
          { ts: 40000, side: 'buy', price: 500, qty: 4 },
          { ts: 59999, side: 'sell', price: 600, qty: 2 }, // just before target end
        ]));
    });

    after(() => {
      try { rmSync(tradeBlockPath(MARKET, '1970-01-01', '00-00-00'), { force: true }); } catch (_) {}
      try { rmSync(tradeBlockPath(MARKET, '1970-01-01', '00-00-30'), { force: true }); } catch (_) {}
    });

    it('includes trade at ts=windowStart (inclusive)', () => {
      // For s=30000, window is [0, 30000). Trade at ts=0 should be included.
      const lookup = buildRawTradedNotionalLookup(TEST_DIR, MARKET, 30000);
      // s=30000: [0, 30000) includes ts=0(100), ts=10000(400), ts=29999(900) = 1400
      assert.equal(lookup.get(30000), 100 * 1 + 200 * 2 + 300 * 3); // 100 + 400 + 900 = 1400
    });

    it('excludes trade at ts=windowEnd (exclusive)', () => {
      // For s=30000, window is [0, 30000). Trade at ts=30000 should NOT be included.
      const lookup = buildRawTradedNotionalLookup(TEST_DIR, MARKET, 30000);
      // ts=30000 is in target block, NOT in [0, 30000)
      const val30000 = lookup.get(30000);
      assert.equal(val30000, 100 * 1 + 200 * 2 + 300 * 3); // only [0,30000) block trades
    });

    it('includes trade at ts=targetStart (inclusive) in later second window', () => {
      // For s=31000, window is [1000, 31000). Trade at ts=30000 should be included.
      const lookup = buildRawTradedNotionalLookup(TEST_DIR, MARKET, 30000);
      // s=31000: [1000, 31000) includes ts=10000(400), ts=29999(900), ts=30000(400) = 1700
      const expected = 200 * 2 + 300 * 3 + 400 * 1;
      assert.equal(lookup.get(31000), expected);
    });

    it('excludes trade at ts=targetEnd (exclusive)', () => {
      // Keys are 30000, 31000, ..., 59000 (30 keys). s=59000 is the last key.
      // For s=59000, window is [29000, 59000).
      // Includes: ts=29999(900), ts=30000(400), ts=40000(2000), ts=59999 is excluded (>= 59000)
      // 900 + 400 + 2000 = 3300
      const lookup = buildRawTradedNotionalLookup(TEST_DIR, MARKET, 30000);
      assert.equal(lookup.get(59000), 300 * 3 + 400 * 1 + 500 * 4); // ts=59999 excluded (not in window)
    });
  });

  // ── Cross-midnight (date boundary) ──
  describe('cross-midnight lookback', () => {
    before(() => {
      mkdirSync(join(TEST_DIR, 'trades', MARKET, '1969-12-31'), { recursive: true });
      // Block at -30000 (1969-12-31 23:59:30 UTC)
      writeFileSync(tradeBlockPath(MARKET, '1969-12-31', '23-59-30'),
        makeTradeBlock([
          { ts: -29000, side: 'buy', price: 50, qty: 10 }, // notional: 500
          { ts: -1000, side: 'sell', price: 60, qty: 2 },   // notional: 120
        ]));

      // Block at 0 (1970-01-01 00:00:00 UTC)
      writeFileSync(tradeBlockPath(MARKET, '1970-01-01', '00-00-00'),
        makeTradeBlock([
          { ts: 500, side: 'buy', price: 100, qty: 1 },     // notional: 100
          { ts: 1000, side: 'sell', price: 200, qty: 3 },    // notional: 600
        ]));
    });

    after(() => {
      try { rmSync(tradeBlockPath(MARKET, '1969-12-31', '23-59-30'), { force: true }); } catch (_) {}
      try { rmSync(tradeBlockPath(MARKET, '1970-01-01', '00-00-00'), { force: true }); } catch (_) {}
    });

    it('correctly reads lookback block from previous date', () => {
      const lookup = buildRawTradedNotionalLookup(TEST_DIR, MARKET, 0);
      assert.equal(lookup.size, 30, 'should have 30 keys');

      // s=0: window [-30000, 0) — only lookback block trades
      const val0 = lookup.get(0);
      assert.equal(val0, 50 * 10 + 60 * 2); // 500 + 120 = 620

      // s=1000: window [-29000, 1000) — includes ts=-29000(500), ts=-1000(120), ts=500(100)
      const val1000 = lookup.get(1000);
      assert.equal(val1000, 50 * 10 + 60 * 2 + 100 * 1); // 500 + 120 + 100 = 720

      // s=2000: window [-28000, 2000) — ts=-29000 excluded, 3 trades: ts=-1000(120), ts=500(100), ts=1000(600)
      const val2000 = lookup.get(2000);
      assert.equal(val2000, 120 + 100 + 600); // 820
    });

    it('validateRawTradeLookback returns coverageComplete=true', () => {
      const result = validateRawTradeLookback(TEST_DIR, MARKET, 0);
      assert.equal(result.coverageComplete, true);
      assert.deepEqual(result.missing, []);
      assert.ok(result.hashes['-30000'], 'should have hash for lookback block');
      assert.ok(result.hashes['0'], 'should have hash for target block');
      assert.equal(result.trades.length, 4);
    });
  });

  // ── 2-block lookback coverage ──
  describe('2-block lookback', () => {
    before(() => {
      writeFileSync(tradeBlockPath(MARKET, '1970-01-01', '00-00-00'),
        makeTradeBlock([
          { ts: 10000, side: 'buy', price: 100, qty: 1 },
        ]));

      writeFileSync(tradeBlockPath(MARKET, '1970-01-01', '00-00-30'),
        makeTradeBlock([
          { ts: 35000, side: 'sell', price: 200, qty: 2 },
        ]));
    });

    after(() => {
      try { rmSync(tradeBlockPath(MARKET, '1970-01-01', '00-00-00'), { force: true }); } catch (_) {}
      try { rmSync(tradeBlockPath(MARKET, '1970-01-01', '00-00-30'), { force: true }); } catch (_) {}
    });

    it('reads both lookback and target block for targetBlockStartMs=30000', () => {
      const lookup = buildRawTradedNotionalLookup(TEST_DIR, MARKET, 30000);
      // The window[s-30000, s) needs trades from both blocks
      // s=35000: [5000, 35000) includes ts=10000(100) from block0, NOT ts=35000 from block1
      const val35000 = lookup.get(35000);
      assert.equal(val35000, 100 * 1); // only ts=10000

      // s=36000: [6000, 36000) includes ts=10000(100) + ts=35000(400) = 500
      const val36000 = lookup.get(36000);
      assert.equal(val36000, 100 * 1 + 200 * 2); // 500
    });

    it('validateRawTradeLookback checks both blocks exist', () => {
      const result = validateRawTradeLookback(TEST_DIR, MARKET, 30000);
      assert.equal(result.coverageComplete, true);
      assert.equal(result.missing.length, 0);
      assert.equal(result.trades.length, 2);
      assert.ok(result.hashes['0']);
      assert.ok(result.hashes['30000']);
    });
  });

  // ── Valid-empty file ──
  describe('valid-empty file', () => {
    before(() => {
      // Empty lookback block
      writeFileSync(tradeBlockPath(MARKET, '1970-01-01', '00-00-00'), '\n');

      // Target block with trades
      writeFileSync(tradeBlockPath(MARKET, '1970-01-01', '00-00-30'),
        makeTradeBlock([
          { ts: 35000, side: 'buy', price: 100, qty: 2 },
        ]));
    });

    after(() => {
      try { rmSync(tradeBlockPath(MARKET, '1970-01-01', '00-00-00'), { force: true }); } catch (_) {}
      try { rmSync(tradeBlockPath(MARKET, '1970-01-01', '00-00-30'), { force: true }); } catch (_) {}
    });

    it('treats empty file as valid (denom=0 for early seconds)', () => {
      const lookup = buildRawTradedNotionalLookup(TEST_DIR, MARKET, 30000);

      // s=30000: window [0, 30000) — no trades, denom=0
      assert.equal(lookup.get(30000), 0, 'early seconds with empty lookback should be 0');

      // s=36000: window [6000, 36000) — includes ts=35000(200)
      assert.equal(lookup.get(36000), 200);
    });

    it('validateRawTradeLookback accepts valid-empty file', () => {
      const result = validateRawTradeLookback(TEST_DIR, MARKET, 30000);
      assert.equal(result.coverageComplete, true);
      // Empty file yields 0 trades but is valid
      assert.ok(result.trades.length >= 1, 'should have trades from non-empty block');
    });
  });

  // ── Absent lookback (Task 7: absent block = valid-empty) ──
  describe('absent lookback (Task 7)', () => {
    before(() => {
      // Only target block exists, lookback is absent
      writeFileSync(tradeBlockPath(MARKET, '1970-01-01', '00-00-30'),
        makeTradeBlock([
          { ts: 35000, side: 'buy', price: 100, qty: 1 },
        ]));
    });

    after(() => {
      try { rmSync(tradeBlockPath(MARKET, '1970-01-01', '00-00-30'), { force: true }); } catch (_) {}
    });

    it('validateRawTradeLookback returns coverageComplete=true and assumedEmptyBlockStarts', () => {
      const result = validateRawTradeLookback(TEST_DIR, MARKET, 30000);
      assert.equal(result.coverageComplete, true, 'absent lookback is not a coverage failure');
      assert.ok(Array.isArray(result.assumedEmptyBlockStarts), 'should have assumedEmptyBlockStarts');
      assert.ok(result.assumedEmptyBlockStarts.includes(0), 'should include lookback block start (0)');
      // Only target block trades present
      assert.equal(result.trades.length, 1, 'should have trades from target block only');
      assert.equal(result.trades[0].ts, 35000);
    });

    it('buildRawTradedNotionalLookup does not throw for absent lookback', () => {
      const lookup = buildRawTradedNotionalLookup(TEST_DIR, MARKET, 30000);
      assert.ok(lookup instanceof Map, 'should return a Map');
      assert.equal(lookup.size, 30, 'should have 30 keys');
      // Early seconds (no lookback block) should have zero notional
      for (let s = 30000; s < 31000; s += 1000) {
        const val = lookup.get(s);
        assert.ok(isFinite(val), `value for second ${s} should be finite, got ${val}`);
        assert.equal(val, 0, `early second ${s} should have zero notional (absent lookback)`);
      }
      // Later seconds should include the target block trade
      assert.ok(lookup.get(36000) > 0, 'second 36000 should have notional from target block');
    });
  });

  // ── Malformed lines ──
  describe('malformed lines', () => {
    before(() => {
      // Lookback block
      writeFileSync(tradeBlockPath(MARKET, '1970-01-01', '00-00-00'),
        makeTradeBlock([
          { ts: 5000, side: 'buy', price: 100, qty: 1 },
        ]));

      // Target block with malformed data
      writeFileSync(tradeBlockPath(MARKET, '1970-01-01', '00-00-30'),
        '{"ts": 31000, "side": "buy", "price": 200, "qty": 2}\n' +
        'bad json line\n' +
        '{"ts": 32000, "side": "sell", "price": 300, "qty": 3}\n');
    });

    after(() => {
      try { rmSync(tradeBlockPath(MARKET, '1970-01-01', '00-00-00'), { force: true }); } catch (_) {}
      try { rmSync(tradeBlockPath(MARKET, '1970-01-01', '00-00-30'), { force: true }); } catch (_) {}
    });

    it('throws E007 for malformed JSON line', () => {
      assert.throws(
        () => validateRawTradeLookback(TEST_DIR, MARKET, 30000),
        /E007.*malformed/,
        'should throw E007 for malformed JSON'
      );
    });
  });

  // ── Non-finite price/qty ──
  describe('non-finite price/qty', () => {
    before(() => {
      writeFileSync(tradeBlockPath(MARKET, '1970-01-01', '00-00-00'),
        makeTradeBlock([
          { ts: 5000, side: 'buy', price: 100, qty: 1 },
        ]));
    });

    after(() => {
      try { rmSync(tradeBlockPath(MARKET, '1970-01-01', '00-00-00'), { force: true }); } catch (_) {}
    });

    it('throws E007 for NaN price', () => {
      writeFileSync(tradeBlockPath(MARKET, '1970-01-01', '00-00-30'),
        '{"ts": 31000, "side": "buy", "price": "NaN", "qty": 1}\n');
      assert.throws(
        () => validateRawTradeLookback(TEST_DIR, MARKET, 30000),
        /E007.*price/,
        'should throw E007 for NaN price'
      );
    });

    it('throws E007 for Infinity qty', () => {
      writeFileSync(tradeBlockPath(MARKET, '1970-01-01', '00-00-30'),
        '{"ts": 31000, "side": "buy", "price": 100, "qty": "Infinity"}\n');
      assert.throws(
        () => validateRawTradeLookback(TEST_DIR, MARKET, 30000),
        /E007.*qty/,
        'should throw E007 for Infinity qty'
      );
    });

    it('throws E007 for negative price', () => {
      writeFileSync(tradeBlockPath(MARKET, '1970-01-01', '00-00-30'),
        '{"ts": 31000, "side": "buy", "price": -5, "qty": 1}\n');
      assert.throws(
        () => validateRawTradeLookback(TEST_DIR, MARKET, 30000),
        /E007.*price/,
        'should throw E007 for negative price'
      );
    });

    it('throws E007 for zero qty', () => {
      writeFileSync(tradeBlockPath(MARKET, '1970-01-01', '00-00-30'),
        '{"ts": 31000, "side": "buy", "price": 100, "qty": 0}\n');
      assert.throws(
        () => validateRawTradeLookback(TEST_DIR, MARKET, 30000),
        /E007.*qty/,
        'should throw E007 for zero qty'
      );
    });
  });

  // ── ts validation (P0-2) ──
  describe('ts validation (E007 for invalid ts)', () => {
    before(() => {
      writeFileSync(tradeBlockPath(MARKET, '1970-01-01', '00-00-00'),
        makeTradeBlock([
          { ts: 5000, side: 'buy', price: 100, qty: 1 },
        ]));
    });

    after(() => {
      try { rmSync(tradeBlockPath(MARKET, '1970-01-01', '00-00-00'), { force: true }); } catch (_) {}
    });

    it('throws E007 for missing ts', () => {
      writeFileSync(tradeBlockPath(MARKET, '1970-01-01', '00-00-30'),
        '{"side": "buy", "price": 100, "qty": 1}\n');
      assert.throws(
        () => validateRawTradeLookback(TEST_DIR, MARKET, 30000),
        /E007.*invalid ts/,
        'should throw E007 for missing ts'
      );
    });

    it('throws E007 for null ts', () => {
      writeFileSync(tradeBlockPath(MARKET, '1970-01-01', '00-00-30'),
        '{"ts": null, "side": "buy", "price": 100, "qty": 1}\n');
      assert.throws(
        () => validateRawTradeLookback(TEST_DIR, MARKET, 30000),
        /E007.*invalid ts/,
        'should throw E007 for null ts'
      );
    });

    it('throws E007 for NaN ts', () => {
      writeFileSync(tradeBlockPath(MARKET, '1970-01-01', '00-00-30'),
        '{"ts": "NaN", "side": "buy", "price": 100, "qty": 1}\n');
      assert.throws(
        () => validateRawTradeLookback(TEST_DIR, MARKET, 30000),
        /E007.*invalid ts/,
        'should throw E007 for NaN ts'
      );
    });

    it('throws E007 for non-integer ts', () => {
      writeFileSync(tradeBlockPath(MARKET, '1970-01-01', '00-00-30'),
        '{"ts": 31000.5, "side": "buy", "price": 100, "qty": 1}\n');
      assert.throws(
        () => validateRawTradeLookback(TEST_DIR, MARKET, 30000),
        /E007.*invalid ts/,
        'should throw E007 for non-integer ts'
      );
    });

    it('throws E007 for ts before block range', () => {
      // bs=30000, ts=29000 → below range
      writeFileSync(tradeBlockPath(MARKET, '1970-01-01', '00-00-30'),
        '{"ts": 29000, "side": "buy", "price": 100, "qty": 1}\n');
      assert.throws(
        () => validateRawTradeLookback(TEST_DIR, MARKET, 30000),
        /E007.*out of range/,
        'should throw E007 for ts before block start'
      );
    });

    it('throws E007 for ts at or past block end', () => {
      // bs=30000, ts=60000 → at block end (exclusive), out of range
      writeFileSync(tradeBlockPath(MARKET, '1970-01-01', '00-00-30'),
        '{"ts": 60000, "side": "buy", "price": 100, "qty": 1}\n');
      assert.throws(
        () => validateRawTradeLookback(TEST_DIR, MARKET, 30000),
        /E007.*out of range/,
        'should throw E007 for ts at block end'
      );
    });

    it('accepts ts at block start boundary (inclusive)', () => {
      writeFileSync(tradeBlockPath(MARKET, '1970-01-01', '00-00-30'),
        '{"ts": 30000, "side": "buy", "price": 100, "qty": 1}\n');
      const result = validateRawTradeLookback(TEST_DIR, MARKET, 30000);
      assert.strictEqual(result.coverageComplete, true);
      assert.strictEqual(result.trades.length, 2); // lookback + target
      assert.strictEqual(result.trades[1].ts, 30000);
    });

    it('accepts ts just before block end (exclusive boundary)', () => {
      writeFileSync(tradeBlockPath(MARKET, '1970-01-01', '00-00-30'),
        '{"ts": 59999, "side": "sell", "price": 200, "qty": 2}\n');
      const result = validateRawTradeLookback(TEST_DIR, MARKET, 30000);
      assert.strictEqual(result.coverageComplete, true);
      assert.strictEqual(result.trades[1].ts, 59999);
    });

    it('accepts ts in middle of block range', () => {
      writeFileSync(tradeBlockPath(MARKET, '1970-01-01', '00-00-30'),
        '{"ts": 45000, "side": "sell", "price": 150, "qty": 5}\n');
      const result = validateRawTradeLookback(TEST_DIR, MARKET, 30000);
      assert.strictEqual(result.coverageComplete, true);
      assert.strictEqual(result.trades[1].ts, 45000);
    });
  });

  // ── Returns exactly 30 keys ──
  describe('returns 30 keys', () => {
    before(() => {
      writeFileSync(tradeBlockPath(MARKET, '1970-01-01', '00-00-00'),
        makeTradeBlock([
          { ts: 10000, side: 'buy', price: 100, qty: 1 },
        ]));

      writeFileSync(tradeBlockPath(MARKET, '1970-01-01', '00-00-30'),
        makeTradeBlock([
          { ts: 35000, side: 'sell', price: 200, qty: 2 },
        ]));
    });

    after(() => {
      try { rmSync(tradeBlockPath(MARKET, '1970-01-01', '00-00-00'), { force: true }); } catch (_) {}
      try { rmSync(tradeBlockPath(MARKET, '1970-01-01', '00-00-30'), { force: true }); } catch (_) {}
    });

    it('returns exactly 30 secondTs keys', () => {
      const lookup = buildRawTradedNotionalLookup(TEST_DIR, MARKET, 30000);
      assert.equal(lookup.size, 30, 'should have exactly 30 keys');
    });

    it('keys are consecutive seconds starting at blockStartMs', () => {
      const lookup = buildRawTradedNotionalLookup(TEST_DIR, MARKET, 30000);
      for (let s = 30000; s < 60000; s += 1000) {
        assert.ok(lookup.has(s), `should have key for second ${s}`);
      }
    });

    it('every value is a finite number', () => {
      const lookup = buildRawTradedNotionalLookup(TEST_DIR, MARKET, 30000);
      for (const [s, v] of lookup) {
        assert.ok(isFinite(v), `value for second ${s} should be finite, got ${v}`);
      }
    });
  });
});
