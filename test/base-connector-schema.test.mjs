// test/base-connector-schema.test.mjs — FIX6: nested/full payload schema validation
//
// BaseConnector の _validateLevel / _validateLevels / _emitDepth / _emitTrade /
// _emitLiquidation が、ネストした bids/asks [price,qty] ペアや trade price/qty を
// 正しく検証し、不正なペイロードを fail-closed で reject することを確認する。

import { describe, it, before, after } from 'node:test';
import assert from 'node:assert';
import { EventEmitter } from 'node:events';
import { BaseConnector } from '../lib/base-connector.mjs';

// ── Mock WebSocket ────────────────────────────────────────────────────────

class MockWebSocket extends EventEmitter {
  constructor(url) {
    super();
    this.url = url;
    this.readyState = 0;
  }
  close() { this.readyState = 3; }
}

// ── Helpers ───────────────────────────────────────────────────────────────

const _cleanup = [];

function createTestConnector() {
  const conn = new BaseConnector(
    {},
    { market: 'test_market', wsUrl: 'ws://localhost:9999', restUrl: 'http://localhost:9999' },
  );
  conn._setWebSocket(MockWebSocket);
  conn.subscribe = () => {};
  _cleanup.push(conn);
  return conn;
}

after(() => {
  for (const conn of _cleanup) {
    conn._clearTimers();
  }
  _cleanup.length = 0;
});

// ── _validateLevel ────────────────────────────────────────────────────────

describe('BaseConnector._validateLevel', () => {
  it('should accept [number, number] valid level', () => {
    const conn = createTestConnector();
    assert.ok(conn._validateLevel([65000, 1.5]));
  });

  it('should accept [string, string] numeric level', () => {
    const conn = createTestConnector();
    assert.ok(conn._validateLevel(['65000.5', '2.0']));
  });

  it('should accept qty=0 as valid delete', () => {
    const conn = createTestConnector();
    assert.ok(conn._validateLevel(['65000', '0']));
    assert.ok(conn._validateLevel([65000, 0]));
  });

  it('should reject negative price', () => {
    const conn = createTestConnector();
    assert.ok(!conn._validateLevel([-1, 1]));
    assert.ok(!conn._validateLevel(['-1', '1']));
  });

  it('should reject zero price', () => {
    const conn = createTestConnector();
    assert.ok(!conn._validateLevel([0, 1]));
  });

  it('should reject negative qty', () => {
    const conn = createTestConnector();
    assert.ok(!conn._validateLevel([100, -1]));
    assert.ok(!conn._validateLevel(['100', '-0.5']));
  });

  it('should reject NaN price or qty', () => {
    const conn = createTestConnector();
    assert.ok(!conn._validateLevel([NaN, 1]));
    assert.ok(!conn._validateLevel([100, NaN]));
  });

  it('should reject Infinity price or qty', () => {
    const conn = createTestConnector();
    assert.ok(!conn._validateLevel([Infinity, 1]));
    assert.ok(!conn._validateLevel([100, Infinity]));
  });

  it('should reject non-array level', () => {
    const conn = createTestConnector();
    assert.ok(!conn._validateLevel(null));
    assert.ok(!conn._validateLevel(undefined));
    assert.ok(!conn._validateLevel('65000'));
    assert.ok(!conn._validateLevel({ price: 65000, qty: 1 }));
  });

  it('should reject level with fewer than 2 elements', () => {
    const conn = createTestConnector();
    assert.ok(!conn._validateLevel([65000]));
    assert.ok(!conn._validateLevel([]));
  });

  it('should reject non-numeric string price', () => {
    const conn = createTestConnector();
    assert.ok(!conn._validateLevel(['abc', '1']));
  });

  it('should reject empty string price', () => {
    const conn = createTestConnector();
    assert.ok(!conn._validateLevel(['', '1']));
  });
});

// ── _validateLevels ───────────────────────────────────────────────────────

describe('BaseConnector._validateLevels', () => {
  it('should accept empty array', () => {
    const conn = createTestConnector();
    assert.ok(conn._validateLevels([]));
  });

  it('should accept array of valid levels', () => {
    const conn = createTestConnector();
    assert.ok(conn._validateLevels([[65000, 1], [64999, 2]]));
  });

  it('should reject when any single level is invalid', () => {
    const conn = createTestConnector();
    assert.ok(!conn._validateLevels([[65000, 1], [-1, 2]]));
  });

  it('should reject non-array input', () => {
    const conn = createTestConnector();
    assert.ok(!conn._validateLevels(null));
    assert.ok(!conn._validateLevels('string'));
    assert.ok(!conn._validateLevels({}));
  });
});

// ── _emitDepth nested level validation ────────────────────────────────────

describe('BaseConnector._emitDepth nested level validation', () => {
  it('should emit depth event with valid levels', () => {
    const conn = createTestConnector();
    const emitted = [];
    conn.on('depth', (ev) => emitted.push(ev));

    conn._emitDepth('snapshot',
      [['65000', '1.5'], ['64999', '2.0']],
      [['65001', '0.8'], ['65002', '1.2']],
      1700000000000, 100);

    assert.strictEqual(emitted.length, 1);
    assert.strictEqual(emitted[0].type, 'snapshot');
    assert.strictEqual(emitted[0].bids.length, 2);
    assert.strictEqual(emitted[0].asks.length, 2);
  });

  it('should reject depth event with invalid bid level (negative price)', () => {
    const conn = createTestConnector();
    const emitted = [];
    conn.on('depth', (ev) => emitted.push(ev));

    conn._emitDepth('update',
      [['-1', '1.0']],  // invalid: negative price
      [['101', '1.0']],
      1700000000000, 101);

    assert.strictEqual(emitted.length, 0);
  });

  it('should reject depth event with invalid ask level (negative qty)', () => {
    const conn = createTestConnector();
    const emitted = [];
    conn.on('depth', (ev) => emitted.push(ev));

    conn._emitDepth('update',
      [['100', '1.0']],
      [['101', '-1.0']],  // invalid: negative qty
      1700000000000, 101);

    assert.strictEqual(emitted.length, 0);
  });

  it('should reject depth event with non-numeric price in bid', () => {
    const conn = createTestConnector();
    const emitted = [];
    conn.on('depth', (ev) => emitted.push(ev));

    conn._emitDepth('update',
      [['abc', '1.0']],
      [['101', '1.0']],
      1700000000000, 101);

    assert.strictEqual(emitted.length, 0);
  });

  it('should reject depth event when one of many bids is invalid', () => {
    const conn = createTestConnector();
    const emitted = [];
    conn.on('depth', (ev) => emitted.push(ev));

    conn._emitDepth('update',
      [['100', '1.0'], ['99', '2.0'], ['invalid', '3.0']],
      [['101', '1.0']],
      1700000000000, 101);

    assert.strictEqual(emitted.length, 0);
  });

  it('should accept qty=0 levels (delete operation)', () => {
    const conn = createTestConnector();
    const emitted = [];
    conn.on('depth', (ev) => emitted.push(ev));

    conn._emitDepth('update',
      [['100', '0']],  // qty=0 is valid (delete)
      [['101', '1.0']],
      1700000000000, 102);

    assert.strictEqual(emitted.length, 1);
    assert.strictEqual(emitted[0].type, 'update');
  });

  it('should reject depth event with mixed string/number invalid level', () => {
    const conn = createTestConnector();
    const emitted = [];
    conn.on('depth', (ev) => emitted.push(ev));

    conn._emitDepth('snapshot',
      [[100, 1.0]],
      [[101, NaN]],  // NaN qty → invalid
      1700000000000, null);

    assert.strictEqual(emitted.length, 0);
  });
});

// ── _emitTrade price/qty validation ───────────────────────────────────────

describe('BaseConnector._emitTrade price/qty validation', () => {
  it('should emit trade with valid price and qty', () => {
    const conn = createTestConnector();
    const emitted = [];
    conn.on('trade', (ev) => emitted.push(ev));

    conn._emitTrade(65000, 1.0, 'buy', 1700000000000, 't1');
    assert.strictEqual(emitted.length, 1);
    assert.strictEqual(emitted[0].price, 65000);
    assert.strictEqual(emitted[0].qty, 1.0);
  });

  it('should reject trade with zero price', () => {
    const conn = createTestConnector();
    const emitted = [];
    conn.on('trade', (ev) => emitted.push(ev));

    conn._emitTrade(0, 1.0, 'buy', 1700000000000, 't1');
    assert.strictEqual(emitted.length, 0);
  });

  it('should reject trade with negative price', () => {
    const conn = createTestConnector();
    const emitted = [];
    conn.on('trade', (ev) => emitted.push(ev));

    conn._emitTrade(-100, 1.0, 'buy', 1700000000000, 't1');
    assert.strictEqual(emitted.length, 0);
  });

  it('should reject trade with zero qty', () => {
    const conn = createTestConnector();
    const emitted = [];
    conn.on('trade', (ev) => emitted.push(ev));

    conn._emitTrade(65000, 0, 'buy', 1700000000000, 't1');
    assert.strictEqual(emitted.length, 0);
  });

  it('should reject trade with negative qty', () => {
    const conn = createTestConnector();
    const emitted = [];
    conn.on('trade', (ev) => emitted.push(ev));

    conn._emitTrade(65000, -0.5, 'buy', 1700000000000, 't1');
    assert.strictEqual(emitted.length, 0);
  });

  it('should reject trade with NaN price', () => {
    const conn = createTestConnector();
    const emitted = [];
    conn.on('trade', (ev) => emitted.push(ev));

    conn._emitTrade(NaN, 1.0, 'buy', 1700000000000, 't1');
    assert.strictEqual(emitted.length, 0);
  });

  it('should reject trade with Infinity qty', () => {
    const conn = createTestConnector();
    const emitted = [];
    conn.on('trade', (ev) => emitted.push(ev));

    conn._emitTrade(65000, Infinity, 'buy', 1700000000000, 't1');
    assert.strictEqual(emitted.length, 0);
  });

  it('should reject trade with invalid side after valid price/qty', () => {
    const conn = createTestConnector();
    const emitted = [];
    conn.on('trade', (ev) => emitted.push(ev));

    conn._emitTrade(65000, 1.0, 'invalid', 1700000000000, 't1');
    assert.strictEqual(emitted.length, 0);
  });

  it('should emit valid combo of side "sell"', () => {
    const conn = createTestConnector();
    const emitted = [];
    conn.on('trade', (ev) => emitted.push(ev));

    conn._emitTrade(65000, 2.5, 'sell', 1700000000000, 't2');
    assert.strictEqual(emitted.length, 1);
    assert.strictEqual(emitted[0].side, 'sell');
  });

  it('should reject with non-number price string', () => {
    const conn = createTestConnector();
    const emitted = [];
    conn.on('trade', (ev) => emitted.push(ev));

    // _emitTrade は数値を受け取ることを前提とする。文字列は number ではないので reject
    conn._emitTrade('65000', 1.0, 'buy', 1700000000000, 't1');
    assert.strictEqual(emitted.length, 0);
  });
});

// ── _emitLiquidation (existing contract coverage) ─────────────────────────

describe('BaseConnector._emitLiquidation validation', () => {
  it('should emit liquidation with valid fields', () => {
    const conn = createTestConnector();
    const emitted = [];
    conn.on('liquidation', (ev) => emitted.push(ev));

    conn._emitLiquidation({
      exchange: 'test_exchange',
      symbol: 'BTCUSDT',
      side: 'sell',
      price: 65000,
      qty: 1.5,
    });

    assert.strictEqual(emitted.length, 1);
    assert.strictEqual(emitted[0].side, 'sell');
    assert.strictEqual(emitted[0].price, 65000);
    assert.strictEqual(emitted[0].qty, 1.5);
    assert.strictEqual(emitted[0].notional, 97500); // 65000 * 1.5
  });

  it('should reject liquidation with negative price', () => {
    const conn = createTestConnector();
    const emitted = [];
    conn.on('liquidation', (ev) => emitted.push(ev));

    conn._emitLiquidation({
      exchange: 'test_exchange',
      symbol: 'BTCUSDT',
      side: 'buy',
      price: -100,
      qty: 1.0,
    });

    assert.strictEqual(emitted.length, 0);
  });

  it('should reject liquidation with zero qty', () => {
    const conn = createTestConnector();
    const emitted = [];
    conn.on('liquidation', (ev) => emitted.push(ev));

    conn._emitLiquidation({
      exchange: 'test_exchange',
      symbol: 'BTCUSDT',
      side: 'sell',
      price: 65000,
      qty: 0,
    });

    assert.strictEqual(emitted.length, 0);
  });

  it('should reject liquidation with invalid side', () => {
    const conn = createTestConnector();
    const emitted = [];
    conn.on('liquidation', (ev) => emitted.push(ev));

    conn._emitLiquidation({
      exchange: 'test_exchange',
      symbol: 'BTCUSDT',
      side: 'Buy',  // invalid: not 'buy' or 'sell'
      price: 65000,
      qty: 1.0,
    });

    assert.strictEqual(emitted.length, 0);
  });
});
