// test/binance-connector-parser.test.mjs — Binance connector parser unit tests

import { describe, it } from 'node:test';
import assert from 'node:assert';
import { BinanceSpotConnector, BinancePerpConnector } from '../lib/binance-connector.mjs';

/**
 * Create a partial connector for testing parser methods only.
 * We use a proxy to avoid needing WebSocket/network.
 */
function createTestConnector() {
  const conn = new BinanceSpotConnector({});
  // Disable WS — we only test message parsing
  conn._ws = null;
  conn._setState('running');
  return conn;
}

describe('BinanceConnector parser', () => {
  describe('trade event parsing (spot)', () => {
    it('should parse buy trade (m=false → buyer is taker → buy)', () => {
      const conn = createTestConnector();
      const tradeEvent = {
        e: 'trade',
        E: 1700000000000,
        t: '12345',
        p: '65000.00',
        q: '1.5',
        m: false, // buyer is maker → taker is seller → sell
        T: 1700000000000,
      };
      let emitted = null;
      conn.on('trade', (ev) => { emitted = ev; });
      conn._handleTrade(tradeEvent);

      assert.notStrictEqual(emitted, null);
      assert.strictEqual(emitted.market, 'binance_spot');
      assert.strictEqual(emitted.price, 65000.00);
      assert.strictEqual(emitted.qty, 1.5);
      assert.strictEqual(emitted.side, 'buy'); // m=false → buyer is taker → buy
      // Wait — the docs say: buyerMaker true means taker sell
      // m=true → isBuyerMaker=true → buyer is the maker → taker is seller → sell
      // m=false → isBuyerMaker=false → buyer is the taker → taker is buyer → buy
      assert.strictEqual(emitted.side, 'buy');
      assert.strictEqual(emitted.ts, 1700000000000);
      assert.strictEqual(emitted.tradeId, '12345');
    });

    it('should parse sell trade (m=true → buyer is maker → taker sells)', () => {
      const conn = createTestConnector();
      const tradeEvent = {
        e: 'trade',
        E: 1700000000001,
        t: '12346',
        p: '65100.00',
        q: '0.3',
        m: true, // buyer is maker → taker is seller
        T: 1700000000001,
      };
      let emitted = null;
      conn.on('trade', (ev) => { emitted = ev; });
      conn._handleTrade(tradeEvent);

      assert.strictEqual(emitted.side, 'sell');
    });

    it('should parse aggTrade (perp)', () => {
      const conn = new BinancePerpConnector({});
      conn._ws = null;
      conn._setState('running');

      const aggTradeEvent = {
        e: 'aggTrade',
        E: 1700000000002,
        t: '999',
        p: '65200.00',
        q: '2.0',
        m: false,
        T: 1700000000002,
      };
      let emitted = null;
      conn.on('trade', (ev) => { emitted = ev; });
      conn._handleTrade(aggTradeEvent);

      assert.strictEqual(emitted.price, 65200.00);
      assert.strictEqual(emitted.qty, 2.0);
      assert.strictEqual(emitted.side, 'buy');
      assert.strictEqual(emitted.tradeId, '999');
    });
  });

  describe('depth event parsing', () => {
    it('should parse depthUpdate and emit depth event', () => {
      const conn = createTestConnector();
      const depthEvent = {
        e: 'depthUpdate',
        E: 1700000000000,
        U: 100,
        u: 105,
        b: [['65000.00', '1.5'], ['64999.00', '2.0']],
        a: [['65001.00', '0.8'], ['65002.00', '1.2']],
      };

      let emitted = null;
      conn.on('depth', (ev) => { emitted = ev; });
      conn._handleDepth(depthEvent);

      assert.notStrictEqual(emitted, null);
      assert.strictEqual(emitted.market, 'binance_spot');
      assert.strictEqual(emitted.type, 'update');
      assert.strictEqual(emitted.ts, 1700000000000);
      assert.strictEqual(emitted.seq, 105);
      assert.strictEqual(emitted.bids.length, 2);
      assert.strictEqual(emitted.asks.length, 2);
      assert.deepStrictEqual(emitted.bids[0], ['65000.00', '1.5']);
      assert.deepStrictEqual(emitted.asks[0], ['65001.00', '0.8']);
    });

    it('should buffer depth events during syncing state', () => {
      const conn = createTestConnector();
      conn._setState('syncing');
      conn._ringBuf = [];

      const depthEvent = {
        e: 'depthUpdate',
        E: 1700000000000,
        U: 100, u: 101,
        b: [['65000', '1']], a: [['65001', '1']],
      };
      conn._handleDepth(depthEvent);
      assert.strictEqual(conn._ringBuf.length, 1);
      assert.strictEqual(conn._ringBuf[0].U, 100);
    });
  });

  describe('_onMessage routing', () => {
    it('should route depthUpdate to _handleDepth (spot)', () => {
      const conn = createTestConnector();
      let called = false;
      conn._handleDepth = () => { called = true; };
      conn._onMessage({ data: { e: 'depthUpdate', b: [], a: [], U: 0, u: 1, E: 0 } });
      assert.ok(called);
    });

    it('should route trade event to _handleTrade (spot)', () => {
      const conn = createTestConnector();
      let called = false;
      conn._handleTrade = () => { called = true; };
      conn._onMessage({ data: { e: 'trade', p: '65000', q: '1', m: false, T: 0, t: '0' } });
      assert.ok(called);
    });

    it('should route aggTrade through combined stream wrapper (perp)', () => {
      const conn = new BinancePerpConnector({});
      conn._ws = null;
      conn._setState('running');

      let emitted = null;
      conn.on('trade', (ev) => { emitted = ev; });

      // Perp combined stream wraps in { stream, data }
      conn._onMessage({
        stream: 'btcusdt@aggTrade',
        data: {
          e: 'aggTrade',
          E: 1700000000002,
          t: '999',
          p: '65200.00',
          q: '2.0',
          m: false,
          T: 1700000000002,
        },
      });

      assert.strictEqual(emitted.price, 65200.00);
      assert.strictEqual(emitted.qty, 2.0);
      assert.strictEqual(emitted.side, 'buy');
      assert.strictEqual(emitted.tradeId, '999');
    });

    it('should route trade event (e:trade) through combined stream wrapper (perp)', () => {
      const conn = new BinancePerpConnector({});
      conn._ws = null;
      conn._setState('running');

      let emitted = null;
      conn.on('trade', (ev) => { emitted = ev; });

      // Perp combined stream with @trade
      conn._onMessage({
        stream: 'btcusdt@trade',
        data: {
          e: 'trade',
          E: 1700000000003,
          t: '888',
          p: '65400.00',
          q: '1.5',
          m: true,
          T: 1700000000003,
        },
      });

      assert.strictEqual(emitted.price, 65400.00);
      assert.strictEqual(emitted.qty, 1.5);
      assert.strictEqual(emitted.side, 'sell');
      assert.strictEqual(emitted.tradeId, '888');
    });

    it('should handle perp direct event (non-combined) as fallback', () => {
      const conn = new BinancePerpConnector({});
      conn._ws = null;
      conn._setState('running');

      let emitted = null;
      conn.on('trade', (ev) => { emitted = ev; });

      // Direct event without {stream, data} wrapper
      conn._onMessage({
        e: 'aggTrade',
        E: 1700000000003,
        t: '1000',
        p: '65300.00',
        q: '1.0',
        m: true,
        T: 1700000000003,
      });

      assert.strictEqual(emitted.price, 65300.00);
      assert.strictEqual(emitted.side, 'sell');
    });
  });

  it('should emit liquidation from forceOrder event (perp)', () => {
    const conn = new BinancePerpConnector({});
    conn._ws = null;
    conn._setState('running');

    let emitted = null;
    conn.on('liquidation', (ev) => { emitted = ev; });

    conn._handleForceOrder({
      e: 'forceOrder',
      E: 1700000000000,
      o: {
        s: 'BTCUSDT',
        S: 'SELL',
        T: 1700000000000,
        p: '65000.00',
        q: '1.5',
        z: '1.5',
        X: 'FILLED',
        f: 'LIQUIDATION',
        l: '1.5',
        ap: '65000.00',
      },
    });

    assert.notStrictEqual(emitted, null);
    assert.strictEqual(emitted.market, 'binance_perp');
    assert.strictEqual(emitted.exchange, 'binance');
    assert.strictEqual(emitted.symbol, 'BTCUSDT');
    assert.strictEqual(emitted.side, 'sell');
    assert.strictEqual(emitted.price, 65000.00);
    assert.strictEqual(emitted.qty, 1.5);
    assert.strictEqual(emitted.notional, 97500.00);
    assert.strictEqual(emitted.raw_type, 'forceOrder');
  });
});

describe('Depth sequence validation (running state)', () => {
  describe('Spot stale/duplicate detection', () => {
    it('should ignore stale depth events with u <= localSeq', () => {
      const conn = createTestConnector();
      conn.book.applySnapshot([['100', '1']], [['101', '1']], 105);

      let depthEmitted = false;
      conn.on('depth', () => { depthEmitted = true; });

      conn._handleDepth({
        e: 'depthUpdate', E: 1700000000000, U: 95, u: 100,
        b: [['100', '0']], a: [],
      });

      assert.strictEqual(depthEmitted, false);
      assert.strictEqual(conn.book._lastSeq, 105); // unchanged
    });

    it('should ignore duplicate event with u === localSeq', () => {
      const conn = createTestConnector();
      conn.book.applySnapshot([['100', '1']], [['101', '1']], 100);

      let depthEmitted = false;
      conn.on('depth', () => { depthEmitted = true; });

      conn._handleDepth({
        e: 'depthUpdate', E: 1700000000000, U: 95, u: 100,
        b: [['100', '0']], a: [],
      });

      assert.strictEqual(depthEmitted, false);
    });
  });

  describe('Spot gap triggers resync', () => {
    it('should emit error and reset book on gap', () => {
      const conn = createTestConnector();
      conn.book.applySnapshot([['100', '1']], [['101', '1']], 100);

      const errors = [];
      conn.on('error', (e) => errors.push(e));

      // Gap: U=200 > localSeq+1=101
      conn._handleDepth({
        e: 'depthUpdate', E: 1700000000000, U: 200, u: 205,
        b: [], a: [],
      });

      assert.ok(errors.length > 0);
      assert.ok(errors[0].message.includes('seq gap'));
      assert.strictEqual(conn.book._lastSeq, null); // reset

      // Cleanup pending reconnect timer
      if (conn._reconnectTimer) {
        clearTimeout(conn._reconnectTimer);
        conn._reconnectTimer = null;
      }
    });

    it('should not apply diff when gap detected', () => {
      const conn = createTestConnector();
      conn.book.applySnapshot([['100', '1']], [['101', '1']], 100);

      let depthEmitted = false;
      conn.on('depth', () => { depthEmitted = true; });
      conn.on('error', () => {}); // suppress unhandled error

      conn._handleDepth({
        e: 'depthUpdate', E: 1700000000000, U: 200, u: 205,
        b: [['100', '5']], a: [],
      });

      assert.strictEqual(depthEmitted, false);
      // Book was reset (cleared) — no diff applied
      assert.strictEqual(conn.book.bids.size, 0);
      assert.strictEqual(conn.book._lastSeq, null);

      // Cleanup pending reconnect timer
      if (conn._reconnectTimer) {
        clearTimeout(conn._reconnectTimer);
        conn._reconnectTimer = null;
      }
    });
  });

  describe('Spot valid sequence applies and updates lastSeq', () => {
    it('should accept valid consecutive depth diff', () => {
      const conn = createTestConnector();
      conn.book.applySnapshot([['100', '1']], [['101', '1']], 100);

      let depthEmitted = null;
      conn.on('depth', (ev) => { depthEmitted = ev; });

      // U=100 <= 101 && 101 <= u=105 → valid
      conn._handleDepth({
        e: 'depthUpdate', E: 1700000000000, U: 100, u: 105,
        b: [['100', '5']], a: [],
      });

      assert.notStrictEqual(depthEmitted, null);
      assert.strictEqual(depthEmitted.seq, 105);
      assert.strictEqual(conn._stats.lastSeq, 105); // health stat updated
      assert.strictEqual(conn.book.bids.get('100'), '5'); // diff applied
      assert.strictEqual(conn.book._lastSeq, 105);
    });
  });

  describe('Perp valid pu match applies', () => {
    it('should apply diff when pu === localSeq', () => {
      const conn = new BinancePerpConnector({});
      conn._ws = null;
      conn._setState('running');
      conn.book.applySnapshot([['100', '1']], [['101', '1']], 100);

      let depthEmitted = null;
      conn.on('depth', (ev) => { depthEmitted = ev; });

      conn._handleDepth({
        e: 'depthUpdate', E: 1700000000000, U: 100, u: 105, pu: 100,
        b: [['100', '5']], a: [],
      });

      assert.notStrictEqual(depthEmitted, null);
      assert.strictEqual(depthEmitted.seq, 105);
      assert.strictEqual(conn._stats.lastSeq, 105);
      assert.strictEqual(conn.book.bids.get('100'), '5');
      assert.strictEqual(conn.book._lastSeq, 105);
    });

    it('should ignore stale perp diff with u <= localSeq', () => {
      const conn = new BinancePerpConnector({});
      conn._ws = null;
      conn._setState('running');
      conn.book.applySnapshot([['100', '1']], [['101', '1']], 100);

      let depthEmitted = false;
      conn.on('depth', () => { depthEmitted = true; });

      conn._handleDepth({
        e: 'depthUpdate', E: 1700000000000, U: 80, u: 90, pu: 100,
        b: [], a: [],
      });

      assert.strictEqual(depthEmitted, false);
      assert.strictEqual(conn.book._lastSeq, 100);
    });
  });

  describe('Perp pu mismatch triggers resync', () => {
    it('should emit error and reset book when pu mismatch', () => {
      const conn = new BinancePerpConnector({});
      conn._ws = null;
      conn._setState('running');
      conn.book.applySnapshot([['100', '1']], [['101', '1']], 100);

      const errors = [];
      conn.on('error', (e) => errors.push(e));

      // pu=50 !== localSeq=100
      conn._handleDepth({
        e: 'depthUpdate', E: 1700000000000, U: 101, u: 105, pu: 50,
        b: [], a: [],
      });

      assert.ok(errors.length > 0);
      assert.ok(errors[0].message.includes('seq gap'));
      assert.strictEqual(conn.book._lastSeq, null); // reset

      // Cleanup pending reconnect timer
      if (conn._reconnectTimer) {
        clearTimeout(conn._reconnectTimer);
        conn._reconnectTimer = null;
      }
    });

    it('should not apply diff when pu mismatch', () => {
      const conn = new BinancePerpConnector({});
      conn._ws = null;
      conn._setState('running');
      conn.book.applySnapshot([['100', '1']], [['101', '1']], 100);

      let depthEmitted = false;
      conn.on('depth', () => { depthEmitted = true; });
      conn.on('error', () => {}); // suppress unhandled error

      conn._handleDepth({
        e: 'depthUpdate', E: 1700000000000, U: 101, u: 105, pu: 50,
        b: [['100', '5']], a: [],
      });

      assert.strictEqual(depthEmitted, false);
      // Book was reset (cleared) — no diff applied
      assert.strictEqual(conn.book.bids.size, 0);
      assert.strictEqual(conn.book._lastSeq, null);

      // Cleanup pending reconnect timer
      if (conn._reconnectTimer) {
        clearTimeout(conn._reconnectTimer);
        conn._reconnectTimer = null;
      }
    });
  });
});

describe('Perp first post-sync bridge (firstRunningDiff)', () => {
  it('should accept bridge when U <= localSeq <= u and firstRunningDiff true', () => {
    const conn = new BinancePerpConnector({});
    conn._ws = null;
    conn._setState('running');
    conn._firstRunningDiff = true;
    conn.book.applySnapshot([['100', '1']], [['101', '1']], 100);

    let depthEmitted = null;
    conn.on('depth', (ev) => { depthEmitted = ev; });

    // pu=90 != localSeq=100 (would fail strict mode)
    // but bridge: U=95 <= 100 <= 105 => accepted
    conn._handleDepth({
      e: 'depthUpdate', E: 1700000000000, U: 95, u: 105, pu: 90,
      b: [['100', '5']], a: [],
    });

    assert.notStrictEqual(depthEmitted, null);
    assert.strictEqual(depthEmitted.seq, 105);
    assert.strictEqual(conn._stats.lastSeq, 105);
    assert.strictEqual(conn.book.bids.get('100'), '5');
    assert.strictEqual(conn.book._lastSeq, 105);
    assert.strictEqual(conn._firstRunningDiff, false);
  });

  it('should trigger gap when bridge condition fails (U > localSeq)', () => {
    const conn = new BinancePerpConnector({});
    conn._ws = null;
    conn._setState('running');
    conn._firstRunningDiff = true;
    conn.book.applySnapshot([['100', '1']], [['101', '1']], 100);

    const errors = [];
    conn.on('error', (e) => errors.push(e));

    // U=200 > localSeq=100 → bridge fail
    conn._handleDepth({
      e: 'depthUpdate', E: 1700000000000, U: 200, u: 205, pu: 90,
      b: [], a: [],
    });

    assert.ok(errors.length > 0);
    assert.ok(errors[0].message.includes('bridge fail'));
    assert.strictEqual(conn.book._lastSeq, null); // reset

    if (conn._reconnectTimer) {
      clearTimeout(conn._reconnectTimer);
      conn._reconnectTimer = null;
    }
  });

  it('should apply strict pu check after first bridge is accepted', () => {
    const conn = new BinancePerpConnector({});
    conn._ws = null;
    conn._setState('running');
    conn._firstRunningDiff = true;
    conn.book.applySnapshot([['100', '1']], [['101', '1']], 100);

    let depthEmitted = null;
    conn.on('depth', (ev) => { depthEmitted = ev; });

    // First event: bridge accepted
    conn._handleDepth({
      e: 'depthUpdate', E: 1700000000000, U: 95, u: 105, pu: 90,
      b: [['100', '5']], a: [],
    });
    assert.strictEqual(conn._firstRunningDiff, false);
    assert.strictEqual(conn.book._lastSeq, 105);

    // Second event: strict mode, pu must match 105
    const errors = [];
    conn.on('error', (e) => errors.push(e));

    conn._handleDepth({
      e: 'depthUpdate', E: 1700000000001, U: 106, u: 110, pu: 100, // pu != 105
      b: [], a: [],
    });

    assert.ok(errors.length > 0);
    assert.ok(errors[0].message.includes('pu mismatch'));
    assert.strictEqual(conn.book._lastSeq, null); // reset

    if (conn._reconnectTimer) {
      clearTimeout(conn._reconnectTimer);
      conn._reconnectTimer = null;
    }
  });
});

describe('Perp ring-buf-applied skips bridge (firstRunningDiff=false)', () => {
  it('should set firstRunningDiff=false when ring buf applied during sync, accept next pu-match diff', () => {
    const conn = new BinancePerpConnector({});
    conn._ws = null;
    conn._setState('syncing');

    // Simulate ring buffer with a diff bridging the snapshot
    conn._ringBuf = [{
      e: 'depthUpdate', E: 1700000001000,
      U: 101, u: 105, pu: 100,
      b: [['100', '5']], a: [],
    }];

    // Simulate snapshot
    const snapshot = {
      lastUpdateId: 100,
      bids: [['100', '1'], ['99', '2']],
      asks: [['101', '1'], ['102', '2']],
    };

    conn._applyRingBuf(snapshot);

    // After applyRingBuf, _ringBufApplied should be true
    assert.strictEqual(conn._ringBufApplied, true);
    // Book seq advanced past snapshot
    assert.strictEqual(conn.book._lastSeq, 105);

    // Simulate the overridden _syncBook behavior
    conn._firstRunningDiff = !conn._ringBufApplied;
    assert.strictEqual(conn._firstRunningDiff, false);

    // Now simulate the live diff that arrives after sync
    conn._setState('running');
    let depthEmitted = null;
    conn.on('depth', (ev) => { depthEmitted = ev; });

    // This diff has U=200 > localSeq=105 (would fail bridge check)
    // but pu=105 === localSeq (passes strict pu check)
    conn._handleDepth({
      e: 'depthUpdate', E: 1700000002000,
      U: 200, u: 210, pu: 105,
      b: [['100', '3']], a: [],
    });

    assert.notStrictEqual(depthEmitted, null);
    assert.strictEqual(depthEmitted.seq, 210);
    assert.strictEqual(conn.book._lastSeq, 210);
    assert.strictEqual(conn.book.bids.get('100'), '3');
  });

  it('should keep firstRunningDiff=true when ring buf had no valid diffs (snapshot ahead)', () => {
    const conn = new BinancePerpConnector({});
    conn._ws = null;
    conn._setState('syncing');

    // Ring buffer has only stale diffs (u <= snapshot lastUpdateId)
    conn._ringBuf = [{
      e: 'depthUpdate', E: 1700000000500,
      U: 50, u: 80, pu: 49,
      b: [], a: [],
    }];

    const snapshot = {
      lastUpdateId: 100,
      bids: [['100', '1']],
      asks: [['101', '1']],
    };

    conn._applyRingBuf(snapshot);

    // No valid diff applied
    assert.strictEqual(conn._ringBufApplied, false);

    // Simulate the overridden _syncBook behavior
    conn._firstRunningDiff = !conn._ringBufApplied;
    assert.strictEqual(conn._firstRunningDiff, true);

    // Bridge check should accept a diff covering localSeq
    conn._setState('running');
    let depthEmitted = null;
    conn.on('depth', (ev) => { depthEmitted = ev; });

    conn._handleDepth({
      e: 'depthUpdate', E: 1700000002000,
      U: 95, u: 105, pu: 90, // U=95 <= localSeq=100 && 100 <= 105 → bridge ok
      b: [['100', '5']], a: [],
    });

    assert.notStrictEqual(depthEmitted, null);
    assert.strictEqual(depthEmitted.seq, 105);
    assert.strictEqual(conn._firstRunningDiff, false);
  });

  it('should still trigger bridge failure when ring buf empty and firstRunningDiff true', () => {
    const conn = new BinancePerpConnector({});
    conn._ws = null;
    conn._setState('syncing');

    conn._ringBuf = [];
    const snapshot = {
      lastUpdateId: 100,
      bids: [['100', '1']],
      asks: [['101', '1']],
    };

    conn._applyRingBuf(snapshot);
    assert.strictEqual(conn._ringBufApplied, false);

    conn._firstRunningDiff = !conn._ringBufApplied;
    assert.strictEqual(conn._firstRunningDiff, true);

    conn._setState('running');
    const errors = [];
    conn.on('error', (e) => errors.push(e));

    // Bridge fails: U=200 > localSeq=100
    conn._handleDepth({
      e: 'depthUpdate', E: 1700000002000,
      U: 200, u: 205, pu: 90,
      b: [], a: [],
    });

    assert.ok(errors.length > 0);
    assert.ok(errors[0].message.includes('bridge fail'));

    if (conn._reconnectTimer) {
      clearTimeout(conn._reconnectTimer);
      conn._reconnectTimer = null;
    }
  });
});

describe('Empty depth diff updates lastSeq', () => {
  it('should update book._lastSeq and stats on empty valid spot diff', () => {
    const conn = new BinanceSpotConnector({});
    conn._ws = null;
    conn._setState('running');
    conn._firstRunningDiff = false;
    conn.book.applySnapshot([['100', '1']], [['101', '1']], 100);

    let depthEmitted = null;
    conn.on('depth', (ev) => { depthEmitted = ev; });

    // Valid diff with empty bids/asks
    conn._handleDepth({
      e: 'depthUpdate', E: 1700000000000, U: 100, u: 105,
      b: [], a: [],
    });

    assert.notStrictEqual(depthEmitted, null);
    assert.strictEqual(depthEmitted.seq, 105);
    assert.strictEqual(conn.book._lastSeq, 105);
    assert.strictEqual(conn._stats.lastSeq, 105);
  });

  it('should update book._lastSeq and stats on empty valid perp diff', () => {
    const conn = new BinancePerpConnector({});
    conn._ws = null;
    conn._setState('running');
    conn._firstRunningDiff = false;
    conn.book.applySnapshot([['100', '1']], [['101', '1']], 100);

    let depthEmitted = null;
    conn.on('depth', (ev) => { depthEmitted = ev; });

    // Valid perp diff with empty bids/asks
    conn._handleDepth({
      e: 'depthUpdate', E: 1700000000000, U: 100, u: 105, pu: 100,
      b: [], a: [],
    });

    assert.notStrictEqual(depthEmitted, null);
    assert.strictEqual(depthEmitted.seq, 105);
    assert.strictEqual(conn.book._lastSeq, 105);
    assert.strictEqual(conn._stats.lastSeq, 105);
  });
});

describe('Reconnect scheduling guards', () => {
  it('should not double-schedule via _reconnectTimer guard', () => {
    const conn = new BinanceSpotConnector({});
    conn._ws = null;
    conn._setState('running');

    conn._scheduleReconnect();
    assert.strictEqual(conn._stats.reconnectCount, 1);
    assert.ok(conn._reconnectTimer !== null);

    const timerRef = conn._reconnectTimer;
    conn._scheduleReconnect();
    assert.strictEqual(conn._stats.reconnectCount, 1);
    assert.strictEqual(conn._reconnectTimer, timerRef); // same timer, not replaced

    if (conn._reconnectTimer) {
      clearTimeout(conn._reconnectTimer);
      conn._reconnectTimer = null;
    }
  });

  it('should not schedule reconnect via close handler when already reconnecting', () => {
    const conn = new BinanceSpotConnector({});
    conn._ws = null;
    conn._setState('running');
    conn.on('error', () => {}); // suppress expected error

    conn._handleSequenceGap('test gap', {});
    assert.strictEqual(conn._stats.reconnectCount, 1);
    assert.strictEqual(conn._state, 'reconnecting');

    // Simulate ws close handler (same logic as in connect())
    if (!conn._isShuttingDown && conn._state !== 'reconnecting' && conn._state !== 'error') {
      conn._setState('reconnecting');
      conn._scheduleReconnect();
    }
    // Guard prevented extra schedule
    assert.strictEqual(conn._stats.reconnectCount, 1);

    if (conn._reconnectTimer) {
      clearTimeout(conn._reconnectTimer);
      conn._reconnectTimer = null;
    }
  });

  it('should not schedule reconnect when state is error via close handler guard', () => {
    const conn = new BinanceSpotConnector({});
    conn._ws = null;
    conn._setState('error');

    // Simulate ws close handler (same guard as in connect())
    if (!conn._isShuttingDown && conn._state !== 'reconnecting' && conn._state !== 'error') {
      conn._setState('reconnecting');
      conn._scheduleReconnect();
    }
    // Guard prevented scheduling — state stays error
    assert.strictEqual(conn._state, 'error');
    assert.strictEqual(conn._reconnectTimer, null);
  });
});

describe('Binance spot maxLevels from config.depthLimit', () => {
  it('should use depthLimit from config for FullBook maxLevels', () => {
    const conn = new BinanceSpotConnector({ depthLimit: 5000 });
    assert.strictEqual(conn.book._maxLevels, 5000);
  });

  it('should fallback to unlimited when depthLimit undefined', () => {
    const conn = new BinanceSpotConnector({});
    assert.strictEqual(conn.book._maxLevels, 0);
  });

  it('should use depthLimit from config for BinancePerpConnector', () => {
    const conn = new BinancePerpConnector({ depthLimit: 1000 });
    assert.strictEqual(conn.book._maxLevels, 1000);
  });
});
