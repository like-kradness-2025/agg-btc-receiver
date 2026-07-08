// test/binance-usdc-connector.test.mjs — BinanceSpotUsdcConnector unit tests
//
// Verifies that the new BTCUSDC spot connector uses the correct market key
// and inherits the same trade/depth parsing logic as BinanceSpotConnector.

import { describe, it } from 'node:test';
import assert from 'node:assert';
import { BinanceSpotUsdcConnector } from '../lib/binance-usdc-connector.mjs';

function createTestConnector() {
  const conn = new BinanceSpotUsdcConnector({});
  conn._ws = null;
  conn._setState('running');
  return conn;
}

describe('BinanceSpotUsdcConnector', () => {
  describe('construction', () => {
    it('should set market to binance_spot_usdc', () => {
      const conn = new BinanceSpotUsdcConnector({});
      assert.strictEqual(conn.market, 'binance_spot_usdc');
    });

    it('should create a FullBook with binance_spot_usdc key', () => {
      const conn = new BinanceSpotUsdcConnector({});
      assert.strictEqual(conn.book.market, 'binance_spot_usdc');
    });

    it('should accept config with depthLimit', () => {
      const conn = new BinanceSpotUsdcConnector({ depthLimit: 100 });
      assert.strictEqual(conn.book._maxLevels, 100);
    });

    it('should default depthLimit to unlimited', () => {
      const conn = new BinanceSpotUsdcConnector({});
      assert.strictEqual(conn.book._maxLevels, 0);
    });

    it('should pass wsUrl and restUrl from config to base', () => {
      const cfg = {
        wsUrl: 'wss://stream.binance.com:9443/stream?streams=btcusdc@trade/btcusdc@depth@100ms',
        restUrl: 'https://api.binance.com/api/v3/depth?symbol=BTCUSDC&limit=5000',
      };
      const conn = new BinanceSpotUsdcConnector(cfg);
      assert.strictEqual(conn.wsUrl, cfg.wsUrl);
      assert.strictEqual(conn.restUrl, cfg.restUrl);
    });
  });

  describe('trade event parsing', () => {
    it('should emit trade with market binance_spot_usdc', () => {
      const conn = createTestConnector();
      const tradeEvent = {
        e: 'trade', E: 1700000000000, t: '12345',
        p: '65000.00', q: '1.5', m: false, T: 1700000000000,
      };
      let emitted = null;
      conn.on('trade', (ev) => { emitted = ev; });
      conn._handleTrade(tradeEvent);

      assert.notStrictEqual(emitted, null);
      assert.strictEqual(emitted.market, 'binance_spot_usdc');
      assert.strictEqual(emitted.price, 65000.00);
      assert.strictEqual(emitted.qty, 1.5);
      assert.strictEqual(emitted.side, 'buy');
      assert.strictEqual(emitted.ts, 1700000000000);
      assert.strictEqual(emitted.tradeId, '12345');
    });

    it('should parse sell trade (m=true)', () => {
      const conn = createTestConnector();
      let emitted = null;
      conn.on('trade', (ev) => { emitted = ev; });
      conn._handleTrade({
        e: 'trade', E: 1700000000001, t: '12346',
        p: '65100.00', q: '0.3', m: true, T: 1700000000001,
      });
      assert.strictEqual(emitted.side, 'sell');
    });
  });

  describe('depth event parsing', () => {
    it('should emit depth with market binance_spot_usdc', () => {
      const conn = createTestConnector();
      const depthEvent = {
        e: 'depthUpdate', E: 1700000000000, U: 100, u: 105,
        b: [['65000', '1.5'], ['64999', '2.0']],
        a: [['65001', '0.8'], ['65002', '1.2']],
      };
      let emitted = null;
      conn.on('depth', (ev) => { emitted = ev; });
      conn._handleDepth(depthEvent);

      assert.notStrictEqual(emitted, null);
      assert.strictEqual(emitted.market, 'binance_spot_usdc');
      assert.strictEqual(emitted.type, 'update');
      assert.strictEqual(emitted.ts, 1700000000000);
      assert.strictEqual(emitted.seq, 105);
      assert.strictEqual(emitted.bids.length, 2);
      assert.strictEqual(emitted.asks.length, 2);
    });

    it('should buffer depth events during syncing state', () => {
      const conn = createTestConnector();
      conn._setState('syncing');
      conn._ringBuf = [];
      conn._handleDepth({
        e: 'depthUpdate', E: 1700000000000, U: 100, u: 101,
        b: [['65000', '1']], a: [['65001', '1']],
      });
      assert.strictEqual(conn._ringBuf.length, 1);
    });
  });

  describe('_onMessage routing', () => {
    it('should route depthUpdate to _handleDepth', () => {
      const conn = createTestConnector();
      let called = false;
      conn._handleDepth = () => { called = true; };
      conn._onMessage({ data: { e: 'depthUpdate', b: [], a: [], U: 0, u: 1, E: 0 } });
      assert.ok(called);
    });

    it('should route trade event to _handleTrade', () => {
      const conn = createTestConnector();
      let called = false;
      conn._handleTrade = () => { called = true; };
      conn._onMessage({ data: { e: 'trade', p: '65000', q: '1', m: false, T: 0, t: '0' } });
      assert.ok(called);
    });
  });

  describe('sync validation (inherited from BinanceSpotConnector)', () => {
    it('should validate sync when U <= lastUpdateId + 1 <= u', () => {
      const conn = createTestConnector();
      conn._ringBuf = [
        { U: 100, u: 110 },
        { U: 106, u: 106 },
      ];
      assert.ok(conn._validateSync({ lastUpdateId: 105 }));
    });

    it('should fail sync when U > lastUpdateId + 1 (gap)', () => {
      const conn = createTestConnector();
      conn._ringBuf = [{ U: 200, u: 205 }];
      assert.ok(!conn._validateSync({ lastUpdateId: 100 }));
    });

    it('should fail sync with empty ring buffer', () => {
      const conn = createTestConnector();
      conn._ringBuf = [];
      assert.ok(!conn._validateSync({ lastUpdateId: 100 }));
    });
  });

  describe('_applyDiff (inherited)', () => {
    it('should apply diff event to book', () => {
      const conn = createTestConnector();
      conn.book.applySnapshot([['100', '1.0']], [['101', '1.0']], 0);
      conn._applyDiff({
        U: 1, u: 1,
        b: [['100', '5.0'], ['99', '2.0']],
        a: [['101', '0']],
      });
      assert.strictEqual(conn.book.bids.get('100'), '5.0');
      assert.strictEqual(conn.book.asks.has('101'), false);
      assert.strictEqual(conn.book.bids.get('99'), '2.0');
    });
  });

  describe('_fetchSnapshot (inherited)', () => {
    it('should use the REST URL from config', () => {
      const cfg = { restUrl: 'https://api.binance.com/api/v3/depth?symbol=BTCUSDC&limit=5000' };
      const conn = new BinanceSpotUsdcConnector(cfg);
      assert.strictEqual(conn.restUrl, cfg.restUrl);
    });
  });
});
