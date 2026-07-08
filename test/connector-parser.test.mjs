// test/connector-parser.test.mjs — Bybit/OKX/Coinbase/Hyperliquid connector parser unit tests

import { describe, it } from 'node:test';
import assert from 'node:assert';
import { BybitConnector } from '../lib/bybit-connector.mjs';
import { OkxConnector } from '../lib/okx-connector.mjs';
import { CoinbaseConnector } from '../lib/coinbase-connector.mjs';
import { HyperliquidConnector } from '../lib/hyperliquid-connector.mjs';
import { BinanceSpotUsdcConnector } from '../lib/binance-usdc-connector.mjs';
import { calculateKrakenChecksum } from '../lib/kraken-connector.mjs';
import { BinanceCoinmPerpConnector, BinancePerpBtcusdcConnector, BybitSpotConnector, OkxSpotConnector, KrakenSpotConnectorAlias } from '../lib/market-connectors.mjs';

// ====== Bybit ======

describe('BybitConnector parser', () => {
  function createBybitConn() {
    const conn = new BybitConnector({});
    conn._ws = null;
    conn._setState('running');
    return conn;
  }

  describe('snapshot', () => {
    it('should apply full book snapshot', () => {
      const conn = createBybitConn();
      const msg = {
        topic: 'orderbook.1000.BTCUSDT',
        type: 'snapshot',
        ts: 1700000000000,
        data: {
          s: 'BTCUSDT',
          seq: 12345,
          b: [['65000.00', '1.5'], ['64999.00', '2.0']],
          a: [['65001.00', '0.8'], ['65002.00', '1.2']],
        },
      };

      let emitted = null;
      conn.on('depth', (ev) => { emitted = ev; });
      conn._handleDepth(msg);

      assert.notStrictEqual(emitted, null);
      assert.strictEqual(emitted.type, 'snapshot');
      assert.strictEqual(emitted.seq, 12345);
      assert.strictEqual(emitted.bids.length, 2);
      assert.strictEqual(emitted.asks.length, 2);
      assert.strictEqual(conn.book.getBestBid(), '65000.00');
      assert.strictEqual(conn.book.getBestAsk(), '65001.00');
    });
  });

  describe('delta', () => {
    it('should apply delta updates and update seq', () => {
      const conn = createBybitConn();
      conn.book.applySnapshot([['65000', '1']], [['65001', '1']], 100);
      conn._notifyWsSnapshotReceived(100);

      let depthEmitted = null;
      conn.on('depth', (ev) => { depthEmitted = ev; });

      conn._handleDepth({
        topic: 'orderbook.1000.BTCUSDT',
        type: 'delta',
        ts: 1700000000100,
        data: {
          s: 'BTCUSDT',
          seq: 101,
          b: [['65000', '2.0', '0']],   // update type 0 = update
          a: [['65001', '0', '2']],      // update type 2 = delete
        },
      });

      assert.notStrictEqual(depthEmitted, null);
      assert.strictEqual(depthEmitted.type, 'update');
      assert.strictEqual(conn.book.getBestBid(), '65000');
      assert.strictEqual(conn.book.bids.get('65000'), '2.0'); // qty updated
      assert.strictEqual(conn.book.asks.has('65001'), false); // deleted
    });

    it('should buffer deltas during syncing and replay after snapshot', () => {
      const conn = new BybitConnector({});
      conn._ws = null;
      conn._setState('syncing');

      // Delta while syncing
      conn._handleDepth({
        topic: 'orderbook.1000.BTCUSDT',
        type: 'delta',
        ts: 1700000000100,
        data: { s: 'BTCUSDT', seq: 102, b: [['65000', '5.0']], a: [] },
      });
      assert.strictEqual(conn._ringBuf.length, 1);

      // Snapshot arrives — should replay ringbuf
      conn._handleDepth({
        topic: 'orderbook.1000.BTCUSDT',
        type: 'snapshot',
        ts: 1700000000000,
        data: { s: 'BTCUSDT', seq: 100, b: [['65000', '1.0']], a: [['65001', '1.0']] },
      });

      // After replay: bid should be updated to 5.0 (from ringbuf delta with seq 102 > 100)
      assert.strictEqual(conn.book.getBestBid(), '65000');
      assert.strictEqual(conn.book.bids.get('65000'), '5.0');
      assert.strictEqual(conn._ringBuf.length, 0); // ringbuf cleared
    });
  });

  describe('trade parsing', () => {
    it('should parse publicTrade data array', () => {
      const conn = createBybitConn();
      const tradesMsg = {
        topic: 'publicTrade.BTCUSDT',
        data: [
          { p: '65000.5', v: '1.2', S: 'Buy', T: 1700000000000, i: 'abc123' },
          { p: '65001.0', v: '0.5', S: 'Sell', T: 1700000000001, i: 'def456' },
        ],
      };

      const emitted = [];
      conn.on('trade', (ev) => emitted.push(ev));
      conn._handleTrade(tradesMsg);

      assert.strictEqual(emitted.length, 2);
      assert.strictEqual(emitted[0].price, 65000.5);
      assert.strictEqual(emitted[0].qty, 1.2);
      assert.strictEqual(emitted[0].side, 'buy');
      assert.strictEqual(emitted[1].side, 'sell');
      assert.strictEqual(emitted[0].tradeId, 'abc123');
    });
  });

  describe('_onMessage routing', () => {
    it('should route by topic', () => {
      const conn = createBybitConn();
      let depthCalled = false, tradeCalled = false;
      conn._handleDepth = () => { depthCalled = true; };
      conn._handleTrade = () => { tradeCalled = true; };

      conn._onMessage({ topic: 'orderbook.1000.BTCUSDT', type: 'snapshot', data: { b: [], a: [] } });
      assert.ok(depthCalled);

      conn._onMessage({ topic: 'publicTrade.BTCUSDT', data: [] });
      assert.ok(tradeCalled);
    });

    it('should ignore subscribe ack and pong', () => {
      const conn = createBybitConn();
      let msgHandled = false;
      conn._handleDepth = () => { msgHandled = true; };
      conn._handleTrade = () => { msgHandled = true; };

      conn._onMessage({ op: 'subscribe', success: true });
      conn._onMessage({ op: 'pong', req_id: '', ret_msg: '', conn_id: '' });
      assert.strictEqual(msgHandled, false);
    });
  });

  describe('liquidation parsing', () => {
    it('should emit liquidation row from _handleLiquidation with data array (allLiquidation)', () => {
      const conn = createBybitConn();
      let emitted = null;
      conn.on('liquidation', (ev) => { emitted = ev; });

      conn._handleLiquidation({
        topic: 'allLiquidation.BTCUSDT',
        data: [{
          price: '65000',
          side: 'Sell',
          size: '2.5',
          symbol: 'BTCUSDT',
          updatedTime: 1700000000000,
        }],
      });

      assert.notStrictEqual(emitted, null);
      assert.strictEqual(emitted.market, 'bybit_perp');
      assert.strictEqual(emitted.exchange, 'bybit');
      assert.strictEqual(emitted.symbol, 'BTCUSDT');
      assert.strictEqual(emitted.side, 'sell');
      assert.strictEqual(emitted.price, 65000);
      assert.strictEqual(emitted.qty, 2.5);
      assert.strictEqual(emitted.notional, 162500);
      assert.strictEqual(emitted.raw_type, 'liquidation');
    });

    it('should emit liquidation row with single object (backward compat)', () => {
      const conn = createBybitConn();
      let emitted = null;
      conn.on('liquidation', (ev) => { emitted = ev; });

      conn._handleLiquidation({
        topic: 'allLiquidation.BTCUSDT',
        data: { price: '66000', side: 'Buy', size: '1.0', symbol: 'BTCUSDT', updatedTime: 1700000000001 },
      });

      assert.notStrictEqual(emitted, null);
      assert.strictEqual(emitted.side, 'buy');
      assert.strictEqual(emitted.price, 66000);
      assert.strictEqual(emitted.qty, 1.0);
    });

    it('should accept alternative field names (p/v/S/s/T)', () => {
      const conn = createBybitConn();
      let emitted = null;
      conn.on('liquidation', (ev) => { emitted = ev; });

      conn._handleLiquidation({
        topic: 'allLiquidation.BTCUSDT',
        data: [{ p: '67000', v: '3.0', S: 'Sell', s: 'BTCUSDT', T: 1700000000002 }],
      });

      assert.notStrictEqual(emitted, null);
      assert.strictEqual(emitted.side, 'sell');
      assert.strictEqual(emitted.price, 67000);
      assert.strictEqual(emitted.qty, 3.0);
    });
  });
});

// ====== Kraken ======

describe('KrakenSpotConnector parser', () => {
  it('should parse book snapshot, trade updates, and route messages', () => {
    const conn = new KrakenSpotConnectorAlias({});
    conn._ws = null;
    conn._setState('running');

    let depth = null;
    let tradeCount = 0;
    conn.on('depth', (ev) => { depth = ev; });
    conn.on('trade', () => { tradeCount += 1; });

    conn._onMessage([42, { as: [['65001.0', '1.2', '1700000000.0']], bs: [['65000.0', '2.3', '1700000000.0']] }, 'book-10', 'XBT/USD']);
    assert.ok(depth);
    assert.strictEqual(depth.type, 'snapshot');
    assert.strictEqual(conn.book.getBestBid(), '65000.0');
    assert.strictEqual(conn.book.getBestAsk(), '65001.0');
    assert.strictEqual(calculateKrakenChecksum(conn.book), 2817500868);

    conn._onMessage([42, { a: [['65001.0', '1.0', '1700000001.0']], b: [['65000.0', '2.0', '1700000001.0']] }, 'book-10', 'XBT/USD']);
    assert.strictEqual(conn.book.bids.get('65000.0'), '2.0');
    assert.strictEqual(conn.book.asks.get('65001.0'), '1.0');

    conn._onMessage([42, [['65010.0', '0.1', '1700000002.0', 'b', 'l', '']], 'trade', 'XBT/USD']);
    assert.strictEqual(tradeCount, 1);
  });

  it('should convert Kraken trade timestamps from seconds to ms', () => {
    const conn = new KrakenSpotConnectorAlias({});
    conn._ws = null;
    conn._setState('running');

    let trade = null;
    conn.on('trade', (ev) => { trade = ev; });
    conn._onMessage([42, [['65010.0', '0.1', '1700000002.0', 'b', 'l', '']], 'trade', 'XBT/USD']);

    assert.ok(trade);
    assert.strictEqual(trade.ts, 1700000002000);
  });

  // Checksum validation is skipped when fewer than 10 levels on either side
  it('should skip checksum validation when fewer than 10 bid or ask levels', () => {
    const conn = new KrakenSpotConnectorAlias({});
    conn._ws = { close: () => {} };
    conn._scheduleReconnect = () => {};
    conn._setState('running');

    let error = null;
    let depthCount = 0;
    conn.on('error', (ev) => { error = ev; });
    conn.on('depth', () => { depthCount += 1; });

    // Snapshot with only 1 bid and 1 ask — not enough for checksum
    conn._onMessage([42, { as: [['65001.0', '1.2', '1700000000.0']], bs: [['65000.0', '2.3', '1700000000.0']] }, 'book-10', 'XBT/USD']);
    assert.strictEqual(depthCount, 1); // snapshot emitted
    // Update with c: '1' — checksum validation SKIPPED because <10 levels
    conn._onMessage([42, { a: [['65001.0', '1.0', '1700000001.0']], c: '1' }, 'book-10', 'XBT/USD']);

    assert.strictEqual(error, null);  // no error — checksum was not validated
    assert.strictEqual(depthCount, 2); // update still emitted
    assert.strictEqual(conn.book.isEmpty(), false); // book NOT cleared
    assert.strictEqual(conn.book.asks.get('65001.0'), '1.0'); // update applied
  });

  // Checksum validation RUNS when >=10 levels each side, but mismatch does NOT trigger reconnect storm
  it('should resync and clear book on checksum mismatch with >=10 levels', () => {
    const conn = new KrakenSpotConnectorAlias({});
    conn._ws = { close: () => {} };
    let reconnectCount = 0;
    conn._scheduleReconnect = () => { reconnectCount++; };
    conn._setState('running');

    let error = null;
    let depthCount = 0;
    conn.on('error', (ev) => { error = ev; });
    conn.on('depth', () => { depthCount += 1; });

    // Build a book with 10 bids and 10 asks
    const tenBids = [];
    const tenAsks = [];
    for (let i = 0; i < 10; i++) {
      tenBids.push([String(65000 - i * 10) + '.0', '1.0', '1700000000.0']);
      tenAsks.push([String(65001 + i * 10) + '.0', '1.0', '1700000000.0']);
    }
    conn._onMessage([42, { as: tenAsks, bs: tenBids }, 'book-10', 'XBT/USD']);
    assert.strictEqual(depthCount, 1); // snapshot

    // Now the checksum is warm and we have 10 levels each side.
    // Send an update with a wrong checksum (c: '1')
    conn._onMessage([42, { a: [['65001.0', '2.0']], c: '1' }, 'book-10', 'XBT/USD']);

    assert.ok(error);
    assert.match(error.message, /Kraken checksum mismatch/);
    assert.strictEqual(reconnectCount, 1);
    assert.strictEqual(conn.book.isEmpty(), true); // book cleared on resync
    assert.strictEqual(depthCount, 1); // mismatched update was not emitted
  });

  // data.c = null should not trigger checksum validation
  it('should skip checksum when data.c is null', () => {
    const conn = new KrakenSpotConnectorAlias({});
    conn._ws = { close: () => {} };
    conn._scheduleReconnect = () => {};
    conn._setState('running');

    let error = null;
    conn.on('error', (ev) => { error = ev; });

    // Snapshot with 10 bids and 10 asks
    const tenBids = [];
    const tenAsks = [];
    for (let i = 0; i < 10; i++) {
      tenBids.push([String(65000 - i * 10) + '.0', '1.0', '1700000000.0']);
      tenAsks.push([String(65001 + i * 10) + '.0', '1.0', '1700000000.0']);
    }
    conn._onMessage([42, { as: tenAsks, bs: tenBids }, 'book-10', 'XBT/USD']);

    // Update with c: null → seq should be undefined, checksum skipped
    conn._onMessage([42, { a: [['65001.0', '1.0']], c: null }, 'book-10', 'XBT/USD']);

    assert.strictEqual(error, null); // no error — checksum was not validated
    assert.strictEqual(conn.book.isEmpty(), false); // book intact
  });

  // Persistent mismatches should NOT cause infinite reconnect loop
  it('should not infinite-reconnect on persistent checksum mismatch', () => {
    const conn = new KrakenSpotConnectorAlias({});
    conn._ws = { close: () => {} };
    let reconnectCount = 0;
    conn._scheduleReconnect = () => { reconnectCount++; };
    conn._setState('running');

    // Build a book with 10 bids and 10 asks (enough for checksum validation)
    const tenBids = [];
    const tenAsks = [];
    for (let i = 0; i < 10; i++) {
      tenBids.push([String(65000 - i * 10) + '.0', '1.0', '1700000000.0']);
      tenAsks.push([String(65001 + i * 10) + '.0', '1.0', '1700000000.0']);
    }
    conn._onMessage([42, { as: tenAsks, bs: tenBids }, 'book-10', 'XBT/USD']);

    let errorCount = 0;
    conn.on('error', () => { errorCount++; });  // must attach listener to avoid unhandled error

    // Inject 5 consecutive mismatches
    for (let i = 0; i < 5; i++) {
      conn._onMessage([42, { a: [['65001.0', String(1.0 + i)]], c: String(i + 1000) }, 'book-10', 'XBT/USD']);
    }

    // reconnect should be scheduled once, and later mismatches must not storm
    assert.strictEqual(reconnectCount, 1);
    assert.strictEqual(errorCount, 1);  // one checksum mismatch routed via _handleSequenceGap
    assert.strictEqual(conn.book.isEmpty(), true); // book cleared on resync
  });

  it('should NOT reset checksum cooldown on snapshot — prevents reconnect storm', () => {
    const conn = new KrakenSpotConnectorAlias({});
    conn._ws = { close: () => {} };
    let reconnectCount = 0;
    conn._scheduleReconnect = () => { reconnectCount++; };
    conn._setState('running');

    const tenBids = [];
    const tenAsks = [];
    for (let i = 0; i < 10; i++) {
      tenBids.push([String(65000 - i * 10) + '.0', '1.0', '1700000000.0']);
      tenAsks.push([String(65001 + i * 10) + '.0', '1.0', '1700000000.0']);
    }

    let errorCount = 0;
    conn.on('error', () => { errorCount++; });

    conn._onMessage([42, { as: tenAsks, bs: tenBids }, 'book-10', 'XBT/USD']);
    conn._onMessage([42, { a: [['65001.0', '2.0']], c: '1' }, 'book-10', 'XBT/USD']);
    assert.strictEqual(reconnectCount, 1);
    assert.strictEqual(errorCount, 1);

    // A fresh snapshot must NOT reset the mismatch cooldown — otherwise every
    // reconnect would bypass the 30s suppression window and cause a reconnect storm.
    conn._handleBookSnapshot({ as: tenAsks, bs: tenBids });
    conn._onMessage([42, { a: [['65001.0', '3.0']], c: '2' }, 'book-10', 'XBT/USD']);
    // Cooldown still active → mismatch suppressed → no additional reconnect
    assert.strictEqual(reconnectCount, 1);
    assert.strictEqual(errorCount, 1);
  });

  it('should fall back to the first REST depth key when pair key is missing', async () => {
    const conn = new KrakenSpotConnectorAlias({ restPair: 'XBTUSD' });
    conn._ws = null;
    conn._setState('running');
    conn._waitForWsSnapshot = async () => { throw Object.assign(new Error('timeout'), { code: 'WS_SNAPSHOT_TIMEOUT' }); };
    conn._fetchSnapshot = async () => ({ result: { XXBTZUSD: { bids: [['65000.0', '1.0']], asks: [['65001.0', '1.1']] } } });
    conn._notifyWsSnapshotReceived = () => {};
    conn._finalizeWsSnapshotSync = () => {};

    await conn._syncBook();
    assert.strictEqual(conn.book.getBestBid(), '65000.0');
    assert.strictEqual(conn.book.getBestAsk(), '65001.0');
  });
});


describe('OkxConnector parser', () => {
  function createOkxConn() {
    const conn = new OkxConnector({});
    conn._ws = null;
    conn._setState('running');
    return conn;
  }

  describe('_preprocessRaw', () => {
    it('should send pong on ping string and return true', () => {
      const conn = createOkxConn();
      let sent = '';
      conn._ws = { send: (s) => { sent = s; } };

      const result = conn._preprocessRaw(Buffer.from('ping'));
      assert.strictEqual(result, true);
      assert.strictEqual(sent, 'pong');
    });

    it('should return false for non-ping data', () => {
      const conn = createOkxConn();
      conn._ws = { send: () => {} };
      assert.strictEqual(conn._preprocessRaw(Buffer.from('{"event":"subscribe"}')), false);
    });
  });

  describe('snapshot', () => {
    it('should apply full book snapshot from books channel', () => {
      const conn = createOkxConn();
      const msg = {
        arg: { channel: 'books', instId: 'BTC-USDT-SWAP' },
        action: 'snapshot',
        data: [{
          asks: [['65001.0', '0.8', '0', '1'], ['65002.0', '1.2', '0', '2']],
          bids: [['65000.0', '1.5', '0', '1'], ['64999.0', '2.0', '0', '2']],
          ts: '1700000000000',
          seqId: 500,
        }],
      };

      let emitted = null;
      conn.on('depth', (ev) => { emitted = ev; });
      conn._handleDepth(msg);

      assert.notStrictEqual(emitted, null);
      assert.strictEqual(emitted.type, 'snapshot');
      assert.strictEqual(emitted.seq, 500);
      assert.strictEqual(conn.book.getBestBid(), '65000.0');
      assert.strictEqual(conn.book.getBestAsk(), '65001.0');
    });
  });

  describe('update', () => {
    it('should apply level update and handle qty=0 as delete', () => {
      const conn = createOkxConn();
      conn.book.applySnapshot([['65000', '1']], [['65001', '1']], 100);
      conn._notifyWsSnapshotReceived(100);

      conn._handleDepth({
        arg: { channel: 'books', instId: 'BTC-USDT-SWAP' },
        action: 'update',
        data: [{
          asks: [['65001', '0', '0', '0']], // qty=0 → delete
          bids: [['65000', '3.0', '0', '0']],
          ts: '1700000000100',
          seqId: 101,
        }],
      });

      assert.strictEqual(conn.book.bids.get('65000'), '3.0');
      assert.strictEqual(conn.book.asks.has('65001'), false);
      assert.strictEqual(conn.book._lastSeq, 101);
    });
  });

  describe('trade parsing', () => {
    it('should parse each trade in data array', () => {
      const conn = createOkxConn();
      const tradesMsg = {
        arg: { channel: 'trades', instId: 'BTC-USDT-SWAP' },
        data: [
          { px: '65000.5', sz: '1.2', side: 'buy', ts: '1700000000000', tradeId: '111' },
          { px: '65001.0', sz: '0.3', side: 'sell', ts: '1700000000001', tradeId: '222' },
        ],
      };

      const emitted = [];
      conn.on('trade', (ev) => emitted.push(ev));
      conn._handleTrade(tradesMsg);

      assert.strictEqual(emitted.length, 2);
      assert.strictEqual(emitted[0].side, 'buy');
      assert.strictEqual(emitted[1].side, 'sell');
      assert.strictEqual(emitted[0].tradeId, '111');
      // qty = sz * contractValue (0.01 for perp): 1.2 * 0.01 = 0.012 BTC
      assert.strictEqual(emitted[0].qty, 1.2 * 0.01);
      assert.strictEqual(emitted[1].qty, 0.3 * 0.01);
    });
  });

  describe('_onMessage routing', () => {
    it('should ignore subscribe event', () => {
      const conn = createOkxConn();
      let called = false;
      conn._handleDepth = () => { called = true; };
      conn._onMessage({ event: 'subscribe', arg: { channel: 'books' } });
      assert.strictEqual(called, false);
    });

    it('should route books to _handleDepth, trades to _handleTrade', () => {
      const conn = createOkxConn();
      let depthCalled = false, tradeCalled = false;
      conn._handleDepth = () => { depthCalled = true; };
      conn._handleTrade = () => { tradeCalled = true; };

      conn._onMessage({ arg: { channel: 'books' }, action: 'snapshot', data: [{ asks: [], bids: [] }] });
      assert.ok(depthCalled);

      conn._onMessage({ arg: { channel: 'trades' }, data: [] });
      assert.ok(tradeCalled);
    });
  });

  describe('liquidation parsing', () => {
    it('should emit liquidation row from _handleLiquidation', () => {
      const conn = createOkxConn();
      let emitted = null;
      conn.on('liquidation', (ev) => { emitted = ev; });

      conn._handleLiquidation({
        arg: { channel: 'liquidation-orders', instType: 'SWAP', instId: 'BTC-USDT-SWAP' },
        data: [{
          instType: 'SWAP',
          instId: 'BTC-USDT-SWAP',
          side: 'sell',
          sz: '3.0',
          ts: '1700000000000',
          fillSz: '3.0',
          fillPx: '64950.0',
          tdMode: 'cross',
          uly: 'BTC-USD',
        }],
      });

      assert.notStrictEqual(emitted, null);
      assert.strictEqual(emitted.market, 'okx_perp');
      assert.strictEqual(emitted.exchange, 'okx');
      assert.strictEqual(emitted.symbol, 'BTC-USDT-SWAP');
      assert.strictEqual(emitted.side, 'sell');
      assert.strictEqual(emitted.price, 64950.0);
      assert.strictEqual(emitted.qty, 0.03);
      assert.strictEqual(emitted.notional, 1948.5);
      assert.strictEqual(emitted.raw_type, 'liquidation-orders');
    });
  });
});

describe('OkxConnector REST fallback seqId', () => {
  it('should use data[0].seqId when REST fallback activates', async () => {
    const conn = new OkxConnector({});
    conn._ws = null;

    // Mock WS snapshot timeout to trigger REST fallback
    conn._waitForWsSnapshot = async () => {
      const err = new Error('timeout');
      err.code = 'WS_SNAPSHOT_TIMEOUT';
      throw err;
    };

    // Mock REST snapshot with data[0].seqId
    conn._fetchSnapshot = async () => ({
      code: '0',
      data: [{
        asks: [['65001.0', '0.8', '0', '1']],
        bids: [['65000.0', '1.5', '0', '1']],
        seqId: 500,  // seqId inside data[0]
      }],
    });

    // Stub notification methods
    conn._notifyWsSnapshotReceived = () => {};
    conn._finalizeWsSnapshotSync = () => {};

    await conn._syncBook();

    // Book should be populated with snapshot data
    assert.strictEqual(conn.book.getBestBid(), '65000.0');
    assert.strictEqual(conn.book.getBestAsk(), '65001.0');
    // seqId should be 500 (from data[0].seqId, not snapshot.seqId)
    assert.strictEqual(conn.book._lastSeq, 500);
  });

  it('should fall back to snapshot.seqId when data[0].seqId is missing', async () => {
    const conn = new OkxConnector({});
    conn._ws = null;

    conn._waitForWsSnapshot = async () => {
      const err = new Error('timeout');
      err.code = 'WS_SNAPSHOT_TIMEOUT';
      throw err;
    };

    // REST snapshot without data[0].seqId, but with snapshot.seqId
    conn._fetchSnapshot = async () => ({
      code: '0',
      msg: '',
      seqId: 999,
      data: [{
        asks: [['65001.0', '0.8', '0', '1']],
        bids: [['65000.0', '1.5', '0', '1']],
      }],
    });

    conn._notifyWsSnapshotReceived = () => {};
    conn._finalizeWsSnapshotSync = () => {};

    await conn._syncBook();

    assert.strictEqual(conn.book.getBestBid(), '65000.0');
    assert.strictEqual(conn.book._lastSeq, 999);
  });

  it('should use 0 as last resort when no seqId available', async () => {
    const conn = new OkxConnector({});
    conn._ws = null;

    conn._waitForWsSnapshot = async () => {
      const err = new Error('timeout');
      err.code = 'WS_SNAPSHOT_TIMEOUT';
      throw err;
    };

    conn._fetchSnapshot = async () => ({
      code: '0',
      data: [{
        asks: [['65001.0', '0.8', '0', '1']],
        bids: [['65000.0', '1.5', '0', '1']],
      }],
    });

    conn._notifyWsSnapshotReceived = () => {};
    conn._finalizeWsSnapshotSync = () => {};

    await conn._syncBook();

    assert.strictEqual(conn.book.getBestBid(), '65000.0');
    assert.strictEqual(conn.book._lastSeq, 0);
  });
});

describe('CoinbaseConnector parser', () => {
  function createCoinbaseConn() {
    const conn = new CoinbaseConnector({});
    conn._ws = null;
    conn._setState('running');
    return conn;
  }

  describe('snapshot', () => {
    it('should apply full book from l2_data snapshot event', () => {
      const conn = createCoinbaseConn();
      const msg = {
        channel: 'l2_data',
        sequence_num: 1000,
        events: [{
          type: 'snapshot',
          product_id: 'BTC-USD',
          updates: [
            { side: 'bid', event_time: '2026-06-05T00:00:00.000Z', price_level: '65000.00', new_quantity: '1.5' },
            { side: 'bid', event_time: '2026-06-05T00:00:00.000Z', price_level: '64999.00', new_quantity: '2.0' },
            { side: 'ask', event_time: '2026-06-05T00:00:00.000Z', price_level: '65001.00', new_quantity: '0.8' },
            { side: 'ask', event_time: '2026-06-05T00:00:00.000Z', price_level: '65002.00', new_quantity: '1.2' },
          ],
        }],
      };

      let emitted = null;
      conn.on('depth', (ev) => { emitted = ev; });
      conn._handleDepth(msg);

      assert.notStrictEqual(emitted, null);
      assert.strictEqual(emitted.type, 'snapshot');
      assert.strictEqual(emitted.seq, 1000);
      assert.strictEqual(emitted.bids.length, 2);
      assert.strictEqual(emitted.asks.length, 2);
      assert.strictEqual(conn.book.getBestBid(), '65000.00');
      assert.strictEqual(conn.book.getBestAsk(), '65001.00');
    });

    it('should treat side=offer as ask in snapshot', () => {
      const conn = createCoinbaseConn();
      const msg = {
        channel: 'l2_data',
        sequence_num: 1000,
        events: [{
          type: 'snapshot',
          product_id: 'BTC-USD',
          updates: [
            { side: 'bid', price_level: '65000.00', new_quantity: '1.0' },
            { side: 'offer', price_level: '65001.00', new_quantity: '2.0' },
          ],
        }],
      };

      conn._handleDepth(msg);
      assert.strictEqual(conn.book.getBestBid(), '65000.00');
      assert.strictEqual(conn.book.getBestAsk(), '65001.00');
      assert.strictEqual(conn.book.asks.size, 1);
    });
  });

  describe('l2_data update', () => {
    it('should apply updates to book using price_level / new_quantity', () => {
      const conn = createCoinbaseConn();
      conn.book.applySnapshot([['65000', '1']], [['65001', '1']], 100);
      conn._notifyWsSnapshotReceived(100);

      conn._handleDepth({
        channel: 'l2_data',
        sequence_num: 101,
        events: [{
          type: 'update',
          product_id: 'BTC-USD',
          updates: [
            { side: 'bid', event_time: '2026-06-05T00:00:01.000Z', price_level: '65000.00', new_quantity: '3.0' },
            { side: 'ask', event_time: '2026-06-05T00:00:01.000Z', price_level: '65001.00', new_quantity: '0' },
          ],
        }],
      });

      assert.strictEqual(conn.book.bids.get('65000.00'), '3.0');
      assert.strictEqual(conn.book.asks.has('65001.00'), false);
      assert.strictEqual(conn.book._lastSeq, 101);
    });

    it('should treat side=offer as ask in update', () => {
      const conn = createCoinbaseConn();
      conn.book.applySnapshot([['65000', '1']], [['65001', '1']], 100);
      conn._notifyWsSnapshotReceived(100);

      conn._handleDepth({
        channel: 'l2_data',
        sequence_num: 101,
        events: [{
          type: 'update',
          product_id: 'BTC-USD',
          updates: [
            { side: 'offer', price_level: '65001.00', new_quantity: '5.0' },
          ],
        }],
      });

      assert.strictEqual(conn.book.asks.get('65001.00'), '5.0');
      assert.strictEqual(conn.book._lastSeq, 101);
    });
  });

  describe('market_trades parsing', () => {
    it('should parse Advanced Trade market_trades events', () => {
      const conn = createCoinbaseConn();
      const emitted = [];
      conn.on('trade', (ev) => emitted.push(ev));

      conn._handleTrade({
        channel: 'market_trades',
        sequence_num: 0,
        events: [{
          type: 'snapshot',
          trades: [{
            product_id: 'BTC-USD',
            trade_id: '999',
            price: '65000.50',
            size: '1.5',
            time: '2026-06-05T00:00:00.500Z',
            side: 'BUY',
          }],
        }],
      });

      assert.strictEqual(emitted.length, 1);
      assert.strictEqual(emitted[0].price, 65000.50);
      assert.strictEqual(emitted[0].qty, 1.5);
      assert.strictEqual(emitted[0].side, 'sell');
      assert.strictEqual(emitted[0].tradeId, '999');
    });
  });

  describe('_onMessage routing', () => {
    it('should route l2_data to _handleDepth, market_trades to _handleTrade', () => {
      const conn = createCoinbaseConn();
      let depthCalled = false, tradeCalled = false;
      conn._handleDepth = () => { depthCalled = true; };
      conn._handleTrade = () => { tradeCalled = true; };

      conn._onMessage({ channel: 'l2_data', sequence_num: 1, events: [{ type: 'snapshot', updates: [] }] });
      assert.ok(depthCalled);

      conn._onMessage({ channel: 'market_trades', events: [{ trades: [] }] });
      assert.ok(tradeCalled);
    });

    it('should ignore subscriptions channel', () => {
      const conn = createCoinbaseConn();
      let depthCalled = false, tradeCalled = false;
      conn._handleDepth = () => { depthCalled = true; };
      conn._handleTrade = () => { tradeCalled = true; };

      conn._onMessage({ channel: 'subscriptions', products: [{ product_id: 'BTC-USD' }] });
      assert.strictEqual(depthCalled, false);
      assert.strictEqual(tradeCalled, false);
    });

    it('should emit error on type=error messages', () => {
      const conn = createCoinbaseConn();
      let errorEmitted = null;
      conn.on('error', (ev) => { errorEmitted = ev; });

      conn._onMessage({ type: 'error', message: 'Failed to subscribe', reason: 'requires auth' });

      assert.notStrictEqual(errorEmitted, null);
      assert.ok(errorEmitted.message.includes('Failed'));
    });
  });
});

// ====== Hyperliquid ======

describe('HyperliquidConnector parser', () => {
  function createHyperliquidConn() {
    const conn = new HyperliquidConnector({});
    conn._ws = null;
    conn._setState('running');
    return conn;
  }

  describe('l2Book (depth snapshot)', () => {
    it('should apply full book from l2Book message (via snapshot-diff)', () => {
      const conn = createHyperliquidConn();
      const msg = {
        channel: 'l2Book',
        data: {
          coin: 'BTC',
          levels: [
            // bids (level[0])
            [{ px: 65000.0, sz: 1.5, n: 2 }, { px: 64999.0, sz: 2.0, n: 1 }],
            // asks (level[1])
            [{ px: 65001.0, sz: 0.8, n: 3 }, { px: 65002.0, sz: 1.2, n: 1 }],
          ],
          time: 1700000000000,
        },
      };

      let emitted = null;
      conn.on('depth', (ev) => { emitted = ev; });
      conn._handleDepth(msg);

      assert.notStrictEqual(emitted, null);
      assert.strictEqual(emitted.type, 'update', 'snapshot-diff emits update type');
      // Book was empty, so all levels appear as changes
      assert.strictEqual(emitted.bids.length, 2);
      assert.strictEqual(emitted.asks.length, 2);
      assert.strictEqual(conn.book.getBestBid(), '65000');
      assert.strictEqual(conn.book.getBestAsk(), '65001');
    });

    it('should set wsSnapshotReceived on first l2Book', () => {
      const conn = new HyperliquidConnector({});
      conn._ws = null;

      assert.strictEqual(conn._wsSnapshotReceived, false);

      conn._handleDepth({
        channel: 'l2Book',
        data: { levels: [[], []], time: 1700000000000 },
      });

      assert.strictEqual(conn._wsSnapshotReceived, true);
    });
  });

  describe('trade parsing', () => {
    it('should parse trades data array', () => {
      const conn = createHyperliquidConn();
      const msg = {
        channel: 'trades',
        data: [
          { px: 65000.5, sz: 1.2, side: 'B', time: 1700000000000, tid: '0xabc' },
          { px: 65001.0, sz: 0.3, side: 'A', time: 1700000000001, tid: '0xdef' },
        ],
      };

      const emitted = [];
      conn.on('trade', (ev) => emitted.push(ev));
      conn._handleTrade(msg);

      assert.strictEqual(emitted.length, 2);
      assert.strictEqual(emitted[0].side, 'buy');   // B → buy
      assert.strictEqual(emitted[1].side, 'sell');  // A → sell
      assert.strictEqual(emitted[0].price, 65000.5);
      assert.strictEqual(emitted[0].qty, 1.2);
      assert.strictEqual(emitted[0].tradeId, '0xabc');
    });

    it('normalizes nanosecond trade timestamps to milliseconds', () => {
      const conn = createHyperliquidConn();
      let emitted = [];
      conn.on('trade', (ev) => emitted.push(ev));

      conn._handleTrade({
        channel: 'trades',
        data: [{ px: 65000, sz: 1.0, side: 'B', time: 1750000000000000000, tid: 'ns1' }],
      });

      assert.strictEqual(emitted.length, 1);
      assert.strictEqual(emitted[0].ts, 1750000000000);
    });

    it('normalizes nanosecond depth timestamps to milliseconds', () => {
      const conn = createHyperliquidConn();
      let emitted = [];
      conn.on('depth', (ev) => emitted.push(ev));

      conn._handleDepth({
        channel: 'l2Book',
        data: {
          coin: 'BTC',
          levels: [
            [{ px: 65000, sz: 1.0, n: 1 }],
            [{ px: 65001, sz: 0.5, n: 1 }],
          ],
          time: 1750000000000000000,
        },
      });

      assert.strictEqual(emitted.length, 1);
      assert.strictEqual(emitted[0].ts, 1750000000000);
    });

    it('passes through millisecond timestamps without modification', () => {
      const conn = createHyperliquidConn();
      let emitted = [];
      conn.on('trade', (ev) => emitted.push(ev));

      conn._handleTrade({
        channel: 'trades',
        data: [{ px: 65000, sz: 1.0, side: 'B', time: 1700000000000, tid: 'ms1' }],
      });

      assert.strictEqual(emitted.length, 1);
      assert.strictEqual(emitted[0].ts, 1700000000000);
    });
  });

  describe('_onMessage routing', () => {
    it('should route l2Book to _handleDepth, trades to _handleTrade', () => {
      const conn = createHyperliquidConn();
      let depthCalled = false, tradeCalled = false;
      conn._handleDepth = () => { depthCalled = true; };
      conn._handleTrade = () => { tradeCalled = true; };

      conn._onMessage({ channel: 'l2Book', data: { levels: [[], []] } });
      assert.ok(depthCalled);

      conn._onMessage({ channel: 'trades', data: [] });
      assert.ok(tradeCalled);
    });
  });
});

// ====== Main CONNECTOR_CLASSES smoke ======

describe('CONNECTOR_CLASSES instantiation', () => {
  const CONNECTOR_CLASSES = {
    bybit_perp: BybitConnector,
    okx_perp: OkxConnector,
    coinbase_spot: CoinbaseConnector,
    hyperliquid_perp: HyperliquidConnector,
    binance_spot_usdc: BinanceSpotUsdcConnector,
  };

  for (const [market, Cls] of Object.entries(CONNECTOR_CLASSES)) {
    it(`should instantiate ${market} without error`, () => {
      const conn = new Cls({});
      assert.ok(conn);
      assert.strictEqual(conn.market, market);
      assert.ok(conn.book);
    });
  }
});

describe('Market alias connectors', () => {
  it('should wire market keys and books for new Binance markets', () => {
    const coinm = new BinanceCoinmPerpConnector({});
    const btcusdc = new BinancePerpBtcusdcConnector({});

    assert.strictEqual(coinm.market, 'binance_coinm_perp');
    assert.strictEqual(coinm.book.market, 'binance_coinm_perp');
    assert.ok(coinm.wsUrl.includes('dstream.binance.com'));
    assert.ok(coinm.restUrl.includes('dapi.binance.com'));

    assert.strictEqual(btcusdc.market, 'binance_perp_btcusdc');
    assert.strictEqual(btcusdc.book.market, 'binance_perp_btcusdc');
    assert.ok(btcusdc.wsUrl.includes('fstream.binance.com'));
    assert.ok(btcusdc.restUrl.includes('BTCUSDC'));
  });

  it('should wire market keys for spot aliases', () => {
    const bybit = new BybitSpotConnector({});
    const okx = new OkxSpotConnector({});

    assert.strictEqual(bybit.market, 'bybit_spot');
    assert.strictEqual(bybit.book.market, 'bybit_spot');
    assert.ok(bybit.wsUrl.includes('/spot'));
    assert.ok(bybit.restUrl.includes('category=spot'));

    assert.strictEqual(okx.market, 'okx_spot');
    assert.strictEqual(okx.book.market, 'okx_spot');
    assert.ok(okx.restUrl.includes('instId=BTC-USDT'));
  });

  it('should emit OkxSpotConnector trade qty as base BTC (contractValue=1)', () => {
    const okx = new OkxSpotConnector({});
    okx._ws = null;
    okx._setState('running');

    const emitted = [];
    okx.on('trade', (ev) => emitted.push(ev));
    okx._handleTrade({
      arg: { channel: 'trades', instId: 'BTC-USDT' },
      data: [
        { px: '65000', sz: '1.5', side: 'buy', ts: '1700000000000', tradeId: 's1' },
        { px: '65001', sz: '0.8', side: 'sell', ts: '1700000000001', tradeId: 's2' },
      ],
    });

    assert.strictEqual(emitted.length, 2);
    // spot: contractValue=1, qty = sz * 1 = sz → base BTC
    assert.strictEqual(emitted[0].qty, 1.5);
    assert.strictEqual(emitted[1].qty, 0.8);
    assert.strictEqual(emitted[0].market, 'okx_spot');
  });

  it('should emit BinanceCoinmPerpConnector trade qty as base BTC (contracts*100/price)', () => {
    const coinm = new BinanceCoinmPerpConnector({});
    coinm._ws = null;
    coinm._setState('running');

    const emitted = [];
    coinm.on('trade', (ev) => emitted.push(ev));
    coinm._handleTrade({ p: '65000', q: '20', m: false, T: 1700000000000, t: 123 }); // buy 20 contracts@65000
    coinm._handleTrade({ p: '65000', q: '10', m: true, T: 1700000000001, t: 124 });  // sell 10 contracts@65000

    assert.strictEqual(emitted.length, 2);
    // COIN-M: qty = contracts * 100 / price
    assert.strictEqual(emitted[0].qty, 20 * 100 / 65000); // ≈0.030769...
    assert.strictEqual(emitted[0].side, 'buy');
    assert.strictEqual(emitted[1].qty, 10 * 100 / 65000); // ≈0.015384...
    assert.strictEqual(emitted[1].side, 'sell');
    assert.strictEqual(emitted[1].tradeId, '124');
  });

  it('should skip BinanceCoinmPerpConnector trade when price or qty is zero', () => {
    const coinm = new BinanceCoinmPerpConnector({});
    coinm._ws = null;
    coinm._setState('running');

    const emitted = [];
    coinm.on('trade', (ev) => emitted.push(ev));
    coinm._handleTrade({ p: '0', q: '10', m: false, T: 1700000000000, t: 1 });
    coinm._handleTrade({ p: '65000', q: '0', m: false, T: 1700000000001, t: 2 });

    assert.strictEqual(emitted.length, 0);
  });

  it('should allow BinanceCoinmPerpConnector sync with empty ring buffer only for low-volume COIN-M', () => {
    const coinm = new BinanceCoinmPerpConnector({});
    coinm._ringBuf = [];
    assert.ok(coinm._validateSync({ lastUpdateId: 100 }));
  });

  it('should let COIN-M snapshot-only sync enter running and bridge first live diff', async () => {
    const coinm = new BinanceCoinmPerpConnector({});
    coinm._ws = null;
    coinm._fetchSnapshot = async () => ({
      lastUpdateId: 100,
      bids: [['65000', '1']],
      asks: [['65001', '1']],
    });
    coinm._scheduleReconnect = () => { coinm._setState('reconnecting'); };

    await coinm._syncBook();
    assert.strictEqual(coinm.getState(), 'running');
    assert.strictEqual(coinm._firstRunningDiff, true);

    coinm._handleDepth({ U: 95, u: 105, pu: -1, b: [], a: [], E: 1700000000000 });
    assert.strictEqual(coinm.book._lastSeq, 105);
    assert.strictEqual(coinm._firstRunningDiff, false);

    let err = null;
    coinm.on('error', (ev) => { err = ev; });
    coinm._handleDepth({ U: 106, u: 106, pu: 999, b: [], a: [], E: 1700000000001 });
    assert.match(err.message, /Perp depth pu mismatch/);
    assert.strictEqual(coinm.getState(), 'reconnecting');
  });

  it('should let BTCUSDC perp snapshot-only sync enter running and bridge first live diff', async () => {
    const btcusdc = new BinancePerpBtcusdcConnector({});
    btcusdc._ws = null;
    btcusdc._fetchSnapshot = async () => ({
      lastUpdateId: 200,
      bids: [['65000', '1']],
      asks: [['65001', '1']],
    });
    btcusdc._scheduleReconnect = () => { btcusdc._setState('reconnecting'); };

    await btcusdc._syncBook();
    assert.strictEqual(btcusdc.getState(), 'running');
    assert.strictEqual(btcusdc._firstRunningDiff, true);

    btcusdc._handleDepth({ U: 190, u: 205, pu: -1, b: [], a: [], E: 1700000000000 });
    assert.strictEqual(btcusdc.book._lastSeq, 205);
    assert.strictEqual(btcusdc._firstRunningDiff, false);

    let err = null;
    btcusdc.on('error', (ev) => { err = ev; });
    btcusdc._handleDepth({ U: 206, u: 206, pu: 999, b: [], a: [], E: 1700000000001 });
    assert.match(err.message, /Perp depth pu mismatch/);
    assert.strictEqual(btcusdc.getState(), 'reconnecting');
  });
});
