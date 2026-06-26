// test/additional-markets-connector.test.mjs — additional BTC market parser tests

import { describe, it } from 'node:test';
import assert from 'node:assert';
import { CryptoComConnector } from '../lib/crypto-com-connector.mjs';
import { BitfinexConnector } from '../lib/bitfinex-connector.mjs';
import { BitmexConnector } from '../lib/bitmex-connector.mjs';
import { CoinbaseInternationalConnector } from '../lib/coinbase-international-connector.mjs';

const approx = (actual, expected, epsilon = 1e-12) => {
  assert.ok(Math.abs(actual - expected) <= epsilon, `${actual} != ${expected}`);
};

const createConn = (Ctor, config = {}) => {
  const conn = new Ctor(config);
  conn._ws = { send: () => {} };
  conn._setState('running');
  return conn;
};

describe('CryptoComConnector parser', () => {
  it('sends the trade subscribe frame', () => {
    const conn = new CryptoComConnector({});
    let sent = [];
    conn._ws = { send: (msg) => { sent.push(msg); } };
    conn.subscribe();
    assert.strictEqual(sent.length, 1);
    const parsed = JSON.parse(sent[0]);
    assert.strictEqual(parsed.method, 'subscribe');
    assert.ok(parsed.params.channels.includes('trade.BTC_USD'));
    assert.ok(parsed.params.channels.includes('book.BTC_USD.10'));
  });

  it('parses BUY trades from the public payload', () => {
    const conn = createConn(CryptoComConnector);
    let emitted = null;
    conn.on('trade', (ev) => { emitted = ev; });

    conn._onMessage({
      channel: 'trade.BTC_USD',
      data: [{ p: '65000.1', q: '0.2', s: 'BUY', t: 1700000000123, tid: 'c1' }],
    });

    assert.ok(emitted);
    assert.strictEqual(emitted.market, 'crypto_com_spot');
    assert.strictEqual(emitted.price, 65000.1);
    assert.strictEqual(emitted.qty, 0.2);
    assert.strictEqual(emitted.side, 'buy');
    assert.strictEqual(emitted.ts, 1700000000123);
    assert.strictEqual(emitted.tradeId, 'c1');
  });

  it('handles book push updates (id=-1) as depth updates (via snapshot-diff)', () => {
    const conn = createConn(CryptoComConnector);
    const emitted = [];
    conn.on('depth', (ev) => emitted.push(ev));

    // Book push (id=-1, full snapshot → diff computed)
    conn._onMessage({
      id: -1,
      channel: 'book.BTC_USD.10',
      data: [{
        bids: [['65000.1', '1.5'], ['64999.0', '2.0']],
        asks: [['65001.0', '0.8'], ['65002.0', '1.2']],
        t: 1700000000000,
      }],
    });

    assert.strictEqual(emitted.length, 1, 'should emit depth for book push');
    assert.strictEqual(emitted[0].type, 'update', 'snapshot-diff emits update type');
    // Book was empty before, so all 4 levels are changes
    assert.strictEqual(emitted[0].bids.length, 2);
    assert.strictEqual(emitted[0].asks.length, 2);

    // Subscribe ACK should NOT trigger depth emit (separate path)
    conn._onMessage({
      id: 1,
      method: 'subscribe',
      code: 0,
      result: { channel: 'book.BTC_USD.10' },
    });
    // Subscribe ACK: no depth emitted (the actual data comes in the result.data path)
    assert.strictEqual(emitted.length, 1, 'subscribe ACK should not produce duplicate depth');
  });
});

describe('BitfinexConnector parser', () => {
  it('sends the trades subscribe frame', () => {
    const conn = new BitfinexConnector({});
    const sent = [];
    conn._ws = { send: (msg) => { sent.push(msg); } };
    conn.subscribe();
    assert.strictEqual(sent.length, 2);
    const parsed0 = JSON.parse(sent[0]);
    const parsed1 = JSON.parse(sent[1]);
    assert.strictEqual(parsed0.event, 'subscribe');
    assert.ok((parsed0.channel === 'trades' && parsed1.channel === 'book') ||
              (parsed0.channel === 'book' && parsed1.channel === 'trades'));
    assert.strictEqual(parsed0.symbol || parsed1.symbol, 'tBTCUSD');
  });

  it('parses te/tu trade arrays and normalizes inverse amount sign', () => {
    const conn = createConn(BitfinexConnector);
    const emitted = [];
    conn.on('trade', (ev) => emitted.push(ev));

    conn._onMessage([42, 'tu', [999, 1700000000456, -2, 65000]]);

    assert.strictEqual(emitted.length, 1);
    assert.strictEqual(emitted[0].market, 'bitfinex_spot');
    assert.strictEqual(emitted[0].price, 65000);
    assert.strictEqual(emitted[0].qty, 2);
    assert.strictEqual(emitted[0].side, 'sell');
    assert.strictEqual(emitted[0].ts, 1700000000456);
    assert.strictEqual(emitted[0].tradeId, '999');
  });

  it('skips te (unconfirmed) and does not double-emit same trade id', () => {
    const conn = createConn(BitfinexConnector);
    const emitted = [];
    conn.on('trade', (ev) => emitted.push(ev));

    // Send 'te' first — should be silently skipped
    conn._onMessage([42, 'te', [999, 1700000000456, -2, 65000]]);
    assert.strictEqual(emitted.length, 0, 'te should be skipped');

    // Send 'tu' for the SAME trade — should emit once
    conn._onMessage([42, 'tu', [999, 1700000000456, -2, 65000]]);
    assert.strictEqual(emitted.length, 1, 'tu should emit once');
    assert.strictEqual(emitted[0].tradeId, '999');
    assert.strictEqual(emitted[0].side, 'sell');
  });
});

describe('BitmexConnector parser', () => {
  it('uses the embedded trade subscription URL', () => {
    const conn = new BitmexConnector({});
    assert.ok(conn.wsUrl.includes('trade:XBTUSD'));
  });

  it('parses trade rows and normalizes XBTUSD contract size', () => {
    const conn = createConn(BitmexConnector);
    const emitted = [];
    conn.on('trade', (ev) => emitted.push(ev));

    conn._onMessage({
      table: 'trade',
      data: [{ price: 50000, size: 50, side: 'Buy', timestamp: '2026-06-05T00:00:00.000Z', trdMatchID: 'mx1' }],
    });

    assert.strictEqual(emitted.length, 1);
    assert.strictEqual(emitted[0].market, 'bitmex_perp');
    approx(emitted[0].qty, 0.001);
    assert.strictEqual(emitted[0].side, 'buy');
    assert.strictEqual(emitted[0].tradeId, 'mx1');
  });

  it('emits BitMEX orderBookL2 updates as changed levels only', () => {
    const conn = createConn(BitmexConnector);
    const emitted = [];
    conn.on('depth', (ev) => emitted.push(ev));

    conn._onMessage({
      table: 'orderBookL2',
      action: 'partial',
      data: [
        { id: 1, side: 'Buy', price: 50000, size: 10 },
        { id: 5, side: 'Buy', price: 50000, size: 2 },
        { id: 2, side: 'Buy', price: 49990, size: 20 },
        { id: 3, side: 'Sell', price: 50010, size: 30 },
      ],
    });

    assert.strictEqual(emitted.length, 1);
    assert.strictEqual(emitted[0].type, 'snapshot');
    assert.strictEqual(emitted[0].bids.length, 2);
    assert.strictEqual(emitted[0].asks.length, 1);

    conn._onMessage({
      table: 'orderBookL2',
      action: 'update',
      data: [{ id: 1, size: 15 }],
    });

    assert.strictEqual(emitted.length, 2);
    assert.strictEqual(emitted[1].type, 'update');
    assert.deepStrictEqual(emitted[1].bids, [['50000', '17']]);
    assert.deepStrictEqual(emitted[1].asks, []);

    conn._onMessage({
      table: 'orderBookL2',
      action: 'update',
      data: [{ id: 5, price: 49980, size: 4 }],
    });

    assert.strictEqual(emitted.length, 3);
    assert.strictEqual(emitted[2].type, 'update');
    assert.deepStrictEqual(emitted[2].bids, [['50000', '15'], ['49980', '4']]);
    assert.deepStrictEqual(emitted[2].asks, []);

    conn._onMessage({
      table: 'orderBookL2',
      action: 'insert',
      data: [{ id: 4, side: 'Sell', price: 50020, size: 40 }],
    });

    assert.strictEqual(emitted.length, 4);
    assert.deepStrictEqual(emitted[3].bids, []);
    assert.deepStrictEqual(emitted[3].asks, [['50020', '40']]);

    conn._onMessage({
      table: 'orderBookL2',
      action: 'delete',
      data: [{ id: 3 }],
    });

    assert.strictEqual(emitted.length, 5);
    assert.deepStrictEqual(emitted[4].bids, []);
    assert.deepStrictEqual(emitted[4].asks, [['50010', '0']]);

    const snap = conn.book.toSnapshot();
    assert.deepStrictEqual(snap.bids.slice(0, 3), [['50000', '15'], ['49990', '20'], ['49980', '4']]);
    assert.deepStrictEqual(snap.asks, [['50020', '40']]);
  });
});

describe('CoinbaseInternationalConnector parser', () => {
  it('requires auth fields before subscribe', () => {
    const conn = new CoinbaseInternationalConnector({});
    let error = null;
    let sent = false;
    conn._ws = { send: () => { sent = true; } };
    conn.on('error', (ev) => { error = ev; });
    conn.subscribe();
    assert.ok(error);
    assert.strictEqual(sent, false);
  });

  it('sends the authenticated MATCH subscribe frame', () => {
    const conn = new CoinbaseInternationalConnector({
      auth: { key: 'k', passphrase: 'p', signature: 'sig', time: '1683730727' },
    });
    let sent = null;
    conn._ws = { send: (msg) => { sent = msg; } };
    conn.subscribe();
    const parsed = JSON.parse(sent);
    assert.strictEqual(parsed.type, 'SUBSCRIBE');
    assert.deepStrictEqual(parsed.product_ids, ['BTC-PERP']);
    assert.deepStrictEqual(parsed.channels, ['MATCH']);
  });

  it('parses MATCH trades and honors aggressor side', () => {
    const conn = createConn(CoinbaseInternationalConnector);
    let emitted = null;
    conn.on('trade', (ev) => { emitted = ev; });

    conn._onMessage({
      channel: 'MATCH',
      events: [{ trades: [{ price: '65012.25', size: '0.42', side: 'SELL', time: '2026-06-05T00:00:00.000Z', trade_id: 'cbi1' }] }],
    });

    assert.ok(emitted);
    assert.strictEqual(emitted.market, 'coinbase_international_perp');
    assert.strictEqual(emitted.price, 65012.25);
    assert.strictEqual(emitted.qty, 0.42);
    assert.strictEqual(emitted.side, 'sell');
    assert.strictEqual(emitted.tradeId, 'cbi1');
  });
});
