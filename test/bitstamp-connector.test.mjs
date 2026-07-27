// test/bitstamp-connector.test.mjs — Bitstamp BTC/USD connector parser tests

import { describe, it } from 'node:test';
import assert from 'node:assert';
import { BitstampConnector } from '../lib/bitstamp-connector.mjs';

describe('BitstampConnector parser', () => {
  function createConn() {
    const conn = new BitstampConnector({});
    conn._ws = { send: () => {} };
    conn._setState('running');
    return conn;
  }

  it('should send live_trades_btcusd and diff_order_book_btcusd subscribe frames', () => {
    const conn = new BitstampConnector({});
    const sent = [];
    conn._ws = { send: (msg) => { sent.push(msg); } };
    conn.subscribe();
    assert.strictEqual(sent.length, 2);
    const channels = sent.map((s) => JSON.parse(s).data.channel).sort();
    assert.deepStrictEqual(channels, ['diff_order_book_btcusd', 'live_trades_btcusd']);
  });

  it('should parse trade payloads and normalize buy/sell side', () => {
    const conn = createConn();
    let emitted = null;
    conn.on('trade', (ev) => { emitted = ev; });

    conn._onMessage({
      event: 'trade',
      channel: 'live_trades_btcusd',
      data: {
        id: 123456,
        price: '65000.12',
        amount: '0.25',
        type: 0,
        microtimestamp: '1700000000123456',
      },
    });

    assert.ok(emitted);
    assert.strictEqual(emitted.market, 'bitstamp_spot');
    assert.strictEqual(emitted.price, 65000.12);
    assert.strictEqual(emitted.qty, 0.25);
    assert.strictEqual(emitted.side, 'buy');
    assert.strictEqual(emitted.ts, 1700000000123);
    assert.strictEqual(emitted.tradeId, '123456');
  });

  it('should ignore subscription ack messages', () => {
    const conn = createConn();
    let emitted = false;
    conn.on('trade', () => { emitted = true; });

    conn._onMessage({
      event: 'bts:subscription_succeeded',
      channel: 'live_trades_btcusd',
      data: { channel: 'live_trades_btcusd' },
    });

    assert.strictEqual(emitted, false);
  });

  it('seeds a complete book from REST and replays buffered WS diffs', async () => {
    const originalFetch = globalThis.fetch;
    globalThis.fetch = async () => ({
      ok: true,
      json: async () => ({
        bids: [['65000', '1.0']],
        asks: [['65001', '2.0']],
      }),
    });
    try {
      const conn = new BitstampConnector({ restUrl: 'https://example.test/book' });
      conn._setState('connected');
      const emitted = [];
      conn.on('depth', (event) => emitted.push(event));

      const sync = conn._syncBook();
      conn._onMessage({
        event: 'data',
        channel: 'diff_order_book_btcusd',
        data: { bids: [['65000', '1.5']], asks: [['65001', '0']], microtimestamp: '1700000000' },
      });
      await sync;

      assert.strictEqual(emitted[0].type, 'snapshot');
      assert.strictEqual(emitted[0].snapshot_origin, 'rest_sync');
      assert.strictEqual(emitted[1].type, 'update');
      assert.strictEqual(conn.book.bids.get('65000'), '1.5');
      assert.strictEqual(conn.book.asks.has('65001'), false);
      assert.strictEqual(conn.getState(), 'running');
    } finally {
      globalThis.fetch = originalFetch;
    }
  });
});
