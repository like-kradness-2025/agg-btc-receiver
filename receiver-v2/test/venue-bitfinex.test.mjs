import { test } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

import { createBitfinexAdapter } from '../src/ingest/venues/bitfinex.mjs';
import { createReceiveConnection } from '../src/ingest/connection.mjs';
import { makeEnvelope } from '../src/envelope.mjs';
import { openDurability } from '../src/durability.mjs';
import { openBook } from '../src/book/state.mjs';

const adapter = createBitfinexAdapter({ market: 'bitfinex_spot', symbol: 'tBTCUSD' });
const parse = (payload) => adapter.parse(JSON.stringify(payload));
const changesOf = (payload) => adapter.changesFor({ raw: Buffer.from(JSON.stringify(payload)) });

test('the subscribe payloads carry the documented fields', () => {
  const [conf, book] = adapter.subscribeMessages().map((message) => JSON.parse(message));
  assert.deepEqual(conf, { event: 'conf', flags: 196608 });
  assert.deepEqual(book, {
    event: 'subscribe',
    channel: 'book',
    symbol: 'tBTCUSD',
    prec: 'P0',
    freq: 'F0',
    len: '25',
  });
  assert.equal(adapter.stream, 'book');
  assert.deepEqual(adapter.expectedSubscriptions(), ['book:tBTCUSD']);
  assert.deepEqual(JSON.parse(adapter.heartbeatMessage()), { event: 'ping' });
  assert.equal(adapter.url, 'wss://api-pub.bitfinex.com/ws/2');
});

test('a subscription is agreed when the venue answers with a channel id', () => {
  const agreed = parse({ event: 'subscribed', channel: 'book', symbol: 'tBTCUSD', chanId: 42 });
  assert.equal(agreed.kind, 'subscription');
  assert.equal(agreed.ok, true);
  assert.equal(agreed.key, 'book:tBTCUSD', 'the channel id is recorded internally, not used as an ack key');
  assert.equal(parse([42, [[50000, 2, 1.5]]]).book, true);
});

test('"already subscribed" is a state, not a failure', () => {
  const already = parse({ event: 'error', code: 10301, msg: 'Already subscribed' });
  assert.equal(already.kind, 'subscription');
  assert.equal(already.ok, true, 'the docs list 10301 among the states, and it is the state we wanted');

  const refused = parse({ event: 'error', code: 10300, msg: 'Unknown channel' });
  assert.equal(refused.ok, false);
  assert.match(refused.detail, /10300/);
});

test('the info codes decide what happens to the connection', () => {
  const restart = parse({ event: 'info', code: 20051, msg: 'Stop/Restart Websocket Server' });
  assert.equal(restart.kind, 'shutdown', 'the docs say: please reconnect');

  const maintenance = parse({ event: 'info', code: 20060, msg: 'Entering in Maintenance mode' });
  assert.equal(maintenance.kind, 'maintenance');
  assert.equal(maintenance.resume, false, 'pause activity, do not reconnect');

  const resumed = parse({ event: 'info', code: 20061, msg: 'Maintenance ended' });
  assert.equal(resumed.resume, true, 'and resubscribe once it is over');

  const version = parse({ event: 'info', code: 0, version: 2, platform: { status: 1 } });
  assert.equal(version.kind, 'heartbeat', 'the opening info message is liveness, not a command');
});

test('the connection admits Bitfinex after synchronous preparation', async () => {
  const subject = createBitfinexAdapter({ market: 'bitfinex_spot', symbol: 'tBTCUSD' });
  const sockets = [];
  function FakeWebSocket(url) {
    const socket = {
      url,
      onopen: null,
      onmessage: null,
      onclose: null,
      onerror: null,
      send(message) {
        const payload = JSON.parse(message);
        if (payload.event === 'subscribe') {
          queueMicrotask(() => socket.onmessage?.({ data: JSON.stringify({ event: 'subscribed', channel: 'book', symbol: 'tBTCUSD', chanId: 42 }) }));
        }
      },
      close() {},
    };
    sockets.push(socket);
    return socket;
  }
  let subscriptionState = null;
  const connection = createReceiveConnection({
    adapter: subject,
    market: 'bitfinex_spot',
    venue: 'bitfinex',
    runId: 'run-sync-preparation',
    webSocketImpl: FakeWebSocket,
    onGeneration: ({ settle }) => settle(true),
    onSubscriptions: ({ state }) => { subscriptionState = state; },
    silenceDeadlineMs: 60_000,
    ackDeadlineMs: 60_000,
  });
  connection.start();
  sockets[0].onopen();
  for (let i = 0; i < 100 && subscriptionState !== 'acknowledged'; i += 1) await new Promise((resolve) => setImmediate(resolve));
  assert.equal(subscriptionState, 'acknowledged');
  connection.stop();
});

test('the codes are read as numbers, because the text is for humans', () => {
  const reworded = parse({ event: 'info', code: 20051, msg: 'サーバーを再起動します (wording is not the protocol)' });
  assert.equal(reworded.kind, 'shutdown');
});

test('channel frames are read by what they carry', () => {
  assert.equal(parse([42, 'hb']).kind, 'heartbeat');
  const snapshot = parse([42, [[50000, 2, 1.5], [50001, 1, -0.5]]]);
  assert.equal(snapshot.kind, 'data');
  assert.equal(snapshot.book, true);
  const trade = parse([7, 'tu', [1234, 1792000000000, 0.25, 50000]]);
  assert.equal(trade.kind, 'protocol-error', 'trade channels are not subscribed by the board adapter');
  assert.deepEqual(changesOf([7, 'tu', [1234, 1792000000000, 0.25, 50000]]), { replace: false, changes: [] });
  const pong = parse({ event: 'pong' });
  assert.equal(pong.kind, 'heartbeat');
  assert.equal(pong.answered, true, 'the server answers the ping the docs describe');
});

test('a heartbeat is classified before channel-id lookup because SEQ_ALL heartbeats can race the subscription ack', () => {
  const fresh = createBitfinexAdapter({ market: 'bitfinex_spot', symbol: 'tBTCUSD' });
  assert.deepEqual(fresh.parse(JSON.stringify([999, 'hb', 7])), { kind: 'heartbeat', answered: false, seq: 7 });
});

test('an unknown channel id is rejected rather than guessed as book data', () => {
  const unknown = parse([999, [[50000, 2, 1.5]]]);
  assert.equal(unknown.kind, 'protocol-error');
  assert.match(unknown.reason, /unknown channel/i);
  assert.deepEqual(changesOf([999, [[50000, 2, 1.5]]]), { replace: false, changes: [] });
});

test('trade-shaped frames cannot enter the book path', () => {
  assert.equal(parse([7, [[1234, 1792000000000, 0.25, 50000]]]).kind, 'protocol-error');
  assert.deepEqual(changesOf([7, [[1234, 1792000000000, 0.25, 50000]]]), { replace: false, changes: [] });
});

test('a snapshot replaces the whole board, and a level becomes a change with the right side and size', () => {
  assert.deepEqual(changesOf([42, [[50000, 2, 1.5], [50001, 1, -0.5]]]), {
    replace: true,
    levels: [
      { side: 'bid', price: 50000, size: 1.5 },
      { side: 'ask', price: 50001, size: 0.5 },
    ],
  });
});

test('an update is a diff, and a count of zero is the venue saying the level is gone', () => {
  assert.deepEqual(changesOf([42, [50000, 0, 1.5]]),
    { replace: false, changes: [{ side: 'bid', price: 50000, size: 0 }] }, 'removal');
});

test('an unparsable frame is reported rather than guessed at', () => {
  assert.equal(adapter.parse('not json'), null);
  assert.equal(adapter.parse(JSON.stringify({ event: 'unknown-thing' })), null);
  assert.deepEqual(adapter.changesFor({ raw: Buffer.from('not json') }), { replace: false, changes: [] });
});

test('the adapter declares checksum and sequence verification', () => {
  assert.equal(adapter.boundary, 'sequence');
  assert.equal(typeof adapter.acceptDepthEvent, 'function');
});

const CRC32_TABLE = (() => {
  const table = new Uint32Array(256);
  for (let n = 0; n < 256; n += 1) {
    let c = n;
    for (let k = 0; k < 8; k += 1) c = c & 1 ? 0xedb88320 ^ (c >>> 1) : c >>> 1;
    table[n] = c >>> 0;
  }
  return table;
})();
const crc32Signed = (text) => {
  let crc = 0xffffffff;
  for (let i = 0; i < text.length; i += 1) crc = (crc >>> 8) ^ CRC32_TABLE[(crc ^ text.charCodeAt(i)) & 0xff];
  return (crc ^ 0xffffffff) | 0;
};
const checksumOf = ({ bids, asks }) => {
  const orderedBids = [...bids].sort((a, b) => Number(b[0]) - Number(a[0])).slice(0, 25);
  const orderedAsks = [...asks].sort((a, b) => Number(a[0]) - Number(b[0])).slice(0, 25);
  const parts = [];
  for (let i = 0; i < 25; i += 1) {
    if (orderedBids[i]) parts.push(String(orderedBids[i][0]), String(orderedBids[i][2]));
    if (orderedAsks[i]) parts.push(String(orderedAsks[i][0]), String(orderedAsks[i][2]));
  }
  return crc32Signed(parts.join(':'));
};

test('connection setup enables the official SEQ_ALL and OB_CHECKSUM flags before subscribing', () => {
  assert.deepEqual(JSON.parse(adapter.subscribeMessages()[0]), { event: 'conf', flags: 196608 });
});

test('sequenced book data is held until the matching official checksum validates the mirror', () => {
  adapter.onConnectionOpen();
  parse({ event: 'subscribed', channel: 'book', symbol: 'tBTCUSD', chanId: 42 });
  const snapshot = [42, [[50000, 2, 1.5], [50001, 1, -0.5]], 10];
  const books = { bids: [[50000, 2, 1.5]], asks: [[50001, 1, -0.5]] };
  assert.equal(parse(snapshot).seq, 10);
  assert.deepEqual(adapter.acceptDepthEvent(JSON.stringify(snapshot)), { status: 'pending' });
  assert.deepEqual(adapter.acceptDepthEvent(JSON.stringify([42, 'cs', checksumOf(books), 11])), {
    status: 'applied', released: [JSON.stringify(snapshot)],
  });
});

test('sequenced heartbeats advance the SEQ_ALL cursor without changing the book', () => {
  adapter.onConnectionOpen();
  parse({ event: 'subscribed', channel: 'book', symbol: 'tBTCUSD', chanId: 42 });
  assert.deepEqual(parse([42, 'hb', 20]), { kind: 'heartbeat', answered: false, seq: 20 });
  assert.equal(adapter.acceptDepthEvent(JSON.stringify([42, 'hb', 20])).status, 'heartbeat');
  assert.equal(adapter.acceptDepthEvent(JSON.stringify([42, [[50000, 1, 1]], 21])).status, 'pending');
});

test('the production connection counts sequenced heartbeats before validating the next update', async () => {
  const subject = createBitfinexAdapter({ market: 'bitfinex_spot', symbol: 'tBTCUSD' });
  const sockets = [];
  const diagnostics = [];
  const envelopes = [];
  function FakeWebSocket(url) {
    const socket = {
      url,
      onopen: null,
      onmessage: null,
      onclose: null,
      onerror: null,
      send(message) {
        if (JSON.parse(message).event === 'subscribe') {
          queueMicrotask(() => socket.onmessage?.({ data: JSON.stringify({ event: 'subscribed', channel: 'book', symbol: 'tBTCUSD', chanId: 42 }) }));
        }
      },
      close() {},
    };
    sockets.push(socket);
    return socket;
  }
  const connection = createReceiveConnection({
    adapter: subject,
    market: 'bitfinex_spot',
    venue: 'bitfinex',
    runId: 'run-sequenced-heartbeat',
    webSocketImpl: FakeWebSocket,
    onGeneration: ({ settle }) => settle(true),
    onEnvelope: (envelope) => envelopes.push(envelope),
    onDiagnostic: (diagnostic) => diagnostics.push(diagnostic),
    silenceDeadlineMs: 60_000,
    ackDeadlineMs: 60_000,
  });
  connection.start();
  sockets[0].onopen();
  for (let i = 0; i < 100 && connection.subscriptionState !== 'acknowledged'; i += 1) await new Promise((resolve) => setImmediate(resolve));
  const snapshot = [42, [[50000, 1, 1.5]], 1];
  const update = [42, [50001, 1, 2.5], 3];
  sockets[0].onmessage({ data: JSON.stringify(snapshot) });
  sockets[0].onmessage({ data: JSON.stringify([42, 'hb', 2]) });
  sockets[0].onmessage({ data: JSON.stringify(update) });
  sockets[0].onmessage({ data: JSON.stringify([42, 'cs', checksumOf({ bids: [[50001, 1, 2.5], [50000, 1, 1.5]], asks: [] }), 4]) });
  await new Promise((resolve) => setImmediate(resolve));
  assert.equal(connection.generation, 1);
  assert.equal(envelopes.length, 2);
  assert.equal(diagnostics.some(({ reason }) => /sequence\/checksum failure/.test(reason)), false);
  connection.stop();
});

test('a sequence gap, duplicate, or backwards sequence fails closed', () => {
  adapter.onConnectionOpen();
  parse({ event: 'subscribed', channel: 'book', symbol: 'tBTCUSD', chanId: 42 });
  assert.equal(adapter.acceptDepthEvent(JSON.stringify([42, [[50000, 1, 1]], 20])).status, 'pending');
  assert.equal(adapter.acceptDepthEvent(JSON.stringify([42, 'cs', 0, 22])).status, 'resync');
  adapter.onConnectionOpen();
  parse({ event: 'subscribed', channel: 'book', symbol: 'tBTCUSD', chanId: 42 });
  assert.equal(adapter.acceptDepthEvent(JSON.stringify([42, [[50000, 1, 1]], 20])).status, 'pending');
  assert.equal(adapter.acceptDepthEvent(JSON.stringify([42, 'cs', 0, 20])).status, 'resync');
});

test('a checksum mismatch releases nothing and requests resynchronization', () => {
  adapter.onConnectionOpen();
  parse({ event: 'subscribed', channel: 'book', symbol: 'tBTCUSD', chanId: 42 });
  const snapshot = [42, [[50000, 2, 1.5]], 30];
  assert.deepEqual(adapter.acceptDepthEvent(JSON.stringify(snapshot)), { status: 'pending' });
  assert.equal(adapter.acceptDepthEvent(JSON.stringify([42, 'cs', 123, 31])).status, 'resync');
});
