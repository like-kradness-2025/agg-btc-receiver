import { test } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

import { createBitfinexAdapter } from '../src/ingest/venues/bitfinex.mjs';
import { makeEnvelope } from '../src/envelope.mjs';
import { openDurability } from '../src/durability.mjs';
import { openBook } from '../src/book/state.mjs';

const adapter = createBitfinexAdapter({ market: 'bitfinex_spot', symbol: 'tBTCUSD' });
const parse = (payload) => adapter.parse(JSON.stringify(payload));
const changesOf = (payload) => adapter.changesFor({ raw: Buffer.from(JSON.stringify(payload)) });

test('the subscribe payloads carry the documented fields', () => {
  const [book, trades] = adapter.subscribeMessages().map((message) => JSON.parse(message));
  assert.deepEqual(book, {
    event: 'subscribe',
    channel: 'book',
    symbol: 'tBTCUSD',
    prec: 'P0',
    freq: 'F0',
    len: '25',
  });
  assert.deepEqual(trades, { event: 'subscribe', channel: 'trades', symbol: 'tBTCUSD' });
  assert.deepEqual(JSON.parse(adapter.heartbeatMessage()), { event: 'ping' });
  assert.equal(adapter.url, 'wss://api-pub.bitfinex.com/ws/2');
});

test('a subscription is agreed when the venue answers with a channel id', () => {
  const agreed = parse({ event: 'subscribed', channel: 'book', symbol: 'tBTCUSD', chanId: 42 });
  assert.equal(agreed.kind, 'subscription');
  assert.equal(agreed.ok, true);
  assert.equal(agreed.key, 'book:tBTCUSD:42', 'the channel id is how updates are addressed');
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

test('the codes are read as numbers, because the text is for humans', () => {
  // The docs are explicit: only rely on the code. A reworded message must not change behaviour.
  const reworded = parse({ event: 'info', code: 20051, msg: 'サーバーを再起動します (wording is not the protocol)' });
  assert.equal(reworded.kind, 'shutdown');
});

test('channel frames are read by what they carry', () => {
  assert.equal(parse([42, 'hb']).kind, 'heartbeat');
  const snapshot = parse([42, [[50000, 2, 1.5], [50001, 1, -0.5]]]);
  assert.equal(snapshot.kind, 'data');
  assert.equal(snapshot.book, true);
  // The venue frames a trade update as [chanId, "tu", [id, mts, amount, price]].
  const trade = parse([7, 'tu', [1234, 1792000000000, 0.25, 50000]]);
  assert.equal(trade.trade, true, 'a trade frame is data, but it does not move this book');
  assert.deepEqual(changesOf([7, 'tu', [1234, 1792000000000, 0.25, 50000]]), { replace: false, changes: [] });
  const pong = parse({ event: 'pong' });
  assert.equal(pong.kind, 'heartbeat');
  assert.equal(pong.answered, true, 'the server answers the ping the docs describe');
});

test('a snapshot replaces the whole board, and a level becomes a change with the right side and size', () => {
  // C5: a snapshot (a list of levels) is a replacement, not a diff.
  assert.deepEqual(changesOf([42, [[50000, 2, 1.5], [50001, 1, -0.5]]]), {
    replace: true,
    levels: [
      { side: 'bid', price: 50000, size: 1.5 },
      { side: 'ask', price: 50001, size: 0.5 },
    ],
  });
});

test('an update is a diff, and a count of zero is the venue saying the level is gone', () => {
  assert.deepEqual(
    changesOf([42, [50000, 0, 1.5]]),
    { replace: false, changes: [{ side: 'bid', price: 50000, size: 0 }] },
    'removal',
  );
});

test('an unparsable frame is reported rather than guessed at', () => {
  assert.equal(adapter.parse('not json'), null);
  assert.equal(adapter.parse(JSON.stringify({ event: 'unknown-thing' })), null);
  assert.deepEqual(adapter.changesFor({ raw: Buffer.from('not json') }), { replace: false, changes: [] });
});

test('the adapter declares unverifiable, and a book opened with it runs without claiming a proof', async () => {
  // Explicit, not left to the default: a book frame here carries no sequence and no checksum, so
  // there is nothing to prove and the declaration says so rather than the absence of one.
  assert.equal(adapter.boundary, 'unverifiable');
  assert.equal(adapter.connects, undefined, 'an unverifiable venue brings no rule to decide a frame');

  const dir = await mkdtemp(join(tmpdir(), 'bitfinex-proof-'));
  const store = openDurability({ path: join(dir, 'state.sqlite'), runId: 'run-1' });
  try {
    const book = openBook({ market: 'bitfinex_spot', stream: 'trades', durability: store, adapter });
    const frame = [42, [[50000, 2, 1.5]]];
    const envelope = makeEnvelope({
      market: 'bitfinex_spot',
      stream: 'trades',
      connectionId: 'conn-1',
      receiveSeq: 1,
      recvTsMs: 1_792_000_000_000,
      recvMonoNs: 1_000_000,
      raw: Buffer.from(JSON.stringify(frame)),
      meta: { first_seq: 1 },
    });
    book.accept('conn-1', { firstSeq: 1 });
    assert.equal(book.apply({ envelope, changes: adapter.changesFor(envelope) }).applied, true);

    const proof = book.proveBoundary();
    assert.equal(proof.proven, true, 'a venue with no means of proof may still run');
    assert.equal(proof.kind, 'unverifiable');
    assert.equal(proof.verified, false, 'but nothing is described as proven');
    assert.match(proof.reason, /unverifiable|without a boundary proof/);
    assert.equal(book.isRunning, true);
  } finally {
    store.close();
    await rm(dir, { recursive: true, force: true });
  }
});
