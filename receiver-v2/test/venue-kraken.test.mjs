import { test } from 'node:test';
import assert from 'node:assert/strict';

import { createKrakenAdapter } from '../src/ingest/venues/kraken.mjs';

const adapter = createKrakenAdapter({ market: 'kraken_spot', symbol: 'XBT/USD' });

test('the subscribe payloads are the ones the working connector sends', () => {
  const messages = adapter.subscribeMessages().map((message) => JSON.parse(message));
  assert.deepEqual(messages[0], {
    event: 'subscribe',
    pair: ['XBT/USD'],
    subscription: { name: 'book', depth: 1000 },
  });
  assert.deepEqual(messages[1], {
    event: 'subscribe',
    pair: ['XBT/USD'],
    subscription: { name: 'trade' },
  });
  assert.equal(adapter.heartbeatMessage(), null, 'Kraken sends its own heartbeats');
  assert.equal(adapter.url, 'wss://ws.kraken.com');
});

test('liveness frames are recognised as liveness, not as data', () => {
  for (const event of ['heartbeat', 'pong', 'ping', 'systemStatus']) {
    const parsed = adapter.parse(JSON.stringify({ event }));
    assert.equal(parsed.kind, 'heartbeat', `${event} must not reach the book`);
  }
  assert.equal(adapter.parse(JSON.stringify({ event: 'pong' })).answered, true);
});

test('a subscription is only agreed when the venue says so', () => {
  const agreed = adapter.parse(
    JSON.stringify({ event: 'subscriptionStatus', status: 'subscribed', pair: ['XBT/USD'], subscription: { name: 'book' } }),
  );
  assert.equal(agreed.kind, 'subscription');
  assert.equal(agreed.ok, true);
  assert.equal(agreed.key, 'book:XBT/USD');

  const refused = adapter.parse(
    JSON.stringify({ event: 'subscriptionStatus', status: 'error', errorMessage: 'too many subscriptions' }),
  );
  assert.equal(refused.ok, false);
  assert.match(refused.detail, /too many/);

  const error = adapter.parse(JSON.stringify({ event: 'error', errorMessage: 'bad pair' }));
  assert.equal(error.ok, false, 'an error frame leaves a trace rather than disappearing');
  assert.match(error.detail, /bad pair/);
});

test('book and trade array frames are data', () => {
  const bookFrame = [1234, { b: [['49999.0', '1.5', '1.0']], a: [], c: '1234567' }, 'book-1000', 'XBT/USD'];
  assert.equal(adapter.parse(JSON.stringify(bookFrame)).kind, 'data');
  const tradeFrame = [0, [['50000.0', '1.0', '1234.5', 'b', 'm', '']], 'trade', 'XBT/USD'];
  assert.equal(adapter.parse(JSON.stringify(tradeFrame)).kind, 'data');
});

test('an unparsable frame is reported rather than guessed at', () => {
  assert.equal(adapter.parse('not json at all'), null);
  assert.equal(adapter.parse(JSON.stringify({ nothing: 'useful' })), null);
});

test('a snapshot becomes level changes for both sides', () => {
  const raw = JSON.stringify([
    1234,
    { bs: [['49999.0', '1.5', '1.0'], ['49998.0', '2.0', '1.0']], as: [['50001.0', '0.5', '1.0']], c: '1' },
    'book-1000',
    'XBT/USD',
  ]);
  const changes = adapter.changesFor({ raw: Buffer.from(raw) });
  assert.deepEqual(changes, [
    { side: 'bid', price: 49999, size: 1.5 },
    { side: 'bid', price: 49998, size: 2 },
    { side: 'ask', price: 50001, size: 0.5 },
  ]);
});

test('an update with a size of zero is a level leaving, not a level of zero', () => {
  const raw = JSON.stringify([1234, { b: [['49999.0', '0', '1.0']], a: [] }, 'book-1000', 'XBT/USD']);
  const changes = adapter.changesFor({ raw: Buffer.from(raw) });
  assert.deepEqual(changes, [{ side: 'bid', price: 49999, size: 0 }], 'the book removes it by size zero');
});

test('a trade frame carries no board changes', () => {
  const raw = JSON.stringify([0, [['50000.0', '1.0', '1234.5', 'b', 'm', '']], 'trade', 'XBT/USD']);
  assert.deepEqual(adapter.changesFor({ raw: Buffer.from(raw) }), []);
});

test('malformed levels are dropped rather than turned into a made-up price', () => {
  const raw = JSON.stringify([1234, { b: [['not-a-number', '1'], ['10', 'x'], []], a: [] }, 'book-1000', 'XBT/USD']);
  assert.deepEqual(adapter.changesFor({ raw: Buffer.from(raw) }), []);
});
