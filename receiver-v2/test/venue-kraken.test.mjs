import { test } from 'node:test';
import assert from 'node:assert/strict';

import { createKrakenAdapter } from '../src/ingest/venues/kraken.mjs';

const adapter = createKrakenAdapter({ market: 'kraken_spot', symbol: 'XBT/USD' });

const KRAKEN_V2_BOOK_SUBSCRIPTION_ACK = {
  method: 'subscribe',
  result: { channel: 'book', depth: 10, snapshot: true, symbol: 'BTC/USD' },
  success: true,
  time_in: '2026-10-07T00:00:00.000000Z',
  time_out: '2026-10-07T00:00:00.001000Z',
};

const KRAKEN_V2_TRADE_SUBSCRIPTION_ACK = {
  method: 'subscribe',
  result: { channel: 'trade', symbol: 'BTC/USD' },
  success: true,
  time_in: '2026-10-07T00:00:00.000000Z',
  time_out: '2026-10-07T00:00:00.001000Z',
};

test('Kraken v2 string-symbol subscription acknowledgements parse as successful subscriptions', () => {
  assert.deepEqual(adapter.parse(JSON.stringify(KRAKEN_V2_BOOK_SUBSCRIPTION_ACK)), {
    kind: 'subscription',
    key: 'book:BTC/USD',
    ok: true,
    detail: '',
  });
  assert.deepEqual(adapter.parse(JSON.stringify(KRAKEN_V2_TRADE_SUBSCRIPTION_ACK)), {
    kind: 'subscription',
    key: 'trade:BTC/USD',
    ok: true,
    detail: '',
  });
});

test('Kraken v2 malformed subscription symbol shapes fail closed', () => {
  for (const symbol of [{ value: 'BTC/USD' }, ['BTC/USD', 42], 42, null]) {
    const frame = {
      method: 'subscribe',
      result: { channel: 'book', symbol },
      success: true,
    };
    assert.equal(adapter.parse(JSON.stringify(frame)), null, `invalid symbol shape: ${JSON.stringify(symbol)}`);
  }
});

test('Kraken Spot v2 uses the v2 endpoint, method/params subscriptions, and v2 acknowledgements', () => {
  assert.equal(adapter.url, 'wss://ws.kraken.com/v2');
  const messages = adapter.subscribeMessages().map((message) => JSON.parse(message));
  assert.deepEqual(messages, [
    { method: 'subscribe', params: { channel: 'book', depth: 1000, symbol: ['XBT/USD'], snapshot: true } },
    { method: 'subscribe', params: { channel: 'trade', symbol: ['XBT/USD'], snapshot: true } },
  ]);
  assert.deepEqual(
    adapter.parse(JSON.stringify({ method: 'subscribe', result: { channel: 'book', symbol: ['XBT/USD'], success: true } })),
    { kind: 'subscription', key: 'book:XBT/USD', ok: true, detail: '' },
  );
  assert.deepEqual(
    adapter.parse(JSON.stringify({ method: 'subscribe', result: { channel: 'trade', symbol: ['XBT/USD'], success: true } })),
    { kind: 'subscription', key: 'trade:XBT/USD', ok: true, detail: '' },
  );
});

test('the v2 subscribe payloads and endpoint are explicit', () => {
  const messages = adapter.subscribeMessages().map((message) => JSON.parse(message));
  assert.deepEqual(messages[0], {
    method: 'subscribe',
    params: { channel: 'book', depth: 1000, symbol: ['XBT/USD'], snapshot: true },
  });
  assert.deepEqual(messages[1], {
    method: 'subscribe',
    params: { channel: 'trade', symbol: ['XBT/USD'], snapshot: true },
  });
  assert.equal(adapter.heartbeatMessage(), null, 'Kraken sends its own heartbeats');
  assert.equal(adapter.url, 'wss://ws.kraken.com/v2');
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

test('v2 book snapshot/update and trade frames are classified and translated without trade leakage', () => {
  const snapshot = {
    channel: 'book',
    type: 'snapshot',
    data: [{
      symbol: 'XBT/USD',
      bids: [{ price: '101.0', qty: '2.0' }],
      asks: [{ price: '103.0', qty: '1.0' }],
      checksum: 'ignored-by-changes',
    }],
  };
  assert.deepEqual(adapter.parse(JSON.stringify(snapshot)), { kind: 'data' });
  assert.deepEqual(adapter.changesFor({ raw: Buffer.from(JSON.stringify(snapshot)) }), {
    replace: true,
    levels: [
      { side: 'bid', price: 101, size: 2 },
      { side: 'ask', price: 103, size: 1 },
    ],
  });

  const update = {
    channel: 'book',
    type: 'update',
    data: [{ symbol: 'XBT/USD', bids: [{ price: '101.0', qty: '0' }], asks: [] }],
  };
  assert.deepEqual(adapter.changesFor({ raw: Buffer.from(JSON.stringify(update)) }), {
    replace: false,
    changes: [{ side: 'bid', price: 101, size: 0 }],
  });

  const trade = {
    channel: 'trade',
    type: 'update',
    data: [{ symbol: 'XBT/USD', trades: [{ price: '102.0', qty: '0.5', side: 'buy' }] }],
  };
  assert.deepEqual(adapter.parse(JSON.stringify(trade)), { kind: 'data' });
  assert.deepEqual(adapter.changesFor({ raw: Buffer.from(JSON.stringify(trade)) }), { replace: false, changes: [] });
});



test('an unparsable frame is reported rather than guessed at', () => {
  assert.equal(adapter.parse('not json at all'), null);
  assert.equal(adapter.parse(JSON.stringify({ nothing: 'useful' })), null);
});

test('a snapshot replaces the whole board, as the v1 contract requires', () => {
  const raw = JSON.stringify([
    1234,
    { bs: [['49999.0', '1.5', '1.0'], ['49998.0', '2.0', '1.0']], as: [['50001.0', '0.5', '1.0']], c: '1' },
    'book-1000',
    'XBT/USD',
  ]);
  const changes = adapter.changesFor({ raw: Buffer.from(raw) });
  // C5: a snapshot is a replacement, not a diff - the levels it does not name are gone.
  assert.deepEqual(changes, {
    replace: true,
    levels: [
      { side: 'bid', price: 49999, size: 1.5 },
      { side: 'bid', price: 49998, size: 2 },
      { side: 'ask', price: 50001, size: 0.5 },
    ],
  });
});

test('an update is a diff, and a size of zero is a level leaving, not a level of zero', () => {
  const raw = JSON.stringify([1234, { b: [['49999.0', '0', '1.0']], a: [] }, 'book-1000', 'XBT/USD']);
  const changes = adapter.changesFor({ raw: Buffer.from(raw) });
  assert.deepEqual(
    changes,
    { replace: false, changes: [{ side: 'bid', price: 49999, size: 0 }] },
    'the book removes it by size zero',
  );
});

test('a trade frame carries no board changes', () => {
  const raw = JSON.stringify([0, [['50000.0', '1.0', '1234.5', 'b', 'm', '']], 'trade', 'XBT/USD']);
  assert.deepEqual(adapter.changesFor({ raw: Buffer.from(raw) }), { replace: false, changes: [] });
});

test('malformed levels are dropped rather than turned into a made-up price', () => {
  const raw = JSON.stringify([1234, { b: [['not-a-number', '1'], ['10', 'x'], []], a: [] }, 'book-1000', 'XBT/USD']);
  assert.deepEqual(adapter.changesFor({ raw: Buffer.from(raw) }), { replace: false, changes: [] });
});

/**
 * The rule's own formatting and CRC32, reimplemented here so the tests can build vectors of their own.
 * The official example's expected value is written out below and never produced by this helper: a
 * helper that generated the expectation could agree with a wrong implementation. These copies exist
 * only to build the non-official vectors, where the point is the mirror's behaviour, not the CRC.
 */
const plain = (value) => String(value).replace(/\./g, '').replace(/^0+/, '');
const CRC32_TABLE = (() => {
  const table = new Uint32Array(256);
  for (let n = 0; n < 256; n += 1) {
    let c = n;
    for (let k = 0; k < 8; k += 1) c = c & 1 ? 0xedb88320 ^ (c >>> 1) : c >>> 1;
    table[n] = c >>> 0;
  }
  return table;
})();
const crc32 = (text) => {
  let crc = 0xffffffff;
  for (let i = 0; i < text.length; i += 1) crc = (crc >>> 8) ^ CRC32_TABLE[(crc ^ text.charCodeAt(i)) & 0xff];
  return (crc ^ 0xffffffff) >>> 0;
};
const checksumOf = ({ bids = [], asks = [] }) => {
  const level = ([price, qty]) => plain(price) + plain(qty);
  const asksString = [...asks].sort((a, b) => Number(a[0]) - Number(b[0])).slice(0, 10).map(level).join('');
  const bidsString = [...bids].sort((a, b) => Number(b[0]) - Number(a[0])).slice(0, 10).map(level).join('');
  return crc32(asksString + bidsString);
};
const envelopeOf = (frame) => ({ raw: Buffer.from(JSON.stringify(frame)) });
const freshKraken = (options = {}) => createKrakenAdapter({ market: 'kraken_spot', symbol: 'XBT/USD', ...options });

// Kraken's own worked example, price/qty exactly as the docs state them.
const OFFICIAL_BIDS = [
  ['45283.5', '0.10000000'],
  ['45283.4', '1.54582015'],
  ['45282.1', '0.10000000'],
  ['45281.0', '0.10000000'],
  ['45280.3', '1.54592586'],
  ['45279.0', '0.07990000'],
  ['45277.6', '0.03310103'],
  ['45277.5', '0.30000000'],
  ['45277.3', '1.54602737'],
  ['45276.6', '0.15445238'],
];
const OFFICIAL_ASKS = [
  ['45285.2', '0.00100000'],
  ['45286.4', '1.54571953'],
  ['45286.6', '1.54571109'],
  ['45289.6', '1.54560911'],
  ['45290.2', '0.15890660'],
  ['45291.8', '1.54553491'],
  ['45294.7', '0.04454749'],
  ['45296.1', '0.35380000'],
  ['45297.5', '0.09945542'],
  ['45299.5', '0.18772827'],
];
const officialSnapshot = (checksum) => [
  1234,
  {
    as: OFFICIAL_ASKS.map(([price, qty]) => [price, qty, '1.0']),
    bs: OFFICIAL_BIDS.map(([price, qty]) => [price, qty, '1.0']),
    c: checksum,
  },
  'book-1000',
  'XBT/USD',
];

test('v2 book frames for another symbol or without a valid type fail closed', () => {
  const wrongSymbol = {
    channel: 'book',
    type: 'snapshot',
    data: [{
      symbol: 'ETH/USD',
      bids: [{ price: '101.0', qty: '2.0' }],
      asks: [],
      checksum: String(checksumOf({ bids: [['101.0', '2.0']], asks: [] })),
    }],
  };
  assert.equal(freshKraken().connects({ current: envelopeOf(wrongSymbol) }), false);

  const missingType = {
    channel: 'book',
    data: [{
      symbol: 'XBT/USD',
      bids: [{ price: '101.0', qty: '2.0' }],
      asks: [],
      checksum: String(checksumOf({ bids: [['101.0', '2.0']], asks: [] })),
    }],
  };
  assert.equal(freshKraken().connects({ current: envelopeOf(missingType) }), false);
});
test('v2 book checksums gate snapshot and update state', () => {
  const rule = freshKraken();
  const snapshot = {
    channel: 'book',
    type: 'snapshot',
    data: [{
      symbol: 'XBT/USD',
      bids: [{ price: '101.0', qty: '2.0' }, { price: '100.0', qty: '1.0' }],
      asks: [{ price: '103.0', qty: '1.0' }],
      checksum: String(checksumOf({ bids: [['101.0', '2.0'], ['100.0', '1.0']], asks: [['103.0', '1.0']] })),
    }],
  };
  assert.equal(rule.connects({ current: envelopeOf(snapshot) }), true);

  const update = {
    channel: 'book',
    type: 'update',
    data: [{
      symbol: 'XBT/USD',
      bids: [{ price: '102.0', qty: '3.0' }],
      asks: [],
      checksum: String(checksumOf({ bids: [['102.0', '3.0'], ['101.0', '2.0'], ['100.0', '1.0']], asks: [['103.0', '1.0']] })),
    }],
  };
  assert.equal(rule.connects({ current: envelopeOf(update) }), true);
  update.data[0].checksum = String(Number(update.data[0].checksum) + 1);
  assert.equal(rule.connects({ current: envelopeOf(update) }), false, 'a wrong checksum refuses the update');
});
test('the official fixed example checksums through the adapter rule', () => {
  assert.equal(adapter.boundary, 'checksum', 'the venue declares the proof it can give');
  assert.equal(typeof adapter.connects, 'function', 'and brings the rule that decides it');

  // Kraken's worked example, expected CRC32 3310070434. The expectation is written out; nothing here
  // produces it from the adapter under test.
  const correct = freshKraken();
  assert.equal(correct.connects({ previous: null, current: envelopeOf(officialSnapshot('3310070434')) }), true);

  // A single wrong digit in the frame's checksum is not the proof - the top of the book does not hash
  // to it.
  const wrong = freshKraken();
  assert.equal(wrong.connects({ previous: null, current: envelopeOf(officialSnapshot('3310070433')) }), false);
});

test('a matching update keeps the proof, and a single wrong digit breaks it', () => {
  const rule = freshKraken();
  const snapshot = [
    1234,
    {
      bs: [['101.0', '2.0', '1.0'], ['100.0', '1.0', '1.0']],
      as: [['103.0', '1.0', '1.0']],
      c: String(checksumOf({ bids: [['101.0', '2.0'], ['100.0', '1.0']], asks: [['103.0', '1.0']] })),
    },
    'book-1000',
    'XBT/USD',
  ];
  assert.equal(rule.connects({ previous: null, current: envelopeOf(snapshot) }), true);

  const update = [
    1234,
    {
      b: [['102.0', '3.0', '1.0']],
      a: [],
      c: String(checksumOf({ bids: [['102.0', '3.0'], ['101.0', '2.0'], ['100.0', '1.0']], asks: [['103.0', '1.0']] })),
    },
    'book-1000',
    'XBT/USD',
  ];
  assert.equal(
    rule.connects({ previous: envelopeOf(snapshot), current: envelopeOf(update) }),
    true,
    'the checksum matched the book the update produced',
  );

  const tampered = [
    1234,
    { b: [['102.0', '3.0', '1.0']], a: [], c: String(Number(update[1].c) + 1) },
    'book-1000',
    'XBT/USD',
  ];
  assert.equal(
    rule.connects({ previous: envelopeOf(update), current: envelopeOf(tampered) }),
    false,
    'one wrong digit is not the proof',
  );
});

test('a qty of zero removes the level from the mirror', () => {
  const rule = freshKraken();
  const beforeRemoval = checksumOf({ bids: [['101.0', '2.0'], ['100.0', '1.0']], asks: [['103.0', '1.0']] });
  const snapshot = [
    1234,
    { bs: [['101.0', '2.0', '1.0'], ['100.0', '1.0', '1.0']], as: [['103.0', '1.0', '1.0']], c: String(beforeRemoval) },
    'book-1000',
    'XBT/USD',
  ];
  assert.equal(rule.connects({ previous: null, current: envelopeOf(snapshot) }), true);

  const afterRemoval = checksumOf({ bids: [['101.0', '2.0']], asks: [['103.0', '1.0']] });
  const remove = [
    1234,
    { b: [['100.0', '0', '1.0']], a: [], c: String(afterRemoval) },
    'book-1000',
    'XBT/USD',
  ];
  assert.equal(
    rule.connects({ previous: envelopeOf(snapshot), current: envelopeOf(remove) }),
    true,
    'the mirror lost the level the zero named',
  );

  // An empty update still carrying the checksum of the pre-removal book is refused, and one carrying
  // the checksum of the book without that level is accepted - the level really is gone.
  const stale = [1234, { b: [], a: [], c: String(beforeRemoval) }, 'book-1000', 'XBT/USD'];
  assert.equal(rule.connects({ previous: envelopeOf(remove), current: envelopeOf(stale) }), false);
  const fresh = [1234, { b: [], a: [], c: String(afterRemoval) }, 'book-1000', 'XBT/USD'];
  assert.equal(rule.connects({ previous: envelopeOf(stale), current: envelopeOf(fresh) }), true);
});

test('the mirror is truncated to the subscribed depth', () => {
  const levels = { bids: [['100.0', '1.0'], ['101.0', '2.0'], ['102.0', '3.0']], asks: [['103.0', '1.0']] };
  const snapshot = (checksum) => [
    1234,
    {
      bs: levels.bids.map(([price, qty]) => [price, qty, '1.0']),
      as: levels.asks.map(([price, qty]) => [price, qty, '1.0']),
      c: checksum,
    },
    'book-1000',
    'XBT/USD',
  ];

  // At depth 2 the book keeps only the two dearest bids, so the checksum over that truncated top is
  // the one the rule agrees with.
  const kept = { bids: [['102.0', '3.0'], ['101.0', '2.0']], asks: [['103.0', '1.0']] };
  const truncated = freshKraken({ bookDepth: 2 });
  assert.equal(truncated.connects({ previous: null, current: envelopeOf(snapshot(String(checksumOf(kept)))) }), true);

  // A checksum that included the level beyond the depth is refused: the mirror does not hold it.
  const tooDeep = freshKraken({ bookDepth: 2 });
  assert.equal(tooDeep.connects({ previous: null, current: envelopeOf(snapshot(String(checksumOf(levels)))) }), false);
});

test('a book frame that carries no checksum is refused, and a frame with no book levels is let through', () => {
  const rule = freshKraken();

  // fail-closed: a book frame with levels but no checksum cannot be judged, so it is not believed.
  const snapshot = [1234, { bs: [['101.0', '2.0', '1.0']], as: [['103.0', '1.0', '1.0']] }, 'book-1000', 'XBT/USD'];
  assert.equal(rule.connects({ previous: null, current: envelopeOf(snapshot) }), false);

  const update = [1234, { b: [['100.0', '1.0', '1.0']], a: [] }, 'book-1000', 'XBT/USD'];
  assert.equal(rule.connects({ previous: null, current: envelopeOf(update) }), false);

  // A trade frame carries no book levels at all and nothing for the proof to judge; it moves neither
  // the board nor the mirror, so it is not refused.
  const trade = [0, [['50000.0', '1.0', '1234.5', 'b', 'm', '']], 'trade', 'XBT/USD'];
  assert.equal(rule.connects({ previous: null, current: envelopeOf(trade) }), true);
});

// The book frames these three tests build: levels as the venue writes them, with the checksum the frame
// carries. `checksumOf` is the test's own oracle, never the adapter under test.
const level3 = (price, qty) => [price, qty, '1.0'];
const bookFrame = ({ bs, as, b, a, checksum }) =>
  [1234, { ...(bs ? { bs } : {}), ...(as ? { as } : {}), ...(b ? { b } : {}), ...(a ? { a } : {}), c: String(checksum) }, 'book-1000', 'XBT/USD'];

test('a frame the checksum refuses leaves the mirror where it was', () => {
  const subject = freshKraken();
  const snapshot = bookFrame({
    bs: [level3('100.0', '1.0')],
    as: [level3('101.0', '1.0')],
    checksum: checksumOf({ bids: [['100.0', '1.0']], asks: [['101.0', '1.0']] }),
  });
  assert.equal(subject.connects({ previous: null, current: envelopeOf(snapshot) }), true, 'the snapshot is the book');

  // An update that does not hash to the checksum it carries: refused, and the mirror must not take it.
  const bad = bookFrame({ b: [level3('102.0', '2.0')], a: [], checksum: '1' });
  assert.equal(subject.connects({ previous: null, current: envelopeOf(bad) }), false);

  // The next frame's checksum is the one computed for the book WITHOUT the refused level, so accepting it
  // is only possible if the refused frame left the mirror alone.
  const good = bookFrame({
    b: [level3('103.0', '3.0')],
    a: [],
    checksum: checksumOf({ bids: [['100.0', '1.0'], ['103.0', '3.0']], asks: [['101.0', '1.0']] }),
  });
  assert.equal(
    subject.connects({ previous: null, current: envelopeOf(good) }),
    true,
    'the refused frame did not move the mirror',
  );
});

test('the mirror does not carry over to another connection', () => {
  const subject = freshKraken();
  const onFirst = bookFrame({
    bs: [level3('100.0', '1.0')],
    as: [level3('101.0', '1.0')],
    checksum: checksumOf({ bids: [['100.0', '1.0']], asks: [['101.0', '1.0']] }),
  });
  assert.equal(
    subject.connects({ previous: null, current: { ...envelopeOf(onFirst), connection_id: 'conn-1' } }),
    true,
  );

  // A new connection is a new book: its frame is judged against an empty mirror, not against the book of
  // the connection that has ended.
  const onSecond = bookFrame({
    b: [level3('200.0', '5.0')],
    a: [],
    checksum: checksumOf({ bids: [['200.0', '5.0']], asks: [] }),
  });
  assert.equal(
    subject.connects({ previous: null, current: { ...envelopeOf(onSecond), connection_id: 'conn-2' } }),
    true,
    "the ended connection's book is not part of this one",
  );
});

test('a snapshot replaces the levels it does not name', () => {
  const subject = freshKraken();
  const first = bookFrame({
    bs: [level3('100.0', '1.0')],
    as: [level3('101.0', '1.0')],
    checksum: checksumOf({ bids: [['100.0', '1.0']], asks: [['101.0', '1.0']] }),
  });
  assert.equal(subject.connects({ previous: null, current: envelopeOf(first) }), true);

  // A second snapshot names only the ask: the bid it does not name is gone from the mirror.
  const second = bookFrame({
    bs: [],
    as: [level3('101.0', '1.0')],
    checksum: checksumOf({ bids: [], asks: [['101.0', '1.0']] }),
  });
  assert.equal(subject.connects({ previous: null, current: envelopeOf(second) }), true);

  // The cheque is computed for the book the second snapshot left - one ask and the new bid - so it only
  // matches if the first snapshot's bid is not still standing.
  const update = bookFrame({
    b: [level3('103.0', '3.0')],
    a: [],
    checksum: checksumOf({ bids: [['103.0', '3.0']], asks: [['101.0', '1.0']] }),
  });
  assert.equal(
    subject.connects({ previous: null, current: envelopeOf(update) }),
    true,
    'the level the second snapshot did not name is gone',
  );
});
