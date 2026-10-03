/**
 * The built-in venue adapters answer in the v1 contract's shape (C5), and a real kraken frame is
 * served end to end by the single-process structure.
 *
 * This is the integration gap the split exposed: `src/changes.mjs` (v1) refuses a bare array, and
 * the built-in kraken/bitfinex adapters used to return one, so every real frame was refused as
 * "level changes refused" and the board never served. The contract is authoritative (C10), so the
 * adapters were made to match it:
 *
 *   - a snapshot -> { replace: true,  levels:  [...] }
 *   - an update   -> { replace: false, changes: [...] }
 *
 * What is fixed here: the adapters' shapes pass `validateChanges` / `deriveChanges`, a snapshot and a
 * diff both survive the envelope round trip, a malformed result is still refused, and a real kraken
 * snapshot+updates drive the single-process structure to serving.
 */

import { test } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

import { createKrakenAdapter } from '../src/ingest/venues/kraken.mjs';
import { createBitfinexAdapter } from '../src/ingest/venues/bitfinex.mjs';
import {
  CHANGES_FORMAT,
  attachChanges,
  deriveChanges,
  readChanges,
  validateChanges,
} from '../src/changes.mjs';
import { makeEnvelope } from '../src/envelope.mjs';
import { openDurability } from '../src/durability.mjs';
import { createStructure } from '../src/supervisor/structure.mjs';

const kraken = createKrakenAdapter({ market: 'kraken_spot', symbol: 'XBT/USD' });
const bitfinex = createBitfinexAdapter({ market: 'bitfinex_spot', symbol: 'tBTCUSD' });
const krakenPayload = (frame) => ({ raw: Buffer.from(JSON.stringify(frame)) });

// The rule's own formatting and CRC32, reimplemented here so the checksums are the test's own oracle
// and never produced by the adapter under test.
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
const krakenBookFrame = ({ bs, as, b, a, checksum }) =>
  [
    1234,
    { ...(bs ? { bs } : {}), ...(as ? { as } : {}), ...(b ? { b } : {}), ...(a ? { a } : {}), c: String(checksum) },
    'book-1000',
    'XBT/USD',
  ];

// ---------------------------------------------------------------------------------------------------
// The adapters answer in the contract's shape
// ---------------------------------------------------------------------------------------------------

test('C5 the built-in adapters answer in the v1 shape: snapshot replaces, update is a diff', () => {
  const krakenSnapshot = kraken.changesFor(
    krakenPayload([1234, { bs: [['100.0', '1.0', '1.0']], as: [['101.0', '1.0', '1.0']], c: '1' }, 'book-1000', 'XBT/USD']),
  );
  assert.equal(krakenSnapshot.replace, true, 'a kraken snapshot (bs/as) replaces the whole board');
  assert.deepEqual(krakenSnapshot.levels, [
    { side: 'bid', price: 100, size: 1 },
    { side: 'ask', price: 101, size: 1 },
  ]);

  const krakenUpdate = kraken.changesFor(
    krakenPayload([1234, { b: [['102.0', '2.0', '1.0']], a: [] }, 'book-1000', 'XBT/USD']),
  );
  assert.equal(krakenUpdate.replace, false, 'a kraken update (b/a) is a diff');
  assert.deepEqual(krakenUpdate.changes, [{ side: 'bid', price: 102, size: 2 }]);

  const bitfinexSnapshot = bitfinex.changesFor({ raw: Buffer.from(JSON.stringify([42, [[50000, 2, 1.5], [50001, 1, -0.5]]])) });
  assert.equal(bitfinexSnapshot.replace, true, 'a bitfinex snapshot (list of levels) replaces the whole board');
  assert.deepEqual(bitfinexSnapshot.levels, [
    { side: 'bid', price: 50000, size: 1.5 },
    { side: 'ask', price: 50001, size: 0.5 },
  ]);

  const bitfinexUpdate = bitfinex.changesFor({ raw: Buffer.from(JSON.stringify([42, [50000, 0, 1.5]])) });
  assert.equal(bitfinexUpdate.replace, false, 'a bitfinex update (one level) is a diff');
  assert.deepEqual(bitfinexUpdate.changes, [{ side: 'bid', price: 50000, size: 0 }]);

  // Both shapes are exactly what the contract validates - and a bare array is not.
  for (const value of [krakenSnapshot, krakenUpdate, bitfinexSnapshot, bitfinexUpdate]) {
    assert.equal(validateChanges(value).ok, true, `the contract accepts ${JSON.stringify(value).slice(0, 40)}`);
  }
  assert.equal(validateChanges([{ side: 'bid', price: 1, size: 1 }]).ok, false, 'a bare array is still refused');
});

test('C5 a kraken snapshot and a kraken diff both derive and survive the envelope round trip', () => {
  const base = (seq, raw) =>
    makeEnvelope({
      market: 'kraken_spot',
      stream: 'trades',
      connectionId: 'conn-1',
      receiveSeq: seq,
      recvTsMs: 1_792_000_000_000 + seq,
      recvMonoNs: 1_000_000 + seq,
      raw,
      meta: { first_seq: 1 },
    });

  const snapshotRaw = JSON.stringify([1234, { bs: [['100.0', '1.0', '1.0']], as: [['101.0', '1.0', '1.0']], c: '1' }, 'book-1000', 'XBT/USD']);
  const derivedSnapshot = deriveChanges(kraken, base(1, snapshotRaw));
  assert.equal(derivedSnapshot.ok, true, `the ingest path derives a snapshot: ${derivedSnapshot.reason ?? ''}`);
  assert.equal(derivedSnapshot.replace, true);
  const carriedSnapshot = attachChanges(base(1, snapshotRaw), derivedSnapshot);
  assert.equal(carriedSnapshot.meta.changes_format, CHANGES_FORMAT, 'the version travels with the frame');
  const readSnapshot = readChanges(carriedSnapshot);
  assert.equal(readSnapshot.ok, true);
  assert.equal(readSnapshot.replace, true);
  assert.deepEqual(readSnapshot.levels, [
    { side: 'bid', price: 100, size: 1 },
    { side: 'ask', price: 101, size: 1 },
  ]);

  const updateRaw = JSON.stringify([1234, { b: [['102.0', '2.0', '1.0']], a: [] }, 'book-1000', 'XBT/USD']);
  const derivedUpdate = deriveChanges(kraken, base(2, updateRaw));
  assert.equal(derivedUpdate.ok, true);
  assert.equal(derivedUpdate.replace, false);
  const readUpdate = readChanges(attachChanges(base(2, updateRaw), derivedUpdate));
  assert.equal(readUpdate.ok, true);
  assert.equal(readUpdate.replace, false);
  assert.deepEqual(readUpdate.changes, [{ side: 'bid', price: 102, size: 2 }]);
});

test('a malformed adapter result is refused by the ingest path, never sent on as an empty change', () => {
  const envelope = makeEnvelope({
    market: 'kraken_spot',
    stream: 'trades',
    connectionId: 'conn-1',
    receiveSeq: 1,
    recvTsMs: 1,
    recvMonoNs: 1,
    raw: '{}',
  });
  assert.equal(deriveChanges({ changesFor: () => [{ side: 'bid', price: 1, size: 1 }] }, envelope).ok, false, 'a bare array');
  assert.equal(deriveChanges({ changesFor: () => ({ replace: true }) }, envelope).ok, false, 'a replacement with no levels');
  assert.equal(deriveChanges({}, envelope).ok, false, 'no changesFor at all');
});

// ---------------------------------------------------------------------------------------------------
// A real kraken frame drives the single-process structure to serving
// ---------------------------------------------------------------------------------------------------

test('a real kraken snapshot and updates bring the single-process board to serving', async () => {
  const dir = await mkdtemp(join(tmpdir(), 'venue-serving-'));
  const store = openDurability({ path: join(dir, 'state.sqlite'), runId: 'run-1' });
  const gaps = [];
  const structure = createStructure({
    market: 'kraken_spot',
    stream: 'trades',
    adapter: createKrakenAdapter({ market: 'kraken_spot', symbol: 'XBT/USD' }),
    durability: store,
    webSocketImpl: function unused() {
      throw new Error('this test feeds frames directly');
    },
    spoolDir: null,
    onGap: (gap) => gaps.push(gap),
  });
  structure.accept('conn-1');
  try {
    const envelope = (seq, frame) =>
      makeEnvelope({
        market: 'kraken_spot',
        stream: 'trades',
        connectionId: 'conn-1',
        receiveSeq: seq,
        recvTsMs: 1_792_000_000_000 + seq,
        recvMonoNs: 1_000_000 + seq,
        raw: JSON.stringify(frame),
        meta: { first_seq: 1 },
      });

    // A snapshot that names its own checksum: the board's proof comes from the levels themselves.
    const snapshot = krakenBookFrame({
      bs: [['100.0', '1.0']],
      as: [['101.0', '1.0']],
      checksum: checksumOf({ bids: [['100.0', '1.0']], asks: [['101.0', '1.0']] }),
    });
    const first = structure.feed(envelope(1, snapshot));
    assert.equal(first.applied, true, `the snapshot was applied: ${first.reason ?? ''}`);

    // An update that adds a bid; its checksum covers the book the snapshot left.
    const update = krakenBookFrame({
      b: [['102.0', '2.0']],
      a: [],
      checksum: checksumOf({ bids: [['100.0', '1.0'], ['102.0', '2.0']], asks: [['101.0', '1.0']] }),
    });
    const second = structure.feed(envelope(2, update));
    assert.equal(second.applied, true, `the update was applied: ${second.reason ?? ''}`);

    assert.equal(structure.book.isRunning, true, 'the board serves');
    assert.equal(structure.stats.applied, 2, 'both frames reached the board');
    assert.equal(structure.book.board.depth, 3, 'the board holds the snapshot and the added bid');
    assert.deepEqual(gaps, [], 'nothing was refused as a level change');
  } finally {
    structure.stop();
    store.close();
    await rm(dir, { recursive: true, force: true });
  }
});
