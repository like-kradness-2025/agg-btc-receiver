import { test } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

import { makeEnvelope } from '../src/envelope.mjs';
import { openBook } from '../src/book/state.mjs';
import { openDurability } from '../src/durability.mjs';
import { createStructure } from '../src/supervisor/structure.mjs';

import { internalsOf } from '../src/internal/wiring.mjs';

/** The parts of a structure, for a test that drives one of them directly: the wiring's private side. */
const partsOf = (structure) => internalsOf(structure);

const envelope = (seq, connectionId = 'conn-1', meta = { first_seq: 1 }) =>
  makeEnvelope({
    market: 'kraken_spot',
    stream: 'trades',
    connectionId,
    receiveSeq: seq,
    recvTsMs: 1_792_000_000_000 + seq,
    recvMonoNs: 1_000_000 + seq,
    raw: `{"seq":${seq}}`,
    meta,
  });

test('a frame from a connection nobody accepted is neither written nor acknowledged', async () => {
  const dir = await mkdtemp(join(tmpdir(), 'refusal-'));
  try {
    const store = openDurability({ path: join(dir, 'state.sqlite'), runId: 'run-1' });
    const gaps = [];
    const acks = [];
    const written = [];
    const structure = createStructure({
      market: 'kraken_spot',
      stream: 'trades',
      adapter: {
        url: 'ws://venue.test/ws',
        stream: 'trades',
        parse: () => ({ kind: 'data' }),
        changesFor: () => [],
      },
      durability: store,
      webSocketImpl: function unused() {
        throw new Error('this test feeds frames directly');
      },
      rawWriter: (frame) => {
        written.push(frame.receive_seq);
        return true;
      },
      spoolDir: null,
      onAck: (ack) => acks.push(ack),
      onGap: (gap) => gaps.push(gap),
      onStop: () => {},
    });

    // Nothing is accepted here on purpose: the frame is durable data for a connection this process was
    // never told to organize, and taking it on sight is exactly what C2 forbids.
    const refused = structure.feed(envelope(1));
    assert.equal(refused.accepted, false, 'it is not accepted');
    assert.equal(refused.reason, 'no connection has been accepted yet');
    assert.equal(refused.ack, null, 'and no acknowledgement is emitted');
    assert.deepEqual(written, [], 'the raw was not written for a connection nobody accepted');
    assert.deepEqual(acks, [], 'nothing was acknowledged');
    assert.equal(structure.book.appliedBoundary.connectionId, null, 'and the board heard nothing');

    // Once the connection is accepted, the same frame is written and reaches the board.
    structure.accept('conn-1');
    const after = structure.feed(envelope(1));
    assert.equal(after.durable, true, 'the raw takes it');
    assert.equal(after.applied, true, 'and the board applies it');
    assert.deepEqual(written, [1]);
    assert.equal(gaps.length, 0, 'a frame that was handled is not a loss');
    store.close();
  } finally {
    await rm(dir, { recursive: true, force: true });
  }
});

test('a run that was already replaced never starts receiving', async () => {
  const dir = await mkdtemp(join(tmpdir(), 'refusal-'));
  try {
    const dbPath = join(dir, 'state.sqlite');
    // A board owned by a run that has since been replaced: run-2 took it over, so run-1 is retired in
    // the store - which is how a restart finds it.
    const seeding = openDurability({ path: dbPath, runId: 'run-1' });
    const board = openBook({ market: 'kraken_spot', stream: 'trades', durability: seeding });
    board.accept('run-1:kraken:kraken_spot:1', { generation: 1, runId: 'run-1', firstSeq: 1 });
    board.accept('run-2:kraken:kraken_spot:1', { generation: 1, runId: 'run-2', firstSeq: 1, takeover: true });
    seeding.close();

    const store = openDurability({ path: dbPath, runId: 'run-1' });
    const sockets = [];
    const diagnostics = [];
    const stops = [];
    const structure = createStructure({
      market: 'kraken_spot',
      stream: 'trades',
      runId: 'run-1',
      venue: 'kraken',
      adapter: {
        url: 'ws://venue.test/ws',
        stream: 'trades',
        parse: () => ({ kind: 'data' }),
        changesFor: () => [],
      },
      durability: store,
      webSocketImpl: function fakeSocket(url) {
        const socket = { url, onopen: null, onmessage: null, onclose: null, onerror: null, send() {}, close() {} };
        sockets.push(socket);
        return socket;
      },
      rawWriter: () => true,
      spoolDir: null,
      onStop: (stop) => stops.push(stop),
      onDiagnostic: (diagnostic) => diagnostics.push(diagnostic),
    });

    structure.start();
    assert.equal(sockets.length, 0, 'the refused run does not get to open a socket at all');
    assert.equal(structure.connection.state, 'refused');
    assert.equal(stops.length, 1, 'and a receiver that cannot receive says so, rather than sitting quiet');
    assert.match(String(stops[0].reason), /was not admitted/);
    assert.ok(
      diagnostics.some((diagnostic) => String(diagnostic.reason).includes('was not admitted downstream')),
      'and the refusal is reported rather than silent',
    );
    assert.ok(
      diagnostics.some((diagnostic) => String(diagnostic.reason).includes('already replaced')),
      'for the reason the book gave',
    );

    // A caller cannot authorise a takeover by asking either: that decision is made where reception
    // starts, from the board's own recorded owner.
    const forced = structure.accept('run-1:kraken:kraken_spot:1', {
      generation: 1,
      runId: 'run-1',
      firstSeq: 1,
      takeover: true,
    });
    assert.equal(forced.accepted, false, 'a caller asking for a takeover does not get one');
    assert.equal(forced.reason, 'this run was already replaced');
    store.close();
  } finally {
    await rm(dir, { recursive: true, force: true });
  }
});

test('a structure whose adapter carries another stream is refused before the book or the spool exist', async () => {
  const dir = await mkdtemp(join(tmpdir(), 'refusal-'));
  try {
    const store = openDurability({ path: join(dir, 'state.sqlite'), runId: 'run-1' });
    assert.throws(
      () =>
        createStructure({
          market: 'kraken_spot',
          stream: 'book',
          adapter: {
            url: 'ws://venue.test/ws',
            stream: 'trades',
            parse: () => ({ kind: 'data' }),
            changesFor: () => [],
          },
          durability: store,
          webSocketImpl: function unused() {
            throw new Error('nothing should be built');
          },
          rawWriter: () => true,
        }),
      /carries trades/,
      'a mismatch that can only produce frames the board refuses is not a configuration to run with',
    );
    store.close();
  } finally {
    await rm(dir, { recursive: true, force: true });
  }
});

test('a frame under a connection name that was handed over earlier is not written again', async () => {
  const dir = await mkdtemp(join(tmpdir(), 'refusal-'));
  try {
    const store = openDurability({ path: join(dir, 'state.sqlite'), runId: 'A' });
    const written = [];
    const gaps = [];
    const structure = createStructure({
      market: 'kraken_spot',
      stream: 'trades',
      runId: 'A',
      venue: 'v',
      adapter: {
        url: 'ws://venue.test/ws',
        stream: 'trades',
        parse: () => ({ kind: 'data' }),
        changesFor: (frame) => [{ side: 'bid', price: 100 + frame.receive_seq, size: 1 }],
      },
      durability: store,
      webSocketImpl: function unused() {
        throw new Error('this test feeds frames directly');
      },
      rawWriter: (frame) => {
        written.push({ seq: frame.receive_seq, raw: frame.raw.toString() });
        return true;
      },
      spoolDir: null,
      onAck: () => {},
      onGap: (gap) => gaps.push(gap),
      onStop: () => {},
    });

    const frame = (connectionId, generation, raw) =>
      makeEnvelope({
        market: 'kraken_spot',
        stream: 'trades',
        connectionId,
        runId: 'A',
        venue: 'v',
        generation,
        receiveSeq: 1,
        recvTsMs: 1_792_000_000_001,
        recvMonoNs: 1_000_000_001,
        raw,
      });

    structure.accept('A:v:m:1', { runId: 'A', generation: 1, firstSeq: 1 });
    assert.equal(structure.feed(frame('A:v:m:1', 1, '{"first":true}')).applied, true);
    assert.equal(structure.book.board.size('bid', 101), 1);
    assert.equal(structure.accept('A:v:m:2', { runId: 'A', generation: 2, firstSeq: 1 }).accepted, true);

    // The old name comes back under a third generation: the name is refused, so neither the raw nor the board
    // takes anything of it - the raw still holds exactly the one frame the first identity wrote.
    const reused = structure.accept('A:v:m:1', { runId: 'A', generation: 3, firstSeq: 1 });
    assert.equal(reused.accepted, false);
    const stale = structure.feed(frame('A:v:m:1', 3, '{"first":false}'));
    assert.equal(stale.accepted, false);
    assert.deepEqual(written, [{ seq: 1, raw: '{"first":true}' }], 'the raw was not written a second time');
    assert.equal(structure.book.board.size('bid', 101), 1, 'and the board still has what it had');
    store.close();
  } finally {
    await rm(dir, { recursive: true, force: true });
  }
});

test('a store that records an owner but no identity for its name does not let that name be re-used', async () => {
  const dir = await mkdtemp(join(tmpdir(), 'refusal-'));
  try {
    const dbPath = join(dir, 'state.sqlite');

    // A store from before the identity record existed: it names an owner and holds a position, and knows nothing
    // about which run or generation that name was accepted as.
    const before = openDurability({ path: dbPath, runId: 'A' });
    const seeded = openBook({ market: 'kraken_spot', stream: 'trades', durability: before });
    seeded.accept('A:v:m:1', { runId: 'A', generation: 1, firstSeq: 1 });
    seeded.apply({
      envelope: makeEnvelope({
        market: 'kraken_spot',
        stream: 'trades',
        connectionId: 'A:v:m:1',
        runId: 'A',
        venue: 'v',
        generation: 1,
        receiveSeq: 1,
        recvTsMs: 1_792_000_000_001,
        recvMonoNs: 1_000_000_001,
        raw: '{"first":true}',
      }),
      changes: [{ side: 'bid', price: 101, size: 1 }],
    });
    before.db.exec('DELETE FROM connection_identity');
    before.close();

    const store = openDurability({ path: dbPath, runId: 'A' });
    const written = [];
    const structure = createStructure({
      market: 'kraken_spot',
      stream: 'trades',
      runId: 'A',
      venue: 'v',
      adapter: {
        url: 'ws://venue.test/ws',
        stream: 'trades',
        parse: () => ({ kind: 'data' }),
        changesFor: (frame) => [{ side: 'bid', price: 100 + frame.receive_seq, size: 1 }],
      },
      durability: store,
      webSocketImpl: function unused() {
        throw new Error('this test feeds frames directly');
      },
      rawWriter: (frame) => {
        written.push(frame.receive_seq);
        return true;
      },
      spoolDir: null,
      onAck: () => {},
      onGap: () => {},
      onStop: () => {},
    });

    const frame = (connectionId, generation) =>
      makeEnvelope({
        market: 'kraken_spot',
        stream: 'trades',
        connectionId,
        runId: 'A',
        venue: 'v',
        generation,
        receiveSeq: 1,
        recvTsMs: 1_792_000_000_001,
        recvMonoNs: 1_000_000_001,
        raw: '{"first":false}',
      });

    // The name the store already records is its own, and re-accepting it is what the row means.
    assert.equal(structure.accept('A:v:m:1', { runId: 'A', generation: 1, firstSeq: 1 }).accepted, true);
    assert.equal(structure.accept('A:v:m:2', { runId: 'A', generation: 2, firstSeq: 1 }).accepted, true);

    const reused = structure.accept('A:v:m:1', { runId: 'A', generation: 3, firstSeq: 1 });
    assert.equal(reused.accepted, false, 'the recorded name cannot be claimed by a third generation');
    assert.match(reused.reason, /already been used for another identity/);

    const stale = structure.feed(frame('A:v:m:1', 3));
    assert.equal(stale.accepted, false);
    assert.deepEqual(written, [], 'nothing of the old name was written again');
    assert.equal(structure.book.board.size('bid', 101), 1, 'and the board still has the level it had');
    store.close();
  } finally {
    await rm(dir, { recursive: true, force: true });
  }
});

test('an owner without a run name is not handed the board back, and nothing of it reaches the raw', async () => {
  const dir = await mkdtemp(join(tmpdir(), 'refusal-'));
  try {
    const dbPath = join(dir, 'state.sqlite');
    const written = [];
    const rawWriter = (frame) => {
      written.push({ seq: frame.receive_seq, raw: frame.raw.toString() });
      return true;
    };
    const on = (store, runId) =>
      createStructure({
        market: 'kraken_spot',
        stream: 'trades',
        runId,
        venue: 'v',
        adapter: {
          url: 'ws://venue.test/ws',
          stream: 'trades',
          parse: () => ({ kind: 'data' }),
          changesFor: (frame) => [{ side: 'bid', price: 100 + frame.receive_seq, size: 1 }],
        },
        durability: store,
        webSocketImpl: function unused() {
          throw new Error('this test feeds frames directly');
        },
        rawWriter,
        spoolDir: null,
        onAck: () => {},
        onGap: () => {},
        onStop: () => {},
      });
    const frame = (connectionId, generation, runId, raw) =>
      makeEnvelope({
        market: 'kraken_spot',
        stream: 'trades',
        connectionId,
        runId,
        venue: 'v',
        generation,
        receiveSeq: 1,
        recvTsMs: 1_792_000_000_001,
        recvMonoNs: 1_000_000_001,
        raw,
      });

    // A receiver running with no run name takes the board and writes its first frame.
    const first = openDurability({ path: dbPath, runId: 'writer' });
    const unnamed = on(first, null);
    assert.equal(unnamed.accept('null:1', { generation: 1, firstSeq: 1 }).accepted, true);
    assert.equal(unnamed.feed(frame('null:1', 1, null, '{"first":true}')).applied, true);
    assert.equal(unnamed.book.board.size('bid', 101), 1);

    // A named run takes it over - the authorisation is issued where reception starts, which is what the board
    // sees here.
    assert.equal(
      partsOf(unnamed).book.accept('B:v:m:1', { runId: 'B', generation: 1, firstSeq: 1, takeover: true }).accepted,
      true,
    );
    first.close();

    // Started again with no run name, it asks for the board back. A replaced owner does not return, so its
    // frames are never written a second time and the board keeps exactly what it had.
    const second = openDurability({ path: dbPath, runId: 'writer-2' });
    const later = on(second, null);
    const returned = later.accept('null:2', { generation: 2, firstSeq: 1 });
    assert.equal(returned.accepted, false);
    assert.match(returned.reason, /already replaced/);

    const stale = later.feed(frame('null:2', 2, null, '{"first":false}'));
    assert.equal(stale.accepted, false);
    assert.deepEqual(written, [{ seq: 1, raw: '{"first":true}' }], 'the raw still holds the one frame');
    assert.equal(later.book.board.size('bid', 101), 1, 'and the board still holds the level it had');
    second.close();
  } finally {
    await rm(dir, { recursive: true, force: true });
  }
});
