/**
 * Set 7: the receive tail is keyed by the board, not just the connection.
 *
 * A connection name is made of run, venue, market and generation and does not carry the stream (C2), so the
 * same connection legitimately has a book tail and a trades tail. Keyed only by the connection, the two
 * overwrote each other - a lower bound attributed to the wrong board. The key gains the stream; rows written
 * before that have no stream recorded and are kept under the empty marker rather than guessed at.
 */

import { test } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { DatabaseSync } from 'node:sqlite';

import { openDurability } from '../src/durability.mjs';
import { createStructure } from '../src/supervisor/structure.mjs';
import { createSupervisor } from '../src/supervisor/supervisor.mjs';

/** An adapter whose frames move this board, so a socket arrival is a real delivery, not a no-op. */
const tradesAdapter = {
  url: 'ws://venue.test/ws',
  stream: 'trades',
  parse: () => ({ kind: 'data' }),
  changesFor: (frame) => [{ side: 'bid', price: 100 + frame.receive_seq, size: 1 }],
};

/**
 * One structure over one store, with a socket the test holds. The socket is opened by reception itself,
 * so a frame handed to `onmessage` travels the real receive path and writes the receive tail.
 */
function receiveStructure(store, runId) {
  const socket = { url: '', onopen: null, onmessage: null, onclose: null, onerror: null, send() {}, close() {} };
  const structure = createStructure({
    market: 'kraken_spot',
    stream: 'trades',
    adapter: tradesAdapter,
    durability: store,
    runId,
    venue: 'kraken',
    webSocketImpl: function openSocket() {
      return socket;
    },
    rawWriter: () => true,
    spoolDir: null,
    onAck: () => {},
    onGap: () => {},
    onStop: () => {},
    onDiagnostic: () => {},
  });
  return { structure, socket };
}

async function withDir(fn) {
  const dir = await mkdtemp(join(tmpdir(), 'tail-'));
  try {
    return await fn(dir);
  } finally {
    await rm(dir, { recursive: true, force: true });
  }
}

test('two streams of one connection keep separate receive tails, and no stream does not overwrite either', async () => {
  await withDir(async (dir) => {
    const store = openDurability({ path: join(dir, 'state.sqlite'), runId: 'run-1' });
    try {
      store.updateReceivedTail({ connectionId: 'c', market: 'kraken_spot', stream: 'trades', lastReceivedSeq: 5, lastRecvMonoNs: 10 });
      store.updateReceivedTail({ connectionId: 'c', market: 'kraken_spot', stream: 'book', lastReceivedSeq: 9, lastRecvMonoNs: 20 });

      assert.equal(store.readReceivedTail('c', 'trades').lastReceivedSeq, 5);
      assert.equal(store.readReceivedTail('c', 'book').lastReceivedSeq, 9);
      assert.equal(store.readReceivedTail('c', 'trades').stream, 'trades');

      // A caller with no stream is recorded under the empty marker: it is not attributed to a board that
      // never wrote it, and it does not overwrite either real board's tail.
      store.updateReceivedTail({ connectionId: 'c', market: 'kraken_spot', lastReceivedSeq: 1, lastRecvMonoNs: 1 });
      assert.equal(store.readReceivedTail('c', 'trades').lastReceivedSeq, 5, 'the trades tail is untouched');
      assert.equal(store.readReceivedTail('c', 'book').lastReceivedSeq, 9, 'and so is the book tail');
      assert.equal(store.readReceivedTail('c', '').lastReceivedSeq, 1, 'the unknown board has its own row');
    } finally {
      store.close();
    }
  });
});

test('an old single-key receive tail is migrated without guessing its stream, and survives reopen', async () => {
  await withDir(async (dir) => {
    const path = join(dir, 'state.sqlite');
    // An old store: the tail keyed by the connection alone, one row already written.
    const old = new DatabaseSync(path);
    old.exec(`CREATE TABLE received_tail (
      connection_id TEXT NOT NULL PRIMARY KEY,
      market TEXT NOT NULL,
      last_received_seq INTEGER NOT NULL,
      last_recv_mono_ns INTEGER NOT NULL,
      updated_at_ms INTEGER NOT NULL
    )`);
    old.exec(`INSERT INTO received_tail (connection_id, market, last_received_seq, last_recv_mono_ns, updated_at_ms)
              VALUES ('conn-old', 'kraken_spot', 42, 1000, 7)`);
    old.close();

    const store = openDurability({ path, runId: 'run-1' });
    const migrated = store.readReceivedTail('conn-old');
    assert.equal(migrated.lastReceivedSeq, 42, 'the old row is kept');
    assert.equal(migrated.stream, '', 'and its stream is left unguessed');
    // The new key works beside the migrated row.
    store.updateReceivedTail({ connectionId: 'conn-old', market: 'kraken_spot', stream: 'book', lastReceivedSeq: 3, lastRecvMonoNs: 5 });
    store.close();

    // Re-open: the migration is idempotent and the rows are still there.
    const again = openDurability({ path, runId: 'run-2' });
    assert.equal(again.readReceivedTail('conn-old').lastReceivedSeq, 42);
    assert.equal(again.readReceivedTail('conn-old', 'book').lastReceivedSeq, 3);
    assert.equal(again.readReceivedTail('conn-old', 'trades'), null);
    again.close();
  });
});

test('a frame received on the socket records the receive tail for its connection, board and stream', async () => {
  await withDir(async (dir) => {
    const path = join(dir, 'state.sqlite');
    const sockets = [];
    // A socket implementation that hands the test each socket the reception opens, so the frames below
    // arrive through the real receive path rather than a caller feeding envelopes into the supervisor.
    const webSocketImpl = function (url) {
      const socket = {
        url,
        sent: [],
        closed: false,
        onopen: null,
        onmessage: null,
        onclose: null,
        onerror: null,
        send(message) {
          socket.sent.push(message);
        },
        close() {
          socket.closed = true;
        },
      };
      sockets.push(socket);
      return socket;
    };
    const adapter = {
      url: 'ws://venue.test/ws',
      stream: 'trades',
      parse: () => ({ kind: 'data' }),
      changesFor: (frame) => [{ side: 'bid', price: 100 + frame.receive_seq, size: 1 }],
    };
    const supervisor = createSupervisor({
      market: 'kraken_spot',
      stream: 'trades',
      adapter,
      path,
      venue: 'kraken',
      runId: 'run-1',
      webSocketImpl,
      rawWriter: () => true,
      spoolDir: join(dir, 'spool'),
      exit: () => {},
      onStop: () => {},
      onRefetch: () => {},
    });
    assert.equal(supervisor.start().started, true, 'the connection was admitted and the socket opened');
    assert.equal(sockets.length, 1, 'reception opened one socket');
    const connectionId = supervisor.connection.connectionId;
    // Three frames arrive on the socket. Reception stamps each one and records how far it has heard as it
    // handles it - this is the receive path, not the supervisor's own `feed`.
    sockets[0].onmessage({ data: '{"seq":1}' });
    sockets[0].onmessage({ data: '{"seq":2}' });
    sockets[0].onmessage({ data: '{"seq":3}' });
    supervisor.stop();
    supervisor.close();

    // Read the store's own table, so the assertion is about what was persisted, not what an API reports.
    const db = new DatabaseSync(path);
    const rows = db.prepare('SELECT * FROM received_tail').all();
    db.close();
    assert.equal(rows.length, 1, 'reception wrote exactly one tail row for the board');
    assert.equal(rows[0].connection_id, connectionId, 'it names the connection the frames arrived on');
    assert.equal(rows[0].market, 'kraken_spot', 'and the market');
    assert.equal(rows[0].stream, 'trades', 'the stream is part of the key, written not guessed');
    assert.equal(rows[0].last_received_seq, 3, 'and it holds the highest sequence received');
    assert.ok(rows[0].last_recv_mono_ns > 0, 'with the monotonic stamp of that frame');
  });
});

test('a receive tail left by a run that did not close cleanly becomes a suspected gap on the next start', async () => {
  await withDir(async (dir) => {
    const path = join(dir, 'state.sqlite');
    // run-1 receives a frame on the socket - so its receive tail is written - and then dies mid-stream:
    // it never writes its completion marker. This is the crash §9.2 exists for, and the interval it
    // leaves behind is exactly what a restart cannot account for.
    const first = openDurability({ path, runId: 'run-1' });
    const { structure: s1, socket } = receiveStructure(first, 'run-1');
    s1.beginRun();
    s1.start();
    const connectionId = s1.connection.connectionId;
    assert.ok(connectionId.length > 0, 'reception reached its connection');
    socket.onmessage({ data: '{"seq":1,"size":1}' });
    const tail = first.readReceivedTail(connectionId, 'trades');
    assert.equal(tail.lastReceivedSeq, 1, 'the tail recorded what arrived before the crash');
    s1.stop(); // no completeRun: the run ended mid-stream
    first.close();

    // The next run over the same store restores the previous boundary. The earlier run's marker is not
    // `complete`, so the interval after its tail is recorded as a suspected gap.
    const second = openDurability({ path, runId: 'run-2' });
    const { structure: s2 } = receiveStructure(second, 'run-2');
    const gaps = second.suspectedGaps({ market: 'kraken_spot' });
    assert.equal(gaps.length, 1, 'the unaccounted interval is recorded');
    assert.equal(gaps[0].stream, 'trades', 'for the board the tail belonged to');
    assert.equal(gaps[0].fromMs, tail.updatedAtMs, 'it starts where the tail was last written');
    assert.ok(gaps[0].toMs >= gaps[0].fromMs, 'and ends at the resume moment');
    assert.match(gaps[0].reason, /did not close cleanly/);
    assert.ok(gaps[0].reason.includes(connectionId), 'the connection it cannot account for is named');
    s2.stop();
    second.close();
  });
});

test('a receive tail left by a run that closed cleanly leaves no suspected gap on the next start', async () => {
  await withDir(async (dir) => {
    const path = join(dir, 'state.sqlite');
    // The same arrival, but this time the run stops on purpose: it writes its completion marker, which
    // is the only thing that says the tail is a true upper bound rather than a lower bound with an
    // unaccounted interval after it (§8.1).
    const first = openDurability({ path, runId: 'run-1' });
    const { structure: s1, socket } = receiveStructure(first, 'run-1');
    s1.beginRun();
    s1.start();
    socket.onmessage({ data: '{"seq":1,"size":1}' });
    assert.equal(first.readReceivedTail(s1.connection.connectionId, 'trades').lastReceivedSeq, 1);
    s1.completeRun();
    s1.stop();
    first.close();

    const second = openDurability({ path, runId: 'run-2' });
    const { structure: s2 } = receiveStructure(second, 'run-2');
    assert.deepEqual(second.suspectedGaps(), [], 'a clean end leaves nothing to suspect');
    s2.stop();
    second.close();
  });
});
