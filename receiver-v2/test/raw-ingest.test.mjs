/**
 * Set 7a: the canonical raw is written from the real receive path, and a depth snapshot is recorded
 * twice (book_updates + snapshots), as v1 did.
 *
 * These tests drive the actual ingest process entrance over the real IPC transport, with a fake
 * WebSocket that hands the test every socket reception opens. A frame delivered on that socket
 * travels the real receive path (`connection.mjs` -> `main.mjs` `onEnvelope`), so the raw row the test
 * reads afterwards proves the wiring, not a call to the writer in isolation.
 */

import { test } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { gunzipSync } from 'node:zlib';
import { DatabaseSync } from 'node:sqlite';

import { openIngestProcess } from '../src/ingest/main.mjs';
import { startFakeOrganize } from '../test-support/fake-organize.mjs';
import { createBinanceFuturesAdapter } from '../src/ingest/venues/binance-spot.mjs';
import { createBybitSpotAdapter } from '../src/ingest/venues/bybit.mjs';

const MARKET = 'binance_perp';
const STREAM = 'trades';
const VENUE = 'binance';

async function until(predicate, { timeoutMs = 4000, stepMs = 5 } = {}) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (predicate()) return;
    await new Promise((resolve) => setTimeout(resolve, stepMs));
  }
  throw new Error('timed out waiting for the condition');
}

function fakeSockets() {
  const sockets = [];
  const impl = function fakeSocket(url) {
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
      deliver(data) {
        socket.onmessage?.({ data });
      },
    };
    sockets.push(socket);
    return socket;
  };
  return { sockets, impl };
}

/** A depth adapter with the two raw hooks: a frame's book record, and a snapshot sink for REST syncs. */
function depthAdapter() {
  let snapshotSink = null;
  return {
    url: 'ws://venue.test/ws',
    stream: STREAM,
    parse: () => ({ kind: 'data' }),
    changesFor: () => ({ replace: false, changes: [] }),
    subscribeMessages: () => [],
    heartbeatMessage: () => null,
    // A depth frame is the book's raw; the test's frames always carry one.
    rawEventFor: () => ({
      stream: 'book_updates',
      event_ts_ms: 1_792_000_000_123,
      source_event_ts_ms: 1_792_000_000_100,
      source_event_time_known: true,
      payload: { type: 'update', bids: [[100, 1]], asks: [[101, 2]], seq: 5 },
    }),
    setRawSnapshotSink(fn) {
      snapshotSink = fn;
    },
    pushSnapshot(record) {
      snapshotSink?.(record);
    },
  };
}

async function withIngest(fn, { rawBatchWindowMs = 3_600_000, adapter: adapterOverride = null, market = MARKET } = {}) {
  const dir = mkdtempSync(join(tmpdir(), 'raw-ingest-'));
  const organize = await startFakeOrganize(join(dir, 'organize.sock'), { batchFrames: 1 });
  const adapter = adapterOverride ?? depthAdapter();
  const { sockets, impl } = fakeSockets();
  const diagnostics = [];
  const stops = [];
  const process = await openIngestProcess({
    tailSaveMs: 0,
    market,
    stream: STREAM,
    adapter,
    venue: VENUE,
    runId: 'run-1',
    webSocketImpl: impl,
    organizeSocketPath: organize.server.path,
    ingestStorePath: join(dir, 'ingest.sqlite'),
    spoolDir: join(dir, 'spool'),
    channelOptions: { batchFrames: 1 },
    rawDir: join(dir, 'raw'),
    // A window the caller chooses: the default stays open so a row is only confirmed by the close
    // (the assertion is then about arrival, not about a race with a timer); a short window lets a test
    // watch a row land while the process is still running.
    rawBatchWindowMs,
    onStop: (stop) => stops.push(stop),
    onDiagnostic: (d) => diagnostics.push(d),
    onGap: () => {},
  });
  const teardown = async () => {
    try {
      process.close();
    } finally {
      await organize.close();
      rmSync(dir, { recursive: true, force: true });
    }
  };
  try {
    return await fn({ dir, process, adapter, sockets, diagnostics, stops, teardown });
  } finally {
    await teardown();
  }
}

function readBatches(dir, market = MARKET) {
  // No file yet means nothing has been confirmed: the writer opens a market's database lazily, on the
  // first batch it actually writes.
  let db;
  try {
    db = new DatabaseSync(join(dir, 'raw', `${market}.sqlite`), { readOnly: true });
  } catch {
    return [];
  }
  try {
    return db
      .prepare('SELECT * FROM raw_batches ORDER BY batch_id')
      .all()
      .map((row) => ({ ...row, row_count: Number(row.row_count), stream: row.stream }));
  } finally {
    db.close();
  }
}

function decodeLines(rawGzip) {
  const text = gunzipSync(Buffer.from(rawGzip)).toString('utf8');
  return text.length === 0 ? [] : text.replace(/\n$/, '').split('\n');
}

test('⑥ a frame delivered on the socket writes its book_updates record to the canonical raw', async () => {
  await withIngest(async ({ dir, process, sockets }) => {
    process.start();
    await until(() => sockets.length === 1);
    sockets[0].onopen();
    sockets[0].deliver('{"depth":1}');
    process.stop();
    process.close();

    const rows = readBatches(dir);
    assert.equal(rows.length, 1, 'one frame produced one book_updates batch');
    assert.equal(rows[0].stream, 'book_updates');
    assert.equal(rows[0].market, MARKET);
    assert.equal(rows[0].row_count, 1);
    const [line] = decodeLines(rows[0].raw_gzip);
    const envelope = JSON.parse(line);
    assert.equal(envelope.stream, 'book_updates');
    assert.equal(envelope.market, MARKET);
    assert.equal(envelope.event_ts_ms, 1_792_000_000_123, 'the adapter-derived event time is stored');
    assert.equal(envelope.connection_id, process.connectionId, 'the reception connection is named on the line');
    assert.ok(envelope.recv_ts_ms > 0, 'the envelope carries the reception wall clock');
    assert.equal(envelope.payload.seq, 5);
  });
});

test('⑧b a frame the connection refuses is still recorded (v1 wrote before judging)', async () => {
  await withIngest(async ({ dir, process, adapter, sockets }) => {
    // A trade frame is dropped by the connection (`connection.mjs`: a trade is not depth's input);
    // Set 7b will route it onward. The raw must still hold it, because v1 recorded every frame it
    // heard before deciding what the book could do with it.
    adapter.parse = () => ({ kind: 'data', trade: true });
    process.start();
    await until(() => sockets.length === 1);
    sockets[0].onopen();
    sockets[0].deliver('{"dropped":1}');
    process.stop();
    process.close();

    const rows = readBatches(dir);
    const lines = rows.flatMap((row) => decodeLines(row.raw_gzip));
    assert.equal(lines.length, 1, 'the refused frame was recorded');
    const envelope = JSON.parse(lines[0]);
    assert.equal(envelope.stream, 'book_updates');
    assert.equal(envelope.receive_seq, 1, 'the raw numbers every frame it heard, refused or not');
    assert.equal(envelope.payload.connection_id, process.connectionId, 'the payload carries the connection too');
  });
});

test('⑨ a frame buffered during the REST preparation is recorded once, and not again on replay', async () => {
  await withIngest(
    async ({ dir, process, adapter, sockets }) => {
      let release = null;
      adapter.onConnectionOpen = () => new Promise((resolve) => { release = resolve; });
      adapter.bufferDuringPreparation = () => {};
      process.start();
      await until(() => sockets.length === 1);
      sockets[0].onopen();
      // The preparation is still pending: these are buffered for the replay, and the raw holds them
      // now, because they arrived.
      sockets[0].deliver('{"prep":1}');
      sockets[0].deliver('{"prep":2}');
      await until(() => readBatches(dir).flatMap((row) => decodeLines(row.raw_gzip)).length === 2);

      release();
      await until(() => process.receivedTails()[0]?.lastReceivedSeq === 2);
      // A further window must pass with no new rows: the replay is not a second recording.
      await new Promise((resolve) => setTimeout(resolve, 80));
      assert.equal(
        readBatches(dir).flatMap((row) => decodeLines(row.raw_gzip)).length,
        2,
        'the replay of the buffer does not record them a second time',
      );
      process.stop();
      process.close();
    },
    { rawBatchWindowMs: 20 },
  );
});

test('⑨b buffered frames stay recorded when the preparation fails and the connection is replaced', async () => {
  await withIngest(
    async ({ dir, process, adapter, sockets, diagnostics }) => {
      adapter.onConnectionOpen = () => Promise.reject(new Error('injected preparation failure'));
      adapter.bufferDuringPreparation = () => {};
      process.start();
      await until(() => sockets.length === 1);
      sockets[0].onopen();
      sockets[0].deliver('{"prep-fail":1}');
      await until(() => diagnostics.some((d) => /preparation failed/.test(String(d.reason))));
      await until(() => readBatches(dir).flatMap((row) => decodeLines(row.raw_gzip)).length === 1);
      process.stop();
      process.close();
    },
    { rawBatchWindowMs: 20 },
  );
});

test('⑪ a preparation still in flight when the connection ends cannot anchor or record its snapshot', async () => {
  let releaseFetch = null;
  // The REST snapshot never arrives before the connection ends: the preparation stays in flight across
  // the stop. Its snapshot must not anchor the book nor reach the raw under the ended connection's name.
  const adapter = createBinanceFuturesAdapter({
    market: MARKET,
    fetchImpl: () => new Promise((resolve) => { releaseFetch = resolve; }),
  });
  await withIngest(
    async ({ dir, process, sockets }) => {
      process.start();
      await until(() => sockets.length === 1);
      sockets[0].onopen();
      await until(() => typeof releaseFetch === 'function');
      process.stop();
      releaseFetch({ ok: true, json: async () => ({ lastUpdateId: 100, bids: [['100', '1']], asks: [['101', '2']] }) });
      // The futures preparation waits up to 5 s for the first buffered frame. An invalidated preparation
      // leaves that wait at its next tick and never applies; one that is still considered current runs
      // the whole window out and then applies. Waiting past the window is what tells the two apart.
      await new Promise((resolve) => setTimeout(resolve, 5500));
      process.close();
      const lines = readBatches(dir).flatMap((row) => decodeLines(row.raw_gzip));
      assert.equal(
        lines.filter((line) => JSON.parse(line).stream === 'snapshots').length,
        0,
        'the ended connection\'s snapshot was neither applied nor recorded',
      );
    },
    { adapter, rawBatchWindowMs: 20 },
  );
});

test('⑦ a depth snapshot is recorded twice: once as book_updates and once under snapshots', async () => {
  await withIngest(async ({ dir, process, adapter, sockets }) => {
    process.start();
    await until(() => sockets.length === 1);
    sockets[0].onopen();
    sockets[0].deliver('{"depth":1}');

    // The REST snapshot is applied inside the adapter's depth synchronizer; the sink main.mjs attached
    // is how that snapshot reaches the raw.
    adapter.pushSnapshot({
      event_ts_ms: 1_792_000_005_000,
      payload: { type: 'snapshot', bids: [[100, 1]], asks: [[101, 2]], seq: 42 },
    });

    process.stop();
    process.close();

    const rows = readBatches(dir);
    const streams = [...new Set(rows.map((row) => row.stream))].sort();
    assert.deepEqual(streams, ['book_updates', 'snapshots'], 'the snapshot is written to both streams');

    // The book_updates batch holds the socket frame's update and the snapshot's full replacement.
    const bookBatch = rows.find((row) => row.stream === 'book_updates');
    assert.equal(bookBatch.row_count, 2, 'the update and the snapshot both landed under book_updates');
    const snapshotBatch = rows.find((row) => row.stream === 'snapshots');
    assert.equal(snapshotBatch.row_count, 1);
    const [line] = decodeLines(snapshotBatch.raw_gzip);
    const envelope = JSON.parse(line);
    assert.equal(envelope.payload.type, 'snapshot');
    assert.equal(envelope.payload.seq, 42);
    assert.equal(envelope.event_ts_ms, 1_792_000_005_000);
  });
});

test('⑦b a trade frame on the real receive path is recorded under trades, never under the book', async () => {
  // A real adapter, not the fake: this proves the Set 7b wiring end to end - the trade reaches the
  // connection (which drops it from the board), and `rawEventFor` writes it to the canonical raw.
  const adapter = createBybitSpotAdapter({ market: 'bybit_spot' });
  await withIngest(
    async ({ dir, process, sockets }) => {
      process.start();
      await until(() => sockets.length === 1);
      sockets[0].onopen();
      sockets[0].deliver(JSON.stringify({
        topic: 'publicTrade.BTCUSDT',
        type: 'snapshot',
        data: [{ s: 'BTCUSDT', S: 'Buy', p: '100', v: '1.5', T: 1_792_000_000_111, i: 'trade-1' }],
      }));
      process.stop();
      process.close();

      const rows = readBatches(dir, 'bybit_spot');
      assert.deepEqual([...new Set(rows.map((row) => row.stream))], ['trades'], 'the trade is recorded, nothing on the book streams');
      const [line] = decodeLines(rows[0].raw_gzip);
      const envelope = JSON.parse(line);
      assert.equal(envelope.market, 'bybit_spot');
      assert.equal(envelope.stream, 'trades');
      assert.equal(envelope.source_id, 'trade-1');
      assert.equal(envelope.payload.market, 'bybit_spot');
      assert.equal(envelope.payload.side, 'buy');
      assert.equal(envelope.payload.tradeId, 'trade-1');
    },
    { adapter, market: 'bybit_spot' },
  );
});
