/**
 * Set 7a: the canonical raw writer and its v1-compatible `raw_batches` sink.
 *
 * These tests are about the persisted bytes, not an API's report: each one opens the market's SQLite
 * file with `node:sqlite` and reads the table, or gunzips `raw_gzip` and counts the lines. That is
 * exactly how the downstream reader (`agg-btc-downstream`) sees it, so a passing test here is a
 * claim about what that reader will find.
 *
 * What each group fixes:
 *   ① the table, its columns and types, and the index names are v1's (plus the Set 7 spec's one extra
 *      stream+batch index);
 *   ② a batch is confirmed on the 10 s window or at 16,384 rows, and the decompressed line count is
 *      exactly `row_count`;
 *   ③ `batch_id` rises monotonically within one market;
 *   ④ every stored line is a v1 envelope with the fields downstream requires;
 *   ⑤ a write that fails throws and leaves the rows pending - it is never swallowed.
 */

import { test } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { gunzipSync } from 'node:zlib';
import { DatabaseSync } from 'node:sqlite';

import {
  openRawWriter,
  RAW_BATCH_SCHEMA,
  RAW_BATCH_INDEXES,
  DEFAULT_RAW_BATCH_WINDOW_MS,
  DEFAULT_RAW_BATCH_MAX_ROWS,
} from '../src/raw.mjs';
import { createBinanceFuturesAdapter, createBinanceSpotAdapter } from '../src/ingest/venues/binance-spot.mjs';
import { loadConfig } from '../src/entry/config.mjs';
import { writeFile } from 'node:fs/promises';

const MARKET = 'binance_perp';

async function withDir(fn) {
  const dir = await mkdtemp(join(tmpdir(), 'raw-'));
  try {
    return await fn(dir);
  } finally {
    await rm(dir, { recursive: true, force: true });
  }
}

function bookRecord(seq, { market = MARKET, stream = 'book_updates', eventTs = 1_792_000_000_000 + seq } = {}) {
  return {
    market,
    stream,
    event_ts_ms: eventTs,
    recv_ts_ms: eventTs + 5,
    recv_mono_ns: 1_000_000 + seq,
    receive_seq: seq,
    connection_id: `${market}:conn:1`,
    payload: { type: 'update', bids: [[100, 1]], asks: [[101, 2]], seq },
  };
}

/** Read every raw_batches row of one market, ordered by batch_id. */
function readBatches(dir, market = MARKET) {
  const db = new DatabaseSync(join(dir, `${market}.sqlite`), { readOnly: true });
  try {
    return db
      .prepare('SELECT * FROM raw_batches ORDER BY batch_id')
      .all()
      .map((row) => ({
        ...row,
        batch_id: Number(row.batch_id),
        first_event_ts_ms: row.first_event_ts_ms === null ? null : Number(row.first_event_ts_ms),
        last_event_ts_ms: row.last_event_ts_ms === null ? null : Number(row.last_event_ts_ms),
        first_recv_ts_ms: Number(row.first_recv_ts_ms),
        last_recv_ts_ms: Number(row.last_recv_ts_ms),
        row_count: Number(row.row_count),
        raw_bytes: Number(row.raw_bytes),
        written_at_ms: Number(row.written_at_ms),
      }));
  } finally {
    db.close();
  }
}

function decodeLines(rawGzip) {
  const text = gunzipSync(Buffer.from(rawGzip)).toString('utf8');
  return text.length === 0 ? [] : text.replace(/\n$/, '').split('\n');
}

// ---------------------------------------------------------------------------------------------------
// ① schema: columns, types, indexes are v1's
// ---------------------------------------------------------------------------------------------------

test('① the raw_batches table and its indexes are v1-compatible', async () => {
  await withDir(async (dir) => {
    const writer = openRawWriter({ dir, batchWindowMs: 10 });
    writer.append(bookRecord(1));
    writer.flush();
    writer.close();

    const db = new DatabaseSync(join(dir, `${MARKET}.sqlite`), { readOnly: true });
    try {
      const columns = db.prepare('PRAGMA table_info(raw_batches)').all().map((row) => ({
        name: row.name,
        type: row.type,
        notnull: Number(row.notnull),
        pk: Number(row.pk),
      }));
      assert.deepEqual(columns, [
        { name: 'batch_id', type: 'INTEGER', notnull: 0, pk: 1 },
        { name: 'schema', type: 'TEXT', notnull: 1, pk: 0 },
        { name: 'market', type: 'TEXT', notnull: 1, pk: 0 },
        { name: 'stream', type: 'TEXT', notnull: 1, pk: 0 },
        { name: 'first_event_ts_ms', type: 'INTEGER', notnull: 0, pk: 0 },
        { name: 'last_event_ts_ms', type: 'INTEGER', notnull: 0, pk: 0 },
        { name: 'first_recv_ts_ms', type: 'INTEGER', notnull: 1, pk: 0 },
        { name: 'last_recv_ts_ms', type: 'INTEGER', notnull: 1, pk: 0 },
        { name: 'row_count', type: 'INTEGER', notnull: 1, pk: 0 },
        { name: 'raw_gzip', type: 'BLOB', notnull: 1, pk: 0 },
        { name: 'raw_bytes', type: 'INTEGER', notnull: 1, pk: 0 },
        { name: 'written_at_ms', type: 'INTEGER', notnull: 1, pk: 0 },
      ], 'the eleven v1 columns with v1 types and nullability');

      const indexes = db
        .prepare("SELECT name FROM sqlite_master WHERE type = 'index' AND name LIKE 'raw_batches%' ORDER BY name")
        .all()
        .map((row) => row.name);
      assert.deepEqual(indexes, [...RAW_BATCH_INDEXES].sort(), 'the v1 indexes plus the Set 7 stream+batch index');

      const win = db.prepare('SELECT journal_mode FROM pragma_journal_mode').get();
      assert.equal(String(win.journal_mode).toLowerCase(), 'wal', 'the database is WAL, so a reader is never blocked');
    } finally {
      db.close();
    }
  });
});

// ---------------------------------------------------------------------------------------------------
// ② confirmation on the window or at the row cap
// ---------------------------------------------------------------------------------------------------

test('② a batch is confirmed when the window closes, and the decoded line count equals row_count', async () => {
  await withDir(async (dir) => {
    const writer = openRawWriter({ dir, batchWindowMs: 15 });
    writer.append(bookRecord(1));
    writer.append(bookRecord(2));
    writer.append(bookRecord(3));
    // Nothing is written yet: the window is still open.
    assert.equal(writer.pendingRows, 3, 'three rows are buffered, not written');
    await new Promise((resolve) => setTimeout(resolve, 40));
    assert.equal(writer.pendingRows, 0, 'the window closed and the group was confirmed');

    writer.close();
    const rows = readBatches(dir);
    assert.equal(rows.length, 1, 'the three rows are one batch');
    assert.equal(rows[0].row_count, 3);
    assert.equal(decodeLines(rows[0].raw_gzip).length, rows[0].row_count, 'decoded lines == row_count');
    assert.equal(rows[0].schema, RAW_BATCH_SCHEMA);
    assert.equal(DEFAULT_RAW_BATCH_WINDOW_MS, 10_000, 'the production window is the v1 ten seconds');
  });
});

test('② a group that reaches 16,384 rows is confirmed at once, before any window', async () => {
  await withDir(async (dir) => {
    const writer = openRawWriter({ dir, batchWindowMs: 3_600_000 });
    for (let i = 0; i < DEFAULT_RAW_BATCH_MAX_ROWS; i += 1) writer.append(bookRecord(i + 1));
    assert.equal(writer.pendingRows, 0, 'the cap confirmed the group without waiting for the window');

    writer.close();
    const rows = readBatches(dir);
    assert.equal(rows.length, 1);
    assert.equal(rows[0].row_count, DEFAULT_RAW_BATCH_MAX_ROWS);
    assert.equal(decodeLines(rows[0].raw_gzip).length, DEFAULT_RAW_BATCH_MAX_ROWS);
    assert.equal(DEFAULT_RAW_BATCH_MAX_ROWS, 16_384, 'the production cap is the v1 16,384 rows');
  });
});

// ---------------------------------------------------------------------------------------------------
// ③ batch_id is monotonic within a market
// ---------------------------------------------------------------------------------------------------

test('③ batch_id rises monotonically within a market and per stream', async () => {
  await withDir(async (dir) => {
    const writer = openRawWriter({ dir, batchWindowMs: 3_600_000 });
    // Three book windows and one snapshot window, interleaved, all for one market.
    for (let w = 0; w < 3; w += 1) {
      for (let i = 0; i < 4; i += 1) writer.append(bookRecord(w * 4 + i + 1));
      writer.flush();
    }
    writer.append(bookRecord(100, { stream: 'snapshots' }));
    writer.flush();
    writer.close();

    const rows = readBatches(dir);
    assert.equal(rows.length, 4);
    const ids = rows.map((row) => row.batch_id);
    for (let i = 1; i < ids.length; i += 1) {
      assert.ok(ids[i] > ids[i - 1], `batch_id ${ids[i]} follows ${ids[i - 1]}`);
    }
    assert.deepEqual([...new Set(rows.map((r) => r.stream))].sort(), ['book_updates', 'snapshots']);
  });
});

// ---------------------------------------------------------------------------------------------------
// ④ the stored envelope has the fields downstream requires
// ---------------------------------------------------------------------------------------------------

test('④ every stored line is a v1 envelope with market/stream/event_ts_ms/payload', async () => {
  await withDir(async (dir) => {
    const writer = openRawWriter({ dir, batchWindowMs: 3_600_000, writerSessionId: 'session-x' });
    writer.append(bookRecord(7));
    writer.flush();
    writer.close();

    const [batch] = readBatches(dir);
    const [line] = decodeLines(batch.raw_gzip);
    const envelope = JSON.parse(line);
    assert.equal(envelope.schema, RAW_BATCH_SCHEMA);
    assert.equal(envelope.market, MARKET);
    assert.equal(envelope.stream, 'book_updates');
    assert.ok(envelope.event_ts_ms > 0, 'event_ts_ms is a positive ms value');
    assert.equal(typeof envelope.payload, 'object');
    assert.equal(envelope.payload.type, 'update');
    assert.equal(envelope.payload.seq, 7);
    assert.ok(Array.isArray(envelope.payload.bids) && Array.isArray(envelope.payload.asks));
    assert.equal(envelope.writer_session_id, 'session-x');
    assert.equal(envelope.connection_id, `${MARKET}:conn:1`);
    assert.equal(envelope.receive_seq, 7);
    assert.equal(envelope.sequence_order, 7);
  });
});

// ---------------------------------------------------------------------------------------------------
// ⑤ a failed write stops loudly and keeps the rows
// ---------------------------------------------------------------------------------------------------

test('⑤b a raw that cannot flush at close throws instead of closing quietly', async () => {
  await withDir(async (dir) => {
    class ExplodingDatabase {
      constructor(path) {
        this.real = new DatabaseSync(path);
        this.path = path;
      }
      exec(sql) {
        if (explode && /COMMIT/i.test(sql)) throw new Error('injected raw close failure');
        return this.real.exec(sql);
      }
      prepare(sql) {
        const statement = this.real.prepare(sql);
        return {
          run: (...args) => {
            if (explode && /INSERT INTO raw_batches/i.test(sql)) throw new Error('injected raw close failure');
            return statement.run(...args);
          },
        };
      }
      close() { return this.real.close(); }
    }
    let explode = true;
    const writer = openRawWriter({ dir, batchWindowMs: 3_600_000, Database: ExplodingDatabase });
    writer.append(bookRecord(1));
    // The rows are still pending and nothing landed; a close that swallowed this would report a clean
    // shutdown for a run whose last frames were never recorded.
    await assert.rejects(async () => writer.close(), /injected raw close failure/);
  });
});

test('⑤ a raw write that fails throws and leaves the rows pending', async () => {
  await withDir(async (dir) => {
    let explode = false;
    class ExplodingDatabase {
      constructor(path) {
        this.real = new DatabaseSync(path);
      }
      exec(sql) {
        return this.real.exec(sql);
      }
      prepare(sql) {
        const statement = this.real.prepare(sql);
        return {
          run: (...args) => {
            if (explode && /INSERT INTO raw_batches/i.test(sql)) throw new Error('injected raw write failure');
            return statement.run(...args);
          },
        };
      }
      close() {
        return this.real.close();
      }
    }

    const writer = openRawWriter({ dir, batchWindowMs: 3_600_000, Database: ExplodingDatabase });
    writer.append(bookRecord(1));
    writer.append(bookRecord(2));

    explode = true;
    assert.throws(() => writer.flush(), /injected raw write failure/, 'the failure is not swallowed');
    assert.equal(writer.pendingRows, 2, 'the rows stay pending so a retry can write them');

    // The failure is recoverable: with the fault cleared, the rows are written on the next flush.
    explode = false;
    writer.flush();
    assert.equal(writer.pendingRows, 0);
    writer.close();
    assert.equal(readBatches(dir)[0].row_count, 2);
  });
});

test('⑤ a malformed record is refused at the call that produced it, and after close nothing is accepted', async () => {
  await withDir(async (dir) => {
    const writer = openRawWriter({ dir, batchWindowMs: 3_600_000 });
    assert.throws(() => writer.append({ market: MARKET, stream: 'book_updates', recv_ts_ms: 1, event_ts_ms: 0, payload: {} }), /event_ts_ms/);
    assert.throws(() => writer.append({ market: MARKET, stream: 'book_updates', event_ts_ms: 5, recv_ts_ms: 1 }), /payload/);
    writer.close();
    assert.throws(() => writer.append(bookRecord(1)), /closed/);
  });
});

test('⑤ the writer refuses to construct without a directory and with a bad window', () => {
  assert.throws(() => openRawWriter({ dir: '' }), /directory/);
  assert.throws(() => openRawWriter({ dir: '/tmp', batchWindowMs: 0 }), /batchWindowMs/);
  assert.throws(() => openRawWriter({ dir: '/tmp', maxBatchRows: 0 }), /maxBatchRows/);
});

// ---------------------------------------------------------------------------------------------------
// the real adapter hook: which frames become raw records, and how a REST snapshot reaches the raw
// ---------------------------------------------------------------------------------------------------

test('the Binance depth adapter classifies a depth frame and lets a snapshot through its sink', () => {
  const adapter = createBinanceSpotAdapter({ market: 'binance_spot' });
  const depthFrame = {
    stream: 'btcusdt@depth@100ms',
    data: { e: 'depthUpdate', s: 'BTCUSDT', U: 157, u: 160, b: [['100', '1']], a: [['101', '2']], E: 1_792_000_000_123 },
  };
  const record = adapter.rawEventFor({ raw: JSON.stringify(depthFrame) });
  assert.equal(record.stream, 'book_updates');
  assert.equal(record.event_ts_ms, 1_792_000_000_123);
  assert.equal(record.source_event_time_known, true);
  // The payload is v1's candidate record: no `type` and no `prev_seq` (a `prev_seq` the downstream did
  // not expect read as a sequence gap and froze the book until the next snapshot).
  assert.equal(record.payload.type, undefined);
  assert.equal(record.payload.prev_seq, undefined);
  assert.equal(record.payload.book_apply, 'candidate');
  assert.equal(record.payload.seq, 160);
  assert.equal(record.payload.seq_start, 157);
  assert.equal(record.payload.seq_end, 160);
  assert.equal(record.payload.ts, 1_792_000_000_123);
  assert.deepEqual(record.payload.bids, [['100', '1']]);

  // The futures product writes v1's futures shape, which *does* carry the venue's `pu` (measured in
  // the running v1 store), unlike spot. Its depth stream is `@depth` (250 ms), not `@depth@100ms`.
  const futures = createBinanceFuturesAdapter({ market: 'binance_perp' });
  const futuresFrame = {
    stream: 'btcusdt@depth',
    data: { e: 'depthUpdate', s: 'BTCUSDT', U: 157, u: 160, pu: 156, b: [['100', '1']], a: [['101', '2']], E: 1_792_000_000_123 },
  };
  const futuresRecord = futures.rawEventFor({ raw: JSON.stringify(futuresFrame) });
  assert.equal(futuresRecord.payload.prev_seq, 156, 'the futures shape carries the venue pu');
  assert.equal(futuresRecord.payload.seq_start, 157);
  const spotWithPu = { ...depthFrame, data: { ...depthFrame.data, pu: 156 } };
  assert.equal(adapter.rawEventFor({ raw: JSON.stringify(spotWithPu) }).payload.prev_seq, undefined, 'spot never writes prev_seq');

  // A trade frame is a Set 7b raw record: v1's `{market, price, qty, side, ts, tradeId}` (`t` is the
  // trade id, `m` the maker flag, `T` the trade time). Set 7b's own test file fixes each venue's shape.
  const tradeFrame = { stream: 'btcusdt@trade', data: { e: 'trade', s: 'BTCUSDT', p: '100', q: '1', t: 1, T: 1_792_000_000_000, m: false } };
  const tradeRecord = adapter.rawEventFor({ raw: JSON.stringify(tradeFrame) });
  assert.equal(tradeRecord.stream, 'trades');
  assert.equal(tradeRecord.payload.tradeId, '1');
  assert.equal(tradeRecord.payload.market, 'binance_spot');

  // The REST snapshot applied by the synchronizer reaches the sink as a raw snapshot record.
  const seen = [];
  adapter.setRawSnapshotSink((snap) => seen.push(snap));
  adapter.syncSnapshot({ lastUpdateId: 42, bids: [['100', '1']], asks: [['101', '2']] });
  assert.equal(seen.length, 1);
  assert.equal(seen[0].payload.type, 'snapshot');
  assert.equal(seen[0].payload.seq, 42);
  assert.equal(seen[0].payload.snapshot_origin, 'rest_sync');
  assert.equal(seen[0].payload.event_time_source, 'rest_snapshot');
  assert.equal(seen[0].source_event_ts_ms, null);
  assert.equal(seen[0].source_event_time_known, false);
  assert.ok(seen[0].event_ts_ms > 0);
});

test('⑩ a snapshot for a preparation that was replaced mid-wait is neither applied nor recorded', async () => {
  const seen = [];
  const started = Date.now();
  // Current while the snapshot is fetched and the buffer wait begins, stale when the wait loop's next
  // tick runs: exactly a connection replaced during the wait. The wait loop can end because of that,
  // and a snapshot applied anyway would anchor the new connection's book - and record its raw snapshot
  // under the new connection's name - on a boundary the venue has already moved past.
  const adapter = createBinanceFuturesAdapter({
    market: 'binance_perp',
    fetchImpl: async () => ({ ok: true, json: async () => ({ lastUpdateId: 100, bids: [['100', '1']], asks: [['101', '2']] }) }),
  });
  adapter.setRawSnapshotSink((snap) => seen.push(snap));
  const result = await adapter.sync.sync([], { isCurrent: () => Date.now() - started < 30 });
  assert.equal(result.status, 'stale', 'a replaced preparation reports itself stale');
  assert.equal(seen.length, 0, 'and its snapshot never reaches the raw');
});

// ---------------------------------------------------------------------------------------------------
// configuration: rawDir is optional and carried through
// ---------------------------------------------------------------------------------------------------

test('the config carries an optional rawDir and still refuses a bare "raw" key', async () => {
  await withDir(async (dir) => {
    const write = (obj) => writeFile(join(dir, 'config.json'), JSON.stringify(obj));
    const base = {
      venue: 'binance_spot',
      market: 'binance_spot',
      spoolDir: join(dir, 'spool'),
      stores: { ingest: 'i.sqlite', organize: 'o.sqlite', book: 'b.sqlite' },
    };

    // A config with no rawDir is valid and carries none.
    await write(base);
    assert.equal(loadConfig(join(dir, 'config.json')).rawDir, undefined);

    // One with a rawDir carries it.
    await write({ ...base, rawDir: join(dir, 'raw') });
    assert.equal(loadConfig(join(dir, 'config.json')).rawDir, join(dir, 'raw'));

    // A half-written rawDir fails loudly.
    await write({ ...base, rawDir: '' });
    assert.throws(() => loadConfig(join(dir, 'config.json')), /rawDir/);

    // The old "raw" key is still refused (this package does not save a bare raw file).
    await write({ ...base, raw: join(dir, 'raw.sqlite') });
    assert.throws(() => loadConfig(join(dir, 'config.json')), /"raw" is not supported/);
  });
});
