// Canonical raw storage tests (issues #10/#11).
// Contract under test (table canonical_frames, schema raw_v7_canonical):
//   1. append-only: rows are INSERTed once and never UPDATE-merged or re-sorted
//   2. physical order (frame_id) == arrival order; event_ts_ms never reorders
//   3. append-safe duplicate guard on (connection_id, receive_seq): skip, no UPDATE
//   4. fail-closed: unexpected error rolls back the whole market batch
//   5. canonical rows never touch the legacy mutable raw_batches table
//   6. TTL-exempt: pruneExpired() deletes legacy raw_batches only, never
//      canonical_frames (Astra P1 regression)
//   7. no newline rejection: multi-line / byte-exact frame_text is stored
//      (Astra P2-2 regression)
//   8. pre-parse capture: frames that fail JSON.parse are preserved under the
//      reserved parse_failed classification (Astra P2-3 regression)

import { test } from 'node:test';
import assert from 'node:assert/strict';
import { DatabaseSync } from 'node:sqlite';
import { EventEmitter } from 'node:events';
import fs from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import { createHash } from 'node:crypto';
import { RawSqliteWriter } from '../lib/raw-sqlite-writer.mjs';
import { BaseConnector, CANONICAL_UNPARSED_STREAM } from '../lib/base-connector.mjs';
import { BinanceSpotConnector, BinancePerpConnector } from '../lib/binance-connector.mjs';

function canonicalEnvelope(market, stream, overrides = {}) {
  return {
    schema: 'raw_v7_canonical',
    market,
    stream,
    channel: overrides.channel ?? 'btcusdt@trade',
    connection_id: overrides.connection_id ?? null,
    receive_seq: overrides.receive_seq ?? null,
    recv_ts_ms: overrides.recv_ts_ms ?? 10_000,
    recv_mono_ns: overrides.recv_mono_ns ?? null,
    event_ts_ms: overrides.event_ts_ms ?? null,
    frame_text: overrides.frame_text ?? JSON.stringify({ e: 'trade', p: '100' }),
  };
}

function query(databasePath, sql) {
  const db = new DatabaseSync(databasePath, { readOnly: true });
  try { return db.prepare(sql).all(); } finally { db.close(); }
}

function sha256(text) {
  return createHash('sha256').update(text, 'utf8').digest('hex');
}

test('canonical append is INSERT-only: same (connection_id, receive_seq) is skipped, never updated', async () => {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), 'canonical-append-only-'));
  const writer = await new RawSqliteWriter({ databaseDir: root }).open();
  const first = canonicalEnvelope('m1', 'trades', {
    connection_id: 'binance_perp:1:1', receive_seq: 5, recv_ts_ms: 1000,
    frame_text: '{"e":"trade","p":"100","q":"1"}',
  });
  await writer.appendCanonical([first]);
  const before = query(path.join(root, 'm1.sqlite'),
    'SELECT frame_id, frame_json, frame_sha256, written_at_ms FROM canonical_frames');
  assert.equal(before.length, 1);

  // Same dedupe key but DIFFERENT payload: the immutable row must win.
  const mutated = canonicalEnvelope('m1', 'trades', {
    connection_id: 'binance_perp:1:1', receive_seq: 5, recv_ts_ms: 9999,
    frame_text: '{"e":"trade","p":"99999","q":"9"}',
  });
  // And an exact re-delivery.
  await writer.appendCanonical([first, mutated]);
  const after = query(path.join(root, 'm1.sqlite'),
    'SELECT frame_id, frame_json, frame_sha256, written_at_ms, recv_ts_ms FROM canonical_frames');
  assert.equal(after.length, 1, 'duplicate keys must not add rows');
  assert.equal(after[0].frame_json, before[0].frame_json, 'stored frame must not be overwritten by redelivery');
  assert.equal(after[0].frame_sha256, before[0].frame_sha256, 'hash must be stable across later appends');
  assert.equal(Number(after[0].written_at_ms), Number(before[0].written_at_ms), 'row must not be touched');
  assert.equal(Number(after[0].recv_ts_ms), 1000, 'metadata of the original arrival must be kept');
  assert.equal(Number(after[0].frame_id), Number(before[0].frame_id));
  assert.equal(writer.canonicalStats.appended, 1);
  assert.equal(writer.canonicalStats.skippedDuplicates, 2);

  // Legacy mutable table must stay untouched by canonical writes.
  assert.equal(query(path.join(root, 'm1.sqlite'), 'SELECT count(*) AS n FROM raw_batches')[0].n, 0);
  await writer.close();
  await fs.rm(root, { recursive: true, force: true });
});

test('canonical physical order is arrival order even when event_ts_ms / recv_ts_ms invert', async () => {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), 'canonical-arrival-'));
  const writer = await new RawSqliteWriter({ databaseDir: root }).open();
  const conn = 'binance_perp:1:2';
  // Frames arrive in sequence order 1..5; frame 3 is a DELAYED event whose
  // source event time is older than frames 1-2 (classic late-event shape).
  const frames = [
    canonicalEnvelope('m2', 'trades', { connection_id: conn, receive_seq: 1, recv_ts_ms: 1000, event_ts_ms: 1000, frame_text: '{"s":1}' }),
    canonicalEnvelope('m2', 'trades', { connection_id: conn, receive_seq: 2, recv_ts_ms: 1100, event_ts_ms: 1100, frame_text: '{"s":2}' }),
    canonicalEnvelope('m2', 'trades', { connection_id: conn, receive_seq: 3, recv_ts_ms: 1200, event_ts_ms: 50, frame_text: '{"s":3,"late":true}' }),
    canonicalEnvelope('m2', 'trades', { connection_id: conn, receive_seq: 4, recv_ts_ms: 1300, event_ts_ms: null, frame_text: '{"s":4,"noTs":true}' }),
    canonicalEnvelope('m2', 'trades', { connection_id: conn, receive_seq: 5, recv_ts_ms: 1400, event_ts_ms: 1050, frame_text: '{"s":5}' }),
  ];
  await writer.appendCanonical(frames);
  // Buffered replay: frames buffered during sync arrive again AFTER frame 5
  // with their original recv metadata; they must append at the tail, not jump
  // into their event-time position.
  await writer.appendCanonical([
    canonicalEnvelope('m2', 'trades', { connection_id: conn, receive_seq: 2, recv_ts_ms: 1100, frame_text: '{"s":2,"replay":true}' }),
  ]);
  await writer.close();

  const rows = query(path.join(root, 'm2.sqlite'),
    'SELECT frame_id, receive_seq, event_ts_ms, frame_json FROM canonical_frames ORDER BY frame_id');
  assert.equal(rows.length, 5, 'duplicate replay must be skipped');
  assert.deepEqual(rows.map((r) => Number(r.receive_seq)), [1, 2, 3, 4, 5],
    'frame_id order must equal arrival order (receive_seq) — never event_ts order');
  assert.deepEqual(rows.map((r) => JSON.parse(r.frame_json).frame.includes('late')),
    [false, false, true, false, false]);
  assert.ok(Number(rows[3].frame_id) > Number(rows[0].frame_id),
    'a no-timestamp frame still lands at its arrival position');
  await fs.rm(root, { recursive: true, force: true });
});

test('canonical duplicates inside one batch are skipped without failing the batch', async () => {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), 'canonical-batch-'));
  const writer = await new RawSqliteWriter({ databaseDir: root }).open();
  const conn = 'binance_perp:1:3';
  const fresh1 = canonicalEnvelope('m3', 'trades', { connection_id: conn, receive_seq: 10, recv_ts_ms: 500, frame_text: '{"a":1}' });
  const dup = canonicalEnvelope('m3', 'trades', { connection_id: conn, receive_seq: 10, recv_ts_ms: 600, frame_text: '{"a":1,"dup":true}' });
  const fresh2 = canonicalEnvelope('m3', 'trades', { connection_id: conn, receive_seq: 11, recv_ts_ms: 700, frame_text: '{"a":2}' });
  const nullSeq = canonicalEnvelope('m3', 'snapshots', { connection_id: conn, receive_seq: null, recv_ts_ms: 800, frame_text: '{"s":"n1"}' });
  const nullSeqDup = canonicalEnvelope('m3', 'snapshots', { connection_id: conn, receive_seq: null, recv_ts_ms: 900, frame_text: '{"s":"n2"}' });
  await writer.appendCanonical([fresh1, dup, fresh2, nullSeq, nullSeqDup]);
  await writer.close();

  const rows = query(path.join(root, 'm3.sqlite'),
    'SELECT receive_seq FROM canonical_frames ORDER BY frame_id');
  assert.deepEqual(rows.map((r) => r.receive_seq === null ? null : Number(r.receive_seq)),
    [10, 11, null, null], 'null receive_seq rows always append (SQLite UNIQUE NULL semantics)');
  const stats = writer.canonicalStats;
  assert.equal(stats.appended, 4);
  assert.equal(stats.skippedDuplicates, 1);
  await fs.rm(root, { recursive: true, force: true });
});

test('canonical append is fail-closed: unexpected error rolls back the whole market batch', async () => {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), 'canonical-rollback-'));
  const writer = await new RawSqliteWriter({ databaseDir: root }).open();
  writer._openMarket('m4'); // pre-create the market entry
  const entry = writer.databases.get('m4');
  const realDb = entry.db;
  let injected = false;
  let runCalls = 0;
  entry.db = new Proxy(realDb, {
    get(target, prop) {
      if (prop === 'prepare') {
        return (sql) => {
          const stmt = target.prepare(sql);
          if (!String(sql).includes('INSERT INTO canonical_frames')) return stmt;
          return new Proxy(stmt, {
            get(st, p) {
              if (p === 'run') {
                return (...args) => {
                  if (injected && ++runCalls === 2) throw new Error('injected write failure');
                  return st.run(...args);
                };
              }
              const v = st[p];
              return typeof v === 'function' ? v.bind(st) : v;
            },
          });
        };
      }
      const v = target[prop];
      return typeof v === 'function' ? v.bind(target) : v;
    },
  });

  const ok = canonicalEnvelope('m4', 'trades', { connection_id: 'c:4', receive_seq: 1, recv_ts_ms: 100, frame_text: '{"x":1}' });
  const boom = canonicalEnvelope('m4', 'trades', { connection_id: 'c:4', receive_seq: 2, recv_ts_ms: 200, frame_text: '{"x":2}' });
  injected = true;
  await assert.rejects(writer.appendCanonical([ok, boom]), /injected write failure/);
  injected = false;

  const after = query(path.join(root, 'm4.sqlite'),
    'SELECT count(*) AS n, min(receive_seq) AS min_s, max(receive_seq) AS max_s FROM canonical_frames');
  assert.equal(Number(after[0].n), 0, 'whole batch must roll back — no partial writes');
  assert.equal(after[0].min_s, null);
  assert.equal(after[0].max_s, null);

  // Writer queue must recover: the next append succeeds.
  await writer.appendCanonical([ok, boom]);
  const rows = query(path.join(root, 'm4.sqlite'),
    'SELECT receive_seq FROM canonical_frames ORDER BY frame_id');
  assert.deepEqual(rows.map((r) => Number(r.receive_seq)), [1, 2]);
  entry.db = realDb;
  await writer.close();
  await fs.rm(root, { recursive: true, force: true });
});

test('canonical and legacy paths stay isolated in the same market database', async () => {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), 'canonical-isolation-'));
  const writer = await new RawSqliteWriter({ databaseDir: root }).open();
  await writer.append([{
    schema: 'raw_v6_sqlite', market: 'm5', stream: 'trades',
    event_ts_ms: 100, recv_ts_ms: 100, payload: { price: 1 },
  }]);
  await writer.appendCanonical([canonicalEnvelope('m5', 'trades', {
    connection_id: 'c:5', receive_seq: 1, recv_ts_ms: 100,
    frame_text: JSON.stringify({ e: 'trade', p: '1', q: '1' }),
  })]);
  await writer.close();

  const dbPath = path.join(root, 'm5.sqlite');
  assert.equal(Number(query(dbPath, 'SELECT count(*) AS n FROM raw_batches')[0].n), 1);
  assert.equal(Number(query(dbPath, 'SELECT count(*) AS n FROM canonical_frames')[0].n), 1);
  const canonical = query(dbPath, 'SELECT frame_sha256, frame_json FROM canonical_frames')[0];
  assert.equal(canonical.frame_sha256, sha256(canonical.frame_json), 'stored hash must match the stored envelope');
  assert.equal(JSON.parse(canonical.frame_json).frame, JSON.stringify({ e: 'trade', p: '1', q: '1' }),
    'frame column must carry the exact source text');
  await fs.rm(root, { recursive: true, force: true });
});

// ---- Connector boundary capture (issue #10) -------------------------------

test('binance combined-stream frames classify for canonical capture', () => {
  const spot = Object.create(BinanceSpotConnector.prototype);
  spot.market = 'binance_spot';
  assert.deepEqual(spot._canonicalFrameMeta({ stream: 'btcusdt@trade', data: {} }),
    { stream: 'trades', channel: 'btcusdt@trade' });
  assert.deepEqual(spot._canonicalFrameMeta({ stream: 'btcusdt@depth@100ms', data: {} }),
    { stream: 'book_updates', channel: 'btcusdt@depth@100ms' });
  assert.equal(spot._canonicalFrameMeta({ data: {} }), null, 'frames without stream token are skipped');
  assert.equal(spot._canonicalFrameMeta({ stream: 'btcusdt@markPrice', data: {} }), null);

  const perp = Object.create(BinancePerpConnector.prototype);
  perp.market = 'binance_perp';
  assert.deepEqual(perp._canonicalFrameMeta({ stream: 'btcusdt@forceOrder', data: {} }),
    { stream: 'liquidations', channel: 'btcusdt@forceOrder' });
});

test('canonical frame envelope keeps the exact source text plus ingress metadata of THAT frame', () => {
  const perp = Object.create(BinancePerpConnector.prototype);
  perp.market = 'binance_perp';
  const rawText = JSON.stringify({ stream: 'btcusdt@trade', data: { e: 'trade', p: '100' } });
  perp._ingress = { recv_ts_ms: 5555, recv_mono_ns: 7777, connection_id: 'binance_perp:9:1', receive_seq: 42 };
  const frame = perp._makeCanonicalFrame({ stream: 'btcusdt@trade', data: {} }, rawText);
  assert.equal(frame.frame_text, rawText, 'source frame text must be byte-exact');
  assert.equal(frame.market, 'binance_perp');
  assert.equal(frame.stream, 'trades');
  assert.equal(frame.connection_id, 'binance_perp:9:1');
  assert.equal(frame.receive_seq, 42);
  assert.equal(frame.recv_ts_ms, 5555);
  assert.equal(frame.recv_mono_ns, 7777);
  // Default base-connector behavior stays disabled for non-opt-in connectors.
  const plain = { market: 'x', _canonicalFrameMeta: undefined };
  const baseProto = Object.getPrototypeOf(perp);
  const generic = Object.create(baseProto);
  generic.market = 'x';
  // A connector that never overrides the hook yields null.
  assert.equal(generic._makeCanonicalFrame?.({}, rawText), null);
});

// ---- Astra audit regressions (PR #18 fixes) -------------------------------

test('pruneExpired is TTL-exempt for canonical_frames: legacy raw_batches pruned, canonical rows survive', async () => {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), 'canonical-ttl-exempt-'));
  const writer = await new RawSqliteWriter({ databaseDir: root, retentionDays: 90 }).open();
  const nowMs = 10_000_000_000;
  const oldTs = nowMs - 91 * 24 * 60 * 60 * 1000; // beyond the 90-day window
  const freshTs = nowMs - 10 * 24 * 60 * 60 * 1000;
  // Legacy raw_batches: one expired, one fresh — the expired one MUST go.
  await writer.append([
    { schema: 'raw_v6_sqlite', market: 'p1m', stream: 'trades', event_ts_ms: oldTs, recv_ts_ms: oldTs, payload: { old: true } },
    { schema: 'raw_v6_sqlite', market: 'p1m', stream: 'trades', event_ts_ms: freshTs, recv_ts_ms: freshTs, payload: { fresh: true } },
  ]);
  // Canonical rows with the same age split: the expired one MUST survive.
  await writer.appendCanonical([
    canonicalEnvelope('p1m', 'trades', { connection_id: 'p1m:1:1', receive_seq: 1, recv_ts_ms: oldTs, frame_text: '{"old":true}' }),
    canonicalEnvelope('p1m', 'trades', { connection_id: 'p1m:1:1', receive_seq: 2, recv_ts_ms: freshTs, frame_text: '{"fresh":true}' }),
  ]);
  await writer.pruneExpired(nowMs);
  await writer.close();

  const dbPath = path.join(root, 'p1m.sqlite');
  const legacy = query(dbPath, 'SELECT last_recv_ts_ms FROM raw_batches ORDER BY batch_id');
  assert.equal(legacy.length, 1, 'legacy expired batch must be pruned by TTL');
  assert.equal(Number(legacy[0].last_recv_ts_ms), freshTs, 'fresh legacy batch must survive');
  const canonical = query(dbPath, 'SELECT recv_ts_ms FROM canonical_frames ORDER BY frame_id');
  assert.equal(canonical.length, 2, 'canonical rows must never be TTL-pruned');
  assert.deepEqual(canonical.map((r) => Number(r.recv_ts_ms)), [oldTs, freshTs],
    'even past-retention canonical rows must be kept (immutable full history)');
  await fs.rm(root, { recursive: true, force: true });
});

test('canonical frames with newline-containing source text append byte-exact (no newline rejection)', async () => {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), 'canonical-newline-'));
  const writer = await new RawSqliteWriter({ databaseDir: root }).open();
  const prettyJson = '{\n  "stream": "btcusdt@trade",\n  "data": { "e": "trade", "p": "100" }\n}';
  const textWithNewlines = 'line1\nline2\n{"partial": true';
  await writer.appendCanonical([
    canonicalEnvelope('m6', 'trades', { connection_id: 'c:6', receive_seq: 1, recv_ts_ms: 100, frame_text: prettyJson }),
    canonicalEnvelope('m6', 'trades', { connection_id: 'c:6', receive_seq: 2, recv_ts_ms: 200, frame_text: textWithNewlines }),
  ]);
  await writer.close();

  const rows = query(path.join(root, 'm6.sqlite'), 'SELECT frame_json FROM canonical_frames ORDER BY frame_id');
  assert.equal(rows.length, 2, 'frames containing newlines must not be rejected');
  const stored = rows.map((r) => JSON.parse(r.frame_json));
  assert.equal(stored[0].frame, prettyJson, 'pretty-printed JSON frame must round-trip byte-exact');
  assert.equal(stored[1].frame, textWithNewlines, 'raw multi-line text must round-trip byte-exact');
  assert.ok(!rows[0].frame_json.includes('\n'), 'outer JSON.stringify keeps frame_json a single line');
  assert.ok(!rows[1].frame_json.includes('\n'));
  await fs.rm(root, { recursive: true, force: true });
});

test('binance connectors opt into parse_failed classification; base default stays disabled', () => {
  for (const Conn of [BinanceSpotConnector, BinancePerpConnector]) {
    const conn = Object.create(Conn.prototype);
    conn.market = 'binance_x';
    assert.deepEqual(conn._canonicalUnparsedFrameMeta('{broken json'),
      { stream: CANONICAL_UNPARSED_STREAM, channel: null },
      'opt-in connectors classify parse failures under the reserved stream');
    assert.equal(CANONICAL_UNPARSED_STREAM, 'parse_failed');
  }
  const base = Object.create(BaseConnector.prototype);
  assert.equal(base._canonicalUnparsedFrameMeta('{broken json'), null,
    'non-opt-in connectors do not capture parse failures');
});

test('canonical envelope for a parse-failed frame keeps byte-exact text + ingress metadata (pre-parse capture)', () => {
  const perp = Object.create(BinancePerpConnector.prototype);
  perp.market = 'binance_perp';
  const badText = '{"stream":"btcusdt@trade","data":{ "e":"trade", broken';
  perp._ingress = { recv_ts_ms: 5555, recv_mono_ns: 7777, connection_id: 'binance_perp:9:1', receive_seq: 42 };
  const frame = perp._makeCanonicalFrame(null, badText, new SyntaxError('Unexpected token'));
  assert.equal(frame.stream, CANONICAL_UNPARSED_STREAM);
  assert.equal(frame.channel, null);
  assert.equal(frame.frame_text, badText, 'failing source text must be preserved byte-exact');
  assert.equal(frame.connection_id, 'binance_perp:9:1');
  assert.equal(frame.receive_seq, 42);
  assert.equal(frame.recv_ts_ms, 5555);
  assert.equal(frame.recv_mono_ns, 7777);
  // Parseable frames still classify by content on the unchanged path.
  const okText = JSON.stringify({ stream: 'btcusdt@trade', data: { e: 'trade', p: '100' } });
  const okFrame = perp._makeCanonicalFrame(JSON.parse(okText), okText);
  assert.equal(okFrame.stream, 'trades');
  assert.equal(okFrame.channel, 'btcusdt@trade');
  assert.equal(okFrame.frame_text, okText);
});

test('parse-failed WS frames are captured through the real socket handler before the error event and persist', async () => {
  // ws-compatible mock (same shape as other connector tests).
  class MockWebSocket extends EventEmitter {
    constructor(url) {
      super();
      this.url = url;
      this.readyState = 0; // CONNECTING
    }

    send() {}
    close() { this.readyState = 3; }
    terminate() { this.readyState = 3; }
  }

  const badFrame1 = '{"stream":"btcusdt@trade","data":{ "e":"trade", broken';
  const badFrame2 = 'this is not json at all';
  const conn = new BinancePerpConnector({ restUrl: 'http://localhost:1/book' });
  conn._setWebSocket(MockWebSocket);
  try {
    const connectPromise = conn.connect();
    setImmediate(() => {
      conn._ws.readyState = 1; // OPEN
      conn._ws.emit('open');
    });
    await connectPromise;

    const frames = [];
    const errors = [];
    const order = [];
    conn.on('canonicalFrame', (frame) => { frames.push(frame); order.push('frame'); });
    conn.on('error', (ev) => { errors.push(ev); order.push('error'); });

    conn._ws.emit('message', Buffer.from(badFrame1));
    conn._ws.emit('message', Buffer.from(badFrame2));

    assert.equal(frames.length, 2, 'parse-failed frames must be captured pre-parse');
    assert.equal(errors.length, 2, 'parse errors must still surface as error events');
    assert.deepEqual(order, ['frame', 'error', 'frame', 'error'],
      'canonical capture must precede the parse error (pre-parse capture)');
    assert.deepEqual(frames.map((f) => f.stream), ['parse_failed', 'parse_failed']);
    assert.deepEqual(frames.map((f) => f.frame_text), [badFrame1, badFrame2],
      'failing text must be preserved byte-exact per frame');
    assert.deepEqual(frames.map((f) => f.receive_seq), [1, 2],
      'parse failures consume receive_seq like any other socket frame');
    assert.deepEqual(errors.map((e) => e.raw), [badFrame1, badFrame2]);
    assert.ok(errors.every((e) => e.message.startsWith('parse error:')));

    // Full path: the captured envelopes persist into canonical_frames.
    const root = await fs.mkdtemp(path.join(os.tmpdir(), 'canonical-preparse-'));
    const writer = await new RawSqliteWriter({ databaseDir: root }).open();
    await writer.appendCanonical(frames);
    await writer.close();
    const rows = query(path.join(root, 'binance_perp.sqlite'),
      'SELECT stream, channel, receive_seq, frame_json FROM canonical_frames ORDER BY frame_id');
    assert.equal(rows.length, 2);
    assert.deepEqual(rows.map((r) => r.stream), ['parse_failed', 'parse_failed']);
    assert.deepEqual(rows.map((r) => r.channel), [null, null]);
    assert.deepEqual(rows.map((r) => JSON.parse(r.frame_json).frame), [badFrame1, badFrame2],
      'persisted frame_json.frame must carry the exact failing source text');
    await fs.rm(root, { recursive: true, force: true });
  } finally {
    conn._clearTimers();
    try { conn.disconnect(); } catch { /* ignore */ }
  }
});
