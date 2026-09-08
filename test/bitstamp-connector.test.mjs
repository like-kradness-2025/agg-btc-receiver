// test/bitstamp-connector.test.mjs — Bitstamp BTC/USD connector tests.
// Issue #13: REST snapshot ↔ WS diff boundary must be provable via source
// microtimestamps, with deterministic race coverage for stale / duplicate /
// missing diffs at the boundary.

import { describe, it } from 'node:test';
import assert from 'node:assert';
import { BitstampConnector } from '../lib/bitstamp-connector.mjs';

// REST snapshot boundary (µs). Buffered diffs strictly older are provably
// inside the snapshot; strictly newer are provably outside it.
const SNAP_MICRO = '1700000001000000'; // → 1700000001000 ms
const SNAP_MS = 1700000001000;

// Helper: microtimestamp strings (µs) a/b milliseconds apart from SNAP_MS.
const micro = (offsetMs) => String((SNAP_MS + offsetMs) * 1000);

const REST_BODY = {
  microtimestamp: SNAP_MICRO,
  timestamp: '1700000001',
  bids: [['65000', '1.0']],
  asks: [['65001', '2.0']],
};

const depthFrame = (bids, asks, microtimestamp) => ({
  event: 'data',
  channel: 'diff_order_book_btcusd',
  data: { bids, asks, microtimestamp },
});

function stubFetch(body, { delayMs = 0, calls } = {}) {
  const original = globalThis.fetch;
  let callCount = 0;
  globalThis.fetch = async () => {
    const idx = Math.min(callCount++, (calls?.length ?? 1) - 1);
    const chosen = calls ? calls[idx] : body;
    if (delayMs > 0) await new Promise((resolve) => setTimeout(resolve, delayMs));
    return { ok: chosen.ok !== false, json: async () => ({ ...chosen }) };
  };
  return { original, callCount: () => callCount };
}

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

describe('BitstampConnector parser', () => {
  function createConn() {
    const conn = new BitstampConnector({});
    conn._ws = { send: () => {} };
    conn._setState('running');
    return conn;
  }

  it('should send live_trades_btcusd and diff_order_book_btcusd subscribe frames', () => {
    const conn = new BitstampConnector({});
    const sent = [];
    conn._ws = { send: (msg) => { sent.push(msg); } };
    conn.subscribe();
    assert.strictEqual(sent.length, 2);
    const channels = sent.map((s) => JSON.parse(s).data.channel).sort();
    assert.deepStrictEqual(channels, ['diff_order_book_btcusd', 'live_trades_btcusd']);
  });

  it('should parse trade payloads and normalize buy/sell side', () => {
    const conn = createConn();
    let emitted = null;
    conn.on('trade', (ev) => { emitted = ev; });

    conn._onMessage({
      event: 'trade',
      channel: 'live_trades_btcusd',
      data: {
        id: 123456,
        price: '65000.12',
        amount: '0.25',
        type: 0,
        microtimestamp: '1700000000123456',
      },
    });

    assert.ok(emitted);
    assert.strictEqual(emitted.market, 'bitstamp_spot');
    assert.strictEqual(emitted.price, 65000.12);
    assert.strictEqual(emitted.qty, 0.25);
    assert.strictEqual(emitted.side, 'buy');
    assert.strictEqual(emitted.ts, 1700000000123);
    assert.strictEqual(emitted.tradeId, '123456');
  });

  it('should ignore subscription ack messages', () => {
    const conn = createConn();
    let emitted = false;
    conn.on('trade', () => { emitted = true; });

    conn._onMessage({
      event: 'bts:subscription_succeeded',
      channel: 'live_trades_btcusd',
      data: { channel: 'live_trades_btcusd' },
    });

    assert.strictEqual(emitted, false);
  });
});

describe('BitstampConnector snapshot/diff boundary (Issue #13)', () => {
  async function syncWithDiff(conn, frames, fetchOpts) {
    const emitted = [];
    conn.on('depth', (event) => emitted.push(event));
    const stubbed = stubFetch(REST_BODY, fetchOpts);
    try {
      const sync = conn._syncBook();
      for (const frame of frames) conn._onMessage(frame);
      await sync;
      return { emitted, stubbed };
    } finally {
      globalThis.fetch = stubbed.original;
    }
  }

  it('seeds the book from REST and replays ONLY diffs provably newer than the snapshot boundary', async () => {
    const conn = new BitstampConnector({ restUrl: 'https://example.test/book' });
    conn._setState('connected');
    const { emitted } = await syncWithDiff(conn, [
      // Stale diff (arrived during sync, ts < snapshot as-of): already inside
      // the REST snapshot → must NOT be replayed.
      depthFrame([['65000', '0.5']], [], micro(-100)),
      // Newer diff (ts > snapshot as-of): provably after the snapshot → replay.
      depthFrame([['65000', '1.5']], [['65001', '0']], micro(+200)),
    ]);

    assert.strictEqual(emitted.length, 2);
    assert.strictEqual(emitted[0].type, 'snapshot');
    assert.strictEqual(emitted[0].snapshot_origin, 'rest_sync');
    assert.strictEqual(emitted[0].snapshot_asof_ts_ms, SNAP_MS);
    // Snapshot event now carries its own exchange source time (issue #13
    // raw metadata requirement) instead of faking an unknown source time.
    assert.strictEqual(emitted[0].source_event_ts_ms, SNAP_MS);
    assert.strictEqual(emitted[0].source_event_time_known, true);
    assert.strictEqual(emitted[1].type, 'update');
    assert.strictEqual(emitted[1].source_event_ts_ms, SNAP_MS + 200);

    assert.strictEqual(conn.book.bids.get('65000'), '1.5');
    assert.strictEqual(conn.book.asks.has('65001'), false);
    assert.strictEqual(conn._stats.snapshotIncludedDiffCount, 1);
    assert.strictEqual(conn.getState(), 'running');
  });

  it('never rolls the book back with a stale buffered diff (issue #13 race)', async () => {
    // The issue's failure scenario: diff A arrives while the REST snapshot
    // (which already includes A) is fetched; replaying A afterwards would
    // roll price X back from the snapshot value.
    const conn = new BitstampConnector({ restUrl: 'https://example.test/book' });
    conn._setState('connected');
    const { emitted } = await syncWithDiff(conn, [
      depthFrame([['65000', '0.1']], [], micro(-500)), // stale: snapshot says 1.0
    ], { delayMs: 30 });

    // Only the snapshot is emitted; the stale diff is dropped, counted.
    assert.deepStrictEqual(emitted.map((e) => e.type), ['snapshot']);
    assert.strictEqual(conn.book.bids.get('65000'), '1.0'); // snapshot value intact
    assert.strictEqual(conn.book.asks.get('65001'), '2.0');
    assert.strictEqual(conn._stats.snapshotIncludedDiffCount, 1);
    assert.strictEqual(conn.getState(), 'running');
  });

  it('fails closed when a buffered diff equals the snapshot boundary, then succeeds on the retry snapshot', async () => {
    // Inclusion of a diff whose microtimestamp EXACTLY equals the snapshot
    // as-of is unprovable (server internals decide pre/post dump). The sync
    // must not guess: it re-fetches a strictly newer snapshot which provably
    // covers the ambiguous diff.
    const boundaryEqualBody = { ...REST_BODY, microtimestamp: SNAP_MICRO };
    const newerBody = { ...REST_BODY, bids: [['65000', '3.0']], microtimestamp: micro(+2000) };
    const conn = new BitstampConnector({ restUrl: 'https://example.test/book' });
    conn._setState('connected');

    const emitted = [];
    conn.on('depth', (event) => emitted.push(event));
    const stubbed = stubFetch(REST_BODY, { calls: [boundaryEqualBody, newerBody] });
    try {
      const sync = conn._syncBook();
      // Buffered diff with ts == first snapshot's boundary.
      conn._onMessage(depthFrame([['65000', '9.9']], [], micro(0)));
      await sync;

      assert.strictEqual(conn.getState(), 'running');
      assert.strictEqual(conn._stats.boundaryResyncCount, 1);
      // Second (newer) snapshot won; the ambiguous diff is provably inside it.
      assert.strictEqual(conn.book.bids.get('65000'), '3.0');
      assert.strictEqual(emitted.filter((e) => e.type === 'snapshot').length, 1);
      assert.strictEqual(emitted[0].snapshot_asof_ts_ms, SNAP_MS + 2000);
      assert.strictEqual(emitted.filter((e) => e.type === 'update').length, 0);
      assert.strictEqual(conn._stats.snapshotIncludedDiffCount, 1);
    } finally {
      globalThis.fetch = stubbed.original;
    }
  });

  it('never reaches running when the REST snapshot has no source microtimestamp (boundary unprovable)', async () => {
    const conn = new BitstampConnector({ restUrl: 'https://example.test/book' });
    conn._setState('connected');
    const errors = [];
    conn.on('error', (ev) => errors.push(ev));
    const stubbed = stubFetch({ bids: [['65000', '1.0']], asks: [['65001', '2.0']] }); // no microtimestamp
    try {
      await assert.rejects(conn._syncBook(), /no source microtimestamp/);
    } finally {
      globalThis.fetch = stubbed.original;
    }
    assert.strictEqual(conn.getState(), 'error');
    assert.ok(errors.some((e) => String(e.message).includes('no source microtimestamp')));
    assert.strictEqual(conn._stats.resyncCount, 0);
  });

  it('fail-closes on a steady-state diff microtimestamp regression (book rollback guard)', async () => {
    const conn = new BitstampConnector({ restUrl: 'https://example.test/book' });
    conn._ws = { send: () => {} };
    conn._setState('running');
    conn._depthSyncing = false;
    // Suppress real reconnect scheduling so the test asserts state only.
    conn._scheduleReconnect = () => { conn._setState('reconnecting'); };
    const errors = [];
    conn.on('error', (ev) => errors.push(ev));
    const emitted = [];
    conn.on('depth', (event) => emitted.push(event));

    // Healthy newer diff first.
    conn._onMessage(depthFrame([['65000', '1.5']], [], micro(+300)));
    assert.strictEqual(conn.book.bids.get('65000'), '1.5');
    assert.strictEqual(conn._lastAppliedDiffTsMs, SNAP_MS + 300);

    // Regression: an OLDER diff must never re-apply over newer book state.
    conn._onMessage(depthFrame([['65000', '0.2']], [], micro(+100)));
    assert.ok(errors.some((e) => String(e.message).includes('diff microtimestamp regression')));
    // Fail-closed: the gapped frame is dropped and the book is cleared for a
    // full re-sync — the stale diff was never applied over the newer state.
    assert.strictEqual(conn.book.bids.size, 0);
    assert.strictEqual(conn.book.asks.size, 0);
    assert.strictEqual(emitted.length, 1); // only the healthy update was emitted
    assert.strictEqual(conn.getState(), 'reconnecting');
  });

  it('keeps buffered diffs across a failed attempt and covers them with the retry snapshot', async () => {
    // A transient HTTP failure mid-sync must not lose buffered diffs: the
    // retry snapshot's boundary is strictly newer and provably covers them.
    const conn = new BitstampConnector({ restUrl: 'https://example.test/book' });
    conn._setState('connected');
    const failBody = { ok: false };
    const stubbed = stubFetch(REST_BODY, { calls: [failBody, REST_BODY] });
    const emitted = [];
    conn.on('depth', (event) => emitted.push(event));
    try {
      const sync = conn._syncBook();
      await sleep(10);
      conn._onMessage(depthFrame([['65000', '0.7']], [], micro(-100)));
      await sync;

      assert.strictEqual(conn.getState(), 'running');
      assert.strictEqual(conn.book.bids.get('65000'), '1.0'); // stale diff covered by retry snapshot
      assert.strictEqual(conn._stats.snapshotIncludedDiffCount, 1);
      assert.strictEqual(emitted.length, 1);
    } finally {
      globalThis.fetch = stubbed.original;
    }
  });

  it('abandons a superseded REST retry so a stale snapshot never overwrites the newer connection book (Astra audit #19 P1)', async () => {
    const conn = new BitstampConnector({ restUrl: 'https://example.test/book' });
    conn._setState('connected');
    const emitted = [];
    conn.on('depth', (event) => emitted.push(event));

    // Fetch call #1 (old generation, attempt 0) fails; call #2 serves the
    // NEW connection's successful sync (qty 7); call #3 is the OLD
    // generation's retry response (stale qty-1 book) landing only AFTER the
    // new book is already live — the rollback window from the audit repro.
    const staleOldBody = { ...REST_BODY, bids: [['65000', '1.0']] };
    const newBody = { ...REST_BODY, bids: [['65000', '7.0']] };
    const stubbed = stubFetch(null, { calls: [{ ok: false }, newBody, staleOldBody] });
    try {
      const oldSync = conn._syncBook(); // generation 0
      await sleep(30); // attempt 0 has failed; the old sync is in its retry delay
      conn._wsGeneration++; // a reconnect opens a new connection generation
      const newSync = conn._syncBook();
      await newSync;
      assert.strictEqual(conn.getState(), 'running');
      assert.strictEqual(conn.book.bids.get('65000'), '7.0');

      // The old generation's retry response finally lands → must be abandoned
      // (result never applied), leaving the newer sync's book untouched.
      await oldSync;
      assert.strictEqual(conn.getState(), 'running', 'superseded sync must not clobber the newer sync');
      assert.strictEqual(conn.book.bids.get('65000'), '7.0', 'stale snapshot must not roll the book back');
      assert.strictEqual(emitted.filter((e) => e.type === 'snapshot').length, 1,
        'only the newer sync may emit a snapshot');
      assert.strictEqual(conn._stats.resyncCount, 1);
    } finally {
      globalThis.fetch = stubbed.original;
    }
  });

  it('never reaches running when a buffered diff has no source timestamp (boundary unprovable, fail-closed) (Astra audit #19 P1)', async () => {
    const conn = new BitstampConnector({ restUrl: 'https://example.test/book' });
    conn._setState('connected');
    const errors = [];
    conn.on('error', (ev) => errors.push(ev));
    const stubbed = stubFetch(REST_BODY); // valid snapshot served on every attempt
    try {
      const sync = conn._syncBook();
      // Diff with NO source microtimestamp buffers during sync: it may be
      // NEWER than the snapshot, so its inclusion can never be proven and it
      // must not be silently dropped while the sync reports success.
      conn._onMessage({
        event: 'data',
        channel: 'diff_order_book_btcusd',
        data: { bids: [['65000', '5.0']], asks: [], microtimestamp: undefined },
      });
      await assert.rejects(sync, /no source timestamp/);
    } finally {
      globalThis.fetch = stubbed.original;
    }
    // Three attempts, each aborted at the unprovable boundary → error state,
    // never 'running'; the diff was NOT silently drop-counted.
    assert.strictEqual(conn.getState(), 'error', 'unprovable boundary must never reach running');
    assert.strictEqual(conn._stats.boundaryResyncCount, 3);
    assert.strictEqual(conn._stats.resyncCount, 0);
    assert.strictEqual(conn._stats.droppedDepthCount, 0, 'unprovable diff is not silently dropped');
    assert.strictEqual(conn.book.bids.size, 0, 'no partial book may survive');
    assert.ok(errors.some((e) => String(e.message).includes('no source timestamp')));
  });

  it('re-syncs instead of replaying an out-of-order (regressing) buffered diff pair (Astra audit #19 P1)', async () => {
    const conn = new BitstampConnector({ restUrl: 'https://example.test/book' });
    conn._setState('connected');
    const emitted = [];
    conn.on('depth', (event) => emitted.push(event));

    // Buffered during sync in arrival order: ts = B+200ms then ts = B+100ms.
    // Replaying them in arrival order would apply the B+200 diff and then
    // ROLL the book back with the older B+100 diff — the replay path must
    // enforce the same source-time monotonicity as the steady-state guard.
    const newerBody = { ...REST_BODY, bids: [['65000', '9.0']], microtimestamp: micro(+5000) };
    const stubbed = stubFetch(REST_BODY, { calls: [REST_BODY, newerBody] });
    try {
      const sync = conn._syncBook();
      conn._onMessage(depthFrame([['65000', '2.0']], [], micro(+200)));
      conn._onMessage(depthFrame([['65000', '3.0']], [], micro(+100))); // regression vs B+200
      await sync;

      assert.strictEqual(conn.getState(), 'running');
      // The retry snapshot (boundary B+5000) provably covers BOTH diffs —
      // neither is replayed, so the book can never hold the rolled-back value.
      assert.strictEqual(conn.book.bids.get('65000'), '9.0');
      assert.strictEqual(conn._stats.boundaryResyncCount, 1);
      assert.strictEqual(conn._stats.snapshotIncludedDiffCount, 2);
      assert.strictEqual(emitted.filter((e) => e.type === 'snapshot').length, 1,
        'the aborted attempt must not emit a snapshot');
      assert.strictEqual(emitted.filter((e) => e.type === 'update').length, 0,
        'no regressing diff may ever be replayed');
    } finally {
      globalThis.fetch = stubbed.original;
    }
  });
});
