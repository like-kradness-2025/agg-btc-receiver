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
});
