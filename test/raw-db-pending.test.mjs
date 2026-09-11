// test/raw-db-pending.test.mjs — R-13: shutdown drain of the raw-DB pending
// queues.
//
// The regression target: once orderflow_monitor latched `rawDbFailure`,
// closeRawDb() had no retry and no marker, so every event still queued (up to
// 65,536 per queue) vanished at process exit. These tests pin the two
// replacement behaviours:
//   1. a bounded retry flushes events that a transient failure blocked, and
//   2. events that still cannot be written are COUNTED (never silently lost).

import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import fsp from 'node:fs/promises';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import {
  RAW_DB_DROP_REPORT_SCHEMA,
  RAW_DB_PENDING_DEFAULT_MAX_EVENTS,
  RAW_DB_SHUTDOWN_RETRY_ATTEMPTS,
  accumulatePostDrainCanonicalDrops,
  admitPendingEnvelopes,
  buildRawDbDropReport,
  drainPendingQueueWithBoundedRetry,
  ingestSeqRange,
  resolveRawDbPendingMaxEvents,
  resolveRawDbPendingOverflowMode,
  writeRawDbDropReport,
} from '../lib/raw-db-pending.mjs';

const noSleep = async () => {};

/** A writer stub whose append() fails while `failuresLeft > 0`. */
function failingAppender({ failuresLeft = 0, error = new Error('unable to open database file') } = {}) {
  const state = { failuresLeft, attempts: [], appended: [] };
  return {
    state,
    append: async (batch) => {
      state.attempts.push(batch.length);
      if (state.failuresLeft > 0) {
        state.failuresLeft -= 1;
        throw error;
      }
      state.appended.push(...batch);
    },
  };
}

describe('R-13 raw-DB shutdown drain', () => {
  it('retries a transient append failure and flushes every pending event', async () => {
    const pending = Array.from({ length: 5 }, (_, i) => ({ ingest_seq: i + 1 }));
    // First append attempt fails (the case that latches rawDbFailure).
    const appender = failingAppender({ failuresLeft: 1 });
    const onAttemptError = [];

    const result = await drainPendingQueueWithBoundedRetry({
      pending,
      append: appender.append,
      maxBatch: 10,
      sleepFn: noSleep,
      onAttemptError: (error, attempt) => onAttemptError.push({ message: error.message, attempt }),
    });

    assert.deepEqual(result, { flushed: 5, remaining: 0, attempts: 2 });
    assert.equal(pending.length, 0, 'queue fully drained after the retry');
    assert.equal(appender.state.appended.length, 5, 'all events reached the writer');
    assert.deepEqual(
      appender.state.appended.map((e) => e.ingest_seq),
      [1, 2, 3, 4, 5],
      'retry preserves the original event order',
    );
    // The failed batch was put back, not duplicated and not lost.
    assert.deepEqual(appender.state.attempts, [5, 5]);
    assert.deepEqual(onAttemptError, [{ message: 'unable to open database file', attempt: 1 }]);
  });

  it('counts the exact drop when every attempt fails (no silent loss)', async () => {
    const pending = Array.from({ length: 7 }, (_, i) => ({ ingest_seq: i + 1 }));
    const appender = failingAppender({ failuresLeft: Number.MAX_SAFE_INTEGER });

    const result = await drainPendingQueueWithBoundedRetry({
      pending,
      append: appender.append,
      maxBatch: 3,
      sleepFn: noSleep,
    });

    assert.equal(result.flushed, 0);
    assert.equal(result.remaining, 7, 'the drop is counted, not swallowed');
    assert.equal(result.attempts, RAW_DB_SHUTDOWN_RETRY_ATTEMPTS, 'bounded: exactly the configured attempt budget');
    assert.equal(appender.state.attempts.length, RAW_DB_SHUTDOWN_RETRY_ATTEMPTS);
    assert.equal(pending.length, 7, 'unwritten events stay in the queue for the caller to report');
    assert.deepEqual(pending.map((e) => e.ingest_seq), [1, 2, 3, 4, 5, 6, 7], 'queue order preserved');
  });

  it('keeps batches acked before the failure and counts only the rest', async () => {
    const pending = Array.from({ length: 4 }, (_, i) => ({ ingest_seq: i + 1 }));
    // First batch of 2 succeeds, everything after fails permanently.
    let calls = 0;
    const failed = new Error('SQLITE_READONLY');
    const append = async (batch) => {
      calls += 1;
      if (calls > 1) throw failed;
      assert.deepEqual(batch.map((e) => e.ingest_seq), [1, 2]);
    };

    const result = await drainPendingQueueWithBoundedRetry({
      pending, append, maxBatch: 2, sleepFn: noSleep,
    });

    assert.equal(result.flushed, 2, 'the acked prefix is not re-sent');
    assert.equal(result.remaining, 2);
    assert.deepEqual(pending.map((e) => e.ingest_seq), [3, 4]);
  });

  it('does not call append at all when the queue is empty', async () => {
    const appender = failingAppender();
    const result = await drainPendingQueueWithBoundedRetry({
      pending: [], append: appender.append, maxBatch: 2, sleepFn: noSleep,
    });
    assert.deepEqual(result, { flushed: 0, remaining: 0, attempts: 0 });
    assert.equal(appender.state.attempts.length, 0);
  });

  it('builds a drop report that omits empty queues and totals the loss', () => {
    const report = buildRawDbDropReport({
      queues: {
        raw: { remaining: 5, flushed: 2, attempts: 3, firstIngestSeq: 41, lastIngestSeq: 57 },
        canonical: { remaining: 0, flushed: 9, attempts: 1 },
      },
      reason: 'unable to open database file',
      nowMs: 1789000000000,
    });

    assert.equal(report.schema, RAW_DB_DROP_REPORT_SCHEMA);
    assert.equal(report.dropped_events, 5);
    assert.equal(report.reason, 'unable to open database file');
    assert.deepEqual(Object.keys(report.queues), ['raw'], 'a queue with nothing dropped is omitted');
    assert.deepEqual(report.queues.raw, {
      dropped_events: 5,
      flushed_events: 2,
      attempts: 3,
      first_ingest_seq: 41,
      last_ingest_seq: 57,
    });
  });

  it('reports zero dropped events when nothing was lost', () => {
    const report = buildRawDbDropReport({ queues: { raw: { remaining: 0, flushed: 3, attempts: 1 } } });
    assert.equal(report.dropped_events, 0);
    assert.deepEqual(report.queues, {});
  });

  it('reads the ingest_seq range of a queue without mutating it', () => {
    const pending = [{ ingest_seq: 10 }, { ingest_seq: 12 }, {}];
    const before = JSON.stringify(pending);
    assert.deepEqual(ingestSeqRange(pending), { first: 10, last: 12 });
    assert.equal(JSON.stringify(pending), before);
    assert.deepEqual(ingestSeqRange([]), { first: null, last: null });
    assert.deepEqual(ingestSeqRange(undefined), { first: null, last: null });
  });

  it('persists the drop report atomically with no tmp residue', async () => {
    const dir = await fsp.mkdtemp(path.join(os.tmpdir(), 'agg-rawdrop-'));
    const target = path.join(dir, 'raw-db-drop-report.json');
    const report = buildRawDbDropReport({
      queues: { raw: { remaining: 3, flushed: 0, attempts: 3 } }, reason: 'ENOSPC',
    });

    const written = await writeRawDbDropReport(target, report);
    assert.equal(written, target);
    assert.deepEqual(JSON.parse(await fsp.readFile(target, 'utf8')), report);
    assert.deepEqual(fs.readdirSync(dir), ['raw-db-drop-report.json'], 'no leftover tmp file');

    // Overwrite is atomic (existing report replaced, still parseable).
    const second = buildRawDbDropReport({ queues: { raw: { remaining: 9, flushed: 0, attempts: 3 } } });
    await writeRawDbDropReport(target, second);
    assert.equal(JSON.parse(await fsp.readFile(target, 'utf8')).dropped_events, 9);
    await fsp.rm(dir, { recursive: true, force: true });
  });
});

describe('R14: pending-queue cap accounting (dropped envelopes are counted)', () => {
  // Regression target: the pre-fix loop hit the cap, called
  // reportRawDbFailure() and `return`ed — the rest of the batch and every later
  // envelope were dropped with no counter anywhere (every later rejection
  // short-circuited on the latched failure), so a queue overflow was
  // indistinguishable from "the exchange sent nothing".

  it('counts every envelope the cap rejects (count mode)', () => {
    const pending = [];
    const envelopes = Array.from({ length: 5 }, (_, i) => ({ id: i + 1 }));

    // cap = 3, already 1 queued → 2 admitted, 3 counted as dropped.
    pending.push({ id: 0 });
    const result = admitPendingEnvelopes({ pending, envelopes, maxEvents: 3 });

    assert.deepEqual(result, { admitted: 2, rejected: 3, overflowed: true, mode: 'count' });
    assert.deepEqual(pending.map((e) => e.id), [0, 1, 2], 'queue keeps order and stops at the cap');
  });

  it('is exact for a single batch: admitted + rejected = offered', () => {
    for (const [queued, offered, cap] of [[0, 10, 4], [3, 1, 4], [4, 7, 4], [0, 1, 1]]) {
      const pending = Array.from({ length: queued }, (_, i) => ({ i }));
      const result = admitPendingEnvelopes({
        pending, envelopes: Array.from({ length: offered }, (_, i) => ({ i })), maxEvents: cap,
      });
      assert.equal(result.admitted + result.rejected, offered, `queued=${queued} offered=${offered} cap=${cap}`);
      assert.equal(pending.length, Math.min(cap, queued + offered));
    }
  });

  it('never hands a rejected envelope an ingest_seq (prepare runs on admit only)', () => {
    const pending = [];
    let seq = 0;
    const result = admitPendingEnvelopes({
      pending,
      envelopes: Array.from({ length: 5 }, (_, i) => ({ id: i })),
      maxEvents: 2,
      prepare: (envelope) => { envelope.ingest_seq = ++seq; },
    });

    assert.equal(result.rejected, 3);
    assert.equal(seq, 2, 'no sequence number is consumed by a dropped envelope');
    assert.deepEqual(pending.map((e) => e.ingest_seq), [1, 2]);
  });

  it('reproduces the pre-fix silent drop in legacy mode', () => {
    const pending = [{ id: 0 }, { id: 1 }];
    const rest = Array.from({ length: 4 }, (_, i) => ({ id: i + 2 }));

    const result = admitPendingEnvelopes({ pending, envelopes: rest, maxEvents: 2, mode: 'legacy' });

    assert.deepEqual(result, { admitted: 0, rejected: 0, overflowed: true, mode: 'legacy' });
    assert.deepEqual(pending.map((e) => e.id), [0, 1], 'the rest of the batch is abandoned, nothing is counted');
  });

  it('reports no overflow when the cap is not reached', () => {
    const pending = [];
    const result = admitPendingEnvelopes({ pending, envelopes: [{ id: 1 }], maxEvents: 4 });
    assert.deepEqual(result, { admitted: 1, rejected: 0, overflowed: false, mode: 'count' });
  });

  it('admits exactly the same envelopes as the pre-fix loop (equivalence)', () => {
    // Literal transcription of the pre-fix admission loop: on the first
    // rejection it calls reportRawDbFailure() and `return`s, abandoning the rest
    // of the batch and counting nothing. The counted replacement must produce a
    // byte-identical queue for the same input — the change is accounting only.
    const preFixAdmit = (pending, envelopes, maxEvents) => {
      for (const envelope of envelopes ?? []) {
        if (pending.length >= maxEvents) return true; // reportRawDbFailure(...); return;
        pending.push(envelope);
      }
      return false;
    };

    let seed = 987654321;
    const rnd = (n) => { seed = (seed * 1103515245 + 12345) & 0x7fffffff; return seed % n; };
    for (let i = 0; i < 300; i += 1) {
      const maxEvents = 1 + rnd(6);
      const queued = rnd(maxEvents + 3);
      const offered = rnd(12);
      const base = Array.from({ length: queued }, (_, k) => ({ k }));
      const envelopes = Array.from({ length: offered }, (_, k) => ({ k }));
      const preFix = base.map((e) => ({ ...e }));
      const counted = base.map((e) => ({ ...e }));

      const preFixOverflowed = preFixAdmit(preFix, envelopes, maxEvents);
      const result = admitPendingEnvelopes({ pending: counted, envelopes, maxEvents });

      assert.deepEqual(counted, preFix, `queue differs (cap=${maxEvents} queued=${queued} offered=${offered})`);
      assert.equal(result.overflowed, preFixOverflowed, 'the latch trigger is unchanged');
      assert.equal(result.admitted + result.rejected, offered, 'every offered envelope is admitted or counted');
    }
  });

  it('rejects invalid arguments instead of guessing', () => {
    assert.throws(() => admitPendingEnvelopes({ pending: null, envelopes: [], maxEvents: 4 }), TypeError);
    assert.throws(() => admitPendingEnvelopes({ pending: [], envelopes: [], maxEvents: 0 }), TypeError);
    assert.throws(() => admitPendingEnvelopes({ pending: [], envelopes: [], maxEvents: 4, mode: 'nope' }), TypeError);
    assert.deepEqual(
      admitPendingEnvelopes({ pending: [], envelopes: undefined, maxEvents: 4 }),
      { admitted: 0, rejected: 0, overflowed: false, mode: 'count' },
    );
  });

  it('resolves the cap from the environment with the production default', () => {
    assert.equal(resolveRawDbPendingMaxEvents({}), RAW_DB_PENDING_DEFAULT_MAX_EVENTS);
    assert.equal(resolveRawDbPendingMaxEvents({ RECEIVER_RAW_DB_PENDING_MAX_EVENTS: '16' }), 16);
    // Invalid values are reported and ignored, never coerced to 0 (a 0 cap would
    // drop every envelope on the first admission).
    for (const bad of ['0', '-4', '1.5', 'abc']) {
      assert.equal(
        resolveRawDbPendingMaxEvents({ RECEIVER_RAW_DB_PENDING_MAX_EVENTS: bad }),
        RAW_DB_PENDING_DEFAULT_MAX_EVENTS,
        bad,
      );
    }
  });

  it('resolves the accounting mode from the environment', () => {
    assert.equal(resolveRawDbPendingOverflowMode({}), 'count');
    assert.equal(resolveRawDbPendingOverflowMode({ RECEIVER_RAW_DB_PENDING_OVERFLOW_MODE: 'legacy' }), 'legacy');
    assert.equal(resolveRawDbPendingOverflowMode({ RECEIVER_RAW_DB_PENDING_OVERFLOW_MODE: 'count' }), 'count');
    assert.equal(resolveRawDbPendingOverflowMode({ RECEIVER_RAW_DB_PENDING_OVERFLOW_MODE: 'silent' }), 'count');
  });

  it('adds the overflow counts to the drop report without changing the no-overflow shape', () => {
    const overflow = {
      dropped_events: 812,
      cap_events: 64,
      mode: 'count',
      raw: 700,
      canonical: 100,
      open_interest: 12,
      first_ts_ms: 1789000000000,
    };
    const report = buildRawDbDropReport({
      queues: { raw: { remaining: 0, flushed: 64, attempts: 1 } },
      reason: 'raw DB pending queue limit exceeded',
      nowMs: 1789000001000,
      overflow,
    });

    assert.equal(report.dropped_events, 0, 'a drained queue is not a drain loss');
    assert.equal(report.pending_queue_overflow_events, 812);
    assert.deepEqual(report.pending_queue_overflow, {
      cap_events: 64,
      mode: 'count',
      raw: 700,
      canonical: 100,
      open_interest: 12,
      first_ts_ms: 1789000000000,
    });

    // Nothing overflowed → byte-identical v1 document (no new keys).
    const clean = buildRawDbDropReport({
      queues: { raw: { remaining: 2, flushed: 0, attempts: 3 } }, reason: 'ENOSPC', nowMs: 1,
    });
    assert.deepEqual(Object.keys(clean), ['schema', 'ts_ms', 'dropped_events', 'reason', 'queues']);
    assert.equal(buildRawDbDropReport({
      queues: {}, overflow: { ...overflow, dropped_events: 0 },
    }).pending_queue_overflow_events, undefined, 'a zero overflow adds no fields');
  });
});

describe('O-02: canonical frames that arrive after the shutdown drain', () => {
  // Regression target: enqueueCanonicalFrames() started with
  // `if (!rawDbWriter || rawDbFailure) return;`, so a canonical frame delivered
  // after the latch was discarded before admitPendingEnvelopes() and before any
  // counter — the one loss population neither the R-13 drain accounting nor the
  // R14 cap counter can see (no queue ever held the frame, no drain is left).

  it('accumulates the frames and timestamps the first and the last arrival', () => {
    const state = { frames: 0, first_ts_ms: null, last_ts_ms: null };
    accumulatePostDrainCanonicalDrops(state, 2, 1_000);
    accumulatePostDrainCanonicalDrops(state, 1, 2_500);
    assert.deepEqual(state, { frames: 3, first_ts_ms: 1_000, last_ts_ms: 2_500 });
  });

  it('ignores an empty or invalid count so nothing is armed for no loss', () => {
    const state = { frames: 0, first_ts_ms: null, last_ts_ms: null };
    for (const bad of [0, -3, Number.NaN, undefined, null]) {
      accumulatePostDrainCanonicalDrops(state, bad, 42);
    }
    assert.deepEqual(state, { frames: 0, first_ts_ms: null, last_ts_ms: null });
  });

  it('throws on a missing state instead of dropping the count silently', () => {
    assert.throws(() => accumulatePostDrainCanonicalDrops(null, 1), /state must be an object/);
  });

  it('keeps the three loss populations in separate fields', () => {
    // They are disjoint by construction: a drain loss was queued, a cap
    // rejection was refused by the queue, a post-drain arrival was never queued
    // and has no drain left. A consumer summing them must not double count.
    const dropReport = buildRawDbDropReport({
      queues: { canonical: { remaining: 4, flushed: 10, attempts: 3 } },
      reason: 'ENOSPC',
      overflow: { dropped_events: 5, cap_events: 64, mode: 'count', canonical: 5 },
      nowMs: 1_000,
    });
    assert.equal(dropReport.dropped_events, 4, 'drain loss stays its own field');
    assert.equal(dropReport.pending_queue_overflow_events, 5, 'cap loss stays its own field');
    assert.equal(dropReport.post_drain_canonical_dropped_events, undefined,
      'the post-drain count has no report field: the report is written at drain time, '
      + 'and a frame counted here arrives after that write');
  });
});
