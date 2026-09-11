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
  RAW_DB_SHUTDOWN_RETRY_ATTEMPTS,
  buildRawDbDropReport,
  drainPendingQueueWithBoundedRetry,
  ingestSeqRange,
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
