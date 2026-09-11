// lib/raw-db-pending.mjs — shutdown-time drain for the main-thread raw-DB
// pending queues (R-13 observability/correctness gap).
//
// orderflow_monitor.mjs re-queues a failed append batch
// (`rawDbPending.unshift(...batch)`), but once reportRawDbFailure() latches
// `rawDbFailure`, flushRawDbQueue()/flushCanonicalDbQueue() return immediately
// and closeRawDb() has no path that retries — every event still queued (up to
// RAW_DB_PENDING_MAX_EVENTS = 65,536 per queue) disappeared at process exit
// with no marker at all, indistinguishable downstream from a genuine no-trade
// interval.
//
// This module owns the two decisions that were missing:
//
//   1. bounded retry  — a latched failure may be transient (device busy, a
//      momentary ENOSPC, SQLITE_BUSY). A shutdown drain therefore gets a small,
//      fixed number of attempts regardless of the latch, so recoverable cases
//      are flushed instead of dropped.
//   2. explicit counted drop — when the attempts are exhausted, the events that
//      really could not be written are COUNTED and reported (drop report file +
//      health.jsonl), so the loss is observable rather than silent.
//
// The module is pure queue/IO logic with injectable sleep so the retry and
// drop accounting are unit-testable without a live receiver.

import fsp from 'node:fs/promises';
import path from 'node:path';

/** Attempts a shutdown drain makes per queue (1 initial + retries). */
export const RAW_DB_SHUTDOWN_RETRY_ATTEMPTS = 3;
/** Delay between shutdown-drain attempts. */
export const RAW_DB_SHUTDOWN_RETRY_DELAY_MS = 200;
/** Schema id of the durable drop report written next to the health log. */
export const RAW_DB_DROP_REPORT_SCHEMA = 'receiver-raw-db-drop-report/v1';

const defaultSleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

/**
 * Drain `pending` into `append` with a bounded retry budget.
 *
 * Semantics:
 *   - the queue is drained in `maxBatch` sized batches, in order;
 *   - a rejected batch is put back at the FRONT of the queue (order preserved)
 *     and the whole drain is retried up to `attempts` times;
 *   - batches already acknowledged before the failure stay flushed (no
 *     re-send, so an append that partially succeeded is not duplicated);
 *   - after the final attempt the caller-visible `remaining` is the exact
 *     number of events that could NOT be written — the caller must report it.
 *
 * @param {Object} opts
 * @param {Array} opts.pending            queue mutated in place
 * @param {(batch: Array) => Promise<any>} opts.append
 * @param {number} opts.maxBatch
 * @param {number} [opts.attempts]
 * @param {number} [opts.retryDelayMs]
 * @param {(ms: number) => Promise<void>} [opts.sleepFn]
 * @param {(error: Error, attempt: number) => void} [opts.onAttemptError]
 * @returns {Promise<{flushed: number, remaining: number, attempts: number}>}
 */
export async function drainPendingQueueWithBoundedRetry({
  pending,
  append,
  maxBatch,
  attempts = RAW_DB_SHUTDOWN_RETRY_ATTEMPTS,
  retryDelayMs = RAW_DB_SHUTDOWN_RETRY_DELAY_MS,
  sleepFn = defaultSleep,
  onAttemptError = null,
}) {
  if (!Array.isArray(pending)) throw new TypeError('pending must be an array');
  if (typeof append !== 'function') throw new TypeError('append must be a function');
  if (!Number.isInteger(maxBatch) || maxBatch < 1) throw new TypeError('maxBatch must be a positive integer');
  if (!Number.isInteger(attempts) || attempts < 1) throw new TypeError('attempts must be a positive integer');

  let flushed = 0;
  let used = 0;

  for (let attempt = 1; attempt <= attempts && pending.length > 0; attempt += 1) {
    used = attempt;
    try {
      while (pending.length > 0) {
        const batch = pending.splice(0, maxBatch);
        try {
          await append(batch);
        } catch (error) {
          // Put the failed batch back at the front, in order, before retrying.
          pending.unshift(...batch);
          throw error;
        }
        flushed += batch.length;
      }
    } catch (error) {
      if (onAttemptError) onAttemptError(error, attempt);
      if (attempt < attempts && pending.length > 0) await sleepFn(retryDelayMs);
    }
  }

  return { flushed, remaining: pending.length, attempts: used };
}

/**
 * Build the durable drop report for the queues that still hold events.
 * Empty queues are omitted; `dropped_events` is the exact total.
 *
 * @param {Object} opts
 * @param {Object<string, {remaining: number, flushed: number, attempts: number, firstIngestSeq?: number|null, lastIngestSeq?: number|null}>} opts.queues
 * @param {string|null} [opts.reason] underlying write failure message
 * @param {number} [opts.nowMs]
 * @returns {Object} receiver-raw-db-drop-report/v1 document
 */
export function buildRawDbDropReport({ queues, reason = null, nowMs = Date.now() }) {
  const reportQueues = {};
  let dropped = 0;
  for (const [name, queue] of Object.entries(queues ?? {})) {
    const remaining = queue?.remaining ?? 0;
    if (remaining <= 0) continue;
    reportQueues[name] = {
      dropped_events: remaining,
      flushed_events: queue.flushed ?? 0,
      attempts: queue.attempts ?? 0,
      first_ingest_seq: queue.firstIngestSeq ?? null,
      last_ingest_seq: queue.lastIngestSeq ?? null,
    };
    dropped += remaining;
  }
  return {
    schema: RAW_DB_DROP_REPORT_SCHEMA,
    ts_ms: nowMs,
    dropped_events: dropped,
    reason,
    queues: reportQueues,
  };
}

/**
 * First/last `ingest_seq` of a pending queue (null when the queue is empty or
 * the envelopes carry no sequence, e.g. canonical frames). Read-only.
 */
export function ingestSeqRange(pending) {
  if (!Array.isArray(pending) || pending.length === 0) return { first: null, last: null };
  const seqs = pending.map((e) => e?.ingest_seq).filter((s) => Number.isFinite(s));
  if (seqs.length === 0) return { first: null, last: null };
  return { first: Math.min(...seqs), last: Math.max(...seqs) };
}

/**
 * Atomically persist the drop report (tmp file + rename in the same dir).
 * A failure to write the report is the caller's to report — it must not be
 * swallowed silently.
 */
export async function writeRawDbDropReport(filePath, report) {
  const destination = path.resolve(filePath);
  await fsp.mkdir(path.dirname(destination), { recursive: true });
  const temporary = `${destination}.tmp-${process.pid}`;
  await fsp.writeFile(temporary, `${JSON.stringify(report, null, 2)}\n`, 'utf8');
  await fsp.rename(temporary, destination);
  return destination;
}
