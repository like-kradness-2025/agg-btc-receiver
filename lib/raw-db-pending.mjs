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
/** Cap on the main-thread raw-DB pending queues (orderflow_monitor.mjs). */
export const RAW_DB_PENDING_DEFAULT_MAX_EVENTS = 65_536;
/**
 * R14 overflow-accounting modes:
 *   `count`  — every envelope rejected at the cap is counted (production).
 *   `legacy` — pre-fix behaviour: drop the rest of the batch, count nothing.
 *              Kept for A/B measurements and rollback only.
 */
export const RAW_DB_PENDING_OVERFLOW_MODES = Object.freeze(['count', 'legacy']);
export const RAW_DB_PENDING_OVERFLOW_MODE_DEFAULT = 'count';

const defaultSleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

/**
 * Resolve the pending-queue cap. `RECEIVER_RAW_DB_PENDING_MAX_EVENTS` exists so
 * the overflow path can be exercised against the real receiver with a small cap
 * (it otherwise needs a multi-minute writer stall plus >65k envelopes); an
 * invalid value is reported and ignored, never silently coerced.
 */
export function resolveRawDbPendingMaxEvents(env = process.env) {
  const raw = env?.RECEIVER_RAW_DB_PENDING_MAX_EVENTS;
  if (raw === undefined || raw === null || raw === '') return RAW_DB_PENDING_DEFAULT_MAX_EVENTS;
  const parsed = Number(raw);
  if (!Number.isInteger(parsed) || parsed < 1) {
    console.error(
      `[raw-db-pending] ignoring invalid RECEIVER_RAW_DB_PENDING_MAX_EVENTS=${raw} `
      + `(must be a positive integer); using ${RAW_DB_PENDING_DEFAULT_MAX_EVENTS}`,
    );
    return RAW_DB_PENDING_DEFAULT_MAX_EVENTS;
  }
  return parsed;
}

/** Resolve the overflow-accounting mode (see RAW_DB_PENDING_OVERFLOW_MODES). */
export function resolveRawDbPendingOverflowMode(env = process.env) {
  const raw = env?.RECEIVER_RAW_DB_PENDING_OVERFLOW_MODE;
  if (raw === undefined || raw === null || raw === '') return RAW_DB_PENDING_OVERFLOW_MODE_DEFAULT;
  if (RAW_DB_PENDING_OVERFLOW_MODES.includes(raw)) return raw;
  console.error(
    `[raw-db-pending] ignoring invalid RECEIVER_RAW_DB_PENDING_OVERFLOW_MODE=${raw} `
    + `(expected one of ${RAW_DB_PENDING_OVERFLOW_MODES.join('|')}); using ${RAW_DB_PENDING_OVERFLOW_MODE_DEFAULT}`,
  );
  return RAW_DB_PENDING_OVERFLOW_MODE_DEFAULT;
}

/**
 * R14: admit envelopes into a pending queue, accounting for the ones the cap
 * rejects.
 *
 * The pre-fix loop (`if (pending.length >= MAX) { reportFailure(); return; }`)
 * abandoned the rest of the batch and counted nothing, and every later rejection
 * short-circuited on the already-latched failure — so an overflow was invisible
 * in health.jsonl and in the drop report, exactly like a no-trade interval.
 *
 * Semantics:
 *   - admitted envelopes are pushed in order; `prepare(envelope, index)` runs
 *     immediately before the push (used for the monotonic `ingest_seq`), so a
 *     rejected envelope never consumes a sequence number;
 *   - `count` mode counts EVERY rejected envelope (`rejected`), and the queue is
 *     left untouched — the rejection set is identical to the pre-fix drop set;
 *   - `legacy` mode reproduces the pre-fix drop with no accounting.
 *
 * @param {Object} opts
 * @param {Array} opts.pending              queue mutated in place
 * @param {Array} opts.envelopes            candidate envelopes
 * @param {number} opts.maxEvents           cap (`pending.length >= cap` rejects)
 * @param {string} [opts.mode]
 * @param {((envelope: any, admittedIndex: number) => void)|null} [opts.prepare]
 * @returns {{admitted: number, rejected: number, overflowed: boolean, mode: string}}
 *          `rejected` is the counted loss (0 in legacy mode); `overflowed` says
 *          the cap was hit even when nothing was counted.
 */
export function admitPendingEnvelopes({
  pending,
  envelopes,
  maxEvents,
  mode = RAW_DB_PENDING_OVERFLOW_MODE_DEFAULT,
  prepare = null,
}) {
  if (!Array.isArray(pending)) throw new TypeError('pending must be an array');
  if (!Number.isInteger(maxEvents) || maxEvents < 1) throw new TypeError('maxEvents must be a positive integer');
  if (!RAW_DB_PENDING_OVERFLOW_MODES.includes(mode)) {
    throw new TypeError(`mode must be one of ${RAW_DB_PENDING_OVERFLOW_MODES.join('|')}`);
  }

  let admitted = 0;
  let rejected = 0;
  for (const envelope of envelopes ?? []) {
    if (pending.length >= maxEvents) {
      if (mode === 'legacy') return { admitted, rejected: 0, overflowed: true, mode };
      rejected += 1;
      continue;
    }
    if (prepare) prepare(envelope, admitted);
    pending.push(envelope);
    admitted += 1;
  }
  return { admitted, rejected, overflowed: rejected > 0, mode };
}

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
 * O-03: the raw-DB loss populations and every field that publishes them, so a
 * consumer can count each population exactly once.
 *
 * Cycle 13 measured the trap directly: the same counted loss is mirrored on two
 * surfaces under different names — the final health.jsonl row (the canonical
 * surface: it is the only one that can carry all three populations) and
 * `raw-db-drop-report.json` (written at drain time, so it can never carry
 * `postDrain`). Summing both surfaces double counted the drain and cap
 * populations and produced a NEGATIVE identity (`gap=-425` on a 14-market leg).
 *
 * Use sumRawDbLoss() rather than hand-summing fields; this registry is the map
 * that resolver and the structural test in test/raw-db-pending.test.mjs read, so
 * adding a fourth loss field to lib/health-monitor.mjs without registering it
 * here fails the suite.
 *
 * @type {Readonly<Object<string, {id: string, healthField: string, reportField: string|null}>>}
 */
export const RAW_DB_LOSS_POPULATIONS = Object.freeze({
  drain: Object.freeze({
    id: 'drain',
    healthField: 'raw_db_dropped_events',
    reportField: 'dropped_events',
  }),
  capRejected: Object.freeze({
    id: 'capRejected',
    healthField: 'raw_db_pending_overflow_events',
    reportField: 'pending_queue_overflow_events',
  }),
  postDrain: Object.freeze({
    id: 'postDrain',
    healthField: 'raw_db_post_drain_dropped_events',
    reportField: null,
  }),
});

/** Surface that owns each loss total when both surfaces are readable (O-03). */
export const RAW_DB_LOSS_CANONICAL_SURFACE = 'health';

/**
 * Total the raw-DB loss from the two surfaces it is published on, counting each
 * population exactly once (O-03).
 *
 * The health row wins for every population it carries (canonical surface); the
 * drop report is only read for a population the row does not carry — which is
 * how a report file stays analysable after its health generation rotated away.
 * Fields that are missing or not finite count as 0, and the three populations
 * are disjoint by construction (queued / refused at the cap / arrived too late
 * to be queued), so `total` is the run's real loss.
 *
 * @param {Object} [surfaces]
 * @param {Object|null} [surfaces.healthRow] final health.jsonl row (any row)
 * @param {Object|null} [surfaces.dropReport] raw-db-drop-report.json document
 * @returns {{total: number, drain: number, capRejected: number, postDrain: number, from: 'health'|'report'|'mixed'|'none'}}
 */
export function sumRawDbLoss({ healthRow = null, dropReport = null } = {}) {
  const row = healthRow && typeof healthRow === 'object' ? healthRow : null;
  const report = dropReport && typeof dropReport === 'object' ? dropReport : null;
  const finite = (source, field) => {
    if (!source || !field) return null;
    const value = source[field];
    return Number.isFinite(value) ? value : null;
  };

  const counts = { drain: 0, capRejected: 0, postDrain: 0 };
  const surfaces = new Set();
  for (const population of Object.values(RAW_DB_LOSS_POPULATIONS)) {
    const fromRow = finite(row, population.healthField);
    if (fromRow !== null) {
      counts[population.id] = fromRow;
      surfaces.add(RAW_DB_LOSS_CANONICAL_SURFACE);
      continue;
    }
    const fromReport = finite(report, population.reportField);
    if (fromReport !== null) {
      counts[population.id] = fromReport;
      surfaces.add('report');
    }
  }

  const total = counts.drain + counts.capRejected + counts.postDrain;
  const from = surfaces.size === 0 ? 'none' : surfaces.size === 2 ? 'mixed' : [...surfaces][0];
  return { total, ...counts, from };
}

/**
 * Build the durable drop report for the queues that still hold events.
 * Empty queues are omitted; `dropped_events` is the exact total.
 *
 * R14: `overflow` (optional) carries the envelopes that were rejected at the
 * pending-queue cap. It is additive: the field set is unchanged when nothing
 * overflowed, so existing consumers keep parsing a v1 document. Note the two
 * numbers are disjoint — `dropped_events` are queued events a failed drain could
 * not write, `pending_queue_overflow_events` are events the queue never accepted.
 *
 * O-03: both of those totals are ALSO published under different names in the
 * final health.jsonl row (`raw_db_dropped_events`, `raw_db_pending_overflow_events`),
 * and the row additionally carries `raw_db_post_drain_dropped_events`, which this
 * report cannot express. Never add this document's fields to the row's fields —
 * the populations are disjoint but the surfaces are mirrors. Use sumRawDbLoss(),
 * or the two numbers are counted twice (cycle 13: `gap=-425`).
 *
 * @param {Object} opts
 * @param {Object<string, {remaining: number, flushed: number, attempts: number, firstIngestSeq?: number|null, lastIngestSeq?: number|null}>} opts.queues
 * @param {string|null} [opts.reason] underlying write failure message
 * @param {number} [opts.nowMs]
 * @param {{dropped_events?: number, cap_events?: number, mode?: string, raw?: number, canonical?: number, open_interest?: number, first_ts_ms?: number|null}|null} [opts.overflow]
 * @returns {Object} receiver-raw-db-drop-report/v1 document
 */
export function buildRawDbDropReport({ queues, reason = null, nowMs = Date.now(), overflow = null }) {
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
  const overflowEvents = Number.isFinite(overflow?.dropped_events) && overflow.dropped_events > 0
    ? overflow.dropped_events
    : 0;
  return {
    schema: RAW_DB_DROP_REPORT_SCHEMA,
    ts_ms: nowMs,
    dropped_events: dropped,
    reason,
    ...(overflowEvents > 0 ? {
      pending_queue_overflow_events: overflowEvents,
      pending_queue_overflow: {
        cap_events: overflow?.cap_events ?? null,
        mode: overflow?.mode ?? null,
        raw: overflow?.raw ?? 0,
        canonical: overflow?.canonical ?? 0,
        open_interest: overflow?.open_interest ?? 0,
        first_ts_ms: overflow?.first_ts_ms ?? null,
      },
    } : {}),
    queues: reportQueues,
  };
}

/**
 * O-02: accumulate canonical frames that arrived after the shutdown drain had
 * already run.
 *
 * These frames are the one population the R-13/R14 accounting cannot reach:
 * `raw_db_dropped_events` covers events a drain could not write, the overflow
 * counter covers envelopes a queue rejected at its cap, and both are evaluated
 * while a drain is still possible. A frame delivered once the drain is finished
 * has no queue attempt left, so it is counted here and surfaced by the caller
 * (final health.jsonl row + stderr) instead of being dropped silently the way
 * the pre-fix `rawDbFailure` guard did.
 *
 * Mutates `state` in place (same shape the health row publishes) and returns it.
 *
 * @param {{frames: number, first_ts_ms: number|null, last_ts_ms: number|null}} state
 * @param {number} count frames in this arrival (<= 0 is ignored)
 * @param {number} [nowMs]
 * @returns {{frames: number, first_ts_ms: number|null, last_ts_ms: number|null}}
 */
export function accumulatePostDrainCanonicalDrops(state, count, nowMs = Date.now()) {
  if (!state || typeof state !== 'object') throw new TypeError('state must be an object');
  const frames = Number.isFinite(count) ? count : 0;
  if (frames <= 0) return state;
  state.frames = (Number.isFinite(state.frames) ? state.frames : 0) + frames;
  if (state.first_ts_ms === null || state.first_ts_ms === undefined) state.first_ts_ms = nowMs;
  state.last_ts_ms = nowMs;
  return state;
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
