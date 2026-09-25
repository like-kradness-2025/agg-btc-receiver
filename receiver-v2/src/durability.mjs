/**
 * The three records that make a restart honest.
 *
 * Everything here exists to answer one question after a crash: what can this process still claim to
 * have? Three records, each with one job:
 *
 *  - pending_boundary   written in the *same transaction* as the watermark it belongs to, and removed
 *                       only once the derived work is applied. It is how "the raw was advanced but
 *                       the derived state was not" becomes a recoverable state instead of a silent
 *                       inconsistency. Resolution at startup is fail-closed: either apply it or roll
 *                       the boundary back.
 *  - received_tail      the highest sequence this process can show it received, per connection. It
 *                       is a *lower bound only*: it lags reality by the update interval, so it can
 *                       never prove completeness, and it must never be used as if it could.
 *  - run_marker         which generation owns this store. Startup invalidates the previous one before
 *                       anything else happens, so a later abnormal exit cannot be mistaken for a
 *                       clean shutdown; only a run that has stopped receiving and acknowledged
 *                       everything writes "complete".
 *
 * Anything that cannot be proven is recorded as a suspected gap rather than smoothed over: the range
 * from the last durable position to the moment continuity was re-established is what a crash leaves
 * behind, and it stays in the store as a fact.
 */

import { DatabaseSync } from 'node:sqlite';

export const SCHEMA = `
CREATE TABLE IF NOT EXISTS pending_boundary (
  market TEXT NOT NULL,
  stream TEXT NOT NULL,
  boundary_seq INTEGER NOT NULL,
  boundary_ts_ms INTEGER NOT NULL,
  run_id TEXT NOT NULL,
  created_at_ms INTEGER NOT NULL,
  state TEXT NOT NULL DEFAULT 'pending',
  PRIMARY KEY (market, stream)
);
CREATE TABLE IF NOT EXISTS received_tail (
  connection_id TEXT NOT NULL PRIMARY KEY,
  market TEXT NOT NULL,
  last_received_seq INTEGER NOT NULL,
  last_recv_mono_ns INTEGER NOT NULL,
  updated_at_ms INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS run_marker (
  run_id TEXT NOT NULL PRIMARY KEY,
  state TEXT NOT NULL,
  at_ms INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS suspected_gap (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  market TEXT NOT NULL,
  stream TEXT NOT NULL,
  from_ms INTEGER,
  to_ms INTEGER NOT NULL,
  reason TEXT NOT NULL,
  recorded_at_ms INTEGER NOT NULL
);
`;

const STATE_RUNNING = 'running';
const STATE_COMPLETE = 'complete';
const STATE_INVALIDATED = 'invalidated';

/**
 * Open the store for one run.
 *
 * The previous generation's marker is invalidated here, before the caller can record anything, and
 * that happens in the same transaction as the write. Doing it later would leave a window in which a
 * crash looks like a clean shutdown.
 */
export function openDurability({ path: dbPath, runId, nowMs = () => Date.now(), Database = DatabaseSync }) {
  if (!dbPath) throw new TypeError('durability needs a path');
  if (!runId) throw new TypeError('durability needs a run id');
  const db = new Database(dbPath);
  db.exec('PRAGMA journal_mode = WAL');
  db.exec('PRAGMA synchronous = FULL');
  db.exec(SCHEMA);

  // Only an unfinished generation is invalidated. A run that closed cleanly keeps that fact: erasing
  // it would hide the very thing the marker exists to record.
  const invalidate = db.prepare('UPDATE run_marker SET state = ?, at_ms = ? WHERE state = ?');
  db.exec('BEGIN IMMEDIATE');
  try {
    invalidate.run(STATE_INVALIDATED, nowMs(), STATE_RUNNING);
    db.exec('COMMIT');
  } catch (error) {
    db.exec('ROLLBACK');
    throw error;
  }

  /** Run fn inside one transaction: the caller uses this so a record cannot land alone. */
  function inTransaction(fn) {
    db.exec('BEGIN IMMEDIATE');
    try {
      const result = fn();
      db.exec('COMMIT');
      return result;
    } catch (error) {
      db.exec('ROLLBACK');
      throw error;
    }
  }

  return {
    db,

    /** Mark this generation as the live one. Called once, before any data is trusted. */
    beginRun() {
      db.prepare('INSERT OR REPLACE INTO run_marker (run_id, state, at_ms) VALUES (?, ?, ?)').run(
        runId,
        STATE_RUNNING,
        nowMs(),
      );
    },

    /**
     * Mark this generation complete. The caller may only reach here after reception has stopped and
     * everything received has been acknowledged as durable.
     */
    completeRun() {
      db.prepare('INSERT OR REPLACE INTO run_marker (run_id, state, at_ms) VALUES (?, ?, ?)').run(
        runId,
        STATE_COMPLETE,
        nowMs(),
      );
    },

    /** Which generation last closed cleanly, if any. Anything else means the tail is unknown. */
    lastCompleteRun() {
      const row = db
        .prepare('SELECT run_id, at_ms FROM run_marker WHERE state = ? ORDER BY at_ms DESC LIMIT 1')
        .get(STATE_COMPLETE);
      return row ? { runId: row.run_id, atMs: row.at_ms } : null;
    },

    /**
     * Advance the watermark and its boundary record together. Passing the watermark update in means
     * the two cannot get out of step: a crash leaves either both or neither.
     */
    advanceWithBoundary({ market, stream, boundarySeq, boundaryTsMs, updateWatermark }) {
      return inTransaction(() => {
        updateWatermark();
        db.prepare(
          `INSERT OR REPLACE INTO pending_boundary
             (market, stream, boundary_seq, boundary_ts_ms, run_id, created_at_ms, state)
           VALUES (?, ?, ?, ?, ?, ?, 'pending')`,
        ).run(market, stream, boundarySeq, boundaryTsMs, runId, nowMs());
      });
    },

    /** The derived work is applied: the boundary is no longer owed. */
    clearBoundary({ market, stream }) {
      db.prepare('DELETE FROM pending_boundary WHERE market = ? AND stream = ?').run(market, stream);
    },

    /** Boundaries still owed at startup. Each one must be applied or rolled back, never ignored. */
    pendingBoundaries() {
      return db
        .prepare('SELECT market, stream, boundary_seq, boundary_ts_ms, run_id, created_at_ms FROM pending_boundary ORDER BY created_at_ms')
        .all()
        .map((row) => ({
          market: row.market,
          stream: row.stream,
          boundarySeq: row.boundary_seq,
          boundaryTsMs: row.boundary_ts_ms,
          runId: row.run_id,
          createdAtMs: row.created_at_ms,
        }));
    },

    /**
     * Record how far this process can prove it received. A lower bound, deliberately: it is written
     * every interval rather than every frame, and nothing may treat it as a completeness claim.
     */
    updateReceivedTail({ connectionId, market, lastReceivedSeq, lastRecvMonoNs }) {
      db.prepare(
        `INSERT OR REPLACE INTO received_tail
           (connection_id, market, last_received_seq, last_recv_mono_ns, updated_at_ms)
         VALUES (?, ?, ?, ?, ?)`,
      ).run(connectionId, market, lastReceivedSeq, lastRecvMonoNs, nowMs());
    },

    readReceivedTail(connectionId) {
      const row = db.prepare('SELECT * FROM received_tail WHERE connection_id = ?').get(connectionId);
      if (!row) return null;
      return {
        connectionId: row.connection_id,
        market: row.market,
        lastReceivedSeq: row.last_received_seq,
        lastRecvMonoNs: row.last_recv_mono_ns,
        updatedAtMs: row.updated_at_ms,
      };
    },

    /**
     * A range that cannot be proven either way. Recorded, never deleted, and never reported as
     * complete: the point is that the loss stays visible after the fact.
     */
    recordSuspectedGap({ market, stream, fromMs = null, toMs, reason }) {
      db.prepare(
        `INSERT INTO suspected_gap (market, stream, from_ms, to_ms, reason, recorded_at_ms)
         VALUES (?, ?, ?, ?, ?, ?)`,
      ).run(market, stream, fromMs, toMs, reason, nowMs());
    },

    suspectedGaps({ market } = {}) {
      const rows = market
        ? db.prepare('SELECT * FROM suspected_gap WHERE market = ? ORDER BY id').all(market)
        : db.prepare('SELECT * FROM suspected_gap ORDER BY id').all();
      return rows.map((row) => ({
        id: row.id,
        market: row.market,
        stream: row.stream,
        fromMs: row.from_ms,
        toMs: row.to_ms,
        reason: row.reason,
        recordedAtMs: row.recorded_at_ms,
      }));
    },

    close() {
      db.close();
    },
  };
}
