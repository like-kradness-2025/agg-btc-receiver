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
import { closeSync, existsSync, openSync, statSync } from 'node:fs';
import { bindInternals } from './internal/wiring.mjs';

/**
 * One store, one handle, one execution right.
 *
 * A change operation must not start while another one is in progress: the caller's hooks are synchronous
 * functions it hands in, so they can call back into anything on the same stack, and a second BEGIN is not
 * nested by SQLite - it simply fails, which the reception path turns into a permanent stop.
 *
 * Rather than deciding which two handles are looking at one database - which means reading names the way
 * SQLite reads them, down to URI fragments, query options, percent escapes, hard links, symlinks and VFS
 * names - the store is held by one handle at a time. The identity is only ever used to refuse the second
 * open, so a name this module reads imperfectly costs an extra attempt, never a second right.
 */
const OPEN_STORES = new Map();

/** The public refusal of a change operation that arrived while another one was in progress. */
export const REENTRANT_OPERATION = 'REENTRANT_OPERATION';

/**
 * The shape a refused public call answers with: a normal refusal the caller can report, never a
 * durability verdict (that word means the raw refused the write) and never an error (an error here
 * would be read as a store failure and stop reception).
 */
export function reentryRefusal(operation, { acceptedKey = 'accepted' } = {}) {
  return {
    [acceptedKey]: false,
    code: REENTRANT_OPERATION,
    reason: `${operation} may not start while another change operation is being processed`,
    ack: null,
  };
}

/**
 * The file SQLite opened, as an identity - or null when there is no file to hold.
 *
 * `PRAGMA database_list` reports what this connection actually opened, with the URI syntax, the fragment and
 * the query already resolved, and an empty name for anything SQLite keeps in memory (an in-memory database,
 * a shared cache, a VFS like memdb). Nothing here reads a name in a way of its own: a database with no file
 * is refused rather than used, and a file is held by one handle at a time.
 */
function databaseFileKey(db) {
  const main = db.prepare('PRAGMA database_list').all().find((row) => row.name === 'main');
  if (!main || !main.file) return null;
  // No fallback to the name: a file that cannot be identified is one this store cannot promise anything
  // about, and two readings of one file must not be two keys.
  const stats = statSync(main.file);
  return `file:${stats.dev}:${stats.ino}`;
}

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
  // Opening the store writes to it - the schema, and the marker of the run it replaces - so it is a change
  // operation like every other one, and the store is one handle. Nothing here reads a name: the handle is
  // opened, what SQLite says it opened decides the identity, and a database with no file, or a file another
  // handle in this process already holds, is refused before anything is written by this handle.
  const refusedOpen = (reason) => {
    const error = new Error(reason);
    error.code = REENTRANT_OPERATION;
    return error;
  };
  let db;
  let fileKey = null;
  let registered = false;
  let closed = false;
  const right = { busy: false };
  // The boards a structure serves from this store, and the claim each one carries. A second structure over
  // the same board in the same store would share its position and outlive the first close, so it is
  // refused; boards that differ are separate pages of one store (the tables are keyed by the board) and
  // claim separately. The claims live in this closure, so every wrapper of the store carries the same ones.
  const structureOwners = new Map();
  try {
    db = new Database(dbPath);

    // What SQLite opened, before this handle writes anything: no file means no identity to hold, so the
    // database is refused rather than used (an in-memory one, a shared cache, a VFS-backed one).
    try {
      fileKey = databaseFileKey(db);
    } catch (error) {
      throw refusedOpen(`this store's file could not be identified: ${error.message}`);
    }
    if (fileKey === null) {
      throw refusedOpen(
        'this database has no file to be held by: an in-memory, shared-cache or VFS-backed store cannot be used here',
      );
    }
    if (OPEN_STORES.has(fileKey)) {
      throw refusedOpen('this file is already held by another handle in this process');
    }
    // Held from here, before anything is written: the initialisation below calls back into the caller's
    // clock, and a re-open from there must find the file already taken.
    OPEN_STORES.set(fileKey, true);
    registered = true;

    db.exec('PRAGMA journal_mode = WAL');
    db.exec('PRAGMA synchronous = FULL');
    db.exec(SCHEMA);

    // Only an unfinished generation is invalidated. A run that closed cleanly keeps that fact: erasing
    // it would hide the very thing the marker exists to record.
    const invalidate = db.prepare('UPDATE run_marker SET state = ?, at_ms = ? WHERE state = ?');
    // Through the same transaction discipline as everything else: a rollback that fails must not replace the
    // error that caused it, or the reason this store could not open is lost behind the attempt to clean up.
    inTransaction(() => {
      invalidate.run(STATE_INVALIDATED, nowMs(), STATE_RUNNING);
    });
  } catch (error) {
    // The file is freed only if this handle really is closed - the same rule as `close()`. The closing is
    // attempted twice, because a connector can refuse the first one (a statement still open, a driver that
    // needs the transaction to unwind); if it still fails, the file stays held, which refuses a second
    // handle rather than admitting one over a connection that may still be live.
    let closedHere = false;
    if (db) {
      for (let attempt = 0; attempt < 2 && !closedHere; attempt += 1) {
        try {
          db.close();
          closedHere = true;
        } catch {
          // the original failure is the one to report
        }
      }
    }
    if (registered && fileKey !== null && closedHere) OPEN_STORES.delete(fileKey);
    throw error;
  }

  /**
   * Run fn inside one transaction, so that a record cannot land alone.
   *
   * Exposed because every module that owns part of this store needs the same discipline, and because a
   * BEGIN that never succeeded must not be rolled back (there is nothing to roll back) and a rollback
   * that fails must not replace the error that caused it.
   *
   * A caller that has to make two records agree - the ledger's row for a loss and the board's proof, say -
   * opens one transaction around both: the modules write their statements through the connection they are
   * handed, so a transaction taken here covers them all. Nothing below takes a transaction of its own, and
   * a second BEGIN is an error rather than a way to nest.
   */
  function inTransaction(fn) {
    let begun = false;
    try {
      db.exec('BEGIN IMMEDIATE');
      begun = true;
      const result = fn();
      db.exec('COMMIT');
      return result;
    } catch (error) {
      if (begun) {
        try {
          db.exec('ROLLBACK');
        } catch {
          // the original failure is the one to report
        }
      }
      throw error;
    }
  }

  function beginChange() {
    if (right.busy) return false;
    right.busy = true;
    return true;
  }

  function endChange() {
    right.busy = false;
  }

  /**
   * Run fn with the right held, refused if it is not free.
   *
   * Opening a module writes to the store, so the whole initialisation is one change operation: without
   * taking the right around it, a hook the initialisation calls (the caller's clock, a writer) could
   * re-enter, and the nested BEGIN that follows fails and is read as a store failure.
   */
  function whileChange(fn) {
    if (!beginChange()) {
      const error = new Error('an initialisation cannot begin while a change operation is being processed');
      error.code = REENTRANT_OPERATION;
      throw error;
    }
    try {
      return fn();
    } finally {
      endChange();
    }
  }

  function inChange() {
    return right.busy;
  }

  /**
   * Wrap one public change operation in this store's execution right.
   *
   * Every module hands its public names through here, and keeps the unguarded function closed over for
   * itself: one frame's processing is one operation, while a hook the caller handed in reaches the
   * public name and is refused like any other caller. The refusal is a shape the caller already
   * understands - never a durability verdict (that word means the raw refused a write) and never an
   * error, which the reception path would read as a store failure and stop over.
   */
  function guard(operation, run, refusalShape = (refusal) => refusal) {
    return (...args) => {
      if (!beginChange()) return refusalShape(reentryRefusal(operation));
      try {
        return run(...args);
      } finally {
        endChange();
      }
    };
  }

  /** This store's own change operations take the right like every other module's, and refuse the same way. */
  const guarded = (operation, run) => guard(operation, run, () => reentryRefusal(operation, { acceptedKey: 'changed' }));

  const api = {
    db,
    inTransaction,

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
      // Closing is once, and it is the closing that frees the file. A second call changes nothing, so a
      // handle that has been closed cannot free a file another handle has taken since. If the close itself
      // fails the file stays held - refusing a second handle rather than admitting one - and closing can be
      // tried again, because the flag is set only by a close that succeeded.
      if (closed) return;
      db.close();
      closed = true;
      if (fileKey !== null) OPEN_STORES.delete(fileKey);
    },
  };

  // The unguarded routes, kept closed over: they exist so that this store's own names can be public and
  // guarded without recursing into themselves. Nothing outside this file can reach them.
  const internal = {
    beginRun: api.beginRun,
    completeRun: api.completeRun,
    advanceWithBoundary: api.advanceWithBoundary,
    clearBoundary: api.clearBoundary,
    updateReceivedTail: api.updateReceivedTail,
    recordSuspectedGap: api.recordSuspectedGap,
    close: api.close,
  };
  for (const [name, run] of Object.entries(internal)) api[name] = guarded(`store.${name}`, run);
  // A caller's transaction is a change operation like any other. Its refusal is thrown rather than
  // returned: a caller expecting its own function's result must not be handed a refusal object as if it
  // were that result.
  api.inTransaction = guard('store.inTransaction', api.inTransaction, () => {
    const error = new Error('a transaction cannot begin while a change operation is being processed');
    error.code = REENTRANT_OPERATION;
    throw error;
  });

  // Nothing about the right is handed out: a caller that can reach `endChange` can release the right that
  // is holding its own frame, and a caller that can reach the unguarded routes can write without one. The
  // store's own change operations are reachable by their public names, which take the right themselves.
  // `inChange` is a read: a module asked to open while a frame is being processed needs to know that it
  // must refuse, and knowing it cannot change the right.
  //
  // The wiring, however, is handed the right, the transaction discipline and that observation - through
  // `internal/wiring.mjs`, and not on this object. A module that called `store.guard(...)` on the object a
  // caller holds would hand its unguarded route to whoever replaced that method: the route would then be
  // callable from inside a frame, and the same frame would be written twice.
  // Neither the database handle nor the transaction helper is handed out: a caller gets a store it can ask
  // about (and its own change operations), and the wiring gets the rest through `internal/wiring.mjs`.
  const { db: _db, inTransaction: _inTransaction, ...publicApi } = api;
  const exported = { ...publicApi, inChange, REENTRANT_OPERATION };
  bindInternals(exported, {
    // The database handle travels only on this path: a module runs its statements through it, and a caller
    // is never handed a way to write around the structure.
    db,
    guard,
    inTransaction,
    inChange,
    whileChange,
    // The unguarded close: the wiring that holds the right for an operation of its own (a structure's
    // termination) closes the store inside it, and the public name would refuse that as a second operation.
    close: internal.close,
    REENTRANT_OPERATION,
    /**
     * One structure serves a board from a store at a time. A second structure over the same board in the
     * same store - under any object that names it, a wrapper included - is refused before it can share
     * the board's position or outlive the first one's close.
     */
    claimStructureOwner({ market, stream }) {
      const board = JSON.stringify([market, stream]);
      if (structureOwners.has(board)) {
        const error = new Error('this store already serves this board from another structure in this process');
        error.code = REENTRANT_OPERATION;
        throw error;
      }
      // What was claimed is identified by this token: only the claim that made it can give it back, so a
      // close that runs again can never free a board the structure no longer holds.
      const token = {};
      structureOwners.set(board, token);
      return token;
    },
    releaseStructureOwner({ market, stream }, token) {
      const board = JSON.stringify([market, stream]);
      if (structureOwners.get(board) === token) structureOwners.delete(board);
    },
  });
  return exported;
}
