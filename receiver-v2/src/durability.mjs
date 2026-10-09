/**
 * The two records that make a restart honest.
 *
 * Everything here exists to answer one question after a crash: what can this process still claim to
 * have? Each record has one job:
 *
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
CREATE TABLE IF NOT EXISTS received_tail (
  connection_id TEXT NOT NULL,
  stream TEXT NOT NULL DEFAULT '',
  market TEXT NOT NULL,
  last_received_seq INTEGER NOT NULL,
  last_recv_mono_ns INTEGER NOT NULL,
  updated_at_ms INTEGER NOT NULL,
  PRIMARY KEY (connection_id, stream)
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
 * Migrate a `received_tail` written before the stream was part of its key.
 *
 * A connection name is made of run, venue, market and generation (C2) and does not carry the stream, so the
 * same connection legitimately has a book tail and a trades tail. The old table could hold only one row per
 * connection, so the two boards were overwriting each other's tail - which is a lower bound, but a lower
 * bound for the wrong board is a lie about which frames arrived. The key gains the stream; old rows have no
 * stream recorded and are stored with the empty marker rather than a guess, exactly as a run-less owner is
 * (there is no way to tell which stream they belonged to, and inventing one would attribute a tail to a
 * board that never wrote it).
 *
 * The primary key cannot be changed in place, so the table is rebuilt inside one transaction: a crash
 * leaves either the old table or the new one, never a half-copied set.
 */
function migrateReceivedTail(db) {
  const columns = db
    .prepare('PRAGMA table_info(received_tail)')
    .all()
    .map((row) => row.name);
  if (columns.includes('stream')) return;
  db.exec('BEGIN IMMEDIATE');
  try {
    db.exec(`CREATE TABLE received_tail_migrated (
      connection_id TEXT NOT NULL,
      stream TEXT NOT NULL DEFAULT '',
      market TEXT NOT NULL,
      last_received_seq INTEGER NOT NULL,
      last_recv_mono_ns INTEGER NOT NULL,
      updated_at_ms INTEGER NOT NULL,
      PRIMARY KEY (connection_id, stream)
    )`);
    db.exec(`INSERT INTO received_tail_migrated
        (connection_id, stream, market, last_received_seq, last_recv_mono_ns, updated_at_ms)
      SELECT connection_id, '', market, last_received_seq, last_recv_mono_ns, updated_at_ms
        FROM received_tail`);
    db.exec('DROP TABLE received_tail');
    db.exec('ALTER TABLE received_tail_migrated RENAME TO received_tail');
    db.exec('COMMIT');
  } catch (error) {
    try {
      db.exec('ROLLBACK');
    } catch {
      // the original failure is the one to report
    }
    throw error;
  }
}


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
  // Set 6c: a batch transaction, open across a run of work. Modules that make a run of frames
  // durable - the book's applyBatch - open one, and every transaction they would otherwise take
  // joins it instead of nesting (a second BEGIN is an error, so joining is the only shape a run
  // can commit together in). The commit happens when the batch closes; a failure anywhere rolls
  // the whole batch back, and the caller is responsible for putting its own in-memory state back
  // the way the rollback leaves the store.
  let batchOpen = false;
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
    inTransaction(() => {
      db.exec(SCHEMA);
      db.exec('DROP TABLE IF EXISTS pending_boundary');
    });
    // An older store's received_tail has no stream in its key; the schema above leaves that old table
    // in place, so it is rebuilt here before anything reads or writes a tail.
    migrateReceivedTail(db);

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
  function beginBatchTransaction() {
    if (batchOpen) throw new Error('a batch transaction is already open');
    db.exec('BEGIN IMMEDIATE');
    batchOpen = true;
  }

  function endBatchTransaction(commit = true) {
    if (!batchOpen) throw new Error('no batch transaction is open');
    if (commit) {
      try {
        db.exec('COMMIT');
      } catch (error) {
        // A commit that fails leaves the transaction open: it must be rolled back here, or the
        // next batch's BEGIN would be refused by the transaction nobody closed. The original
        // failure is the one to report.
        try {
          db.exec('ROLLBACK');
        } catch {
          // the original failure is the one to report
        }
        batchOpen = false;
        throw error;
      }
      batchOpen = false;
      return;
    }
    db.exec('ROLLBACK');
    batchOpen = false;
  }

  function inTransaction(fn) {
    if (batchOpen) {
      // This work joins the open batch: it becomes part of the one transaction the batch commits,
      // and a failure of any of it rolls the whole batch back.
      return fn();
    }
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
    beginBatchTransaction,
    endBatchTransaction,

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
     * Record how far this process can prove it received. A lower bound, deliberately: it is written on
     * the reception side as frames arrive rather than being derived from anything durable, so it can lag
     * the socket by the frame being processed, and nothing may treat it as a completeness claim.
     *
     * The tail belongs to one board: a connection name is shared by the market's book and its trades
     * (C2), so the stream is part of what identifies the row. A caller that has no stream to give is
     * recorded under the empty marker rather than attributed to a board that never wrote it.
     */
    updateReceivedTail({ connectionId, market, stream = '', lastReceivedSeq, lastRecvMonoNs }) {
      db.prepare(
        `INSERT OR REPLACE INTO received_tail
           (connection_id, stream, market, last_received_seq, last_recv_mono_ns, updated_at_ms)
         VALUES (?, ?, ?, ?, ?, ?)`,
      ).run(connectionId, stream ?? '', market, lastReceivedSeq, lastRecvMonoNs, nowMs());
    },

    readReceivedTail(connectionId, stream = '') {
      const row = db
        .prepare('SELECT * FROM received_tail WHERE connection_id = ? AND stream = ?')
        .get(connectionId, stream ?? '');
      if (!row) return null;
      return {
        connectionId: row.connection_id,
        stream: row.stream,
        market: row.market,
        lastReceivedSeq: row.last_received_seq,
        lastRecvMonoNs: row.last_recv_mono_ns,
        updatedAtMs: row.updated_at_ms,
      };
    },

    /**
     * Every receive tail this store holds, one row per connection and board. A read, not a change: a
     * restart uses it to see what earlier runs claimed to have received without being handed any write
     * route. Rows written before the stream was part of the key carry the empty stream marker exactly as
     * they were recorded - the migration does not guess which board they belonged to.
     */
    receivedTails() {
      return db
        .prepare(
          'SELECT connection_id, stream, market, last_received_seq, last_recv_mono_ns, updated_at_ms FROM received_tail ORDER BY connection_id, stream',
        )
        .all()
        .map((row) => ({
          connectionId: row.connection_id,
          stream: row.stream,
          market: row.market,
          lastReceivedSeq: row.last_received_seq,
          lastRecvMonoNs: row.last_recv_mono_ns,
          updatedAtMs: row.updated_at_ms,
        }));
    },

    /**
     * The state a run's marker holds, or null when this store has no marker for it. A read: a restart
     * uses it to tell a run that closed cleanly (whose tail is therefore an upper bound) from one that
     * did not (whose tail is followed by an interval nothing can account for, §9.2).
     */
    runMarkerState(runId) {
      const row = db.prepare('SELECT state FROM run_marker WHERE run_id = ?').get(runId);
      return row ? row.state : null;
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
    // Set 6c: the batch transaction a module opens to make a run of its work durable together. The
    // inner transaction calls join it instead of nesting, and the caller owns putting its in-memory
    // state back if the batch rolls back.
    beginBatchTransaction,
    endBatchTransaction,
    inChange,
    whileChange,
    // The unguarded close: the wiring that holds the right for an operation of its own (a structure's
    // termination) closes the store inside it, and the public name would refuse that as a second operation.
    close: internal.close,
    // The run marker's writes, without the right: the structure runs them inside an operation of its
    // own (a begin or a clean end), and the public names would refuse that as a second operation.
    beginRun: internal.beginRun,
    completeRun: internal.completeRun,
    // The receive tail's write, without the right, for the same reason: reception records how far it has
    // heard from inside the operation that handles the frame, and the guarded public name would refuse
    // that as a second operation. It is a write the structure owns, not a route a caller is handed.
    updateReceivedTail: internal.updateReceivedTail,
    // Recording a suspected gap, without the right, for the same reason: a restart's recovery step records
    // the interval an uncleaned previous run left behind from inside that operation, and the guarded public
    // name would refuse it as a second operation. A write the structure owns, not a route a caller holds.
    recordSuspectedGap: internal.recordSuspectedGap,
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
