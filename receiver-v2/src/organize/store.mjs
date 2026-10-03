/**
 * The organize role's own store: the records organization owns when it is its own process.
 *
 * The three-process design (docs/fix-plan-sets.md §5.8, ruling ②) splits the persistent resources by
 * role. Organization owns `run_marker`, `pending_boundary`, `suspected_gap`, `organized_watermark`,
 * `organize_gap`, `delivery_ledger` and the raw; reception owns `received_tail` and the spool; the
 * book owns the board. This store creates only organize's own tables. The watermark and ledger tables
 * are created by their own modules when they are opened (they take this store as their `durability`),
 * so the schema here is the three that belong to the process itself plus the invalidation request
 * record the loss protocol needs (rulings ④⑤).
 *
 * Why a dedicated store rather than `durability.mjs`: that module is the single-process store and it
 * also carries `received_tail`, which belongs to reception. Reusing it here would make organize create
 * a table it does not own. This module is deliberately not `durability.mjs` - the single-process path
 * keeps using that store unchanged (stage 5 is where the routes are rewired) - and it mirrors the
 * parts of it that the watermark and ledger modules need to be opened on it.
 *
 * The execution right, the one-handle rule and the transaction discipline are the same as the
 * single-process store, kept here so the modules that were written against the wiring open unchanged.
 */

import { DatabaseSync } from 'node:sqlite';
import { statSync } from 'node:fs';
import { bindInternals } from '../internal/wiring.mjs';

/**
 * One store, one handle, one execution right (the same rule as `durability.mjs`). A separate registry
 * because this is a different file in a different process; nothing about the single-process store is
 * shared with it.
 */
const OPEN_STORES = new Map();

/** The public refusal of a change operation that arrived while another one was in progress. */
export const REENTRANT_OPERATION = 'REENTRANT_OPERATION';

export const ORGANIZE_STORE_SCHEMA = `
CREATE TABLE IF NOT EXISTS run_marker (
  run_id TEXT NOT NULL PRIMARY KEY,
  state TEXT NOT NULL,
  at_ms INTEGER NOT NULL
);
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
CREATE TABLE IF NOT EXISTS suspected_gap (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  market TEXT NOT NULL,
  stream TEXT NOT NULL,
  from_ms INTEGER,
  to_ms INTEGER NOT NULL,
  reason TEXT NOT NULL,
  recorded_at_ms INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS invalidation_request (
  request_id TEXT NOT NULL PRIMARY KEY,
  market TEXT NOT NULL,
  stream TEXT NOT NULL,
  connection_id TEXT NOT NULL,
  missing_from INTEGER NOT NULL,
  missing_to INTEGER NOT NULL,
  revision INTEGER NOT NULL,
  state TEXT NOT NULL,
  reason TEXT NOT NULL,
  requested_at_ms INTEGER NOT NULL,
  confirmed_at_ms INTEGER,
  confirmed_by TEXT
);
CREATE INDEX IF NOT EXISTS invalidation_request_board
  ON invalidation_request (market, stream, state);
`;

const STATE_RUNNING = 'running';
const STATE_COMPLETE = 'complete';
const STATE_INVALIDATED = 'invalidated';

/** The invalidation request states. A confirmed loss may not be cancelled (ruling ⑤). */
export const INVALIDATION_REQUESTED = 'requested';
export const INVALIDATION_CONFIRMED = 'confirmed';

/**
 * The file SQLite opened, as an identity - or null when there is no file to hold. Identical in reading
 * to `durability.mjs`: `PRAGMA database_list` reports what this connection actually opened, and the
 * identity is only ever used to refuse a second open, never to decide anything else.
 */
function databaseFileKey(db) {
  const main = db.prepare('PRAGMA database_list').all().find((row) => row.name === 'main');
  if (!main || !main.file) return null;
  const stats = statSync(main.file);
  return `file:${stats.dev}:${stats.ino}`;
}

/**
 * Open the organize store for one run.
 *
 * The previous generation's marker is invalidated here, before the caller can record anything
 * (ruling ⑧), in the same transaction as the write: a crash cannot leave a window in which an
 * unfinished previous run looks like a clean shutdown.
 */
export function openOrganizeStore({ path: dbPath, runId, nowMs = () => Date.now(), Database = DatabaseSync } = {}) {
  if (!dbPath) throw new TypeError('the organize store needs a path');
  if (!runId) throw new TypeError('the organize store needs a run id');

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
  try {
    db = new Database(dbPath);
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
    OPEN_STORES.set(fileKey, true);
    registered = true;

    db.exec('PRAGMA journal_mode = WAL');
    db.exec('PRAGMA synchronous = FULL');
    // Ruling ⑮: one role, one file, one process. A busy is an anomaly; surface it at once.
    db.exec('PRAGMA busy_timeout = 0');
    db.exec(ORGANIZE_STORE_SCHEMA);

    const invalidate = db.prepare('UPDATE run_marker SET state = ?, at_ms = ? WHERE state = ?');
    inTransaction(() => {
      invalidate.run(STATE_INVALIDATED, nowMs(), STATE_RUNNING);
    });
  } catch (error) {
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

  function guard(operation, run, refusalShape = (refusal) => refusal) {
    return (...args) => {
      if (!beginChange()) {
        return refusalShape({
          accepted: false,
          changed: false,
          code: REENTRANT_OPERATION,
          reason: `${operation} may not start while another change operation is being processed`,
          ack: null,
        });
      }
      try {
        return run(...args);
      } finally {
        endChange();
      }
    };
  }

  const guarded = (operation, run) =>
    guard(operation, run, () => ({ changed: false, code: REENTRANT_OPERATION, reason: `${operation} is busy` }));

  const api = {
    /** Mark this generation as the live one (ruling ⑧: organize writes the run marker). */
    beginRun() {
      db.prepare('INSERT OR REPLACE INTO run_marker (run_id, state, at_ms) VALUES (?, ?, ?)').run(
        runId,
        STATE_RUNNING,
        nowMs(),
      );
      return { begun: true };
    },

    /** Mark this generation complete. Only a normal completion is written here; never an abnormal end. */
    completeRun() {
      db.prepare('INSERT OR REPLACE INTO run_marker (run_id, state, at_ms) VALUES (?, ?, ?)').run(
        runId,
        STATE_COMPLETE,
        nowMs(),
      );
      return { completed: true };
    },

    /** The state a run's marker holds, or null. A read: a restart tells a clean end from a crash by it. */
    runMarkerState(runIdQuery) {
      const row = db.prepare('SELECT state FROM run_marker WHERE run_id = ?').get(runIdQuery);
      return row ? row.state : null;
    },

    /** A range that cannot be proven either way. Recorded, never deleted. */
    recordSuspectedGap({ market, stream, fromMs = null, toMs, reason }) {
      db.prepare(
        `INSERT INTO suspected_gap (market, stream, from_ms, to_ms, reason, recorded_at_ms)
         VALUES (?, ?, ?, ?, ?, ?)`,
      ).run(market, stream, fromMs, toMs, reason, nowMs());
      return { recorded: true };
    },

    suspectedGaps({ market } = {}) {
      const rows = market
        ? db.prepare('SELECT * FROM suspected_gap WHERE market = ? ORDER BY id').all(market)
        : db.prepare('SELECT * FROM suspected_gap ORDER BY id').all();
      return rows;
    },

    close() {
      if (closed) return;
      db.close();
      closed = true;
      if (fileKey !== null) OPEN_STORES.delete(fileKey);
    },
  };

  const internal = {
    beginRun: api.beginRun,
    completeRun: api.completeRun,
    recordSuspectedGap: api.recordSuspectedGap,
    close: api.close,
  };
  for (const [name, run] of Object.entries(internal)) api[name] = guarded(`store.${name}`, run);

  const exported = { ...api, inChange, REENTRANT_OPERATION };
  bindInternals(exported, {
    db,
    guard,
    inTransaction,
    inChange,
    whileChange,
    close: internal.close,
    beginRun: internal.beginRun,
    completeRun: internal.completeRun,
    recordSuspectedGap: internal.recordSuspectedGap,
    REENTRANT_OPERATION,
  });
  return exported;
}
