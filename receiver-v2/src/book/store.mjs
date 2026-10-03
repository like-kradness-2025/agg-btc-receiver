/**
 * The book role's own store: the records the book owns when it is its own process.
 *
 * The three-process design (docs/fix-plan-sets.md §5.8, ruling ②) splits the persistent resources by
 * role. The book owns the board: `applied_boundary`, `book_level`, `board_anchor`, `book_missing_record`,
 * `book_gap`, `retired_run`, `connection_identity` and `legacy_owner` - and, from the loss protocol
 * (rulings ④⑤), the invalidation record it must persist together with the missing it declares
 * (`book_invalidation`). Reception owns `received_tail` and the spool; organization owns `run_marker`,
 * `pending_boundary`, `suspected_gap`, `organized_watermark`, `organize_gap`, `delivery_ledger` and the
 * raw. None of those belong here, and this store creates none of them.
 *
 * Most of the book's tables are created by `src/book/state.mjs` itself, the first time the book is
 * opened against this store (`BOOK_SCHEMA`). The one table this store creates is the invalidation
 * record, which the stage-4 process entrance owns rather than the state module. Keeping it here means
 * `src/book/state.mjs` is untouched by the split (it stays the single-process book), and the
 * ownership boundary is exactly what the file manifests.
 *
 * Why a dedicated store rather than `durability.mjs`: that module is the single-process store and it
 * also carries `received_tail`, `run_marker` and `pending_boundary`, which belong to other roles.
 * Reusing it here would make the book create tables it does not own. This module mirrors the parts of
 * `durability.mjs` the book needs to be opened on it: the execution right, the one-handle rule and the
 * transaction discipline.
 */

import { DatabaseSync } from 'node:sqlite';
import { statSync } from 'node:fs';
import { bindInternals } from '../internal/wiring.mjs';

/** One store, one handle, one execution right (the same rule as `durability.mjs`). */
const OPEN_STORES = new Map();

/** The public refusal of a change operation that arrived while another one was in progress. */
export const REENTRANT_OPERATION = 'REENTRANT_OPERATION';

/**
 * The one table this store owns. The board's own tables are created by `state.mjs` when the book is
 * opened. `book_invalidation` records a loss the book was asked to declare: the request's identity,
 * its range and monotonic revision, and the fact that serving stopped for it - written in the same
 * transaction as the missing record that stops the board (rulings ④⑤).
 */
export const BOOK_STORE_SCHEMA = `
CREATE TABLE IF NOT EXISTS book_invalidation (
  request_id TEXT NOT NULL PRIMARY KEY,
  market TEXT NOT NULL,
  stream TEXT NOT NULL,
  connection_id TEXT NOT NULL,
  missing_from INTEGER,
  missing_to INTEGER,
  revision INTEGER,
  state TEXT NOT NULL,
  reason TEXT NOT NULL,
  requested_at_ms INTEGER NOT NULL,
  invalidated_at_ms INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS book_invalidation_board
  ON book_invalidation (market, stream, connection_id, revision);
`;

/** The state an invalidation reaches in the book: persisted and answered. There is no way back. */
export const INVALIDATION_INVALIDATED = 'invalidated';

/** The file SQLite opened, as an identity - or null when there is no file to hold. */
function databaseFileKey(db) {
  const main = db.prepare('PRAGMA database_list').all().find((row) => row.name === 'main');
  if (!main || !main.file) return null;
  const stats = statSync(main.file);
  return `file:${stats.dev}:${stats.ino}`;
}

/**
 * Open the book store. Opening it writes to it - the schema - so the whole initialisation is one
 * change operation, and a second handle on the same file is refused before anything is written by
 * this handle.
 */
export function openBookStore({ path: dbPath, nowMs = () => Date.now(), Database = DatabaseSync } = {}) {
  if (!dbPath) throw new TypeError('the book store needs a path');

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
    db.exec(BOOK_STORE_SCHEMA);
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
    close() {
      if (closed) return;
      db.close();
      closed = true;
      if (fileKey !== null) OPEN_STORES.delete(fileKey);
    },
  };
  api.close = guarded('store.close', api.close);

  const exported = { ...api, inChange, REENTRANT_OPERATION };
  bindInternals(exported, {
    db,
    guard,
    inTransaction,
    inChange,
    whileChange,
    close: () => {
      if (closed) return;
      db.close();
      closed = true;
      if (fileKey !== null) OPEN_STORES.delete(fileKey);
    },
    REENTRANT_OPERATION,
  });
  return exported;
}
