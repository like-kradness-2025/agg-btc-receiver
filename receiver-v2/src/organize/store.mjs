/**
 * The organize role's own store: the records organization owns when it is its own process.
 *
 * The three-process design (docs/fix-plan-sets.md §5.8, ruling ②) splits the persistent resources by
 * role. Organization owns `run_marker`, `suspected_gap`, `organized_watermark`, `organize_gap`,
 * `delivery_ledger` and the raw; reception owns `received_tail` and the spool; the book owns the board.
 * This store creates only organize's base tables and the invalidation request record; the watermark and
 * ledger tables are created by their own modules when opened on this store.
 *
 * The separate store is necessary because `durability.mjs` is the single-process test seam and also carries
 * reception's `received_tail`. Reusing it here would create tables the organizer does not own.
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
  at_ms INTEGER NOT NULL,
  finalize_request_id TEXT,
  finalize_request_payload TEXT,
  finalize_receipt_json TEXT
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

function hasOnlyKeys(value, keys) {
  return value !== null &&
    typeof value === 'object' &&
    !Array.isArray(value) &&
    Object.keys(value).length === keys.length &&
    keys.every((key) => Object.hasOwn(value, key));
}

/** Normalize every immutable field before it is stored or compared. */
function canonicalFinalizeRequest(runId, request) {
  if (!hasOnlyKeys(request, ['finalizeRequestId', 'barrierId', 'tails', 'bookStop'])) {
    throw new TypeError('a finalize request must contain exactly its id, barrier, tails and book-stop confirmation');
  }
  if (typeof request.finalizeRequestId !== 'string' || request.finalizeRequestId.length === 0) {
    throw new TypeError('a finalize request needs a non-empty request id');
  }
  if (typeof request.barrierId !== 'string' || request.barrierId.length === 0) {
    throw new TypeError('a finalize request needs a non-empty barrier id');
  }
  if (!Array.isArray(request.tails)) throw new TypeError('a finalize request needs its final-tail list');
  const tails = request.tails.map((tail) => {
    if (!hasOnlyKeys(tail, ['connectionId', 'lastReceivedSeq'])) {
      throw new TypeError('each final tail needs exactly a connection id and last received sequence');
    }
    if (typeof tail.connectionId !== 'string' || tail.connectionId.length === 0) {
      throw new TypeError('each final tail needs a non-empty connection id');
    }
    if (!Number.isInteger(tail.lastReceivedSeq) || tail.lastReceivedSeq < 0) {
      throw new TypeError('each final tail needs a non-negative integer sequence');
    }
    return { connectionId: tail.connectionId, lastReceivedSeq: tail.lastReceivedSeq };
  });
  tails.sort((left, right) => (left.connectionId < right.connectionId ? -1 : left.connectionId > right.connectionId ? 1 : 0));
  if (new Set(tails.map((tail) => tail.connectionId)).size !== tails.length) {
    throw new TypeError('a finalize request may name each connection tail only once');
  }
  const { bookStop } = request;
  if (!hasOnlyKeys(bookStop, ['requestId', 'roleInstance', 'stopped']) || bookStop.stopped !== true) {
    throw new TypeError('a finalize request needs a confirmed book stop');
  }
  if (typeof bookStop.requestId !== 'string' || bookStop.requestId.length === 0) {
    throw new TypeError('the book-stop confirmation needs its request id');
  }
  if (typeof bookStop.roleInstance !== 'string' || bookStop.roleInstance.length === 0) {
    throw new TypeError('the book-stop confirmation needs its target instance');
  }

  const payload = {
    version: 1,
    runId,
    finalizeRequestId: request.finalizeRequestId,
    barrierId: request.barrierId,
    tails,
    bookStop: {
      requestId: bookStop.requestId,
      roleInstance: bookStop.roleInstance,
      stopped: true,
    },
  };
  return { requestId: request.finalizeRequestId, payload, payloadJson: JSON.stringify(payload) };
}

function finalizeReceipt(request, completedAtMs) {
  return {
    version: request.payload.version,
    runId: request.payload.runId,
    finalizeRequestId: request.requestId,
    barrierId: request.payload.barrierId,
    tails: request.payload.tails,
    bookStop: request.payload.bookStop,
    completedAtMs,
  };
}

const FINALIZATION_PROOF_COLUMNS = [
  'run_id',
  'state',
  'at_ms',
  'finalize_request_id',
  'finalize_request_payload',
  'finalize_receipt_json',
];

function supportsReadOnlyDatabaseSync(version = process.versions.node) {
  const [major, minor] = version.split('.').map(Number);
  return (
    (major === 22 && minor >= 12) ||
    (major === 23 && minor >= 8) ||
    major >= 24
  );
}

/**
 * Inspect one run marker using a new SQLite read-only connection and a fresh read transaction.
 * This deliberately does not use `openOrganizeStore`: that path migrates the schema and invalidates
 * running markers before its caller can inspect them.
 */
export function probeRunMarker({ path: dbPath, runId, request } = {}) {
  if (typeof dbPath !== 'string' || dbPath.length === 0 || typeof runId !== 'string' || runId.length === 0) {
    return { status: 'error', fresh: false, reason: 'a database path and run id are required' };
  }
  if (!supportsReadOnlyDatabaseSync()) {
    return { status: 'error', fresh: false, reason: 'this Node.js runtime does not support read-only SQLite connections' };
  }

  let canonical = null;
  if (request !== null && request !== undefined) {
    try {
      canonical = canonicalFinalizeRequest(runId, request);
    } catch (error) {
      return { status: 'error', fresh: false, reason: error.message };
    }
  }

  let db;
  let transactionOpen = false;
  try {
    db = new DatabaseSync(dbPath, { readOnly: true });
    db.exec('BEGIN');
    transactionOpen = true;

    const table = db.prepare("SELECT name FROM sqlite_schema WHERE type = 'table' AND name = 'run_marker'").get();
    if (!table) {
      return { status: 'legacy', fresh: true, runId, state: null, reason: 'the run_marker table is absent' };
    }

    const columns = new Set(db.prepare('PRAGMA table_info(run_marker)').all().map((row) => row.name));
    if (!FINALIZATION_PROOF_COLUMNS.every((column) => columns.has(column))) {
      const baseColumns = ['run_id', 'state', 'at_ms'];
      if (baseColumns.every((column) => columns.has(column))) {
        const legacyRow = db.prepare('SELECT state FROM run_marker WHERE run_id = ?').get(runId);
        return {
          status: 'legacy',
          fresh: true,
          runId,
          state: legacyRow?.state ?? null,
          reason: 'the run_marker schema has no complete finalization proof fields',
        };
      }
      return { status: 'legacy', fresh: true, runId, state: null, reason: 'the run_marker schema is not proof-capable' };
    }

    const row = db
      .prepare(
        `SELECT run_id, state, at_ms, finalize_request_id, finalize_request_payload, finalize_receipt_json
           FROM run_marker WHERE run_id = ?`,
      )
      .get(runId);
    if (!row) return { status: 'absent', fresh: true, runId, schema: 'proof-capable' };

    if (canonical === null) {
      if (
        row.state === STATE_RUNNING &&
        row.finalize_request_id === null &&
        row.finalize_request_payload === null &&
        row.finalize_receipt_json === null
      ) {
        return {
          status: 'unprepared',
          fresh: true,
          runId,
          state: row.state,
          requestId: null,
          payloadJson: null,
          receipt: null,
        };
      }
      return {
        status: 'mismatch',
        fresh: true,
        runId,
        state: row.state,
        reason: 'a finalize request is required to match this run marker',
      };
    }

    const matchingRequest =
      row.run_id === runId &&
      row.finalize_request_id === canonical.requestId &&
      row.finalize_request_payload === canonical.payloadJson;
    if (!matchingRequest) {
      return {
        status: 'mismatch',
        fresh: true,
        runId,
        state: row.state,
        reason: 'the run marker does not match the requested run and payload',
      };
    }

    if (row.state === STATE_RUNNING && row.finalize_receipt_json === null) {
      return {
        status: 'prepared',
        fresh: true,
        runId,
        state: row.state,
        requestId: row.finalize_request_id,
        payloadJson: row.finalize_request_payload,
        receipt: null,
      };
    }

    if (row.state === STATE_COMPLETE && Number.isSafeInteger(row.at_ms) && typeof row.finalize_receipt_json === 'string') {
      const expectedReceiptJson = JSON.stringify(finalizeReceipt(canonical, row.at_ms));
      if (row.finalize_receipt_json === expectedReceiptJson) {
        return {
          status: 'complete',
          fresh: true,
          runId,
          state: row.state,
          requestId: row.finalize_request_id,
          payloadJson: row.finalize_request_payload,
          receipt: JSON.parse(row.finalize_receipt_json),
        };
      }
    }

    return {
      status: 'mismatch',
      fresh: true,
      runId,
      state: row.state,
      reason: 'the run marker has a mismatched or incomplete completion receipt',
    };
  } catch (error) {
    return { status: 'error', fresh: false, reason: error.message };
  } finally {
    if (db) {
      if (transactionOpen) {
        try {
          db.exec('ROLLBACK');
        } catch {
          // Closing the read-only connection releases an interrupted read transaction.
        }
      }
      try {
        db.close();
      } catch {
        // A probe result is never replaced by a close error.
      }
    }
  }
}

export const FINALIZATION_OUTCOME = Object.freeze({
  COMPLETE_CONFIRMED: 'COMPLETE_CONFIRMED',
  NO_COMPLETE_CONFIRMED: 'NO_COMPLETE_CONFIRMED',
  UNKNOWN: 'UNKNOWN',
});

/** Classify only fresh, exact readback evidence; uncertainty is never converted into failure. */
export function classifyFinalizationOutcome({ runId, request, readback, writerFenced = false } = {}) {
  const unknown = FINALIZATION_OUTCOME.UNKNOWN;
  if (typeof runId !== 'string' || runId.length === 0 || readback?.fresh !== true || readback.runId !== runId) {
    return unknown;
  }

  let canonical = null;
  if (request !== null && request !== undefined) {
    try {
      canonical = canonicalFinalizeRequest(runId, request);
    } catch {
      return unknown;
    }
  }

  const exactPreparedRequest =
    canonical !== null &&
    readback.requestId === canonical.requestId &&
    readback.payloadJson === canonical.payloadJson;
  if (
    readback.status === 'complete' &&
    readback.state === STATE_COMPLETE &&
    exactPreparedRequest &&
    Number.isSafeInteger(readback.receipt?.completedAtMs) &&
    JSON.stringify(readback.receipt) === JSON.stringify(finalizeReceipt(canonical, readback.receipt.completedAtMs))
  ) {
    return FINALIZATION_OUTCOME.COMPLETE_CONFIRMED;
  }

  if (writerFenced !== true) return unknown;
  if (
    readback.status === 'unprepared' &&
    canonical === null &&
    readback.state === STATE_RUNNING &&
    readback.requestId === null &&
    readback.payloadJson === null &&
    readback.receipt === null
  ) {
    return FINALIZATION_OUTCOME.NO_COMPLETE_CONFIRMED;
  }
  if (
    readback.status === 'prepared' &&
    readback.state === STATE_RUNNING &&
    exactPreparedRequest &&
    readback.receipt === null
  ) {
    return FINALIZATION_OUTCOME.NO_COMPLETE_CONFIRMED;
  }
  if (readback.status === 'absent' && readback.schema === 'proof-capable') {
    return FINALIZATION_OUTCOME.NO_COMPLETE_CONFIRMED;
  }
  return unknown;
}

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
    inTransaction(() => {
      db.exec(ORGANIZE_STORE_SCHEMA);
      const runMarkerColumns = new Set(db.prepare('PRAGMA table_info(run_marker)').all().map((row) => row.name));
      if (!runMarkerColumns.has('finalize_request_id')) {
        db.exec('ALTER TABLE run_marker ADD COLUMN finalize_request_id TEXT');
      }
      if (!runMarkerColumns.has('finalize_request_payload')) {
        db.exec('ALTER TABLE run_marker ADD COLUMN finalize_request_payload TEXT');
      }
      if (!runMarkerColumns.has('finalize_receipt_json')) {
        db.exec('ALTER TABLE run_marker ADD COLUMN finalize_receipt_json TEXT');
      }
      // The old scalar duplicates the contiguous watermark; every unapplied frame is already in the ledger.
      // Drop it once, transactionally, so upgrades and fresh stores use the same recovery source.
      db.exec('DROP TABLE IF EXISTS pending_boundary');
    });

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

  function prepareFinalize(request) {
    let canonical;
    try {
      canonical = canonicalFinalizeRequest(runId, request);
    } catch (error) {
      return { prepared: false, refused: true, reason: error.message };
    }
    return inTransaction(() => {
      const row = db
        .prepare(
          `SELECT state, finalize_request_id, finalize_request_payload, finalize_receipt_json
             FROM run_marker WHERE run_id = ?`,
        )
        .get(runId);
      if (!row) return { prepared: false, refused: true, reason: 'the run marker does not exist' };
      if (row.finalize_request_id !== null || row.finalize_request_payload !== null) {
        if (
          row.finalize_request_id === canonical.requestId &&
          row.finalize_request_payload === canonical.payloadJson &&
          (row.state === STATE_RUNNING || (row.state === STATE_COMPLETE && row.finalize_receipt_json !== null))
        ) {
          return { prepared: true, requestId: canonical.requestId };
        }
        return { prepared: false, refused: true, reason: 'this run already has a different immutable finalize request' };
      }
      if (row.state !== STATE_RUNNING) {
        return { prepared: false, refused: true, reason: 'only a running organize marker can prepare finalization' };
      }
      db.prepare(
        `UPDATE run_marker
            SET finalize_request_id = ?, finalize_request_payload = ?
          WHERE run_id = ? AND state = ? AND finalize_request_id IS NULL AND finalize_request_payload IS NULL`,
      ).run(canonical.requestId, canonical.payloadJson, runId, STATE_RUNNING);
      return { prepared: true, requestId: canonical.requestId };
    });
  }

  function completeRun(request) {
    let canonical;
    try {
      canonical = canonicalFinalizeRequest(runId, request);
    } catch (error) {
      return { completed: false, refused: true, reason: error.message };
    }
    return inTransaction(() => {
      const row = db
        .prepare(
          `SELECT state, finalize_request_id, finalize_request_payload, finalize_receipt_json
             FROM run_marker WHERE run_id = ?`,
        )
        .get(runId);
      if (!row) return { completed: false, refused: true, reason: 'the run marker does not exist' };
      if (row.state === STATE_COMPLETE) {
        if (
          row.finalize_request_id === canonical.requestId &&
          row.finalize_request_payload === canonical.payloadJson &&
          row.finalize_receipt_json !== null
        ) {
          return { completed: true, receipt: JSON.parse(row.finalize_receipt_json) };
        }
        return { completed: false, refused: true, reason: 'the completed marker has no matching request proof' };
      }
      if (row.state !== STATE_RUNNING) {
        return { completed: false, refused: true, reason: 'the run marker is not running' };
      }
      if (
        row.finalize_request_id !== canonical.requestId ||
        row.finalize_request_payload !== canonical.payloadJson
      ) {
        return { completed: false, refused: true, reason: 'the finalize request was not prepared for this payload' };
      }
      const completedAtMs = nowMs();
      const receipt = finalizeReceipt(canonical, completedAtMs);
      const receiptJson = JSON.stringify(receipt);
      const changed = db
        .prepare(
          `UPDATE run_marker
              SET state = ?, at_ms = ?, finalize_receipt_json = ?
            WHERE run_id = ? AND state = ? AND finalize_request_id = ? AND finalize_request_payload = ?`,
        )
        .run(
          STATE_COMPLETE,
          completedAtMs,
          receiptJson,
          runId,
          STATE_RUNNING,
          canonical.requestId,
          canonical.payloadJson,
        ).changes;
      if (changed !== 1) {
        return { completed: false, refused: true, reason: 'the prepared finalize request changed before commit' };
      }
      return { completed: true, receipt };
    });
  }

  const api = {
    /** Mark this generation as the live one (ruling ⑧: organize writes the run marker). */
    beginRun() {
      return inTransaction(() => {
        const existing = db.prepare('SELECT state FROM run_marker WHERE run_id = ?').get(runId);
        if (existing?.state === STATE_COMPLETE) {
          throw new Error('a completed run marker cannot be started again with the same run id');
        }
        if (!existing) {
          db.prepare('INSERT INTO run_marker (run_id, state, at_ms) VALUES (?, ?, ?)').run(
            runId,
            STATE_RUNNING,
            nowMs(),
          );
          return { begun: true };
        }
        if (existing.state !== STATE_RUNNING && existing.state !== STATE_INVALIDATED) {
          throw new Error(`the run marker state ${JSON.stringify(existing.state)} cannot be resumed`);
        }
        db.prepare('UPDATE run_marker SET state = ?, at_ms = ? WHERE run_id = ?').run(STATE_RUNNING, nowMs(), runId);
        return { begun: true };
      });
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
    recordSuspectedGap: api.recordSuspectedGap,
    close: api.close,
  };
  for (const [name, run] of Object.entries(internal)) api[name] = guarded(`store.${name}`, run);
  const prepareFinalizeGuarded = guard('store.prepareFinalize', prepareFinalize, () => ({
    prepared: false,
    refused: true,
    code: REENTRANT_OPERATION,
    reason: 'store.prepareFinalize is busy',
  }));
  const completeRunGuarded = guard('store.completeRun', completeRun, () => ({
    completed: false,
    refused: true,
    code: REENTRANT_OPERATION,
    reason: 'store.completeRun is busy',
  }));

  const exported = { ...api, inChange, REENTRANT_OPERATION };
  bindInternals(exported, {
    db,
    guard,
    inTransaction,
    inChange,
    whileChange,
    close: internal.close,
    beginRun: internal.beginRun,
    prepareFinalize: prepareFinalizeGuarded,
    completeRun: completeRunGuarded,
    recordSuspectedGap: internal.recordSuspectedGap,
    REENTRANT_OPERATION,
  });
  return exported;
}
