/**
 * The ingest role's own store: where the receive tail lives when reception is its own process.
 *
 * The three-process design (§5.8, ruling ②) splits the persistent resources by role, and the row this
 * module owns is `received_tail` alone. Organize owns the run marker, the watermark, the delivery
 * ledger and the raw; the book owns the board. Ingest owns the spool and this one record - how far
 * reception can show it heard, per connection and per board.
 *
 * Why a store of its own rather than the shared one: in the split each role opens its own database and
 * reaches the others by IPC, so the tail is written by the process that did the receiving and by no
 * one else. This module is deliberately not `durability.mjs`: the single-process path keeps using
 * that store unchanged (stage 5 is where the routes are actually rewired), and a second module that
 * mirrors only the `received_tail` schema keeps the two from colliding on the same file or the same
 * single-handle registry.
 *
 * The row is a lower bound only, exactly as the durability store's note says: it is written on the
 * reception side as frames arrive, it lags the socket by the frame being processed, and nothing may
 * treat it as a completeness claim. The stream is part of the key because a connection name (run,
 * venue, market, generation - C2) does not carry the stream, so one connection legitimately has a
 * book tail and a trades tail.
 */

import { DatabaseSync } from 'node:sqlite';

/**
 * The schema this store owns. It mirrors the `received_tail` half of `durability.mjs` so a row written
 * by one is readable by the other, but it creates only its own table - there is no run_marker, no
 * watermark and no ledger here, because none of those belongs to reception.
 */
export const INGEST_SCHEMA = `
CREATE TABLE IF NOT EXISTS received_tail (
  connection_id TEXT NOT NULL,
  stream TEXT NOT NULL DEFAULT '',
  market TEXT NOT NULL,
  last_received_seq INTEGER NOT NULL,
  last_recv_mono_ns INTEGER NOT NULL,
  updated_at_ms INTEGER NOT NULL,
  PRIMARY KEY (connection_id, stream)
);
`;

/**
 * Open the ingest store.
 *
 * Only a file-backed database is used: the path is the identity of the run's reception record, and a
 * store with no file would not survive the restart the tail exists to inform. WAL and FULL syncing are
 * the same durability discipline the rest of the receiver uses.
 */
export function openIngestStore({ path, nowMs = () => Date.now(), Database = DatabaseSync } = {}) {
  if (!path) throw new TypeError('the ingest store needs a path');
  const db = new Database(path);
  db.exec('PRAGMA journal_mode = WAL');
  db.exec('PRAGMA synchronous = FULL');
  // Ruling ⑮: with each role its own process and its own file, a busy is an anomaly, not something to
  // wait out. Set explicitly to 0 so a second writer surfaces as `database is locked` at once rather
  // than stalling a run that looks healthy.
  db.exec('PRAGMA busy_timeout = 0');
  db.exec(INGEST_SCHEMA);

  const upsert = db.prepare(
    `INSERT OR REPLACE INTO received_tail
       (connection_id, stream, market, last_received_seq, last_recv_mono_ns, updated_at_ms)
     VALUES (?, ?, ?, ?, ?, ?)`,
  );
  const readOne = db.prepare('SELECT * FROM received_tail WHERE connection_id = ? AND stream = ?');
  const readAll = db.prepare(
    'SELECT connection_id, stream, market, last_received_seq, last_recv_mono_ns, updated_at_ms FROM received_tail ORDER BY connection_id, stream',
  );

  function rowToTail(row) {
    return {
      connectionId: row.connection_id,
      stream: row.stream,
      market: row.market,
      lastReceivedSeq: row.last_received_seq,
      lastRecvMonoNs: row.last_recv_mono_ns,
      updatedAtMs: row.updated_at_ms,
    };
  }

  let closed = false;

  return {
    /**
     * Record how far this process can prove it received. A caller with no stream is recorded under the
     * empty marker rather than attributed to a board that never wrote it.
     */
    updateReceivedTail({ connectionId, market, stream = '', lastReceivedSeq, lastRecvMonoNs }) {
      if (closed) throw new TypeError('this ingest store is closed');
      upsert.run(connectionId, stream ?? '', market, lastReceivedSeq, lastRecvMonoNs, nowMs());
    },

    /**
     * Set 6d: several tails in one transaction. The reception side writes its tail on every arrival,
     * and a commit per frame is what a disk with millisecond fsyncs charges for; the caller keeps
     * only the latest position of each identity and writes them together on its own clock.
     */
    updateReceivedTails(entries) {
      if (closed) throw new TypeError('this ingest store is closed');
      if (entries.length === 0) return;
      db.exec('BEGIN IMMEDIATE');
      try {
        for (const { connectionId, market, stream = '', lastReceivedSeq, lastRecvMonoNs } of entries) {
          upsert.run(connectionId, stream ?? '', market, lastReceivedSeq, lastRecvMonoNs, nowMs());
        }
        db.exec('COMMIT');
      } catch (error) {
        try {
          db.exec('ROLLBACK');
        } catch {
          // the original failure is the one to report
        }
        throw error;
      }
    },

    readReceivedTail(connectionId, stream = '') {
      const row = readOne.get(connectionId, stream ?? '');
      return row ? rowToTail(row) : null;
    },

    /** Every receive tail this store holds, one row per connection and board. */
    receivedTails() {
      return readAll.all().map(rowToTail);
    },

    close() {
      if (closed) return;
      closed = true;
      db.close();
    },

    get path() {
      return path;
    },
  };
}
