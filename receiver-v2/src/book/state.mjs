/**
 * The book: keep the board, and be able to prove where it stands.
 *
 * Three rules decide everything here, each of them a correction of a way this could quietly lose
 * data while looking healthy.
 *
 * 1. The board is persisted in the same transaction as the position that describes it. A position
 *    that is remembered while the board is only in memory is worse than no memory at all: after a
 *    restart the board is empty and every resend up to that position is refused as already applied.
 *    The levels and the position move together or not at all.
 *
 * 2. A range applies contiguously. A frame whose predecessors have not arrived is not applied and
 *    does not move the position - otherwise the missing data is refused later as "already applied",
 *    which is the one way a gap can become permanent. The hole is recorded and waited for.
 *
 * 3. Signals that only make sense once the boundary is proven do not come from applying data. A book
 *    serves because its snapshot was checked against the stream, not because frames kept arriving.
 *    Any doubt puts it back to syncing, and only an explicit, successful proof puts it back.
 *
 * Trust about connections is not decided here either. Reception issues the generation; this module
 * keeps the highest one it has been shown and accepts nothing that is not strictly newer.
 */

const SYNCING = 'syncing';
const RUNNING = 'running';

const BOOK_SCHEMA = `
CREATE TABLE IF NOT EXISTS applied_boundary (
  market TEXT NOT NULL,
  stream TEXT NOT NULL,
  connection_id TEXT NOT NULL,
  generation INTEGER,
  up_to_receive_seq INTEGER,
  updated_at_ms INTEGER NOT NULL,
  PRIMARY KEY (market, stream)
);
CREATE TABLE IF NOT EXISTS book_level (
  market TEXT NOT NULL,
  stream TEXT NOT NULL,
  side TEXT NOT NULL,
  price REAL NOT NULL,
  size REAL NOT NULL,
  PRIMARY KEY (market, stream, side, price)
);
CREATE TABLE IF NOT EXISTS book_gap (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  market TEXT NOT NULL,
  stream TEXT NOT NULL,
  connection_id TEXT NOT NULL,
  waiting_for INTEGER NOT NULL,
  seen_seq INTEGER NOT NULL,
  detected_at_ms INTEGER NOT NULL,
  filled_at_ms INTEGER
);
`;

/** Read-only view of a board: the levels this process currently believes in. */
export function createBoard() {
  const levels = new Map(); // `${side}:${price}` -> size
  return {
    apply({ side, price, size }) {
      const key = `${side}:${price}`;
      if (size === 0) levels.delete(key);
      else levels.set(key, size);
    },
    restore(rows) {
      levels.clear();
      for (const row of rows) levels.set(`${row.side}:${row.price}`, row.size);
    },
    size(side, price) {
      return levels.get(`${side}:${price}`) ?? null;
    },
    rows() {
      return [...levels.entries()].map(([key, size]) => {
        const [side, price] = key.split(':');
        return { side, price: Number(price), size };
      });
    },
    get depth() {
      return levels.size;
    },
  };
}

export function openBook({ market, stream, durability, nowMs = () => Date.now() }) {
  if (!market || !stream) throw new TypeError('a book needs a market and a stream');
  if (!durability?.db) throw new TypeError('a book needs the durability store for its position');

  durability.db.exec(BOOK_SCHEMA);
  const board = createBoard();

  // The board comes back from the store, not from a caller's memory, and it comes back together with
  // the position it was written with.
  board.restore(
    durability.db
      .prepare('SELECT side, price, size FROM book_level WHERE market = ? AND stream = ?')
      .all(market, stream),
  );

  const row = durability.db
    .prepare('SELECT connection_id, generation, up_to_receive_seq FROM applied_boundary WHERE market = ? AND stream = ?')
    .get(market, stream);
  let applied = row
    ? { connectionId: row.connection_id, generation: row.generation ?? null, upToSeq: row.up_to_receive_seq ?? null }
    : { connectionId: null, generation: null, upToSeq: null };

  let phase = SYNCING; // a fresh or reopened book proves its boundary before serving
  let lastRefusal = null;

  const boundaryStatement = durability.db.prepare(
    `INSERT OR REPLACE INTO applied_boundary (market, stream, connection_id, generation, up_to_receive_seq, updated_at_ms)
     VALUES (?, ?, ?, ?, ?, ?)`,
  );
  const levelUpsert = durability.db.prepare(
    `INSERT OR REPLACE INTO book_level (market, stream, side, price, size) VALUES (?, ?, ?, ?, ?)`,
  );
  const levelDelete = durability.db.prepare(
    'DELETE FROM book_level WHERE market = ? AND stream = ? AND side = ? AND price = ?',
  );

  /** Everything that makes one range durable: the levels and the position, in one transaction. */
  function commitRange({ changes, next }) {
    durability.db.exec('BEGIN IMMEDIATE');
    try {
      for (const change of changes) {
        if (change.size === 0) levelDelete.run(market, stream, change.side, change.price);
        else levelUpsert.run(market, stream, change.side, change.price, change.size);
      }
      boundaryStatement.run(
        market,
        stream,
        next.connectionId,
        next.generation,
        next.upToSeq,
        nowMs(),
      );
      durability.db.exec('COMMIT');
    } catch (error) {
      durability.db.exec('ROLLBACK');
      throw error;
    }
    // Only after the commit does the in-memory board follow the store.
    for (const change of changes) board.apply(change);
    applied = next;
  }

  return {
    market,
    stream,
    board,

    /** Which connection this book is willing to accept: only a strictly newer generation replaces. */
    accept(connectionId, { generation = null } = {}) {
      if (applied.connectionId === null) {
        applied = { connectionId, generation, upToSeq: null };
        phase = SYNCING;
        return { accepted: true, reason: 'first connection' };
      }
      if (connectionId === applied.connectionId) return { accepted: true, reason: 'same connection' };
      const supersedes = typeof generation === 'number' && (applied.generation === null || generation > applied.generation);
      if (supersedes) {
        applied = { connectionId, generation, upToSeq: null };
        phase = SYNCING;
        return { accepted: true, reason: 'superseded by a newer generation' };
      }
      lastRefusal = { connectionId, generation, atMs: nowMs(), reason: 'superseded connection' };
      return { accepted: false, reason: 'superseded connection' };
    },

    get lastRefusal() {
      return lastRefusal ? { ...lastRefusal } : null;
    },
    get phase() {
      return phase;
    },
    get isRunning() {
      return phase === RUNNING;
    },
    get appliedBoundary() {
      return { ...applied };
    },

    resumeFrom() {
      if (applied.connectionId === null) return null;
      return { connectionId: applied.connectionId, upToSeq: applied.upToSeq };
    },

    /**
     * Apply one envelope together with the level changes it carries.
     *
     * Contiguous only: a frame whose predecessors are missing is refused and the hole is recorded,
     * because applying it would make the missing data permanently unapplicable. Duplicates are
     * no-ops. Applying data never changes the phase - only a proven boundary does.
     */
    apply({ envelope, changes = [] }) {
      if (envelope.connection_id !== applied.connectionId) {
        const accepted = this.accept(envelope.connection_id, { generation: envelope.generation ?? null });
        if (!accepted.accepted) return { applied: false, reason: accepted.reason };
      }
      const seq = envelope.receive_seq;
      if (applied.upToSeq !== null && seq <= applied.upToSeq) {
        return { applied: false, reason: 'already applied' };
      }
      if (applied.upToSeq !== null && seq !== applied.upToSeq + 1) {
        // A hole: record it and wait. Applying this would move the position past data that has not
        // arrived, and the resend of that data would then be refused as already applied.
        durability.db
          .prepare(
            `INSERT INTO book_gap (market, stream, connection_id, waiting_for, seen_seq, detected_at_ms)
             VALUES (?, ?, ?, ?, ?, ?)`,
          )
          .run(market, stream, applied.connectionId, applied.upToSeq + 1, seq, nowMs());
        return { applied: false, reason: 'gap before this sequence', waitingFor: applied.upToSeq + 1 };
      }
      if (applied.upToSeq === null && seq !== (envelope.first_seq ?? seq)) {
        // The first sequence of this connection never arrived: that hole is a fact, not a licence to
        // start somewhere in the middle.
        durability.db
          .prepare(
            `INSERT INTO book_gap (market, stream, connection_id, waiting_for, seen_seq, detected_at_ms)
             VALUES (?, ?, ?, ?, ?, ?)`,
          )
          .run(market, stream, applied.connectionId, envelope.first_seq ?? seq, seq, nowMs());
        return { applied: false, reason: 'waiting for the first sequence', waitingFor: envelope.first_seq ?? seq };
      }

      commitRange({ changes, next: { ...applied, connectionId: applied.connectionId, upToSeq: seq } });
      durability.db
        .prepare(
          'UPDATE book_gap SET filled_at_ms = ? WHERE market = ? AND stream = ? AND filled_at_ms IS NULL AND waiting_for <= ?',
        )
        .run(nowMs(), market, stream, seq);
      return { applied: true, reason: 'applied' };
    },

    /** Holes this book is waiting for. Unfilled ones are what it cannot claim to have. */
    openGaps() {
      return durability.db
        .prepare(
          `SELECT waiting_for, seen_seq, detected_at_ms FROM book_gap
           WHERE market = ? AND stream = ? AND filled_at_ms IS NULL ORDER BY waiting_for`,
        )
        .all(market, stream)
        .map((row) => ({
          waitingFor: row.waiting_for,
          seenSeq: row.seen_seq,
          detectedAtMs: row.detected_at_ms,
        }));
    },

    /** A sync is starting: the board is not to be trusted until its boundary is proven. */
    beginSync() {
      phase = SYNCING;
    },

    /**
     * The snapshot has been checked against the stream. Only this puts the book back in service; a
     * fresh book has proven nothing, and reaching this with no connection accepted is not possible.
     */
    proveBoundary() {
      if (applied.connectionId === null) return { proven: false, reason: 'no connection accepted yet' };
      phase = RUNNING;
      return { proven: true, reason: 'boundary proven' };
    },
  };
}
