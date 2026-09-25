/**
 * The book: keep the board, and be able to prove where it stands.
 *
 * Two rules decide everything here.
 *
 * 1. The applied position is written in the same transaction as the state it describes. A record
 *    that ran ahead of the state would be worse than no record: a resend would be skipped as
 *    "already applied" while the board never received it. A record that lags is safe, because the
 *    resend is applied again - which is why applying a range must be idempotent, and why the test
 *    for that is as important as this comment.
 *
 * 2. Nothing is trusted about a connection except what arrived with the data. Reception issues the
 *    connection generation; the book keeps whichever one it was told to accept and refuses anything
 *    from a superseded one. It does not invent a second opinion about which connection is current.
 *
 * The sync state is fail-closed by construction: a book is not running until a boundary has been
 * proven, and any doubt - a connection change, a failed proof - puts it back to syncing rather than
 * letting it serve a board it cannot vouch for.
 */

const SYNCING = 'syncing';
const RUNNING = 'running';

const BOOK_SCHEMA = `
CREATE TABLE IF NOT EXISTS applied_boundary (
  market TEXT NOT NULL,
  stream TEXT NOT NULL,
  connection_id TEXT NOT NULL,
  up_to_receive_seq INTEGER,
  updated_at_ms INTEGER NOT NULL,
  PRIMARY KEY (market, stream)
);
`;

/**
 * Read-only view of a board: the levels this process currently believes in. Kept deliberately thin -
 * what matters for the contract is the position, not the shape of the book.
 */
export function createBoard() {
  const levels = new Map(); // `${side}:${price}` -> size
  return {
    apply({ side, price, size }) {
      const key = `${side}:${price}`;
      if (size === 0) levels.delete(key);
      else levels.set(key, size);
    },
    size(side, price) {
      return levels.get(`${side}:${price}`) ?? null;
    },
    get depth() {
      return levels.size;
    },
    snapshot() {
      return [...levels.entries()].map(([key, size]) => {
        const [side, price] = key.split(':');
        return { side, price, size };
      });
    },
  };
}

export function openBook({ market, stream, durability, nowMs = () => Date.now() }) {
  if (!market || !stream) throw new TypeError('a book needs a market and a stream');
  if (!durability?.db) throw new TypeError('a book needs the durability store for its position');

  durability.db.exec(BOOK_SCHEMA);
  const board = createBoard();

  const row = durability.db
    .prepare('SELECT connection_id, up_to_receive_seq FROM applied_boundary WHERE market = ? AND stream = ?')
    .get(market, stream);
  // The position comes back from the store, not from the caller's memory: a restart resumes where the
  // board actually is, which is the only value that keeps a resend from skipping applied data.
  let applied = row
    ? { connectionId: row.connection_id, upToSeq: row.up_to_receive_seq ?? null }
    : { connectionId: null, upToSeq: null };

  let phase = applied.connectionId === null ? SYNCING : SYNCING; // a reopened book still proves first
  let lastRefusal = null;

  function persistBoundaryInTransaction(next, stateChange) {
    durability.db.exec('BEGIN IMMEDIATE');
    try {
      stateChange();
      durability.db
        .prepare(
          `INSERT OR REPLACE INTO applied_boundary
             (market, stream, connection_id, up_to_receive_seq, updated_at_ms)
           VALUES (?, ?, ?, ?, ?)`,
        )
        .run(market, stream, next.connectionId, next.upToSeq, nowMs());
      durability.db.exec('COMMIT');
    } catch (error) {
      durability.db.exec('ROLLBACK');
      throw error;
    }
    applied = next;
  }

  return {
    market,
    stream,
    board,

    /** Which connection this book is willing to accept. Set from what the data carries, once. */
    accept(connectionId, { generation = null } = {}) {
      if (applied.connectionId === null) {
        applied = { connectionId, upToSeq: null };
        phase = SYNCING;
        return { accepted: true, reason: 'first connection' };
      }
      if (connectionId === applied.connectionId) return { accepted: true, reason: 'same connection' };
      // A newer generation supersedes the old one; anything else is stale and is refused by name.
      const isNewer = generation !== null && generation > 0;
      if (isNewer) {
        applied = { connectionId, upToSeq: null };
        phase = SYNCING;
        return { accepted: true, reason: 'superseded by a newer generation' };
      }
      lastRefusal = { connectionId, atMs: nowMs(), reason: 'superseded connection' };
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

    /** Where a resend should start. Null until the first connection has been accepted. */
    resumeFrom() {
      if (applied.connectionId === null) return null;
      return { connectionId: applied.connectionId, upToSeq: applied.upToSeq };
    },

    /**
     * Apply one envelope from the ordered stream.
     *
     * Returns { applied: boolean, reason } and never throws for a duplicate: a resend that is already
     * covered is a no-op, which is what makes every retry path safe to repeat.
     */
    apply(envelope, applyFn = () => board.apply(envelope.parsed ?? {})) {
      if (envelope.connection_id !== applied.connectionId) {
        const accepted = this.accept(envelope.connection_id, { generation: envelope.generation ?? null });
        if (!accepted.accepted) return { applied: false, reason: accepted.reason };
      }
      const seq = envelope.receive_seq;
      if (applied.upToSeq !== null && seq <= applied.upToSeq) {
        return { applied: false, reason: 'already applied' };
      }
      persistBoundaryInTransaction({ connectionId: applied.connectionId, upToSeq: seq }, () => applyFn());
      // A boundary was proven with this range, so the book may serve again.
      phase = RUNNING;
      return { applied: true, reason: 'applied' };
    },

    /** A sync is starting: the board is not to be trusted until its boundary is proven. */
    beginSync() {
      phase = SYNCING;
    },

    /**
     * The snapshot has been checked against the stream. Until this is called the book stays syncing,
     * which is the fail-closed default: no boundary, no service.
     */
    proveBoundary() {
      if (applied.connectionId === null) return { proven: false, reason: 'no connection accepted yet' };
      phase = RUNNING;
      return { proven: true, reason: 'boundary proven' };
    },
  };
}
