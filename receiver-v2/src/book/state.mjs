/**
 * The book: keep the board, and be able to prove where it stands.
 *
 * Four rules decide everything here, each of them a correction of a way this could quietly lose
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
 * 4. Ownership is recorded, not inferred. Which run owns this board and where its connection's
 *    numbering starts are written down before anything is applied and read back after a restart,
 *    because a book that reopens without them cannot tell a legitimate successor from a replay of a
 *    run that was replaced. Generation numbers order connections inside one run only; a different run
 *    is admitted by an explicit takeover and by nothing else, and a retired run is refused whatever
 *    number it quotes - retired in the store, so a restart does not forget who was already replaced.
 *    A null run is an identity of its own: "we were never told the run" is not "any run will do".
 *
 * 5. The proof is the adapter's, and it comes before the commit. A venue number going up is not a proof by
 *    itself: the connection declares the kind of proof it can give and decides, per frame, whether the frame
 *    connects to what came before. The book binds that proof to the board, the connection, the anchor and the
 *    range it has verified, and a frame that fails it is judged against the board it would produce and writes
 *    nothing - the levels, the position and the ledger all stay where they were. An unanchored book (fresh,
 *    reopened, or a connection change) applies ordinary diffs, because that is how an anchor is built and how a
 *    hole is filled; only a broken proof refuses, and only a replacement re-anchors.
 * 6. A missing record of the connection the board follows is a hole in the range it proved. It is written down
 *    where it is decided, so a restart comes back waiting for a re-anchor rather than proving a range with a
 *    hole in it. A record of another connection is history, and it must not block a new board.
 */

import { bindConstructor, bindInternals, internalsOf } from '../internal/wiring.mjs';

const SYNCING = 'syncing';
const RUNNING = 'running';

// The proof of where the board stands, and what an adapter may declare about it. A proof is the adapter's:
// a `sequence` or `checksum` rule the connection provides, or an explicit `unverifiable` for a venue that
// has no means of proof. The states are the book's own - a fresh or reopened book is unanchored, a proof
// that has held since an anchor is holding, and a frame that did not connect breaks it until a replacement
// re-anchors.
const UNVERIFIABLE = 'unverifiable';
const PROOF_BROKEN = 'the boundary proof is broken';
const UNANCHORED = 'unanchored';
const HOLDING = 'holding';
const BROKEN = 'broken';

/** The kind an adapter declares, refused before anything is applied if it cannot be verified. */
function resolveProofKind(adapter) {
  const kind = adapter?.boundary ?? UNVERIFIABLE;
  if (kind !== 'sequence' && kind !== 'checksum' && kind !== UNVERIFIABLE) {
    throw new TypeError(`unknown boundary kind: ${JSON.stringify(kind)}`);
  }
  // A kind that claims a proof but brings no rule to decide it would put the book straight into a
  // service it cannot justify. Refused at the door rather than believed frame by frame.
  if (kind !== UNVERIFIABLE && typeof adapter?.connects !== 'function') {
    throw new TypeError(`a ${kind} boundary needs the adapter's connection rule`);
  }
  return kind;
}

const BOOK_SCHEMA = `
CREATE TABLE IF NOT EXISTS applied_boundary (
  market TEXT NOT NULL,
  stream TEXT NOT NULL,
  connection_id TEXT NOT NULL,
  generation INTEGER,
  up_to_receive_seq INTEGER,
  run_id TEXT,
  first_seq INTEGER,
  updated_at_ms INTEGER NOT NULL,
  PRIMARY KEY (market, stream)
);
CREATE TABLE IF NOT EXISTS retired_run (
  market TEXT NOT NULL,
  stream TEXT NOT NULL,
  run_id TEXT NOT NULL,
  retired_at_ms INTEGER NOT NULL,
  PRIMARY KEY (market, stream, run_id)
);
CREATE TABLE IF NOT EXISTS connection_identity (
  market TEXT NOT NULL,
  stream TEXT NOT NULL,
  connection_id TEXT NOT NULL,
  run_id TEXT,
  generation INTEGER,
  PRIMARY KEY (market, stream, connection_id)
);
CREATE TABLE IF NOT EXISTS legacy_owner (
  market TEXT NOT NULL,
  stream TEXT NOT NULL,
  migrated_at_ms INTEGER NOT NULL,
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
CREATE TABLE IF NOT EXISTS book_missing_record (
  market TEXT NOT NULL,
  stream TEXT NOT NULL,
  connection_id TEXT NOT NULL,
  reason TEXT NOT NULL,
  declared_at_ms INTEGER NOT NULL,
  PRIMARY KEY (market, stream, connection_id)
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

/**
 * Open a book. Opening it writes to the store, so the whole initialisation is one change operation: the
 * execution right is taken here, which makes a re-entrant call from a hook the initialisation calls (the
 * caller's clock, a writer, a migration's reader) an ordinary refusal instead of a nested `BEGIN`.
 *
 * The right, the transaction discipline and the observation of the right come from the wiring, never from
 * the object handed in: a module that used a method on that object could be made to hand its unguarded
 * routes to whoever replaced the method. Unbound means unopenable, before anything is written.
 */
export function openBook(options) {
  const wiring = internalsOf(options.durability);
  return wiring.whileChange(() => openBookWithin(options, wiring));
}

bindConstructor('book', openBookWithin);

function openBookWithin(options, wiring) {
  const {
    market, stream, durability, nowMs = () => Date.now(), adapter = null
  } = options;
  if (!market || !stream) throw new TypeError('a book needs a market and a stream');
  // The boundary proof is the adapter's, resolved once here so nothing downstream has to interpret a
  // declaration. An adapter that says nothing is a venue that proved nothing - running, but never claiming
  // a proof it did not make.
  const proofKind = resolveProofKind(adapter);
  const connectionRule = proofKind === UNVERIFIABLE ? null : (context) => adapter.connects(context);
  // The book writes several records in one transaction, so it needs the store's transaction discipline
  // rather than its own copy of it: a half-written ownership chain is not recoverable.


  wiring.db.exec(BOOK_SCHEMA);
  const board = createBoard();

  // The board comes back from the store, not from a caller's memory, and it comes back together with
  // the position it was written with.
  board.restore(
    wiring.db
      .prepare('SELECT side, price, size FROM book_level WHERE market = ? AND stream = ?')
      .all(market, stream),
  );

  // Two facts a boundary needs in order to mean the same thing after a restart: which run owns the
  // board, and where that connection's numbering starts. A store written before they existed is
  // migrated here by asking which columns it has rather than by assuming it is new; the question is asked
  // inside the write lock, and nothing that fails is swallowed, because a half-migrated store that looks
  // migrated is worse than one that refuses to open.
  {
    const columnNames = () =>
      new Set(
        wiring.db.prepare('PRAGMA table_info(applied_boundary)').all().map((column) => column.name),
      );
    const ownershipColumns = [
      ['run_id', 'TEXT'],
      ['first_seq', 'INTEGER'],
    ];
    // The look before the transaction is only a reason to enter it: what is actually added is decided inside,
    // with the write lock held, because another process may have migrated this same store since.
    if (ownershipColumns.some(([name]) => !columnNames().has(name))) {
      wiring.inTransaction(() => {
        // §6.2: the columns are read inside the transaction, so the set that is missing is the set at the
        // moment of writing - with the write lock held, nothing else can be moving the store underneath.
        const present = columnNames();
        const created = new Set();
        for (const [name, type] of ownershipColumns) {
          if (present.has(name)) continue;
          wiring.db.exec(`ALTER TABLE applied_boundary ADD COLUMN ${name} ${type}`);
          created.add(name);
        }

        // Only a transaction that created run_id itself can have rows whose NULL means "there was no column
        // to write an owner in when this row was made": if the column was already there - even if this
        // process's earlier look said otherwise, because another process migrated in between - then every row
        // had its chance to record an owner, and a NULL in it is a recorded "no run". Marking those rows here
        // would hand the board to an uninvited run on the next start, which is the one thing the marker
        // exists to prevent.
        if (!created.has('run_id')) return;
        const boards = wiring.db
          .prepare('SELECT market, stream FROM applied_boundary WHERE run_id IS NULL')
          .all();
        const mark = wiring.db.prepare(
          'INSERT OR REPLACE INTO legacy_owner (market, stream, migrated_at_ms) VALUES (?, ?, ?)',
        );
        for (const board of boards) mark.run(board.market, board.stream, nowMs());
      });
    }
  }

  const row = wiring.db
    .prepare(
      `SELECT connection_id, generation, up_to_receive_seq, run_id, first_seq
       FROM applied_boundary WHERE market = ? AND stream = ?`,
    )
    .get(market, stream);

  let applied = row
    ? {
        connectionId: row.connection_id,
        generation: row.generation ?? null,
        upToSeq: row.up_to_receive_seq ?? null,
        runId: row.run_id ?? null,
        firstSeq: row.first_seq ?? null,
      }
    : { connectionId: null, generation: null, upToSeq: null, runId: null, firstSeq: null };

  // A missing range recorded for the connection this board follows means the range it could prove has a
  // hole in it. That fact outlives the process that found it, so it is read back here: the board comes back
  // waiting for a re-anchor instead of proving a range it knows is incomplete (C7).
  const missingForFollowed =
    applied.connectionId !== null &&
    wiring.db
      .prepare(
        'SELECT 1 AS recorded FROM book_missing_record WHERE market = ? AND stream = ? AND connection_id = ?',
      )
      .get(market, stream, applied.connectionId) !== undefined;

  // A run recorded in the store is the authority here. A NULL on a row that was written before the
  // column existed says nothing about ownership - nobody was ever asked - so those boards are treated as
  // unestablished, one board at a time, until the first explicit accept says who owns them. A NULL on a
  // row written by this version is a recorded "no run", which is a different thing.
  const ownerWasNeverRecorded =
    wiring.db
      .prepare('SELECT 1 AS recorded FROM legacy_owner WHERE market = ? AND stream = ?')
      .get(market, stream) !== undefined;
  let ownerEstablished = row ? !ownerWasNeverRecorded : false;

  // Runs that have been replaced, read back from the store: a run does not come back after a restart
  // either, whatever number it quotes.
  const retiredRuns = new Set(
    wiring.db
      .prepare('SELECT run_id FROM retired_run WHERE market = ? AND stream = ?')
      .all(market, stream)
      .map((retired) => retired.run_id),
  );

  // A run name is never empty, so the empty string is how this store names the owner that has no run name. An
  // owner is an owner whether it carries a name or not: the one without a name is replaced by a takeover and is
  // refused afterwards like any other. It is stored as '' rather than NULL because a key cannot be NULL - SQLite
  // would let many NULL rows stand for the same owner, and the retirement would not hold.
  const RETIRED_UNNAMED = '';

  let phase = SYNCING; // a fresh or reopened book proves its boundary before serving
  // The proof the book is holding, bound to the board, the connection, the anchor it started from and the
  // range it has verified. A book whose followed connection has a missing record stored comes back broken,
  // not unanchored: there is no range it may prove until a replacement re-anchors it.
  const proof = {
    kind: proofKind,
    state: missingForFollowed ? BROKEN : UNANCHORED,
    connectionId: missingForFollowed ? applied.connectionId : null,
    anchorSeq: null,
    upToSeq: null,
  };
  // The last frame the rule accepted: the rule's own idea of "what came before". It is memory, like the
  // proof itself - a reopened book is unanchored and builds a new anchor from the first frame it applies.
  let provenEnvelope = null;
  let lastRefusal = null;
  // The lowest sequence this book has been handed while the origin of the connection was still unknown. A
  // frame that has arrived can only be at or after the origin, so an origin above it would skip past data
  // the book has already seen - and completing the origin there is how those frames become permanently
  // unapplicable, refused later as "already applied" while nothing ever applied them (§2.2).
  let lowestSeenWithoutOrigin = null;
  // Frames that arrived ahead of a hole, kept rather than dropped: applying them now would move the
  // position past data that has not arrived and make that data permanently unapplicable, but
  // discarding them would mean the missing frame arrives and nothing else follows.
  const waiting = new Map(); // receive_seq -> { envelope, changes }
  // Whether the last walk over the waiting queue ended because the rule refused a held frame. The caller
  // hears it as a proof break, so a re-anchor can be asked for - a break found while draining is the same
  // event as a break found on a frame that arrived on its own.
  let lastDrainBroke = false;

  const boundaryStatement = wiring.db.prepare(
    `INSERT OR REPLACE INTO applied_boundary
       (market, stream, connection_id, generation, up_to_receive_seq, run_id, first_seq, updated_at_ms)
     VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
  );
  const retireStatement = wiring.db.prepare(
    `INSERT OR REPLACE INTO retired_run (market, stream, run_id, retired_at_ms) VALUES (?, ?, ?, ?)`,
  );
  const clearUnrecordedOwnerStatement = wiring.db.prepare(
    'DELETE FROM legacy_owner WHERE market = ? AND stream = ?',
  );
  const identityStatement = wiring.db.prepare(
    `INSERT OR REPLACE INTO connection_identity (market, stream, connection_id, run_id, generation)
     VALUES (?, ?, ?, ?, ?)`,
  );
  const recordedIdentityStatement = wiring.db.prepare(
    'SELECT run_id, generation FROM connection_identity WHERE market = ? AND stream = ? AND connection_id = ?',
  );
  // A store that records an owner but no identity for the name it records was written before that record
  // existed. The name is its own - the owner is right there in the row - so the identity is written down now
  // rather than left open to being claimed by another run or generation. A store whose row predates the
  // ownership columns is left alone: it has no established owner to write down yet.
  if (row && ownerEstablished && recordedIdentityStatement.get(market, stream, row.connection_id) === undefined) {
    wiring.inTransaction(() => {
      identityStatement.run(market, stream, row.connection_id, applied.runId, applied.generation);
    });
  }

  const levelUpsert = wiring.db.prepare(
    `INSERT OR REPLACE INTO book_level (market, stream, side, price, size) VALUES (?, ?, ?, ?, ?)`,
  );
  const levelDelete = wiring.db.prepare(
    'DELETE FROM book_level WHERE market = ? AND stream = ? AND side = ? AND price = ?',
  );
  // A replacement carries the whole board, so the levels it does not name are gone rather than left
  // standing - the one change a diff can never express.
  const levelDeleteAll = wiring.db.prepare('DELETE FROM book_level WHERE market = ? AND stream = ?');
  // The record that a range of this connection was declared missing. It is the proof's own state written
  // down, so a restart reads it back rather than re-deriving a proof that no longer covers what it holds.
  const missingRecordStatement = wiring.db.prepare(
    `INSERT OR REPLACE INTO book_missing_record (market, stream, connection_id, reason, declared_at_ms)
     VALUES (?, ?, ?, ?, ?)`,
  );
  const clearMissingRecordStatement = wiring.db.prepare(
    'DELETE FROM book_missing_record WHERE market = ? AND stream = ? AND connection_id = ?',
  );

  /**
   * Write down who owns this board, and that the run it replaces is finished.
   *
   * The retirement, the end of "nobody ever recorded an owner here", and the new owner land in one
   * transaction and in this order, so a crash cannot leave a store that has handed the board to a new
   * run and forgotten that the old one was replaced. Nothing in memory changes until the commit has
   * succeeded: a failed write must leave the book exactly as it was, not halfway into a takeover it did
   * not complete.
   */
  function persistAcceptance(next, { retiredOwner = undefined, clearUnrecordedOwner = false } = {}) {
    // undefined means there is nothing to retire; null is the owner that has no run name.
    const retiring = retiredOwner !== undefined;
    const retiredKey = retiredOwner ?? RETIRED_UNNAMED;
    // A different connection, run or generation numbers from its own start: what was seen under the previous
    // identity says nothing about where this one begins.
    const identityChanged =
      next.connectionId !== applied.connectionId ||
      (next.generation ?? null) !== (applied.generation ?? null) ||
      (next.runId ?? null) !== (applied.runId ?? null);
    wiring.inTransaction(() => {
      if (retiring) retireStatement.run(market, stream, retiredKey, nowMs());
      if (clearUnrecordedOwner) clearUnrecordedOwnerStatement.run(market, stream);
      // The name and the identity it was accepted as are written down together with the position: a name is what
      // frames are deduped by, so it must not be re-usable for another run or generation later (§2, C2).
      identityStatement.run(market, stream, next.connectionId, next.runId ?? null, next.generation ?? null);
      boundaryStatement.run(
        market,
        stream,
        next.connectionId,
        next.generation ?? null,
        next.upToSeq ?? null,
        next.runId ?? null,
        next.firstSeq ?? null,
        nowMs(),
      );
    });
    // Only after the commit does the in-memory book follow the store.
    if (retiring) retiredRuns.add(retiredKey);
    if (identityChanged) lowestSeenWithoutOrigin = null;
    applied = next;
  }

  /**
   * A frame's changes, whether the adapter hands back the diff shape or the snapshot shape (C5). A bare
   * array is a diff; `{ replace: true, levels }` replaces the board; `{ replace: false, changes }` is a
   * diff spelled out. Nothing downstream has to know which the adapter chose.
   */
  function normalizeFrame(changes) {
    if (Array.isArray(changes)) return { replace: false, changes };
    if (changes && typeof changes === 'object' && changes.replace === true) {
      return { replace: true, levels: Array.isArray(changes.levels) ? changes.levels : [] };
    }
    const list = changes && typeof changes === 'object' && Array.isArray(changes.changes) ? changes.changes : [];
    return { replace: false, changes: list };
  }

  /**
   * The board the frame would produce, plus the position it would take: what a proof that depends on the
   * levels themselves (a checksum over the top of the board) has to see before it can decide.
   */
  function candidateOf(frame) {
    if (frame.replace) {
      return frame.levels
        .filter((level) => level.size !== 0)
        .map((level) => ({ side: level.side, price: level.price, size: level.size }));
    }
    const merged = new Map(board.rows().map((row) => [`${row.side}:${row.price}`, row]));
    for (const change of frame.changes) {
      const key = `${change.side}:${change.price}`;
      if (change.size === 0) merged.delete(key);
      else merged.set(key, { side: change.side, price: change.price, size: change.size });
    }
    return [...merged.values()];
  }

  /**
   * Whether this frame connects to what came before, as the adapter's rule decides it.
   *
   * The rule is the proof: a bare number does not connect anything by itself, and a venue number going up
   * is not a connection either. The rule is consulted for every frame a verifiable venue sends, including
   * the first of a range - the frame that builds an anchor is judged on its own, with nothing before it,
   * which is exactly how a snapshot proves itself. An unanchored book is not a book that believes
   * anything: it is a book whose range is not verified yet, and only the frames the rule accepts become
   * part of that range.
   */
  function frameConnects(envelope, frame) {
    if (proofKind === UNVERIFIABLE) return true;
    return connectionRule({
      previous: provenEnvelope,
      current: envelope,
      replace: frame.replace,
      candidate: { position: envelope.receive_seq, rows: candidateOf(frame) },
    }) === true;
  }

  /**
   * The proof held over this frame: it binds to the connection and extends the verified range.
   *
   * A replacement re-anchors - its own position is where the proof starts again - so the hole records
   * older than it are history and must not block the recovery (C7). Nothing else moves the anchor: a diff
   * extends the range the proof already has.
   */
  function recordVerifiedProof(envelope, seq, { reanchor = false } = {}) {
    if (reanchor || proof.state === UNANCHORED || proof.state === BROKEN) proof.anchorSeq = seq;
    proof.state = HOLDING;
    proof.connectionId = envelope.connection_id;
    proof.upToSeq = seq;
    provenEnvelope = envelope;
  }

  /**
   * The rule did not hold: the proof is dropped and the board stops serving until a replacement.
   *
   * The break is written down rather than kept in memory: a restart that came back merely unanchored
   * would apply the very frames the rule refused, with nothing left to say they were refused (C6,
   * fail-closed across a restart). A replacement clears the record, because re-anchoring is what
   * answers it.
   */
  function markProofBroken(envelope, reason = 'the boundary rule did not hold over this frame') {
    const connectionId = envelope?.connection_id ?? proof.connectionId;
    // The write comes first, and the in-memory book follows it: a record that cannot be written must leave
    // the book exactly as it was, memory included - the discipline every durable change here follows.
    if (connectionId !== null) {
      missingRecordStatement.run(market, stream, connectionId, reason, nowMs());
    }
    proof.state = BROKEN;
    proof.connectionId = connectionId;
    proof.anchorSeq = null;
    proof.upToSeq = null;
    phase = SYNCING;
  }

  /**
   * A different connection is not a continuation of the old one's proof: it numbers from its own start,
   * so what was verified under the previous name says nothing about it. A fresh anchor has to be built.
   */
  function resetProof() {
    proof.state = UNANCHORED;
    proof.connectionId = null;
    proof.anchorSeq = null;
    proof.upToSeq = null;
    provenEnvelope = null;
  }

  /** Everything that makes one range durable: the levels and the position, in one transaction. */
  function commitRange({ frame, next }) {
    wiring.inTransaction(() => {
      for (const change of frame.changes) {
        if (change.size === 0) levelDelete.run(market, stream, change.side, change.price);
        else levelUpsert.run(market, stream, change.side, change.price, change.size);
      }
      boundaryStatement.run(
        market,
        stream,
        next.connectionId,
        next.generation ?? null,
        next.upToSeq ?? null,
        next.runId ?? null,
        next.firstSeq ?? null,
        nowMs(),
      );
    });
    // Only after the commit does the in-memory board follow the store.
    for (const change of frame.changes) board.apply(change);
    applied = next;
  }

  /**
   * A replacement is self-contained, so it is written down as a replacement: the levels it does not name
   * are gone, the position moves to its numbering, and the missing record this connection was waiting on is
   * resolved. The past hole records stay (C7: a loss is history); it is the board's readiness that resets.
   */
  function commitReplacement({ frame, next }) {
    const levels = frame.levels.filter((level) => level.size !== 0).map((level) => ({ ...level }));
    wiring.inTransaction(() => {
      levelDeleteAll.run(market, stream);
      for (const level of levels) levelUpsert.run(market, stream, level.side, level.price, level.size);
      boundaryStatement.run(
        market,
        stream,
        next.connectionId,
        next.generation ?? null,
        next.upToSeq ?? null,
        next.runId ?? null,
        next.firstSeq ?? null,
        nowMs(),
      );
      clearMissingRecordStatement.run(market, stream, next.connectionId);
    });
    // Only after the commit does the in-memory board follow the store.
    board.restore(levels);
    applied = next;
  }

  function recordGap(waitingFor, seenSeq) {
    wiring.db
      .prepare(
        `INSERT INTO book_gap (market, stream, connection_id, waiting_for, seen_seq, detected_at_ms)
         VALUES (?, ?, ?, ?, ?, ?)`,
      )
      .run(market, stream, applied.connectionId, waitingFor, seenSeq, nowMs());
    // C7: a hole in what this connection has delivered means the board is no longer in a state it may
    // call serving. The hole stays a record; it is the phase that has to change.
    phase = SYNCING;
  }

  function closeGaps(upTo) {
    // C7: a hole belongs to the connection that opened it. Closing by sequence alone lets a new
    // connection's numbering fill a hole a dead one left behind, which reads as "the missing data
    // arrived" when nothing of the sort happened.
    wiring.db
      .prepare(
        `UPDATE book_gap SET filled_at_ms = ?
         WHERE market = ? AND stream = ? AND connection_id = ? AND filled_at_ms IS NULL AND waiting_for <= ?`,
      )
      .run(nowMs(), market, stream, applied.connectionId, upTo);
  }

  /**
   * Apply whatever was held ahead of a hole, now that the hole is filled. In order, one transaction
   * each, so a crash between them leaves the stored position at whatever was actually applied.
   *
   * Each frame out of the queue is judged by the same rule a live one is: a frame that does not connect
   * stops the walk where it is, and the frames behind it are not applied either - a replacement is what
   * re-anchors a queue whose rule has broken.
   */
  function drainWaiting() {
    let count = 0;
    lastDrainBroke = false;
    for (;;) {
      if (applied.upToSeq === null) break;
      const nextSeq = applied.upToSeq + 1;
      if (!waiting.has(nextSeq)) break;
      const held = waiting.get(nextSeq);
      if (proof.state === BROKEN || !frameConnects(held.envelope, held.frame)) {
        if (proof.state !== BROKEN) markProofBroken(held.envelope);
        lastDrainBroke = true;
        break;
      }
      commitRange({ frame: held.frame, next: { ...applied, upToSeq: nextSeq } });
      recordVerifiedProof(held.envelope, nextSeq);
      // Released only once it is durable: a failed write must leave the frame held so it can be
      // applied later, not leave it dropped with no record of it ever having arrived.
      waiting.delete(nextSeq);
      closeGaps(nextSeq);
      count += 1;
    }
    return count;
  }

  /**
   * The snapshot has been checked against the stream. Only this puts the book back in service; a
   * fresh book has proven nothing, and reaching this with no connection accepted is not possible.
   *
   * C6: a connection existing is not a boundary, and a declared origin is not a proof either - what is
   * required is a range that was actually applied. Corrected: the adapter's own rule has to have held
   * over that range since the anchor. A range the rule never judged, or whose proof broke, is not a
   * boundary. Note the loose comparison: on reopen the field comes back undefined rather than null.
   *
   * A closure rather than a method, because the book itself asks this question when a hole it recorded
   * is filled: the board knows when its range is whole again, and a repair that waits to be asked for is
   * a repair that does not happen (C7).
   */
  function proveNow() {
    if (applied.connectionId !== null && applied.upToSeq == null) {
      return { proven: false, kind: proofKind, verified: false, reason: 'the board holds nothing from this connection yet' };
    }
    // C7: an open hole within the range the proof covers is not a boundary either. Holes older than the
    // anchor are history - exactly the past losses a replacement re-anchors over - so they are deliberately
    // not consulted here, or a board could never recover from one.
    const openForThisConnection = wiring.db
      .prepare(
        `SELECT COUNT(*) AS n FROM book_gap
         WHERE market = ? AND stream = ? AND connection_id = ? AND filled_at_ms IS NULL AND waiting_for >= ?`,
      )
      .get(market, stream, applied.connectionId, proof.anchorSeq ?? -1).n;
    if (openForThisConnection > 0) {
      return { proven: false, kind: proofKind, verified: false, reason: 'this connection has an unresolved hole' };
    }
    if (applied.connectionId === null) {
      return { proven: false, kind: proofKind, verified: false, reason: 'no connection accepted yet' };
    }
    if (proof.state === BROKEN) {
      return { proven: false, kind: proofKind, verified: false, reason: PROOF_BROKEN };
    }
    if (proofKind === UNVERIFIABLE) {
      // C6: a venue with no means of proof may still run - but nothing here is a proof, and the result says
      // both things rather than dressing the running up as one.
      phase = RUNNING;
      return {
        proven: true,
        kind: UNVERIFIABLE,
        verified: false,
        reason: 'the book runs without a boundary proof: this venue is unverifiable',
        anchorSeq: proof.anchorSeq,
        upToSeq: proof.upToSeq,
      };
    }
    const covers =
      proof.state === HOLDING &&
      proof.connectionId === applied.connectionId &&
      proof.anchorSeq !== null &&
      proof.upToSeq === applied.upToSeq;
    if (!covers) {
      return { proven: false, kind: proofKind, verified: false, reason: 'no verified range covers the applied position' };
    }
    phase = RUNNING;
    return {
      proven: true,
      kind: proofKind,
      verified: true,
      reason:
        proofKind === 'checksum'
          ? 'the top levels of the board are proved against the connection (checksum)'
          : 'the connection rule is proved over the applied range (sequence)',
      anchorSeq: proof.anchorSeq,
      upToSeq: proof.upToSeq,
    };
  }

  const api = {
    market,
    stream,
    board,

    /**
     * Which connection this book is willing to accept.
     *
     * C11: generation numbers only order connections inside one run. A different run is not "older" or
     * "newer", it is unrelated, and the only safe way to hand the book to it is to say so. Without this
     * a restarted process (whose generation starts again) is refused as stale, and a replay of a dead
     * run can take over by accident - both directions were reproduced in an audit, which is why the
     * takeover is authorised by the caller and never inferred from a bigger number.
     */
    accept(connectionId, { generation = null, firstSeq = null, runId = null, takeover = false } = {}) {
      // A replaced run is finished. Letting it back in with a larger number would undo the takeover it
      // lost, and a replay of its old frames would look newer than it is.
      if (retiredRuns.has(runId ?? RETIRED_UNNAMED)) {
        return {
          accepted: false,
          reason:
            runId === null
              ? 'the owner without a run name was already replaced'
              : 'this run was already replaced',
        };
      }

      // A name that disagrees with the identity announcing it is a contradiction, whoever is announcing it -
      // and it is refused before any hand-over is considered. A takeover of a connection that holds this very
      // name would reset the position for a run whose frames the organizer, still numbering under that name,
      // would then treat as already durable: the board and the raw would describe different data under one
      // name. A connection that really is new has a name of its own, because the name carries the run.
      //
      // This asks about an owner that is established: a board whose row predates the ownership columns has no
      // owner to disagree with yet, and the accept that establishes one is exactly what should be allowed
      // through - including for the name the store already carries.
      if (
        ownerEstablished &&
        connectionId === applied.connectionId &&
        (generation !== applied.generation || runId !== (applied.runId ?? null))
      ) {
        return { accepted: false, reason: 'this connection id disagrees with the identity accepting it' };
      }

      // A name belongs to the identity it was accepted as, for good. Letting the same name be re-used for
      // another run or generation would apply frames of one identity to a board of another while the raw still
      // holds the first one's data under that same name - and a frame under the old name would be answered as
      // "already durable", so it would never be written again (C2, C8).
      const recorded = recordedIdentityStatement.get(market, stream, connectionId);
      if (
        recorded !== undefined &&
        ((recorded.run_id ?? null) !== runId || (recorded.generation ?? null) !== generation)
      ) {
        return { accepted: false, reason: 'this connection name has already been used for another identity' };
      }

      if (applied.connectionId === null) {
        // Nothing has ever owned this board, so this connection does. The generation is recorded as the
        // baseline rather than compared against a number that does not exist.
        persistAcceptance({ connectionId, generation, firstSeq, runId, upToSeq: null });
        ownerEstablished = true;
        resetProof();
        phase = SYNCING;
        return { accepted: true, reason: 'first connection' };
      }

      if (!ownerEstablished) {
        // This board's row predates the ownership columns, so who owns it is genuinely unknown - and the
        // first explicit accept is what says. No takeover is demanded (there is nobody to take it from)
        // and the generation it brings is taken as the baseline rather than compared. The board is kept:
        // those levels are facts. The position is not, because it belongs to the numbering of a
        // connection that cannot speak again - a run-scoped name is issued once. Claiming the board is
        // what ends "nobody ever recorded an owner here", and it ends in the same transaction as the
        // claim itself, so a failed claim leaves the board exactly as unestablished as it was.
        persistAcceptance({ connectionId, generation, firstSeq, runId, upToSeq: null }, { clearUnrecordedOwner: true });
        ownerEstablished = true;
        waiting.clear();
        resetProof();
        phase = SYNCING;
        return { accepted: true, reason: 'ownership established for a store that predates it' };
      }

      const ownerRun = applied.runId ?? null;
      // Null is an identity, not a wildcard: a null run matches a null owner and nothing else.
      const sameRun = ownerRun === runId;
      if (!sameRun && takeover !== true) {
        return { accepted: false, reason: 'a different run needs an explicit takeover' };
      }
      if (!sameRun) {
        // A takeover is not a comparison: the new run starts its own numbering, so the previous run's
        // generation is replaced rather than beaten, and the previous run is retired in the store.
        persistAcceptance(
          { connectionId, generation, firstSeq, runId, upToSeq: null },
          { retiredOwner: ownerRun },
        );
        waiting.clear();
        resetProof();
        phase = SYNCING;
        return { accepted: true, reason: 'a new run took the board over' };
      }

      // The same run, and the same connection: this is a reopen or a reconnect of a connection that is
      // still current (a name that disagreed with its identity was refused above), and the only thing that
      // can be completed is an origin nobody established yet.
      if (connectionId === applied.connectionId && generation === applied.generation) {
        if (applied.firstSeq === null && firstSeq !== null) {
          // The start of this connection arrived after its frames did. Recording it changes no position,
          // no hole and no phase: it is the anchor, not the data. It may not, however, be above a frame this
          // book has already been handed - that would skip past data already seen, and §2.2 forbids moving an
          // origin onto a sequence that has arrived.
          if (lowestSeenWithoutOrigin !== null && firstSeq > lowestSeenWithoutOrigin) {
            return { accepted: false, reason: 'this origin is beyond frames that have already arrived' };
          }
          persistAcceptance({ ...applied, firstSeq });
          return { accepted: true, reason: 'the start of this connection is now known' };
        }
        if (applied.firstSeq !== null && firstSeq !== null && applied.firstSeq !== firstSeq) {
          // An established origin is not renegotiable: moving it is how a hole gets jumped.
          return { accepted: false, reason: 'the start of this connection is already established' };
        }
        return { accepted: true, reason: 'same connection' };
      }

      // A different connection inside the same run: only a strictly newer generation replaces it.
      const supersedes =
        typeof generation === 'number' && (applied.generation === null || generation > applied.generation);
      if (supersedes) {
        persistAcceptance({ connectionId, generation, firstSeq, runId, upToSeq: null });
        waiting.clear();
        resetProof();
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
    /** The proof in hand: its kind, its state, and the range it has verified from its anchor. */
    get proof() {
      return {
        kind: proof.kind,
        state: proof.state,
        connectionId: proof.connectionId,
        anchorSeq: proof.anchorSeq,
        upToSeq: proof.upToSeq,
      };
    },

    /**
     * Whether this board has an owner at all. A row that predates the ownership columns has none until an
     * explicit accept says who it is, whatever connection name that row carries - and until then nothing
     * may be written off as belonging to a connection that is gone, because the accept that claims the
     * board may be the very one that admits the frames in question.
     */
    get ownerEstablished() {
      return ownerEstablished;
    },

    resumeFrom() {
      if (applied.connectionId === null) return null;
      return { connectionId: applied.connectionId, upToSeq: applied.upToSeq };
    },

    /** Runs this board has replaced, oldest first as far as the store remembers them. */
    retiredRuns() {
      // The owner without a run name is reported as null, which is the identity it was accepted as.
      return [...retiredRuns].map((key) => (key === RETIRED_UNNAMED ? null : key));
    },

    /**
     * Apply one envelope together with the level changes it carries.
     *
     * The frame must belong to this board and to the connection this book accepted - market, stream,
     * run, generation and connection id are all part of that identity, and checking only the connection
     * id is how a frame from a retired run, or a frame meant for another board, walks in on a
     * technicality. That check comes before the duplicate test and before any hole is recorded, so a
     * frame that is not ours never moves anything.
     *
     * Contiguous only: a frame whose predecessors are missing is refused and the hole is recorded,
     * because applying it would make the missing data permanently unapplicable. Duplicates are no-ops.
     * Applying data never changes the phase, and the proof the adapter declares is judged against the
     * frame's own candidate before anything is committed: a frame that fails it writes nothing and drops
     * the board back to syncing. A self-contained replacement is the one frame that walks past a hole and
     * a broken proof, because re-anchoring is what both are waiting for.
     */
    apply({ envelope, changes = [] }) {
      // Ownership comes first. A board whose row predates the ownership columns, or that has never been claimed,
      // has no owner until an explicit accept says who it is - and a frame applied before that would be applied
      // on behalf of nobody, which is exactly the state the ownership record and its marker exist to keep out.
      if (!ownerEstablished) {
        return { applied: false, reason: 'the owner of this board has not been established yet' };
      }
      if (envelope.market !== market || envelope.stream !== stream) {
        return { applied: false, reason: 'this frame belongs to another board' };
      }
      if (envelope.connection_id !== applied.connectionId) {
        // A connection is taken over only by an explicit accept. Accepting on sight is how a frame from
        // a connection nobody agreed to trust walks straight into the book, which is what a restarted
        // process replaying an old connection's tail would look like.
        const reason =
          applied.connectionId === null ? 'no connection has been accepted yet' : 'connection not accepted';
        return { applied: false, reason };
      }
      if ((envelope.run_id ?? null) !== (applied.runId ?? null)) {
        return { applied: false, reason: 'this frame belongs to another run' };
      }
      if ((envelope.generation ?? null) !== (applied.generation ?? null)) {
        return { applied: false, reason: 'this frame belongs to another generation' };
      }
      const seq = envelope.receive_seq;
      if (
        applied.upToSeq !== null &&
        seq <= applied.upToSeq &&
        (applied.firstSeq === null || seq >= applied.firstSeq)
      ) {
        return { applied: false, reason: 'already applied' };
      }

      // Whatever else happens, a frame this book has been handed is the ceiling on where this connection's
      // numbering can start: the origin is at or before it. Recorded here - before any declaration is judged -
      // so that a declaration refused for being too high cannot be replaced by a later one that skips this very
      // frame, which would leave it refused for ever as already applied (§2.2).
      if (applied.firstSeq === null) {
        lowestSeenWithoutOrigin = lowestSeenWithoutOrigin === null ? seq : Math.min(lowestSeenWithoutOrigin, seq);
      }

      // The origin a frame declares is judged here, before anything is written, because a declaration
      // refused for being too high must not be replaceable by a later one that skips this very frame -
      // the in-memory ceiling below is what holds that. The declaration itself is written down only once
      // the frame has passed every check: a frame the proof refuses writes nothing, origin included.
      let declaredOrigin = null;
      if (applied.firstSeq === null && envelope.meta?.first_seq != null) {
        const declared = envelope.meta.first_seq;
        // A connection's numbering starts at or before the first frame that carries it, so a frame declaring an
        // origin above its own sequence contradicts itself - and one above a sequence already handed over
        // contradicts that. Either way the frames below the declaration would be lost if it won, so it is
        // refused rather than believed.
        if (declared > seq || (lowestSeenWithoutOrigin !== null && declared > lowestSeenWithoutOrigin)) {
          return { applied: false, reason: 'the origin this frame declares is beyond frames that have already arrived' };
        }
        declaredOrigin = declared;
      }

      // The declaration is written down wherever the frame is relied on - held for a hole, or applied -
      // and never on a frame the proof refuses: a refused frame writes nothing, origin included.
      const declareOrigin = () => {
        if (declaredOrigin !== null && applied.firstSeq === null) {
          persistAcceptance({ ...applied, firstSeq: declaredOrigin });
        }
      };

      const frame = normalizeFrame(changes);
      const firstSeq = applied.firstSeq ?? envelope.meta?.first_seq ?? null;

      // A replacement carries the whole board, so it is not held behind a hole and it is judged only by
      // its own proof. It is the one frame that may walk past a broken proof, because re-anchoring is
      // exactly what a broken proof waits for.
      if (frame.replace) {
        if (!frameConnects(envelope, frame)) {
          markProofBroken(envelope);
          return { applied: false, reason: PROOF_BROKEN, proofBroken: true };
        }
        if (declaredOrigin !== null) declareOrigin();
        commitReplacement({
          frame,
          next: { ...applied, connectionId: applied.connectionId, upToSeq: seq, firstSeq },
        });
        recordVerifiedProof(envelope, seq, { reanchor: true });
        // The replacement accounts for the board from here on: frames it has overtaken are no longer
        // ahead of the position and are dropped. The hole records stay (C7) - they are history.
        waiting.clear();
        return { applied: true, reason: 'applied', replaced: true };
      }

      // Below where this connection's numbering starts, whatever the position says: it was never applied and
      // never will be, so it is a permanent loss rather than a duplicate - and reporting it as a duplicate
      // would hide the fact that the frame is nowhere on this board.
      if (firstSeq !== null && seq < firstSeq) {
        return { applied: false, reason: 'below the first sequence' };
      }

      // Only a broken proof refuses an ordinary frame outright. An unanchored book applies diffs - that is how
      // an anchor is built and how a hole is filled - and a frame held for a hole is not being applied yet.
      if (proof.state === BROKEN) {
        return { applied: false, reason: PROOF_BROKEN, proofBroken: true };
      }

      if (applied.upToSeq === null) {
        if (firstSeq === null) {
          // Nowhere to anchor the boundary. Starting at whatever arrived first would be guessing at
          // where this connection's stream begins.
          return { applied: false, reason: 'first sequence unknown' };
        }
        if (seq > firstSeq) {
          // The first sequence never arrived. That hole is a fact, and this frame is kept until it
          // is filled rather than dropped on the floor.
          declareOrigin();
          recordGap(firstSeq, seq);
          waiting.set(seq, { envelope, frame });
          return { applied: false, reason: 'waiting for the first sequence', waitingFor: firstSeq };
        }
      } else if (seq !== applied.upToSeq + 1) {
        // A hole: record it, keep this frame, and let the position stay where the board really is.
        declareOrigin();
        recordGap(applied.upToSeq + 1, seq);
        waiting.set(seq, { envelope, frame });
        return { applied: false, reason: 'gap before this sequence', waitingFor: applied.upToSeq + 1 };
      }

      // Verify before commit: the frame is judged against the board it would produce and the position it
      // would take, so a frame that does not connect writes nothing at all.
      if (!frameConnects(envelope, frame)) {
        markProofBroken(envelope);
        return { applied: false, reason: PROOF_BROKEN, proofBroken: true };
      }

      // A hole the proof already covered does not break the proof, it only takes the board out of service
      // (C7) - so whether this frame may put it back is decided by the state the proof was in before it.
      const wasHolding = proof.state === HOLDING;

      // The origin this frame is anchored on is recorded with the position it makes durable: a boundary
      // whose start is only in memory is a boundary that cannot be shown again after a restart.
      declareOrigin();
      commitRange({
        frame,
        next: { ...applied, connectionId: applied.connectionId, upToSeq: seq, firstSeq },
      });
      recordVerifiedProof(envelope, seq);
      closeGaps(seq);
      const alsoApplied = drainWaiting();
      // The range a hole cut this connection's proof across is whole again, and the proof still covers it:
      // the board goes back to serving by itself, because a repair that waits to be asked for is a repair
      // that does not happen. A break found while draining is reported as the break it is.
      if (!lastDrainBroke && wasHolding && phase !== RUNNING) proveNow();
      return { applied: true, reason: 'applied', alsoApplied, ...(lastDrainBroke ? { proofBroken: true } : {}) };
    },

    /** Holes this book is waiting for. Unfilled ones are what it cannot claim to have. */
    openGaps() {
      return wiring.db
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

    /** The guarded door to `proveNow`: a caller asks whether the boundary is proved, and only a proven
     *  boundary puts the book back in service. See `proveNow` for what a proof has to be. */
    proveBoundary() {
      return proveNow();
    },

    /**
     * A range of this connection has been declared missing. The proof covered a range with a hole in it,
     * so it is dropped rather than left standing: a proof that outlived its range would put the book
     * straight back into service on the next success. The record is written down, so a restart comes back
     * waiting for a re-anchor; the hole records themselves stay, because a loss is history (C7).
     *
     * A loss of another connection is history too, and it must not block the board running now: this is a
     * no-op unless the connection named is the one this board follows (C11).
     */
    invalidateProof(connectionId, reason = 'a range of this connection was declared missing') {
      if (connectionId !== applied.connectionId) return { invalidated: false };
      // The record and the dropped proof are the same fact, so the reason is written with them: writing a
      // default first and overwriting it would leave the store saying one thing and then the other, and
      // the reason is what the restart reads.
      markProofBroken({ connection_id: connectionId }, reason);
      return { invalidated: true };
    },
  };

  // The board as a caller sees it. The levels this book believes in are worth reading - a boundary proof is
  // about exactly them - but writing them from outside would move the book's state without its position, and
  // the store and the memory disagreeing about where this board stands is the one thing that is not allowed.
  api.board = Object.freeze({
    size: (side, price) => board.size(side, price),
    rows: () => board.rows(),
    get depth() {
      return board.depth;
    },
  });

  // The unguarded routes. A caller that already holds the store's execution right - the module
  // that *is* the operation in progress - goes through these; every other caller takes the public
  // name above, which refuses a call that arrives while another change operation is running.
  const internal = {
    accept: api.accept,
    apply: api.apply,
    beginSync: api.beginSync,
    proveBoundary: api.proveBoundary,
    invalidateProof: api.invalidateProof,
  };
  api.accept = wiring.guard('book.accept', internal.accept);
  api.apply = wiring.guard('book.apply', internal.apply, (refusal) => ({ applied: false, code: refusal.code, reason: refusal.reason }));
  api.beginSync = wiring.guard('book.beginSync', internal.beginSync);
  api.proveBoundary = wiring.guard('book.proveBoundary', internal.proveBoundary);
  api.invalidateProof = wiring.guard('book.invalidateProof', internal.invalidateProof, (refusal) => ({ invalidated: false, code: refusal.code, reason: refusal.reason }));

  bindInternals(api, internal);
  return api;
}
