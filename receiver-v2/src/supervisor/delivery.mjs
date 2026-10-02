/**
 * The delivery ledger: what the raw holds and the board does not have yet.
 *
 * C8 and C11 keep two positions apart on purpose. The raw's durable position says what this process can
 * still claim to have received; the board's applied position says what the board's state already
 * reflects. The difference between them is a set of frames that are durable and *owed* to the board -
 * and that difference is the one thing that cannot live in memory. A crash takes the knowledge with it,
 * and the frames are then never delivered: the canonical record holds data the board silently never saw,
 * which is precisely the failure this ledger exists to prevent.
 *
 * So a frame is written down here twice, in a sense, because the raw write is not part of this store's
 * transactions:
 *
 *  - an *intent* goes in before the raw write, so the worst a crash can leave behind is an entry for a
 *    frame the raw may not hold - which is recoverable, because the raw write can be decided again from
 *    the frame the entry carries;
 *  - it is *confirmed* in the same transaction that claims the frame durable, so an entry the raw really
 *    does hold and the claim to have received it cannot exist without each other.
 *
 * Only a confirmed entry is delivered to the board: an intent is a frame the raw may not have, and
 * delivering it would put the board ahead of the canonical record. An intent that is still an intent
 * after a restart is therefore offered back through the organizer, which decides the raw write again -
 * the one direction in which a crash is safe.
 *
 * The frame's own bytes travel with the entry instead of a pointer into the raw, because the raw writer
 * is a caller's hook with no read side: the ledger has to be able to hand the frame back on its own.
 * What it holds is one crash window wide - the frames between the two positions - rather than a second
 * copy of the stream.
 *
 * The order entries come back in is the order they were written down, taken from the store's own row order and
 * never from a timestamp: the wall clock can step backwards, and a recovery that read the entries in clock order
 * would deliver the frame written down second before the one written down first - which is how a start heard out
 * of order gets adopted and the frames below it are refused for ever.
 *
 * Release is by position, bounded at both ends: from the connection's first sequence - everything below
 * it can never be applied, so it is a permanent loss and stays here as the record of one - up to the
 * board's applied ceiling. That ceiling only moves contiguously (§2.2), so releasing up to it cannot
 * release a frame above a hole. Entries belonging to another connection are left alone: nothing here
 * decides that a permanent loss is finished with. What is written down, once and only where the decision
 * is made, is that a frame will never be applied: the entry keeps its row and changes its state, so a
 * restart repeats neither the decision nor the report.
 */

import { bindConstructor, bindInternals, internalsOf } from '../internal/wiring.mjs';

const LEDGER_SCHEMA = `
CREATE TABLE IF NOT EXISTS delivery_ledger (
  market TEXT NOT NULL,
  stream TEXT NOT NULL,
  connection_id TEXT NOT NULL,
  receive_seq INTEGER NOT NULL,
  run_id TEXT,
  venue TEXT,
  generation INTEGER,
  recv_ts_ms INTEGER NOT NULL,
  recv_mono_ns INTEGER NOT NULL,
  raw BLOB NOT NULL,
  meta TEXT,
  reason TEXT NOT NULL,
  state TEXT NOT NULL,
  recorded_at_ms INTEGER NOT NULL,
  PRIMARY KEY (market, stream, connection_id, receive_seq)
);
CREATE INDEX IF NOT EXISTS delivery_ledger_arrival
  ON delivery_ledger (market, stream, recorded_at_ms, receive_seq);
`;

export const INTENT = 'intent';
export const OWED = 'owed';
export const SKIPPED = 'skipped';

/**
 * Open the delivery ledger. Opening it writes to the store, so the whole initialisation is one change operation: the
 * execution right is taken here, which makes a re-entrant call from a hook the initialisation calls (the
 * caller's clock, a writer, a migration's reader) an ordinary refusal instead of a nested `BEGIN`.
 *
 * The right, the transaction discipline and the observation of the right come from the wiring, never from
 * the object handed in: a module that used a method on that object could be made to hand its unguarded
 * routes to whoever replaced the method. Unbound means unopenable, before anything is written.
 */
export function openDeliveryLedger(options) {
  const wiring = internalsOf(options.durability);
  return wiring.whileChange(() => openDeliveryLedgerWithin(options, wiring));
}

bindConstructor('ledger', openDeliveryLedgerWithin);

function openDeliveryLedgerWithin(options, wiring) {
  const {
    durability, market, stream, nowMs = () => Date.now()
  } = options;
  if (!market || !stream) throw new TypeError('the delivery ledger needs a market and a stream');


  wiring.db.exec(LEDGER_SCHEMA);

  const recordStatement = wiring.db.prepare(
    `INSERT OR IGNORE INTO delivery_ledger
       (market, stream, connection_id, receive_seq, run_id, venue, generation,
        recv_ts_ms, recv_mono_ns, raw, meta, reason, state, recorded_at_ms)
     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
  );
  // A state change, never a re-insert: an entry's place in the queue is where its frame arrived, and a
  // frame confirmed late must not be delivered last because of it.
  const confirmStatement = wiring.db.prepare(
    'UPDATE delivery_ledger SET state = ? WHERE market = ? AND stream = ? AND connection_id = ? AND receive_seq = ?',
  );
  // A decision, never a removal: a frame that will never be applied stays in the ledger as the record of
  // that loss, and the first decision keeps the row - a state that is already `skipped` changes nothing.
  const skipStatement = wiring.db.prepare(
    `UPDATE delivery_ledger SET state = 'skipped', reason = ?
      WHERE market = ? AND stream = ? AND connection_id = ? AND receive_seq = ? AND state <> 'skipped'`,
  );
  const releaseStatement = wiring.db.prepare(
    `DELETE FROM delivery_ledger
      WHERE market = ? AND stream = ? AND connection_id = ? AND receive_seq <= ? AND receive_seq >= ?`,
  );
  const dropStatement = wiring.db.prepare(
    'DELETE FROM delivery_ledger WHERE market = ? AND stream = ? AND connection_id = ? AND receive_seq = ?',
  );
  const pendingStatement = wiring.db.prepare(
    `SELECT connection_id, receive_seq, run_id, venue, generation, recv_ts_ms, recv_mono_ns, raw, meta,
            reason, state
       FROM delivery_ledger
      WHERE market = ? AND stream = ?
      ORDER BY rowid`,
  );
  const byStateStatement = wiring.db.prepare(
    `SELECT connection_id, receive_seq, run_id, venue, generation, recv_ts_ms, recv_mono_ns, raw, meta,
            reason, state
       FROM delivery_ledger
      WHERE market = ? AND stream = ? AND state = ?
      ORDER BY rowid`,
  );
  const oneStatement = wiring.db.prepare(
    `SELECT connection_id, receive_seq, run_id, venue, generation, recv_ts_ms, recv_mono_ns, raw, meta,
            reason, state
       FROM delivery_ledger
      WHERE market = ? AND stream = ? AND connection_id = ? AND receive_seq = ?`,
  );
  const countStatement = wiring.db.prepare(
    'SELECT COUNT(*) AS owed FROM delivery_ledger WHERE market = ? AND stream = ?',
  );

  function rowToEntry(row) {
    return {
      connectionId: row.connection_id,
      receiveSeq: row.receive_seq,
      runId: row.run_id,
      venue: row.venue,
      generation: row.generation,
      recvTsMs: row.recv_ts_ms,
      recvMonoNs: row.recv_mono_ns,
      raw: Buffer.from(row.raw),
      meta: row.meta === null ? undefined : JSON.parse(row.meta),
      reason: row.reason,
      state: row.state,
    };
  }

  /**
   * Write down that this frame is about to be made durable, or that it already is.
   *
   * The first reason and the first place in the queue are the ones kept: a frame that is offered again
   * repeats the same fact, and rewriting the record would turn a state into a log of attempts. Returns
   * whether this was the frame's first entry, so that a report about it is made once rather than once
   * per attempt.
   */
  function record(envelope, reason, state = OWED) {
    const result = recordStatement.run(
      market,
      stream,
      envelope.connection_id,
      envelope.receive_seq,
      envelope.run_id ?? null,
      envelope.venue ?? null,
      envelope.generation ?? null,
      envelope.recv_ts_ms,
      envelope.recv_mono_ns,
      envelope.raw,
      envelope.meta ? JSON.stringify(envelope.meta) : null,
      reason,
      state,
      nowMs(),
    );
    // A frame that is already written down can still be confirmed: the state is a fact about the raw, and
    // an entry that was an intent when it was first written down is the same frame.
    if (state === OWED) confirm(envelope);
    return { recorded: result.changes === 1 };
  }

  /** The raw holds this frame now. Called inside the transaction that claims it durable. */
  function confirm(envelope) {
    const result = confirmStatement.run(OWED, market, stream, envelope.connection_id, envelope.receive_seq);
    return { confirmed: result.changes };
  }

  /**
   * The board's applied position has passed these: they are delivered and their entries go. The range
   * starts at the connection's first sequence: everything below it can never be applied, so a permanent
   * loss is recorded here rather than released as though it had been delivered.
   */
  function release({ connectionId, firstSeq, upToSeq }) {
    if (connectionId === null || upToSeq === null || !Number.isInteger(firstSeq)) return { released: 0 };
    const result = releaseStatement.run(market, stream, connectionId, upToSeq, firstSeq);
    return { released: result.changes };
  }

  /** One frame's entry, if it has one: what a resend has to know before it decides anything. */
  function find(connectionId, receiveSeq) {
    const row = oneStatement.get(market, stream, connectionId, receiveSeq);
    return row === undefined ? null : rowToEntry(row);
  }

  /** One entry, by hand: what a caller removed after deciding it will never be delivered. */
  function drop(connectionId, receiveSeq) {
    const result = dropStatement.run(market, stream, connectionId, receiveSeq);
    return { dropped: result.changes };
  }

  /**
   * Write down that this frame will never be applied. The first decision keeps the row: the entry is the
   * record of a permanent loss, and a frame whose loss is already written down is the same fact offered
   * again - a later reason does not rewrite it. Returns whether this call was the decision, so that the
   * report about it is made once rather than once per attempt.
   */
  function skip(connectionId, receiveSeq, reason) {
    const result = skipStatement.run(reason, market, stream, connectionId, receiveSeq);
    return { decided: result.changes === 1 };
  }

  /** Everything still written down, in arrival order - the order the board is fed in. */
  function pending({ state = null } = {}) {
    const rows =
      state === null
        ? pendingStatement.all(market, stream)
        : byStateStatement.all(market, stream, state);
    return rows.map(rowToEntry);
  }

  const api = {
    market,
    stream,
    record,
    confirm,
    release,
    drop,
    skip,
    find,
    pending,
    size() {
      return countStatement.get(market, stream).owed;
    },
  };

  // The unguarded routes. A caller that already holds the store's execution right - the module
  // that *is* the operation in progress - goes through these; every other caller takes the public
  // name above, which refuses a call that arrives while another change operation is running.
  const internal = {
    record: api.record,
    confirm: api.confirm,
    release: api.release,
    drop: api.drop,
    skip: api.skip,
  };
  api.record = wiring.guard('ledger.record', internal.record);
  api.confirm = wiring.guard('ledger.confirm', internal.confirm);
  api.release = wiring.guard('ledger.release', internal.release);
  api.drop = wiring.guard('ledger.drop', internal.drop);
  api.skip = wiring.guard('ledger.skip', internal.skip);

  bindInternals(api, internal);
  return api;
}
