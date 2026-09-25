/**
 * Organization: decide what is durable, what is missing, and what may be acknowledged.
 *
 * This is where the awkward questions are settled, so that nothing else has to ask them. Reception
 * writes down what arrived, in arrival order; this module owns the ordering judgement - which is
 * exactly the point of the split, because the received order is a fact while the correct order is a
 * decision, and only one place should be making it.
 *
 * Three things happen for every envelope, in this order and no other:
 *   1. the raw is written down (the hook the caller passes), because the raw is the canonical record
 *      and nothing may be acknowledged before it is safe;
 *   2. the contiguous ceiling moves if this sequence is exactly the next one, and only then;
 *   3. an acknowledgement may be emitted, and it may only ever carry that ceiling.
 *
 * A sequence that arrives above the ceiling while something is missing is not an error and is not
 * discarded: it is remembered, and the hole is written down as a suspected gap, because a gap that
 * is not recorded is indistinguishable later from data that never existed. When the hole is filled,
 * everything waiting behind it is released at once.
 *
 * The watermark is persisted per connection, but it only moves after the raw is durable: a restart
 * resumes from what was written down, never from what was merely received.
 */

const ORGANIZE_SCHEMA = `
CREATE TABLE IF NOT EXISTS organized_watermark (
  connection_id TEXT NOT NULL PRIMARY KEY,
  market TEXT NOT NULL,
  up_to_receive_seq INTEGER,
  updated_at_ms INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS organize_gap (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  connection_id TEXT NOT NULL,
  market TEXT NOT NULL,
  missing_from INTEGER NOT NULL,
  missing_to INTEGER NOT NULL,
  detected_at_ms INTEGER NOT NULL,
  filled_at_ms INTEGER,
  reason TEXT NOT NULL
);
`;

export function openOrganizer({
  market,
  stream,
  durability,
  writeRaw,
  firstSeq = 1,
  nowMs = () => Date.now(),
  capacity = () => 'ok',
}) {
  if (!market || !stream) throw new TypeError('an organizer needs a market and a stream');
  if (!durability?.db) throw new TypeError('an organizer needs the durability store');
  if (typeof writeRaw !== 'function') throw new TypeError('an organizer needs a way to write raw data');
  // writeRaw must be durable before it returns, and must say so: true means the canonical record is
  // safe. Anything else - false, a Promise, a count, silence - counts as "not durable yet", and
  // nothing is acknowledged or advanced on the strength of it.

  durability.db.exec(ORGANIZE_SCHEMA);

  let connectionId = null;
  let upToSeq = null; // null means: nothing durable yet, which is not the same as 0
  let baselineSeq = firstSeq; // the first sequence this connection will ever have
  let outOfOrder = [];

  function persist() {
    if (connectionId === null) return;
    durability.db
      .prepare(
        `INSERT OR REPLACE INTO organized_watermark
           (connection_id, market, up_to_receive_seq, updated_at_ms)
         VALUES (?, ?, ?, ?)`,
      )
      .run(connectionId, market, upToSeq, nowMs());
  }

  function recordGap(from, to) {
    durability.db
      .prepare(
        `INSERT INTO organize_gap (connection_id, market, missing_from, missing_to, detected_at_ms, reason)
         VALUES (?, ?, ?, ?, ?, ?)`,
      )
      .run(connectionId, market, from, to, nowMs(), 'sequence gap seen in the received order');
  }

  /**
   * Close every hole the ceiling has now passed. A recorded range is a fact about what was missing;
   * it stops being open when the durable position is at or past its end, whatever filled it.
   */
  function closeGapsUpTo(upTo) {
    durability.db
      .prepare(
        `UPDATE organize_gap SET filled_at_ms = ?
         WHERE connection_id = ? AND filled_at_ms IS NULL AND missing_to <= ?`,
      )
      .run(nowMs(), connectionId, upTo);
  }

  return {
    market,
    stream,

    /** Adopt a connection and resume its watermark if this process has organized it before. */
    accept(connectionIdNext, { firstSeq: first = firstSeq } = {}) {
      connectionId = connectionIdNext;
      const row = durability.db
        .prepare('SELECT up_to_receive_seq FROM organized_watermark WHERE connection_id = ?')
        .get(connectionId);
      if (row && row.up_to_receive_seq !== null) {
        upToSeq = row.up_to_receive_seq;
        baselineSeq = 1; // an existing watermark means the baseline is already established
      } else {
        upToSeq = null;
        baselineSeq = first;
      }
      outOfOrder = [];
      return { connectionId, upToSeq, firstSeq };
    },

    get ackState() {
      return { connectionId, upToSeq, capacity: capacity() };
    },

    /**
     * Take one envelope, in the order it was received.
     *
     * Returns the acknowledgement to send, which is null while the raw is not yet durable. It is the
     * caller's writeRaw that decides that: this method only moves the ceiling after the hook returns.
     */
    note(envelope) {
      if (connectionId === null) this.accept(envelope.connection_id);
      if (envelope.connection_id !== connectionId) {
        return { accepted: false, reason: 'not the accepted connection', ack: null };
      }
      const seq = envelope.receive_seq;

      // Already durable: a resend after a reconnect. Nothing to write, nothing to acknowledge anew.
      if (upToSeq !== null && seq <= upToSeq) {
        // "Already durable" is not "already applied". A crash between the raw write and the board
        // leaves exactly this frame, and a resend is the only way it comes back - so the caller is told
        // the raw is safe and the frame may still need routing onward.
        return { accepted: true, duplicate: true, alreadyDurable: true, reason: 'already durable', ack: null };
      }

      const durable = writeRaw(envelope) === true;
      if (!durable) {
        // The raw is not safe, so the watermark does not move and no acknowledgement is emitted.
        return { accepted: true, durable: false, reason: 'raw not durable yet', ack: null };
      }

      if (upToSeq === null) {
        if (seq < baselineSeq) {
          return { accepted: true, duplicate: true, alreadyDurable: true, reason: 'below this connection\'s first sequence', ack: null };
        }
        if (seq > baselineSeq) {
          // The connection's first sequence never arrived: that hole is a fact worth keeping.
          recordGap(baselineSeq, seq - 1);
          if (!outOfOrder.includes(seq)) outOfOrder = [...outOfOrder, seq].sort((a, b) => a - b);
          persist();
          return { accepted: true, reason: 'waiting for the first sequence', ack: null };
        }
        upToSeq = seq;
      } else if (seq === upToSeq + 1) {
        upToSeq = seq;
        let next = upToSeq;
        let rest = outOfOrder;
        while (rest.length > 0 && rest[0] === next + 1) {
          next = rest[0];
          rest = rest.slice(1);
        }
        upToSeq = next;
        outOfOrder = rest;
        closeGapsUpTo(upToSeq);
      } else {
        // Above the ceiling with something missing in between: remember it, and write the hole down.
        if (!outOfOrder.includes(seq)) outOfOrder = [...outOfOrder, seq].sort((a, b) => a - b);
        recordGap(upToSeq + 1, seq - 1);
      }

      persist();
      return { accepted: true, durable: true, reason: 'durable', ack: { connectionId, upToSeq, capacity: capacity() } };
    },

    /** Holes seen and not yet filled: the ranges this process cannot claim to have. */
    openGaps() {
      return durability.db
        .prepare(
          `SELECT missing_from, missing_to, detected_at_ms, reason FROM organize_gap
           WHERE connection_id = ? AND filled_at_ms IS NULL ORDER BY missing_from`,
        )
        .all(connectionId)
        .map((row) => ({
          missingFrom: row.missing_from,
          missingTo: row.missing_to,
          detectedAtMs: row.detected_at_ms,
          reason: row.reason,
        }));
    },
  };
}
