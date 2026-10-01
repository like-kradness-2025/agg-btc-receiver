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
 * A connection is taken over explicitly and by nobody's guess: a frame from a connection this process
 * has not been told to organize is refused rather than written down (C2, fail-closed). "We have not
 * accepted anything yet" is not "this is the one".
 *
 * A sequence that arrives above the ceiling while something is missing is not an error and is not
 * discarded: it is remembered, and the hole is written down as a suspected gap, because a gap that
 * is not recorded is indistinguishable later from data that never existed. When the hole is filled,
 * everything waiting behind it is released at once.
 *
 * The start of a connection's numbering is the one thing that may still arrive after its frames do.
 * Until it does, the raw is safe but the ceiling does not move and nothing is acknowledged - the frames
 * are held, and released only as far as they are contiguous once the start is known. A start that has
 * been established is never renegotiated.
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
  // The ceiling and the holes it implies are written together, so this module needs the store's
  // transaction discipline rather than a second copy of it.
  if (typeof durability.inTransaction !== 'function') {
    throw new TypeError('an organizer needs the durability store and its transactions');
  }
  if (typeof writeRaw !== 'function') throw new TypeError('an organizer needs a way to write raw data');
  // writeRaw must be durable before it returns, and must say so: true means the canonical record is
  // safe. Anything else - false, a Promise, a count, silence - counts as "not durable yet", and
  // nothing is acknowledged or advanced on the strength of it.

  durability.db.exec(ORGANIZE_SCHEMA);

  let connectionId = null;
  let upToSeq = null; // null means: nothing durable yet, which is not the same as 0
  // null means the start of this connection's numbering has not been established. The default a caller
  // configured is a declaration, not a discovery - and an unknown start is not a licence to guess.
  let baselineSeq = Number.isInteger(firstSeq) ? firstSeq : null;
  let outOfOrder = [];
  // Sequences made durable while the start was still unknown. Held rather than counted, because the
  // moment the start arrives they can be released - but only as far as they are contiguous, never past
  // a hole, which is the whole reason the ceiling is a ceiling and not a high-water mark.
  const durableAboveBaseline = new Set();
  // The lowest sequence made durable while the start was unknown. The start can only be at or below it, so a
  // completion above it would skip frames this process already holds - and, having moved the ceiling past
  // them, it would never acknowledge them and never ask for them again (§2.2).
  let lowestHeldWithoutBaseline = null;
  // The identity a frame has to carry to belong to this connection: the board's own idea of its owner and
  // generation, not a second opinion invented here. Checked before the duplicate test and before the raw,
  // because the canonical record must not be written for a frame nobody accepted.
  let identityRunId = null;
  let identityGeneration = null;

  function persist(ceiling) {
    if (connectionId === null) return;
    durability.db
      .prepare(
        `INSERT OR REPLACE INTO organized_watermark
           (connection_id, market, up_to_receive_seq, updated_at_ms)
         VALUES (?, ?, ?, ?)`,
      )
      .run(connectionId, market, ceiling, nowMs());
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

  /**
   * The state this organizer would be in once the start of its connection's numbering is known, worked
   * out without touching anything.
   *
   * It is planned rather than applied because the completion is a write: memory may only follow a commit,
   * and a failed write has to leave the completion still outstanding so that asking again can finish it.
   * The ceiling moves only as far as the frames are contiguous - a hole stops it exactly where it is - and
   * what is left above it waits exactly like a frame that arrived above the ceiling on a connection whose
   * start was known all along, with the hole it waits for written down, because a gap only this process
   * knows about disappears with the process.
   */
  function planFromOrigin(known) {
    const held = new Set(durableAboveBaseline);
    let ceiling = upToSeq;
    let moved = false;
    if (ceiling === null && held.has(known)) {
      ceiling = known;
      held.delete(known);
      moved = true;
    }
    while (ceiling !== null && held.has(ceiling + 1)) {
      ceiling += 1;
      held.delete(ceiling);
      moved = true;
    }
    const carried = [...held].sort((a, b) => a - b);
    const gaps = [];
    if (carried.length > 0) {
      const from = ceiling === null ? known : ceiling + 1;
      if (carried[0] > from) gaps.push({ from, to: carried[0] - 1 });
    }
    return { ceiling, moved, carried, gaps };
  }

  /** Where the ceiling lands when a sequence becomes durable, and what is left waiting above it. */
  function ceilingAfter(from, waiting) {
    let ceiling = from;
    let rest = waiting;
    while (rest.length > 0 && rest[0] === ceiling + 1) {
      ceiling = rest[0];
      rest = rest.slice(1);
    }
    return { ceiling, outOfOrder: rest };
  }

  /**
   * Everything taking one envelope would change, worked out without touching anything: the ceiling, what
   * waits above it, the holes it opens and the ceiling it closes up to.
   *
   * It is planned rather than applied because the write and the memory have to agree: the ceiling, the
   * holes it opens and the holes it closes go into one transaction, and a frame the store could not
   * describe must leave this process exactly as it was rather than leave a position that only exists in
   * memory. A frame can also be durable without moving the ceiling - that is a state, not a claim, and it
   * is what tells the caller the raw is safe while the board still has nowhere to put the frame.
   */
  function planNote(seq) {
    const held = new Set(durableAboveBaseline);
    const gaps = [];
    const closing = null;

    if (baselineSeq === null) {
      // The start of this connection's numbering is still unknown: the raw can be safe, but no ceiling may
      // be claimed for it. The frame waits where it can be released once the start arrives.
      held.add(seq);
      return {
        write: true,
        ceiling: upToSeq,
        outOfOrder,
        held,
        lowestHeld: lowestHeldWithoutBaseline === null ? seq : Math.min(lowestHeldWithoutBaseline, seq),
        gaps,
        closing,
        result: { accepted: true, durable: true, reason: "the connection's start is not known yet", ack: null },
      };
    }

    if (upToSeq === null) {
      if (seq < baselineSeq) {
        return {
          write: false,
          result: {
            accepted: true,
            duplicate: true,
            alreadyDurable: true,
            reason: "below this connection's first sequence",
            ack: null,
          },
        };
      }
      if (seq > baselineSeq) {
        // The connection's first sequence never arrived: that hole is a fact worth keeping, and this frame
        // is durable while it waits - which is what tells the caller it may still be routed onward.
        gaps.push({ from: baselineSeq, to: seq - 1 });
        return {
          write: true,
          ceiling: upToSeq,
          outOfOrder: [...new Set([...outOfOrder, seq])].sort((a, b) => a - b),
          held,
          gaps,
          closing,
          result: { accepted: true, durable: true, reason: 'waiting for the first sequence', ack: null },
        };
      }
      const advanced = ceilingAfter(seq, outOfOrder);
      return {
        write: true,
        ceiling: advanced.ceiling,
        outOfOrder: advanced.outOfOrder,
        held,
        gaps,
        closing: advanced.ceiling,
        result: {
          accepted: true,
          durable: true,
          reason: 'durable',
          ack: { connectionId, upToSeq: advanced.ceiling, capacity: capacity() },
        },
      };
    }

    if (seq === upToSeq + 1) {
      const advanced = ceilingAfter(seq, outOfOrder);
      return {
        write: true,
        ceiling: advanced.ceiling,
        outOfOrder: advanced.outOfOrder,
        held,
        gaps,
        closing: advanced.ceiling,
        result: {
          accepted: true,
          durable: true,
          reason: 'durable',
          ack: { connectionId, upToSeq: advanced.ceiling, capacity: capacity() },
        },
      };
    }

    // Above the ceiling with something missing in between: remember it, and write the hole down. The frame
    // is durable even though nothing is acknowledged for it, and the acknowledgement never crosses the hole.
    gaps.push({ from: upToSeq + 1, to: seq - 1 });
    return {
      write: true,
      ceiling: upToSeq,
      outOfOrder: [...new Set([...outOfOrder, seq])].sort((a, b) => a - b),
      held,
      gaps,
      closing,
      result: { accepted: true, durable: true, reason: 'durable', ack: { connectionId, upToSeq, capacity: capacity() } },
    };
  }

  return {
    market,
    stream,

    /**
     * Adopt a connection, resume its watermark if this process has organized it before, and complete
     * the start of its numbering if that is what the caller finally knows.
     *
     * Re-accepting the same connection keeps the watermark and the frames waiting behind holes: the
     * only thing still outstanding there is a start nobody had, and it may only be completed, never
     * renegotiated. A different connection starts from its own numbering, so what was waiting for the
     * previous one is not carried over.
     */
    accept(connectionIdNext, { firstSeq: first = firstSeq, runId: nextRunId, generation: nextGeneration } = {}) {
      const known = Number.isInteger(first) ? first : null;
      if (nextRunId !== undefined) identityRunId = nextRunId;
      if (nextGeneration !== undefined) identityGeneration = nextGeneration;
      if (connectionIdNext === connectionId) {
        if (baselineSeq === null && known !== null) {
          // The start cannot be above a sequence already made durable: one of the two is wrong, and moving the
          // ceiling past the frames below it would acknowledge a range this process never received while
          // refusing them for ever as "already durable".
          if (lowestHeldWithoutBaseline !== null && known > lowestHeldWithoutBaseline) {
            return { connectionId, upToSeq, firstSeq: null, advanced: false, refusedOrigin: known };
          }
          // The completion is worked out first and written before anything in memory moves: a failed write
          // must leave the organizer as it was - with the completion still outstanding, so that asking
          // again can finish it - rather than leave a ceiling that is only in this process's head.
          const candidate = planFromOrigin(known);
          durability.inTransaction(() => {
            persist(candidate.ceiling);
            for (const gap of candidate.gaps) recordGap(gap.from, gap.to);
            if (candidate.ceiling !== null) closeGapsUpTo(candidate.ceiling);
          });
          // Only after the commit: the start, the ceiling, and what is still waiting behind a hole.
          baselineSeq = known;
          upToSeq = candidate.ceiling;
          if (candidate.carried.length > 0) {
            outOfOrder = [...new Set([...outOfOrder, ...candidate.carried])].sort((a, b) => a - b);
          }
          durableAboveBaseline.clear();
          lowestHeldWithoutBaseline = null;
          return { connectionId, upToSeq, firstSeq: baselineSeq, advanced: candidate.moved };
        }
        return { connectionId, upToSeq, firstSeq: baselineSeq, advanced: false };
      }
      connectionId = connectionIdNext;
      if (nextRunId === undefined) identityRunId = null;
      if (nextGeneration === undefined) identityGeneration = null;
      const row = durability.db
        .prepare('SELECT up_to_receive_seq FROM organized_watermark WHERE connection_id = ?')
        .get(connectionId);
      if (row && row.up_to_receive_seq !== null) {
        upToSeq = row.up_to_receive_seq;
        baselineSeq = 1; // an existing watermark means the start of this connection is already known
      } else {
        upToSeq = null;
        baselineSeq = known;
      }
      outOfOrder = [];
      durableAboveBaseline.clear();
      lowestHeldWithoutBaseline = null;
      return { connectionId, upToSeq, firstSeq: baselineSeq, advanced: false };
    },

    get ackState() {
      return { connectionId, upToSeq, capacity: capacity() };
    },

    /** The ceiling as it stands, in the shape an acknowledgement is made of. Null before it exists. */
    currentAck() {
      if (connectionId === null || upToSeq === null) return null;
      return { connectionId, upToSeq, capacity: capacity() };
    },

    /**
     * Take one envelope, in the order it was received.
     *
     * Returns the acknowledgement to send, which is null while the raw is not yet durable or while the
     * start of this connection's numbering is unknown. It is the caller's writeRaw that decides the
     * first of those: this method only moves the ceiling after the hook returns.
     *
     * A frame that is not this connection's - another board, another run, another generation - is refused
     * before the duplicate test and before the raw, because the canonical record must not hold a frame
     * nobody accepted, and a "durable" answer for one would leave the board holding nothing while the raw
     * says otherwise.
     */
    note(envelope) {
      if (connectionId === null) {
        // Fail-closed: nothing is accepted by default. A frame that is written down for a connection
        // nobody accepted is a frame that becomes the canonical record on the strength of its own
        // arrival, which is exactly what C2 forbids.
        return { accepted: false, reason: 'no connection has been accepted yet', ack: null };
      }
      if (envelope.connection_id !== connectionId) {
        return { accepted: false, reason: 'not the accepted connection', ack: null };
      }
      if (envelope.market !== market || envelope.stream !== stream) {
        return { accepted: false, reason: 'this frame belongs to another board', ack: null };
      }
      if ((envelope.run_id ?? null) !== identityRunId || (envelope.generation ?? null) !== identityGeneration) {
        return { accepted: false, reason: 'this frame belongs to another run or generation', ack: null };
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

      const plan = planNote(seq);
      if (plan.write === false) return plan.result;

      // The ceiling, the holes it opens and the holes it closes go in one transaction, and memory follows
      // the commit: a frame the store could not describe leaves this process exactly as it was.
      durability.inTransaction(() => {
        persist(plan.ceiling);
        for (const gap of plan.gaps) recordGap(gap.from, gap.to);
        if (plan.closing !== null) closeGapsUpTo(plan.closing);
      });
      upToSeq = plan.ceiling;
      outOfOrder = plan.outOfOrder;
      if (plan.lowestHeld !== undefined) lowestHeldWithoutBaseline = plan.lowestHeld;
      durableAboveBaseline.clear();
      for (const held of plan.held) durableAboveBaseline.add(held);
      return plan.result;
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
