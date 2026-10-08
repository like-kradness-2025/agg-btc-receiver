/**
 * The release FIFO: how far an acknowledgement may move the spool's cursor when the frames waiting
 * in the spool do not all belong to one connection.
 *
 * The spool retains a frame until the durable acknowledgement that covers it, and only a
 * contiguous position may be confirmed to it - moving the cursor over a record the consumer has not
 * consumed would delete that record. With a hand-over the spool holds frames of two connections at
 * once, and an acknowledgement names only its own; a plain walk over the spool cannot decide which
 * positions are contiguous, because a record of one connection may sit between two of another's.
 *
 * This module answers one question: given the acknowledgements so far, how far may the cursor move?
 * The rule is physical order - a record is only passed when that record itself is covered. Every
 * unacknowledged record's position is kept in the order it was written, and each acknowledgement
 * raises its connection's ceiling; the FIFO releases from the head, and only while the head is
 * covered by its own ceiling. A record behind an uncovered head is never released, so no
 * acknowledgement can carry the cursor across a record the spool has not actually confirmed.
 */

/** How many released head entries may accumulate before the backing array is compacted. */
const COMPACT_AT = 1024;

/**
 * The identity a delivery is acknowledged under: the connection it belongs to (its run, market and
 * generation are in its name), plus the stream. `durable_ack` and the frames it covers both carry
 * these fields, so the two sides derive the same string.
 */
export function ackIdentityOf(frame) {
  return [
    frame?.market ?? null,
    frame?.stream ?? null,
    frame?.run_id ?? null,
    frame?.connection_id ?? null,
    frame?.generation ?? null,
  ].join('\u0000');
}

/**
 * Build an empty FIFO. `record` is called once per written frame, in write order; `noteAck` once
 * per durable acknowledgement. `noteAck` returns the number of entries it released and the position
 * to hand to `spool.advance` - the end of the last released record, or null when the head is still
 * uncovered. Its ceilings are monotonic: a repeated or stale acknowledgement cannot lower one, so
 * it can never release less than an earlier acknowledgement already justified.
 *
 * A ceiling is kept only while its identity has records waiting, and retired with the last one: a
 * record is registered before the frame is sent, an acknowledgement covers only what was sent, so
 * an acknowledgement cannot arrive before the record that will need it - and a long run of
 * reconnects and hand-overs does not accumulate one identity per connection. An acknowledgement
 * that names an identity with nothing waiting is a no-op, not a reason to keep state.
 */
export function createAckFifo() {
  let entries = []; // unacknowledged records, in physical order; `head` is the first unreleased
  let head = 0;
  const ceilingBy = new Map(); // identity -> the highest acknowledged sequence
  const heldBy = new Map(); // identity -> how many of its records are still unreleased

  function record({ identity, seq, position }) {
    entries.push({ identity, seq, position });
    heldBy.set(identity, (heldBy.get(identity) ?? 0) + 1);
  }

  function noteAck({ identity, upToSeq }) {
    if (!Number.isInteger(upToSeq)) return { released: 0, position: null };
    if (!heldBy.has(identity)) return { released: 0, position: null };
    const previous = ceilingBy.get(identity);
    if (previous === undefined || upToSeq > previous) ceilingBy.set(identity, upToSeq);
    let released = 0;
    let position = null;
    while (head < entries.length) {
      const entry = entries[head];
      const ceiling = ceilingBy.get(entry.identity);
      if (ceiling === undefined || entry.seq > ceiling) break;
      const held = heldBy.get(entry.identity) - 1;
      if (held === 0) {
        heldBy.delete(entry.identity);
        ceilingBy.delete(entry.identity);
      } else {
        heldBy.set(entry.identity, held);
      }
      position = entry.position;
      head += 1;
      released += 1;
    }
    if (head === entries.length) {
      entries = [];
      head = 0;
    } else if (head >= COMPACT_AT) {
      entries = entries.slice(head);
      head = 0;
    }
    return { released, position };
  }

  return {
    record,
    noteAck,
    /** How many records are written and not yet released. */
    get size() {
      return entries.length - head;
    },
    /** How many identities have a ceiling - only those with records waiting. */
    get identities() {
      return ceilingBy.size;
    },
    /** The next record the cursor is waiting on, or null when nothing is held. */
    get head() {
      return head < entries.length ? { ...entries[head] } : null;
    },
  };
}

/**
 * Rebuild the FIFO from a walk of the spool (the order `drainRecords` hands records out is the
 * physical order `record` expects). This is what a restart does once: the positions are not
 * persisted anywhere else, and the walk reconstructs them from the cursor onwards.
 */
export function rebuildAckFifo(records) {
  const fifo = createAckFifo();
  for (const record of records) {
    fifo.record({
      identity: ackIdentityOf(record.envelope),
      seq: record.envelope.receive_seq,
      position: { segment: record.segment, offset: record.offset },
    });
  }
  return fifo;
}
