/**
 * The structure: reception, organization and book update assembled into one thing that runs.
 *
 * Each role is its own module with its own contract, and this is the wiring that feeds them in the
 * only order that is safe:
 *
 *   reception stamps a frame  ->  organization makes it durable and says what may be acknowledged
 *      ->  the book applies it and keeps its position, or refuses it and waits for the hole
 *
 * Two things this wiring is responsible for, and they are the reason it is a module rather than a
 * few lines in an entry point:
 *
 *  - the ladder. When the queue in front of organization is full, the frame goes to the spool rather
 *    than being dropped, and if the spool is full too, reception stops and the gap is recorded. No
 *    step of that decision belongs to a buffer.
 *  - keeping the roles from lying to each other. The book's refusal to apply out of order ("waiting
 *    for a sequence") is not swallowed: the frame is handed back to the book later, in order, by the
 *    book itself, and the acknowledgement the book's caller sees is the organizer's, not a guess.
 *
 * The process split is a deployment matter: the same contracts run over sockets between three
 * processes or, as here, in one for a test or a dry run against a replaying adapter.
 */

import { openBook } from '../book/state.mjs';
import { openOrganizer } from '../organize/watermark.mjs';
import { createSpool } from '../spool.mjs';
import { createReceiveConnection } from '../ingest/connection.mjs';

export function createStructure({
  market,
  stream = 'trades',
  adapter,
  durability,
  webSocketImpl,
  rawWriter,
  spoolDir = null,
  maxQueuedFrames = 5_000,
  onAck = () => {},
  onGap = () => {},
  onStop = () => {},
  onDiagnostic = () => {},
  ...receiveOptions
}) {
  if (!durability?.db) throw new TypeError('the structure needs the durability store');
  if (typeof rawWriter !== 'function') throw new TypeError('the structure needs a raw writer');

  const book = openBook({ market, stream, durability });
  const spool = spoolDir ? createSpool({ dir: spoolDir }) : null;

  // Backpressure is decided here rather than inside a buffer. In one process the roles are called
  // synchronously, so the pressure shows up as the raw writer refusing: the frame then goes to the
  // spool, and only if the spool cannot hold it either does reception stop. (Across processes the
  // same decision is made when the channel reports a full queue.)
  let stopped = false;
  let spooledFrames = 0;
  let refusedFrames = 0;

  const organizer = openOrganizer({
    market,
    stream,
    durability,
    writeRaw: (envelope) => {
      const written = rawWriter(envelope);
      return written === true; // only a durable write may be acknowledged
    },
    capacity: () => (stopped ? 'stopped' : 'ok'),
  });

  // Apply-refusals that are the book holding a frame on purpose rather than losing it. Matched by
  // shape rather than by one exact sentence: the previous version listed a phrase the book never used,
  // so a frame the book was deliberately holding was reported as a loss.
  const HELD_BY_DESIGN = /already applied|gap before this sequence|first sequence/i;

  let refusedByBook = 0;
  // Frames that are durable in the raw and not on the board, keyed by connection and sequence. This is
  // the difference between the two positions, held so it can be repaired instead of merely reported.
  const unapplied = new Map();
  // Frames whose connection is gone: durable in the raw, never going to be applied, kept as a record.
  const skipped = [];

  let structure_redeliver = () => {};

  function feed(envelope) {
    if (stopped) {
      // Reception is stopped, so anything still arriving is recorded as a hole rather than lost
      // silently or applied out of order.
      refusedFrames += 1;
      onGap({ market, reason: 'reception stopped: nothing more can be held', seq: envelope.receive_seq });
      return { accepted: false, reason: 'stopped' };
    }
    try {
      const note = organizer.note(envelope);
      if (note.ack) onAck(note.ack);
      if (note.accepted === false) return note;

      // C8: raw durability and board application are separate questions with separate positions. A
      // frame that is already durable may still be unapplied, so a resend is routed to the book rather
      // than dropped here; the book's own (connection, sequence) dedupe makes a second application a
      // no-op, which is what keeps this from becoming a double write.
      if (note.durable || note.alreadyDurable) {
        const applied = book.apply({
          envelope: { ...envelope, generation: envelope.generation },
          changes: adapter.changesFor ? adapter.changesFor(envelope) : [],
        });
        // Two refusals are the book working as designed: a frame it already holds, and a frame it is
        // holding until the hole before it is filled. Those are states, not losses.
        //
        // Any other refusal is a frame that is durable in the raw, acknowledged as durable, and not on
        // the board - and if that is not written down, the raw position and the applied position drift
        // apart with nothing to say so. C8 keeps those two positions separate precisely so that the
        // difference can be seen, so it is recorded here rather than returned to a caller that may not
        // look.
        if (applied.applied === false && applied.reason && !HELD_BY_DESIGN.test(applied.reason)) {
          // The frame is durable and unapplied, so it is kept rather than handed back for someone else
          // to remember. It is also reported once per frame: a resend repeats the same refusal, and
          // repeating the report turns one lost frame into a stream of noise.
          const key = `${envelope.connection_id}:${envelope.receive_seq}`;
          if (!unapplied.has(key)) {
            unapplied.set(key, { envelope, reason: applied.reason, note });
            refusedByBook += 1;
            onGap({
              market,
              reason: `the board refused the frame: ${applied.reason}`,
              seq: envelope.receive_seq,
            });
          }
        }
        return { ...note, ...applied };
      }

      // The raw writer refused: the frame is spilled rather than dropped.
      if (spool && spool.append(envelope) && !spool.failed) {
        spooledFrames += 1;
        return { ...note, spooled: true };
      }
      // Nothing could hold it. Reception stops and the gap is written down, which is the only honest
      // outcome left: continuing would mean pretending the frame was handled.
      stopped = true;
      onGap({ market, reason: 'raw refused and the spool could not hold it', seq: envelope.receive_seq });
      onStop({ market, reason: 'nothing could hold the frame' });
      return { ...note, stopped: true };
    } catch (error) {
      // An exception anywhere in the path is not swallowed: reception stops and the caller hears
      // about it, rather than the structure carrying on with a frame whose fate is unknown.
      stopped = true;
      onGap({ market, reason: `failure while handling a frame: ${error.message}`, seq: envelope.receive_seq });
      onStop({ market, reason: error.message });
      return { accepted: false, reason: 'failure', error };
    }
  }

  const connection = createReceiveConnection({
    adapter,
    market,
    webSocketImpl,
    onEnvelope: feed,
    // The book is told which connection it is about to receive, before any of its frames arrive. A
    // generation change is announced here, and this is the only place that announces it: the book
    // refuses frames from a connection that was never accepted.
    onGeneration: ({ connectionId, generation, firstSeq }) => {
      const accepted = book.accept(connectionId, { generation, firstSeq: firstSeq ?? null });
      if (!accepted.accepted) {
        onDiagnostic({ market, reason: `the book did not accept this connection: ${accepted.reason}` });
      }
    },
    ...receiveOptions,
  });

  const api = {
    market,
    /** Accept a connection explicitly. Frames from any other connection are refused by the book. */
    accept: (connectionId, options = {}) => {
      const accepted = book.accept(connectionId, options);
      // The held frames were refused because the book did not know this connection. Now that it does,
      // they are offered again without anyone having to remember to ask - a repair that depends on
      // somebody calling it is a repair that does not happen.
      if (accepted && accepted.accepted && unapplied.size > 0) {
        structure_redeliver(connectionId);
      }
      return accepted;
    },
    /**
     * Offer the held frames to the book again, oldest first. What was refused because the book did not
     * know the connection can be applied once it does; a frame the book still refuses stays held, so a
     * failed attempt costs nothing and the difference between the raw and the board remains visible.
     */
    redeliverPending: () => {
      // Three answers, kept apart on purpose: what reached the board, what the board is holding until
      // a hole is filled, and what will never apply because the connection it belongs to is gone. The
      // previous version called the second one "redelivered" and threw the third one away, which is
      // how a frame that never arrived gets counted as one that did.
      let appliedCount = 0;
      let heldCount = 0;
      let skippedCount = 0;
      const remaining = [];
      const gone = [];
      for (const [key, entry] of unapplied) {
        const currentConnection = book.appliedBoundary.connectionId;
        if (entry.envelope.connection_id !== currentConnection) {
          // Its connection was replaced, so nothing will ever accept it. That is a permanent loss and
          // it is recorded as one instead of being held for a delivery that cannot happen.
          gone.push([key, entry]);
          continue;
        }
        const result = book.apply({
          envelope: entry.envelope,
          changes: adapter.changesFor ? adapter.changesFor(entry.envelope) : [],
        });
        if (result.applied === true) {
          appliedCount += 1;
        } else if (result.reason && HELD_BY_DESIGN.test(result.reason)) {
          heldCount += 1;
          remaining.push([key, entry]);
        } else {
          const key2 = key;
          remaining.push([key2, entry]);
        }
      }
      unapplied.clear();
      for (const [key, entry] of remaining) unapplied.set(key, entry);
      for (const [key, entry] of gone) {
        skipped.push({ connectionId: entry.envelope.connection_id, seq: entry.envelope.receive_seq, reason: entry.reason });
      }
      if (gone.length > 0) {
        for (const entry of skipped.slice(-gone.length)) {
          onGap({ market, reason: `the connection this frame belonged to is gone: ${entry.reason}`, seq: entry.seq });
        }
      }
      skippedCount = gone.length;
      return { applied: appliedCount, held: heldCount, skipped: skippedCount, stillPending: unapplied.size };
    },
    stream,
    book,
    organizer,
    connection,
    spool,
    start: () => connection.start(),
    stop: () => {
      connection.stop();
      spool?.close();
    },
    /** A frame handed in directly, for a replay adapter or a dry run. */
    feed,
    applyHeld: () => {
      // The book applies what it held as soon as a hole fills; this exposes that for a caller that
      // wants to drive a replay rather than wait for the next arrival.
      return book.openGaps();
    },
    get stats() {
      return {
        market,
        generation: connection.generation,
        state: connection.state,
        subscriptionState: connection.subscriptionState,
        receiveSeq: connection.receiveSeq,
        applied: book.appliedBoundary.upToSeq,
        gaps: book.openGaps().length,
        spooledFrames,
        refusedFrames,
        stopped,
      };
    },
  };

  // The public redeliver, wired so accept() can use it without the caller arranging anything.
  structure_redeliver = (connectionId) => api.redeliverPending(connectionId);

  return api;
}
