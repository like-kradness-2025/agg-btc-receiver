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
 *
 * The one decision the wiring owns alone is admission: which run may speak for this board. It is
 * issued here, at the moment a new run actually starts receiving, from the board's own recorded owner
 * - never by a caller asking, and never from a bigger number. Until it is issued, reception does not
 * start, because a connection nobody admitted would have every one of its frames refused downstream,
 * after they had been counted as received.
 */

import { openBook } from '../book/state.mjs';
import { openOrganizer } from '../organize/watermark.mjs';
import { openDeliveryLedger, INTENT, OWED, SKIPPED } from './delivery.mjs';
import { makeEnvelope } from '../envelope.mjs';
import { createSpool } from '../spool.mjs';
import { createReceiveConnection } from '../ingest/connection.mjs';
import { bindInternals, constructorOf, internalsOf } from '../internal/wiring.mjs';
import { openDurability } from '../durability.mjs';

/**
 * The delivery ledger's retention bound (§9.1): the earlier of five minutes old or one gigabyte held.
 * Past it, the frames it still names are declared missing rather than kept for a delivery that will
 * not come - the decision is written down exactly like any other permanent loss, so a restart repeats
 * neither it nor the report. Here rather than in the ledger because the bound is a policy the wiring
 * owns, and because a test has to be able to shrink it to something it can actually reach.
 */
export const DEFAULT_LEDGER_RETENTION_MS = 5 * 60 * 1000;
export const DEFAULT_LEDGER_RETENTION_BYTES = 1024 * 1024 * 1024;

export function createStructure({
  market,
  stream = 'trades',
  adapter,
  path = null,
  Database = null,
  durability = null,
  webSocketImpl,
  rawWriter,
  spoolDir = null,
  maxQueuedFrames = 5_000,
  runId = null,
  venue = null,
  onAck = () => {},
  onGap = () => {},
  onStop = () => {},
  onDiagnostic = () => {},
  onRefetch = () => {},
  // Called once, after the store's execution right has been handed back at the end of an operation -
  // a socket arrival, a caller's feed, a recovery. It is an observation, not a capability: it is told
  // that the operation is over and cannot change its result. The supervisor uses it to carry out a
  // notification (a stop or a re-anchor) that was raised from inside the operation, because the
  // structure refuses to act on one while its own right is held. A hook that throws changes nothing.
  onOperationEnd = () => {},
  nowMs = () => Date.now(),
  maxWaitingEvents = 1_000,
  ledgerRetentionMs = DEFAULT_LEDGER_RETENTION_MS,
  ledgerRetentionBytes = DEFAULT_LEDGER_RETENTION_BYTES,
  // Whether recovery is left to the caller instead of running at construction. The entry point drives the
  // startup sequence itself - begin the run, restore the board's boundary, drain the spool, redeliver - and
  // a recovery that ran before the run was marked running would put the restore before the begin. A test or
  // a dry run builds a structure and drives it directly, so the default keeps the old behaviour.
  deferRecovery = false,
  ...receiveOptions
}) {
  // The checks that can refuse the configuration run before anything is opened: a construction that never
  // runs must not leave a store - or a file - behind for the next attempt to trip over.
  if (typeof rawWriter !== 'function') throw new TypeError('the structure needs a raw writer');
  // A structure that organizes one stream while its adapter carries another can only produce frames the
  // board will refuse as belonging to another board - after they have been written to the raw. The
  // mismatch is therefore refused here, before the book and the spool exist.
  if (adapter?.stream && adapter.stream !== stream) {
    throw new TypeError(
      `the structure organizes the ${stream} stream, but its adapter carries ${adapter.stream}`,
    );
  }
  if (!durability && !path) throw new TypeError('the structure needs a path for its store');

  // The store is the structure's own: given a path, it opens it here and never hands it out, so nothing a
  // caller holds is a way to write around the structure. (The wiring may still pass one it opened itself -
  // the tests do - and that path stays inside the module either way.)
  const openedHere = !durability;
  const store = durability ?? openDurability({ path, runId, ...(Database ? { Database } : {}) });
  durability = store;

  let claimToken = null;
  /**
   * Run one construction step that can fail.
   *
   * A structure that is never returned is a store nobody can close: this attempt undoes what it did to
   * the store - the claim it took, and, when it opened the store itself, the store - so the next
   * construction can take the file. A store the caller handed in is left open: it was never this
   * attempt's to end, and the caller still holds it.
   */
  function constructing(step) {
    try {
      return step();
    } catch (error) {
      if (claimToken !== null) {
        try {
          internalsOf(durability).releaseStructureOwner({ market, stream }, claimToken);
        } catch {
          // The store cannot be reached to release; there is nothing this attempt can undo.
        }
      }
      if (openedHere) {
        try {
          internalsOf(durability).close();
        } catch {
          // The original failure is the one to report; a store that will not close stays held on purpose.
        }
      }
      throw error;
    }
  }

  // The right, the transactions and the observation come from the wiring, never from the object handed in.
  // The claim is taken here too: a store serves one structure per board, and a second structure over the
  // same board would share its position and outlive the first close. (Boards that differ are separate
  // pages of one store, and each claims for itself.)
  const wiring = constructing(() => {
    const internals = internalsOf(durability);
    claimToken = internals.claimStructureOwner({ market, stream });
    return internals;
  });

  // Backpressure is decided here rather than inside a buffer. In one process the roles are called
  // synchronously, so the pressure shows up as the raw writer refusing: the frame then goes to the
  // spool, and only if the spool cannot hold it either does reception stop. (Across processes the
  // same decision is made when the channel reports a full queue.)
  let stopped = false;
  // Whether a re-anchor has already been asked for. The request is a state, not a per-frame report: a
  // broken proof refuses every ordinary frame that follows it, and one request is what a recovery needs.
  let refetchRequested = false;
  // The terminal state: close() has completed. Nothing may act on a closed structure - its store may
  // already serve another structure for this board, and a frame taken here would land in that other life.
  let closed = false;
  let spooledFrames = 0;
  let refusedFrames = 0;
  // True only while the spool is being walked. A record met here is *already* in the spool, so a raw
  // that refuses it again must not append it a second time: the walk would never make progress and the
  // spool would hold the same bytes over and over. It is a walk flag rather than an argument so that
  // the public frame handler keeps its shape and no caller can reach this state.
  let walkingSpool = false;
  // Re-entrancy guard for the drain: it re-injects frames through the same handler a live frame gets,
  // and that handler must never be able to start a second walk of the spool it is already walking.
  let drainingSpool = false;

  /**
   * Report that an operation has ended and the store's execution right is free again.
   *
   * This is the one point a notification raised from inside an operation can be acted on: while the
   * right is held the structure refuses to stop or start, so a request made from a socket arrival or a
   * caller's frame waits for the operation to finish. It is called after the right has been handed back
   * - never inside the take - and a hook that throws cannot change the operation's result.
   */
  const operationEnded = () => {
    try {
      onOperationEnd();
    } catch {
      // An observation that throws is not a fact about the operation it followed.
    }
  };

  // The construction is one operation of its own: the parts are opened through their internal path, which
  // does not take the right again, and the whole of it runs inside one take - so no hook an initialisation
  // calls can find the store free. The right is the store's own; nothing here adds a second flag for it.
  // The private routes are collected as the parts are built: the part objects themselves carry none of
  // them, and neither does anything a caller is handed - see `internal/wiring.mjs`.
  const { book, ledger, organizer } = constructing(() =>
    wiring.whileChange(() => ({
      book: constructorOf('book')({ market, stream, durability, nowMs, adapter }, wiring),
      // What the raw holds and the board does not have yet. It is a record in the store rather than a map in
      // this process, because the frames it names are only recoverable while that knowledge survives a crash.
      ledger: constructorOf('ledger')({ durability, market, stream, nowMs }, wiring),
      organizer: constructorOf('organizer')(
        {
          market,
          stream,
          durability,
          writeRaw: (envelope) => {
            const written = rawWriter(envelope);
            return written === true; // only a durable write may be acknowledged
          },
          capacity: () => (stopped ? 'stopped' : 'ok'),
          nowMs,
        },
        wiring,
      ),
    })),
  );
  const bookInternal = internalsOf(book);
  const ledgerInternal = internalsOf(ledger);
  const organizerInternal = internalsOf(organizer);
  const spool = spoolDir ? constructing(() => createSpool({ dir: spoolDir })) : null;

  // The run this structure has already admitted for this board. A takeover is issued once per run: the
  // second connection of the same run is a change of connection, not a new claim on the board, and the
  // run this process is not running cannot be admitted by this process at all.
  let admittedRunId = null;

  // A completion the caller has not taken delivery of yet. It is held until onAck returns, because the
  // notification is the one part of a completion that can fail on its own, and a repair that only works
  // the first time is a repair that is lost with the failure. Kept in memory on purpose: a ledger that
  // survives a crash belongs with the durable delivery ledger, not here.
  let pendingOriginAck = null;

  /**
   * Tell the organizer which connection it is organizing, and what the board knows about where that
   * connection's numbering starts.
   *
   * The origin is taken from the board rather than from the caller: two places deciding where a
   * connection starts is exactly how the raw position and the applied position drift apart. A
   * completion is acknowledged only for the ceiling it actually moved - the contiguous durable one -
   * because an acknowledgement that carried anything else would claim more than is durable, and asking
   * again delivers an acknowledgement whose notification failed.
   */
  function followConnection(connectionId, { origin = null } = {}) {
    // The board's own identity, not a second opinion from the caller: if the organizer measured a frame
    // against anything else, a frame from another run or generation would be written to the canonical
    // record on the strength of a connection id that happens to match.
    const boundary = book.appliedBoundary;
    const organized = organizerInternal.accept(connectionId, {
      firstSeq: origin,
      runId: boundary.runId ?? null,
      generation: boundary.generation ?? null,
    });
    if (organized?.advanced === true) {
      const ack = organizer.currentAck();
      if (ack) pendingOriginAck = ack;
    }
    if (pendingOriginAck !== null && pendingOriginAck.connectionId === connectionId) {
      const ack = pendingOriginAck;
      onAck(ack);
      pendingOriginAck = null; // only once the caller has taken it
    }
    return organized;
  }

  // The board's own vocabulary, by exact name - taken from the board, not guessed at. A pattern loose
  // enough to catch one wording also catches "first sequence unknown", which is a frame that is
  // durable and not on the board: the one state all of this bookkeeping exists to expose, and the last
  // thing that should be filtered out by a regular expression.
  //
  //   held by the board   the board has it and is waiting (it says so with waitingFor, and openGaps()
  //                       reports it) - quiet here, because the board will apply it itself
  //   already applied     a resend of something the board holds - quiet, nothing happened
  //   never applicable    it belongs to numbering this connection cannot use - recorded as a permanent
  //                       loss rather than held for a delivery that will not happen
  //   anything else       durable and unapplied: held here, reported once, offered again on accept
  const HELD_BY_BOOK = new Set([
    'waiting for the first sequence',
    'gap before this sequence',
    // A frame refused because the boundary proof is broken is not a loss: it is durable and owed, and a
    // replacement re-anchors the board it is waiting for. It stays owed, like a frame held for a hole.
    'the boundary proof is broken',
  ]);
  const ALREADY_APPLIED = 'already applied';
  const NEVER_APPLICABLE = new Set(['below the first sequence']);
  const OWED_REASON = 'durable in the raw and not applied to the board yet';

  let refusedByBook = 0;
  // Frames this process wrote into the ledger for the first time. Cumulative, like the spool and reception
  // counters, and not the same question as what is still owed - that is the ledger's own count - because an
  // intent a refused raw write takes back was still written down once.
  let framesWrittenDown = 0;

  /**
   * Write down that this frame can never be applied. The entry carries the decision from here on, so a
   * restart repeats neither the decision nor the report: the report is made where the decision is made.
   */
  function recordNeverApplicable(connectionId, receiveSeq, reason) {
    const { decided } = ledgerInternal.skip(connectionId, receiveSeq, reason);
    if (decided) onGap({ market, reason: `this frame can never be applied: ${reason}`, seq: receiveSeq });
  }

  /**
   * Write down that the raw holds this frame and the board does not have it yet - or, before the write is
   * attempted, that it is about to.
   *
   * This entry is what makes the difference between the two positions recoverable, so a frame that is
   * confirmed durable is written down in the same commit that claims it (the organizer's hook), while a
   * frame that was already durable is written down here and confirmed at once - its entry can be missing
   * in a store written before this record existed, and it would otherwise be delivered by nobody. Returns
   * whether it was the frame's first entry, so that a report about it is made once rather than once per
   * attempt, and touches no shared counter: that is a change to make after the commit, not inside it.
   */
  function owe(envelope, reason = OWED_REASON, state = OWED) {
    const { recorded } = ledgerInternal.record(envelope, reason, state);
    // The insert is a commit of its own - the organizer's transaction runs later, around a hook that only
    // confirms - so the counter moves after it and never before: a frame that was written down is a
    // committed fact, and one whose write never landed is not counted at all.
    if (recorded) framesWrittenDown += 1;
    return recorded;
  }

  /**
   * The write half of declaring a range of the followed connection missing: the board's record of it, and
   * nothing else. It is separate from dropping the proof because the two have to happen on either side of a
   * commit - the record inside the transaction that also writes the ledger's row, the proof afterwards. A
   * rollback takes the write back, and it cannot take an in-memory fact back with it.
   *
   * Only the connection the board follows concerns the board (C11); a loss of another connection is history.
   */
  function declareFollowedMissing(connectionId, reason) {
    const followed = book.appliedBoundary.connectionId;
    if (followed === null || connectionId !== followed) return false;
    bookInternal.declareMissing(connectionId, `a range of this connection was declared missing: ${reason}`);
    return true;
  }

  /**
   * Ask for a re-anchor: the board is not serving, and only a fresh replacement puts it back. It is a
   * request, made once per broken proof - the re-subscribe that answers it belongs to the entry point that
   * does not exist yet, and this is as far up as the wiring can carry it.
   */
  function requestRefetch(connectionId) {
    if (refetchRequested) return;
    // The request *is* the notification, so the flag is set only once the caller has been told: a hook that
    // throws leaves the request un-made and the next delivery asks again. A recovery request that was never
    // delivered must not be recorded as delivered, or nobody ever hears it and the board waits for ever.
    onRefetch({
      market,
      stream,
      connectionId,
      reason: 'the board needs a re-anchor: no replacement has arrived',
    });
    refetchRequested = true;
  }

  /**
   * Declare the entries past the retention bound missing, oldest first.
   *
   * §9.1 bounds how long organize keeps a frame the board has not taken: the earlier of five minutes or
   * one gigabyte. What is past that bound is not going to be delivered - the frame it names is a
   * permanent loss - so it is written down through exactly the same decision a frame below the first
   * sequence goes through: the row stays as the record, the first decision keeps it, and the report is
   * made once. An entry inside the bound is never touched.
   *
   * The byte half is measured over what the ledger is still *holding for delivery* - entries not already
   * decided. A decided loss keeps its row and its bytes, so counting those would leave the byte bound
   * impossible to satisfy and every sweep would re-skip rows that are already decided. The total the
   * ledger reports (heldSize) still counts them; this is which of them the bound can still act on.
   */
  function sweepRetentionInternal() {
    if (ledger.size() === 0) return { swept: 0 };
    const entries = ledger.oldestEntries();
    let remaining = 0;
    for (const entry of entries) if (entry.state !== SKIPPED) remaining += entry.bytes;
    let swept = 0;
    for (const entry of entries) {
      if (entry.state === SKIPPED) continue; // already decided: its loss is written down
      const tooOld = pastTimeBound(entry.recordedAtMs);
      const tooMuch = pastByteBound(remaining);
      if (!tooOld && !tooMuch) break; // this one, and everything newer, is inside the bound
      const reason = tooOld
        ? `the delivery ledger's retention bound passed this frame: it is older than ${ledgerRetentionMs} ms`
        : `the delivery ledger's retention bound passed this frame: the ledger holds more than ${ledgerRetentionBytes} bytes`;
      // The loss and the record of the proof it invalidates are one fact about the store, so they are
      // written in one transaction: a ledger row that says the range is gone while the board still holds a
      // proof over it is a state only a crash could leave, and the restart would then apply frames the
      // proof never covered. The proof itself is dropped in memory only after this transaction commits -
      // and the report and the request are hooks, so they are made after it too.
      const { decided, invalidated } = wiring.inTransaction(() => {
        const skipped = ledgerInternal.skip(entry.connectionId, entry.receiveSeq, reason);
        return {
          decided: skipped.decided,
          invalidated: skipped.decided ? declareFollowedMissing(entry.connectionId, reason) : false,
        };
      });
      if (decided) {
        swept += 1;
        // The state changes before any caller's hook: the proof goes with the commit that decided the loss,
        // because a hook that throws must not be able to leave the board serving on a range the store has
        // already written off - the row is decided, so nothing would come back to heal it later.
        if (invalidated) {
          bookInternal.dropProof(entry.connectionId);
          requestRefetch(entry.connectionId);
        }
        onGap({ market, reason: `this frame can never be applied: ${reason}`, seq: entry.receiveSeq });
      }
      remaining -= entry.bytes;
    }
    return { swept };
  }

  /**
   * Walk what the spool holds, oldest first, and re-inject each record through the same handling a live
   * frame gets - the raw write is attempted again, the ledger records it, the board applies it, and the
   * acknowledgement follows. There is no second delivery path here, which is the point: a frame that
   * comes back from the spool is treated exactly like one that has just arrived.
   *
   * The cursor moves once, to the end of the last *contiguously* consumed record: the first record the
   * raw still refuses stops the walk where it is, and nothing past it is confirmed - the caller may only
   * acknowledge a contiguous range, and advancing over a record that was not consumed would delete it.
   * The walk ends with the retention sweep, because a drain is one of the moments the bound is re-applied.
   */
  function drainSpoolInternal({ limit = 512 } = {}) {
    if (closed) {
      return { walked: 0, consumed: 0, advanced: false, swept: 0, refused: true, stopped: true, stoppedCode: 'closed', reason: 'this structure is closed' };
    }
    // Nothing is walked out of a stopped structure: its frame handler refuses everything, so the walk
    // would report each record as a hole instead of delivering it. The bound is still applied.
    if (stopped) {
      return { walked: 0, consumed: 0, advanced: false, ...sweepRetentionInternal(), stopped: true, stoppedCode: 'stopped', reason: 'this structure has stopped' };
    }
    let walked = 0;
    let consumed = 0;
    let lastConsumed = null;
    let stoppedReason = null;
    // The machine-readable half of the stop: the caller classifies by this rather than by matching the
    // report's words. Only `raw-refused` is transient - the raw may be able to take the record later.
    let stoppedCode = null;
    if (spool !== null && spool.bytes > 0 && !drainingSpool) {
      drainingSpool = true;
      walkingSpool = true;
      try {
        for (const record of spool.drainRecords({ limit })) {
          walked += 1;
          const result = feed(record.envelope);
          if (result.durable === true || result.alreadyDurable === true || result.applied === true) {
            consumed += 1;
            lastConsumed = { segment: record.segment, offset: record.offset };
            continue;
          }
          stoppedReason = result.stillSpilled
            ? 'the raw still refused the record'
            : result.reason ?? 'the record was not consumed';
          stoppedCode = result.stillSpilled ? 'raw-refused' : 'not-consumed';
          break;
        }
      } catch (error) {
        // A record the spool cannot hand back - a frame whose bytes do not describe the identity they
        // claim, a length that desynchronised - is not a record this walk may guess at. It stops where
        // it is, leaves the spool untouched, and says so, rather than letting a broken record take down
        // the frame handling that triggered the drain.
        stoppedReason = `the spool could not hand back a record: ${error.message}`;
        stoppedCode = 'spool-unreadable';
        onDiagnostic({ market, reason: stoppedReason });
      } finally {
        walkingSpool = false;
        drainingSpool = false;
      }
      if (lastConsumed !== null) spool.advance(lastConsumed);
    }
    const swept = sweepRetentionInternal();
    return { walked, consumed, advanced: lastConsumed !== null, ...swept, stopped: stoppedReason, stoppedCode };
  }

  /**
   * Drain at the moment the ladder can actually climb: when a frame's own write has just succeeded and
   * the spool is holding something. It is bounded and it runs inside the operation that wrote the frame,
   * never from a caller's hook - a repair that depends on somebody remembering to call it is a repair
   * that does not happen.
   */
  function maybeDrainSpool() {
    if (spool === null || spool.bytes === 0 || drainingSpool) return;
    drainSpoolInternal();
  }

  /** Past the bound means strictly past it: reaching the bound is not passing it. One place, so the
   *  cheap check below and the walk that acts on it cannot disagree about where the bound is. */
  const pastTimeBound = (recordedAtMs) => nowMs() - recordedAtMs > ledgerRetentionMs;
  const pastByteBound = (heldBytes) => heldBytes > ledgerRetentionBytes;

  /**
   * Re-apply the retention bound when the ledger's own numbers say it may have been passed. The aggregate
   * is one scan of one table with no sort; the oldest-first walk behind it runs only when that cheap answer
   * says the bound is plausibly crossed, so an ordinary frame pays one aggregate and nothing else.
   */
  function maybeSweepRetention() {
    if (ledger.size() === 0) return { swept: 0 };
    const held = ledger.heldSize();
    if (held.oldestMs === null) return { swept: 0 };
    if (!pastTimeBound(held.oldestMs) && !pastByteBound(held.bytes)) return { swept: 0 };
    return sweepRetentionInternal();
  }

  /**
   * The moment after a frame's own write succeeded: the ladder can climb and the bound can be re-applied.
   * It runs wherever a frame is handled - a socket arrival, a caller's feed, a replay - because a repair
   * that happens on one road in and not another is a repair that does not happen; and it runs inside the
   * operation that wrote the frame, never from a caller's hook.
   */
  function healAfterFrame(result) {
    if (!result || (result.durable !== true && result.alreadyDurable !== true)) return;
    maybeDrainSpool();
    maybeSweepRetention();
  }

  /**
   * Stop this structure receiving, for good.
   *
   * Reception is stopped *and closed*: a stopped structure refuses every further frame, so a socket left
   * open would keep delivering frames that go nowhere - each one reported as a hole and dropped, which is
   * the one outcome this design never allows. Escalating this to a non-zero exit belongs to the entry point
   * that does not exist yet; keeping the receiver from quietly eating data belongs here.
   */
  function stopReception(reason) {
    if (stopped) return;
    stopped = true;
    try {
      connection?.stop?.();
    } catch (error) {
      try {
        onDiagnostic({ market, reason: `reception could not be closed: ${error.message}` });
      } catch {
        // A diagnostic is best-effort by contract. It is wrapped rather than left to run: a diagnostic hook
        // that throws must not be able to swallow the news that reception has stopped, which comes next.
      }
    }
    onStop({ market, reason });
  }

  /**
   * Whether this frame could be one of this board's at all, asked without touching anything.
   *
   * The same questions the book and the organizer ask, in one place, so that a route which reaches the board
   * without passing through them (a resend that is served from the ledger) cannot skip them: the board's
   * identity, the board's market and stream, and the connection it currently holds. Refusing here has no
   * side effects, which is what makes it usable before anything has been decided.
   */
  function belongsToBoard(envelope) {
    const boundary = book.appliedBoundary;
    if (envelope.market !== market || envelope.stream !== stream) {
      return { ok: false, reason: 'this frame belongs to another board' };
    }
    if (boundary.connectionId === null) {
      return { ok: false, reason: 'no connection has been accepted yet' };
    }
    if (envelope.connection_id !== boundary.connectionId) {
      return { ok: false, reason: 'not the accepted connection' };
    }
    if (
      (envelope.run_id ?? null) !== (boundary.runId ?? null) ||
      (envelope.generation ?? null) !== (boundary.generation ?? null)
    ) {
      return { ok: false, reason: 'this frame belongs to another run or generation' };
    }
    return { ok: true };
  }

  /**
   * Let go of what is delivered: as far as the board has reached, and as far as the raw's own record can
   * vouch for it.
   *
   * Both bounds are needed. A frame the raw holds above its contiguous position has no record anywhere else
   * than this one, and forgetting it would make the next resend of that frame a rewrite - the raw refuses to
   * write what it already has, and that refusal would be read as "not durable" and stop reception over a
   * frame nothing is wrong with. The board's ceiling moves contiguously only (§2.2), so releasing up to it
   * cannot drop a frame still waiting behind a hole.
   */
  function releaseDelivered() {
    const boundary = book.appliedBoundary;
    const organized = organizer.ackState?.upToSeq ?? null;
    ledgerInternal.release({
      connectionId: boundary.connectionId,
      firstSeq: boundary.firstSeq,
      // A raw position of NULL is the raw saying it holds nothing contiguously, so there is nothing it can
      // vouch for: the entry is the only record of what is in the raw under this connection, and it stays
      // until a position exists to compare against.
      upToSeq: boundary.upToSeq === null || organized === null ? null : Math.min(boundary.upToSeq, organized),
    });
  }

  /**
   * Restore what the store recorded about the board's boundary: which connection the board follows, where
   * that connection's numbering starts, and what the raw already holds for it.
   *
   * This is the (b) step of the startup sequence and it is deliberately not conditional on the ledger
   * holding anything. A restart whose ledger is empty but whose spool still holds spilled frames has to
   * organize them on the connection the board recorded - and the organizer learns that connection nowhere
   * else. Skipping this because there is nothing owed leaves the organizer with no accepted connection, so
   * the spool walk is refused frame by frame and the spool never drains. The frames the raw already holds
   * are declared here too, before anything is released, because a frame the raw holds above its contiguous
   * position has no other record and the next resend of it would otherwise be written to the raw again.
   */
  function restoreBoundaryInternal() {
    const owed = ledger.pending({ state: OWED });
    const boundary = book.appliedBoundary;
    if (boundary.connectionId !== null) {
      followConnection(boundary.connectionId, { origin: boundary.firstSeq ?? null });
      for (const entry of owed) organizerInternal.note(envelopeFromEntry(entry), { rawAlreadyHolds: true });
    }
    return { restored: boundary.connectionId !== null, owed: owed.length };
  }

  /**
   * Put one frame on the board, and say what happened to it.
   *
   * One route for every frame that reaches the board - the one that just arrived, and the one the ledger
   * kept - so there is no second behaviour that quietly differs from the first: the book's own dedupe makes
   * a repeat a no-op, the applied ceiling releases what the board now holds, and a refusal the board only
   * records as a loss is reported exactly once.
   */
  function deliver(target, note, { newlyWritten: wrote = false } = {}) {
    const applied = bookInternal.apply({
      envelope: target,
      changes: adapter.changesFor ? adapter.changesFor(target) : [],
    });
    // A frame the proof refused left the board broken: only a replacement re-anchors it, so ask for one.
    // A replacement that did land answered the request, and a later break may ask again.
    if (applied.proofBroken === true) {
      requestRefetch(book.appliedBoundary.connectionId);
    } else if (applied.replaced === true) {
      refetchRequested = false;
    }
    // The board has taken the frame, so the structure asks whether its boundary is proved: a board that
    // never leaves syncing is a board nobody may read from, and a repair that waits for a caller to ask is
    // a repair that does not happen. The book answers with the truth - a proof covering what it applied, or
    // the reason there is none - and only the first puts the board in service (C6, C7).
    if (applied.applied === true) bookInternal.proveBoundary();
    // The board may have anchored its boundary on the origin this frame declares. The organizer has to hear
    // the same origin: the ceiling lives there, and a start that reached only the board would leave every
    // frame durable and unacknowledged, waiting for a start that has arrived.
    //
    // Who is followed comes from the board's own boundary, never from the frame that was just handed in: a
    // frame of a connection the board does not hold is refused above, and letting it name the organizer's
    // connection would refuse the frames of the connection that really is the board's - before the raw, so
    // they would not even be spooled.
    const followed = book.appliedBoundary;
    if (followed.connectionId !== null && followed.firstSeq !== null) {
      followConnection(followed.connectionId, { origin: followed.firstSeq });
    }
    releaseDelivered();
    // Two refusals are the book working as designed: a frame it already holds, and a frame it is holding
    // until the hole before it is filled. Those are states, not losses.
    //
    // Any other refusal is a frame that is durable in the raw, acknowledged as durable, and not on the board
    // - and if that is not written down, the raw position and the applied position drift apart with nothing
    // to say so. C8 keeps those two positions separate precisely so that the difference can be seen.
    if (
      applied.applied === false &&
      applied.reason &&
      applied.reason !== ALREADY_APPLIED &&
      !HELD_BY_BOOK.has(applied.reason)
    ) {
      if (NEVER_APPLICABLE.has(applied.reason)) {
        recordNeverApplicable(target.connection_id, target.receive_seq, applied.reason);
        return { ...note, ...applied };
      }
      // Reported once per frame: a resend repeats the same refusal, and repeating the report turns one lost
      // frame into a stream of noise.
      if (wrote) {
        refusedByBook += 1;
        onGap({
          market,
          reason: `the board refused the frame: ${applied.reason}`,
          seq: target.receive_seq,
        });
      }
    }
    return { ...note, ...applied };
  }

  function feed(envelope) {
    if (closed) {
      // A closed structure is not a writer: its close ended it, and the board may already belong to
      // another structure - a frame taken here would be applied into a life that is over.
      return { accepted: false, reason: 'this structure is closed' };
    }
    if (stopped) {
      // Reception is stopped, so anything still arriving is recorded as a hole rather than lost
      // silently or applied out of order.
      refusedFrames += 1;
      onGap({ market, reason: 'reception stopped: nothing more can be held', seq: envelope.receive_seq });
      return { accepted: false, reason: 'stopped' };
    }
    // Whether this call created the frame's entry in the ledger: the report about a frame is made once, on
    // the first time it is written down, not once per attempt to deliver it.
    let newlyWritten = false;
    // Whether *this* attempt wrote the intent it may take back. An entry that was already there is not this
    // attempt's to remove: an existing one may be a frame the raw really does hold (its claim was committed
    // in an earlier life, and the attempt that refused now says nothing about it), or one whose write is
    // still owed an attempt.
    let wroteIntentNow = false;
    try {
      // The raw already holds this frame, so a resend of it is not a question about the raw any more. Asking
      // for the write again would rewrite a frame the canonical record has, and a refusal of that rewrite
      // says nothing about whether the frame is durable - it is, that is what the entry means. The stored
      // frame is what goes to the board, and the refusal path (spool, stopping) is never reached on this
      // account.
      const stored = ledger.find(envelope.connection_id, envelope.receive_seq);
      if (stored !== null) {
        // The arrival is judged by what it claims to be, not by what the store holds: serving a frame from the
        // ledger must not become a way for another run, generation or board to be told about this one.
        const claim = belongsToBoard(envelope);
        if (!claim.ok) return { accepted: false, reason: claim.reason, ack: null };
      }
      // What the frame *is* was decided when it was first written down: a resend under the same key is the
      // same frame, and the store's copy is the one the raw holds (or is about to). Taking the arrival's
      // bytes, meta or identity for it afterwards would let the canonical record and the board describe
      // different data under one key.
      const target = stored === null ? envelope : envelopeFromEntry(stored);
      // A resend is served from the entry whenever the raw is already known to hold the frame - an owed
      // entry, or one whose permanent loss is already decided. Only an intent is still a question, and only
      // it goes back through the organizer: a skipped frame sent through it would be rewritten to the raw
      // and reported again, which is the noise the decision exists to end.
      if (stored !== null && stored.state !== INTENT) {
        return deliver(target, {
          accepted: true,
          alreadyDurable: true,
          reason: 'the raw already holds this frame',
          ack: null,
        });
      }
      const note = organizerInternal.note(target, {
        // The intent is written before the raw is touched and confirmed in the commit that claims the frame
        // durable. Neither hook touches this process's counters: those move after the commit, when the store
        // has actually changed.
        onIntent: (frame) => {
          if (owe(frame, OWED_REASON, INTENT)) {
            newlyWritten = true;
            wroteIntentNow = true;
          }
        },
        onDurable: (frame) => ledgerInternal.confirm(frame),
      });
      if (note.accepted === false) return note;
      if (note.durable === false && wroteIntentNow) {
        // The raw refused this attempt and the entry exists only because of it, so it is taken back: a
        // restart would otherwise deliver a frame the canonical record never took. An entry that was already
        // there stays, whatever this attempt decided - the raw may hold that frame, and deleting it here is
        // deleting the only record that it still has to be delivered (C8).
        ledgerInternal.drop(target.connection_id, target.receive_seq);
      }
      // A frame that was already durable is still owed unless the board's position has passed it: it may be
      // exactly the frame a crash left behind, and the record of that is what brings it back.
      if (note.alreadyDurable && owe(target, note.reason ?? OWED_REASON)) newlyWritten = true;
      // Only now may the raw be acknowledged. An acknowledgement that outran the record would let a crash
      // take with it the only statement that this frame still has to reach the board (C8, C11).
      if (note.ack) onAck(note.ack);

      // C8: raw durability and board application are separate questions with separate positions. A frame
      // that is already durable may still be unapplied, so a resend is routed to the book rather than
      // dropped here; the book's own (connection, sequence) dedupe makes a second application a no-op,
      // which is what keeps this from becoming a double write.
      if (note.durable || note.alreadyDurable) {
        return deliver(target, note, { newlyWritten });
      }

      // The raw writer refused: the frame is spilled rather than dropped.
      // What is spilled is the frame this key means - the one that was written down - not the resend that
      // arrived under it.
      if (walkingSpool) {
        // This frame came out of the spool and the raw refused it again. Appending it here would copy a
        // record that is already in the spool; the bytes would grow by themselves and never be read. It
        // stays exactly where it is, and the walk that met it stops there (nothing past it may move).
        return { ...note, spooled: false, stillSpilled: true };
      }
      if (spool && spool.append(target) && !spool.failed) {
        spooledFrames += 1;
        return { ...note, spooled: true };
      }
      // Nothing could hold it. Reception stops and the gap is written down, which is the only honest
      // outcome left: continuing would mean pretending the frame was handled. The stop is the state that
      // matters and the report is only news of it, so the stop is recorded first - a hook that throws must
      // not leave reception running, or the next frame is taken by a process with nowhere to put it.
      stopReception('nothing could hold the frame');
      onGap({ market, reason: 'raw refused and the spool could not hold it', seq: envelope.receive_seq });
      return { ...note, stopped: true };
    } catch (error) {
      // An exception from a caller's hook stops reception and the caller hears about it. That includes the
      // one our own gate raises: a module opened from inside a frame is refused before it writes anything
      // (so no nested BEGIN is ever attempted), but the point at which it was refused may be after the raw
      // has taken the frame - and continuing from there would leave a durable frame with no record that it
      // is owed, which is the loss this set exists to prevent. A refusal returned by the guard, by contrast,
      // writes nothing and needs no stop.
      // Same order as above: the stop is recorded before it is reported, because the report can throw and
      // the stop cannot be left undone. Note the report about the failure is itself a hook a caller supplies.
      stopReception(error.message);
      onGap({ market, reason: `failure while handling a frame: ${error.message}`, seq: envelope.receive_seq });
      return { accepted: false, reason: 'failure', error };
    }
  }

  /**
   * Rebuild the frame a ledger entry stands for. The entry carries everything the envelope needs,
   * including the bytes and the meta the frame arrived with, so nothing here has to ask the raw what it
   * holds - the raw writer is a caller's hook with no read side.
   */
  function envelopeFromEntry(entry) {
    return makeEnvelope({
      market,
      stream,
      connectionId: entry.connectionId,
      runId: entry.runId,
      venue: entry.venue,
      generation: entry.generation,
      receiveSeq: entry.receiveSeq,
      recvTsMs: entry.recvTsMs,
      recvMonoNs: entry.recvMonoNs,
      raw: entry.raw,
      meta: entry.meta,
    });
  }

  // Every arrival from outside is handed to this executor. When the store is free the arrival is kept like
  // any other and the wait-list runs: what waited goes first, in order, and this arrival follows it. When
  // an operation is running the arrival is kept and runs when that operation's own work is done - nothing
  // is dropped, and nothing runs in the middle of a frame. A wait-list that fills up stops reception; the
  // stop is recorded where it happens and carried out inside the executor once the work it interrupted has
  // ended, because silence would look like a quiet stream, a notification must not come from inside a frame
  // - the caller's own hook - and the stop's own hooks must meet the same gate as everyone else.
  const waiting = [];
  let draining = false;
  // The first arrival that did not fit, recorded but not answered for yet: the answer is a stop, and a stop
  // called from the arrival that overflowed the list would run inside the frame it arrived in.
  let overflowed = null;

  /** The stop a full wait-list owes, carried out inside the executor once the work it interrupted has ended. */
  function settleOverflow() {
    if (overflowed === null || stopped) return;
    const label = overflowed;
    overflowed = null;
    stopReception(`the wait-list for arrivals is full (${maxWaitingEvents}); ${label} was dropped`);
  }

  const receiveEvent = (label, work) => {
    if (wiring.inChange() === true) {
      // An operation is running: the arrival is kept, in order, and runs when that operation's own work is
      // done. Recorded only when it does not fit: the stop belongs to the operation this arrival arrived
      // during, and it is carried out when that operation has finished, not from inside it.
      if (waiting.length >= maxWaitingEvents) {
        if (overflowed === null) overflowed = label;
        return false;
      }
      waiting.push({ label, work, result: undefined });
      return undefined;
    }
    // The executor is free. This arrival is kept BEFORE anything runs: what waited - the arrivals an
    // operation that threw left behind - goes first, in order, and this arrival follows it, so nothing that
    // was already accepted is overtaken by what came after it, and an exception from a task that goes first
    // leaves this arrival held rather than lost. The exception still reaches the caller: the executor does
    // not swallow it. The stop a full wait-list owes is carried out here too, inside the same take, so the
    // hooks it calls meet the same gate as any other caller.
    const entry = { label, work, result: undefined };
    if (waiting.length < maxWaitingEvents) {
      waiting.push(entry);
    } else {
      // A full list and a free executor: the queued work runs, and this arrival is the one that did not fit.
      if (overflowed === null) overflowed = label;
      entry.dropped = true;
    }
    try {
      return wiring.whileChange(() => {
        try {
          drainWaiting();
          return entry.dropped === true ? false : entry.result;
        } finally {
          settleOverflow();
        }
      });
    } finally {
      // The right is back: this is where a request raised by the frame just handled is carried out. The
      // socket arrival is the main road in, so a notification raised here that nobody acted on would be
      // the ordinary case, not the corner one.
      operationEnded();
    }
  };
  // Runs inside the take that owns the work: the tasks are continuations, so they take no right of their
  // own, and a task that causes another arrival queues it for the same loop.
  const drainWaiting = () => {
    if (draining) return;
    draining = true;
    try {
      while (waiting.length > 0) {
        const next = waiting.shift();
        next.result = next.work();
      }
    } finally {
      draining = false;
    }
  };

  // Nothing here is a special case for the socket this structure opened itself: an event that arrives while
  // an operation is running waits for it, including the announcement of the socket's own generation, which
  // carries the continuation that opens it.
  // A frame the socket delivered and a frame a caller handed in take the same road once the write has
  // happened: the ladder is climbed and the bound re-applied there - one place, not one per entrance.
  const feedAndHeal = (envelope) => {
    const result = feed(envelope);
    healAfterFrame(result);
    return result;
  };
  const feedEntry = (envelope) => receiveEvent('message', () => feedAndHeal(envelope));
  const admitEntry = (details) =>
    receiveEvent('generation', () => {
      const admitted = admitOnGeneration(details);
      details.settle?.(admitted);
      return admitted;
    });

  const connection = constructing(() =>
    createReceiveConnection({
      // The caller's options come first so that the wiring below cannot be replaced by them: a caller who
      // could override onEnvelope could bypass the book entirely, and a caller who could override
      // onGeneration could take a connection without the book ever hearing about it.
      ...receiveOptions,
      adapter,
      market,
      runId,
      venue,
      webSocketImpl,
      onEnvelope: feedEntry,
      onEvent: (label, work) => receiveEvent(label, work),
      // Reception's own reports - a connection it refused to open, a socket it tore down, a frame it could
      // not parse - travel to the same caller that hears about the book, so a refusal that stops reception
      // is not something only the connection knows.
      onDiagnostic: (diagnostic) => onDiagnostic(diagnostic),
      onGeneration: admitEntry,
    }),
  );

  // What a caller is handed for the connection: its reads only. Starting and stopping reception are change
  // operations of the structure, not of the connection a caller holds - a connection replaced from inside an
  // operation would hand the board a new identity while the frame being processed belongs to the old one.
  // What a caller is handed for each part: the reads only. The parts themselves stay in this module's own
  // closure, which is the wiring's private side, so nothing a caller holds can change what the structure is
  // doing - and the containers the reads return are copies, so mutating what was handed over is not a way
  // in either. The change operations of these parts are the structure's own windows, or nothing at all.
  const copyEntry = (entry) =>
    entry === null || typeof entry !== 'object'
      ? entry
      : { ...entry, ...(entry.raw === undefined ? {} : { raw: Buffer.from(entry.raw) }) };
  const copyList = (list) => list.map(copyEntry);
  const bookView = {
    get market() {
      return book.market;
    },
    get stream() {
      return book.stream;
    },
    get board() {
      return {
        size: (side, price) => book.board.size(side, price),
        get depth() {
          return book.board.depth;
        },
        rows: () => copyList(book.board.rows()),
      };
    },
    get lastRefusal() {
      return copyEntry(book.lastRefusal);
    },
    get phase() {
      return book.phase;
    },
    get isRunning() {
      return book.isRunning;
    },
    get appliedBoundary() {
      return { ...book.appliedBoundary };
    },
    get proof() {
      return { ...book.proof };
    },
    get ownerEstablished() {
      return book.ownerEstablished;
    },
    resumeFrom: () => copyEntry(book.resumeFrom()),
    retiredRuns: () => copyList(book.retiredRuns()),
    openGaps: () => copyList(book.openGaps()),
  };
  const organizerView = {
    get market() {
      return organizer.market;
    },
    get stream() {
      return organizer.stream;
    },
    get ackState() {
      return copyEntry(organizer.ackState);
    },
    currentAck: () => copyEntry(organizer.currentAck()),
    openGaps: () => copyList(organizer.openGaps()),
  };
  const ledgerView = {
    get market() {
      return ledger.market;
    },
    get stream() {
      return ledger.stream;
    },
    find: (...args) => copyEntry(ledger.find(...args)),
    pending: (...args) => copyList(ledger.pending(...args)),
    heldSize: () => ledger.heldSize(),
    oldestEntries: (...args) => ledger.oldestEntries(...args),
    size: () => ledger.size(),
  };
  const connectionSurface = {};
  for (const [name, descriptor] of Object.entries(Object.getOwnPropertyDescriptors(connection))) {
    if (name === 'start' || name === 'stop') continue;
    Object.defineProperty(
      connectionSurface,
      name,
      descriptor.get ? { get: () => connection[name], enumerable: true } : descriptor,
    );
  }
  // What a caller is handed for the spool: the reads only. Appending, advancing the cursor past a
  // record, syncing and closing are how the structure drives it, and they stay in this module's own
  // closure - an advance from outside deletes the segments behind the position it names, and with
  // them frames nobody has delivered yet. The reads return copies already (a segment list, a cursor,
  // freshly decoded envelopes), so editing what was handed over changes nothing either.
  const spoolView =
    spool === null
      ? null
      : {
          get bytes() {
            return spool.bytes;
          },
          get segments() {
            return spool.segments;
          },
          get cursor() {
            return spool.cursor;
          },
          get isOverBound() {
            return spool.isOverBound;
          },
          get failed() {
            return spool.failed;
          },
          drain: (...args) => spool.drain(...args),
        };

  /**
   * The same acceptance as a caller's, reached from the connection's own announcement. It is one
   * operation - the book, the organizer, the acknowledgement and the redelivery - so it takes the
   * store's execution right for the whole of it, and a re-entrant arrival is refused the way a refused
   * connection is: reception does not start.
   */
  function admitOnGeneration({ connectionId, generation, firstSeq, runId: incomingRunId }) {
      // A closed structure admits nothing: a generation that was still waiting when close() ran must not
      // start a reception that is over.
      if (closed) return false;
      // Acceptance goes through the same route as a caller's accept(), so there is one behaviour rather
      // than a manual path and an automatic path that quietly differ. The generation is announced
      // before the connection's frames arrive, which is when the book needs to hear about it.
      //
      // The takeover is issued here and only here. It is not a comparison of generations - the new run
      // starts its own numbering - so it needs two facts the book cannot see: that the connection
      // belongs to the run *this* process is running, and that the board is currently owned by a
      // different run. The recorded owner may be NULL - a run-less owner this store wrote down - and that
      // is still an owner: the board has to be handed over explicitly (C11), which is exactly what this
      // condition does, rather than being refused for ever. Recovering an older run's board is not a
      // takeover and must not consume the authorisation, or the run that actually replaces it would then
      // be refused.
      const ownerRun = book.appliedBoundary.runId ?? null;
      const takeover = admittedRunId === null && runId !== null && incomingRunId === runId && ownerRun !== runId;
      const accepted = admit(connectionId, { generation, firstSeq: firstSeq ?? null, runId: incomingRunId }, { takeover });
      if (!accepted.accepted) {
        // The diagnostic is best-effort and the stop is not: a hook that throws from the diagnostic must
        // not be able to swallow the news that this connection was not admitted, which is the only thing
        // that tells the caller to stop and exit non-zero rather than sit on a socket nobody admitted.
        try {
          onDiagnostic({ market, reason: `the book did not accept this connection: ${accepted.reason}` });
        } catch {
          // the stop below is the fact that matters
        }
        // Reception is not allowed to start: a connection the book refused would have every frame of it
        // refused further down, after it had been stamped and counted as received. And it is not allowed
        // to be silent either: a receiver that has stopped receiving without saying so is worse than one
        // that stopped loudly. The supervisor turns this into a stop and a non-zero exit.
        onStop({ market, reason: `this connection was not admitted: ${accepted.reason}` });
        return false;
      }
      admittedRunId = incomingRunId;
      return true;
  }

  /**
   * Accept a connection: one route for the manual case and the automatic one, so there is no second
   * behaviour that quietly differs from the first.
   *
   * The takeover is not a caller's argument. A caller may name a connection, declare a generation and
   * complete an origin; handing this board to a different run is authorised where reception actually
   * starts, because that is the only place that knows which run is running here.
   */
  function admit(connectionId, options, { takeover = false } = {}) {
    const accepted = bookInternal.accept(connectionId, { ...options, takeover });
    if (accepted?.accepted) {
      // Every part of the structure follows the same connection. The organizer kept its own idea of
      // which connection it was working on, so it is told here - with the origin the board actually
      // anchored to, not the one a caller hoped for - rather than left to infer it from whichever frame
      // happens to arrive first.
      followConnection(connectionId, { origin: book.appliedBoundary.firstSeq ?? null });
      // And the held frames were refused because the book did not know this connection; now that it
      // does, they are offered again without anyone having to remember to ask, because a repair that
      // depends on somebody calling it is a repair that does not happen.
      if (ledger.size() > 0) redeliverPendingInternal();
    }
    return accepted;
  }

  const api = {
    market,
    /** Accept a connection explicitly. Frames from any other connection are refused by the book. */
    accept: (connectionId, options = {}) => {
      if (closed) return { accepted: false, reason: 'this structure is closed' };
      // A caller may not hand this board to another run by asking: that authorisation is issued where
      // reception actually starts, from the board's own recorded owner and the run that is running here.
      const { takeover: _ignoredTakeover, ...callerOptions } = options;
      return admit(connectionId, callerOptions);
    },
    /**
     * Offer the owed frames to the book again, oldest first. What was refused because the book did not
     * know the connection can be applied once it does; a frame the book still refuses stays owed, so a
     * failed attempt costs nothing and the difference between the raw and the board remains visible.
     */
    redeliverPending: () => {
      if (closed) return { applied: 0, refused: true, reason: 'this structure is closed' };
      // Four answers, kept apart on purpose and exclusive by construction: every owed entry the pass is
      // offered leaves as exactly one of applied, held, skipped or unadmitted. What reached the board is
      // applied; what the board is holding stays owed, and so does anything else it refuses for now; what
      // belongs to a connection that is gone will never be applied and is written down as a permanent
      // loss; and what no connection has been accepted for yet is neither a delivery nor a loss - it may
      // still be admitted, so it waits. Summing the four counts is the pass's own account of what it was
      // offered, so a frame that never arrived is never counted as one that did.
      let appliedCount = 0;
      let heldCount = 0;
      let skippedCount = 0;
      let unadmittedCount = 0;
      // Only what the raw is confirmed to hold: an intent is a frame the raw may not have, and delivering
      // it would put the board ahead of the canonical record.
      for (const entry of ledger.pending({ state: OWED })) {
        if (book.appliedBoundary.connectionId === null || !book.ownerEstablished) {
          // Nobody has been accepted for this board yet, and a row that predates the ownership columns is
          // nobody's either, whatever connection name it carries. The frame may yet be admitted - it is not
          // a loss and must not be recorded as one, because a report says what happened, and nothing has.
          unadmittedCount += 1;
          continue;
        }
        if (entry.connectionId !== book.appliedBoundary.connectionId) {
          // The owner is established and it belongs to another connection: the run this frame arrived in
          // never comes back, so nothing will ever accept it. That is a permanent loss, and the decision
          // is written down where it is made - the entry keeps the row, and the ledger is what says the
          // loss is known. A restart repeats neither the decision nor the report.
          recordNeverApplicable(
            entry.connectionId,
            entry.receiveSeq,
            `the connection this frame belonged to is gone: ${entry.reason}`,
          );
          skippedCount += 1;
          continue;
        }
        // Through the same route as a live frame: the board may adopt an origin here, and the organizer has to
        // hear it before anything is released - a start that reached only the board would leave the raw's
        // position behind, and the next resend of that frame would be written again.
        const result = deliver(
          envelopeFromEntry(entry),
          { accepted: true, alreadyDurable: true, reason: 'the raw already holds this frame', ack: null },
        );
        if (result.applied === true || result.reason === ALREADY_APPLIED) {
          appliedCount += 1;
        } else if (result.reason && NEVER_APPLICABLE.has(result.reason)) {
          // deliver wrote the decision down itself, through the same helper.
          skippedCount += 1;
        } else {
          // The board is holding it until the hole before it is filled, or its start is known, or refused
          // it for another reason: either way it stays owed, and a failed attempt costs nothing.
          heldCount += 1;
        }
      }
      // Whatever the board now holds stops being owed, released by position: the ceiling the board
      // reached covers exactly the contiguous frames it applied.
      releaseDelivered();
      return {
        applied: appliedCount,
        held: heldCount,
        skipped: skippedCount,
        unadmitted: unadmittedCount,
        stillPending: ledger.size(),
      };
    },
    /**
     * A restart resumes from the board's applied position (C8, C11): everything the raw holds and the
     * board does not have yet is offered to it again, oldest first.
     *
     * The frames go straight to the book rather than back through the organizer. The organizer's own
     * record says the raw already holds them - that is what the ledger entry means - and asking it to
     * take the frame again would write it to the raw a second time for no gain. What the book takes is
     * released by position; what it cannot take yet stays in the store, which is exactly what the store
     * is for - a hole it waits behind is still owed, and a connection that is gone is written down as
     * the permanent loss it is.
     */
    resume: () => {
      if (closed) return { delivered: 0, refused: true, reason: 'this structure is closed' };
      // What the raw is confirmed to hold goes first, and it is not a matter of taste: those frames are what
      // tells the board where this connection's numbering can start (a frame it has been handed fixes the
      // ceiling, §2.2). An intent re-decided before them could carry a start declaration above a frame the
      // store already holds, and the board would accept it - skipping that frame for ever.
      // Before anything is released, the organizer is told what the raw already holds. A frame the raw holds
      // above its contiguous position has no record anywhere else, and without this the next resend of it would
      // be written to the raw again - the raw refuses a rewrite of what it has, and that refusal would be read
      // as a frame that is not durable. Telling it here is what makes the release below safe.
      // (b) first, and whether or not anything is owed: the organizer has to know the board's connection
      // before a spilled frame or a redelivered one can be organized at all, and a restart with an empty
      // ledger is exactly the case where it would otherwise learn nothing.
      restoreBoundaryInternal();
      const delivered = redeliverPendingInternal();
      // Then an intent, which is a frame the raw may not hold: it is offered back through the organizer,
      // which decides the write again - the one direction in which a crash leaves something recoverable.
      // The organizer has to be on the connection the board recorded for that to be anything but a refusal
      // (fail-closed), and the board's own identity is where that comes from.
      const intended = ledger.pending({ state: INTENT });
      if (intended.length > 0) {
        const boundary = book.appliedBoundary;
        if (boundary.connectionId !== null) {
          followConnection(boundary.connectionId, { origin: boundary.firstSeq });
        }
        for (const entry of intended) feed(envelopeFromEntry(entry));
      }
      // Everything the raw holds is now in the organizer's own record, which is what a resend is judged
      // against.
      return { intended: intended.length, offered: delivered.stillPending, ...delivered };
    },
    stream,
    book,
    organizer,
    ledger,
    connection: connectionSurface,
    book: bookView,
    organizer: organizerView,
    ledger: ledgerView,
    spool: spoolView,
    /**
     * (b): put the organizer on the connection the board recorded, and tell it what the raw already holds,
     * whether or not the ledger holds anything. The startup sequence calls this before the spool walk.
     */
    restore: () => {
      if (closed) return { restored: false, reason: 'this structure is closed' };
      return restoreBoundaryInternal();
    },
    /** (a): mark this run as the live one. Called by the entry point before anything is restored. */
    beginRun: () => {
      if (closed) return { begun: false, reason: 'this structure is closed' };
      wiring.beginRun();
      return { begun: true };
    },
    /** A clean end: reception has stopped and everything received has been acknowledged as durable. */
    completeRun: () => {
      if (closed) return { completed: false, reason: 'this structure is closed' };
      wiring.completeRun();
      return { completed: true };
    },
    start: () => {
      if (closed) return { started: false, reason: 'this structure is closed' };
      // A structure that has stopped does not start again by being asked to: reception was closed because the
      // frames could not be handled, and reopening the socket would deliver frames this structure can only
      // refuse - the state is what says so, not the socket. Restarting is a new process with a new run.
      if (stopped) return { started: false, reason: 'this structure has stopped' };
      // A restart delivers what the raw already holds before it listens again: the two positions are where
      // this process and its store disagree, and nothing new should be added to the stream before that
      // difference is closed.
      resumeInternal();
      // Recovery can stop this structure (a frame nothing could hold): opening the socket then would deliver
      // frames the structure can only refuse, and the stop is exactly the information that reception must not
      // start.
      if (stopped) return { started: false, reason: 'this structure has stopped' };
      return connection.start();
    },
    stop: () => {
      if (closed) return;
      connection.stop();
      spool?.close();
    },
    /** The termination of the structure: reception, the spool, and the store it opened itself. */
    close: () => {
      // Once: a closed structure has ended, and a close called again must not reach anything a later
      // structure holds - the board given back by the first close may already belong to another one.
      if (closed) return;
      // The structure ends the store it opened itself; a store a caller handed in is the caller's to end -
      // the structure stops using it and gives its board back. Reception first, and the file is free only
      // after the close that succeeded.
      connection.stop();
      spool?.close();
      if (openedHere) {
        // The store's own close would be refused inside this operation; the wiring's is the same close
        // without a second take.
        wiring.close();
      }
      closed = true;
      // The board is given back last, and only the claim that made it: a close that failed can be tried
      // again, and until it succeeds the board is still this structure's.
      wiring.releaseStructureOwner({ market, stream }, claimToken);
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
        // What the raw holds and the board does not have yet: the difference between the two positions
        // that a restart is able to close, rather than a number kept in this process's head.
        owed: ledger.size(),
        framesWrittenDown,
        stopped,
      };
    },
  };

  // The structure's own change operations take the store's execution right like every other module's: a
  // caller's hook is a synchronous function it handed in, so it can call back in while a frame is being
  // processed, and the answer has to be the same wherever it lands. The unguarded names stay local to this
  // function - admit, resume and redeliver are one operation rather than three - so that nothing reachable
  // from a caller's hook can run one while another is in progress. The refusals are shapes the callers
  // already read: `feed` answers like a refused frame, `start` like a structure that did not start, the
  // rest carry the code.
  const feedInternal = api.feed;
  const acceptInternal = api.accept;
  const redeliverPendingInternal = api.redeliverPending;
  const resumeInternal = api.resume;
  const startInternal = api.start;
  const stopInternal = api.stop;
  const restoreInternal = api.restore;
  const beginRunInternal = api.beginRun;
  const completeRunInternal = api.completeRun;
  // A window takes the right, runs the operation, and then lets the arrivals that waited for it through -
  // all inside the same take, so an arrival is never processed in the middle of the frame that was running.
  // The wait is emptied even when the operation throws, and a full wait-list is answered for here as well:
  // an overflow that happened during a window is the same stop, carried out the same way. Once the right is
  // back the operation has ended, and that is reported - a refused window never held the right, so it has
  // no end to report.
  const window = (name, run, refusalShape, after) => {
    const guarded = wiring.guard(
      name,
      (...args) => {
        try {
          const result = run(...args);
          // The after-step runs while this operation still holds the store's right, so it is a
          // continuation of the operation rather than a second one - and never a caller's hook.
          if (typeof after === 'function') after(result);
          return result;
        } finally {
          drainWaiting();
          settleOverflow();
        }
      },
      refusalShape,
    );
    return (...args) => {
      const tookTheRight = wiring.inChange() !== true;
      try {
        return guarded(...args);
      } finally {
        if (tookTheRight) operationEnded();
      }
    };
  };
  const closeInternal = api.close;
  api.feed = window('structure.feed', feedAndHeal);
  api.accept = window('structure.accept', acceptInternal);
  api.redeliverPending = window('structure.redeliverPending', redeliverPendingInternal, (refusal) => ({
    applied: 0,
    refused: true,
    code: refusal.code,
    reason: refusal.reason,
  }));
  api.resume = window(
    'structure.resume',
    resumeInternal,
    (refusal) => ({
      delivered: 0,
      refused: true,
      code: refusal.code,
      reason: refusal.reason,
    }),
    () => {
      // Recovery is the other moment the retention bound is re-applied: what could not be delivered and
      // is now too old is declared missing. There is deliberately no spool walk here - re-injecting
      // spilled frames at startup is a later set's job, and the automatic drain already happens the
      // moment reception writes its first frame successfully.
      sweepRetentionInternal();
    },
  );
  api.drainSpool = window('structure.drainSpool', drainSpoolInternal, (refusal) => ({
    walked: 0,
    consumed: 0,
    advanced: false,
    swept: 0,
    refused: true,
    stopped: true,
    stoppedCode: 'refused',
    code: refusal.code,
    reason: refusal.reason,
  }));
  api.start = window('structure.start', startInternal, (refusal) => ({
    started: false,
    code: refusal.code,
    reason: refusal.reason,
  }));
  api.restore = window('structure.restore', restoreInternal, (refusal) => ({
    restored: false,
    code: refusal.code,
    reason: refusal.reason,
  }));
  api.beginRun = window('structure.beginRun', beginRunInternal, (refusal) => ({
    begun: false,
    code: refusal.code,
    reason: refusal.reason,
  }));
  api.completeRun = window('structure.completeRun', completeRunInternal, (refusal) => ({
    completed: false,
    code: refusal.code,
    reason: refusal.reason,
  }));
  api.stop = window('structure.stop', stopInternal);
  api.close = window('structure.close', closeInternal);

  // A restart resumes here, and not only in start(): what the raw holds and the board does not has to reach
  // the book before anything else can accept a connection. Those frames are also what fixes the ceiling on
  // where a connection's numbering may start - a completion above a frame already written down is a
  // completion that skips it (§2.2) - so resuming in start() alone leaves the window open to any caller that
  // accepts first, which is exactly the order a recovery is written in. It is driven through the guarded
  // name, so the recovery holds the store while it runs: a hook it calls cannot start a second operation and
  // hand the same frame to the same module twice.
  //
  // The entry point drives the startup sequence itself (begin the run, restore, drain, redeliver) and asks
  // for the recovery to be deferred: a recovery that ran here would restore the board before the run was
  // marked running, which is the order the sequence exists to fix.
  if (!deferRecovery) constructing(() => api.resume());

  // The parts themselves, for the wiring: a test that drives a part directly (a book's own accept, a
  // ledger's own record) goes through this, and nothing a caller is handed reaches it.
  bindInternals(api, { book, ledger, organizer, connection, spool });

  return api;
}
