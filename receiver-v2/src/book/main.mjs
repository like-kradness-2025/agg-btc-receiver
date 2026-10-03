/**
 * The book process entrance: the board extracted so it can run as its own process and speak to
 * organize over IPC (three-process design, docs/fix-plan-sets.md §5.8, rulings ②③④⑤⑫).
 *
 * This module is stage 4 of the split. It does not change the single-process path - `bin/receiver.mjs`,
 * the supervisor, `structure.mjs` and the other roles are untouched and keep their routes until stage 5
 * rewires them. What this adds is a second, independent way to run the book: a process whose store is
 * its own, which owns the board (the levels, the applied boundary, the board-side anchor, the missing
 * records, the gaps, the retired runs, the connection identities, the legacy-owner marker and the
 * invalidation record), and whose only way to reach the rest of the structure is a channel from
 * `src/ipc.mjs`.
 *
 * What the book owns (ruling ②) and where each is:
 *
 *   - `applied_boundary`     the position, the owner and the connection origin (the state module).
 *   - `book_level`           the levels themselves.
 *   - `board_anchor`         the board's own position, written in the same transaction as the levels
 *                            and the boundary (ruling ③, §9.4(1) is met on this side).
 *   - `book_missing_record`  a range of the followed connection that is gone (the state module).
 *   - `book_gap`             a hole seen in the received order (the state module).
 *   - `retired_run`          runs already replaced (the state module).
 *   - `connection_identity`  which identity a connection name was accepted as (the state module).
 *   - `legacy_owner`         a board whose row predates the ownership columns (the state module).
 *   - `book_invalidation`    the loss protocol's record: the request, its range, its monotonic revision
 *                            and the fact that serving stopped - persisted with the missing record.
 *
 * What the book deliberately does not own: `run_marker`, `pending_boundary`, `suspected_gap`,
 * `organized_watermark`, `organize_gap`, `delivery_ledger` and the raw (organize), and `received_tail`
 * and the spool (ingest). There is no run marker here and no receive tail; the book is told a boundary
 * was applied by sending an `applied_ack`, and it never re-derives durability from anything it does not
 * hold.
 *
 * The three things this entrance fixes:
 *   1. the accept is authorized here (ruling ⑫): the owner, the generation ordering, an explicit
 *      takeover and the refusal of an old instance are the book's verdict, and it answers `accepted`.
 *   2. the loss is persisted before it is answered (rulings ④⑤): an `invalidate` stops serving and is
 *      written down - with its revision - in one transaction, then answered `invalidated`; a duplicate
 *      is a no-op, and a confirmed loss may not be taken back.
 *   3. an applied boundary is announced (ruling ⑥): after the board applies, the book sends
 *      `applied_ack`, so organize can clear the pending boundary and release the ledger.
 *
 * The accept's own persistence (ownership, boundary and anchor in one transaction) and the record-first
 * startup comparison (§9.4(1)) live in `src/book/state.mjs` unchanged, and are exercised through this
 * process rather than re-implemented.
 */

import { connect } from '../ipc.mjs';
import { IPC_VERSION, makeMessage } from '../ipc-message.mjs';
import { readChanges } from '../changes.mjs';
import { internalsOf } from '../internal/wiring.mjs';
import { openBook } from './state.mjs';
import { INVALIDATION_INVALIDATED, openBookStore } from './store.mjs';

const ORGANIZE = 'organize';

/**
 * Build the book process around its own store. The store may be passed in (a test), or opened from a
 * path. The organize channel is attached later - by `openBookProcess` when it connects, or by a test -
 * so construction stays synchronous.
 */
export function createBookProcess({
  market,
  stream = 'trades',
  runId,
  adapter = null,
  store = null,
  storePath = null,
  organizeChannel = null,
  roleInstance = null,
  nowMs = () => Date.now(),
  // Frames carry their own derived level changes (ruling ③): ingest computes them with the venue
  // adapter and writes them into the envelope's `meta`, so the book reads the block off the frame and
  // never asks an adapter of its own (the supervisor holds no adapter either). A frame with no valid
  // block is refused - a missing derivation is not read as an empty change.
  channelOptions = {},
  onDiagnostic = () => {},
  onApplied = () => {},
  onInvalidated = () => {},
  onStop = () => {},
} = {}) {
  if (!market || !stream) throw new TypeError('the book process needs a market and a stream');
  if (!runId) throw new TypeError('the book process needs a run id');
  const instance = roleInstance ?? `book-${runId}`;

  const openedStoreHere = store === null;
  const bookStore = store ?? (storePath ? openBookStore({ path: storePath, nowMs }) : null);
  if (bookStore === null) throw new TypeError('the book process needs a store (a path or an open store)');
  const wiring = internalsOf(bookStore);

  // Opening the book writes to the store, so it is one change operation of the process itself. The book
  // creates its own tables (state.mjs) and performs the record-first comparison (§9.4(1)) here: a store
  // whose boundary and anchor disagree is refused before the process can serve anything. A store this
  // process opened itself is closed again if the book refuses it, so a refused open leaves no handle
  // holding the file.
  let book;
  try {
    book = openBook({ market, stream, durability: bookStore, adapter, nowMs, onDiagnostic });
  } catch (error) {
    if (openedStoreHere) {
      try {
        internalsOf(bookStore).close();
      } catch {
        // the refusal is the fact to report
      }
    }
    throw error;
  }
  const bookInternal = internalsOf(book);

  let organizeRef = organizeChannel;
  let stopped = false;
  let closed = false;
  let framesApplied = 0;
  let framesRefused = 0;
  let appliedAcksSent = 0;
  const invalidationsAnswered = [];

  const invalidationByRequest = wiring.db.prepare('SELECT * FROM book_invalidation WHERE request_id = ?');
  const insertInvalidation = wiring.db.prepare(
    `INSERT OR REPLACE INTO book_invalidation
       (request_id, market, stream, connection_id, missing_from, missing_to, revision, state, reason, requested_at_ms, invalidated_at_ms)
     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
  );
  const invalidationsForBoard = wiring.db.prepare(
    'SELECT * FROM book_invalidation WHERE market = ? AND stream = ? ORDER BY requested_at_ms, request_id',
  );

  function diagnostic(reason) {
    try {
      onDiagnostic({ market, stream, reason });
    } catch {
      // a diagnostic is best-effort by contract
    }
  }

  function sendControl(message) {
    if (organizeRef === null) return false;
    try {
      return organizeRef.sendControl(message);
    } catch (error) {
      diagnostic(`a control message could not be sent: ${error.message}`);
      return false;
    }
  }

  // -------------------------------------------------------------------------------------------
  // hello and role attachment
  // -------------------------------------------------------------------------------------------

  function announceHello() {
    return sendControl(
      makeMessage({
        version: IPC_VERSION,
        type: 'hello',
        role_instance: instance,
        run_id: runId,
        payload: { role: 'book' },
      }),
    );
  }

  function setOrganize(channel) {
    // A control callback that carries no channel (the connected process already knows its link) must not
    // erase the one it has: only a real channel replaces it.
    if (channel !== undefined && channel !== null) organizeRef = channel;
  }

  // -------------------------------------------------------------------------------------------
  // accept: the authorization verdict is the book's (ruling ⑫)
  // -------------------------------------------------------------------------------------------

  function replyAccepted(message, { accepted, reason }) {
    // The answer carries the identity the accept named plus this book's board, so the role that adopts
    // (organize) and the role that opens the socket (ingest) can act on it without inventing anything.
    return sendControl(
      makeMessage({
        version: IPC_VERSION,
        type: 'accepted',
        role_instance: instance,
        request_id: message.request_id,
        run_id: message.run_id ?? runId,
        market,
        stream,
        connection_id: message.connection_id,
        generation: message.generation,
        payload: {
          accepted,
          reason,
          first_seq: message.payload?.first_seq ?? 1,
          takeover: message.payload?.takeover === true,
        },
      }),
    );
  }

  function handleAccept(message) {
    const connectionId = message.connection_id;
    if (typeof connectionId !== 'string' || connectionId.length === 0) {
      replyAccepted(message, { accepted: false, reason: 'an acceptance must name a connection' });
      return { accepted: false, reason: 'an acceptance must name a connection' };
    }
    const firstSeq = message.payload?.first_seq;
    // The verdict is entirely the state module's: owner/generation/明示 takeover/旧 instance 拒否. This
    // process only carries the answer back on the wire - it does not soften or second-guess it.
    const outcome = book.accept(connectionId, {
      generation: message.generation ?? null,
      firstSeq: Number.isInteger(firstSeq) ? firstSeq : null,
      runId: message.run_id ?? null,
      takeover: message.payload?.takeover === true,
    });
    replyAccepted(message, outcome);
    return outcome;
  }

  // -------------------------------------------------------------------------------------------
  // invalidate: serving stops and the loss is persisted before it is answered (rulings ④⑤)
  // -------------------------------------------------------------------------------------------

  function replyInvalidated(message, { revision = null, noop = false }) {
    return sendControl(
      makeMessage({
        version: IPC_VERSION,
        type: 'invalidated',
        role_instance: instance,
        request_id: message.request_id,
        run_id: message.run_id ?? runId,
        connection_id: message.connection_id,
        generation: message.generation ?? 0,
        payload: { revision, noop },
      }),
    );
  }

  /**
   * A range of a connection was declared missing. The offering stops and the loss is written down, in
   * one transaction: the invalidation record (with its revision) and the missing record that stops the
   * board are committed together, and only then does the in-memory proof follow the commit - the same
   * discipline as every durable change here. A duplicate request is a no-op: it is answered the same
   * way and changes nothing.
   */
  function handleInvalidate(message) {
    const requestId = message.request_id;
    const connectionId = message.connection_id;
    const existing = invalidationByRequest.get(requestId);
    if (existing !== undefined) {
      replyInvalidated(message, { revision: existing.revision, noop: true });
      return { invalidated: true, noop: true, requestId, revision: existing.revision };
    }
    const revision = Number.isInteger(message.payload?.revision) ? message.payload.revision : null;
    const from = Number.isInteger(message.payload?.from) ? message.payload.from : null;
    const to = Number.isInteger(message.payload?.to) ? message.payload.to : null;
    const reason = message.payload?.reason ?? 'a range of this connection was declared missing';
    const stampedAt = nowMs();
    wiring.inTransaction(() => {
      insertInvalidation.run(
        requestId,
        market,
        stream,
        connectionId,
        from,
        to,
        revision,
        INVALIDATION_INVALIDATED,
        reason,
        message.payload?.requested_at_ms ?? stampedAt,
        stampedAt,
      );
      // The book's own act, in the same transaction: the missing record the proof reads back. The
      // in-memory break follows the commit, because a rollback takes a write back and cannot take a
      // memory fact with it.
      bookInternal.declareMissing(connectionId, reason);
    });
    // Only after the commit does the board stop serving for this connection. A loss of another
    // connection is history and must not stop the board running now (C11).
    const dropped = bookInternal.dropProof(connectionId);
    replyInvalidated(message, { revision });
    const record = {
      requestId,
      connectionId,
      from,
      to,
      revision,
      reason,
      servingStopped: dropped.dropped === true,
    };
    invalidationsAnswered.push(record);
    try {
      onInvalidated(record);
    } catch {
      // an announcement that throws is not a fact about the loss
    }
    return { invalidated: true, noop: false, ...record };
  }

  /**
   * Refuse to take a confirmed missing back (ruling ⑤). The book persists a loss as already-declared
   * and answered, so every recorded invalidation is a confirmed one: a compensation that would cancel
   * it is not a state this protocol allows, and it is refused here as well as on organize's side.
   */
  function cancelInvalidation(requestId) {
    const row = invalidationByRequest.get(requestId);
    if (row === undefined) return { cancelled: false, reason: 'no such invalidation' };
    return {
      cancelled: false,
      refused: true,
      reason: 'a confirmed missing may not be cancelled',
      requestId,
      revision: row.revision,
    };
  }

  function invalidationRecords() {
    return invalidationsForBoard.all(market, stream).map((row) => ({
      requestId: row.request_id,
      connectionId: row.connection_id,
      from: row.missing_from,
      to: row.missing_to,
      revision: row.revision,
      state: row.state,
      reason: row.reason,
      requestedAtMs: row.requested_at_ms,
      invalidatedAtMs: row.invalidated_at_ms,
    }));
  }

  // -------------------------------------------------------------------------------------------
  // applied frames and the applied_ack (ruling ⑥)
  // -------------------------------------------------------------------------------------------

  function sendAppliedAck(envelope) {
    const applied = book.appliedBoundary;
    const connectionId = applied.connectionId ?? envelope.connection_id;
    const generation = applied.generation ?? envelope.generation ?? 0;
    const runIdOut = applied.runId ?? envelope.run_id ?? runId;
    const sent = sendControl(
      makeMessage({
        version: IPC_VERSION,
        type: 'applied_ack',
        role_instance: instance,
        run_id: runIdOut,
        market,
        stream,
        connection_id: connectionId,
        generation,
        payload: { up_to_seq: applied.upToSeq },
      }),
    );
    if (sent) {
      appliedAcksSent += 1;
      try {
        onApplied({ connectionId, upToSeq: applied.upToSeq });
      } catch {
        // an announcement that throws is not a fact about the frame
      }
    }
    return sent;
  }

  function handleEnvelope(envelope, channel) {
    if (channel !== undefined) setOrganize(channel);
    if (closed) return { applied: false, reason: 'this book process is closed' };
    if (stopped) return { applied: false, reason: 'this book process has stopped' };
    const read = readChanges(envelope);
    if (read.ok !== true) {
      framesRefused += 1;
      // A frame with no usable level-changes block is refused outright: applying it as an empty change
      // would silently drop the levels it carried.
      return { applied: false, reason: `the frame's level changes were refused: ${read.reason}` };
    }
    const changes = read.replace ? { replace: true, levels: read.levels } : { replace: false, changes: read.changes };
    const result = book.apply({ envelope, changes });
    if (result.applied === true) {
      framesApplied += 1;
      sendAppliedAck(envelope);
    } else {
      framesRefused += 1;
      // A duplicate is still a fact organize needs: the boundary it owes is already on the board, and
      // the acknowledgement is what lets the pending boundary be cleared. Only the reasons that mean
      // "this is ours and it is durable" are acknowledged.
      if (result.reason === 'already applied' && book.appliedBoundary.connectionId !== null) {
        sendAppliedAck(envelope);
      }
    }
    return result;
  }

  // -------------------------------------------------------------------------------------------
  // control routing and the stop
  // -------------------------------------------------------------------------------------------

  function handleStop(message) {
    if (stopped || closed) return { stopped: false, reason: 'this book process has already stopped' };
    stopped = true;
    try {
      onStop({ market, stream, reason: 'a stop was requested over IPC' });
    } catch {
      // a stop notification is best-effort; the state is the fact
    }
    if (message && message.request_id !== undefined) {
      sendControl(
        makeMessage({
          version: IPC_VERSION,
          type: 'stopped',
          role_instance: instance,
          request_id: message.request_id,
        }),
      );
    }
    return { stopped: true };
  }

  function handleControl(message, channel) {
    switch (message?.type) {
      case 'hello':
        if (message.payload?.role === ORGANIZE) setOrganize(channel);
        return { role: message.payload?.role ?? null };
      case 'accept':
        setOrganize(channel);
        return handleAccept(message);
      case 'invalidate':
        setOrganize(channel);
        return handleInvalidate(message);
      case 'resend':
        setOrganize(channel);
        // Frames owed to the book are re-offered by the delivery path in a later stage; recording the
        // request here keeps it from being silently dropped.
        return { resend: true };
      case 'readiness':
        return { readiness: message.payload ?? null };
      case 'error':
        diagnostic(`a peer reported an error: ${message.payload?.reason ?? 'unknown'}`);
        return { seen: true };
      case 'stop':
        return handleStop(message);
      default:
        return undefined;
    }
  }

  function handleError(error, channel) {
    diagnostic(`a peer channel failed: ${error?.message ?? error}`);
    if (channel === organizeRef) organizeRef = null;
    return { error: true };
  }

  /** Attach (or replace) the organize channel. A process that started before its link may do this. */
  function attachOrganize(channel) {
    setOrganize(channel);
    announceHello();
    return true;
  }

  const api = {
    market,
    stream,
    roleInstance: instance,
    runId,
    /** The book state itself, for a caller that must read the board or the proof. */
    book,

    handleControl,
    handleEnvelope,
    handleError,
    attachOrganize,
    announceHello,
    /** Stop serving, for the supervisor's processing stop. Returns the stop result the run must confirm. */
    stop: () => handleStop(undefined),

    cancelInvalidation,
    invalidationRecords,

    get phase() {
      return book.phase;
    },
    get isRunning() {
      return book.isRunning;
    },
    get appliedBoundary() {
      return book.appliedBoundary;
    },
    get board() {
      return book.board;
    },
    proveBoundary: () => book.proveBoundary(),
    openGaps: () => book.openGaps(),
    retiredRuns: () => book.retiredRuns(),

    get stopped() {
      return stopped;
    },
    get stats() {
      return {
        market,
        stream,
        roleInstance: instance,
        connectionId: book.appliedBoundary.connectionId,
        phase: book.phase,
        framesApplied,
        framesRefused,
        appliedAcksSent,
        invalidations: invalidationsAnswered.length,
        stopped,
        closed,
      };
    },

    close() {
      if (closed) return;
      closed = true;
      stopped = true;
      if (organizeRef !== null) {
        try {
          organizeRef.close();
        } catch {
          // the socket may already be gone
        }
      }
      if (openedStoreHere) {
        try {
          internalsOf(bookStore).close();
        } catch {
          // a store that will not close is not this close's failure to report
        }
      }
    },
  };

  return api;
}

/**
 * Open the book process against an organize socket path: this is the process entrance the supervisor
 * will call in stage 5 (and the tests call now). The book connects to organize - the same direction as
 * ingest - announces itself with `hello`, and speaks the stage-1 vocabulary. The channel is connected
 * first, so construction has a live link and the announcement goes out immediately.
 */
export async function openBookProcess({ organizeSocketPath = null, channelOptions = {}, ...rest } = {}) {
  let process = null;
  const channel = organizeSocketPath
    ? await connect(organizeSocketPath, {
        ...channelOptions,
        onControl: (message) => process?.handleControl(message),
        onEnvelope: (envelope) => process?.handleEnvelope(envelope),
        onError: (error) => process?.handleError(error),
      })
    : null;
  process = createBookProcess({ ...rest, organizeChannel: channel });
  if (channel !== null) process.announceHello();
  return process;
}
