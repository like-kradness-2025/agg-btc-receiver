/**
 * The organize process entrance: organization extracted so it can run as its own process and speak to
 * ingest and the book over IPC (three-process design, docs/fix-plan-sets.md §5.8, rulings ②③④⑤⑦⑧⑨⑩).
 *
 * This module is stage 3 of the split. It does not change the single-process path - `bin/receiver.mjs`,
 * the supervisor, `structure.mjs` and `src/book` are untouched and keep their routes until stage 5
 * rewires them. What this adds is a second, independent way to run organization: a process whose store
 * is its own, which owns the run marker, the watermark, the delivery ledger, the pending boundary and
 * the raw, and whose only way to reach the rest of the structure is a channel from `src/ipc.mjs`.
 *
 * What organize owns (ruling ②) and where each is here:
 *
 *   - `run_marker`          this process's own store (`beginRun` / `prepareFinalize` / `finalize`, ruling ⑧).
 *                           Startup invalidates the previous running marker; completion carries the
 *                           immutable supervisor request and its receipt in this same run row.
 *   - `organized_watermark` the contiguous durable ceiling per connection (the organizer module).
 *   - `delivery_ledger`     the exact confirmed frames still owed to the book (the delivery module).
 *   - the raw               an optional `writeRaw` hook; only its `true` makes a frame durable. When
 *                           it is absent the raw stage is skipped (`rawSkipped`) exactly as the
 *                           organizer and the entrance already prescribe (ruling ⑥, §5.7).
 *
 * What organize deliberately does not own: the spool and `received_tail`, which belong to ingest
 * (ruling ⑦). There is no spool here, and no receive tail: frames are received from ingest over IPC,
 * made durable, acknowledged, and the durability of a frame is never re-derived from a spool.
 *
 * The three orderings that matter and where they are enforced here:
 *   1. durability first, acknowledgement last: the raw write (fsync) happens before the store's
 *      transaction, and the `durable_ack` is sent only after that transaction committed (ruling ⑦).
 *   2. one transaction: the watermark, the ledger confirmation and the pending boundary are written
 *      inside the same organizer transaction (ruling ③), so a crash cannot separate them.
 *   3. a loss is a request and a round trip: a missing range is persisted as an invalidation request
 *      (stable id, connection, range, monotonic revision), the book is asked, and only its
 *      `invalidated` response (or the supervisor's confirmation that the book ended) confirms the
 *      loss. A duplicate request is a no-op; a confirmed loss may not be cancelled (rulings ④⑤).
 *
 * "All acknowledged" is judged here (rulings ⑨⑩): the sealed final tails of every connection - old
 * generations included - must be reached by a hole-less durable ceiling, with no unprocessed spool. A
 * Explicit legacy finalization is gated by this judgement. Ordinary stop never invokes it.
 */

import net from 'node:net';

import { createChannel, connect } from '../ipc.mjs';
import { IPC_VERSION, makeMessage } from '../ipc-message.mjs';
import { makeEnvelope } from '../envelope.mjs';
import { internalsOf } from '../internal/wiring.mjs';
import { openOrganizer } from './watermark.mjs';
import { openDeliveryLedger, INTENT, OWED } from '../supervisor/delivery.mjs';
import {
  INVALIDATION_CONFIRMED,
  INVALIDATION_REQUESTED,
  openOrganizeStore,
} from './store.mjs';

// The organizer's reason for a frame that is durable and owed to the board. When there is no raw
// writer, "durable" means the store's own record alone, and the words say so.
const OWED_REASON_RAW = 'durable in the raw and not applied to the board yet';
const OWED_REASON_STORE = 'durable in the store and not applied to the board yet (no raw writer is configured)';

/**
 * The "all acknowledged" judgement (rulings ⑨⑩), as a pure function of the facts it rests on.
 *
 * The condition is: for every connection in the sealed final tail list - all connections, old
 * generations included - a hole-less durable ceiling (the `organized_watermark` contiguous ceiling)
 * has reached that connection's last received sequence; and there is no unprocessed spool and no raw
 * hole. Sending success or a book application acknowledgement may neither stand in for the ceiling nor
 * be substituted for it.
 *
 * `tails` is the final list ingest persisted after it stopped receiving. `ceilings` maps a connection
 * id to `{ upToSeq, firstSeq }` (the persisted watermark row; `upToSeq` null means nothing durable is
 * contiguous yet). `spoolEmpty` says ingest has no unprocessed spool. `holes` are open raw holes. An
 * unknown tail - a missing or empty list - is never a normal completion.
 */
export function judgeAllAcked({ tails, ceilings, spoolEmpty = false, holes = [] } = {}) {
  const unmet = [];
  if (!Array.isArray(tails) || tails.length === 0) {
    return { allAcked: false, reason: 'the sealed final tail list is empty or unknown', unmet: ['tail'] };
  }
  const ceilingOf = ceilings instanceof Map ? (id) => ceilings.get(id) : (id) => ceilings?.[id];
  for (const tail of tails) {
    const connectionId = tail.connectionId ?? tail.connection_id;
    const lastReceivedSeq = tail.lastReceivedSeq ?? tail.last_received_seq;
    const row = ceilingOf(connectionId);
    const upToSeq = row === undefined || row === null ? null : row.upToSeq ?? row.up_to_receive_seq ?? null;
    if (upToSeq === null || upToSeq < lastReceivedSeq) {
      unmet.push({ reason: 'the durable ceiling has not reached the tail', connectionId, upToSeq, lastReceivedSeq });
    }
  }
  if (spoolEmpty !== true) unmet.push({ reason: 'the spool is not known to be empty' });
  for (const hole of holes) unmet.push({ reason: 'a hole remains in the durable ceiling', ...hole });
  return { allAcked: unmet.length === 0, reason: unmet.length === 0 ? 'every tail was reached' : unmet[0].reason, unmet };
}

function canonicalTailIdentity(tails) {
  if (!Array.isArray(tails)) return null;
  const normalized = [];
  for (const tail of tails) {
    if (
      tail === null ||
      typeof tail !== 'object' ||
      Array.isArray(tail) ||
      typeof tail.connectionId !== 'string' ||
      tail.connectionId.length === 0 ||
      !Number.isInteger(tail.lastReceivedSeq) ||
      tail.lastReceivedSeq < 0
    ) {
      return null;
    }
    normalized.push({ connectionId: tail.connectionId, lastReceivedSeq: tail.lastReceivedSeq });
  }
  normalized.sort((left, right) => (left.connectionId < right.connectionId ? -1 : left.connectionId > right.connectionId ? 1 : 0));
  if (new Set(normalized.map((tail) => tail.connectionId)).size !== normalized.length) return null;
  return JSON.stringify(normalized);
}

/**
 * Build the organize process around its own store. The store may be passed in (a test), or opened from
 * a path. The channels are attached later - by `openOrganizeProcess` when it listens, or by a test - so
 * construction stays synchronous.
 */
export function createOrganizeProcess({
  market,
  stream = 'trades',
  runId,
  store = null,
  storePath = null,
  rawWriter = null,
  roleInstance = null,
  nowMs = () => Date.now(),
  channelOptions = {},
  onMissing = () => {},
  onGap = () => {},
  onDiagnostic = () => {},
  onStop = () => {},
  onAck = () => {},
  // Whether this process writes its own `running` marker on construction. The store has already
  // invalidated the previous running run (ruling ⑧); this writes the live one.
  markRunning = true,
  // Stage 5c: the periodic readiness report (ruling ⑬). It travels the ordinary control path to the
  // supervisor's router, which observes it. Disabled (0) by default.
  readinessIntervalMs = 0,
} = {}) {
  if (!market || !stream) throw new TypeError('the organize process needs a market and a stream');
  if (!runId) throw new TypeError('the organize process needs a run id');
  const instance = roleInstance ?? `organize-${runId}`;

  const openedStoreHere = store === null;
  const organizeStore = store ?? (storePath ? openOrganizeStore({ path: storePath, runId, nowMs }) : null);
  if (organizeStore === null) throw new TypeError('the organize process needs a store (a path or an open store)');
  const wiring = internalsOf(organizeStore);

  const hasRawWriter = typeof rawWriter === 'function';

  // Opening the parts writes to the store, so it is one change operation of the process itself; the
  // parts are opened through their own public names, which take the right and refuse a re-entrant open.
  const organizer = openOrganizer({
    market,
    stream,
    durability: organizeStore,
    writeRaw: hasRawWriter ? (envelope) => rawWriter(envelope) === true : null,
    capacity: () => (stopped ? 'stopped' : 'ok'),
    nowMs,
  });
  const ledger = openDeliveryLedger({ market, stream, durability: organizeStore, nowMs });
  const ledgerInternal = internalsOf(ledger);

  let ingestChannel = null;
  let bookChannel = null;
  // What each connection's owed frames have already been offered to the book on the channel that is
  // up now: connectionId -> the set of offered receive_seqs. Entries are tracked one by one - a
  // single high-water mark would call a late frame "offered" because a later one preceded it, and
  // that frame would never reach the book. Cleared when the book channel changes or fails (a new
  // audience has seen nothing) and on an explicit resend request. `owedSweepNeeded` remembers that
  // an offer was blocked: the next frame runs a sweep instead of offering only itself.
  const owedOffered = new Map();
  let owedSweepNeeded = false;
  const channelRoles = new Map();
  // Behind the supervisor, organize has one channel to the router, so a peer cannot be told apart by
  // the channel it arrived on: the message's own type is the routing fact (the router already decided
  // which role may speak it). In that mode both reply routes point at the router, which derives the
  // destination from the message type.
  let routerMode = false;

  let acceptedConnectionId = null;
  let acceptedRunId = null;
  let acceptedGeneration = null;
  let sealedTails = null;
  // The prepared finalize is one immutable barrier. A later, different seal cannot reuse the mutable
  // all-ACK result for its own tails to authorize this older request.
  let preparedBarrier = null;
  let preparedBarrierInvalid = false;
  let allAcked = false;
  let allAckedReason = 'no tail has been sealed yet';
  // The book's recorded applied boundary, handed to organize by the supervisor at startup (b). It is
  // recorded here even when it is empty: a restart whose ledger is empty but whose spool still holds
  // frames must still organize them on the connection the board recorded, and organize learns that
  // somewhere other than a frame. Its absence is never inferred.
  let bookBoundary = null;
  let deliveredFrames = 0;
  let stopped = false;
  let closed = false;
  let serving = true;
  const invalidationsConfirmed = [];
  let framesDurable = 0;
  let framesAlreadyDurable = 0;

  function diagnostic(reason, extra = {}) {
    try {
      onDiagnostic({ market, reason, ...extra });
    } catch {
      // a diagnostic is best-effort by contract
    }
  }

  // Set 1 (observability): every refused frame is counted by reason; a burst is one rate-limited
  // report, and what the window leaves unreported is flushed as the process closes.
  const REFUSAL_REPORT_MIN_MS = 1000;
  const refusalTally = new Map(); // reason -> count
  let refusedFrames = 0;
  let refusalBurst = 0;
  let lastRefusalReason = null;
  let lastRefusedIdentity = null;
  let refusalReportedAtMs = 0;

  function refuseFrame(result, envelope) {
    refusedFrames += 1;
    refusalBurst += 1;
    lastRefusalReason = result.reason;
    lastRefusedIdentity = {
      connection_id: envelope?.connection_id ?? null,
      receive_seq: envelope?.receive_seq ?? null,
    };
    refusalTally.set(result.reason, (refusalTally.get(result.reason) ?? 0) + 1);
    const nowMs = Date.now();
    if (nowMs - refusalReportedAtMs >= REFUSAL_REPORT_MIN_MS) {
      refusalReportedAtMs = nowMs;
      reportRefusalBurst('');
    }
    return result;
  }

  /** Report - and reset - whatever the reporting window is holding back. */
  function reportRefusalBurst(suffix) {
    if (refusalBurst === 0) return;
    const count = refusalBurst;
    refusalBurst = 0;
    diagnostic(`the organize refused ${count} frame${count === 1 ? '' : 's'}${suffix}: ${lastRefusalReason}`, {
      refused: count,
      ...(lastRefusedIdentity ?? {}),
    });
  }

  /** The board's identity check for a frame, before anything is decided about it. */
  function belongsToBoard(envelope) {
    if (envelope.market !== market || envelope.stream !== stream) {
      return { ok: false, reason: 'this frame belongs to another board' };
    }
    if (acceptedConnectionId === null) {
      return { ok: false, reason: 'no connection has been accepted yet' };
    }
    if (envelope.connection_id !== acceptedConnectionId) {
      return { ok: false, reason: 'not the accepted connection' };
    }
    if ((envelope.run_id ?? null) !== acceptedRunId || (envelope.generation ?? null) !== acceptedGeneration) {
      return { ok: false, reason: 'this frame belongs to another run or generation' };
    }
    return { ok: true };
  }



  /**
   * A row remains here until the book's applied_ack releases it. The watermark names the contiguous
   * durable ceiling; the ledger names the exact frames beyond the book's confirmed state. Together they
   * are sufficient to resume after any crash without a duplicate scalar boundary record.
   */
  function recoveryStatus() {
    const owedCount = ledger.pending({ state: OWED }).length;
    return { resolved: owedCount === 0, owedCount };
  }

  /**
   * One received frame, from ingest. Durability first and the acknowledgement last: the intent is
   * written before the raw is touched, the raw is written (fsync) before the transaction, and the
   * transaction writes the watermark and ledger confirmation together. Only after it commits is the
   * `durable_ack` sent (rulings ③⑦).
   */
  function organizeFrame(envelope) {
    if (closed) return refuseFrame({ accepted: false, reason: 'this organize process is closed' }, envelope);
    if (stopped) return refuseFrame({ accepted: false, reason: 'this organize process has stopped' }, envelope);
    const claim = belongsToBoard(envelope);
    if (!claim.ok) return refuseFrame({ accepted: false, reason: claim.reason, ack: null }, envelope);

    const note = organizer.note(envelope, {
      onIntent: (frame) => {
        ledgerInternal.record(frame, hasRawWriter ? OWED_REASON_RAW : OWED_REASON_STORE, INTENT);
      },
      onDurable: (frame) => {
        // The same transaction as the watermark: confirm the ledger row.
        ledgerInternal.confirm(frame);
      },
    });
    if (note.accepted === false) return refuseFrame(note, envelope);

    if (note.durable === true) framesDurable += 1;
    else if (note.alreadyDurable === true) framesAlreadyDurable += 1;

    const ack = note.ack ?? (note.alreadyDurable || note.duplicate ? organizer.currentAck() : null);
    if (ack) {
      sendDurableAck(ack, envelope);
      try {
        onAck(ack);
      } catch {
        // an observation that throws is not a fact about the frame
      }
    }
    // What is durable and owed is handed to the book here (startup (d) covers the recovery case; this
    // covers the ordinary path, so a frame reaches the board without waiting for a resend nobody asked
    // for). A frame the book cannot take yet stays owed - only its `applied_ack` releases it. The
    // ordinary path offers just this frame; a sweep runs only after an offer was blocked.
    if (owedSweepNeeded) {
      const sweep = deliverOwed();
      if (!sweep.blocked) owedSweepNeeded = false;
    } else if (note.durable === true || note.alreadyDurable === true) {
      deliverEntry(envelope);
    }
    return note;
  }

  /**
   * Set 3: restore the acceptance a previous life of this board recorded. Without it, a restart whose
   * spool still holds frames of the accepted connection could never acknowledge them again - the
   * frames would be refused ("no connection has been accepted yet") and the retention that is supposed
   * to guarantee delivery would pin the cursor for ever.
   *
   * The written-down acceptance is what is restored first: it exists from the moment the connection
   * was adopted, even when no frame of it was ever durably organised (the watermark row only appears
   * with the first frame, so a crash between adoption and that first frame is exactly the case it
   * must cover). The most recently updated watermark row is the fallback for a store written before
   * this record existed; the connection name is `run:venue:market:generation`, so the run and the
   * generation can be recovered from it. A store with neither restores nothing.
   */
  function restoreAcceptedConnection() {
    let connectionId = null;
    let runIdRestored = null;
    let generation = null;
    let firstSeq = undefined;
    let acceptance = null;
    let watermark = null;
    try {
      acceptance = wiring.db
        .prepare(
          `SELECT connection_id, run_id, generation, updated_at_ms FROM organize_acceptance
            WHERE market = ? AND stream = ?`,
        )
        .get(market, stream);
      watermark = wiring.db
        .prepare(
          `SELECT connection_id, first_seq, updated_at_ms FROM organized_watermark
            WHERE market = ? AND stream = ? ORDER BY updated_at_ms DESC, rowid DESC LIMIT 1`,
        )
        .get(market, stream);
    } catch (error) {
      diagnostic(`the stored acceptance could not be read: ${error.message}`);
      return null;
    }
    // The newest fact wins. The acceptance is written at adoption, so it covers a crash before the
    // first durable frame; the watermark advances with every organised frame, so it covers an
    // acceptance whose write failed while its frames still made it to the board. A tie keeps the
    // acceptance: at the same instant it reflects the later adoption.
    const acceptanceAt = Number.isInteger(acceptance?.updated_at_ms) ? acceptance.updated_at_ms : null;
    const watermarkAt = Number.isInteger(watermark?.updated_at_ms) ? watermark.updated_at_ms : null;
    if (acceptanceAt !== null && (watermarkAt === null || acceptanceAt >= watermarkAt)) {
      if (typeof acceptance.connection_id === 'string' && acceptance.connection_id.length > 0) {
        connectionId = acceptance.connection_id;
        runIdRestored = acceptance.run_id ?? null;
        generation = Number.isInteger(acceptance.generation) ? acceptance.generation : null;
      }
    } else if (watermarkAt !== null) {
      if (typeof watermark.connection_id === 'string' && watermark.connection_id.length > 0) {
        connectionId = watermark.connection_id;
        firstSeq = watermark.first_seq ?? undefined;
      }
    }
    if (connectionId === null && watermarkAt !== null) {
      // An acceptance row too damaged to name a connection still must not hide a usable watermark.
      if (typeof watermark.connection_id === 'string' && watermark.connection_id.length > 0) {
        connectionId = watermark.connection_id;
        firstSeq = watermark.first_seq ?? undefined;
      }
    }
    if (connectionId === null) return null;
    if (runIdRestored === null || generation === null) {
      const parts = connectionId.split(':');
      if (runIdRestored === null && parts.length >= 4) runIdRestored = parts[0];
      if (generation === null && parts.length >= 4 && Number.isInteger(Number(parts[parts.length - 1]))) {
        generation = Number(parts[parts.length - 1]);
      }
    }
    acceptedConnectionId = connectionId;
    acceptedRunId = runIdRestored;
    acceptedGeneration = generation;
    organizer.accept(connectionId, {
      firstSeq,
      runId: runIdRestored,
      generation: generation ?? undefined,
    });
    diagnostic('the stored acceptance was restored for recovery', {
      connection_id: connectionId,
      run_id: runIdRestored,
      generation,
    });
    return { connectionId, runId: runIdRestored, generation };
  }

  function sendDurableAck(ack, envelope) {
    if (ingestChannel === null) return false;
    try {
      ingestChannel.sendControl(
        makeMessage({
          version: IPC_VERSION,
          type: 'durable_ack',
          role_instance: instance,
          run_id: envelope.run_id ?? runId,
          market,
          stream,
          connection_id: ack.connectionId,
          generation: envelope.generation ?? acceptedGeneration ?? 0,
          payload: { up_to_seq: ack.upToSeq, capacity: ack.capacity ?? (stopped ? 'stopped' : 'ok') },
        }),
      );
      return true;
    } catch (error) {
      diagnostic(`a durable acknowledgement could not be sent: ${error.message}`);
      return false;
    }
  }

  function replyAccepted(message, accepted, reason = '') {
    const channel = ingestChannel;
    if (channel === null) return false;
    try {
      channel.sendControl(
        makeMessage({
          version: IPC_VERSION,
          type: 'accepted',
          role_instance: instance,
          request_id: message.request_id,
          connection_id: message.connection_id,
          generation: message.generation,
          payload: { accepted, reason },
        }),
      );
      return true;
    } catch (error) {
      diagnostic(`an acceptance could not be sent: ${error.message}`);
      return false;
    }
  }

  /**
   * The adoption, and only the adoption (ruling ②). Organize no longer decides an accept: the
   * authorization is the book's, and organize is handed the book's answer by the supervisor. When
   * that answer arrives, organize reflects the adoption and confirms it - and that confirmation is
   * what lets ingest open the socket. A raw `accept` reaching organize is refused: organize is not
   * the place an acceptance is granted, and pretending to grant one here is exactly the unconditional
   * `accepted:true` ruling ② removes.
   */
  function handleAcceptFromPeer(message) {
    diagnostic(
      'an accept arrived at organize; the book authorizes an accept and organize only adopts what the supervisor relays',
    );
    return { accepted: false, reason: 'organize does not authorize an accept' };
  }

  /**
   * The book authorized the connection (relayed by the supervisor). Organize reflects the adoption in
   * its organizer and confirms it. The confirmation carries the request identity so the acceptance can
   * be matched back to the ingest that asked, and it is not an authorization of its own - it is the
   * adoption's completion.
   */
  function adoptConnection(message) {
    if (message.payload?.accepted === false) {
      // The book refused; organize adopts nothing and the refusal travels on to ingest.
      return replyAccepted(message, false, message.payload?.reason ?? 'the book refused the connection');
    }
    if (!message.connection_id || message.connection_id.length === 0) {
      return replyAccepted(message, false, 'an adoption must name a connection');
    }
    const firstSeq = message.payload?.first_seq;
    acceptedConnectionId = message.connection_id;
    acceptedRunId = message.run_id ?? null;
    acceptedGeneration = message.generation ?? 0;
    organizer.accept(message.connection_id, {
      firstSeq: Number.isInteger(firstSeq) ? firstSeq : undefined,
      runId: acceptedRunId,
      generation: acceptedGeneration,
    });
    // Set 3: the acceptance itself is written down the moment it is made. The watermark row only
    // appears with the first durably organised frame, so a crash in between would otherwise lose
    // the fact that this connection was adopted - and its retained frames could never be
    // acknowledged after the restart. The record is what makes the confirmation true, so a write
    // that fails is not confirmed: the adoption stays in memory (its frames still flow), and the
    // requester hears the refusal - an unmade record stops the ingest loudly rather than passing
    // as confirmed, and the restart re-runs the adoption and the write. An acceptance the next
    // life could not read must never be reported as accepted.
    try {
      wiring.db
        .prepare(
          `INSERT OR REPLACE INTO organize_acceptance
             (market, stream, connection_id, run_id, generation, updated_at_ms)
           VALUES (?, ?, ?, ?, ?, ?)`,
        )
        .run(market, stream, acceptedConnectionId, acceptedRunId, acceptedGeneration, nowMs());
    } catch (error) {
      diagnostic(`the acceptance could not be written down: ${error.message}`);
      return replyAccepted(message, false, `the acceptance could not be written down: ${error.message}`);
    }
    replyAccepted(message, true, '');
    // Now that the connection is adopted, any owed frame from a previous life can reach the board.
    deliverOwed();
    return { accepted: true, connectionId: acceptedConnectionId };
  }

  /** The sealed final tails: the fact the all-acknowledged judgement runs on. */
  function handleTailSealed(message) {
    const tails = message.payload?.tails;
    const spoolEmpty = message.payload?.spool_empty;
    const sealedIdentity = canonicalTailIdentity(tails);
    if (preparedBarrier !== null && sealedIdentity !== preparedBarrier.tailIdentity) {
      preparedBarrierInvalid = true;
    }
    sealedTails = Array.isArray(tails) ? tails : null;
    const verdict = evaluateAllAcked({ tails, spoolEmpty });
    allAcked = verdict.allAcked;
    allAckedReason = verdict.reason;
    if (allAcked) sendDrained();
    return verdict;
  }

  function sendDrained() {
    if (ingestChannel === null) return false;
    try {
      ingestChannel.sendControl(
        makeMessage({
          version: IPC_VERSION,
          type: 'drained',
          role_instance: instance,
          run_id: runId,
          payload: { tails: sealedTails, reason: allAckedReason },
        }),
      );
      return true;
    } catch (error) {
      diagnostic(`the drained report could not be sent: ${error.message}`);
      return false;
    }
  }

  /** Read the persisted ceilings and open holes, then judge. */
  function evaluateAllAcked({ tails = sealedTails, spoolEmpty = null } = {}) {
    const ceilings = new Map();
    for (const row of wiring.db
      .prepare(
        `SELECT connection_id, up_to_receive_seq, first_seq FROM organized_watermark
          WHERE market = ? AND stream = ?`,
      )
      .all(market, stream)) {
      ceilings.set(row.connection_id, { upToSeq: row.up_to_receive_seq ?? null, firstSeq: row.first_seq ?? null });
    }
    const holes = wiring.db
      .prepare(
        `SELECT connection_id, missing_from, missing_to FROM organize_gap
          WHERE market = ? AND stream = ? AND filled_at_ms IS NULL`,
      )
      .all(market, stream)
      .map((row) => ({ connectionId: row.connection_id, from: row.missing_from, to: row.missing_to }));
    return judgeAllAcked({ tails, ceilings, spoolEmpty: spoolEmpty === true, holes });
  }

  /**
   * The book reports that it applied a boundary. Release only ledger entries the raw also holds contiguously;
   * an entry above that position remains the sole durable record of its frame.
   */
  function handleAppliedAck(message) {
    const connectionId = message.connection_id ?? acceptedConnectionId;
    const applied = message.payload?.up_to_seq;
    if (!Number.isInteger(applied)) return { applied: false, reason: 'an applied acknowledgement needs a ceiling' };
    let releasedUpTo = null;
    const result = wiring.inTransaction(() => {
      const row = wiring.db
        .prepare(
          `SELECT up_to_receive_seq, first_seq FROM organized_watermark
            WHERE connection_id = ? AND market = ? AND stream = ?`,
        )
        .get(connectionId, market, stream);
      let released = 0;
      if (row && row.up_to_receive_seq !== null && Number.isInteger(row.first_seq)) {
        releasedUpTo = Math.min(applied, row.up_to_receive_seq);
        released = ledgerInternal.release({
          connectionId,
          firstSeq: row.first_seq,
          upToSeq: releasedUpTo,
        }).released;
      }
      return { released };
    });
    // What the board has applied needs no further offer: the memory of it goes with the entries.
    if (releasedUpTo !== null) pruneOffers(connectionId, releasedUpTo);
    return { applied: true, ...result };
  }

  // ---------------------------------------------------------------------------------------------
  // Invalidation: a loss is a persisted request and a round trip (rulings ④⑤).
  // ---------------------------------------------------------------------------------------------

  function requestRow(requestId) {
    const row = wiring.db.prepare('SELECT * FROM invalidation_request WHERE request_id = ?').get(requestId);
    return row === undefined ? null : row;
  }

  function invalidationRequests({ state = null } = {}) {
    const rows = state
      ? wiring.db
          .prepare(
            'SELECT * FROM invalidation_request WHERE market = ? AND stream = ? AND state = ? ORDER BY revision',
          )
          .all(market, stream, state)
      : wiring.db
          .prepare('SELECT * FROM invalidation_request WHERE market = ? AND stream = ? ORDER BY revision')
          .all(market, stream);
    return rows.map((row) => ({
      requestId: row.request_id,
      connectionId: row.connection_id,
      from: row.missing_from,
      to: row.missing_to,
      revision: row.revision,
      state: row.state,
      reason: row.reason,
      requestedAtMs: row.requested_at_ms,
      confirmedAtMs: row.confirmed_at_ms ?? null,
      confirmedBy: row.confirmed_by ?? null,
    }));
  }

  /**
   * Persist an invalidation request for a range that must be declared missing, and ask the book.
   *
   * The request carries a stable id (derived from the board, the connection and the range), the target
   * connection, the range and a monotonic revision. A request that already exists for the same range is
   * a no-op: the revision does not move, and the book is not asked again (ruling ④).
   */
  function requestInvalidation({ connectionId, from, to, reason = 'a range must be declared missing' }) {
    if (!Number.isInteger(from) || !Number.isInteger(to)) {
      throw new TypeError('an invalidation request needs an integer range');
    }
    const stableId = `${market}:${stream}:${connectionId}:${from}-${to}`;
    const existing = requestRow(stableId);
    if (existing !== null) {
      return { created: false, noop: true, requestId: stableId, state: existing.state, revision: existing.revision };
    }
    const request = wiring.inTransaction(() => {
      const row = wiring.db
        .prepare('SELECT MAX(revision) AS top FROM invalidation_request WHERE market = ? AND stream = ?')
        .get(market, stream);
      const revision = (row?.top ?? 0) + 1;
      wiring.db
        .prepare(
          `INSERT INTO invalidation_request
             (request_id, market, stream, connection_id, missing_from, missing_to, revision, state, reason, requested_at_ms)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
        )
        .run(stableId, market, stream, connectionId, from, to, revision, INVALIDATION_REQUESTED, reason, nowMs());
      return { requestId: stableId, connectionId, from, to, revision, state: INVALIDATION_REQUESTED, reason };
    });
    announceInvalidate(request);
    return { created: true, ...request };
  }

  /** Ask the book to stop serving and persist the invalidation. */
  function announceInvalidate(request) {
    if (bookChannel === null) return false;
    try {
      bookChannel.sendControl(
        makeMessage({
          version: IPC_VERSION,
          type: 'invalidate',
          role_instance: instance,
          request_id: request.requestId,
          run_id: runId,
          connection_id: request.connectionId,
          generation: acceptedGeneration ?? 0,
          payload: { from: request.from, to: request.to, revision: request.revision, reason: request.reason },
        }),
      );
      return true;
    } catch (error) {
      diagnostic(`an invalidation request could not be sent: ${error.message}`);
      return false;
    }
  }

  /**
   * Confirm a requested invalidation. Confirmation is the missing becoming durable - the row moves to
   * `confirmed` in one transaction, and only then is the loss announced. A response for an unknown or
   * already-confirmed request is a no-op (a duplicate response changes nothing).
   */
  function confirmInvalidation(requestId, { by = 'book', reason = null } = {}) {
    const row = requestRow(requestId);
    if (row === null) return { confirmed: false, reason: 'no such invalidation request' };
    if (row.state === INVALIDATION_CONFIRMED) return { confirmed: false, noop: true, reason: 'already confirmed' };
    wiring.inTransaction(() => {
      wiring.db
        .prepare(
          `UPDATE invalidation_request
              SET state = ?, confirmed_at_ms = ?, confirmed_by = ?
            WHERE request_id = ? AND state = ?`,
        )
        .run(INVALIDATION_CONFIRMED, nowMs(), by, requestId, INVALIDATION_REQUESTED);
    });
    const confirmed = { requestId, connectionId: row.connection_id, from: row.missing_from, to: row.missing_to, revision: row.revision, by };
    invalidationsConfirmed.push(confirmed);
    try {
      onMissing(confirmed);
    } catch {
      // the announcement is best-effort; the confirmed fact is durable
    }
    return { confirmed: true, ...confirmed };
  }

  function handleInvalidated(message) {
    return confirmInvalidation(message.request_id, { by: 'book' });
  }

  /**
   * The alternative condition (ruling ⑤): when the book cannot answer, the supervisor's confirmation
   * that the book ended stands in for its response. This is the stage-5 connection point; the state
   * machine and the switch are fixed here and driven by a fake book in the tests.
   */
  function confirmInvalidationBySupervisor(requestId, reason = 'the book was confirmed ended by the supervisor') {
    return confirmInvalidation(requestId, { by: 'supervisor', reason });
  }

  /**
   * Refuse to take a confirmed missing back (ruling ⑤). A compensation that would cancel a loss already
   * confirmed is not a state this protocol allows. A request that was never confirmed may be withdrawn.
   */
  function cancelInvalidation(requestId) {
    const row = requestRow(requestId);
    if (row === null) return { cancelled: false, reason: 'no such invalidation request' };
    if (row.state === INVALIDATION_CONFIRMED) {
      return { cancelled: false, refused: true, reason: 'a confirmed missing may not be cancelled' };
    }
    wiring.db
      .prepare('DELETE FROM invalidation_request WHERE request_id = ? AND state = ?')
      .run(requestId, INVALIDATION_REQUESTED);
    return { cancelled: true, requestId };
  }

  /** On start, re-derive the outstanding requests: a request the book never answered is asked again. */
  function rederiveInvalidations() {
    const outstanding = invalidationRequests({ state: INVALIDATION_REQUESTED });
    for (const request of outstanding) announceInvalidate(request);
    serving = outstanding.length === 0 ? true : serving;
    return { rederived: outstanding.length };
  }

  // ---------------------------------------------------------------------------------------------
  // The owed-frame delivery body: what is durable and not yet on the board (startup (d), ruling ③).
  // ---------------------------------------------------------------------------------------------

  /** Record the book's applied boundary handed over by the supervisor at startup (b). Never inferred. */
  function resumeFromBoundary(boundary) {
    bookBoundary = boundary ?? null;
    return { recorded: bookBoundary !== null, boundary: bookBoundary };
  }

  /**
   * Hand every confirmed-and-owed frame to the book, in arrival order (startup (d), and the ordinary
   * path as frames become durable). The ledger holds the frame's own bytes and meta, so the envelope is
   * reconstructed from the stored entry, never re-derived - the derived changes block rides along and a
   * recovery delivers the same result a live frame would. Only `owed` entries are delivered; an `intent`
   * may not be durable yet, and a `skipped` frame's fate is already written down. A frame the book
   * refuses (for instance before its connection is adopted) simply stays owed: nothing here releases it,
   * only the book's `applied_ack` does.
   */
  function deliverOwed() {
    if (bookChannel === null) return { delivered: 0, blocked: false, reason: 'no book is connected to organize' };
    let delivered = 0;
    for (const entry of ledger.pending({ state: 'owed' })) {
      const outcome = offerOwedEntry(entry);
      if (outcome === 'blocked') return { delivered, blocked: true };
      if (outcome === 'sent') delivered += 1;
    }
    return { delivered, blocked: false };
  }

  /** The set of receive_seqs this channel has already been offered for one connection. */
  function offeredSetFor(connectionId) {
    let set = owedOffered.get(connectionId);
    if (set === undefined) {
      set = new Set();
      owedOffered.set(connectionId, set);
    }
    return set;
  }

  /** Offer one owed entry to the book. 'blocked' means the link is full and the entry stays owed. */
  function offerOwedEntry(entry) {
    const offered = owedOffered.get(entry.connectionId);
    if (offered !== undefined && offered.has(entry.receiveSeq)) return 'skipped';
    let envelope;
    try {
      envelope = makeEnvelope({
        market,
        stream,
        connectionId: entry.connectionId,
        runId: entry.runId ?? null,
        venue: entry.venue ?? null,
        generation: entry.generation ?? null,
        receiveSeq: entry.receiveSeq,
        recvTsMs: entry.recvTsMs,
        recvMonoNs: entry.recvMonoNs,
        raw: entry.raw,
        meta: entry.meta,
      });
    } catch (error) {
      diagnostic(`an owed frame could not be rebuilt for delivery: ${error.message}`);
      return 'skipped';
    }
    let ok = false;
    try {
      ok = bookChannel.sendEnvelope(envelope);
    } catch (error) {
      diagnostic(`the book link refused an owed frame: ${error.message}`);
      ok = false;
    }
    if (!ok) return 'blocked'; // backpressure: keep the rest owed rather than dropping them
    offeredSetFor(entry.connectionId).add(entry.receiveSeq);
    deliveredFrames += 1;
    return 'sent';
  }

  /**
   * Offer the frame that just became durable - one entry, not the whole owed set: with a hole open,
   * the owed set grows, and a full walk per frame would be paid on every arrival. The ledger is
   * asked first, so only what it still holds as owed is the book's to receive.
   */
  function deliverEntry(envelope) {
    if (bookChannel === null) return;
    const offered = owedOffered.get(envelope.connection_id);
    if (offered !== undefined && offered.has(envelope.receive_seq)) return;
    const entry = ledger.find(envelope.connection_id, envelope.receive_seq);
    if (entry === null || entry.state !== 'owed') return;
    let ok = false;
    try {
      ok = bookChannel.sendEnvelope(envelope);
    } catch (error) {
      diagnostic(`the book link refused an owed frame: ${error.message}`);
      ok = false;
    }
    if (!ok) {
      // Not lost, only postponed: the next frame runs a sweep instead of offering just itself.
      owedSweepNeeded = true;
      return;
    }
    offeredSetFor(envelope.connection_id).add(envelope.receive_seq);
    deliveredFrames += 1;
  }

  /**
   * A new audience has seen nothing: every owed frame may be offered again, and the next frame runs
   * a sweep rather than trusting an offer memory that no longer describes this channel.
   */
  function resetOffers() {
    owedOffered.clear();
    owedSweepNeeded = true;
  }

  /** The board has applied these: their entries are released, and their offer memory goes with them. */
  function pruneOffers(connectionId, upToSeq) {
    const set = owedOffered.get(connectionId);
    if (set === undefined) return;
    for (const seq of set) {
      if (seq <= upToSeq) set.delete(seq);
    }
    if (set.size === 0) owedOffered.delete(connectionId);
  }

  // ---------------------------------------------------------------------------------------------
  // The run marker and the clean end.
  // ---------------------------------------------------------------------------------------------

  function beginRun() {
    return organizeStore.beginRun();
  }

  /** Announce this process to the supervisor. */
  function announceHello() {
    const channel = ingestChannel ?? bookChannel;
    if (channel === null) return false;
    try {
      return channel.sendControl(
        makeMessage({
          version: IPC_VERSION,
          type: 'hello',
          role_instance: instance,
          run_id: runId,
          payload: { role: 'organize' },
        }),
      );
    } catch (error) {
      diagnostic(`the hello could not be sent: ${error.message}`);
      return false;
    }
  }

  /**
   * A clean end: only when every sealed tail has been reached is a normal completion written. Writing
   * `complete` over an unaccounted tail, a raw hole or an unprocessed spool would be the one lie the
   * marker exists to prevent (rulings ⑨⑩). The book's unapplied frames stay in the ledger; a normal
   * completion does not claim the board is complete.
   */
  function prepareFinalize(request) {
    const sealedIdentity = canonicalTailIdentity(sealedTails);
    const requestedIdentity = canonicalTailIdentity(request?.tails);
    if (preparedBarrierInvalid) {
      return { prepared: false, refused: true, reason: 'the prepared finalize barrier no longer matches organize’s sealed tails' };
    }
    if (sealedIdentity === null || requestedIdentity === null || sealedIdentity !== requestedIdentity) {
      return {
        prepared: false,
        refused: true,
        reason: 'the finalize request does not identify organize’s sealed final tails',
      };
    }
    const result = wiring.prepareFinalize(request);
    if (!result.prepared) return result;
    const identity = {
      requestId: request.finalizeRequestId,
      barrierId: request.barrierId,
      tailIdentity: requestedIdentity,
    };
    if (preparedBarrier === null) {
      preparedBarrier = Object.freeze(identity);
    } else if (
      preparedBarrier.requestId !== identity.requestId ||
      preparedBarrier.barrierId !== identity.barrierId ||
      preparedBarrier.tailIdentity !== identity.tailIdentity
    ) {
      preparedBarrierInvalid = true;
      return { prepared: false, refused: true, reason: 'this run already has a different prepared finalize barrier' };
    }
    return result;
  }

  /** A proof-backed clean end is possible only for the exact prepared barrier and its own all-ACK result. */
  function finalize(request) {
    if (request === null || request === undefined) {
      return { completed: false, refused: true, reason: 'a prepared finalize request is required' };
    }
    const alreadyComplete = organizeStore.runMarkerState(runId) === 'complete';
    if (alreadyComplete) {
      const result = wiring.completeRun(request);
      if (result.completed) return { ...result, reason: 'every tail was reached' };
      return result;
    }
    if (preparedBarrierInvalid || preparedBarrier === null) {
      return { completed: false, refused: true, reason: 'the prepared finalize barrier is no longer valid' };
    }
    const requestedIdentity = canonicalTailIdentity(request?.tails);
    if (
      request.finalizeRequestId !== preparedBarrier.requestId ||
      request.barrierId !== preparedBarrier.barrierId ||
      requestedIdentity !== preparedBarrier.tailIdentity
    ) {
      return { completed: false, refused: true, reason: 'the finalize request does not match the prepared barrier' };
    }
    if (canonicalTailIdentity(sealedTails) !== preparedBarrier.tailIdentity) {
      preparedBarrierInvalid = true;
      return { completed: false, refused: true, reason: 'the prepared finalize barrier no longer matches organize’s sealed tails' };
    }
    if (!allAcked) {
      return { completed: false, reason: allAckedReason };
    }
    const result = wiring.completeRun(request);
    if (result.completed) return { ...result, reason: 'every tail was reached' };
    return result;
  }

  /** Stop accepting frames. Pending durable work is retained for startup; no finalize is called. */
  function stop(reason = 'a stop was requested') {
    if (closed) return { stopped: false, reason: 'this organize process is closed' };
    stopped = true;
    stopReadinessReporting();
    try {
      onStop({ market, reason });
    } catch {
      // a stop notification is best-effort; the state is the fact
    }
    const intents = ledger.pending({ state: INTENT }).length;
    return { stopped: true, abnormal: intents > 0, ...(intents > 0 ? { reason: 'undurable processing intents remain' } : {}) };
  }

  const requestStop = stop;

  function handleStop(message) {
    const result = stop('a stop was requested over IPC');
    if (message && ingestChannel !== null) {
      try {
        ingestChannel.sendControl(
          makeMessage({
            version: IPC_VERSION,
            type: 'stopped',
            role_instance: instance,
            request_id: message.request_id,
          }),
        );
      } catch (error) {
        diagnostic(`a stop acknowledgement could not be sent: ${error.message}`);
      }
    }
    return result;
  }

  // ---------------------------------------------------------------------------------------------
  // IPC surface.
  // ---------------------------------------------------------------------------------------------

  function setRole(channel, role) {
    // Behind the supervisor there is one channel to the router, already bound; the peer is told apart
    // by the message's type, not the channel, so nothing here needs re-attributing.
    if (routerMode || channel === undefined || channel === null) return;
    const previous = channelRoles.get(channel);
    channelRoles.set(channel, role);
    if (role === 'ingest' && ingestChannel !== channel) ingestChannel = channel;
    if (role === 'book' && bookChannel !== channel) {
      bookChannel = channel;
      // A new book channel is a new audience: what the previous one was offered must be offered
      // again, because nothing on this channel has seen it.
      resetOffers();
      // A request can be persisted before the book's channel is up. Announcing it when the channel
      // attaches is what keeps the round trip from being lost to connection ordering (ruling ⑥).
      if (previous !== 'book') announceOutstanding();
    }
  }

  /** Behind the supervisor: one channel to the router carries both the ingest and the book traffic. */
  function attachRouter(channel) {
    routerMode = true;
    ingestChannel = channel;
    bookChannel = channel;
    // The router channel carries the book's traffic here: a fresh one has seen nothing.
    resetOffers();
    return true;
  }

  /** Re-announce every request the book has not answered, oldest first. */
  function announceOutstanding() {
    for (const request of invalidationRequests({ state: INVALIDATION_REQUESTED })) announceInvalidate(request);
  }

  function handleControl(message, channel) {
    switch (message?.type) {
      case 'hello': {
        const role = message.payload?.role;
        if (role === 'ingest' || role === 'book') setRole(channel, role);
        return { role: role ?? null };
      }
      case 'accept':
        setRole(channel, 'ingest');
        return handleAcceptFromPeer(message);
      case 'accepted':
        // The book's authorization, relayed by the supervisor: adopt and confirm (ruling ②). This is
        // the only path that adopts a connection - there is no unconditional acceptance here.
        return adoptConnection(message);
      case 'tail_sealed':
        setRole(channel, 'ingest');
        return handleTailSealed(message);
      case 'applied_ack':
        setRole(channel, 'book');
        return handleAppliedAck(message);
      case 'invalidated':
        setRole(channel, 'book');
        return handleInvalidated(message);
      case 'resend': {
        setRole(channel, 'book');
        // Frames owed to the book are re-offered on request: the offer memory forgets what the
        // current channel has seen and the delivery path walks the owed set again, arrival order.
        resetOffers();
        const sweep = deliverOwed();
        owedSweepNeeded = sweep.blocked;
        return { resend: true, pending: ledger.size(), delivered: sweep.delivered };
      }
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

  function handleEnvelope(envelope, channel) {
    setRole(channel, 'ingest');
    return organizeFrame(envelope);
  }

  function handleError(error, channel) {
    diagnostic(`a peer channel failed: ${error?.message ?? error}`);
    if (channel === bookChannel || channel === undefined || channel === null) {
      // The frames offered on the failed channel were offered to an audience that no longer exists:
      // whatever channel comes next is owed the whole set again. Behind the supervisor the error
      // arrives without a channel - the router's one channel carries the book's traffic there.
      resetOffers();
    }
    if (channel === bookChannel) return { bookDown: true };
    return { error: true };
  }

  /** Attach a channel (used by the listener, and reachable by a test that opens its own). */
  function adoptChannel(channel) {
    return channel;
  }

  // Stage 5c: the periodic readiness report. Behind the supervisor the channel to the router is the
  // one it arrived on (routerMode), so both reply routes point at it; the message's own type is the
  // routing fact. The interval is unref'd and cleared the moment the process stops or closes.
  let readinessTimer = null;

  function readinessPayload() {
    return { role: 'organize', ready: !stopped && !closed, market, stream };
  }

  function sendReadiness() {
    const channel = ingestChannel ?? bookChannel;
    if (channel === null) return false;
    try {
      return channel.sendControl(
        makeMessage({ version: IPC_VERSION, type: 'readiness', role_instance: instance, payload: readinessPayload() }),
      );
    } catch (error) {
      diagnostic(`a readiness report could not be sent: ${error.message}`);
      return false;
    }
  }

  function startReadinessReporting() {
    if (!Number.isFinite(readinessIntervalMs) || readinessIntervalMs <= 0) return;
    readinessTimer = setInterval(sendReadiness, readinessIntervalMs);
    if (typeof readinessTimer.unref === 'function') readinessTimer.unref();
  }

  function stopReadinessReporting() {
    if (readinessTimer !== null) {
      clearInterval(readinessTimer);
      readinessTimer = null;
    }
  }

  // Set 3: a restart recovers the acceptance an earlier life recorded, before anything can arrive.
  restoreAcceptedConnection();

  if (markRunning) organizeStore.beginRun();
  startReadinessReporting();

  const api = {
    market,
    stream,
    roleInstance: instance,
    runId,

    handleControl,
    handleEnvelope,
    handleError,
    adoptChannel,
    attachRouter,
    announceHello,

    beginRun,
    stop,
    requestStop,
    prepareFinalize,
    finalize,
    deliverOwed,
    resumeFromBoundary,

    requestInvalidation,
    confirmInvalidationBySupervisor,
    cancelInvalidation,
    rederiveInvalidations,
    invalidationRequests,

    recoveryStatus,
    refusalSummary: () => ({ total: refusedFrames, byReason: Object.fromEntries(refusalTally) }),
    evaluateAllAcked,
    judge: (tails, options) => judgeAllAcked({ tails, ...options }),
    ledgerEntries: ({ state = null } = {}) => ledger.pending({ state }),

    watermarkRows() {
      return wiring.db
        .prepare(
          `SELECT connection_id, up_to_receive_seq, first_seq FROM organized_watermark
            WHERE market = ? AND stream = ? ORDER BY connection_id`,
        )
        .all(market, stream)
        .map((row) => ({ connectionId: row.connection_id, upToSeq: row.up_to_receive_seq ?? null, firstSeq: row.first_seq ?? null }));
    },
    runMarkerState: (query = runId) => organizeStore.runMarkerState(query),
    ledgerSize: () => ledger.size(),
    get serving() {
      return serving;
    },
    get allAcked() {
      return allAcked;
    },
    get bookBoundary() {
      return bookBoundary;
    },

    close() {
      if (closed) return;
      reportRefusalBurst(' more');
      closeServer();
      stopReadinessReporting();
      stopped = true;
      closed = true;
      if (openedStoreHere) {
        organizeStore.close();
      }
    },

    get acceptedConnectionId() {
      return acceptedConnectionId;
    },
    get stats() {
      return {
        market,
        stream,
        roleInstance: instance,
        acceptedConnectionId,
        acceptedGeneration,
        framesDurable,
        framesAlreadyDurable,
        refusedFrames,
        ledger: ledger.size(),
        allAcked,
        allAckedReason,
        stopped,
        closed,
      };
    },
  };

  let closeServer = () => {};
  api.attachServer = (s) => {
    closeServer = () => {
      try {
        s.close();
      } catch {
        // the server may already be closed
      }
    };
  };
  api.channels = () => ({ ingest: ingestChannel, book: bookChannel });

  return api;
}

/**
 * Open the organize process. Behind the supervisor it connects to the router's socket (the supervisor
 * owns the rendezvous, ruling ①) and adopts the single channel; standalone - the stage-3 test seam - it
 * listens and tells its peers apart by which channel they connected on. The two are the same process;
 * only the meeting point differs.
 */
export async function openOrganizeProcess({ listenPath = null, routerSocketPath = null, channelOptions = {}, ...rest } = {}) {
  if (!listenPath && !routerSocketPath) {
    throw new TypeError('the organize process needs a socket path to listen on or a router to connect to');
  }
  const process = createOrganizeProcess({ ...rest, channelOptions });

  if (routerSocketPath) {
    const channel = await connect(routerSocketPath, {
      ...channelOptions,
      onControl: (message) => process.handleControl(message),
      onEnvelope: (envelope) => process.handleEnvelope(envelope),
      onError: (error) => process.handleError(error),
    });
    process.attachRouter(channel);
    process.announceHello();
    process.rederiveInvalidations();
    return process;
  }

  const server = net.createServer((socket) => {
    let channel;
    channel = createChannel(socket, {
      ...channelOptions,
      onControl: (message) => process.handleControl(message, channel),
      onEnvelope: (envelope) => process.handleEnvelope(envelope, channel),
      onError: (error) => process.handleError(error, channel),
    });
    process.adoptChannel(channel);
  });
  await new Promise((resolve, reject) => {
    server.once('error', reject);
    server.listen(listenPath, () => {
      server.removeListener('error', reject);
      resolve();
    });
  });
  process.attachServer(server);
  process.rederiveInvalidations();
  return process;
}
