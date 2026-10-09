/**
 * The ingest process entrance: reception extracted so it can run as its own process and speak to
 * organize over IPC (three-process design, docs/fix-plan-sets.md §5.8, rulings ②⑦⑧⑫).
 *
 * This module is stage 2 of the split. It does not change the single-process path - `bin/receiver.mjs`,
 * the supervisor and `structure.mjs` are untouched and keep writing `received_tail` through
 * `durability.mjs` until stage 5 rewires the routes. What this adds is a second, independent way to run
 * reception: a process whose store is its own, whose spool it owns alone, and whose only way to reach
 * the rest of the structure is a channel from `src/ipc.mjs`.
 *
 * What ingest owns (ruling ②), and where each is here:
 *
 *   - `received_tail`     `src/ingest/store.mjs`, written on every arrival (`writeReceivedTail`).
 *   - the spool           `createSpool`. Appending, fsyncing, resending oldest first, and moving the
 *                         cursor and deleting fully-consumed segments after organize acknowledges.
 *   - the generation      `issueGeneration` is injected by the run (as the supervisor does) and a new
 *                         generation is issued only when `connection.mjs` replaces a socket.
 *   - the `accept`        issued by ingest, inside the real `onGeneration`, with an explicit takeover
 *                         flag; the socket is opened only once the peer answers `accepted`. An
 *                         acceptance that does not name the generation reception is waiting on is an
 *                         old instance's answer and is ignored (C2, ruling ⑫).
 *
 * What is deliberately not here yet (stage 3+): the takeover *decision* itself. In the single-process
 * structure the takeover is computed from the board's recorded owner; in the split that knowledge lives
 * with organize and the book, so ingest only carries the explicit flag, decided by the injected
 * `takeoverFor` policy. Where the decision moves to is recorded as an open point, not guessed at.
 */

import { createReceiveConnection } from './connection.mjs';
import { createSpool } from '../spool.mjs';
import { ackIdentityOf, rebuildAckFifo } from '../ack-fifo.mjs';
import { openIngestStore } from './store.mjs';
import {
  openRawWriter,
  DEFAULT_RAW_BATCH_WINDOW_MS,
  DEFAULT_RAW_BATCH_MAX_ROWS,
} from '../raw.mjs';
import { createAuxCollector, hasRestOiSource } from './aux.mjs';
import { IPC_VERSION, makeMessage } from '../ipc-message.mjs';
import { attachChanges, deriveChanges } from '../changes.mjs';
import { connect } from '../ipc.mjs';

function assertInlineReceiveExecutor(options) {
  if (options?.onEvent !== undefined) {
    throw new TypeError('the ingest process does not accept a deferred receive-event executor');
  }
}

/**
 * Build the ingest process around an already-open organize channel.
 *
 * The channel is passed in rather than opened here so construction stays synchronous, exactly like the
 * structure's: a caller (the supervisor in stage 5, a test now) connects the socket and wires its
 * `onControl`/`onError` to `handleControl`/`handleError`. A channel that is not up yet may be attached
 * later with `attachOrganize`, which is how a process that started before organize learns where to
 * send the acceptance reception is waiting for.
 */
const DEFAULT_ACK_STALL_MS = 5000;

export function createIngestProcess({
  market,
  stream = 'trades',
  adapter,
  venue,
  runId,
  webSocketImpl,
  organizeChannel = null,
  ingestStore = null,
  ingestStorePath = null,
  spoolDir = null,
  spoolOptions = {},
  // Set 7a: where the canonical raw lives. One database per market is opened under this root and the
  // frames reception hears are written there (see `src/raw.mjs`). Absent means no raw writer: the
  // process behaves exactly as before, which is what keeps every pre-Set-7 configuration unchanged.
  rawDir = null,
  rawBatchWindowMs = DEFAULT_RAW_BATCH_WINDOW_MS,
  rawBatchMaxRows = DEFAULT_RAW_BATCH_MAX_ROWS,
  // Set 8: the auxiliary open-interest REST poller. Disabled (0) by default, so every pre-Set-8
  // configuration is unchanged; a positive value starts the v1-style 30 s poll for this market's
  // REST OI source (see `src/ingest/aux.mjs`). The fetch and the interval timer are injectable so a
  // test can drive them; production gets the real ones.
  oiPollIntervalMs = 0,
  auxFetchImpl = globalThis.fetch,
  auxSetTimer = setInterval,
  auxClearTimer = clearInterval,
  // The restart identity of this child process. It is deliberately separate from the receive run and
  // from the connection generation (ruling ⑧): a book or organize restart is a fact about that child,
  // and it must not move the receive generation. Overridden by the caller so two lives of one run get
  // two ids; defaulted to the run so a single process has a stable one.
  roleInstance = null,
  // Whether an `accept` for a generation should carry an explicit takeover. In the split ingest does
  // not itself know the board's owner (that is organize/book), so the policy is injected; the default
  // asserts nothing.
  takeoverFor = () => false,
  // Stage 5c: the periodic readiness report (ruling ⑬, §5.8). A role reports its own readiness on
  // this interval so the supervisor's aggregator can lose it when the report ages past its deadline -
  // a live but stuck role is otherwise indistinguishable from a healthy one. Disabled (0) by default,
  // so a process built without it behaves exactly as before.
  readinessIntervalMs = 0,
  onStop = () => {},
  onDiagnostic = () => {},
  onGap = () => {},
  // Set 1 (observability): how long frames may go unacknowledged before a stall is reported. Reports
  // are silenced to one line per interval; 0 or a non-positive value turns the observation off.
  ackStallMs = DEFAULT_ACK_STALL_MS,
  // Set 3: the timers of the progress deadline and the resend scheduler. Injected so a test can drive
  // them, exactly like the connection's timers; production gets the real ones.
  setTimer = setTimeout,
  clearTimer = clearTimeout,
  // Set 6d: how long a received position may wait before it is written to the receive tail. The
  // tail is written on every arrival, and one commit per frame is what a disk with millisecond
  // fsyncs charges for; the latest position of each identity waits for this clock instead. Zero is
  // the synchronous mode (the frame-by-frame order tests pin).
  tailSaveMs = 100,
  ...receiveOptions
} = {}) {
  if (!adapter) throw new TypeError('the ingest process needs a venue adapter');
  if (!runId) throw new TypeError('the ingest process needs a run id');
  if (!venue) throw new TypeError('the ingest process needs a venue');
  assertInlineReceiveExecutor(receiveOptions);
  const instance = roleInstance ?? `ingest-${runId}`;

  const openedStoreHere = ingestStore === null;
  const store = ingestStore ?? (ingestStorePath ? openIngestStore({ path: ingestStorePath }) : null);
  if (store === null) throw new TypeError('the ingest process needs an ingest store (a path or an open store)');

  const spool = spoolDir ? createSpool({ dir: spoolDir, ...spoolOptions }) : null;
  // Set 7a: the canonical raw writer. It owns one database per market under `rawDir`, and it is the
  // single writer of those files (the same discipline as the store above). Opened here, synchronously,
  // so a directory that cannot be created fails construction rather than the first frame.
  const raw =
    rawDir === null
      ? null
      : openRawWriter({
          dir: rawDir,
          batchWindowMs: rawBatchWindowMs,
          maxBatchRows: rawBatchMaxRows,
          // The session id travels on every stored line. It names this writer instance the way v1's did
          // (`sqlite:pid:time`), so a restart's rows are distinguishable from a previous life's.
          writerSessionId: `raw:${instance}:${Date.now()}`,
          // A flush raised from the raw's own window timer has no caller to throw to: it stops
          // reception here, exactly like a failed write on the receive path.
          onError: (error) => {
            try {
              onDiagnostic({ market, reason: `the canonical raw could not be flushed: ${error.message}` });
            } catch {
              // a diagnostic must not replace the loud stop
            }
            stopReception('the canonical raw could not be flushed');
          },
        });
  // Set 3: the release order of everything the spool holds, rebuilt once from the walk. The cursor
  // only ever moves past a record that itself is acknowledged (see ack-fifo.mjs); a spool-less
  // configuration has nothing to release.
  // Set 5: the same walk remembers the first retained sequence, because everything a restart finds
  // retained is unsent as far as this life knows - the ordered pump starts there.
  let firstRetainedSeq = null;
  const fifo =
    spool === null
      ? null
      : rebuildAckFifo(
          (function* () {
            for (const record of spool.drainRecords()) {
              if (firstRetainedSeq === null && Number.isInteger(record.envelope?.receive_seq)) {
                firstRetainedSeq = record.envelope.receive_seq;
              }
              yield record;
            }
          })(),
        );

  let organizeChannelRef = organizeChannel;
  // What organize last told us about its capacity. Anything but 'ok' means the frame cannot be handed
  // on right now, so the spool is the rung it goes to. A channel that is not up is the same answer.
  let organizeCapacity = 'ok';
  let stopped = false;
  let quiescing = false;
  let closed = false;
  let tailWriteFailure = null;
  let receptionCloseFailure = null;
  let rawFlushFailure = null;
  // Set 8: whether the auxiliary poller's current failure episode has already been reported. The
  // episode ends when a sample is written again (see `writeAuxRecord`).
  let auxFailureReported = false;
  let unretainedFrameFailure = false;
  // The generations reception is waiting for an acceptance on, keyed by generation. Only these may be
  // settled by an `accepted`; anything else is a stale instance's answer (C2).
  const pendingAdmissions = new Map();
  // The restart id each role last announced, by role. A peer restart is recorded here and changes
  // nothing about the receive run or the connection generation (⑥).
  const peerInstances = new Map();
  let sentFrames = 0;
  let spooledFrames = 0;
  let resentFrames = 0;
  // Set 1 (observability): the state behind the stall report and the spool-holding edge report.
  let ackConnectionId = null;
  let lastAckUpToSeq = null;
  let framesSinceAckProgress = 0;
  let linkHeldReported = false;
  // Set 3: the single resend scheduler and the progress deadline. A hand-over waits for the previous
  // connection's retention to empty before its acceptance is announced (deferredAdmission), and the
  // deadline is what turns "no acknowledgement is coming" into a resend instead of a wait.
  let progressTimer = null;
  let progressClockMs = 0;
  let stalledStrikes = 0;
  let resendTimer = null;
  let resendRunning = false;
  let resendQueued = false;
  let resendWalk = null;
  let resendPending = null;
  // Set 5: the ordered pump. `nextToSendSeq` is the sequence the next send must carry - everything
  // before it has been sent; `pumpPos` is the spool position just past the last record sent. A new
  // frame is sent directly only when it is exactly this next sequence; anything behind a held frame
  // waits for the walk, which sends in order. `resendMode` picks the walk's start: the drain
  // continues from the pump's position; a recovery walks again from the first unreleased record.
  let nextToSendSeq = firstRetainedSeq;
  let pumpPos = spool === null ? null : spool.cursor;
  let resendMode = 'drain';
  // A stall seen while the link could not take frames: the recovery it wants runs the moment the
  // link opens (or the next attempt can try), not never.
  let recoveryWanted = false;
  let deferredAdmission = null;
  // Set 6b: a generation that waits on a cursor save it could not write must not wait for a trigger
  // that will never come. The last acknowledgement emptied the FIFO (so the stall clock is off) and
  // the failed save cleared the save clock itself; the retry below is what keeps the gate live. The
  // failure is reported once per episode, the retry runs until the save succeeds, and the venue
  // link's own reconnection model - wait, keep asking, report once - is the one it follows.
  let admissionRetryTimer = null;
  let admissionSaveFailureReported = false;
  const ADMISSION_RETRY_MS = 200;

  /** The clock that keeps a deferred acceptance live when the save or the send could not go through. */
  function armAdmissionRetry() {
    if (admissionRetryTimer !== null) return;
    admissionRetryTimer = setTimer(() => {
      admissionRetryTimer = null;
      tryDeferredAdmission();
    }, ADMISSION_RETRY_MS);
    if (typeof admissionRetryTimer?.unref === 'function') admissionRetryTimer.unref();
  }

  const connection = createReceiveConnection({
    ...receiveOptions,
    setTimer,
    clearTimer,
    adapter,
    market,
    runId,
    venue,
    webSocketImpl,
    onEnvelope: (envelope) => {
      // (④) the receive tail is written on the reception side, before anything else: a frame the board
      // refuses was still received, and "received" is a fact about reception, not a verdict downstream.
      writeReceivedTail(envelope);
      // Set 7a: the canonical raw is not written here but at the connection, for every parsed data
      // frame and before anything judges it (see `onRawFrame`): a frame this connection refuses - a
      // gap, a stale diff - is still a frame that was received, and v1 recorded it too. Writing it
      // here would record only the frames that already passed the judgment.
      return deriveAndSend(envelope);
    },
    onGeneration: handleGeneration,
    onRawFrame: (frame) => writeRawFrame(frame),
    onSubscriptions: handleSubscriptions,
    onState: (info) => {
      try {
        receiveOptions.onState?.(info);
      } catch {
        // an observation that throws is not a fact about reception
      }
    },
    onDiagnostic: (diagnostic) => {
      try {
        onDiagnostic(diagnostic);
      } catch {
        // a diagnostic is best-effort by contract
      }
    },
  });

  /**
   * Set 7a: the REST depth snapshot, as one raw record. A snapshot does not arrive as a socket frame -
   * the adapter's depth synchronizer fetches and applies it while preparing the connection - so the
   * adapter exposes a sink (`setRawSnapshotSink`) and this is what is installed there. v1 wrote a
   * snapshot under both `book_updates` (it is a full level replacement) and `snapshots` (the stream
   * downstream reads to anchor a book), so the same record is appended to both streams here.
   */
  function writeRawSnapshot(snapshot) {
    if (raw === null) return;
    if (!snapshot || !Number.isFinite(snapshot.event_ts_ms) || snapshot.event_ts_ms <= 0) {
      // A snapshot with no event time cannot be a raw row downstream can read; failing loudly here is
      // the same rule as a socket frame's record.
      onDiagnostic({ market, reason: 'the depth snapshot carried no usable event time and was not written raw' });
      return;
    }
    const base = {
      market,
      event_ts_ms: snapshot.event_ts_ms,
      // v1's REST-sync snapshot states no source time: `source_event_ts_ms: null` and
      // `source_event_time_known: false` (`lib/binance-connector.mjs:299-305`). Claiming the wall clock
      // as the source time would assert a venue timestamp the venue never gave.
      source_event_ts_ms: snapshot.source_event_ts_ms ?? null,
      source_event_time_known: snapshot.source_event_time_known === true,
      recv_ts_ms: Date.now(),
      recv_mono_ns: Number(process.hrtime.bigint()),
      connection_id: connection.connectionId,
      sequence_order: null,
      receive_seq: null,
      payload: snapshot.payload,
    };
    try {
      raw.append({ ...base, stream: 'book_updates' });
      raw.append({ ...base, stream: 'snapshots' });
    } catch (error) {
      try {
        onDiagnostic({ market, reason: `the canonical raw snapshot could not be written: ${error.message}` });
      } catch {
        // a diagnostic must not replace the loud stop
      }
      stopReception('the canonical raw snapshot could not be written');
      throw error;
    }
  }

  // The sink belongs to the adapter and is installed before the connection can open a socket (the
  // socket is opened by `start()`, after construction). An adapter with no snapshot (a pure WS venue)
  // has no such hook and this is a no-op.
  adapter?.setRawSnapshotSink?.((snapshot) => writeRawSnapshot(snapshot));

  // Set 8: the open-interest poller, when the deployment asked for one and this market has a REST
  // source. It writes to the same raw writer as the received frames (one owner per market), so the
  // `open_interest` rows land in the market's database beside them. Started in `start()`, stopped
  // by `stop()`/`close()`.
  const aux =
    raw !== null && Number.isFinite(oiPollIntervalMs) && oiPollIntervalMs > 0 && hasRestOiSource(market)
      ? createAuxCollector({
          market,
          fetchImpl: auxFetchImpl,
          intervalMs: oiPollIntervalMs,
          setTimer: auxSetTimer,
          clearTimer: auxClearTimer,
          append: (record) => writeAuxRecord(record),
          onError: (error) => {
            // A sample that could not be fetched is not a reception failure. v1 polled this on a 30 s
            // clock and wrote a `status:'error'` placeholder row when the fetch failed (with
            // `event_ts_ms: 0`, which the downstream drops the moment it sees it), and the
            // open-interest stream is not consumed by the live stages at all. Stopping reception over
            // a REST hiccup would trade a hole in an auxiliary stream for the market's whole
            // reception going dark. The failure is reported once per episode instead - the same
            // "one episode, one report" rule the capacity and acknowledgement stalls follow - and
            // reception carries on.
            if (auxFailureReported) return;
            auxFailureReported = true;
            try {
              onDiagnostic({ market, reason: `the open-interest poller failed: ${error.message}` });
            } catch {
              // a diagnostic is best-effort by contract
            }
          },
        })
      : null;

  /**
   * Set 7a: write one received frame to the canonical raw, from the connection's `onRawFrame` hook -
   * that is, for every parsed data frame and before anything judges it. The adapter decides whether
   * the frame is a raw record at all and what stream it belongs to (`rawEventFor`), because only the
   * adapter can read the venue's bytes. A frame the adapter does not classify is not written - Set 7a
   * covers the board frames; trades are Set 7b's. A write that fails is loud: reported, reception
   * stopped, and the error rethrown, so a raw that cannot be written is never mistaken for a healthy
   * run.
   */
  function writeRawFrame(frame) {
    if (raw === null) return;
    const derived = adapter?.rawEventFor ? adapter.rawEventFor(frame) : null;
    if (!derived) return;
    // Set 7b: one frame can carry more than one raw record - a trade frame with several trades, or a
    // snapshot the adapter writes to both `book_updates` and `snapshots`. Set 7a's single-record
    // return is unchanged (an object), so an adapter that still returns one record still works.
    const records = Array.isArray(derived) ? derived : [derived];
    try {
      for (const record of records) {
        raw.append({
          market,
          stream: record.stream,
          event_ts_ms: record.event_ts_ms,
          recv_ts_ms: frame.atMs,
          recv_mono_ns: frame.atNs,
          // The raw's own arrival counter, not the stamped receive sequence: v1 numbered every frame it
          // heard on the way in, including the frames its synchronization later refused, and the raw is
          // the record of the hearing.
          receive_seq: frame.arrivalSeq,
          worker_seq: frame.arrivalSeq,
          connection_id: frame.connectionId,
          sequence_order: frame.arrivalSeq,
          source_event_ts_ms: record.source_event_ts_ms ?? null,
          source_event_time_known: record.source_event_time_known === true,
          source_id: record.source_id ?? null,
          payload: record.payload,
        });
      }
    } catch (error) {
      try {
        onDiagnostic({ market, reason: `the canonical raw could not be written: ${error.message}` });
      } catch {
        // a diagnostic must not replace the loud stop
      }
      stopReception('the canonical raw could not be written');
      throw error;
    }
  }

  /**
   * Set 8: write one open-interest row (from the REST poller) to the canonical raw. The record is the
   * adapter-shaped one `openInterestRecord` produced; the arrival stamps are this process's clock,
   * because a REST sample has no socket frame behind it. A write that fails is loud, exactly like a
   * received frame's: reported, reception stopped, and the error rethrown.
   */
  function writeAuxRecord(record) {
    if (raw === null || !record) return;
    try {
      raw.append({
        market,
        stream: record.stream,
        event_ts_ms: record.event_ts_ms,
        recv_ts_ms: Date.now(),
        recv_mono_ns: Number(process.hrtime.bigint()),
        connection_id: connection.connectionId,
        sequence_order: null,
        receive_seq: null,
        source_event_ts_ms: record.source_event_ts_ms ?? null,
        source_event_time_known: record.source_event_time_known === true,
        source_id: record.source_id ?? null,
        payload: record.payload,
      });
      // A row written and not an error placeholder means the source answered: the failure episode is
      // over, and the next failure is a new one that must be reported in its own right.
      if (record.payload?.status !== 'error') auxFailureReported = false;
    } catch (error) {
      try {
        onDiagnostic({ market, reason: `the open-interest row could not be written: ${error.message}` });
      } catch {
        // a diagnostic must not replace the loud stop
      }
      stopReception('the open-interest row could not be written');
      throw error;
    }
  }

  /**
   * Set 6d: the receive tail, batched. The tail is the reception side's evidence of what it heard,
   * and it is written on every arrival - but a commit per frame is what a disk with millisecond
   * fsyncs charges for, and the record only needs the latest position of each identity. The latest
   * positions wait for the save clock, and everything that judges the tail reads it only after a
   * flush: `finalTailCandidate` (the seal) forces one, and the close makes a best effort of one.
   * The failure latch is unchanged: a tail that could not be written stays sticky, so the seal is
   * refused rather than made over a claim the store does not hold.
   */
  let tailSaveTimer = null;
  const pendingTails = new Map();
  const tailKey = (connectionId, streamName) => `${connectionId}\u0000${streamName ?? ''}`;

  function scheduleTailSave() {
    if (closed || tailSaveTimer !== null || tailSaveMs <= 0) return;
    tailSaveTimer = setTimer(() => {
      tailSaveTimer = null;
      try {
        flushTails();
      } catch (error) {
        // The positions stay pending, unacknowledged by the store: the next arrival, the next
        // flush point or the seal retries them, and the clock is re-armed so a quiet link does
        // not leave them waiting for a trigger that never comes.
        try {
          onDiagnostic({ market, reason: `the receive tail could not be written: ${error.message}` });
        } catch {
          // a diagnostic is best-effort by contract
        }
        if (pendingTails.size > 0) scheduleTailSave();
      }
    }, tailSaveMs);
    if (typeof tailSaveTimer?.unref === 'function') tailSaveTimer.unref();
  }

  /** Write the pending positions down; a failure keeps them pending and latches the fact. */
  function flushTails() {
    if (tailSaveTimer !== null) {
      clearTimer(tailSaveTimer);
      tailSaveTimer = null;
    }
    if (pendingTails.size === 0) return;
    const entries = [...pendingTails.values()];
    try {
      store.updateReceivedTails(entries);
    } catch (error) {
      if (tailWriteFailure === null) tailWriteFailure = error;
      // The positions stay pending, and the clock is re-armed here so a forced flush (the seal, the
      // stop) that failed leaves a retry behind rather than positions waiting for an explicit ask.
      if (pendingTails.size > 0) scheduleTailSave();
      throw error;
    }
    pendingTails.clear();
  }

  /** Record how far this process can show it heard, per connection and board. */
  function writeReceivedTail(envelope) {
    const entry = {
      connectionId: envelope.connection_id,
      market,
      stream: envelope.stream ?? stream,
      lastReceivedSeq: envelope.receive_seq,
      lastRecvMonoNs: envelope.recv_mono_ns,
    };
    if (tailSaveMs <= 0) {
      try {
        store.updateReceivedTail(entry);
      } catch (error) {
        if (tailWriteFailure === null) tailWriteFailure = error;
        try {
          onDiagnostic({ market, reason: `the receive tail could not be written: ${error.message}` });
        } catch {
          // a diagnostic must not prevent this accepted frame from being handed on or spooled
        }
      }
      return;
    }
    pendingTails.set(tailKey(entry.connectionId, entry.stream), entry);
    scheduleTailSave();
  }

  /**
   * A frame that was heard but could not be retained. The spool does not hold it, so the tail is
   * the only durable carrier of "this was received" - the claim is forced onto disk here rather
   * than waiting for the clock, because a crash in that window would leave a frame that was heard
   * and never delivered with nothing to answer for it.
   */
  function markUnretained() {
    unretainedFrameFailure = true;
    try {
      flushTails();
    } catch {
      // the latch above is the fact; a tail that could not be written has its own latch
    }
  }

  /**
   * Derive the frame's level changes here and carry them on the envelope (ruling ③). The adapter is
   * the only part that can read the raw bytes, and it is consulted once, on reception: a frame whose
   * changes cannot be derived - no `changesFor`, a malformed shape, an unknown version - is refused
   * and reported, never silently sent on as "an empty change".
   */
  function deriveAndSend(envelope) {
    const derived = deriveChanges(adapter, envelope);
    if (!derived.ok) {
      markUnretained();
      onDiagnostic({ market, reason: `the level changes were refused: ${derived.reason}` });
      onGap({ market, reason: derived.reason, seq: envelope.receive_seq });
      return { accepted: false, reason: derived.reason };
    }
    return sendOrSpool(attachChanges(envelope, derived));
  }

  /** The frames this process hands on are the ones with a derived, validated changes block. */

  /**
   * Hand one frame to organize. Set 3: retention comes first - the frame is written to the spool and
   * registered with the release FIFO before it is offered to the link, so a frame the link refuses,
   * or one that never gets the chance to be sent, is still something this process holds and can send
   * again. A frame the spool cannot hold stops reception (the ladder's third rung) and is never
   * handed on - a frame the process cannot keep is one it must not pretend to have delivered.
   * False back from the channel means the same thing it means in `ipc.mjs`: the queue is over its
   * bound and the frame must be kept elsewhere, never dropped.
   */
  function sendOrSpool(envelope) {
    if (closed) return { accepted: false, reason: 'this ingest process is closed' };
    if (spool !== null) {
      const retained = spoolFrame(envelope);
      if (retained.accepted === false) return retained;
      // Set 5: the ordered pump. A frame goes out directly only when it is exactly the next in
      // order - nothing held in front of it. Anything else waits for the pump, which walks in
      // order, so a new frame can never overtake one that is still retained (the overtaking that
      // opened a hole nothing could close under the soak).
      if (Number.isInteger(envelope.receive_seq) && nextToSendSeq === null) {
        nextToSendSeq = envelope.receive_seq; // the first frame of the connection starts the pump here
      }
      if (envelope.receive_seq === nextToSendSeq && trySend(envelope)) {
        if (retained.position !== undefined && retained.position !== false) pumpPos = retained.position;
        nextToSendSeq = envelope.receive_seq + 1;
        return { accepted: true, sent: true };
      }
      // Held because the link would not take it, or because it is not next: counted, and the
      // episode is reported once - not once per frame - so a capacity episode is visible without
      // flooding the log. The retention is also put under the progress deadline here: a first send
      // that failed while the link is nominally ready must age into a report and a retry rather
      // than wait for another event, and the pump is asked to carry what it can.
      spooledFrames += 1;
      armProgressDeadlineIfIdle();
      scheduleResend();
      if (!linkHeldReported) {
        linkHeldReported = true;
        try {
          onDiagnostic({
            market,
            reason: 'the organize link could not take a frame; it is being held in the spool',
            spoolBytes: spool.bytes,
            capacity: organizeCapacity,
            connectionState: connection.state,
          });
        } catch {
          // a diagnostic is best-effort; it must never interrupt retention of the frame
        }
      }
      return { accepted: true, spooled: true };
    }
    // No spool configured: there is nothing to retain, so the link is the only rung before the stop.
    if (trySend(envelope)) return { accepted: true, sent: true };
    return spoolFrame(envelope);
  }

  /** Offer one frame to the organize link. False means the link would not take it, never "sent". */
  function trySend(envelope) {
    if (organizeChannelRef === null || organizeCapacity !== 'ok') return false;
    let ok = false;
    try {
      ok = organizeChannelRef.sendEnvelope(envelope);
    } catch (error) {
      try {
        onDiagnostic({ market, reason: `the organize link refused a frame: ${error.message}` });
      } catch {
        // a diagnostic must not interrupt retention of this accepted frame
      }
      ok = false;
    }
    if (!ok) return false;
    sentFrames += 1;
    linkHeldReported = false;
    noteFrameSent();
    return true;
  }

  /**
   * Append one frame to the spool and register its end position for release. False from the spool is
   * a stop signal, not a reason to drop it: the ladder's third rung stops reception and records the
   * gap.
   */
  function spoolFrame(envelope) {
    let position = false;
    try {
      position = spool !== null ? spool.append(envelope) : false;
    } catch (error) {
      // A spool that throws has nothing more to offer - a torn record stops it taking anything -
      // so the ladder's third rung is taken here exactly as it is on a refusal; the failure still
      // surfaces to the caller.
      markUnretained();
      stopReception('nothing could hold the frame');
      onGap({ market, reason: `the spool could not hold the frame: ${error.message}`, seq: envelope.receive_seq });
      throw error;
    }
    if (position !== false && position !== null) {
      fifo.record({ identity: ackIdentityOf(envelope), seq: envelope.receive_seq, position });
      return { accepted: true, spooled: true, position };
    }
    markUnretained();
    stopReception('nothing could hold the frame');
    onGap({ market, reason: 'the spool could not hold the frame and it was not handed on', seq: envelope.receive_seq });
    return { accepted: false, reason: 'nothing could hold the frame' };
  }

  function fenceReception() {
    if (quiescing) return false;
    quiescing = true;
    stopped = true;
    stopReadinessReporting();
    // The drain timers deliberately keep their lives through quiescence: sealing fences new
    // reception, and what is retained still owes an acknowledgement-driven drain (and a stalled
    // acknowledgement is still detected). They guard on `closed` themselves.
    try {
      connection.stop();
    } catch (error) {
      receptionCloseFailure = error;
      try {
        onDiagnostic({ market, reason: `reception could not be closed: ${error.message}` });
      } catch {
        // the fence below is the fact that matters
      }
    }
    return true;
  }

  async function quiesce() {
    // Set 8: quiescence fences new raw records, so the auxiliary poller stops with it.
    aux?.stop();
    fenceReception();
    return { quiesced: true };
  }

  /**
   * Stop this process receiving, for good. Reception is closed as well as flagged: a stopped process
   * would refuse every further frame, and a socket left open would keep delivering frames that go
   * nowhere. Turning this into a non-zero exit is the supervisor's job in stage 5.
   */
  function stopReception(reason) {
    if (stopped || closed) return;
    // Set 8: the auxiliary poller writes into the same raw, so it stops with the reception - no row
    // may land after the last flush.
    aux?.stop();
    fenceReception();
    onStop({ market, reason });
  }

  /** Build the `accept` for one generation announcement. */
  function acceptMessage(info) {
    return makeMessage({
      version: IPC_VERSION,
      type: 'accept',
      role_instance: instance,
      request_id: `${instance}:accept:${info.connectionId}:${info.generation}`,
      run_id: runId,
      connection_id: info.connectionId,
      generation: info.generation,
      market,
      stream,
      payload: { takeover: takeoverFor(info) === true, first_seq: info.firstSeq ?? 1, venue },
    });
  }

  /**
   * A generation has been announced. The `accept` is issued here and only here - the real
   * `onGeneration`, where the generation became current - and reception waits for the answer: nothing
   * is returned false, so `connection.mjs` opens no socket until `settle` hears `accepted`.
   */
  function handleGeneration(info) {
    if (closed || quiescing) return false;
    // Set 6d: the reception's start is written down before the socket can open. The tail's later
    // positions wait for their save clock, so without this row a run that died before its first
    // save would leave nothing at all behind - and a restart's §9.2 scan, which accuses the
    // interval after each unclean run's tail, would have no tail to accuse. The marker is written
    // synchronously (a reception's start is rare, one per connection) and its position of zero
    // says exactly what it is: nothing has been heard yet.
    try {
      store.updateReceivedTail({
        connectionId: info.connectionId,
        market,
        stream,
        lastReceivedSeq: 0,
        lastRecvMonoNs: info.recvMonoNs ?? 0,
      });
    } catch (error) {
      if (tailWriteFailure === null) tailWriteFailure = error;
      try {
        onDiagnostic({ market, reason: `the reception's start could not be written down: ${error.message}` });
      } catch {
        // a diagnostic must not replace the refusal
      }
      // Fail closed: a reception whose start is not written down must not begin. A crash before any
      // tail exists would leave this generation's interval unaccounted for - the restart's §9.2
      // scan has only the tails to accuse - so the generation is refused here and the socket stays
      // closed; the venue's next attempt tries again, and the run's deadline reports a reception
      // that never started.
      return false;
    }
    const message = acceptMessage(info);
    pendingAdmissions.set(info.generation, {
      generation: info.generation,
      requestId: message.request_id,
      connectionId: info.connectionId,
      info,
      message,
    });
    // Set 3: a hand-over is serialized. Until every frame of the previous connection is
    // acknowledged, the next generation is not announced at all - the venue socket stays closed and
    // no frame of the new generation can be stamped before the old one is fully resolved. The
    // frames still unacknowledged are offered again now, so the wait is usually over in one round
    // trip; the acceptance goes out the moment the release FIFO empties (tryDeferredAdmission).
    deferredAdmission = { generation: info.generation, message };
    if (fifo !== null && fifo.size > 0) {
      // The wait ends when the old connection's retention drains, and the frames that need an
      // answer are exactly the ones already sent: the offer is a recovery walk from the first
      // unreleased record, not a drain of what has never been sent.
      resendMode = 'recover';
      resendWalk = null;
      resendPending = null;
      scheduleResend();
      return undefined;
    }
    tryDeferredAdmission();
    return undefined;
  }

  /**
   * Announce a hand-over that was waiting for the previous connection's retention to empty. Called
   * whenever the release FIFO drains (an acknowledgement) and whenever the link is attached - the
   * two moments the wait can end. The announcement is kept until it is actually sent: a link that
   * is not there yet is a reason to wait, not a reason to lose the acceptance.
   */
  function tryDeferredAdmission() {
    if (deferredAdmission === null) return false;
    if (fifo !== null && fifo.size > 0) return false;
    // Set 6b: the cursor file must name the current confirmed position before a new generation is
    // announced. The save clock may still be holding it back, and a crash with a lagging file would
    // rebuild the release order from a position the old generation has already moved past - its
    // records, once the new connection is accepted, would be refused for ever, and the release
    // order would stop at a record nobody can acknowledge. The save is what makes the gate's
    // emptiness true of the file as well as of the FIFO; a save that fails withholds the
    // acceptance, and the deadline that follows is the honest report of it.
    if (spool !== null) {
      try {
        spool.saveCursor();
        admissionSaveFailureReported = false;
      } catch (error) {
        if (!admissionSaveFailureReported) {
          admissionSaveFailureReported = true;
          onDiagnostic({ market, reason: `the cursor could not be saved before the acceptance: ${error.message}` });
        }
        armAdmissionRetry();
        return false;
      }
    }
    const sent = announceAccept(deferredAdmission.message);
    if (sent === true) {
      deferredAdmission = null;
      return true;
    }
    // The send itself was refused - the link is full, or the channel is momentarily gone. The same
    // clock covers it: the FIFO is empty, so no acknowledgement is coming to ask again, and the
    // gate would wait for a trigger that does not exist.
    armAdmissionRetry();
    return false;
  }

  function announceAccept(message) {
    if (organizeChannelRef === null) return false;
    try {
      return organizeChannelRef.sendControl(message);
    } catch (error) {
      onDiagnostic({ market, reason: `the acceptance could not be sent: ${error.message}` });
      return false;
    }
  }

  /**
   * Announce this process to the supervisor (ruling ①). The router needs the role before it can route
   * anything; a message sent before the hello is refused, so the hello goes out the moment the channel
   * is attached, not lazily.
   */
  function announceHello() {
    if (organizeChannelRef === null) return false;
    try {
      return organizeChannelRef.sendControl(
        makeMessage({
          version: IPC_VERSION,
          type: 'hello',
          role_instance: instance,
          run_id: runId,
          payload: { role: 'ingest' },
        }),
      );
    } catch (error) {
      onDiagnostic({ market, reason: `the hello could not be sent: ${error.message}` });
      return false;
    }
  }

  function handleControl(message) {
    switch (message?.type) {
      case 'accepted':
        return handleAccepted(message);
      case 'durable_ack':
        return handleDurableAck(message);
      case 'resend':
        return handleResend(message);
      case 'hello':
        return handleHello(message);
      case 'readiness':
        return handleReadiness(message);
      case 'error':
        return handleErrorReport(message);
      default:
        return undefined;
    }
  }

  /**
   * The peer's answer to an `accept`. Only the generation reception is actually waiting on may open a
   * socket: an `accepted` that names another generation (or another request) is an old instance's
   * answer, and acting on the wrong one is how a stale connection is let in over a live one (C2).
   */
  function handleAccepted(message) {
    const pending = pendingAdmissions.get(message.generation);
    if (
      !pending ||
      pending.requestId !== message.request_id ||
      pending.connectionId !== message.connection_id
    ) {
      try {
        onDiagnostic({
          market,
          reason: `ignored an acceptance for a generation this process is not waiting on (${message.generation})`,
        });
      } catch {
        // a diagnostic is best-effort
      }
      return { accepted: false, stale: true };
    }
    pendingAdmissions.delete(message.generation);
    if (message.payload?.accepted === false) {
      const reason = message.payload?.reason ?? 'the peer did not accept the connection';
      pending.info.settle?.(false);
      try {
        onDiagnostic({ market, reason: `organize did not accept this connection: ${reason}` });
      } catch {
        // the stop below is the fact that matters
      }
      // A connection nobody admitted is not allowed to be silent either: reception did not start, and
      // the process must hear about it so it can stop and end non-zero.
      onStop({ market, reason: `this connection was not admitted: ${reason}` });
      return { accepted: false, reason };
    }
    pending.info.settle?.(true);
    // Set 5: the new connection's numbering starts over. Everything the previous connection held was
    // released before this acceptance went out (the hand-over gate), so the pump restarts cleanly at
    // the spool's end and the first frame of the new connection opens it again.
    nextToSendSeq = null;
    pumpPos = spool === null ? null : spool.cursor;
    resendMode = 'drain';
    resendWalk = null;
    resendPending = null;
    return { accepted: true };
  }

  /**
   * Organize is durable up to a contiguous ceiling, and the spool may only be released as far as
   * that ceiling covers - in physical order, from the FIFO's head, never past a record that is not
   * itself acknowledged. Set 3: the acknowledgement itself never triggers a resend; it may only
   * open capacity to one (the scheduler's 'capacity-returned' trigger).
   */
  function handleDurableAck(message) {
    const before = organizeCapacity;
    if (typeof message.payload?.capacity === 'string') organizeCapacity = message.payload.capacity;
    const upToSeq = message.payload?.up_to_seq;
    let advanced = false;
    if (fifo !== null && Number.isInteger(upToSeq)) {
      const released = fifo.noteAck({ identity: ackIdentityOf(message), upToSeq });
      if (released.position !== null) {
        spool.advance(released.position);
        if (resendMode === 'recover') {
          // The release moved the cursor under the recovery walk; its next segments may be gone.
          // A drain walk sits ahead of the cursor and is never invalidated by a release.
          resendWalk = null;
          resendPending = null;
        }
        noteAckProgress(message.connection_id, upToSeq);
        advanced = true;
      }
    }
    if (organizeCapacity === 'ok' && before !== 'ok') scheduleResend('capacity-returned');
    // A hand-over waits here: the next generation is announced the moment nothing of the previous
    // one is left unacknowledged.
    tryDeferredAdmission();
    return { advanced, capacity: organizeCapacity };
  }

  /**
   * Set 1/3 (observability + recovery): a sent frame starts the progress clock once, and the
   * deadline is armed once. The deadline firing while frames remain unacknowledged is what reports
   * the stall and asks for a resend; an acknowledgement that moves the ceiling clears it. A
   * same-value acknowledgement or a readiness report does not reset the clock, so a ceiling that
   * stopped moving is what triggers the deadline - not mere quiet.
   */
  const STALL_STOPS_AFTER = 6;

  function noteFrameSent() {
    framesSinceAckProgress += 1;
    armProgressDeadlineIfIdle();
  }

  /**
   * Arm the progress deadline once while there is unacknowledged retention and the link can take
   * frames. The clock starts when the wait starts and restarts on every release; a same-value
   * acknowledgement, a readiness report or a resend does not reset it, so a ceiling that stopped
   * moving - not mere quiet - is what fires it. Only the process closing ends it.
   */
  function armProgressDeadlineIfIdle() {
    if (progressTimer !== null) return;
    if (!Number.isFinite(ackStallMs) || ackStallMs <= 0) return;
    if (closed) return;
    if (fifo === null || fifo.size === 0) return;
    // No capacity gate here: retention that began while the link was full must still age into a
    // report (and into the recovery the report wants) - the expiry itself decides what the link's
    // state allows, and the retry budget only counts the attempts it could have taken.
    progressClockMs = Date.now();
    progressTimer = setTimer(() => {
      progressTimer = null;
      onProgressDeadline();
    }, ackStallMs);
    if (typeof progressTimer?.unref === 'function') progressTimer.unref();
  }

  function onProgressDeadline() {
    if (closed) return;
    if (fifo === null || fifo.size === 0) {
      stalledStrikes = 0;
      return;
    }
    // The report is the fact whether or not the link can take frames: a ceiling that stopped
    // moving is exactly what a reader needs to see, and the retry budget only counts the attempts
    // the link could actually have taken (a full link's stall is the episode report's subject).
    const canTry = organizeCapacity === 'ok';
    if (canTry) stalledStrikes += 1;
    const nowMs = Date.now();
    try {
      onDiagnostic({
        market,
        reason: `the durable ceiling has stalled: ${fifo.size} frames are retained and unacknowledged`,
        unreleasedFrames: fifo.size,
        framesSinceProgress: framesSinceAckProgress,
        stalledMs: nowMs - progressClockMs,
        upToSeq: lastAckUpToSeq,
        capacity: organizeCapacity,
        spoolBytes: spool?.bytes ?? 0,
        strikes: stalledStrikes,
      });
    } catch {
      // a diagnostic is best-effort
    }
    if (canTry) {
      enterRecovery();
      scheduleResend();
    } else {
      // The link cannot take frames: the recovery is wanted, and the moment the link opens is the
      // moment it runs (the readiness transition or the next attempt takes it).
      recoveryWanted = true;
    }
    if (canTry && stalledStrikes >= STALL_STOPS_AFTER) {
      // A release has not come for the whole retry budget while the link kept accepting resends:
      // the retention cannot be drained here. Stopping loudly beats holding a hand-over (or a
      // board) for ever - the answer to a cause that cannot be recovered from in place.
      stopReception(`the retained frames could not be acknowledged after ${stalledStrikes} attempts`);
      return;
    }
    armProgressDeadlineIfIdle();
  }

  /**
   * Switch the pump to a recovery walk: what was lost between the hops cannot be known, so
   * everything unacknowledged goes again, in order, from the first unreleased record. A recovery
   * already under way continues where it stopped.
   */
  function enterRecovery() {
    recoveryWanted = false;
    if (resendMode !== 'recover' || resendWalk === null) {
      resendMode = 'recover';
      resendWalk = null;
      resendPending = null;
    }
    return true;
  }

  function clearProgressDeadline() {
    if (progressTimer !== null) {
      clearTimer(progressTimer);
      progressTimer = null;
    }
  }

  /**
   * Set 1/3: an acknowledgement that releases from the FIFO is progress - it restarts the stall
   * clock and the retry budget. A same-value acknowledgement, or one that releases nothing (a
   * stale value, another identity's range), does not, so a ceiling that stops moving is reported
   * rather than masked by repetition and a stalled hand-over cannot be silenced by irrelevant
   * acknowledgements. The clock is per connection: a new connection's releases start a fresh one.
   */
  function noteAckProgress(connectionId, upToSeq) {
    if (connectionId !== ackConnectionId) {
      ackConnectionId = connectionId;
      lastAckUpToSeq = null;
      framesSinceAckProgress = 0;
    }
    if (lastAckUpToSeq !== null && upToSeq <= lastAckUpToSeq) return;
    lastAckUpToSeq = upToSeq;
    framesSinceAckProgress = 0;
    stalledStrikes = 0;
    clearProgressDeadline();
    armProgressDeadlineIfIdle();
  }

  /**
   * Set 3: the single resend scheduler. Everything the spool still holds is offered again for
   * exactly four reasons - the link became ready (attached, or its capacity returned), a hand-over
   * is waiting for the previous connection to drain, the progress deadline expired with frames still
   * unacknowledged, or a peer asked explicitly. An acknowledgement never triggers one. A run is
   * bounded to a window of frames so the event loop gets a turn between windows, and a send the
   * link refuses stops the window where it is - the rest waits for the next opportunity instead of
   * the head being re-sent over and over.
   */
  const RESEND_WINDOW_FRAMES = 256;

  function scheduleResend() {
    // Quiescence is not a reason to stop: sealing fences new reception, and the retained frames
    // still owe an acknowledgement-driven drain. Only closing the process ends resends.
    if (closed) return 0;
    if (resendRunning || resendTimer !== null) {
      resendQueued = true;
      return 0;
    }
    return runResendWindow();
  }

  function runResendWindow() {
    if (closed) return 0;
    if (spool === null || spool.bytes === 0) {
      resendWalk = null;
      resendPending = null;
      return 0;
    }
    if (organizeChannelRef === null || organizeCapacity !== 'ok') return 0;
    // The deadline watches this drain attempt itself: a window whose every offer is refused must
    // still age into a report and the retry budget, not wait for some later event to notice.
    armProgressDeadlineIfIdle();
    resendRunning = true;
    let sent = 0;
    let budget = RESEND_WINDOW_FRAMES;
    try {
      // The walk is kept across windows and across a refusal: it resumes exactly where the link
      // stopped taking frames, so a bounded window never re-sends the head while the rest waits.
      // A record already drawn from the walk and then refused is held in `resendPending` and tried
      // first next time - a refusal never advances past a record.
      if (resendWalk === null) {
        resendPending = null;
        // Drain continues from where the last send stopped; recovery walks again from the first
        // unreleased record, because what was lost between the hops cannot be known.
        resendWalk =
          resendMode === 'recover'
            ? spool.drainRecords({ from: spool.cursor })
            : spool.drainRecords({ from: pumpPos ?? spool.cursor });
      }
      while (budget > 0) {
        if (resendPending !== null) {
          if (!sendRecord(resendPending)) break; // still refused: held for the next window
          resendPending = null;
          sent += 1;
          budget -= 1;
          continue;
        }
        const step = resendWalk.next();
        if (step.done === true) {
          resendWalk = null;
          resendMode = 'drain'; // the walk finished: the pump continues from its end position
          break;
        }
        budget -= 1;
        const record = step.value;
        const seq = record.envelope?.receive_seq;
        if (resendMode === 'drain' && Number.isInteger(seq) && Number.isInteger(nextToSendSeq) && seq < nextToSendSeq) {
          continue; // already sent while this walk was parked (the direct path): never sent twice
        }
        if (!sendRecord(record)) {
          resendPending = record;
          break;
        }
        sent += 1;
      }
    } catch (error) {
      // A record the spool cannot hand back is not one this walk may guess at: it stops where it is,
      // leaves the spool untouched, and says so rather than taking the organize link down with it.
      resendWalk = null;
      resendPending = null;
      try {
        onDiagnostic({ market, reason: `the spool could not hand back a record: ${error.message}` });
      } catch {
        // a diagnostic is best-effort; it must never escape into the delivery path
      }
    } finally {
      resendRunning = false;
    }
    if (!closed && budget === 0 && resendWalk !== null) {
      // The window was full: the walk has more, and the loop yields before the next window.
      resendTimer = setTimer(() => {
        resendTimer = null;
        runResendWindow();
      }, 0);
      if (typeof resendTimer?.unref === 'function') resendTimer.unref();
    } else if (resendQueued) {
      resendQueued = false;
      runResendWindow();
    }
    return sent;
  }

  /**
   * One send out of the pump: the same link and the same accounting as a first send, and the record
   * it confirmed becomes the pump's position and the next sequence.
   */
  function sendRecord(record) {
    if (!trySendResend(record.envelope)) return false;
    pumpPos = { segment: record.segment, offset: record.offset };
    if (Number.isInteger(record.envelope.receive_seq)) nextToSendSeq = record.envelope.receive_seq + 1;
    return true;
  }

  /** One re-send: the same link and the same accounting as a first send, only the counter differs. */
  function trySendResend(envelope) {
    let ok = false;
    try {
      ok = organizeChannelRef.sendEnvelope(envelope);
    } catch (error) {
      try {
        onDiagnostic({ market, reason: `the organize link refused a resend: ${error.message}` });
      } catch {
        // a diagnostic is best-effort; it must never escape into the delivery path
      }
      ok = false;
    }
    if (!ok) return false;
    resentFrames += 1;
    linkHeldReported = false;
    armProgressDeadlineIfIdle();
    return true;
  }

  function handleResend(message) {
    // An explicit resend re-offers what is unacknowledged, in order, from the first unreleased
    // record: a recovery, not a drain.
    resendMode = 'recover';
    resendWalk = null;
    resendPending = null;
    return { resent: scheduleResend() };
  }

  /**
   * A role announced a new instance of itself. Recorded as the peer's identity - and nothing else: a
   * book or organize restart is a fact about that child's process, and the receive run and the
   * connection generation must not move with it (ruling ⑧).
   */
  function handleHello(message) {
    const payloadRole = message.payload?.role;
    const role = typeof payloadRole === 'string' && payloadRole.length > 0 ? payloadRole : String(message.role_instance).split('-')[0];
    peerInstances.set(role, message.role_instance);
    return { role, roleInstance: message.role_instance };
  }

  function handleReadiness(message) {
    const before = organizeCapacity;
    if (typeof message.payload?.capacity === 'string') organizeCapacity = message.payload.capacity;
    if (message.payload?.ready === false) organizeCapacity = 'down';
    // Set 3: a capacity that has just come back is the moment the retained frames can move again. A
    // repeated 'ok' is not - re-sending on every readiness report would duplicate a window that is
    // already waiting for its acknowledgement.
    let resent = 0;
    if (organizeCapacity === 'ok' && before !== 'ok') {
      // A stall that was seen while the link was full runs its recovery now that the link is back.
      if (recoveryWanted) enterRecovery();
      resent = scheduleResend();
    }
    return { capacity: organizeCapacity, resent };
  }

  function handleErrorReport(message) {
    onDiagnostic({ market, reason: `organize reported an error: ${message.payload?.reason ?? 'unknown'}` });
    return { seen: true };
  }

  /**
   * The organize link failed. From here frames cannot be handed on, so they are spooled until it is
   * back rather than dropped - the same answer as a full queue, for the same reason.
   */
  function handleError(error) {
    organizeCapacity = 'down';
    onDiagnostic({ market, reason: `the organize link failed: ${error?.message ?? error}` });
  }

  /**
   * C3 over IPC: a subscription that failed (a refusal or an ack deadline that passed) is a fact the
   * run has to face, and it travels on the vocabulary stage 1 defined - `readiness` says the role is
   * not ready, and `error` carries the reason on the diagnostic path.
   */
  function handleSubscriptions(info) {
    const connectionId = info?.connectionId ?? connection.connectionId;
    const generation = info?.generation ?? connection.generation;
    if (info?.state === 'failed') {
      sendControlBestEffort({
        version: IPC_VERSION,
        type: 'readiness',
        role_instance: instance,
        payload: {
          role: 'ingest',
          ready: false,
          state: 'failed',
          reason: info.reason ?? 'the subscription failed',
          connection_id: connectionId,
          generation,
          market,
          stream,
        },
      });
      sendControlBestEffort({
        version: IPC_VERSION,
        type: 'error',
        role_instance: instance,
        payload: {
          role: 'ingest',
          reason: info.reason ?? 'the subscription failed',
          connection_id: connectionId,
          generation,
          market,
          stream,
        },
      });
      return;
    }
    if (info?.state === 'acknowledged') {
      sendControlBestEffort({
        version: IPC_VERSION,
        type: 'readiness',
        role_instance: instance,
        payload: { role: 'ingest', ready: true, connection_id: connectionId, generation, market, stream },
      });
    }
  }

  function sendControlBestEffort(message) {
    if (organizeChannelRef === null) return false;
    try {
      return organizeChannelRef.sendControl(message);
    } catch (error) {
      try {
        onDiagnostic({ market, reason: `a control message could not be sent: ${error.message}` });
      } catch {
        // the candidate remains local evidence, not a control-channel acknowledgement
      }
      return false;
    }
  }

  /** Return a local tail snapshot after intake has been fenced. */
  function finalTailCandidate() {
    if (!quiescing || tailWriteFailure !== null || unretainedFrameFailure) return null;
    if (spool !== null && (spool.bytes !== 0 || spool.failed !== null)) return null;
    const tails = Object.freeze(
      store
        .receivedTails()
        .filter((row) => row.market === market && (row.stream === stream || row.stream === ''))
        .map((row) => Object.freeze({ connectionId: row.connectionId, lastReceivedSeq: row.lastReceivedSeq })),
    );
    return Object.freeze({ tails, spoolEmpty: true });
  }

  /**
   * Fence intake, then return the local final-tail candidate if every local receipt is trustworthy.
   * Sending this candidate is only a request; neither a successful nor refused IPC enqueue is an ACK.
   */
  async function sealTails() {
    await quiesce();
    // Set 6b: the released-but-unsaved position holds `bytes` above zero until the save runs, and
    // `finalTailCandidate` reads exactly that to decide the spool is empty. The save is what turns
    // "every acknowledgement arrived" into "the spool says so"; without it the seal would wait for
    // a clock that has nothing left to wait for, and the run could never be reported complete.
    if (spool !== null) {
      try {
        spool.saveCursor();
      } catch (error) {
        onDiagnostic({ market, reason: `the cursor could not be saved before sealing: ${error.message}` });
        return null;
      }
    }
    // Set 6d: the candidate is read from the store, so the pending positions must be written down
    // first - a seal that judged a tail the store does not hold yet would claim less than this
    // process heard (or, worse, nothing of the last clock's reception).
    try {
      flushTails();
    } catch (error) {
      onDiagnostic({ market, reason: `the receive tail could not be written before sealing: ${error.message}` });
      return null;
    }
    const candidate = finalTailCandidate();
    if (candidate === null) return null;
    sendControlBestEffort(
      makeMessage({
        version: IPC_VERSION,
        type: 'tail_sealed',
        role_instance: instance,
        run_id: runId,
        payload: { tails: candidate.tails, spool_empty: true },
      }),
    );
    return candidate;
  }

  /**
   * The old spool, drained oldest-first and handed back through the ordinary organize link (startup (c)).
   * `force` because this runs before any acknowledgement has been heard; only the cursor moves on an
   * acknowledgement, so nothing here is released by the send.
   */
  function drainSpool() {
    return { resent: scheduleResend() };
  }

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

  // Stage 5c: the periodic readiness report. It travels the ordinary control path to the supervisor's
  // router, which observes it (it is not relayed to a peer). The interval is unref'd so a process that
  // is otherwise quiet can still let the loop go, and it is cleared the moment reception stops or the
  // process closes - a stopped role does not keep reporting itself ready.
  let readinessTimer = null;

  function readinessPayload() {
    return {
      role: 'ingest',
      ready: !closed && !stopped,
      connection_id: connection.connectionId,
      generation: connection.generation,
      market,
      stream,
    };
  }

  function startReadinessReporting() {
    if (!Number.isFinite(readinessIntervalMs) || readinessIntervalMs <= 0) return;
    readinessTimer = setInterval(() => {
      sendControlBestEffort(
        makeMessage({
          version: IPC_VERSION,
          type: 'readiness',
          role_instance: instance,
          payload: readinessPayload(),
        }),
      );
    }, readinessIntervalMs);
    if (typeof readinessTimer.unref === 'function') readinessTimer.unref();
  }

  function stopReadinessReporting() {
    if (readinessTimer !== null) {
      clearInterval(readinessTimer);
      readinessTimer = null;
    }
  }

  startReadinessReporting();

  return {
    /** The consume side of the organize link, wired by whoever opened the channel. */
    handleControl,
    /** The link to organize drained: the retained frames may move again. */
    handleLinkDrain: () => ({ resent: scheduleResend() }),
    handleError,

    /** Announce this process to the supervisor. */
    announceHello,

    /** Seal the final tails on the reception side (startup stop, ruling ⑨⑩). */
    sealTails,
    /** Drain the old spool oldest-first through the ordinary organize link (startup (c)). */
    drainSpool,
    /** Every receive tail this process has written, as a copy. */
    receivedTails: () => store.receivedTails(),

    /** Attach (or replace) the organize channel. A generation that waited for a link is announced now. */
    attachOrganize(channel) {
      organizeChannelRef = channel;
      announceHello();
      for (const pending of pendingAdmissions.values()) {
        if (deferredAdmission !== null && pending.generation === deferredAdmission.generation) continue;
        announceAccept(pending.message);
      }
      if (organizeCapacity === 'ok') {
        // A stall that was seen while the link was gone runs its recovery now that it is back.
        if (recoveryWanted) enterRecovery();
        scheduleResend();
      }
      tryDeferredAdmission();
      return true;
    },

    start() {
      if (closed) return { started: false, reason: 'this ingest process is closed' };
      if (quiescing || stopped) return { started: false, reason: 'this ingest process has stopped' };
      connection.start();
      // Set 8: the auxiliary poller starts with reception. Its timer is unref'd, so a run that has
      // nothing else to do can still let the loop go.
      aux?.start();
      return { started: true };
    },

    /**
     * Fence new socket events and reconnects without closing the spool. The production fork executes
     * accepted receive callbacks synchronously; the async boundary lets a reentrant caller await the
     * remainder of the current JavaScript callback before inspecting its tail.
     */
    quiesce,

    /** Fence new reception after accepted synchronous callbacks, then close the resumable spool. */
    async stop() {
      if (closed) return { stopped: false, reason: 'this ingest process is closed' };
      await quiesce();
      // Set 6d: the last clock's positions belong to the store before this life ends; a failure
      // is not fatal here - it latches, and the stop's own result reports it.
      try {
        flushTails();
      } catch {
        // the latch is the fact
      }
      // Set 7a: the last window's raw rows belong on disk before this life ends - a run that stops
      // without confirming them would leave frames it heard unrecorded. The failure is kept, not
      // swallowed: it is carried into the stop's own result below.
      try {
        raw?.flush();
      } catch (error) {
        if (rawFlushFailure === null) rawFlushFailure = error;
      }
      spool?.close();
      // Set 8: stop the auxiliary poller - no row may be produced after the process is closing.
      aux?.close();
      const failure = receptionCloseFailure ?? rawFlushFailure ?? tailWriteFailure ?? spool?.failed;
      const abnormal = failure != null || unretainedFrameFailure;
      return {
        stopped: receptionCloseFailure === null,
        abnormal,
        ...(abnormal ? { reason: failure?.message ?? 'a received frame could not be retained' } : {}),
      };
    },

    /** The termination of the process: reception, the spool, and the store it opened itself. */
    close() {
      if (closed) return;
      fenceReception();
      if (admissionRetryTimer !== null) {
        clearTimer(admissionRetryTimer);
        admissionRetryTimer = null;
      }
      if (tailSaveTimer !== null) {
        clearTimer(tailSaveTimer);
        tailSaveTimer = null;
      }
      try {
        // Best effort: a tail that could not be written keeps its latch, and the store is closing
        // either way.
        flushTails();
      } catch {
        // the latch is the fact
      }
      spool?.close();
      // Set 8: closing stops the auxiliary poller too.
      aux?.close();
      // Set 7a: closing the raw flushes the last window and closes the databases. A failure here means
      // the last rows did not land, and a run that ends without recording what it heard must not be
      // reported as a clean shutdown: the failure is reported and rethrown (after the stores are
      // closed) so the role exits non-zero.
      let rawCloseFailure = null;
      try {
        raw?.close();
      } catch (error) {
        rawCloseFailure = error;
        try {
          onDiagnostic({ market, reason: `the canonical raw could not be flushed at close: ${error.message}` });
        } catch {
          // a diagnostic must not replace the failure
        }
      }
      if (openedStoreHere) {
        store.close();
      }
      closed = true;
      if (rawCloseFailure !== null) throw rawCloseFailure;
    },

    get roleInstance() {
      return instance;
    },
    get runId() {
      return runId;
    },
    get market() {
      return market;
    },
    get stream() {
      return stream;
    },
    get generation() {
      return connection.generation;
    },
    get connectionId() {
      return connection.connectionId;
    },
    get state() {
      return connection.state;
    },
    get subscriptionState() {
      return connection.subscriptionState;
    },
    get subscriptionFailure() {
      return connection.subscriptionFailure;
    },
    get receiveSeq() {
      return connection.receiveSeq;
    },
    get organizeCapacity() {
      return organizeCapacity;
    },
    /** The restart id each role last announced, as a copy a caller cannot edit. */
    get peerInstances() {
      return new Map(peerInstances);
    },
    get spool() {
      return spoolView;
    },
    get ingestStore() {
      return store;
    },
    get stats() {
      return {
        market,
        roleInstance: instance,
        generation: connection.generation,
        connectionId: connection.connectionId,
        state: connection.state,
        subscriptionState: connection.subscriptionState,
        receiveSeq: connection.receiveSeq,
        capacity: organizeCapacity,
        sentFrames,
        spooledFrames,
        resentFrames,
        spoolBytes: spool?.bytes ?? 0,
        unreleasedFrames: fifo?.size ?? 0,
        framesSinceProgress: framesSinceAckProgress,
        stalledMs: (fifo?.size ?? 0) > 0 && progressClockMs > 0 ? Date.now() - progressClockMs : 0,
        lastAckUpToSeq,
        stopped,
      };
    },
  };
}

/**
 * Open the ingest process against an organize socket path: this is the process entrance the supervisor
 * will call in stage 5 (and the tests call now). The channel is connected first, so construction has a
 * live link and the first generation's `accept` can go out immediately.
 */
export async function openIngestProcess(options) {
  assertInlineReceiveExecutor(options);
  const { organizeSocketPath = null, channelOptions = {}, ...rest } = options;
  let process = null;
  const channel = organizeSocketPath
    ? await connect(organizeSocketPath, {
        ...channelOptions,
        onControl: (message) => process?.handleControl(message),
        onError: (error) => process?.handleError(error),
        onDrain: () => process?.handleLinkDrain(),
      })
    : null;
  process = createIngestProcess({ ...rest, organizeChannel: channel });
  if (channel !== null) process.announceHello();
  return process;
}
