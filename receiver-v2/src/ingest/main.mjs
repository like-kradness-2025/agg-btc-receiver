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
import { openIngestStore } from './store.mjs';
import { IPC_VERSION, makeMessage } from '../ipc-message.mjs';
import { attachChanges, deriveChanges } from '../changes.mjs';
import { connect } from '../ipc.mjs';

/**
 * Build the ingest process around an already-open organize channel.
 *
 * The channel is passed in rather than opened here so construction stays synchronous, exactly like the
 * structure's: a caller (the supervisor in stage 5, a test now) connects the socket and wires its
 * `onControl`/`onError` to `handleControl`/`handleError`. A channel that is not up yet may be attached
 * later with `attachOrganize`, which is how a process that started before organize learns where to
 * send the acceptance reception is waiting for.
 */
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
  ...receiveOptions
} = {}) {
  if (!adapter) throw new TypeError('the ingest process needs a venue adapter');
  if (!runId) throw new TypeError('the ingest process needs a run id');
  if (!venue) throw new TypeError('the ingest process needs a venue');
  const instance = roleInstance ?? `ingest-${runId}`;

  const openedStoreHere = ingestStore === null;
  const store = ingestStore ?? (ingestStorePath ? openIngestStore({ path: ingestStorePath }) : null);
  if (store === null) throw new TypeError('the ingest process needs an ingest store (a path or an open store)');

  const spool = spoolDir ? createSpool({ dir: spoolDir, ...spoolOptions }) : null;

  let organizeChannelRef = organizeChannel;
  // What organize last told us about its capacity. Anything but 'ok' means the frame cannot be handed
  // on right now, so the spool is the rung it goes to. A channel that is not up is the same answer.
  let organizeCapacity = 'ok';
  let stopped = false;
  let closed = false;
  // The generations reception is waiting for an acceptance on, keyed by generation. Only these may be
  // settled by an `accepted`; anything else is a stale instance's answer (C2).
  const pendingAdmissions = new Map();
  // The restart id each role last announced, by role. A peer restart is recorded here and changes
  // nothing about the receive run or the connection generation (⑥).
  const peerInstances = new Map();
  let sentFrames = 0;
  let spooledFrames = 0;
  let resentFrames = 0;

  const connection = createReceiveConnection({
    ...receiveOptions,
    adapter,
    market,
    runId,
    venue,
    webSocketImpl,
    onEnvelope: (envelope) => {
      // (④) the receive tail is written on the reception side, before anything else: a frame the board
      // refuses was still received, and "received" is a fact about reception, not a verdict downstream.
      writeReceivedTail(envelope);
      return deriveAndSend(envelope);
    },
    onGeneration: handleGeneration,
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

  /** Record how far this process can show it heard, per connection and board. */
  function writeReceivedTail(envelope) {
    try {
      store.updateReceivedTail({
        connectionId: envelope.connection_id,
        market,
        stream: envelope.stream ?? stream,
        lastReceivedSeq: envelope.receive_seq,
        lastRecvMonoNs: envelope.recv_mono_ns,
      });
    } catch (error) {
      onDiagnostic({ market, reason: `the receive tail could not be written: ${error.message}` });
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
      onDiagnostic({ market, reason: `the level changes were refused: ${derived.reason}` });
      onGap({ market, reason: derived.reason, seq: envelope.receive_seq });
      return { accepted: false, reason: derived.reason };
    }
    return sendOrSpool(attachChanges(envelope, derived));
  }

  /** The frames this process hands on are the ones with a derived, validated changes block. */

  /**
   * Hand one frame to organize if the link is up and has capacity; otherwise spool it. False back from
   * the channel means the same thing it means in `ipc.mjs`: the queue is over its bound and the frame
   * must be kept elsewhere, never dropped.
   */
  function sendOrSpool(envelope) {
    if (closed) return { accepted: false, reason: 'this ingest process is closed' };
    if (organizeChannelRef !== null && organizeCapacity === 'ok') {
      let ok = false;
      try {
        ok = organizeChannelRef.sendEnvelope(envelope);
      } catch (error) {
        onDiagnostic({ market, reason: `the organize link refused a frame: ${error.message}` });
        ok = false;
      }
      if (ok) {
        sentFrames += 1;
        return { accepted: true, sent: true };
      }
    }
    return spoolFrame(envelope);
  }

  /** Append one frame to the spool. False from the spool is a stop signal, not a reason to drop it. */
  function spoolFrame(envelope) {
    if (spool !== null && spool.append(envelope) && !spool.failed) {
      spooledFrames += 1;
      return { accepted: true, spooled: true };
    }
    stopReception('nothing could hold the frame');
    onGap({ market, reason: 'the organize link could not take the frame and the spool could not hold it', seq: envelope.receive_seq });
    return { accepted: false, reason: 'nothing could hold the frame' };
  }

  /**
   * Stop this process receiving, for good. Reception is closed as well as flagged: a stopped process
   * would refuse every further frame, and a socket left open would keep delivering frames that go
   * nowhere. Turning this into a non-zero exit is the supervisor's job in stage 5.
   */
  function stopReception(reason) {
    if (stopped || closed) return;
    stopped = true;
    stopReadinessReporting();
    try {
      connection.stop();
    } catch (error) {
      try {
        onDiagnostic({ market, reason: `reception could not be closed: ${error.message}` });
      } catch {
        // the stop below is the fact that matters
      }
    }
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
    if (closed) return false;
    const message = acceptMessage(info);
    pendingAdmissions.set(info.generation, {
      requestId: message.request_id,
      connectionId: info.connectionId,
      info,
      message,
    });
    announceAccept(message);
    return undefined;
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
    return { accepted: true };
  }

  /**
   * Organize is durable up to a contiguous ceiling. The spool may only move past what that ceiling
   * covers, oldest first, and only a contiguous run may be released - moving over a record that was
   * not consumed would delete it. What is left after the move is offered again when the peer has
   * capacity, so a frame that could not be handed on does not wait for a resend nobody asked for.
   */
  function handleDurableAck(message) {
    if (typeof message.payload?.capacity === 'string') organizeCapacity = message.payload.capacity;
    const upToSeq = message.payload?.up_to_seq;
    const advanced = Number.isInteger(upToSeq) ? advanceSpoolTo(message.connection_id, upToSeq) : false;
    const resent = organizeCapacity === 'ok' ? resendSpool({ connectionId: message.connection_id }) : 0;
    return { advanced, resent, capacity: organizeCapacity };
  }

  /**
   * Move the spool cursor to the end of the last record the acknowledgement covers, for the connection
   * it names. `advance` deletes every segment entirely behind that position, which is what keeps the
   * spool bounded; a position inside a segment deletes nothing, so a partially-consumed segment is
   * never discarded.
   */
  function advanceSpoolTo(connectionId, upToSeq) {
    if (spool === null) return false;
    let last = null;
    try {
      for (const record of spool.drainRecords()) {
        const envelope = record.envelope;
        if (envelope.connection_id !== connectionId) continue;
        if (envelope.receive_seq > upToSeq) break;
        last = { segment: record.segment, offset: record.offset };
      }
    } catch (error) {
      // A segment whose bytes do not describe the records it claims is not something this walk may
      // guess at; it stops where it is and says so rather than letting the read take down the link.
      onDiagnostic({ market, reason: `the spool could not be walked to advance the cursor: ${error.message}` });
      return false;
    }
    if (last === null) return false;
    spool.advance(last);
    return true;
  }

  /**
   * Offer what the spool still holds, oldest segment first and within a segment in write order, so a
   * resend after a reconnect cannot overtake data that was already waiting. The cursor is not moved
   * here: only an acknowledgement moves it. A send that reports backpressure stops the walk where it
   * is, leaving the rest for the next opportunity rather than dropping it.
   */
  function resendSpool({ connectionId = null, force = false } = {}) {
    if (spool === null || spool.bytes === 0) return 0;
    if (organizeChannelRef === null || (!force && organizeCapacity !== 'ok')) return 0;
    let sent = 0;
    try {
      for (const envelope of spool.drain()) {
        if (connectionId !== null && envelope.connection_id !== connectionId) continue;
        let ok = false;
        try {
          ok = organizeChannelRef.sendEnvelope(envelope);
        } catch (error) {
          onDiagnostic({ market, reason: `the organize link refused a resend: ${error.message}` });
          ok = false;
        }
        if (!ok) break;
        sent += 1;
      }
    } catch (error) {
      // A record the spool cannot hand back is not one this walk may guess at: it stops where it is,
      // leaves the spool untouched, and says so rather than taking the organize link down with it.
      onDiagnostic({ market, reason: `the spool could not hand back a record: ${error.message}` });
    }
    resentFrames += sent;
    return sent;
  }

  function handleResend(message) {
    return { resent: resendSpool({ connectionId: message.connection_id, force: true }) };
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
    if (typeof message.payload?.capacity === 'string') organizeCapacity = message.payload.capacity;
    if (message.payload?.ready === false) organizeCapacity = 'down';
    const resent = organizeCapacity === 'ok' ? resendSpool({ connectionId: null }) : 0;
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
      onDiagnostic({ market, reason: `a control message could not be sent: ${error.message}` });
      return false;
    }
  }

  /**
   * The final tails, sealed on the reception side (ruling ⑨⑩). After reception has stopped this is the
   * list organize judges "all acknowledged" against: every connection this process wrote a tail for,
   * with the last sequence it received, plus whether the spool is empty. The tail is a reception fact
   * only - it does not claim durability or application - and organize is the one that decides.
   */
  function sealTails() {
    const tails = store
      .receivedTails()
      .filter((row) => row.market === market && (row.stream === stream || row.stream === ''))
      .map((row) => ({ connectionId: row.connectionId, lastReceivedSeq: row.lastReceivedSeq }));
    const spoolEmpty = spool === null ? true : spool.bytes === 0;
    sendControlBestEffort(
      makeMessage({
        version: IPC_VERSION,
        type: 'tail_sealed',
        role_instance: instance,
        run_id: runId,
        payload: { tails, spool_empty: spoolEmpty },
      }),
    );
    return { tails, spoolEmpty };
  }

  /**
   * The old spool, drained oldest-first and handed back through the ordinary organize link (startup (c)).
   * `force` because this runs before any acknowledgement has been heard; only the cursor moves on an
   * acknowledgement, so nothing here is released by the send.
   */
  function drainSpool() {
    return { resent: resendSpool({ connectionId: null, force: true }) };
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
      for (const pending of pendingAdmissions.values()) announceAccept(pending.message);
      if (organizeCapacity === 'ok') resendSpool({ connectionId: null });
      return true;
    },

    start() {
      if (closed) return { started: false, reason: 'this ingest process is closed' };
      if (stopped) return { started: false, reason: 'this ingest process has stopped' };
      connection.start();
      return { started: true };
    },

    /** A clean stop: reception ends and the spool is closed. */
    stop() {
      if (closed) return;
      stopReadinessReporting();
      connection.stop();
      spool?.close();
    },

    /** The termination of the process: reception, the spool, and the store it opened itself. */
    close() {
      if (closed) return;
      stopReadinessReporting();
      connection.stop();
      spool?.close();
      if (openedStoreHere) {
        try {
          store.close();
        } catch {
          // a store that will not close is not this close's failure to report
        }
      }
      closed = true;
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
  const { organizeSocketPath = null, channelOptions = {}, ...rest } = options;
  let process = null;
  const channel = organizeSocketPath
    ? await connect(organizeSocketPath, {
        ...channelOptions,
        onControl: (message) => process?.handleControl(message),
        onError: (error) => process?.handleError(error),
      })
    : null;
  process = createIngestProcess({ ...rest, organizeChannel: channel });
  if (channel !== null) process.announceHello();
  return process;
}
