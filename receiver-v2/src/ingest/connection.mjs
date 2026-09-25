/**
 * Reception: one connection at a time, stamped so nothing downstream has to guess.
 *
 * Everything here is venue-agnostic. The venue-specific knowledge lives behind the adapter the
 * caller passes: a url, how to subscribe, how to keep the connection alive, and how to read a
 * message. This module owns the parts that are the same everywhere and that the rest of the
 * structure depends on:
 *
 *  - the connection generation: reception is the only thing that issues one, and it moves forward
 *    every time a socket is replaced. Nothing downstream is allowed to invent its own idea about
 *    which connection is current.
 *  - the receive metadata: recv_ts_ms is stamped at the socket boundary and never revised,
 *    recv_mono_ns comes from a monotonic clock and is strictly increasing within one connection, and
 *    receive_seq is monotonic per connection. Those three are what make the raw replayable and the
 *    dedupe meaningful.
 *  - the link's health, judged by silence rather than by message rate: a connection is dead when
 *    nothing has been heard for the deadline, whether that is a missing pong or simply no data. The
 *    deadlines come from measurement, not preference.
 *  - reconnect with a stability window: attempts only reset after the link has actually held, so a
 *    flapping venue backs off instead of spinning.
 *
 * Subscription acknowledgements are tracked because "we asked" and "they agreed" are different
 * states: a connection that has not been acknowledged is not usable yet, and that difference is
 * recorded rather than assumed.
 */

import { makeEnvelope } from '../envelope.mjs';

export const DEFAULT_SILENCE_DEADLINE_MS = 15_000; // measured: a real stall runs 20s+, normal gaps do not
export const DEFAULT_STABILITY_MS = 60_000; // attempts reset only after the link has held this long

const UNSUBSCRIBED = 'unsubscribed';
const PENDING = 'pending';
const ACKNOWLEDGED = 'acknowledged';
const FAILED = 'failed';

export function createReceiveConnection({
  adapter,
  market,
  runId = null,
  venue = null,
  webSocketImpl,
  openSocket = (url) => new webSocketImpl(url),
  monotonicNs = () => Number(process.hrtime.bigint()),
  wallClockMs = () => Date.now(),
  silenceDeadlineMs = DEFAULT_SILENCE_DEADLINE_MS,
  stabilityMs = DEFAULT_STABILITY_MS,
  setTimer = setTimeout,
  clearTimer = clearTimeout,
  maxBackoffMs = 30_000,
  onEnvelope = () => {},
  onGeneration = () => {},
  onFailure = () => {},
  onSubscriptions = () => {},
  onState = () => {},
  onDiagnostic = () => {},
}) {
  if (!adapter || typeof adapter.parse !== 'function') {
    throw new TypeError('reception needs an adapter that can parse a message');
  }
  if (!webSocketImpl) throw new TypeError('reception needs a websocket implementation');

  let generation = 0;
  let connectionId = null;
  let socket = null;
  let receiveSeq = 0;
  let subscriptionState = UNSUBSCRIBED;
  let lastHeardMs = 0;
  let silentTimer = null;
  let stabilityTimer = null;
  let attempts = 0;
  let closed = false;
  let state = 'idle';
  const subscriptions = new Map();

  function setState(next, detail = '') {
    if (state === next) return;
    state = next;
    onState({ market, state: next, generation, detail });
  }

  function clearSilenceTimer() {
    if (silentTimer !== null) {
      clearTimer(silentTimer);
      silentTimer = null;
    }
  }

  /** Judged by silence, not by volume: no data and no heartbeat for the deadline means dead. */
  function armSilenceTimer() {
    clearSilenceTimer();
    silentTimer = setTimer(() => {
      silentTimer = null;
      if (closed) return;
      onDiagnostic({
        market,
        generation,
        reason: 'silence deadline passed',
        silentMs: wallClockMs() - lastHeardMs,
        deadlineMs: silenceDeadlineMs,
      });
      replaceSocket('silence');
    }, silenceDeadlineMs);
    if (typeof silentTimer.unref === 'function') silentTimer.unref();
  }

  function clearStabilityTimer() {
    if (stabilityTimer !== null) {
      clearTimer(stabilityTimer);
      stabilityTimer = null;
    }
  }

  /**
   * Attempts only reset once the link has held for the stability window. Resetting on open is how a
   * flapping venue turns into a reconnect loop, which is a measured failure of this system's past.
   */
  function armStabilityTimer() {
    clearStabilityTimer();
    stabilityTimer = setTimer(() => {
      stabilityTimer = null;
      if (closed) return;
      attempts = 0;
      onDiagnostic({ market, generation, reason: 'link held long enough to reset attempts' });
    }, stabilityMs);
    if (typeof stabilityTimer.unref === 'function') stabilityTimer.unref();
  }

  function noteHeard() {
    lastHeardMs = wallClockMs();
    armSilenceTimer();
  }

  function stamp(raw) {
    receiveSeq += 1;
    // The envelope is built by the one factory that knows the identity, never assembled here. When a
    // run and a venue are configured the factory derives a run-scoped connection id, so a restarted
    // process cannot take over the name of a connection that has already been written down; the
    // generation travels with the envelope instead of riding along as a loose extra.
    const derivedIdentity = runId && venue;
    return makeEnvelope({
      market,
      stream: adapter.stream ?? 'unknown',
      connectionId: derivedIdentity ? null : connectionId,
      runId,
      venue,
      generation,
      receiveSeq,
      recvTsMs: wallClockMs(),
      recvMonoNs: monotonicNs(),
      raw,
      meta: {
        // Downstream needs to know where this connection's stream begins, otherwise a book cannot
        // anchor its boundary and would be starting from a guess.
        first_seq: firstSeq,
        venue_seq: adapter.venueSeqOf ? adapter.venueSeqOf(raw) : undefined,
      },
    });
  }

  let firstSeq = 1;

  function handleMessage(raw) {
    noteHeard();
    let parsed;
    try {
      parsed = adapter.parse(raw);
    } catch (error) {
      // A frame that cannot be understood is not data; it is also not fatal by itself, but it is
      // counted so a venue changing its format shows up as a number rather than a mystery.
      onDiagnostic({ market, generation, reason: 'unparsable frame', error: String(error) });
      return;
    }
    if (!parsed) return;

    if (parsed.kind === 'heartbeat') {
      if (parsed.answered) onDiagnostic({ market, generation, reason: 'heartbeat answered' });
      return;
    }
    if (parsed.kind === 'subscription') {
      const entry = subscriptions.get(parsed.key) ?? { state: PENDING, atMs: wallClockMs() };
      subscriptions.set(parsed.key, {
        state: parsed.ok ? ACKNOWLEDGED : FAILED,
        atMs: wallClockMs(),
        detail: parsed.detail ?? '',
        askedAtMs: entry.askedAtMs ?? entry.atMs,
      });
      // "We asked" is not "they agreed": only an acknowledged subscription makes the link usable.
      subscriptionState = [...subscriptions.values()].every((s) => s.state === ACKNOWLEDGED)
        ? ACKNOWLEDGED
        : [...subscriptions.values()].some((s) => s.state === FAILED)
          ? FAILED
          : PENDING;
      onSubscriptions({ market, generation, state: subscriptionState, subscriptions: new Map(subscriptions) });
      setState(subscriptionState === ACKNOWLEDGED ? 'subscribed' : 'awaiting-subscription');
      return;
    }
    if (parsed.kind === 'shutdown') {
      onDiagnostic({ market, generation, reason: 'venue announced a shutdown', detail: parsed.detail ?? '' });
      replaceSocket('venue-shutdown');
      return;
    }
    if (parsed.kind === 'data') {
      onEnvelope(stamp(raw));
      return;
    }
    onDiagnostic({ market, generation, reason: `unhandled message kind ${parsed.kind}` });
  }

  function teardownSocket(reason) {
    clearSilenceTimer();
    clearStabilityTimer();
    if (!socket) return;
    const dying = socket;
    socket = null;
    try {
      dying.onopen = null;
      dying.onmessage = null;
      dying.onclose = null;
      dying.onerror = null;
      dying.close?.();
    } catch (error) {
      onDiagnostic({ market, generation, reason: 'closing a socket failed', error: String(error) });
    }
    subscriptions.clear();
    subscriptionState = UNSUBSCRIBED;
    onDiagnostic({ market, generation, reason: `socket torn down (${reason})` });
  }

  /**
   * Replace the socket and issue a new generation. The old generation's frames must not be trusted
   * afterwards, which is why this is the only place a generation moves.
   */
  function replaceSocket(reason) {
    if (closed) return;
    if (socket) teardownSocket(reason);
    generation += 1;
    // The name of the connection is the same one the envelope will carry: the run, the venue, the
    // market and the generation. Naming it twice from two rules is how a book ends up accepting one
    // id and being handed frames labelled with another.
    connectionId =
      runId && venue ? `${runId}:${venue}:${market}:${generation}` : `${market}:${generation}`;
    receiveSeq = 0;
    firstSeq = 1;
    attempts += 1;
    onGeneration({ market, generation, connectionId, reason, firstSeq });
    setState('connecting', reason);

    const delay = delayForAttempt(attempts);
    const start = () => {
      if (closed) return;
      let next;
      try {
        next = openSocket(adapter.url);
      } catch (error) {
        onFailure({ market, generation, error, attempts });
        replaceSocket('open failed');
        return;
      }
      socket = next;
      next.onopen = () => {
        if (socket !== next) return;
        noteHeard();
        setState('subscribing');
        for (const message of adapter.subscribeMessages?.() ?? []) next.send(message);
        next.send?.(adapter.heartbeatMessage?.() ?? '');
        armStabilityTimer();
      };
      next.onmessage = (event) => {
        if (socket !== next) return; // a frame from a socket we have already abandoned
        handleMessage(event?.data ?? event);
      };
      next.onerror = (error) => {
        if (socket !== next) return;
        onFailure({ market, generation, error, attempts });
      };
      next.onclose = () => {
        if (socket !== next) return;
        replaceSocket('closed');
      };
    };
    if (delay === 0) start();
    else {
      const timer = setTimer(start, delay);
      if (typeof timer.unref === 'function') timer.unref();
    }
  }

  function delayForAttempt(attempt) {
    if (attempt <= 1) return 0;
    const base = Math.min(maxBackoffMs, 1000 * 2 ** Math.min(attempt - 2, 5));
    return Math.min(maxBackoffMs, base) + Math.floor(Math.random() * 250);
  }

  return {
    start() {
      closed = false;
      replaceSocket('start');
    },
    stop() {
      closed = true;
      clearSilenceTimer();
      clearStabilityTimer();
      if (socket) teardownSocket('stopped');
      setState('stopped');
    },
    get generation() {
      return generation;
    },
    get connectionId() {
      return connectionId;
    },
    get receiveSeq() {
      return receiveSeq;
    },
    get state() {
      return state;
    },
    get attempts() {
      return attempts;
    },
    get subscriptionState() {
      return subscriptionState;
    },
    get subscriptions() {
      return new Map(subscriptions);
    },
  };
}
