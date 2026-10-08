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
// C3: how long an expected subscription may go unanswered before the link is failed. A venue that
// accepts what it was asked for answers within this window; one that does not is not a usable link.
export const DEFAULT_ACK_DEADLINE_MS = 10_000;

const UNSUBSCRIBED = 'unsubscribed';
const PENDING = 'pending';
const ACKNOWLEDGED = 'acknowledged';
const FAILED = 'failed';
// The connection's own state while its subscription is failed. Distinct from `awaiting-subscription`:
// "we have not heard yet" and "the venue refused / never answered" are different facts, and only the
// latter blocks the link from being reported as serving (C3).
const SUBSCRIPTION_FAILED_STATE = 'subscription-failed';

/**
 * A copy of one subscription's record, nested data included.
 *
 * The record is what the connection believes about a subscription, and a caller that reads the map
 * must not be able to edit that belief: editing `subscriptions.get(key).state` used to edit the
 * connection's own memory (the map was copied, its values were not), and the next subscription message
 * recomputed the whole link's state from the edited value. Anything that is not plain data - a buffer,
 * an error, a function - is handed on as it is.
 */
function copySubscription(entry) {
  const copy = {};
  for (const [key, value] of Object.entries(entry)) copy[key] = copySubscriptionValue(value);
  return copy;
}

function copySubscriptionValue(value) {
  if (Array.isArray(value)) return value.map(copySubscriptionValue);
  if (value instanceof Map) {
    return new Map([...value].map(([key, nested]) => [key, copySubscriptionValue(nested)]));
  }
  if (value instanceof Set) return new Set([...value].map((nested) => copySubscriptionValue(nested)));
  if (value !== null && typeof value === 'object') {
    const prototype = Object.getPrototypeOf(value);
    if (prototype === Object.prototype || prototype === null) {
      return Object.fromEntries(Object.entries(value).map(([key, nested]) => [key, copySubscriptionValue(nested)]));
    }
  }
  return value;
}

export function createReceiveConnection({
  adapter,
  market,
  runId = null,
  venue = null,
  webSocketImpl,
  openSocket = (url) => new webSocketImpl(url),
  monotonicNs = () => Number(process.hrtime.bigint()),
  wallClockMs = () => Date.now(),
  // Who issues a connection's generation when its socket is replaced. Reception is the only thing that
  // issues one, but the issuer is a per-run object rather than a per-connection counter: two sockets of the
  // same run, venue and market built from two connection objects would otherwise both number themselves
  // from 1, and their connection names - run:venue:market:generation (C2) - would collide. The entry point
  // makes one issuer for the run and injects it into every connection that run builds; a connection opened
  // without one numbers itself, which is all a single connection ever needs.
  issueGeneration = null,
  silenceDeadlineMs = DEFAULT_SILENCE_DEADLINE_MS,
  stabilityMs = DEFAULT_STABILITY_MS,
  setTimer = setTimeout,
  clearTimer = clearTimeout,
  maxBackoffMs = 30_000,
  onEnvelope = () => {},
  onGeneration = () => {},
  // Every event - a socket message, open/close/error, a timer - is handed here with the socket it came from,
  // and the work runs inside whatever the caller puts behind this: the structure runs it as an operation of
  // its own, or keeps it in order when one is already running. Nothing is done from the event entry itself.
  onEvent = (_label, work) => work(),
  onFailure = () => {},
  // C3: how establishment is decided for this connection. `explicit` waits for the venue's
  // acknowledgement of every expected subscription; `first-data` treats the first data frame of the
  // stream as establishment (a venue that does not answer). Declared per connection, not per venue
  // (C11), so a venue can be received from more than one way.
  ackMode = adapter?.ackMode ?? 'explicit',
  // C3: how long the expected subscriptions may go unanswered before the link is failed.
  ackDeadlineMs = DEFAULT_ACK_DEADLINE_MS,
  // C3: the set of subscription keys this connection asks for. The adapter names them, because only it
  // knows the keys its own `parse()` answers with. Absent means the connection was not told a set, and
  // establishment falls back to "every acknowledgement heard so far is a success".
  expectedSubscriptions = adapter?.expectedSubscriptions ?? null,
  onSubscriptions = () => {},
  onState = () => {},
  onDiagnostic = () => {},
}) {
  if (!adapter || typeof adapter.parse !== 'function') {
    throw new TypeError('reception needs an adapter that can parse a message');
  }
  if (!webSocketImpl) throw new TypeError('reception needs a websocket implementation');
  if (ackMode !== 'explicit' && ackMode !== 'first-data') {
    throw new TypeError(`unknown ack mode: ${JSON.stringify(ackMode)}`);
  }
  if (!Number.isFinite(ackDeadlineMs) || ackDeadlineMs <= 0) {
    throw new TypeError('the ack deadline must be a positive number of milliseconds');
  }

  let generation = 0;
  let connectionId = null;
  let socket = null;
  let receiveSeq = 0;
  let subscriptionState = UNSUBSCRIBED;
  // C3: the keys asked for on the current socket, or null when the connection was not told a set. The
  // ack deadline timer for that same socket, and the reason the link failed (for the band's report).
  let expectedKeys = null;
  let ackTimer = null;
  let subscriptionFailure = null;
  let preparationPending = false;
  let preparationFrames = [];
  let lastHeardMs = 0;
  let silentTimer = null;
  let stabilityTimer = null;
  // C4: the keep-alive the adapter declares for the current socket, in its three forms, and the timer that
  // carries it. Kept apart from the silence deadline above: a venue that sends nothing is still watched for
  // silence, and disabling the keep-alive never disables that watch (they answer different questions).
  let keepAlivePlan = null;
  let keepAliveTimer = null;
  let noActivityTimer = null;
  let attempts = 0;
  let closed = false;
  let state = 'idle';
  let maintenancePaused = false;
  const subscriptions = new Map();

  /**
   * The subscriptions as a caller may see them: every record a copy, so a caller that edits what it was
   * handed changes nothing the connection computes from. Used by the read view and by the notification
   * hook alike, because the two must not disagree about what a caller can reach.
   */
  function snapshotSubscriptions() {
    return new Map([...subscriptions].map(([key, entry]) => [key, copySubscription(entry)]));
  }

  /** The adapter's expected set for this connection, as a Set of keys, or null when it declares none. */
  function expectedKeysOf() {
    const declared = typeof expectedSubscriptions === 'function' ? expectedSubscriptions() : expectedSubscriptions;
    return Array.isArray(declared) && declared.length > 0 ? new Set(declared) : null;
  }

  /**
   * C3: whether the link is established. With a declared expected set, establishment is exactly "every
   * expected key has been acknowledged" - one unanswered key is not a usable link. Without a declared
   * set the connection only knows what it has heard, so establishment is "every acknowledgement heard so
   * far is a success", which is the behaviour before the set was carried. A refusal always wins: one
   * failed key is a failed link, whether or not it was in the expected set.
   */
  function computeSubscriptionState() {
    const entries = [...subscriptions.values()];
    if (entries.some((entry) => entry.state === FAILED)) return FAILED;
    if (expectedKeys !== null) {
      const allAcked = [...expectedKeys].every((key) => subscriptions.get(key)?.state === ACKNOWLEDGED);
      return allAcked ? ACKNOWLEDGED : PENDING;
    }
    if (entries.length === 0) return UNSUBSCRIBED;
    return entries.every((entry) => entry.state === ACKNOWLEDGED) ? ACKNOWLEDGED : PENDING;
  }

  function clearAckTimer() {
    if (ackTimer !== null) {
      clearTimer(ackTimer);
      ackTimer = null;
    }
  }

  /** Report the subscription state and follow it with the connection's own state (C3: "the band's state"). */
  function publishSubscriptionState(reason = '') {
    onSubscriptions({
      market,
      generation,
      connectionId,
      state: subscriptionState,
      subscriptions: snapshotSubscriptions(),
      ...(reason ? { reason } : {}),
    });
    setState(
      subscriptionState === ACKNOWLEDGED
        ? 'subscribed'
        : subscriptionState === FAILED
          ? SUBSCRIPTION_FAILED_STATE
          : 'awaiting-subscription',
    );
  }

  /**
   * C3: the ack deadline for the current socket. It is armed when the subscriptions are sent, and it is
   * about the socket that sent them - a replacement's window is a new one. When it passes with the link
   * not established, the link is failed: waiting for ever is how a run sits looking alive while it
   * cannot receive the stream it was admitted for.
   */
  function armAckDeadline() {
    clearAckTimer();
    const armedFor = socket;
    const armedGeneration = generation;
    ackTimer = setTimer(() => {
      ackTimer = null;
      if (closed) return;
      onEvent('ack-deadline', () => {
        if (closed || socket !== armedFor || generation !== armedGeneration) return;
        if (subscriptionState === ACKNOWLEDGED) return;
        failSubscription('the subscription ack deadline passed');
      });
    }, ackDeadlineMs);
    if (typeof ackTimer?.unref === 'function') ackTimer.unref();
  }

  /** C3: record the link as failed, with every expected key that was never acknowledged marked failed. */
  function failSubscription(reason) {
    if (closed) return;
    clearAckTimer();
    subscriptionFailure = reason;
    subscriptionState = FAILED;
    if (expectedKeys !== null) {
      for (const key of expectedKeys) {
        if (subscriptions.get(key)?.state === ACKNOWLEDGED) continue;
        const previous = subscriptions.get(key);
        subscriptions.set(key, {
          state: FAILED,
          atMs: wallClockMs(),
          detail: reason,
          askedAtMs: previous?.askedAtMs ?? previous?.atMs ?? wallClockMs(),
        });
      }
    }
    publishSubscriptionState(reason);
    onDiagnostic({ market, generation, reason: `subscription failed: ${reason}` });
  }

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
    // The deadline belongs to the socket that was receiving when it was armed. By the time it fires -
    // or by the time the event it hands over is run, which may be after an operation in between - the
    // socket may have been replaced, and a deadline that passed for an abandoned connection says
    // nothing about the one receiving now.
    const armedFor = socket;
    const armedGeneration = generation;
    silentTimer = setTimer(() => {
      silentTimer = null;
      if (closed) return;
      onEvent('silence-timer', () => {
        if (closed || socket !== armedFor || generation !== armedGeneration) return;
        onDiagnostic({
          market,
          generation,
          reason: 'silence deadline passed',
          silentMs: wallClockMs() - lastHeardMs,
          deadlineMs: silenceDeadlineMs,
        });
        replaceSocket('silence');
      });
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
    // Like the silence deadline: this window is about the link that was open when it started. A socket
    // replaced in the meantime has not held for anything, so its timer must not reset the attempts.
    const armedFor = socket;
    const armedGeneration = generation;
    stabilityTimer = setTimer(() => {
      stabilityTimer = null;
      if (closed) return;
      onEvent('stability-timer', () => {
        if (closed || socket !== armedFor || generation !== armedGeneration) return;
        attempts = 0;
        onDiagnostic({ market, generation, reason: 'link held long enough to reset attempts' });
      });
    }, stabilityMs);
    if (typeof stabilityTimer.unref === 'function') stabilityTimer.unref();
  }

  function noteHeard() {
    lastHeardMs = wallClockMs();
    armSilenceTimer();
    // C4 no-activity form: hearing anything resets the silence window, so the keep-alive is sent only when
    // the socket has really gone quiet - a venue still talking is not pinged.
    if (keepAlivePlan?.form === 'noactivity') scheduleNoActivityTick(socket);
  }

  function stamp(raw, atMs, atNs) {
    receiveSeq += 1;
    // The envelope is built by the one factory that knows the identity, never assembled here. The name is
    // derived by the factory from the run, the venue, the market and the generation - the same four things
    // the connection was named from - so a restarted process cannot take over the name of a connection that
    // has already been written down, and the generation travels with the envelope instead of riding along
    // as a loose extra.
    return makeEnvelope({
      market,
      stream: adapter.stream ?? 'unknown',
      connectionId: null,
      runId,
      venue,
      generation,
      receiveSeq,
      recvTsMs: atMs,
      recvMonoNs: atNs,
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

  function handleMessage(raw, atMs, atNs) {
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
      if (typeof adapter.acceptDepthEvent === 'function' && parsed.seq !== undefined) {
        const accepted = adapter.acceptDepthEvent(raw);
        if (accepted?.status === 'resync' || accepted?.status === 'malformed') {
          replaceSocket('venue sequence/checksum failure');
          return;
        }
      }
      if (parsed.answered) onDiagnostic({ market, generation, reason: 'heartbeat answered' });
      return;
    }
    if (parsed.kind === 'subscription') {
      // A venue answers in its own shape: one key per frame (Bybit, OKX), or - with `full` - one
      // frame carrying the whole acknowledged set at once (Coinbase's cumulative `subscriptions`
      // state). Either way every key the frame names is acknowledged by it; a full-state frame
      // additionally *replaces* the acknowledged set, so a key that was acknowledged before and is
      // absent from the frame is no longer subscribed and the link fails - a subscription that
      // vanished is not a subscription.
      const keys = Array.isArray(parsed.keys) ? parsed.keys : parsed.key !== undefined ? [parsed.key] : [];
      let vanished = null;
      if (parsed.full === true) {
        for (const [key, entry] of subscriptions) {
          if (entry.state === ACKNOWLEDGED && !keys.includes(key)) {
            subscriptions.set(key, {
              state: FAILED,
              atMs: wallClockMs(),
              detail: 'missing from the acknowledged set',
              askedAtMs: entry.askedAtMs ?? entry.atMs,
            });
            vanished = vanished ?? key;
          }
        }
      }
      for (const key of keys) {
        const entry = subscriptions.get(key) ?? { state: PENDING, atMs: wallClockMs() };
        // A failed subscription stays failed until the connection is replaced: a venue that
        // refused once, or dropped a key it had acknowledged, is not talked back into a usable
        // link by a later frame - replacing the connection is the recovery.
        if (entry.state === FAILED) continue;
        subscriptions.set(key, {
          state: parsed.ok ? ACKNOWLEDGED : FAILED,
          atMs: wallClockMs(),
          detail: parsed.detail ?? '',
          askedAtMs: entry.askedAtMs ?? entry.atMs,
        });
      }
      // "We asked" is not "they agreed": only an acknowledged subscription makes the link usable, and
      // with a declared expected set the link is established only when every expected key is acknowledged
      // (C3). A refusal is a failure whatever the set says.
      const wasFailed = subscriptionState === FAILED;
      subscriptionState = computeSubscriptionState();
      subscriptionFailure =
        subscriptionState === FAILED
          ? parsed.detail || (vanished !== null ? `a subscription left the acknowledged set (${vanished})` : 'a subscription was refused')
          : null;
      if (subscriptionState === ACKNOWLEDGED) clearAckTimer();
      publishSubscriptionState(subscriptionFailure ?? '');
      // A-2: the refusal is a fact the run has to face, and its reason belongs on the diagnostic path -
      // not carried only by the stop the supervisor raises afterwards. Reported as the link enters
      // `failed`; a link that recovers and refuses again is a new failure and reports again.
      if (subscriptionState === FAILED && !wasFailed) {
        onDiagnostic({ market, generation, reason: `subscription failed: ${subscriptionFailure}` });
      }
      return;
    }
    if (parsed.kind === 'shutdown') {
      onDiagnostic({ market, generation, reason: 'venue announced a shutdown', detail: parsed.detail ?? '' });
      replaceSocket('venue-shutdown');
      return;
    }
    if (parsed.kind === 'maintenance') {
      maintenancePaused = parsed.resume !== true;
      onDiagnostic({ market, generation, reason: parsed.resume ? 'venue maintenance ended' : 'venue entered maintenance', detail: parsed.detail ?? '' });
      if (parsed.resume) {
        for (const message of adapter.subscribeMessages?.() ?? []) socket?.send?.(message);
      }
      setState(parsed.resume ? 'subscribed' : 'maintenance', parsed.detail ?? '');
      return;
    }
    if (parsed.kind === 'protocol-error') {
      onDiagnostic({ market, generation, reason: `venue protocol error: ${parsed.reason ?? 'unknown'}` });
      replaceSocket('venue-protocol-error');
      return;
    }
    // A connection may carry valid non-book data alongside a depth stream (for example Binance's
    // combined trade + depth URL). It is valid stream data, but it is not an input to the adapter's
    // depth sequence/checksum verifier. Forwarding it there turns an unrelated trade into a false
    // sequence failure; depth frames still take the verifier below.
    if (parsed.kind === 'data' && ackMode === 'first-data' && subscriptionState !== ACKNOWLEDGED) {
      clearAckTimer();
      subscriptionFailure = null;
      subscriptionState = ACKNOWLEDGED;
      publishSubscriptionState('the first data frame established the stream');
    }
    if (parsed.kind === 'data' && parsed.trade === true) return;
    if (parsed.kind === 'data' || parsed.kind === 'checksum') {
      if (typeof adapter.acceptDepthEvent === 'function') {
        const accepted = adapter.acceptDepthEvent(raw);
        if (accepted?.status === 'resync' || accepted?.status === 'malformed') {
          replaceSocket('venue sequence/checksum failure');
          return;
        }
        if (parsed.kind === 'checksum') {
          for (const released of accepted?.released ?? []) onEnvelope(stamp(released, atMs, atNs));
          return;
        }
        if (accepted?.status !== 'applied') return;
        for (const released of accepted?.released ?? []) onEnvelope(stamp(released, atMs, atNs));
        if (accepted?.released) return;
      }
    }
    if (parsed.kind === 'data') {
      if (maintenancePaused) return;
      // C3 first-data: a venue that never answers is established by the first frame of the stream itself.
      // Only a data frame establishes it - a heartbeat or a subscription event does not.
      if (ackMode === 'first-data' && subscriptionState !== ACKNOWLEDGED) {
        clearAckTimer();
        subscriptionFailure = null;
        subscriptionState = ACKNOWLEDGED;
        publishSubscriptionState('the first data frame established the stream');
      }
      onEnvelope(stamp(raw, atMs, atNs));
      return;
    }
    onDiagnostic({ market, generation, reason: `unhandled message kind ${parsed.kind}` });
  }

  /**
   * C4: the keep-alive the adapter declares, resolved once per socket.
   *
   * The adapter returns one of the three forms the contract names - `{ intervalMs, payload() }` for a
   * periodic send, `{ noActivityMs, payload() }` for a send only when the socket has gone quiet, or `null`
   * for no keep-alive at all - or, for the adapters written before the forms existed, a single message
   * string sent once on open (the legacy `heartbeatMessage` shape). `keepAlive` is authoritative when the
   * adapter declares it; otherwise `heartbeatMessage` is asked. A shape that is none of these is a
   * configuration error: it is reported as a diagnostic when the socket opens rather than sent as garbage.
   */
  function resolveKeepAlive() {
    let spec;
    if (adapter.keepAlive !== undefined) {
      spec = typeof adapter.keepAlive === 'function' ? adapter.keepAlive() : adapter.keepAlive;
    } else if (typeof adapter.heartbeatMessage === 'function') {
      spec = adapter.heartbeatMessage();
    } else {
      spec = adapter.heartbeatMessage;
    }
    if (spec === undefined || spec === null) return { form: 'none', payload: null };
    if (typeof spec === 'string' || Buffer.isBuffer(spec)) return { form: 'once', payload: () => spec };
    if (typeof spec === 'object') {
      const payload = typeof spec.payload === 'function' ? spec.payload : () => spec.payload;
      if (spec.intervalMs !== undefined) {
        if (!Number.isFinite(spec.intervalMs) || spec.intervalMs <= 0) {
          throw new TypeError('keep-alive intervalMs must be a positive number of milliseconds');
        }
        return { form: 'interval', intervalMs: spec.intervalMs, payload };
      }
      if (spec.noActivityMs !== undefined) {
        if (!Number.isFinite(spec.noActivityMs) || spec.noActivityMs <= 0) {
          throw new TypeError('keep-alive noActivityMs must be a positive number of milliseconds');
        }
        return { form: 'noactivity', noActivityMs: spec.noActivityMs, payload };
      }
      throw new TypeError('keep-alive must be { intervalMs, payload() }, { noActivityMs, payload() } or null');
    }
    throw new TypeError(`unknown keep-alive form: ${JSON.stringify(spec)}`);
  }

  function clearKeepAliveTimers() {
    if (keepAliveTimer !== null) {
      clearTimer(keepAliveTimer);
      keepAliveTimer = null;
    }
    if (noActivityTimer !== null) {
      clearTimer(noActivityTimer);
      noActivityTimer = null;
    }
  }

  /**
   * Send one keep-alive frame, or nothing. A payload that resolves to null, undefined or an empty frame is
   * deliberately not sent: an empty string is not a keep-alive, and sending one is a frame the venue has to
   * interpret (the bug this replaced). An adapter that depends on the venue's own heartbeats declares
   * `null` and reaches here never.
   */
  function sendKeepAlive(activeSocket) {
    if (keepAlivePlan === null || keepAlivePlan.payload === null) return;
    let message;
    try {
      message = keepAlivePlan.payload();
    } catch (error) {
      onDiagnostic({ market, generation, reason: 'a keep-alive payload could not be built', error: String(error) });
      return;
    }
    if (message === null || message === undefined || message === '') return;
    if (Buffer.isBuffer(message) && message.length === 0) return;
    activeSocket.send?.(message);
  }

  /** Arm the keep-alive for a socket that has just opened, replacing whatever the previous socket had. */
  function armKeepAlive(activeSocket) {
    clearKeepAliveTimers();
    try {
      keepAlivePlan = resolveKeepAlive();
    } catch (error) {
      keepAlivePlan = { form: 'none', payload: null };
      onDiagnostic({ market, generation, reason: `keep-alive declaration refused: ${error.message}` });
      return;
    }
    if (keepAlivePlan.form === 'once') {
      sendKeepAlive(activeSocket);
      return;
    }
    if (keepAlivePlan.form === 'interval') {
      scheduleIntervalTick(activeSocket);
      return;
    }
    if (keepAlivePlan.form === 'noactivity') {
      scheduleNoActivityTick(activeSocket);
    }
    // 'none': nothing is sent. The silence deadline still runs - disabling the keep-alive is not the same
    // question as watching for silence, and the two are kept apart on purpose.
  }

  /** C4 interval form: a rhythm, so the first send is one interval away rather than an opening burst. */
  function scheduleIntervalTick(activeSocket) {
    const armedFor = activeSocket;
    const armedGeneration = generation;
    const delay = keepAlivePlan.intervalMs;
    keepAliveTimer = setTimer(() => {
      keepAliveTimer = null;
      if (closed) return;
      onEvent('keep-alive', () => {
        if (closed || socket !== armedFor || generation !== armedGeneration || keepAlivePlan === null) return;
        sendKeepAlive(armedFor);
        scheduleIntervalTick(armedFor);
      });
    }, delay);
    if (typeof keepAliveTimer?.unref === 'function') keepAliveTimer.unref();
  }

  /** C4 no-activity form: re-armed whenever anything is heard, so it fires only after real silence. */
  function scheduleNoActivityTick(activeSocket) {
    if (noActivityTimer !== null) {
      clearTimer(noActivityTimer);
      noActivityTimer = null;
    }
    if (keepAlivePlan === null || keepAlivePlan.form !== 'noactivity' || closed) return;
    const armedFor = activeSocket;
    const armedGeneration = generation;
    const delay = keepAlivePlan.noActivityMs;
    noActivityTimer = setTimer(() => {
      noActivityTimer = null;
      if (closed) return;
      onEvent('keep-alive', () => {
        if (closed || socket !== armedFor || generation !== armedGeneration || keepAlivePlan === null) return;
        sendKeepAlive(armedFor);
        // No answer is assumed: the window restarts, and silence that continues fires it again.
        scheduleNoActivityTick(armedFor);
      });
    }, delay);
    if (typeof noActivityTimer?.unref === 'function') noActivityTimer.unref();
  }

  function teardownSocket(reason) {
    clearSilenceTimer();
    clearStabilityTimer();
    clearAckTimer();
    clearKeepAliveTimers();
    preparationPending = false;
    preparationFrames = [];
    keepAlivePlan = null;
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
    // The next socket asks for its own set and gets its own window: what this one asked for says
    // nothing about it (C3).
    expectedKeys = null;
    subscriptionFailure = null;
    onDiagnostic({ market, generation, reason: `socket torn down (${reason})` });
  }

  /**
   * Replace the socket and issue a new generation. The old generation's frames must not be trusted
   * afterwards, which is why this is the only place a generation moves.
   */
  function replaceSocket(reason) {
    if (closed) return;
    if (socket) teardownSocket(reason);
    generation = issueGeneration === null ? generation + 1 : issueGeneration();
    // The name of a connection is the run, the venue, the market and the generation (C2). There is no
    // fallback: a name built from the market and the generation alone is a name two runs can both hold, and
    // a hand-over of one then looks like a reconnect of the other - which is how a board and a watermark end
    // up describing different data under one name.
    connectionId = `${runId}:${venue}:${market}:${generation}`;
    receiveSeq = 0;
    firstSeq = 1;
    attempts += 1;
    // The run and the venue travel with the announcement: without them the book is told which
    // connection proposes to take over but not which run it belongs to, and a takeover - which is a
    // question about runs, not about connections - cannot be decided from that.
    // The announcement is settled by this, not by its return value alone: when the structure is free it runs
    // the announcement now and calls this, and when an operation is running the announcement waits, and this
    // is called when it runs. A connection nobody admitted never opens its socket.
    const settle = (admitted) => {
      if (closed) return;
      if (admitted === false) {
        // A connection nobody downstream admitted must not start receiving. Opening the socket anyway would
        // make this process speak - and stamp frames - for a connection the book refused, and the frames of
        // a refused connection are refused again further down, after they have been counted as received.
        setState('refused', reason);
        onDiagnostic({ market, generation, reason: 'this connection was not admitted downstream' });
        return;
      }
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
        const fromGeneration = generation;
        const finishOpen = () => {
          setState('subscribing');
          const subscribeMessages = adapter.subscribeMessages?.() ?? [];
          for (const message of subscribeMessages) next.send(message);
          // C4: the keep-alive the adapter declares, in whichever of the three forms. A null declaration
          // sends nothing at all - never an empty frame, which is what the old `?? ''` fallback did.
          armKeepAlive(next);
          // C3: the expected set is registered where the request is sent, and the ack window opens with
          // it. A venue that answers is judged by its expected set (one unanswered key is not a link); a
          // first-data venue is judged by its first data frame. A connection that asked for nothing and
          // does not use first-data arms no window: there is nothing to wait for.
          expectedKeys = expectedKeysOf();
          if (ackMode === 'first-data' || subscribeMessages.length > 0) armAckDeadline();
          armStabilityTimer();
        };
        const openWork = () => {
          // Checked again where the work runs: a socket that was replaced while this announcement
          // waited is not receiving any more, and subscribing on it would talk to a connection that
          // is already gone.
          if (closed || socket !== next || generation !== fromGeneration) return;
          noteHeard();
          if (typeof adapter.onConnectionOpen !== 'function') {
            setState('subscribing');
            return finishOpen();
          }
          preparationPending = true;
          preparationFrames = [];
          let prepared;
          try {
            prepared = adapter.onConnectionOpen({ connectionId, generation, socket: next });
          } catch (error) {
            onDiagnostic({ market, generation, reason: `adapter connection preparation failed: ${error.message}` });
            replaceSocket('adapter preparation failed');
            return;
          }
          if (prepared && typeof prepared.then === 'function') {
            return prepared.then(
              () => {
                if (closed || socket !== next || generation !== fromGeneration) return;
                preparationPending = false;
                finishOpen();
                const pending = preparationFrames;
                preparationFrames = [];
                for (const frame of pending) {
                  // A replayed frame may itself replace the socket (for example, a Binance depth gap).
                  // Once that happens, the remainder belongs to the abandoned generation and must not
                  // be stamped or interpreted by the replacement.
                  if (closed || socket !== next || generation !== fromGeneration || state === 'stopped' || state === 'refused') return;
                  handleMessage(frame.raw, frame.atMs, frame.atNs);
                }
              },
              (error) => {
                if (closed || socket !== next || generation !== fromGeneration) return;
                onDiagnostic({ market, generation, reason: `adapter connection preparation failed: ${error.message}` });
                preparationPending = false;
                preparationFrames = [];
                replaceSocket('adapter preparation failed');
              },
            );
          }
          preparationPending = false;
          finishOpen();
        };
        onEvent('open', openWork);
      };
      const handleSocketMessage = (event) => {
        if (socket !== next) return; // a frame from a socket we have already abandoned
        const payload = event?.data ?? event;
        // The arrival is copied and timed here, where it arrives: what the work does later must not change
        // when it was heard, and the socket's buffer may be reused by the time it runs.
        const raw = typeof payload === 'string' ? payload : Buffer.from(payload);
        const atMs = wallClockMs();
        const atNs = monotonicNs();
        const fromGeneration = generation;
        if (preparationPending) {
          // REST preparation can be slower than the silence deadline. The frame is still buffered and
          // must not reach the stream before synchronization, but its arrival proves the socket is alive.
          noteHeard();
          adapter.bufferDuringPreparation?.(raw);
          preparationFrames.push({ raw, atMs, atNs });
          return;
        }
        onEvent('message', () => {
          // Checked here again, at the moment the work runs: while this arrival waited its turn the
          // socket may have been replaced, and a frame of an older generation must not be stamped as
          // the new connection's - that would write the past into the raw under the current name.
          if (closed || socket !== next || generation !== fromGeneration) return;
          handleMessage(raw, atMs, atNs);
        });
      };
      next.onmessage = handleSocketMessage;
      next.onerror = (error) => {
        if (socket !== next) return;
        const fromGeneration = generation;
        onEvent('error', () => {
          if (closed || socket !== next || generation !== fromGeneration) return;
          onFailure({ market, generation, error, attempts });
        });
      };
        next.onclose = () => {
          if (socket !== next) return;
          const fromGeneration = generation;
          onEvent('close', () => {
            if (closed || socket !== next || generation !== fromGeneration) return;
            replaceSocket('closed');
          });
        };
      };
      if (delay === 0) start();
      else {
        // The replacement is scheduled for one generation. If that generation has been replaced again
        // by the time the timer runs - or a socket is somehow already open for it - opening one here
        // would receive for a connection that is not the current one.
        const opensGeneration = generation;
        const timer = setTimer(
          () => onEvent('reconnect-timer', () => {
            if (closed || generation !== opensGeneration || socket !== null) return;
            start();
          }),
          delay,
        );
        if (typeof timer.unref === 'function') timer.unref();
      }
    };

    const announced = onGeneration({
      market,
      generation,
      connectionId,
      reason,
      firstSeq,
      runId,
      venue,
      settle,
    });
    if (announced === false) settle(false);
  }

  function delayForAttempt(attempt) {
    if (attempt <= 1) return 0;
    const base = Math.min(maxBackoffMs, 1000 * 2 ** Math.min(attempt - 2, 5));
    return Math.min(maxBackoffMs, base) + Math.floor(Math.random() * 250);
  }

  return {
    start() {
      // A connection cannot be named without the run and the venue it belongs to: the name is what makes it
      // unique across processes, and without it two runs on the same market would share one identity. This
      // is a configuration that cannot receive, so it is refused before any socket exists.
      if (!runId || !venue) {
        throw new TypeError('reception needs a run and a venue to name its connections');
      }
      closed = false;
      replaceSocket('start');
    },
    stop() {
      closed = true;
      // The subscription verdict is captured before the socket is torn down: a stop that follows a failed
      // subscription must still report that the stream was never established (C3), rather than reading as
      // if nothing had been asked.
      const lastState = subscriptionState;
      const lastFailure = subscriptionFailure;
      clearSilenceTimer();
      clearStabilityTimer();
      clearAckTimer();
      if (socket) teardownSocket('stopped');
      if (lastState === FAILED) {
        subscriptionState = FAILED;
        subscriptionFailure = lastFailure;
      }
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
    /** C3: how this connection decides establishment, and why it last failed (null when it did not). */
    get ackMode() {
      return ackMode;
    },
    get subscriptionFailure() {
      return subscriptionFailure;
    },
    get subscriptions() {
      return snapshotSubscriptions();
    },
  };
}
