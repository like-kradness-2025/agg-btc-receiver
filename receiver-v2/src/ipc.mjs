/**
 * The transport between the three processes.
 *
 * One unix socket carries two kinds of payload: envelope frames (raw data on its way to
 * organization or to the book) and control messages (acknowledgements, resends, capacity state).
 * A single tag byte keeps them apart, so the reader never has to guess what it is holding.
 *
 * The parts that matter for not losing data:
 *  - the queue is bounded, and a full queue is reported rather than absorbed. The caller runs the
 *    ladder (memory, then spool, then stop and record a gap); this module only says it is full.
 *  - acknowledgements never skip a hole. contiguousCeiling advances only while the next sequence
 *    number is exactly one past the last, so a gap stops the ack at the gap, which is what makes
 *    "acknowledged" mean "durable and complete up to here" rather than "mostly there".
 *
 * Nothing here opens a clock or a file by itself: batching timers are unref'd so a process waiting
 * on a socket can still exit, and the socket path is the caller's business.
 */

import net from 'node:net';

import { FRAME_MAX_BYTES, createFrameDecoder, decodeEnvelope, encodeEnvelope, frame } from './envelope.mjs';

/** First byte of every payload. Data and control must never be confused. */
export const TAG_ENVELOPE = 0x01;
export const TAG_CONTROL = 0x02;

/** Batching: send when this many envelopes are queued, or when this long has passed. */
export const DEFAULT_BATCH_FRAMES = 512;
export const DEFAULT_BATCH_MS = 100;

/** Bytes held for a peer that is not reading. Past this the caller is told, not slowed down. */
export const DEFAULT_MAX_BUFFERED_BYTES = 8 * 1024 * 1024;

/**
 * The contiguous durable ceiling.
 *
 * Called once per durably written envelope, in order, for one connection. It returns the highest
 * sequence number that is durable with nothing missing before it - which is the only value an
 * acknowledgement may carry. A hole leaves the ceiling where it was until the missing sequence
 * arrives (a resend), and a duplicate is absorbed without moving it.
 */
export function contiguousCeiling(state, envelope) {
  const { connectionId, firstSeq, upToSeq } = state;
  const seq = envelope.receive_seq;
  if (envelope.connection_id !== connectionId) return state;
  // Before anything is acknowledged there is no baseline, and only the connection's first sequence
  // may start one - starting anywhere else would acknowledge a range whose beginning was never seen.
  if (upToSeq === null) {
    if (seq < firstSeq) return state; // below this connection's range: not ours to track
    if (seq > firstSeq) {
      // Frames that arrived before the first one are remembered, not dropped: they are real
      // positions, and forgetting them would stall the ceiling once the baseline opens.
      if (state.outOfOrder.includes(seq)) return state;
      return {
        connectionId,
        firstSeq,
        upToSeq: null,
        outOfOrder: [...state.outOfOrder, seq].sort((a, b) => a - b),
      };
    }
    let next = seq;
    let rest = state.outOfOrder;
    while (rest.length > 0 && rest[0] === next + 1) {
      next = rest[0];
      rest = rest.slice(1);
    }
    return { connectionId, firstSeq, upToSeq: next, outOfOrder: rest };
  }
  if (seq === upToSeq + 1) {
    let next = seq;
    let rest = state.outOfOrder;
    while (rest.length > 0 && rest[0] === next + 1) {
      next = rest[0];
      rest = rest.slice(1);
    }
    return { connectionId, firstSeq, upToSeq: next, outOfOrder: rest };
  }
  if (seq <= upToSeq) return state; // already covered: a resend changes nothing.
  // A repeat of a frame that is genuinely out of order must not be recorded twice: a stale copy
  // would then sit in the list and block the release of everything after the hole.
  if (state.outOfOrder.includes(seq)) return state;
  return {
    connectionId,
    firstSeq,
    upToSeq,
    outOfOrder: [...state.outOfOrder, seq].sort((a, b) => a - b),
  };
}

/**
 * A fresh ceiling for a connection. upToSeq null means "nothing acknowledged yet", which is not the
 * same as acknowledging 0: a connection whose first sequence is 1 has acknowledged nothing until a
 * frame actually arrives, and treating 0 as a starting point would let the first durable frame
 * acknowledge over a hole.
 */
export function newCeiling(connectionId, { firstSeq = 1 } = {}) {
  return { connectionId, firstSeq, upToSeq: null, outOfOrder: [] };
}

function framePayload(tag, body) {
  const payload = Buffer.alloc(1 + body.length);
  payload[0] = tag;
  body.copy(payload, 1);
  return payload;
}

function encodeControl(message) {
  return Buffer.from(JSON.stringify(message), 'utf8');
}

/**
 * One side of a channel over an existing socket (server side from a connection, client from
 * connect). Batches outgoing envelopes, parses incoming frames, and reports a full queue instead
 * of growing without bound.
 */
export function createChannel(socket, options = {}) {
  const {
    batchFrames = DEFAULT_BATCH_FRAMES,
    batchMs = DEFAULT_BATCH_MS,
    maxBufferedBytes = DEFAULT_MAX_BUFFERED_BYTES,
    onEnvelope = () => {},
    onControl = () => {},
    onError = () => {},
    onBackpressure = () => {},
    onDrain = () => {},
    setTimer = setTimeout,
    clearTimer = clearTimeout,
  } = options;

  const decoder = createFrameDecoder({ maxBytes: FRAME_MAX_BYTES + 1 });
  let batch = [];
  let batchTimer = null;
  let bufferedBytes = 0;
  let backpressured = false;
  let closed = false;

  function flushBatch() {
    if (batchTimer !== null) {
      clearTimer(batchTimer);
      batchTimer = null;
    }
    if (batch.length === 0 || closed) return;
    const payload = Buffer.concat(batch);
    batch = [];
    bufferedBytes = Math.max(0, bufferedBytes - payload.length);
    const ok = socket.write(payload);
    if (!ok) socket.once('drain', () => {
      backpressured = false;
      onDrain();
    });
  }

  /**
   * A stream whose framing is broken cannot be resynchronised, so the channel stops rather than
   * continuing: the peer's next chunk is never reinterpreted as a boundary, and the caller
   * reconnects. Anything already read from a desynchronised stream is not treated as data.
   */
  function fail(error) {
    // One report per channel, however many things go wrong after the first: a caller reacting to a
    // failure must not be told twice, and the second report would arrive after the socket is gone.
    if (closed) return;
    closed = true;
    bufferedBytes = 0;
    batch = [];
    onError(error);
    try {
      socket.destroy();
    } catch {
      /* the socket may already be gone */
    }
  }

  function scheduleBatch() {
    if (batchTimer === null) {
      batchTimer = setTimer(() => {
        batchTimer = null;
        flushBatch();
      }, batchMs);
      if (typeof batchTimer.unref === 'function') batchTimer.unref();
    }
  }

  function push(payload) {
    if (closed) return false;
    // Every payload goes out length-prefixed: the reader takes whole frames off the socket, so a
    // partial read is a state it can hold rather than an error. Without this the peer sees a tag
    // byte where it expects a length.
    const framed = frame(payload);
    // Only our own outgoing bytes count. Incoming traffic must never make the queue look emptier
    // than it is, and the bound is checked before anything is accepted - false means the caller
    // keeps or spools this frame, it never means the frame was taken and dropped.
    const wouldHold = bufferedBytes + framed.length + socket.writableLength;
    if (wouldHold > maxBufferedBytes) {
      if (!backpressured) {
        backpressured = true;
        onBackpressure({ bufferedBytes: wouldHold, maxBufferedBytes });
      }
      return false;
    }
    batch.push(framed);
    bufferedBytes += framed.length;
    if (batch.length >= batchFrames) flushBatch();
    else scheduleBatch();
    return true;
  }

  /** Queue one envelope. False means the queue is over its bound: run the ladder, do not ignore it. */
  function sendEnvelope(envelope) {
    return push(framePayload(TAG_ENVELOPE, encodeEnvelope(envelope)));
  }

  /** Queue one control message (ack, resend request, capacity report). */
  function sendControl(message) {
    return push(framePayload(TAG_CONTROL, encodeControl(message)));
  }

  /** Acknowledge a contiguous durable range. The value comes from contiguousCeiling, not from a guess. */
  function sendAck({ connectionId, upToSeq, capacity }) {
    return sendControl({ t: 'ack', connection_id: connectionId, up_to_seq: upToSeq, capacity });
  }

  socket.on('data', (chunk) => {
    let payloads;
    try {
      payloads = decoder.push(chunk);
    } catch (error) {
      fail(error);
      return;
    }
    for (const payload of payloads) {
      if (payload.length < 1) {
        fail(new TypeError('empty payload'));
        continue;
      }
      const tag = payload[0];
      const body = payload.subarray(1);
      try {
        if (tag === TAG_ENVELOPE) onEnvelope(decodeEnvelope(body), payload);
        else if (tag === TAG_CONTROL) onControl(JSON.parse(body.toString('utf8')), payload);
        else fail(new TypeError(`unknown tag ${tag}`));
      } catch (error) {
        // Any error on this channel is terminal. A frame that cannot be decoded leaves the stream's
        // integrity in doubt, and continuing would mean reading the next boundary on trust.
        fail(error);
      }
      if (closed) return;
    }
  });

  socket.on('error', (error) => fail(error));
  socket.on('close', () => {
    closed = true;
    flushBatch();
  });

  return {
    socket,
    sendEnvelope,
    sendControl,
    sendAck,
    flush: flushBatch,
    close() {
      flushBatch();
      closed = true;
      socket.end();
    },
    get queuedBytes() {
      return bufferedBytes;
    },
    get isBackpressured() {
      return backpressured;
    },
  };
}

/** Listen on a unix socket path. The caller owns the path and must remove a stale one first. */
export async function listen(path, options = {}) {
  const server = net.createServer();
  const channels = new Set();
  server.on('connection', (socket) => {
    const channel = createChannel(socket, options);
    channels.add(channel);
    options.onChannel?.(channel);
    socket.on('close', () => channels.delete(channel));
  });
  await new Promise((resolve, reject) => {
    server.once('error', reject);
    server.listen(path, () => {
      server.removeListener('error', reject);
      resolve();
    });
  });
  return {
    server,
    path,
    channels,
    close: () =>
      new Promise((resolve) => {
        for (const channel of channels) channel.close();
        server.close(() => resolve());
      }),
  };
}

/** Connect to a unix socket; resolves to a channel ready to send. */
export async function connect(path, options = {}) {
  const socket = await new Promise((resolve, reject) => {
    const s = net.createConnection(path);
    s.once('connect', () => {
      s.removeListener('error', reject);
      resolve(s);
    });
    s.once('error', reject);
  });
  return createChannel(socket, options);
}
