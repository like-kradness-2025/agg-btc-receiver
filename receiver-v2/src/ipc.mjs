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
  const { connectionId, upToSeq } = state;
  const seq = envelope.receive_seq;
  if (envelope.connection_id !== connectionId) return state;
  if (seq === upToSeq + 1) {
    let next = seq;
    let head = 0;
    while (head < state.outOfOrder.length && state.outOfOrder[head] === next + 1) {
      next = state.outOfOrder[head];
      head += 1;
    }
    return { connectionId, upToSeq: next, outOfOrder: state.outOfOrder.slice(head) };
  }
  if (seq <= upToSeq) return state; // a resend of something already covered: nothing changes.
  const outOfOrder = [...state.outOfOrder, seq].sort((a, b) => a - b);
  return { connectionId, upToSeq, outOfOrder };
}

/** A fresh ceiling for a connection. Nothing is durable yet, so it acknowledges nothing. */
export function newCeiling(connectionId) {
  return { connectionId, upToSeq: 0, outOfOrder: [] };
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
    const ok = socket.write(payload);
    if (!ok) socket.once('drain', () => {
      backpressured = false;
      onDrain();
    });
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
    batch.push(framed);
    bufferedBytes += framed.length;
    if (batch.length >= batchFrames) flushBatch();
    else scheduleBatch();
    // The queue is reported, never absorbed: the caller decides whether to spool or stop.
    if (bufferedBytes > maxBufferedBytes) {
      if (!backpressured) {
        backpressured = true;
        onBackpressure({ bufferedBytes, maxBufferedBytes });
      }
      return false;
    }
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
      onError(error);
      return;
    }
    for (const payload of payloads) {
      if (payload.length < 1) {
        onError(new TypeError('empty payload'));
        continue;
      }
      const tag = payload[0];
      const body = payload.subarray(1);
      try {
        if (tag === TAG_ENVELOPE) onEnvelope(decodeEnvelope(body), payload);
        else if (tag === TAG_CONTROL) onControl(JSON.parse(body.toString('utf8')), payload);
        else onError(new TypeError(`unknown tag ${tag}`));
      } catch (error) {
        onError(error);
      }
    }
    bufferedBytes = Math.max(0, bufferedBytes - chunk.length);
    if (backpressured && bufferedBytes <= maxBufferedBytes && socket.writableLength === 0) {
      backpressured = false;
      onDrain();
    }
  });

  socket.on('error', (error) => onError(error));
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
