/**
 * The raw envelope and the frame format that carry it between the three processes.
 *
 * This is the contract everything else is built on, so it is deliberately small and total: one
 * shape for every venue, one framing for every hop, and the receive metadata that the rest of the
 * structure trusts without ever re-deriving it.
 *
 * Design (docs/current/process-separation-design.md, receiver v2):
 *  - recv_ts_ms  - wall clock at the socket boundary. Immutable once written; replay never changes it.
 *  - recv_mono_ns - monotonic clock, strictly increasing within one connection. Ordering inside a
 *                  connection is decided from this, never from the venue's own timestamps.
 *  - connection_id / receive_seq - the dedupe key. A resend after a reconnect is absorbed by
 *                  (connection_id, receive_seq), which is how every retry path stays idempotent.
 *  - raw         - the bytes as received, unmodified. This is the canonical record; everything
 *                  downstream is derived and may be rebuilt from it.
 *
 * Nothing here reads a clock, opens a socket or touches a file: the caller stamps, this module
 * validates and encodes. That keeps it testable without a venue and without waiting.
 */

/** Frames larger than this are refused rather than buffered. A single venue message never needs more. */
export const FRAME_MAX_BYTES = 8 * 1024 * 1024;

/** Bytes of length prefix per frame (uint32, big endian). */
export const FRAME_HEADER_BYTES = 4;

const MAX_UINT32 = 0xffffffff;

/** Fields every envelope must carry, in the order they are serialized. */
const REQUIRED_FIELDS = Object.freeze([
  'market',
  'stream',
  'connection_id',
  'receive_seq',
  'recv_ts_ms',
  'recv_mono_ns',
]);

function assertNonEmptyString(value, name) {
  if (typeof value !== 'string' || value.length === 0) {
    throw new TypeError(`${name} must be a non-empty string, got ${JSON.stringify(value)}`);
  }
  return value;
}

function assertUint32(value, name) {
  if (!Number.isInteger(value) || value < 0 || value > MAX_UINT32) {
    throw new TypeError(`${name} must be a uint32, got ${JSON.stringify(value)}`);
  }
  return value;
}

function assertSafePositiveInteger(value, name) {
  if (!Number.isSafeInteger(value) || value <= 0) {
    throw new TypeError(`${name} must be a positive safe integer, got ${JSON.stringify(value)}`);
  }
  return value;
}

/**
 * Build one canonical envelope.
 *
 * The caller owns the clocks and the counters; this only refuses to build something the rest of the
 * structure could not trust. raw may be a string (already-decoded text) or a Buffer (bytes exactly
 * as received) - both are stored as-is.
 */
export function makeEnvelope({
  market,
  stream,
  connectionId,
  receiveSeq,
  recvTsMs,
  recvMonoNs,
  raw,
  meta,
}) {
  assertNonEmptyString(market, 'market');
  assertNonEmptyString(stream, 'stream');
  assertNonEmptyString(connectionId, 'connectionId');
  assertUint32(receiveSeq, 'receiveSeq');
  assertSafePositiveInteger(recvTsMs, 'recvTsMs');
  assertSafePositiveInteger(recvMonoNs, 'recvMonoNs');
  if (typeof raw !== 'string' && !Buffer.isBuffer(raw)) {
    throw new TypeError('raw must be a string or a Buffer');
  }
  return Object.freeze({
    market,
    stream,
    connection_id: connectionId,
    receive_seq: receiveSeq,
    recv_ts_ms: recvTsMs,
    recv_mono_ns: recvMonoNs,
    raw: Buffer.isBuffer(raw) ? Buffer.from(raw) : Buffer.from(raw, 'utf8'),
    ...(meta && typeof meta === 'object' ? { meta: Object.freeze({ ...meta }) } : {}),
  });
}

/** The dedupe key for a resend. Append-safe: writing the same key twice is a no-op, never a duplicate. */
export function dedupeKey(envelope) {
  return `${envelope.connection_id}:${envelope.receive_seq}`;
}

/**
 * True when two envelopes from the same connection are in receive order.
 *
 * Ordering is decided from recv_mono_ns (a monotonic clock within the connection), never from the
 * venue's own timestamps, which are only ever metadata. Equal nanoseconds are refused: two frames
 * cannot share one instant on one connection, and accepting that would hide a clock that stepped.
 */
export function isAfter(previous, next) {
  if (!previous) return true;
  if (previous.connection_id !== next.connection_id) return true;
  return next.recv_mono_ns > previous.recv_mono_ns && next.receive_seq > previous.receive_seq;
}

/** Serialize an envelope for the wire: invariant keys first, extras after, raw as raw bytes. */
export function encodeEnvelope(envelope) {
  for (const field of REQUIRED_FIELDS) {
    if (!(field in envelope)) throw new TypeError(`envelope is missing ${field}`);
  }
  const header = {};
  for (const field of REQUIRED_FIELDS) header[field] = envelope[field];
  if (envelope.meta) header.meta = envelope.meta;
  const headerBytes = Buffer.from(JSON.stringify(header), 'utf8');
  const headerLength = Buffer.alloc(FRAME_HEADER_BYTES);
  headerLength.writeUInt32BE(headerBytes.length, 0);
  return Buffer.concat([headerLength, headerBytes, Buffer.from(envelope.raw)]);
}

/** Inverse of encodeEnvelope. Throws on anything malformed rather than guessing a default. */
export function decodeEnvelope(frameBytes) {
  if (!Buffer.isBuffer(frameBytes) || frameBytes.length < FRAME_HEADER_BYTES * 2) {
    throw new TypeError('frame too short to contain a header');
  }
  const headerLength = frameBytes.readUInt32BE(0);
  const headerEnd = FRAME_HEADER_BYTES + headerLength;
  if (headerEnd > frameBytes.length) throw new TypeError('frame header length exceeds frame size');
  const header = JSON.parse(frameBytes.subarray(FRAME_HEADER_BYTES, headerEnd).toString('utf8'));
  return makeEnvelope({
    market: header.market,
    stream: header.stream,
    connectionId: header.connection_id,
    receiveSeq: header.receive_seq,
    recvTsMs: header.recv_ts_ms,
    recvMonoNs: header.recv_mono_ns,
    raw: frameBytes.subarray(headerEnd),
    meta: header.meta,
  });
}

/**
 * Frame one payload for a stream socket: uint32 length prefix, then the payload.
 *
 * The prefix is what lets a reader take whole frames off a socket without knowing the venue, and it
 * is why a partial read is a state the reader can hold rather than an error.
 */
export function frame(payload) {
  const body = Buffer.isBuffer(payload) ? payload : Buffer.from(payload);
  if (body.length > FRAME_MAX_BYTES) {
    throw new RangeError(`frame of ${body.length} bytes exceeds FRAME_MAX_BYTES`);
  }
  const header = Buffer.alloc(FRAME_HEADER_BYTES);
  header.writeUInt32BE(body.length, 0);
  return Buffer.concat([header, body]);
}

/**
 * Incremental frame decoder for a stream socket.
 *
 * push() returns every complete frame now available and keeps the remainder, so a message split
 * across two reads is normal, not an error. A frame larger than FRAME_MAX_BYTES is refused at the
 * prefix - before any of it is buffered - so a corrupt length cannot make the process grow.
 */
export function createFrameDecoder({ maxBytes = FRAME_MAX_BYTES } = {}) {
  let pending = Buffer.alloc(0);
  return {
    push(chunk) {
      pending = pending.length === 0 ? Buffer.from(chunk) : Buffer.concat([pending, chunk]);
      const frames = [];
      let offset = 0;
      while (pending.length - offset >= FRAME_HEADER_BYTES) {
        const length = pending.readUInt32BE(offset);
        if (length > maxBytes) {
          // A declared length beyond the limit desynchronises the stream: there is no way to know
          // where the next real boundary is, so nothing is kept and the caller must reconnect.
          pending = Buffer.alloc(0);
          throw new RangeError(`declared frame of ${length} bytes exceeds the ${maxBytes} byte limit`);
        }
        if (pending.length - offset - FRAME_HEADER_BYTES < length) break;
        const start = offset + FRAME_HEADER_BYTES;
        frames.push(pending.subarray(start, start + length));
        offset = start + length;
      }
      if (offset > 0) pending = pending.subarray(offset);
      return frames;
    },
    get bufferedBytes() {
      return pending.length;
    },
  };
}
