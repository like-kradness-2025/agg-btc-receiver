/**
 * The raw envelope and the frame format that carry it between the three processes.
 *
 * Contract (docs/spec-v2.md):
 *  - recv_ts_ms     wall clock at the socket boundary, immutable; replay never changes it.
 *  - recv_mono_ns   monotonic clock, strictly increasing within one connection; ordering inside a
 *                   connection comes from this, never from the venue's own timestamps.
 *  - connection_id / receive_seq   the dedupe key: a resend is a no-op, not a duplicate.
 *  - raw            the bytes as received. The canonical record everything else is derived from.
 *
 * The canonical bytes are held privately and handed out as a copy, so nothing outside can edit what
 * will be sent or stored. Framing is a uint32 length prefix; a reader holds a partial read and
 * refuses a bad length before any of that frame's bytes are retained.
 */

export const FRAME_MAX_BYTES = 8 * 1024 * 1024;
export const FRAME_HEADER_BYTES = 4;

const MAX_UINT32 = 0xffffffff;
const EMPTY = Buffer.alloc(0);

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
 * raw is copied into private storage and exposed through a getter that returns a fresh copy, so the
 * canonical bytes cannot be edited after the fact by whoever holds the envelope. Callers that need
 * the bytes pay for a copy; callers that only forward the envelope pay nothing.
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
  const rawBytes = Buffer.isBuffer(raw) ? Buffer.from(raw) : Buffer.from(raw, 'utf8');
  return Object.freeze({
    market,
    stream,
    connection_id: connectionId,
    receive_seq: receiveSeq,
    recv_ts_ms: recvTsMs,
    recv_mono_ns: recvMonoNs,
    get raw() {
      return Buffer.from(rawBytes);
    },
    ...(meta && typeof meta === 'object' ? { meta: Object.freeze({ ...meta }) } : {}),
  });
}

/** The dedupe key for a resend. Append-safe: writing the same key twice is a no-op. */
export function dedupeKey(envelope) {
  return `${envelope.connection_id}:${envelope.receive_seq}`;
}

/**
 * True when two envelopes from the same connection are in receive order. Equal nanoseconds are
 * refused: two frames cannot share one instant on one connection, and accepting that would hide a
 * clock that stepped.
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
  return Buffer.concat([headerLength, headerBytes, envelope.raw]);
}

/** Inverse of encodeEnvelope. Throws on anything malformed rather than guessing a default. */
export function decodeEnvelope(frameBytes) {
  if (!Buffer.isBuffer(frameBytes) || frameBytes.length < FRAME_HEADER_BYTES + 2) {
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

/** Frame one payload for a stream socket: uint32 length prefix, then the payload. */
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
 * push() returns every complete frame now available and keeps the remainder. The declared length is
 * checked before any of those bytes are retained: when nothing is buffered the check reads the
 * prefix straight out of the arriving chunk, so an oversized or corrupt length costs no allocation.
 * A length that disagrees with the stream desynchronises it, and the caller must reconnect rather
 * than resynchronise - therefore nothing from that frame is kept and the reader is unusable after.
 */
export function createFrameDecoder({ maxBytes = FRAME_MAX_BYTES } = {}) {
  let pending = EMPTY;
  let failed = null;

  function fail(message) {
    pending = EMPTY;
    failed = new RangeError(message);
    throw failed;
  }

  return {
    push(chunk) {
      if (failed) throw failed;
      // Fast path: nothing buffered, so the prefix of the arriving chunk can be judged on its own,
      // before it is copied anywhere.
      if (pending.length === 0 && chunk.length >= FRAME_HEADER_BYTES) {
        const declared = chunk.readUInt32BE(0);
        if (declared > maxBytes) {
          fail(`declared frame of ${declared} bytes exceeds the ${maxBytes} byte limit`);
        }
      }
      // A prefix split across two reads must be judged too, and judging it means reading those four
      // bytes - not copying the chunk that carries them. Without this, a corrupt length arriving one
      // byte after a partial read would still be paid for before it was refused.
      if (pending.length > 0 && pending.length < FRAME_HEADER_BYTES) {
        const needed = FRAME_HEADER_BYTES - pending.length;
        if (chunk.length >= needed) {
          const prefix = Buffer.allocUnsafe(FRAME_HEADER_BYTES);
          pending.copy(prefix, 0);
          chunk.copy(prefix, pending.length, 0, needed);
          const declared = prefix.readUInt32BE(0);
          if (declared > maxBytes) {
            fail(`declared frame of ${declared} bytes exceeds the ${maxBytes} byte limit`);
          }
        }
      }
      const buf = pending.length === 0 ? chunk : Buffer.concat([pending, chunk]);
      const frames = [];
      let offset = 0;
      while (buf.length - offset >= FRAME_HEADER_BYTES) {
        const length = buf.readUInt32BE(offset);
        if (length > maxBytes) {
          fail(`declared frame of ${length} bytes exceeds the ${maxBytes} byte limit`);
        }
        if (buf.length - offset - FRAME_HEADER_BYTES < length) break;
        const start = offset + FRAME_HEADER_BYTES;
        frames.push(Buffer.from(buf.subarray(start, start + length)));
        offset = start + length;
      }
      pending = offset === 0 ? buf : Buffer.from(buf.subarray(offset));
      return frames;
    },
    get bufferedBytes() {
      return pending.length;
    },
    get failed() {
      return failed !== null;
    },
  };
}


