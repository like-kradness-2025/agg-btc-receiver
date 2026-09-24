import { test } from 'node:test';
import assert from 'node:assert/strict';

import {
  FRAME_HEADER_BYTES,
  createFrameDecoder,
  decodeEnvelope,
  dedupeKey,
  encodeEnvelope,
  frame,
  isAfter,
  makeEnvelope,
} from '../src/envelope.mjs';

const base = {
  market: 'kraken_spot',
  stream: 'trades',
  connectionId: 'conn-1',
  receiveSeq: 7,
  recvTsMs: 1_792_000_000_000,
  recvMonoNs: 12_345_678_900,
  raw: '{"channel":"trade"}',
};

test('an envelope carries the receive metadata unchanged', () => {
  const env = makeEnvelope(base);
  assert.equal(env.market, 'kraken_spot');
  assert.equal(env.connection_id, 'conn-1');
  assert.equal(env.receive_seq, 7);
  assert.equal(env.recv_ts_ms, base.recvTsMs);
  assert.equal(env.recv_mono_ns, base.recvMonoNs);
  assert.equal(env.raw.toString('utf8'), '{"channel":"trade"}');
});

test('the envelope is frozen - receive metadata cannot be rewritten after the fact', () => {
  const env = makeEnvelope(base);
  assert.ok(Object.isFrozen(env));
  assert.throws(() => {
    'use strict';
    env.recv_ts_ms = 1;
  }, TypeError);
  assert.equal(env.recv_ts_ms, base.recvTsMs);
});

test('an envelope refuses metadata the rest of the structure could not trust', () => {
  assert.throws(() => makeEnvelope({ ...base, market: '' }), TypeError);
  assert.throws(() => makeEnvelope({ ...base, connectionId: undefined }), TypeError);
  assert.throws(() => makeEnvelope({ ...base, receiveSeq: -1 }), TypeError);
  assert.throws(() => makeEnvelope({ ...base, receiveSeq: 2 ** 33 }), TypeError);
  assert.throws(() => makeEnvelope({ ...base, recvTsMs: 0 }), TypeError);
  assert.throws(() => makeEnvelope({ ...base, recvMonoNs: 1.5 }), TypeError);
  assert.throws(() => makeEnvelope({ ...base, raw: 42 }), TypeError);
});

test('the dedupe key is the connection and the per-connection sequence', () => {
  const env = makeEnvelope(base);
  assert.equal(dedupeKey(env), 'conn-1:7');
  const sameSeqNewConnection = makeEnvelope({ ...base, connectionId: 'conn-2' });
  assert.notEqual(dedupeKey(env), dedupeKey(sameSeqNewConnection));
});

test('ordering is decided inside a connection by the monotonic clock', () => {
  const first = makeEnvelope(base);
  const later = makeEnvelope({ ...base, receiveSeq: 8, recvMonoNs: base.recvMonoNs + 1 });
  const earlier = makeEnvelope({ ...base, receiveSeq: 6, recvMonoNs: base.recvMonoNs - 1 });
  assert.equal(isAfter(first, later), true);
  assert.equal(isAfter(first, earlier), false);
  assert.equal(isAfter(null, first), true, 'the first frame of a connection has no predecessor');
});

test('a queued frame from a closed connection never blocks the new one', () => {
  const oldConn = makeEnvelope(base);
  const newConn = makeEnvelope({ ...base, connectionId: 'conn-2', receiveSeq: 1, recvMonoNs: 1 });
  assert.equal(isAfter(oldConn, newConn), true);
});

test('two frames cannot share one instant on one connection', () => {
  const first = makeEnvelope(base);
  const sameInstant = makeEnvelope({ ...base, receiveSeq: 8, recvMonoNs: base.recvMonoNs });
  assert.equal(isAfter(first, sameInstant), false);
});

test('an envelope survives the round trip byte for byte, raw bytes included', () => {
  const rawBytes = Buffer.from([0x7b, 0x00, 0xff, 0x7d]);
  const env = makeEnvelope({ ...base, raw: rawBytes, meta: { venue_seq: 99 } });
  const decoded = decodeEnvelope(encodeEnvelope(env));
  assert.equal(decoded.market, env.market);
  assert.equal(decoded.connection_id, env.connection_id);
  assert.equal(decoded.receive_seq, env.receive_seq);
  assert.equal(decoded.recv_ts_ms, env.recv_ts_ms);
  assert.equal(decoded.recv_mono_ns, env.recv_mono_ns);
  assert.deepEqual(decoded.raw, rawBytes);
  assert.deepEqual(decoded.meta, { venue_seq: 99 });
});

test('a replayed frame decodes to the original receive times, not new ones', () => {
  const env = makeEnvelope(base);
  const encoded = encodeEnvelope(env);
  const first = decodeEnvelope(encoded);
  const second = decodeEnvelope(encoded);
  assert.equal(first.recv_ts_ms, second.recv_ts_ms);
  assert.equal(first.recv_mono_ns, second.recv_mono_ns);
  assert.equal(first.receive_seq, second.receive_seq);
});

test('a truncated frame is refused instead of decoded to a guess', () => {
  const encoded = encodeEnvelope(makeEnvelope(base));
  // Too short to even hold a length prefix and a header: refused as malformed input.
  assert.throws(() => decodeEnvelope(encoded.subarray(0, FRAME_HEADER_BYTES + 3)), TypeError);
  assert.throws(() => decodeEnvelope(Buffer.alloc(3)), TypeError);
  // Long enough to read a prefix, but the prefix promises more bytes than exist: refused, not guessed.
  const lying = Buffer.from(encoded);
  lying.writeUInt32BE(lying.length + 10, 0);
  assert.throws(() => decodeEnvelope(lying), TypeError);
});

test('framing puts the length first so a reader can take whole frames', () => {
  const framed = frame(Buffer.from('abc'));
  assert.equal(framed.readUInt32BE(0), 3);
  assert.equal(framed.subarray(FRAME_HEADER_BYTES).toString('utf8'), 'abc');
});

test('a message split across two reads is held, not treated as an error', () => {
  const decoder = createFrameDecoder();
  const framed = frame(Buffer.from('hello world'));
  assert.deepEqual(decoder.push(framed.subarray(0, 5)), []);
  assert.equal(decoder.bufferedBytes, 5);
  const frames = decoder.push(framed.subarray(5));
  assert.equal(frames.length, 1);
  assert.equal(frames[0].toString('utf8'), 'hello world');
  assert.equal(decoder.bufferedBytes, 0);
});

test('several frames arriving in one read all come out', () => {
  const decoder = createFrameDecoder();
  const chunk = Buffer.concat([frame(Buffer.from('one')), frame(Buffer.from('two')), frame(Buffer.from('three'))]);
  const frames = decoder.push(chunk).map((f) => f.toString('utf8'));
  assert.deepEqual(frames, ['one', 'two', 'three']);
});

test('a corrupt length is refused before the bytes are buffered', () => {
  const decoder = createFrameDecoder({ maxBytes: 1024 });
  const header = Buffer.alloc(FRAME_HEADER_BYTES);
  header.writeUInt32BE(1025, 0);
  assert.throws(() => decoder.push(header), RangeError);
  assert.equal(decoder.bufferedBytes, 0, 'nothing was retained for the oversized frame');
});
