import { test } from 'node:test';
import assert from 'node:assert/strict';

import { createFrameDecoder, encodeEnvelope, makeEnvelope } from '../src/envelope.mjs';
import { contiguousCeiling, createChannel, newCeiling } from '../src/ipc.mjs';

const envelope = (seq, connectionId = 'conn-1') =>
  makeEnvelope({
    market: 'kraken_spot',
    stream: 'trades',
    connectionId,
    receiveSeq: seq,
    recvTsMs: 1_792_000_000_000 + seq,
    recvMonoNs: 1_000_000 + seq,
    raw: `{"seq":${seq}}`,
  });

/** A socket that accepts nothing: writes report backpressure and never drain. */
function stuckSocket(overrides = {}) {
  const calls = { write: 0, destroy: 0 };
  const socket = {
    writableLength: 0,
    write: () => {
      calls.write += 1;
      return false;
    },
    once: () => {},
    end: () => {},
    destroy: () => {
      calls.destroy += 1;
    },
    on: (event, handler) => {
      socket.handlers[event] = handler;
    },
    handlers: {},
    ...overrides,
  };
  return { socket, calls };
}

test('nothing is acknowledged until the connection has actually started', () => {
  let state = newCeiling('conn-1'); // first sequence is 1
  assert.equal(state.upToSeq, null, 'null means nothing acknowledged, which is not the same as 0');
  state = contiguousCeiling(state, envelope(2));
  assert.equal(state.upToSeq, null, 'a later frame cannot start the ceiling on its own');
  state = contiguousCeiling(state, envelope(1));
  // 1 and 2 are both durable and contiguous now, so the ceiling opens over both of them at once.
  assert.equal(state.upToSeq, 2);
  assert.deepEqual(state.outOfOrder, []);
});

test('a connection whose first sequence is 0 acknowledges 0 when 0 is durable', () => {
  let state = newCeiling('conn-1', { firstSeq: 0 });
  state = contiguousCeiling(state, envelope(0));
  assert.equal(state.upToSeq, 0, '0 is a real sequence number here, not a placeholder');
  state = contiguousCeiling(state, envelope(1));
  assert.equal(state.upToSeq, 1);
});

test('a repeated out-of-order frame does not block the release behind a filled hole', () => {
  // The exact order that used to stick: a stale copy of 3 sat in the list and 5 never came out.
  let state = newCeiling('conn-1');
  for (const seq of [3, 3, 5, 1, 2, 4]) state = contiguousCeiling(state, envelope(seq));
  assert.equal(state.upToSeq, 5, 'every hole was filled, so everything is durable up to 5');
  assert.deepEqual(state.outOfOrder, []);
});

test('a queue over its bound refuses the frame instead of taking and dropping it', () => {
  const reported = [];
  const { socket } = stuckSocket();
  const channel = createChannel(socket, {
    batchFrames: 1_000, // keep everything in the batch so the bound is what decides
    maxBufferedBytes: 300,
    onBackpressure: (info) => reported.push(info),
  });
  const results = [];
  for (let i = 1; i <= 10; i += 1) results.push(channel.sendEnvelope(envelope(i)));
  assert.ok(results.includes(false), 'the caller learns it must keep or spool the frame');
  assert.equal(reported.length, 1, 'one report per episode, not one per frame');
  assert.ok(reported[0].bufferedBytes > 300);
  assert.ok(channel.queuedBytes <= 300, 'nothing over the bound was accepted');
});

test('incoming traffic never makes the outgoing queue look emptier than it is', () => {
  const reported = [];
  const { socket, calls } = stuckSocket({ writableLength: 5_000 });
  const channel = createChannel(socket, {
    batchFrames: 1_000,
    maxBufferedBytes: 4_000,
    onBackpressure: (info) => reported.push(info),
  });
  // The peer's own bytes are already buffered in the socket, so the queue is over its bound.
  assert.equal(channel.sendEnvelope(envelope(1)), false);
  assert.equal(reported.length, 1);
  assert.equal(calls.write, 0, 'the refused frame was not written either');
});

test('the canonical bytes cannot be edited through the envelope', () => {
  const env = makeEnvelope({
    market: 'kraken_spot',
    stream: 'trades',
    connectionId: 'conn-1',
    receiveSeq: 1,
    recvTsMs: 1_792_000_000_000,
    recvMonoNs: 1,
    raw: 'abc',
  });
  const handedOut = env.raw;
  handedOut[0] = 0x7a; // 'z'
  assert.equal(env.raw.toString('utf8'), 'abc', 'editing the copy did not touch the record');
  assert.equal(encodeEnvelope(env).subarray(-3).toString('utf8'), 'abc');
});

test('an oversized declared length is refused before any of it is buffered', () => {
  const decoder = createFrameDecoder({ maxBytes: 64 });
  const header = Buffer.alloc(4);
  header.writeUInt32BE(1_000_000, 0);
  assert.throws(() => decoder.push(header), RangeError);
  assert.equal(decoder.bufferedBytes, 0, 'no allocation was made for a frame that will never come');
  assert.equal(decoder.failed, true);
  assert.throws(() => decoder.push(Buffer.from('x')), RangeError, 'a desynchronised reader stays failed');
});

test('a protocol violation closes the channel instead of reading on', () => {
  const errors = [];
  const { socket, calls } = stuckSocket();
  const channel = createChannel(socket, { onError: (error) => errors.push(error) });
  const body = Buffer.from([0x7f, 0x00]); // unknown tag
  const header = Buffer.alloc(4);
  header.writeUInt32BE(body.length, 0);
  socket.handlers.data(Buffer.concat([header, body]));
  assert.equal(errors.length, 1);
  assert.match(String(errors[0].message), /unknown tag/);
  assert.equal(calls.destroy, 1, 'the stream cannot be resynchronised, so it is closed');
  assert.equal(channel.sendEnvelope(envelope(1)), false, 'nothing is sent on a failed channel');
});

test('a corrupt length arriving one byte after a partial read is still refused first', () => {
  const decoder = createFrameDecoder({ maxBytes: 64 });
  // One byte of a prefix, then the rest of it declares a million bytes.
  assert.deepEqual(decoder.push(Buffer.from([0x00])), []);
  const rest = Buffer.alloc(200 * 1024, 0);
  rest.writeUInt32BE(1_000_000, 0);
  rest[0] = 0x0f; // completes the corrupt length with the byte already held
  assert.throws(() => decoder.push(rest.subarray(1)), RangeError);
  assert.equal(decoder.bufferedBytes, 0, 'the declared length was judged before anything was copied');
  assert.equal(decoder.failed, true);
});

test('after a frame is refused, the rest of that chunk is not processed', () => {
  const errors = [];
  const delivered = [];
  const { socket, calls } = stuckSocket();
  const channel = createChannel(socket, {
    onEnvelope: (env) => delivered.push(env),
    onError: (error) => errors.push(error),
  });
  const empty = Buffer.alloc(4); // declares a frame of zero bytes: no tag, so it is a violation
  const good = Buffer.from(encodeEnvelope(envelope(1)));
  const goodLen = Buffer.alloc(4);
  goodLen.writeUInt32BE(good.length, 0);
  socket.handlers.data(Buffer.concat([empty, goodLen, good]));
  assert.equal(errors.length, 1);
  assert.equal(delivered.length, 0, 'a valid frame later in the same chunk is not data any more');
  assert.equal(calls.destroy, 1);
});
