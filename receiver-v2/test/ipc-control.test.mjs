import { test } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

import { encodeEnvelope, frame, makeEnvelope } from '../src/envelope.mjs';
import { encodeMessage } from '../src/ipc-message.mjs';
import {
  TAG_CONTROL,
  TAG_ENVELOPE,
  contiguousCeiling,
  connect,
  createChannel,
  listen,
  newCeiling,
} from '../src/ipc.mjs';

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

const durableAck = (overrides = {}) => ({
  version: 1,
  type: 'durable_ack',
  role_instance: 'organize-1',
  run_id: 'run-1',
  market: 'kraken_spot',
  stream: 'trades',
  connection_id: 'conn-1',
  generation: 0,
  payload: { up_to_seq: 2 },
  ...overrides,
});

const tagged = (tag, bytes) => frame(Buffer.concat([Buffer.from([tag]), bytes]));
const controlFrame = (message) => tagged(TAG_CONTROL, encodeMessage(message));
const envelopeFrame = (env) => tagged(TAG_ENVELOPE, encodeEnvelope(env));

/** A socket that records every write and lets a test drive its events. */
function fakeSocket({ writableLength = 0, writeResult = true } = {}) {
  const writes = [];
  const handlers = {};
  let destroyed = 0;
  const socket = {
    writableLength,
    write(buf) {
      writes.push(Buffer.from(buf));
      return writeResult;
    },
    once() {},
    end() {},
    destroy() {
      destroyed += 1;
    },
    on(event, handler) {
      handlers[event] = handler;
    },
  };
  return { socket, writes, handlers, destroyed: () => destroyed };
}

/** A clock a test fires by hand, so batching is proven without waiting wall time. */
function fakeClock() {
  let next = 1;
  const timers = new Map();
  return {
    setTimer(fn, ms) {
      const id = next++;
      timers.set(id, { fn, ms });
      return id;
    },
    clearTimer(id) {
      timers.delete(id);
    },
    pending: () => timers.size,
    delays: () => [...timers.values()].map((entry) => entry.ms),
    fireAll() {
      const entries = [...timers.values()];
      timers.clear();
      for (const entry of entries) entry.fn();
    },
  };
}

test('a full batch of 512 frames goes out at once, a single one waits for the timer', () => {
  const { socket, writes } = fakeSocket();
  const clock = fakeClock();
  const channel = createChannel(socket, {
    batchFrames: 512,
    batchMs: 100,
    setTimer: clock.setTimer,
    clearTimer: clock.clearTimer,
  });

  channel.sendControl(durableAck());
  assert.equal(writes.length, 0, 'one frame is held, not written immediately');
  assert.equal(clock.pending(), 1, 'a batch timer is armed');
  assert.deepEqual(clock.delays(), [100], 'the batch window is 100ms');

  clock.fireAll();
  assert.equal(writes.length, 1, 'the timer flushed the held frame');

  for (let i = 0; i < 512; i += 1) channel.sendControl(durableAck());
  assert.equal(writes.length, 2, 'the 512th frame flushes the batch immediately, not on the timer');
});

test('control messages ride the same bounded queue and are refused when it is full', () => {
  const reported = [];
  const { socket } = fakeSocket({ writeResult: false });
  const channel = createChannel(socket, {
    batchFrames: 1_000,
    maxBufferedBytes: 100,
    onBackpressure: (info) => reported.push(info),
  });
  const results = [];
  for (let i = 0; i < 10; i += 1) results.push(channel.sendControl(durableAck()));
  assert.ok(results.includes(false), 'the caller learns the control queue is over its bound');
  assert.equal(reported.length, 1, 'one report per episode');
  assert.equal(channel.queuedBytes, 0, 'nothing over the bound was accepted');
});

test('an invalid control message is refused before it is framed, never sent', () => {
  const { socket, writes } = fakeSocket();
  const channel = createChannel(socket, {});
  assert.throws(() => channel.sendControl(durableAck({ type: 'nope' })), /unknown message type/);
  assert.throws(() => channel.sendControl({ version: 1, type: 'stop', role_instance: 'x' }), /request_id/);
  assert.equal(writes.length, 0, 'nothing off-contract reached the wire');
});

test('a control frame split across reads is delivered once it is whole', () => {
  const got = [];
  const { socket, handlers } = fakeSocket();
  createChannel(socket, { onControl: (message) => got.push(message) });
  const bytes = controlFrame(durableAck());
  handlers.data(bytes.subarray(0, 3));
  assert.equal(got.length, 0, 'a partial frame is held, not guessed at');
  handlers.data(bytes.subarray(3));
  assert.equal(got.length, 1);
  assert.equal(got[0].type, 'durable_ack');
});

test('consecutive control frames in one chunk all arrive', () => {
  const got = [];
  const { socket, handlers } = fakeSocket();
  createChannel(socket, { onControl: (message) => got.push(message) });
  handlers.data(Buffer.concat([controlFrame(durableAck()), controlFrame(durableAck({ request_id: 'r2' }))]));
  assert.equal(got.length, 2);
  assert.equal(got[0].request_id, undefined);
  assert.equal(got[1].request_id, 'r2');
});

test('envelopes and control messages multiplex without being confused', () => {
  const log = [];
  const { socket, handlers } = fakeSocket();
  createChannel(socket, {
    onEnvelope: (env) => log.push(`env:${env.receive_seq}`),
    onControl: (message) => log.push(`ctl:${message.type}`),
  });
  handlers.data(
    Buffer.concat([
      envelopeFrame(envelope(1)),
      controlFrame(durableAck()),
      envelopeFrame(envelope(2)),
    ]),
  );
  assert.deepEqual(log, ['env:1', 'ctl:durable_ack', 'env:2'], 'the two paths stay apart and in order');
});

test('a control frame that violates the contract reports once and closes the channel', () => {
  const errors = [];
  const { socket, handlers, destroyed } = fakeSocket();
  const channel = createChannel(socket, { onError: (error) => errors.push(error) });
  // A valid frame carrying a message that is missing the fields durable_ack requires.
  const bad = tagged(TAG_CONTROL, Buffer.from(JSON.stringify({ version: 1, type: 'durable_ack' }), 'utf8'));
  handlers.data(bad);
  assert.equal(errors.length, 1, 'one report, not one per problem');
  assert.match(String(errors[0].message), /missing required field/);
  assert.equal(destroyed(), 1, 'a stream that cannot be trusted is closed');
  assert.equal(channel.sendEnvelope(envelope(1)), false, 'nothing is sent on a failed channel');
});

test('a durable acknowledgement round-trips over a real socket and matches the ceiling', async () => {
  const dir = await mkdtemp(join(tmpdir(), 'ipc-control-'));
  const path = join(dir, 'sock');
  const got = { envelopes: [], controls: [] };
  const server = await listen(path, {
    onEnvelope: (env) => got.envelopes.push(env),
    onControl: (message) => got.controls.push(message),
  });
  const client = await connect(path);

  // The ceiling is the authority for the value the ack carries, not a guess.
  let state = newCeiling('conn-1');
  state = contiguousCeiling(state, envelope(1));
  state = contiguousCeiling(state, envelope(2));
  client.sendEnvelope(envelope(1));
  client.sendEnvelope(envelope(2));
  client.sendAck({
    roleInstance: 'organize-1',
    runId: 'run-1',
    market: 'kraken_spot',
    stream: 'trades',
    connectionId: 'conn-1',
    generation: 0,
    upToSeq: state.upToSeq,
    capacity: 'ok',
  });
  client.flush();

  await until(() => got.envelopes.length === 2 && got.controls.length === 1);
  assert.equal(got.controls[0].type, 'durable_ack');
  assert.equal(got.controls[0].role_instance, 'organize-1');
  assert.equal(got.controls[0].payload.up_to_seq, 2);
  assert.equal(got.envelopes[1].receive_seq, 2);

  client.close();
  await server.close();
  await rm(dir, { recursive: true, force: true });
});

async function until(predicate, { timeoutMs = 4000, stepMs = 10 } = {}) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (predicate()) return;
    await new Promise((resolve) => setTimeout(resolve, stepMs));
  }
  throw new Error('timed out waiting for the condition');
}
