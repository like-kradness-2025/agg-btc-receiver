import { test } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

import { makeEnvelope } from '../src/envelope.mjs';
import { contiguousCeiling, connect, createChannel, listen, newCeiling } from '../src/ipc.mjs';

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

/** Wait for a condition, failing the test rather than hanging forever. */
async function until(predicate, { timeoutMs = 4000, stepMs = 10 } = {}) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (predicate()) return;
    await new Promise((resolve) => setTimeout(resolve, stepMs));
  }
  throw new Error('timed out waiting for the condition');
}

test('the ceiling advances while everything before it is durable', () => {
  let state = newCeiling('conn-1');
  state = contiguousCeiling(state, envelope(1));
  state = contiguousCeiling(state, envelope(2));
  assert.equal(state.upToSeq, 2);
  assert.deepEqual(state.outOfOrder, []);
});

test('the ceiling stops at a hole instead of skipping over it', () => {
  let state = newCeiling('conn-1');
  state = contiguousCeiling(state, envelope(1));
  state = contiguousCeiling(state, envelope(3)); // 2 is missing
  assert.equal(state.upToSeq, 1, 'acknowledging 3 here would claim 2 was durable');
  state = contiguousCeiling(state, envelope(5));
  assert.equal(state.upToSeq, 1);
  assert.deepEqual(state.outOfOrder, [3, 5]);
});

test('a hole that gets filled releases everything after it', () => {
  let state = newCeiling('conn-1');
  state = contiguousCeiling(state, envelope(1));
  state = contiguousCeiling(state, envelope(3));
  state = contiguousCeiling(state, envelope(5));
  state = contiguousCeiling(state, envelope(2)); // the resend that closes the gap
  assert.equal(state.upToSeq, 3, '3 was already durable, so it is released with the hole');
  state = contiguousCeiling(state, envelope(4));
  assert.equal(state.upToSeq, 5);
  assert.deepEqual(state.outOfOrder, []);
});

test('a resend of something already acknowledged changes nothing', () => {
  let state = newCeiling('conn-1');
  state = contiguousCeiling(state, envelope(1));
  state = contiguousCeiling(state, envelope(2));
  const before = state;
  state = contiguousCeiling(state, envelope(2));
  state = contiguousCeiling(state, envelope(1));
  assert.equal(state.upToSeq, 2);
  assert.deepEqual(state, before);
});

test("a frame from another connection never advances this ceiling", () => {
  let state = newCeiling('conn-1');
  state = contiguousCeiling(state, envelope(1));
  const other = contiguousCeiling(state, envelope(2, 'conn-2'));
  assert.equal(other.upToSeq, 1);
  assert.deepEqual(other.outOfOrder, [], "another connection's sequences are not ours to track");
});

test('envelopes and control messages arrive on their own paths', async () => {
  const dir = await mkdtemp(join(tmpdir(), 'ipc-'));
  const path = join(dir, 'sock');
  const got = { envelopes: [], controls: [] };
  const server = await listen(path, {
    onEnvelope: (env) => got.envelopes.push(env),
    onControl: (msg) => got.controls.push(msg),
  });
  const client = await connect(path);
  client.sendEnvelope(envelope(1));
  client.sendEnvelope(envelope(2));
  client.sendAck({
    roleInstance: 'organize-1',
    runId: 'run-1',
    market: 'kraken_spot',
    stream: 'trades',
    connectionId: 'conn-1',
    generation: 0,
    upToSeq: 2,
    capacity: 'ok',
  });
  client.flush();

  await until(() => got.envelopes.length === 2 && got.controls.length === 1);
  assert.equal(got.envelopes[0].receive_seq, 1);
  assert.equal(got.envelopes[1].receive_seq, 2);
  assert.equal(got.envelopes[1].raw.toString('utf8'), '{"seq":2}');
  assert.equal(got.envelopes[1].connection_id, 'conn-1');
  assert.deepEqual(got.controls[0], {
    version: 1,
    type: 'durable_ack',
    role_instance: 'organize-1',
    run_id: 'run-1',
    market: 'kraken_spot',
    stream: 'trades',
    connection_id: 'conn-1',
    generation: 0,
    payload: { up_to_seq: 2, capacity: 'ok' },
  });

  client.close();
  await server.close();
  await rm(dir, { recursive: true, force: true });
});

test('a queue over its bound is reported rather than absorbed', () => {
  const reported = [];
  const fakeSocket = {
    write: () => false, // a peer that never drains
    once: () => {},
    end: () => {},
    on: () => {},
    writableLength: 0,
  };
  const channel = createChannel(fakeSocket, {
    batchFrames: 1,
    maxBufferedBytes: 100,
    onBackpressure: (info) => reported.push(info),
  });
  let accepted = true;
  for (let i = 1; i <= 50; i += 1) accepted = channel.sendEnvelope(envelope(i)) && accepted;
  assert.equal(reported.length >= 1, true, 'the caller was told the queue is over its bound');
  assert.equal(channel.isBackpressured, true);
  assert.equal(accepted, false, 'the caller learns it must spool or stop');
  assert.ok(reported[0].bufferedBytes > 100);
});

test('a refused write arms one drain listener, however many writes are refused', () => {
  const drainListeners = [];
  const fakeSocket = {
    write: () => false, // a peer that never drains
    once: (event, fn) => {
      if (event !== 'drain') return;
      // A real socket removes a once-listener when it fires; the fake has to do the same.
      const wrapped = () => {
        const index = drainListeners.indexOf(wrapped);
        if (index >= 0) drainListeners.splice(index, 1);
        fn();
      };
      drainListeners.push(wrapped);
    },
    removeListener: (event, fn) => {
      const index = drainListeners.indexOf(fn);
      if (index >= 0) drainListeners.splice(index, 1);
    },
    end: () => {},
    on: () => {},
    writableLength: 0,
  };
  let drained = 0;
  const channel = createChannel(fakeSocket, {
    batchFrames: 1,
    onDrain: () => {
      drained += 1;
    },
  });
  for (let i = 1; i <= 5; i += 1) channel.sendEnvelope(envelope(i));
  assert.equal(drainListeners.length, 1, 'one listener, not one per refused write');
  drainListeners[0](); // the socket drains
  assert.equal(drained, 1, 'the recovery callback fires once');
  assert.equal(drainListeners.length, 0, 'and the listener is spent');
  channel.sendEnvelope(envelope(6));
  assert.equal(drainListeners.length, 1, 'the next refusal arms a fresh one');
});

test("the channel's own bound announces its relief like the socket's", async () => {
  const fakeSocket = {
    write: () => true,
    once: () => {},
    removeListener: () => {},
    end: () => {},
    on: () => {},
    writableLength: 0,
  };
  let drained = 0;
  const channel = createChannel(fakeSocket, {
    batchFrames: 100,
    maxBufferedBytes: 700,
    onDrain: () => {
      drained += 1;
    },
  });
  let refused = false;
  for (let i = 1; i <= 10 && !refused; i += 1) refused = channel.sendEnvelope(envelope(i)) === false;
  assert.equal(refused, true, 'the bound refused something');
  assert.equal(drained, 0, 'nothing is announced while the bound holds');
  channel.flush();
  await new Promise((resolve) => setImmediate(resolve));
  assert.equal(drained, 1, 'the relief is announced once');
});

test('an unknown tag is reported as an error, not treated as data', () => {
  const errors = [];
  const handlers = {};
  const fakeSocket = {
    write: () => true,
    once: () => {},
    end: () => {},
    writableLength: 0,
    on: (event, handler) => {
      handlers[event] = handler;
    },
  };
  const channel = createChannel(fakeSocket, { onError: (error) => errors.push(error) });
  const body = Buffer.from([0xff, 0x00]);
  const header = Buffer.alloc(4);
  header.writeUInt32BE(body.length, 0);
  handlers.data(Buffer.concat([header, body]));
  assert.equal(errors.length, 1);
  assert.match(String(errors[0].message), /unknown tag/);
  assert.equal(channel.queuedBytes, 0);
});

test('a close with frames still in the queue is reported, not dropped in silence', () => {
  const errors = [];
  const handlers = {};
  const fakeSocket = {
    write: () => true,
    once: () => {},
    end: () => {},
    writableLength: 0,
    on: (event, handler) => {
      handlers[event] = handler;
    },
  };
  const channel = createChannel(fakeSocket, { batchFrames: 100, onError: (error) => errors.push(error) });
  channel.sendEnvelope(envelope(1));
  assert.equal(channel.queuedBytes > 0, true, 'the frame is queued, not yet written');

  handlers.close();
  assert.equal(errors.length, 1, 'the lost queue is reported once');
  assert.match(String(errors[0].message), /1 unsent frame/);
  handlers.close();
  assert.equal(errors.length, 1, 'one report per channel, however many closes arrive');
});

test('a close with nothing queued is not an error', () => {
  const errors = [];
  const handlers = {};
  const fakeSocket = {
    write: () => true,
    once: () => {},
    end: () => {},
    writableLength: 0,
    on: (event, handler) => {
      handlers[event] = handler;
    },
  };
  const channel = createChannel(fakeSocket, { onError: (error) => errors.push(error) });
  handlers.close();
  assert.equal(errors.length, 0);
});

test('a close after a failure is not reported a second time', () => {
  const errors = [];
  const handlers = {};
  const fakeSocket = {
    write: () => true,
    once: () => {},
    end: () => {},
    writableLength: 0,
    on: (event, handler) => {
      handlers[event] = handler;
    },
  };
  const channel = createChannel(fakeSocket, { batchFrames: 100, onError: (error) => errors.push(error) });
  channel.sendEnvelope(envelope(1));
  handlers.error(new Error('the pipe broke'));
  handlers.close();
  assert.equal(errors.length, 1, 'the failure owns the report; the close adds none');
  assert.match(String(errors[0].message), /the pipe broke/);
});
