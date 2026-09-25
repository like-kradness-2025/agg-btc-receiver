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
  client.sendAck({ connectionId: 'conn-1', upToSeq: 2, capacity: 'ok' });
  client.flush();

  await until(() => got.envelopes.length === 2 && got.controls.length === 1);
  assert.equal(got.envelopes[0].receive_seq, 1);
  assert.equal(got.envelopes[1].receive_seq, 2);
  assert.equal(got.envelopes[1].raw.toString('utf8'), '{"seq":2}');
  assert.equal(got.envelopes[1].connection_id, 'conn-1');
  assert.deepEqual(got.controls[0], { t: 'ack', connection_id: 'conn-1', up_to_seq: 2, capacity: 'ok' });

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
