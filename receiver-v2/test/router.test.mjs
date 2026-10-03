/**
 * Stage 5a (ruling ①): the supervisor owns the socket and routes by destination.
 *
 * These tests drive `src/supervisor/router.mjs` with the real IPC transport and fake role peers - the
 * supervisor is what routes, and the roles here are only what they must be: channels that announce a
 * role and record what reaches them. The router's contract under test:
 *   - a channel may say nothing until it has announced a known role;
 *   - a new instance of a role replaces the old channel (a restart is a fact about that role);
 *   - every message is delivered to the one destination its sender and type imply, and a message with
 *     no legal destination is refused, never invented;
 *   - the business payload passes through untouched;
 *   - the router never fabricates an `accepted` - a silent book produces no answer;
 *   - the relay is bounded and non-durable: an over-full destination is refused and the sender is told.
 */

import { test } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

import { connect } from '../src/ipc.mjs';
import { IPC_VERSION, makeMessage } from '../src/ipc-message.mjs';
import { createRouter, openRouter } from '../src/supervisor/router.mjs';

const MARKET = 'kraken_spot';
const STREAM = 'trades';

async function until(predicate, { timeoutMs = 4000, stepMs = 5 } = {}) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (predicate()) return;
    await new Promise((resolve) => setTimeout(resolve, stepMs));
  }
  throw new Error('timed out waiting for the condition');
}

/** Connect one role peer to the router and announce it. */
async function connectRole(path, role, instance) {
  const received = { controls: [], envelopes: [] };
  const channel = await connect(path, {
    batchFrames: 1,
    onControl: (message) => received.controls.push(message),
    onEnvelope: (envelope) => received.envelopes.push(envelope),
  });
  channel.sendControl(
    makeMessage({ version: IPC_VERSION, type: 'hello', role_instance: instance, run_id: 'run-1', payload: { role } }),
  );
  return { channel, received, role, instance };
}

function acceptMessage({ connectionId = 'run-1:kraken:kraken_spot:1', generation = 1, requestId = 'req-accept' } = {}) {
  return makeMessage({
    version: IPC_VERSION,
    type: 'accept',
    role_instance: 'ingest-1',
    request_id: requestId,
    run_id: 'run-1',
    connection_id: connectionId,
    generation,
    market: MARKET,
    stream: STREAM,
    payload: { first_seq: 1, takeover: false },
  });
}

async function withWorld(fn) {
  const dir = mkdtempSync(join(tmpdir(), 'router-'));
  const path = join(dir, 'router.sock');
  const routed = [];
  const router = await openRouter({
    listenPath: path,
    channelOptions: { batchFrames: 1 },
    onRouted: (event) => routed.push(event),
  });
  try {
    return await fn({ dir, path, router, routed });
  } finally {
    router.close();
    rmSync(dir, { recursive: true, force: true });
  }
}

test('① a channel may say nothing until it has announced a role', async () => {
  await withWorld(async ({ path, router }) => {
    const channel = await connect(path, { batchFrames: 1 });
    channel.sendControl(
      makeMessage({ version: IPC_VERSION, type: 'tail_sealed', role_instance: 'x', run_id: 'run-1', payload: {} }),
    );
    await until(() => router.refusals().some((r) => /has not announced a role/.test(r.reason)));
    assert.equal(router.routedCount, 0, 'nothing was routed');
    channel.close();
  });
});

test('① a hello naming an unknown role is refused', async () => {
  await withWorld(async ({ path, router }) => {
    const channel = await connect(path, { batchFrames: 1 });
    channel.sendControl(
      makeMessage({ version: IPC_VERSION, type: 'hello', role_instance: 'x', run_id: 'run-1', payload: { role: 'sensor' } }),
    );
    await until(() => router.refusals().some((r) => /unknown role/.test(r.reason)));
    assert.equal(router.stats.roles.length, 0);
    channel.close();
  });
});

test('① a new instance of a role replaces, and closes, the old channel', async () => {
  await withWorld(async ({ path, router }) => {
    const first = await connectRole(path, 'ingest', 'ingest-a');
    await until(() => router.instances().get('ingest') === 'ingest-a');
    const second = await connectRole(path, 'ingest', 'ingest-b');
    await until(() => router.instances().get('ingest') === 'ingest-b');
    assert.equal(router.channels().get('ingest') !== undefined, true, 'one live channel remains for the role');
    // The old channel is closed: a message on it no longer leaves.
    assert.equal(
      first.channel.sendControl(
        makeMessage({ version: IPC_VERSION, type: 'tail_sealed', role_instance: 'ingest-a', run_id: 'run-1', payload: {} }),
      ),
      false,
      'the replaced channel is closed',
    );
    second.channel.close();
  });
});

test('① ingest route: accept goes to the book, never to organize', async () => {
  await withWorld(async ({ path, router }) => {
    const ingest = await connectRole(path, 'ingest', 'ingest-1');
    const organize = await connectRole(path, 'organize', 'organize-1');
    const book = await connectRole(path, 'book', 'book-1');
    await until(() => router.stats.roles.length === 3);

    ingest.channel.sendControl(acceptMessage());
    await until(() => book.received.controls.some((m) => m.type === 'accept'));
    assert.equal(organize.received.controls.length, 0, 'organize never sees the authorization request');
    assert.equal(router.pendingAcceptCount, 1, 'the supervisor holds the request until the book answers');

    ingest.channel.close();
    organize.channel.close();
    book.channel.close();
  });
});

test('① the business payload passes through untouched', async () => {
  await withWorld(async ({ path, router }) => {
    const ingest = await connectRole(path, 'ingest', 'ingest-1');
    const organize = await connectRole(path, 'organize', 'organize-1');
    await until(() => router.stats.roles.length === 2);

    const payload = { tails: [{ connectionId: 'c1', lastReceivedSeq: 7 }], spool_empty: true, note: 'unchanged' };
    const message = makeMessage({
      version: IPC_VERSION,
      type: 'tail_sealed',
      role_instance: 'ingest-1',
      run_id: 'run-1',
      payload,
    });
    ingest.channel.sendControl(message);
    await until(() => organize.received.controls.some((m) => m.type === 'tail_sealed'));
    const received = organize.received.controls.find((m) => m.type === 'tail_sealed');
    assert.deepEqual(received.payload, payload, 'the payload is delivered exactly as sent');
    assert.equal(received.request_id, undefined);

    ingest.channel.close();
    organize.channel.close();
  });
});

test('① a silent book produces no accepted: the router never fabricates an answer', async () => {
  await withWorld(async ({ path, router }) => {
    const ingest = await connectRole(path, 'ingest', 'ingest-1');
    await connectRole(path, 'organize', 'organize-1');
    await connectRole(path, 'book', 'book-1');
    await until(() => router.stats.roles.length === 3);

    ingest.channel.sendControl(acceptMessage());
    // The book is connected but answers nothing (a dead book). Give the router every chance to lie.
    await new Promise((resolve) => setTimeout(resolve, 120));
    assert.equal(
      ingest.received.controls.some((m) => m.type === 'accepted'),
      false,
      'no accepted is invented for a book that did not answer',
    );
    assert.equal(router.pendingAcceptCount, 1, 'the request is still held, waiting for the book');
    assert.equal(router.routedCount, 1, 'only the request itself was routed');
  });
});

test('① a message with no route from its sender, or no destination connected, is refused', async () => {
  await withWorld(async ({ path, router }) => {
    const ingest = await connectRole(path, 'ingest', 'ingest-1');
    const book = await connectRole(path, 'book', 'book-1');
    await until(() => router.stats.roles.length === 2);

    // A book may not seal a tail: no such route.
    book.channel.sendControl(
      makeMessage({ version: IPC_VERSION, type: 'tail_sealed', role_instance: 'book-1', run_id: 'run-1', payload: {} }),
    );
    await until(() => router.refusals().some((r) => /no destination/.test(r.reason)));

    // A valid route whose destination is simply not connected is refused too, not invented.
    assert.equal(router.channels().get('organize'), undefined);
    book.channel.sendControl(
      makeMessage({
        version: IPC_VERSION,
        type: 'applied_ack',
        role_instance: 'book-1',
        run_id: 'run-1',
        market: MARKET,
        stream: STREAM,
        connection_id: 'c1',
        generation: 1,
        payload: { up_to_seq: 1 },
      }),
    );
    await until(() => router.refusals().some((r) => /no organize is connected/.test(r.reason)));

    ingest.channel.close();
    book.channel.close();
  });
});

// ---------------------------------------------------------------------------------------------------
// Unit-level: the bounded relay and the idempotent accept retry, driven with in-memory channels.
// ---------------------------------------------------------------------------------------------------

function memoryRole() {
  return {
    sent: [],
    envelopes: [],
    open: true,
    refuseControls: false,
    sendControl(message) {
      if (!this.open || this.refuseControls) return false;
      this.sent.push(message);
      return true;
    },
    sendEnvelope(envelope) {
      if (!this.open) return false;
      this.envelopes.push(envelope);
      return true;
    },
    close() {
      this.open = false;
    },
  };
}

function bindRole(router, channel, role, instance) {
  router.attach(channel);
  router.handleControl(
    makeMessage({ version: IPC_VERSION, type: 'hello', role_instance: instance, run_id: 'run-1', payload: { role } }),
    channel,
  );
  return channel;
}

test('① the relay is bounded: an over-full destination is refused, and the sender is told', () => {
  const router = createRouter();
  const ingest = bindRole(router, memoryRole(), 'ingest', 'ingest-1');
  const book = bindRole(router, memoryRole(), 'book', 'book-1');
  book.refuseControls = true;

  ingest.sendControl(acceptMessage());
  const outcome = router.handleControl(acceptMessage({ requestId: 'req-accept' }), ingest);
  assert.equal(outcome.refused, true, 'the over-full relay is refused');
  assert.ok(router.refusals().some((r) => /could not take the message/.test(r.reason)));
  const capacity = ingest.sent.find((m) => m.type === 'readiness');
  assert.ok(capacity, 'the sender is told its capacity is full, not that the message was accepted');
  assert.equal(capacity.payload.capacity, 'full');
});

test('② an accept lost on the way is recovered by an idempotent resend of the same request', () => {
  const router = createRouter();
  const ingest = bindRole(router, memoryRole(), 'ingest', 'ingest-1');
  const book = bindRole(router, memoryRole(), 'book', 'book-1');
  const organize = bindRole(router, memoryRole(), 'organize', 'organize-1');

  // The request reaches the book.
  ingest.sendControl(acceptMessage());
  router.handleControl(acceptMessage(), ingest);
  assert.equal(book.sent.filter((m) => m.type === 'accept').length, 1);

  // The book's answer is lost in transit: the supervisor still holds the request. The same request is
  // sent again (same request_id) - it must be idempotent, not a second distinct request.
  router.handleControl(acceptMessage(), ingest);
  assert.equal(book.sent.filter((m) => m.type === 'accept').length, 2, 'the request traveled again');
  assert.equal(router.pendingAcceptCount, 1, 'the same request, not a second one');

  // The book answers now; the router relays the authorization to organize, which adopts.
  const accepted = makeMessage({
    version: IPC_VERSION,
    type: 'accepted',
    role_instance: 'book-1',
    request_id: 'req-accept',
    run_id: 'run-1',
    market: MARKET,
    stream: STREAM,
    connection_id: 'run-1:kraken:kraken_spot:1',
    generation: 1,
    payload: { accepted: true, reason: '', first_seq: 1, takeover: false },
  });
  router.handleControl(accepted, book);
  assert.equal(organize.sent.filter((m) => m.type === 'accepted').length, 1, 'organize is asked to adopt');
  assert.equal(ingest.sent.filter((m) => m.type === 'accepted').length, 0, 'the book answer alone does not let ingest in');

  // organize confirms the adoption; only now does the answer reach ingest.
  const confirmation = makeMessage({
    version: IPC_VERSION,
    type: 'accepted',
    role_instance: 'organize-1',
    request_id: 'req-accept',
    run_id: 'run-1',
    market: MARKET,
    stream: STREAM,
    connection_id: 'run-1:kraken:kraken_spot:1',
    generation: 1,
    payload: { accepted: true, reason: '' },
  });
  router.handleControl(confirmation, organize);
  assert.equal(ingest.sent.filter((m) => m.type === 'accepted').length, 1, 'the adoption lets ingest open');
  assert.equal(router.pendingAcceptCount, 0, 'the request is settled');
});

test('② a stale authorization or confirmation for a request not held is refused', () => {
  const router = createRouter();
  const ingest = bindRole(router, memoryRole(), 'ingest', 'ingest-1');
  const book = bindRole(router, memoryRole(), 'book', 'book-1');
  const organize = bindRole(router, memoryRole(), 'organize', 'organize-1');

  const stray = makeMessage({
    version: IPC_VERSION,
    type: 'accepted',
    role_instance: 'book-1',
    request_id: 'req-never-asked',
    run_id: 'run-1',
    market: MARKET,
    stream: STREAM,
    connection_id: 'c1',
    generation: 1,
    payload: { accepted: true },
  });
  router.handleControl(stray, book);
  router.handleControl(makeMessage({ ...stray, role_instance: 'organize-1' }), organize);
  assert.equal(ingest.sent.length, 0, 'nothing was let in');
  assert.ok(router.refusals().some((r) => /not holding/.test(r.reason)), 'the stray answers are refused');
});

test('① the router refuses an envelope from a role with no frame route (the book)', () => {
  const router = createRouter();
  const book = bindRole(router, memoryRole(), 'book', 'book-1');
  const outcome = router.handleEnvelope({ connection_id: 'c1', receive_seq: 1 }, book);
  assert.equal(outcome.refused, true);
  assert.ok(router.refusals().some((r) => /no route for frames/.test(r.reason)));
});
