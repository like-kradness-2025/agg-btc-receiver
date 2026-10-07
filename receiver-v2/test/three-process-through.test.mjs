/**
 * Stage 5b: the real three-process through test.
 *
 * The supervisor starts ingest, organize and the book as three processes behind its own router, wires
 * them, and runs the startup sequence (a)-(e). A fake venue socket feeds frames into real ingest, they
 * travel ingest -> organize -> book over the real IPC transport, and the board serves. Then the run is
 * stopped reception -> organization -> board without claiming complete. Store reopening invalidates
 * the running marker using the existing startup rule.
 *
 * Nothing here reaches into product code through a test-only route: the frames arrive at a fake socket,
 * the control messages travel on the stage-1 vocabulary, and the stores write to real files.
 */

import { test } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

import { createRunSupervisor, RUN_ROLES } from '../src/supervisor/run.mjs';
import { openOrganizeStore } from '../src/organize/store.mjs';

const MARKET = 'kraken_spot';
const STREAM = 'trades';
const VENUE = 'kraken';
const RUN = 'run-1';

async function until(predicate, { timeoutMs = 8000, stepMs = 5, label = 'the condition' } = {}) {
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    if (predicate()) return true;
    if (Date.now() >= deadline) throw new Error(`timed out (${timeoutMs} ms) waiting for ${label}`);
    await new Promise((done) => setTimeout(done, stepMs));
  }
}

/** A fake venue socket: a data frame per deliver, and a subscription acknowledgement on 'sub-ack'. */
function fakeSockets() {
  const sockets = [];
  const impl = function fakeSocket(url) {
    const socket = {
      url,
      sent: [],
      closed: false,
      onopen: null,
      onmessage: null,
      onclose: null,
      onerror: null,
      send(message) {
        socket.sent.push(message);
      },
      close() {
        socket.closed = true;
      },
      deliver(data) {
        socket.onmessage?.({ data });
      },
    };
    sockets.push(socket);
    return socket;
  };
  return { sockets, impl };
}

const venueAdapter = {
  url: 'ws://venue.test/ws',
  stream: STREAM,
  expectedSubscriptions: ['trades'],
  parse(raw) {
    const text = Buffer.isBuffer(raw) ? raw.toString('utf8') : String(raw);
    if (text === 'sub-ack') return { kind: 'subscription', key: 'trades', ok: true };
    return { kind: 'data', raw: text };
  },
  changesFor: (envelope) => ({
    replace: false,
    changes: [{ side: 'bid', price: 100 + envelope.receive_seq, size: envelope.receive_seq }],
  }),
  subscribeMessages: () => ['{"subscribe":"trades"}'],
  heartbeatMessage: () => null,
};

async function withRun(fn) {
  const dir = mkdtempSync(join(tmpdir(), 'run-supervisor-'));
  const steps = [];
  const venue = fakeSockets();
  const supervisor = createRunSupervisor({
    market: MARKET,
    stream: STREAM,
    venue: VENUE,
    adapter: venueAdapter,
    runId: RUN,
    routerListenPath: join(dir, 'router.sock'),
    ingestStorePath: join(dir, 'ingest.sqlite'),
    organizeStorePath: join(dir, 'organize.sqlite'),
    bookStorePath: join(dir, 'book.sqlite'),
    spoolDir: join(dir, 'spool'),
    webSocketImpl: venue.impl,
    startupDeadlineMs: 8000,
    onStep: (event) => steps.push(event.step),
  });
  try {
    return await fn({ dir, supervisor, steps, sockets: venue.sockets });
  } finally {
    try {
      await supervisor.close();
    } catch {
      /* already closed */
    }
    rmSync(dir, { recursive: true, force: true });
  }
}

test('the three real processes are spawned, wired, and the startup sequence runs (a)-(e) in order', async () => {
  await withRun(async ({ supervisor, steps }) => {
    const started = await supervisor.start();
    assert.equal(started.started, true, `the run started: ${started.reason ?? ''}`);

    const order = steps.filter((name) =>
      ['a:beginRun', 'b:boundary', 'c:drainSpool', 'd:deliverOwed', 'e:accept'].includes(name),
    );
    assert.deepEqual(
      order,
      ['a:beginRun', 'b:boundary', 'c:drainSpool', 'd:deliverOwed', 'e:accept'],
      'the startup sequence runs (a) through (e) in order',
    );
    assert.ok(steps.indexOf('d:deliverOwed') < steps.indexOf('e:accept'), '(d) precedes (e)');

    assert.equal(supervisor.router.stats.roles.length, 3, 'all three roles are bound to the router');
    assert.equal(supervisor.children.ingest.process.connectionId, `${RUN}:${VENUE}:${MARKET}:1`);
  });
});

test('frames flow ingest -> organize -> book and the board serves', async () => {
  await withRun(async ({ supervisor, sockets }) => {
    await supervisor.start();
    const ingest = supervisor.children.ingest.process;
    const book = supervisor.children.book.process;

    assert.equal(sockets.length, 1, 'the socket opened only after the book authorized and organize adopted');

    // Open the fake venue socket and acknowledge the subscription.
    const venueSocket = sockets[0];
    venueSocket.onopen();
    venueSocket.deliver('sub-ack');
    await until(() => ingest.subscriptionState === 'acknowledged', { label: 'the subscription to be established' });

    // Three frames arrive at the venue and travel the whole path.
    for (const seq of [1, 2, 3]) venueSocket.deliver(`{"seq":${seq}}`);
    await until(() => book.isRunning === true, { label: 'the board to serve' });

    assert.equal(book.appliedBoundary.upToSeq, 3, 'every frame reached the board');
    assert.equal(book.appliedBoundary.connectionId, `${RUN}:${VENUE}:${MARKET}:1`);
    assert.equal(ingest.receivedTails().length, 1, 'the receive tail was written on the reception side');
    assert.equal(ingest.receivedTails()[0].lastReceivedSeq, 3);

    const readiness = supervisor.readiness();
    assert.equal(readiness.ready, true, `the run is ready: ${JSON.stringify(readiness.reasons)}`);
    assert.deepEqual(RUN_ROLES.filter((role) => readiness.roles[role].connected), ['ingest', 'organize', 'book']);
  });
});

test('a clean stop keeps the processing order and leaves the run incomplete for restart', async () => {
  await withRun(async ({ supervisor, sockets }) => {
    await supervisor.start();
    const ingest = supervisor.children.ingest.process;
    const book = supervisor.children.book.process;
    const venueSocket = sockets[0];
    venueSocket.onopen();
    venueSocket.deliver('sub-ack');
    for (const seq of [1, 2, 3]) venueSocket.deliver(`{"seq":${seq}}`);
    await until(() => book.isRunning === true, { label: 'the board to serve' });
    assert.equal(ingest.receivedTails()[0].lastReceivedSeq, 3);

    const result = await supervisor.stop();

    assert.deepEqual(result.processingOrder, ['ingest', 'organize', 'book'], 'processing stops reception first');
    assert.equal(result.stopped, true);
    assert.equal('allAcked' in result, false, 'shutdown makes no final-tail judgement');
    assert.equal('completion' in result, false, 'shutdown makes no completeness claim');
    assert.equal(result.book.stopped, true, 'the board stop result was confirmed');
    assert.equal(result.abnormal, false, 'a clean stop is not an abnormal end');

    const organizeStorePath = supervisor.stats.roles.organize.storePath;
    const closed = await supervisor.close();
    assert.deepEqual(closed.terminationOrder, ['book', 'organize', 'ingest'], 'processes terminate in their own order');
    assert.equal(closed.shutdownSucceeded, true);
    assert.equal('completed' in closed, false);
    assert.equal(supervisor.exclusion.heldCount, 0, 'all store leases were released');

    // Organize's own store is free once the process is gone; the run marker is read back from it.
    const store = openOrganizeStore({ path: organizeStorePath, runId: `${RUN}-reader` });
    try {
      assert.equal(store.runMarkerState(RUN), 'invalidated', 'opening the store invalidates the stopped running marker');
      store.beginRun();
      assert.equal(store.runMarkerState(`${RUN}-reader`), 'running', 'the store can begin a new run');
    } finally {
      store.close();
    }
  });
});

test('a book failure restarts only the book, and the receive generation does not move', async () => {
  await withRun(async ({ supervisor }) => {
    await supervisor.start();
    const ingest = supervisor.children.ingest.process;
    const generationBefore = ingest.generation;
    const bookInstanceBefore = supervisor.children.book.instance;

    const outcome = await supervisor.handleChildFailure('book', { reason: 'a simulated book failure' });
    assert.equal(outcome.restarted, 'book', 'the book is restarted');
    assert.equal(outcome.renewGeneration, false, 'the restart did not renew the receive generation');
    assert.equal(ingest.generation, generationBefore, 'the receive generation is unchanged');
    assert.notEqual(supervisor.children.book.instance, bookInstanceBefore, 'a new book instance was spawned');
    await until(() => supervisor.router.channels().has('book'), { label: 'the new book to bind' });
  });
});

test('an unconfirmed book stop remains abnormal without a completeness judgement', async () => {
  await withRun(async ({ supervisor, sockets }) => {
    await supervisor.start();
    const ingest = supervisor.children.ingest.process;
    const book = supervisor.children.book.process;
    const venueSocket = sockets[0];
    venueSocket.onopen();
    venueSocket.deliver('sub-ack');
    for (const seq of [1, 2, 3]) venueSocket.deliver(`{"seq":${seq}}`);
    await until(() => book.isRunning === true, { label: 'the board to serve' });
    assert.equal(ingest.receivedTails()[0].lastReceivedSeq, 3);

    // Simulate a board whose stop cannot be confirmed: its stop returns no true result.
    book.stop = () => ({ stopped: false, reason: 'the stop could not be confirmed' });

    const result = await supervisor.stop();
    assert.equal(result.stopped, false, 'the processing stop was not confirmed');
    assert.equal('completion' in result, false);
    const closed = await supervisor.close();
    assert.equal(closed.shutdownSucceeded, false, 'termination cannot erase a stop failure');
    assert.equal(result.abnormal, true, 'the run is not a normal end');
  });
});


test('normal stop never seals tails, waits for all-ACK, or calls finalization, even before any frame', async () => {
  await withRun(async ({ supervisor, sockets }) => {
    await supervisor.start();
    const ingest = supervisor.children.ingest.process;
    const organize = supervisor.children.organize.process;
    const forbidden = () => { throw new Error('shutdown used a finalization proof'); };
    ingest.sealTails = forbidden;
    organize.prepareFinalize = forbidden;
    organize.finalize = forbidden;
    Object.defineProperty(organize, 'allAcked', { get: forbidden });
    const stopped = await supervisor.stop();
    assert.equal(stopped.stopped, true);
    assert.equal(stopped.abnormal, false);
    assert.equal(sockets[0].closed, true, 'new reception is fenced');
    assert.equal(organize.runMarkerState(RUN), 'running', 'stop does not write complete');
    const again = await supervisor.stop();
    assert.equal(again.stopped, true);
    const closed = await supervisor.close();
    assert.equal(closed.shutdownSucceeded, true);
  });
});

test('a stop RPC failure still stops the remaining roles and closes every store', async () => {
  await withRun(async ({ supervisor }) => {
    await supervisor.start();
    const organize = supervisor.children.organize.process;
    const book = supervisor.children.book.process;
    supervisor.children.ingest.process.stop = () => { throw new Error('stop IO failure'); };
    const result = await supervisor.stop();
    assert.equal(result.stopped, false);
    assert.equal(result.abnormal, true);
    assert.equal(organize.stats.stopped, true);
    assert.equal(book.stopped, true);
    const closed = await supervisor.close();
    assert.equal(closed.shutdownSucceeded, false);
    assert.equal(closed.terminationConfirmed, true);
    assert.equal(supervisor.exclusion.heldCount, 0);
  });
});


test('store close failures stay abnormal while all remaining roles are terminated', async () => {
  await withRun(async ({ supervisor }) => {
    await supervisor.start();
    const book = supervisor.children.book.process;
    const storePath = supervisor.stats.roles.book.storePath;
    const originalClose = book.close;
    book.close = () => { originalClose(); throw new Error('store close IO failure'); };
    const closed = await supervisor.close();
    assert.equal(closed.shutdownSucceeded, false);
    assert.equal(closed.abnormal, true);
    assert.deepEqual(closed.unconfirmedTermination, ['book']);
    assert.equal(supervisor.children.ingest, null);
    assert.equal(supervisor.children.organize, null);
    assert.equal(supervisor.exclusion.heldCount, 1, 'uncertain store ownership is retained');
    // The injected close threw after the real in-process store closed, so cleanup can confirm it.
    supervisor.exclusion.release({ path: storePath, confirmedTerminated: true });
  });
});
