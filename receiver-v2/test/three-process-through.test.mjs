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
    // Set 6d: the tail is written on its own clock now, so the row appears within the save interval.
    await until(() => ingest.receivedTails()[0]?.lastReceivedSeq === 3, { label: 'the receive tail to be written' });
    assert.equal(ingest.receivedTails().length, 1, 'the receive tail was written on the reception side');

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
    await until(() => ingest.receivedTails()[0]?.lastReceivedSeq === 3, { label: 'the receive tail to be written' });

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
    await until(() => ingest.receivedTails()[0]?.lastReceivedSeq === 3, { label: 'the receive tail to be written' });

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

test('a restart records the interval an unclean earlier run left behind as a suspected gap', async () => {
  const dir = mkdtempSync(join(tmpdir(), 'run-restart-gaps-'));
  const paths = {
    routerListenPath: join(dir, 'router.sock'),
    ingestStorePath: join(dir, 'ingest.sqlite'),
    organizeStorePath: join(dir, 'organize.sqlite'),
    bookStorePath: join(dir, 'book.sqlite'),
    spoolDir: join(dir, 'spool'),
  };
  const firstVenue = fakeSockets();
  const first = createRunSupervisor({
    market: MARKET,
    stream: STREAM,
    venue: VENUE,
    adapter: venueAdapter,
    runId: 'run-1',
    ...paths,
    webSocketImpl: firstVenue.impl,
    startupDeadlineMs: 8000,
  });
  try {
    await first.start();
    const socket = firstVenue.sockets[0];
    socket.onopen();
    socket.deliver('sub-ack');
    for (const seq of [1, 2, 3]) socket.deliver(`{"seq":${seq}}`);
    await until(() => first.children.book.process.isRunning === true, { label: 'the board to serve' });
    // Set 6d: the tail is written on its own clock, so the restart check waits for the save interval.
    await until(
      () => first.children.ingest.process.receivedTails()[0]?.lastReceivedSeq === 3,
      { label: 'the receive tail to be written' },
    );
    // The run stops without completing: its marker is not `complete`, so its tail is only a lower
    // bound on what it heard.
    await first.stop();
  } finally {
    await first.close();
  }

  const secondVenue = fakeSockets();
  const second = createRunSupervisor({
    market: MARKET,
    stream: STREAM,
    venue: VENUE,
    adapter: venueAdapter,
    runId: 'run-2',
    ...paths,
    webSocketImpl: secondVenue.impl,
    startupDeadlineMs: 8000,
  });
  try {
    await second.start();
    const gaps = second.children.organize.process.suspectedGaps();
    assert.equal(gaps.length, 1, 'the unclean earlier run left exactly one suspected interval');
    assert.equal(gaps[0].stream, STREAM);
    assert.match(gaps[0].reason, /did not close cleanly received up to sequence 3 on run-1:/);
    assert.ok(gaps[0].from_ms <= gaps[0].to_ms, 'the interval runs from the tail to this restart');
  } finally {
    await second.close();
    rmSync(dir, { recursive: true, force: true });
  }
});

test('a run that died before it heard anything is still accused by its start marker', async () => {
  const dir = mkdtempSync(join(tmpdir(), 'run-restart-gaps-empty-'));
  const paths = {
    routerListenPath: join(dir, 'router.sock'),
    ingestStorePath: join(dir, 'ingest.sqlite'),
    organizeStorePath: join(dir, 'organize.sqlite'),
    bookStorePath: join(dir, 'book.sqlite'),
    spoolDir: join(dir, 'spool'),
  };
  const firstVenue = fakeSockets();
  const first = createRunSupervisor({
    market: MARKET,
    stream: STREAM,
    venue: VENUE,
    adapter: venueAdapter,
    runId: 'run-1',
    ...paths,
    webSocketImpl: firstVenue.impl,
    startupDeadlineMs: 8000,
  });
  try {
    await first.start();
    // The socket is open, but no frame ever arrives: only the reception's start marker exists.
    assert.equal(firstVenue.sockets.length, 1);
    const tails = first.children.ingest.process.receivedTails();
    assert.equal(tails.length, 1, 'the reception start was written down before the socket opened');
    assert.equal(tails[0].lastReceivedSeq, 0, 'and it claims nothing yet');
    await first.stop();
  } finally {
    await first.close();
  }

  const secondVenue = fakeSockets();
  const second = createRunSupervisor({
    market: MARKET,
    stream: STREAM,
    venue: VENUE,
    adapter: venueAdapter,
    runId: 'run-2',
    ...paths,
    webSocketImpl: secondVenue.impl,
    startupDeadlineMs: 8000,
  });
  try {
    await second.start();
    const gaps = second.children.organize.process.suspectedGaps();
    assert.equal(gaps.length, 1, 'even a run that heard nothing leaves its interval accused');
    assert.match(gaps[0].reason, /received up to sequence 0 on run-1:/);
  } finally {
    await second.close();
    rmSync(dir, { recursive: true, force: true });
  }
});
