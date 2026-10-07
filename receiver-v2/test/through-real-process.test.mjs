/**
 * Stage 5c: the real three-process through test.
 *
 * Where `three-process-through.test.mjs` runs the three roles *in this process* (the test seam), this
 * file runs them as three real OS processes, forked by `createForkSpawner` from `bin/role.mjs`. The
 * supervisor's router still carries the business IPC; the fork's own channel carries only the
 * supervision calls. A fake venue - a line-framed TCP server - feeds frames into real ingest, which
 * derives their level changes and sends them ingest -> organize -> book; the board serves.
 *
 * The websocket implementation and the venue adapter are supplied as module paths (the role CLI's
 * seams): the production default is Node's global `WebSocket` and the built-in venue adapter, but a
 * synthetic venue needs its own. Here the adapter module is a one-line rename of the *built-in
 * kraken adapter* (so the real `changesFor`/`parse` code runs, in the v1 contract's shape), and the
 * fake venue speaks Kraken's own frames: a `subscriptionStatus` per subscription, then book frames.
 * The stores write to real files and the run marker is read back from organize's own store.
 */

import { test } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import net from 'node:net';
import { fileURLToPath } from 'node:url';

import { createRunSupervisor, RUN_ROLES } from '../src/supervisor/run.mjs';
import { createForkSpawner } from '../src/supervisor/process-spawner.mjs';
import { openOrganizeStore } from '../src/organize/store.mjs';
import { createOrganizeProcess } from '../src/organize/main.mjs';
import { createBookProcess } from '../src/book/main.mjs';
import { makeEnvelope } from '../src/envelope.mjs';
import { IPC_VERSION, makeMessage } from '../src/ipc-message.mjs';

const KRAKEN_ADAPTER = fileURLToPath(new URL('../test-support/kraken-venue-adapter.mjs', import.meta.url));
const FAKE_WEBSOCKET = fileURLToPath(new URL('../test-support/fake-websocket.mjs', import.meta.url));

const MARKET = 'fake_market';
const STREAM = 'trades';
const VENUE = 'fake_venue';
const RUN = 'run-real-1';
const PAIR = 'XBT/USD';

/** A Kraken book frame: the first is a snapshot (a replacement), the rest are updates (diffs). */
const krakenFrame = (seq) =>
  seq === 1
    ? JSON.stringify([1234, { bs: [['100.0', '1.0', '1.0']], as: [['101.0', '1.0', '1.0']], c: '1' }, 'book-1000', PAIR])
    : JSON.stringify([1234, { b: [[`${100 + seq}.0`, '1.0', '1.0']], a: [] }, 'book-1000', PAIR]);

async function until(predicate, { timeoutMs = 20_000, stepMs = 10, label = 'the condition' } = {}) {
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    if (predicate()) return true;
    if (Date.now() >= deadline) throw new Error(`timed out (${timeoutMs} ms) waiting for ${label}`);
    await new Promise((done) => setTimeout(done, stepMs));
  }
}

/** A fake venue server: one frame per line; the first `sub-ack` establishes the subscription. */
async function fakeVenue() {
  let socket = null;
  const server = net.createServer((connection) => {
    socket = connection;
    connection.on('data', () => {});
    connection.on('error', () => {});
  });
  await new Promise((resolve) => server.listen(0, '127.0.0.1', resolve));
  const { port } = server.address();
  return {
    port,
    get connected() {
      return socket !== null;
    },
    send(line) {
      socket.write(`${line}\n`);
    },
    ack() {
      // Kraken answers each subscription with its own `subscriptionStatus`; the link is established
      // only when every expected key is acknowledged, so both are sent.
      const status = (name) =>
        `{"event":"subscriptionStatus","status":"subscribed","pair":["${PAIR}"],"subscription":{"name":"${name}"}}`;
      socket.write(`${status('book')}\n${status('trade')}\n`);
    },
    close() {
      server.close();
    },
  };
}

async function withRun(fn) {
  const dir = mkdtempSync(join(tmpdir(), 'through-real-'));
  const venue = await fakeVenue();
  const steps = [];
  const diagnostics = [];
  const exits = [];
  let supervisor = null;
  const spawner = createForkSpawner({
    adapterSpec: { url: `ws://127.0.0.1:${venue.port}`, symbol: PAIR },
    adapterModule: KRAKEN_ADAPTER,
    websocketModule: FAKE_WEBSOCKET,
    readinessIntervalMs: 50,
    onDiagnostic: (d) => diagnostics.push(d),
  });
  supervisor = createRunSupervisor({
    market: MARKET,
    stream: STREAM,
    venue: VENUE,
    runId: RUN,
    routerListenPath: join(dir, 'router.sock'),
    ingestStorePath: join(dir, 'ingest.sqlite'),
    organizeStorePath: join(dir, 'organize.sqlite'),
    bookStorePath: join(dir, 'book.sqlite'),
    spoolDir: join(dir, 'spool'),
    startupDeadlineMs: 30_000,
    readinessIntervalMs: 50,
    onStep: (event) => steps.push(event.step),
    onChildExit: (info) => exits.push(info),
    spawner,
  });
  try {
    return await fn({ dir, supervisor, steps, diagnostics, exits, venue });
  } finally {
    try {
      await supervisor.close();
    } catch {
      /* already closed */
    }
    venue.close();
    rmSync(dir, { recursive: true, force: true });
  }
}

/** Establish the subscription and drive `count` frames into the board's applied boundary. */
async function serve(supervisor, venue, count) {
  await until(() => venue.connected, { label: 'the venue socket to open' });
  // The acknowledgement is idempotent; resend it until the link is established, so a slow/loaded child
  // that was not yet listening when the first one was written still gets it.
  const deadline = Date.now() + 20_000;
  while (supervisor.children.ingest?.process.subscriptionState !== 'acknowledged') {
    if (Date.now() >= deadline) throw new Error('timed out waiting for the subscription to be established');
    if (venue.connected) venue.ack();
    await new Promise((done) => setTimeout(done, 250));
  }
  for (let seq = 1; seq <= count; seq += 1) venue.send(krakenFrame(seq));
  await until(() => supervisor.children.book?.process.appliedBoundary?.upToSeq === count, {
    label: 'the board to serve every frame',
  });
}

test('three roles run as real OS processes: frames reach the board and a clean stop leaves no completion', async () => {
  await withRun(async ({ supervisor, steps, venue }) => {
    const started = await supervisor.start();
    assert.equal(started.started, true, `the run started: ${started.reason ?? ''}`);

    // Real separation: three distinct pids, none of them this process.
    const pids = RUN_ROLES.map((role) => supervisor.children[role].process.pid);
    for (const pid of pids) assert.equal(Number.isInteger(pid) && pid > 0, true, 'each role is an OS process');
    assert.equal(new Set(pids).size, 3, 'the three roles are three distinct processes');
    assert.equal(pids.includes(process.pid), false, 'no role runs in the supervisor process');

    const order = steps.filter((name) =>
      ['a:beginRun', 'b:boundary', 'c:drainSpool', 'd:deliverOwed', 'e:accept'].includes(name),
    );
    assert.deepEqual(order, ['a:beginRun', 'b:boundary', 'c:drainSpool', 'd:deliverOwed', 'e:accept']);

    await serve(supervisor, venue, 3);
    assert.equal(supervisor.children.book.process.appliedBoundary.upToSeq, 3, 'every frame reached the board');
    assert.equal(supervisor.children.book.process.appliedBoundary.connectionId, `${RUN}:${VENUE}:${MARKET}:1`);

    // The readiness reports from the child processes were observed and fed to the aggregator.
    await until(() => RUN_ROLES.every((role) => supervisor.readiness().roles[role].reported), {
      label: 'every role to report readiness',
    });
    const readiness = supervisor.readiness();
    assert.equal(readiness.ready, true, `the run is ready: ${JSON.stringify(readiness.reasons)}`);
    for (const role of RUN_ROLES) assert.equal(readiness.roles[role].reported, true, `${role} reported`);

    const result = await supervisor.stop();
    assert.deepEqual(result.processingOrder, ['ingest', 'organize', 'book']);
    assert.equal(result.stopped, true);
    assert.equal('allAcked' in result, false, 'shutdown makes no final-tail judgement');
    assert.equal('completion' in result, false, 'shutdown makes no completeness claim');
    assert.equal(result.book.stopped, true, 'the board stop result was confirmed');
    assert.equal(result.abnormal, false, 'a clean stop is not an abnormal end');

    const organizeStorePath = supervisor.stats.roles.organize.storePath;
    const closed = await supervisor.close();
    assert.deepEqual(closed.terminationOrder, ['book', 'organize', 'ingest'], 'processes terminate in their own order');
    assert.equal(closed.shutdownSucceeded, true);
    for (const pid of pids) assert.throws(() => process.kill(pid, 0), { code: 'ESRCH' }, 'every role exited');
    assert.equal('completed' in closed, false);
    assert.equal(supervisor.exclusion.heldCount, 0, 'all store leases were released');

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

test('startup refuses to admit a new connection while an old owed ledger frame is unresolved', async () => {
  const dir = mkdtempSync(join(tmpdir(), 'through-recovery-gate-'));
  const venue = await fakeVenue();
  const organizeStorePath = join(dir, 'organize.sqlite');
  const bookStorePath = join(dir, 'book.sqlite');
  const connectionId = `${RUN}:${VENUE}:${MARKET}:1`;
  const seedChannel = { sendControl: () => true, sendEnvelope: () => true, close() {} };
  let supervisor = null;
  const steps = [];
  try {
    const oldBook = createBookProcess({
      market: MARKET,
      stream: STREAM,
      runId: RUN,
      storePath: bookStorePath,
      roleInstance: 'book-seed',
    });
    const accepted = oldBook.handleControl(
      makeMessage({
        version: IPC_VERSION,
        type: 'accept',
        role_instance: 'ingest-seed',
        request_id: 'seed-accept',
        run_id: RUN,
        market: MARKET,
        stream: STREAM,
        connection_id: connectionId,
        generation: 1,
        payload: { first_seq: 1, takeover: false },
      }),
      seedChannel,
    );
    assert.equal(accepted.accepted, true, 'the previous connection is accepted in the persistent book store');
    oldBook.close();

    const oldOrganizer = createOrganizeProcess({
      market: MARKET,
      stream: STREAM,
      runId: RUN,
      storePath: organizeStorePath,
      roleInstance: 'organize-seed',
      rawWriter: () => true,
    });
    oldOrganizer.handleControl(
      makeMessage({
        version: IPC_VERSION,
        type: 'accepted',
        role_instance: 'book-seed',
        request_id: 'seed-accept',
        run_id: RUN,
        market: MARKET,
        stream: STREAM,
        connection_id: connectionId,
        generation: 1,
        payload: { accepted: true, first_seq: 1, takeover: false },
      }),
      seedChannel,
    );
    oldOrganizer.handleEnvelope(
      makeEnvelope({
        market: MARKET,
        stream: STREAM,
        connectionId,
        runId: RUN,
        venue: VENUE,
        generation: 1,
        receiveSeq: 1,
        recvTsMs: 1_001,
        recvMonoNs: 1,
        raw: 'unapplied-frame',
        meta: { first_seq: 1, changes_format: 'unsupported', changes: { replace: false, changes: [] } },
      }),
      seedChannel,
    );
    assert.equal(oldOrganizer.recoveryStatus().owedCount, 1, 'the organizer has one committed frame still owed to the board');
    oldOrganizer.close();

    supervisor = createRunSupervisor({
      market: MARKET,
      stream: STREAM,
      venue: VENUE,
      runId: RUN,
      routerListenPath: join(dir, 'router.sock'),
      ingestStorePath: join(dir, 'ingest.sqlite'),
      organizeStorePath,
      bookStorePath,
      spoolDir: join(dir, 'spool'),
      startupDeadlineMs: 3_000,
      onStep: (event) => steps.push(event.step),
      spawner: createForkSpawner({
        adapterSpec: { url: `ws://127.0.0.1:${venue.port}`, symbol: PAIR },
        adapterModule: KRAKEN_ADAPTER,
        websocketModule: FAKE_WEBSOCKET,
        readinessIntervalMs: 50,
      }),
    });

    const started = await supervisor.start();
    assert.equal(started.started, false, `an unapplied old frame fails startup closed: ${started.reason}`);
    assert.equal(steps.includes('e:accept'), false, 'the new generation was never requested before recovery');
    assert.equal(venue.connected, false, 'the new venue socket was never opened');

    const closed = await supervisor.close();
    assert.notEqual(closed.completed, true, 'the unresolved recovery wrote no clean completion');
    const reader = openOrganizeStore({ path: organizeStorePath, runId: `${RUN}-reader` });
    try {
      assert.notEqual(reader.runMarkerState(RUN), 'complete');
    } finally {
      reader.close();
    }
  } finally {
    if (supervisor !== null) {
      try {
        await supervisor.close();
      } catch {
        /* already closed */
      }
    }
    venue.close();
    rmSync(dir, { recursive: true, force: true });
  }
});

test('killing the book child alone keeps reception and organization running, restarts the book, and does not move the receive generation', async () => {
  await withRun(async ({ supervisor, venue, exits }) => {
    await supervisor.start();
    await serve(supervisor, venue, 3);

    const ingestPidBefore = supervisor.children.ingest.process.pid;
    const generationBefore = supervisor.children.ingest.process.generation;
    const connectionIdBefore = supervisor.children.ingest.process.connectionId;
    const bookInstanceBefore = supervisor.children.book.instance;

    // Kill only the book child, as a real death (not a close the supervisor asked for).
    process.kill(supervisor.children.book.process.pid, 'SIGKILL');

    await until(() => supervisor.children.book?.instance !== bookInstanceBefore, {
      label: 'the book to be restarted',
    });
    await until(() => supervisor.router.channels().has('book'), { label: 'the new book to bind to the router' });

    // Reception and organization survived the book's death: same processes, same connection.
    assert.equal(supervisor.children.ingest.process.pid, ingestPidBefore, 'ingest is the same process');
    assert.notEqual(supervisor.children.ingest.process.state, 'stopped', 'reception did not stop');
    assert.ok(supervisor.children.organize, 'organization is still running');
    assert.equal(supervisor.children.ingest.process.generation, generationBefore, 'the receive generation did not move');
    assert.equal(supervisor.children.ingest.process.connectionId, connectionIdBefore, 'the connection is unchanged');
    assert.equal(supervisor.stats.restarts.book, 1, 'the book alone was restarted');
    // The death was seen as a real process death, with its signal - not invented.
    assert.ok(
      exits.some((info) => info.role === 'book' && info.signal === 'SIGKILL'),
      `the supervisor saw the book's death with its signal: ${JSON.stringify(exits)}`,
    );

    // The restarted book serves again on the same connection.
    venue.send(krakenFrame(4));
    await until(() => supervisor.children.book?.process.appliedBoundary?.upToSeq === 4, {
      label: 'the restarted board to serve',
    });
  });
});
