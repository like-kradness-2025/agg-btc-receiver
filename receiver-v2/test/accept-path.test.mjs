/**
 * Stage 5a (ruling ②): the accept path straightened.
 *
 * The real ingest, book and organize processes run behind the supervisor's router - not against one
 * another - and the accept travels the whole way: ingest issues it, the supervisor routes it to the
 * book, the book authorizes, the supervisor relays the authorization to organize, organize reflects
 * the adoption and confirms it, and only then is ingest's socket opened.
 *
 * What is fixed here:
 *   - the full ingest -> supervisor -> book -> organize -> ingest path completes, and the socket opens
 *     exactly once, after organize's adoption;
 *   - the book's success alone does NOT open the socket: with organize absent, the authorization has
 *     nowhere to be adopted and ingest stays shut;
 *   - the routing order is the contract's order.
 */

import { test } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

import { openRouter } from '../src/supervisor/router.mjs';
import { openOrganizeProcess } from '../src/organize/main.mjs';
import { openBookProcess } from '../src/book/main.mjs';
import { openIngestProcess } from '../src/ingest/main.mjs';

const MARKET = 'kraken_spot';
const STREAM = 'trades';
const VENUE = 'kraken';
const RUN = 'run-1';

async function until(predicate, { timeoutMs = 5000, stepMs = 5 } = {}) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (predicate()) return true;
    await new Promise((resolve) => setTimeout(resolve, stepMs));
  }
  throw new Error('timed out waiting for the condition');
}

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

const dataAdapter = {
  url: 'ws://venue.test/ws',
  stream: STREAM,
  parse: () => ({ kind: 'data' }),
  changesFor: () => ({ replace: false, changes: [] }),
  subscribeMessages: () => ['{"subscribe":"trades"}'],
  heartbeatMessage: () => null,
};

async function buildIngest({ dir, router, label = 'ingest' }) {
  const { sockets, impl } = fakeSockets();
  const starts = [];
  const diagnostics = [];
  const process = await openIngestProcess({ tailSaveMs: 0, 
    market: MARKET,
    stream: STREAM,
    adapter: dataAdapter,
    venue: VENUE,
    runId: RUN,
    roleInstance: `ingest-${RUN}`,
    webSocketImpl: impl,
    organizeSocketPath: router.path,
    ingestStorePath: join(dir, `${label}.sqlite`),
    spoolDir: join(dir, `${label}-spool`),
    channelOptions: { batchFrames: 1 },
    onStop: (stop) => starts.push(stop),
    onDiagnostic: (diagnostic) => diagnostics.push(diagnostic),
  });
  return { process, sockets, stops: starts, diagnostics };
}

async function buildBook({ dir, router }) {
  return openBookProcess({
    organizeSocketPath: router.path,
    market: MARKET,
    stream: STREAM,
    runId: RUN,
    roleInstance: 'book-1',
    storePath: join(dir, 'book.sqlite'),
    channelOptions: { batchFrames: 1 },
  });
}

async function buildOrganize({ dir, router }) {
  return openOrganizeProcess({ frameBatchMs: 0, frameBatchMax: 1, 
    routerSocketPath: router.path,
    market: MARKET,
    stream: STREAM,
    runId: RUN,
    roleInstance: 'organize-1',
    storePath: join(dir, 'organize.sqlite'),
    channelOptions: { batchFrames: 1 },
  });
}

async function withWorld(fn) {
  const dir = mkdtempSync(join(tmpdir(), 'accept-path-'));
  const routed = [];
  const router = await openRouter({
    listenPath: join(dir, 'router.sock'),
    channelOptions: { batchFrames: 1 },
    onRouted: (event) => routed.push(event),
  });
  world.router = router;
  world.dir = dir;
  world.routed = routed;
  try {
    return await fn({ dir, router, routed });
  } finally {
    try {
      world.ingest?.process.close();
    } catch {
      /* already closed */
    }
    try {
      world.book?.close();
    } catch {
      /* already closed */
    }
    try {
      world.organize?.close();
    } catch {
      /* already closed */
    }
    router.close();
    rmSync(dir, { recursive: true, force: true });
  }
}

const world = {};

test('② the whole accept path opens the socket once, and only after organize adopts', async () => {
  await withWorld(async ({ dir, router, routed }) => {
    const organize = await buildOrganize({ dir, router });
    const book = await buildBook({ dir, router });
    const ingest = await buildIngest({ dir, router });
    world.ingest = ingest;
    world.book = book;
    world.organize = organize;

    await until(() => router.stats.roles.length === 3);

    ingest.process.start();
    await until(() => ingest.sockets.length === 1);

    // The socket opened only because the whole path ran: the request went to the book, the book's
    // authorization went to organize, and organize's confirmation came back before anything opened.
    assert.deepEqual(
      routed.map((event) => event.to),
      ['book', 'organize', 'ingest'],
      'the request is authorized by the book, adopted by organize, and only then let into ingest',
    );
    assert.equal(ingest.process.connectionId, `${RUN}:${VENUE}:${MARKET}:1`);
    assert.equal(ingest.process.generation, 1);
    assert.ok(
      !ingest.diagnostics.some((d) => /not admitted/.test(String(d.reason))),
      'the connection was admitted',
    );
  });
});

test('② the book\u2019s success alone does not open the socket: with no organize, ingest stays shut', async () => {
  await withWorld(async ({ dir, router, routed }) => {
    const book = await buildBook({ dir, router });
    const ingest = await buildIngest({ dir, router, label: 'ingest-no-org' });
    world.ingest = ingest;
    world.book = book;

    await until(() => router.stats.roles.length === 2);

    ingest.process.start();
    // The book received the request and authorized it; the authorization could not be adopted because
    // organize is not there. Give the round trip every chance to (wrongly) open the socket.
    await until(() => routed.some((event) => event.to === 'book'));
    await new Promise((resolve) => setTimeout(resolve, 200));

    assert.equal(ingest.sockets.length, 0, 'the book\u2019s success alone opens nothing');
    assert.notEqual(
      ingest.process.state,
      'open',
      'reception is still waiting for the adoption it never heard',
    );
    assert.ok(
      router.refusals().some((r) => /no organize is connected/.test(r.reason)),
      'the relay was refused, not faked',
    );
  });
});
