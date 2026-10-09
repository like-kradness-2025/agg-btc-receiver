/**
 * Stage 5a (ruling ③): the level changes are derived on reception, carried on the envelope, and held
 * for recovery.
 *
 * What is fixed here:
 *   - the format has a version and a validation rule; a missing or malformed block is refused, never
 *     read as an empty change;
 *   - ingest derives the block with the venue adapter and writes it into the envelope;
 *   - the derived result is held: the spool stores it on the frame, and the delivery ledger keeps the
 *     frame's `meta` (and `raw`) verbatim, so a recovery or a redelivery reuses the same result.
 */

import { test } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

import { makeEnvelope } from '../src/envelope.mjs';
import {
  CHANGES_FORMAT,
  attachChanges,
  deriveChanges,
  readChanges,
  validateChanges,
} from '../src/changes.mjs';
import { createOrganizeProcess } from '../src/organize/main.mjs';
import { openIngestProcess } from '../src/ingest/main.mjs';
import { IPC_VERSION, makeMessage } from '../src/ipc-message.mjs';
import { startFakeOrganize } from '../test-support/fake-organize.mjs';

const MARKET = 'kraken_spot';
const STREAM = 'trades';
const VENUE = 'kraken';
const RUN = 'run-1';
const CID = `${RUN}:${VENUE}:${MARKET}:1`;

const baseEnvelope = (seq, meta = undefined) =>
  makeEnvelope({
    market: MARKET,
    stream: STREAM,
    connectionId: CID,
    runId: RUN,
    venue: VENUE,
    generation: 1,
    receiveSeq: seq,
    recvTsMs: 1_792_000_000_000 + seq,
    recvMonoNs: 1_000_000_000 + seq,
    raw: `{"seq":${seq}}`,
    ...(meta === undefined ? {} : { meta }),
  });

async function until(predicate, { timeoutMs = 4000, stepMs = 5 } = {}) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (predicate()) return;
    await new Promise((resolve) => setTimeout(resolve, stepMs));
  }
  throw new Error('timed out waiting for the condition');
}

// ---------------------------------------------------------------------------------------------------
// The format and its validation rule
// ---------------------------------------------------------------------------------------------------

test('③ the format accepts the two contract shapes and refuses everything else', () => {
  assert.equal(validateChanges({ replace: true, levels: [{ side: 'bid', price: 1, size: 2 }] }).ok, true);
  assert.equal(validateChanges({ replace: false, changes: [{ side: 'ask', price: 1, size: 0 }] }).ok, true);

  // A bare array is not the contract's shape; it is refused, not coerced.
  assert.equal(validateChanges([{ side: 'bid', price: 1, size: 2 }]).ok, false);
  assert.equal(validateChanges({ replace: 'yes', changes: [] }).ok, false);
  assert.equal(validateChanges({ replace: true }).ok, false, 'a replacement must carry levels');
  assert.equal(validateChanges({ replace: false }).ok, false, 'a diff must carry changes');
  assert.equal(
    validateChanges({ replace: false, changes: [{ side: 'middle', price: 1, size: 1 }] }).ok,
    false,
    'a malformed level is refused',
  );
});

test('③ a frame with a missing block, a wrong version or a malformed block is refused', () => {
  assert.equal(readChanges(baseEnvelope(1)).ok, false, 'no meta at all is refused');
  assert.equal(readChanges(baseEnvelope(1, { first_seq: 1 })).ok, false, 'no format is refused');

  const wrongVersion = baseEnvelope(1, { changes_format: 'v0', changes: { replace: false, changes: [] } });
  const wrong = readChanges(wrongVersion);
  assert.equal(wrong.ok, false);
  assert.match(wrong.reason, /format is not supported/);

  const formatOnly = baseEnvelope(1, { changes_format: CHANGES_FORMAT });
  assert.equal(readChanges(formatOnly).ok, false, 'a format with no changes is refused, not read as empty');

  const valid = attachChanges(baseEnvelope(1), { replace: false, changes: [{ side: 'bid', price: 1, size: 2 }] });
  const read = readChanges(valid);
  assert.equal(read.ok, true);
  assert.deepEqual(read.changes, [{ side: 'bid', price: 1, size: 2 }]);
});

test('③ deriveChanges refuses an adapter that cannot derive, and a malformed result', () => {
  const adapter = (changesFor) => (typeof changesFor === 'function' ? { changesFor } : {});
  assert.equal(deriveChanges(adapter(undefined), baseEnvelope(1)).ok, false, 'no changesFor is refused');
  assert.equal(deriveChanges(adapter(() => null), baseEnvelope(1)).ok, false, 'null is refused');
  assert.equal(deriveChanges(adapter(() => undefined), baseEnvelope(1)).ok, false, 'undefined is refused');
  assert.equal(deriveChanges(adapter(() => [{ side: 'bid', price: 1, size: 1 }]), baseEnvelope(1)).ok, false, 'a bare array is refused');
  const ok = deriveChanges(adapter(() => ({ replace: false, changes: [{ side: 'bid', price: 1, size: 1 }] })), baseEnvelope(1));
  assert.equal(ok.ok, true);
});

// ---------------------------------------------------------------------------------------------------
// Ingest derives and holds the result
// ---------------------------------------------------------------------------------------------------

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

async function buildIngest({ dir, organize, adapter, label = 'a', spoolOptions = {}, spoolDir = null }) {
  const { sockets, impl } = fakeSockets();
  const diagnostics = [];
  const gaps = [];
  const process = await openIngestProcess({ tailSaveMs: 0, 
    market: MARKET,
    stream: STREAM,
    adapter,
    venue: VENUE,
    runId: RUN,
    roleInstance: `ingest-${label}`,
    webSocketImpl: impl,
    organizeSocketPath: organize.server.path,
    ingestStorePath: join(dir, `ingest-${label}.sqlite`),
    spoolDir: spoolDir ?? join(dir, `spool-${label}`),
    spoolOptions,
    channelOptions: { batchFrames: 1 },
    onDiagnostic: (d) => diagnostics.push(d),
    onGap: (g) => gaps.push(g),
  });
  return { process, sockets, diagnostics, gaps };
}

test('③ ingest derives the changes with the adapter and carries them on the frame', async () => {
  const dir = mkdtempSync(join(tmpdir(), 'changes-ingest-'));
  const organize = await startFakeOrganize(join(dir, 'organize.sock'), { batchFrames: 1 });
  const adapter = {
    url: 'ws://venue.test/ws',
    stream: STREAM,
    parse: () => ({ kind: 'data' }),
    changesFor: (envelope) => ({ replace: false, changes: [{ side: 'bid', price: 100 + envelope.receive_seq, size: 1 }] }),
    subscribeMessages: () => ['{"subscribe":"trades"}'],
    heartbeatMessage: () => null,
  };
  const h = await buildIngest({ dir, organize, adapter });
  try {
    h.process.start();
    await until(() => h.sockets.length === 1);
    h.sockets[0].onopen();
    h.sockets[0].deliver('{"seq":1}');
    await until(() => organize.state.envelopes.length === 1);

    const frame = organize.state.envelopes[0];
    assert.equal(frame.meta.changes_format, CHANGES_FORMAT, 'the format version travels with the frame');
    assert.deepEqual(frame.meta.changes, { replace: false, changes: [{ side: 'bid', price: 101, size: 1 }] });
  } finally {
    h.process.close();
    await organize.close();
    rmSync(dir, { recursive: true, force: true });
  }
});

test('③ a frame whose changes cannot be derived is refused and reported, never sent as empty', async () => {
  const dir = mkdtempSync(join(tmpdir(), 'changes-refuse-'));
  const organize = await startFakeOrganize(join(dir, 'organize.sock'), { batchFrames: 1 });
  const adapter = {
    url: 'ws://venue.test/ws',
    stream: STREAM,
    parse: () => ({ kind: 'data' }),
    // No changesFor: the derivation cannot be made.
    subscribeMessages: () => ['{"subscribe":"trades"}'],
    heartbeatMessage: () => null,
  };
  const h = await buildIngest({ dir, organize, adapter });
  try {
    h.process.start();
    await until(() => h.sockets.length === 1);
    h.sockets[0].onopen();
    h.sockets[0].deliver('{"seq":1}');
    await until(() => h.diagnostics.some((d) => /level changes were refused/.test(String(d.reason))));

    assert.equal(organize.state.envelopes.length, 0, 'nothing was sent on as an empty change');
    assert.ok(h.gaps.some((g) => /level changes/.test(String(g.reason))), 'the refusal is recorded, not hidden');
  } finally {
    h.process.close();
    await organize.close();
    rmSync(dir, { recursive: true, force: true });
  }
});

test('③ the spool holds the derived changes, so a resend sends the same result', async () => {
  const dir = mkdtempSync(join(tmpdir(), 'changes-spool-'));
  const organize = await startFakeOrganize(join(dir, 'organize.sock'), { batchFrames: 1 });
  const adapter = {
    url: 'ws://venue.test/ws',
    stream: STREAM,
    parse: () => ({ kind: 'data' }),
    changesFor: (envelope) => ({ replace: false, changes: [{ side: 'ask', price: 200 + envelope.receive_seq, size: 2 }] }),
    subscribeMessages: () => ['{"subscribe":"trades"}'],
    heartbeatMessage: () => null,
  };
  const h = await buildIngest({ dir, organize, adapter, label: 'spool', spoolOptions: { segmentBytes: 4096 } });
  try {
    h.process.start();
    await until(() => h.sockets.length === 1);
    h.sockets[0].onopen();

    // Organize reports no room: frames go to the spool, where the derived block is stored with them.
    organize.sendReadiness({ capacity: 'full' });
    await until(() => h.process.organizeCapacity === 'full');
    h.sockets[0].deliver('{"seq":1}');
    h.sockets[0].deliver('{"seq":2}');
    await until(() => h.process.stats.spooledFrames === 2);

    const spooled = [...h.process.spool.drain()];
    assert.equal(spooled.length, 2, 'the spool holds both frames');
    assert.equal(spooled[0].meta.changes_format, CHANGES_FORMAT);
    assert.deepEqual(spooled[0].meta.changes, { replace: false, changes: [{ side: 'ask', price: 201, size: 2 }] });

    // Capacity returns: the resend hands back the stored frame, block and all - the same result.
    organize.sendReadiness({ capacity: 'ok' });
    await until(() => organize.state.envelopes.length === 2);
    assert.deepEqual(
      organize.state.envelopes[0].meta.changes,
      spooled[0].meta.changes,
      'the redelivered frame carries the same derived changes the spool held',
    );
  } finally {
    h.process.close();
    await organize.close();
    rmSync(dir, { recursive: true, force: true });
  }
});

// ---------------------------------------------------------------------------------------------------
// The delivery ledger keeps the frame's changes verbatim
// ---------------------------------------------------------------------------------------------------

test('③ the delivery ledger holds the frame and its derived changes, so a redelivery reuses the same result', () => {
  const dir = mkdtempSync(join(tmpdir(), 'changes-ledger-'));
  try {
    const process = createOrganizeProcess({ frameBatchMs: 0, frameBatchMax: 1, 
      market: MARKET,
      stream: STREAM,
      runId: RUN,
      storePath: join(dir, 'organize.sqlite'),
      rawWriter: () => true,
    });
    const channel = {
      sent: [],
      sendControl(message) {
        this.sent.push(message);
        return true;
      },
      sendEnvelope() {
        return true;
      },
      close() {},
    };
    process.handleControl(
      makeMessage({
        version: IPC_VERSION,
        type: 'accepted',
        role_instance: 'book-1',
        request_id: 'req-1',
        run_id: RUN,
        market: MARKET,
        stream: STREAM,
        connection_id: CID,
        generation: 1,
        payload: { accepted: true, first_seq: 1 },
      }),
      channel,
    );

    const changes = { replace: false, changes: [{ side: 'bid', price: 42, size: 7 }] };
    const frame = attachChanges(baseEnvelope(1), changes);
    process.handleEnvelope(frame, channel);

    const entry = process.ledgerEntries()[0];
    assert.equal(entry.state, 'owed');
    assert.equal(entry.meta.changes_format, CHANGES_FORMAT, 'the ledger kept the block with the frame');
    assert.deepEqual(entry.meta.changes, changes, 'the exact derived result is held, not re-derived');
    process.close();
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});
