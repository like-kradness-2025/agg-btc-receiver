/**
 * The audit's remaining condition for bybit: a partial subscription failure (the venue refuses one
 * of the topics while acknowledging the others) ends the production run, and the system recovers on
 * the next run.
 *
 * The run is the real three-process supervisor forking `bin/role.mjs`, with the real bybit adapter
 * and a line-framed fake venue. Phase 1: the venue refuses allLiquidation.BTCUSDT, so the link is
 * failed (C3) and the run ends abnormally - non-zero, with the venue's own reason heard, and no
 * store lease left behind. Phase 2: a fresh run over the same stores and run id starts, the venue
 * acknowledges everything, and frames reach the board: the failed run did not strand the system.
 */

import { test } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';

import { createRunSupervisor } from '../src/supervisor/run.mjs';
import { createForkSpawner } from '../src/supervisor/process-spawner.mjs';
import { startBybitVenueServer } from '../test-support/bybit-venue-server.mjs';

const BYBIT_ADAPTER = fileURLToPath(new URL('../test-support/bybit-venue-adapter.mjs', import.meta.url));
const FAKE_WEBSOCKET = fileURLToPath(new URL('../test-support/fake-websocket.mjs', import.meta.url));

async function until(predicate, { timeoutMs = 20_000, stepMs = 10, label = 'the condition' } = {}) {
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    if (predicate()) return true;
    if (Date.now() >= deadline) throw new Error(`timed out (${timeoutMs} ms) waiting for ${label}`);
    await new Promise((done) => setTimeout(done, stepMs));
  }
}

function buildRun({ dir, runId, venuePort, exits, diagnostics }) {
  return createRunSupervisor({
    market: 'bybit_perp',
    stream: 'trades',
    venue: 'bybit_perp',
    runId,
    routerListenPath: join(dir, 'router.sock'),
    ingestStorePath: join(dir, 'ingest.sqlite'),
    organizeStorePath: join(dir, 'organize.sqlite'),
    bookStorePath: join(dir, 'book.sqlite'),
    spoolDir: join(dir, 'spool'),
    startupDeadlineMs: 15_000,
    readinessIntervalMs: 50,
    exit: (code) => exits.push(code),
    onDiagnostic: (diagnostic) => diagnostics.push(diagnostic),
    spawner: createForkSpawner({
      adapterSpec: { url: `ws://127.0.0.1:${venuePort}`, symbol: 'BTCUSDT' },
      adapterModule: BYBIT_ADAPTER,
      websocketModule: FAKE_WEBSOCKET,
      readinessIntervalMs: 50,
    }),
  });
}

test('a refused topic ends the production run, and the next run on the same stores recovers to serving', async () => {
  const dir = mkdtempSync(join(tmpdir(), 'bybit-subfail-'));
  const runId = 'run-bybit-refused';
  const refusing = await startBybitVenueServer({ refuseTopics: ['allLiquidation.BTCUSDT'], ackDelayMs: 120 });
  let first = null;
  let second = null;
  try {
    // Phase 1: one topic refused among the acknowledged ones - the run must not survive it.
    const exits = [];
    const diagnostics = [];
    first = buildRun({ dir, runId, venuePort: refusing.port, exits, diagnostics });
    const started = await first.start();
    await until(() => started.started === false || exits.length > 0 || first.stats.closed, {
      label: 'the refusal to end the run',
    });
    const failure = diagnostics.find((d) => d.kind === 'failure' && /ingest subscription failed/.test(d.reason ?? ''));
    assert.ok(failure, `the refusal is heard as a failure: ${JSON.stringify(diagnostics)}`);
    assert.match(failure.reason, /handler not found/);
    assert.match(failure.reason, /allLiquidation\.BTCUSDT/);
    assert.equal(first.stats.abnormal, true);
    if (started.started === true) {
      assert.deepEqual(exits, [1], 'the failed subscription ended the run non-zero');
    } else {
      assert.match(started.reason, /subscription failed/);
    }
    const closed = await first.close();
    assert.equal(closed.shutdownSucceeded, false, 'a fatal end is never a successful shutdown');
    assert.equal(first.exclusion.heldCount, 0, 'every store lease was released');

    // Phase 2: the next run over the same stores and run id establishes and serves.
    const accepting = await startBybitVenueServer({});
    const exits2 = [];
    const diagnostics2 = [];
    second = buildRun({ dir, runId, venuePort: accepting.port, exits: exits2, diagnostics: diagnostics2 });
    try {
      const startedAgain = await second.start();
      assert.equal(startedAgain.started, true, `the recovery run started: ${startedAgain.reason ?? ''}`);
      const pids = ['ingest', 'organize', 'book'].map((role) => second.children[role].process.pid);
      for (const pid of pids) assert.equal(Number.isInteger(pid) && pid > 0, true, 'each role is an OS process');
      assert.equal(new Set(pids).size, 3, 'the three roles are three distinct processes');
      assert.equal(pids.includes(process.pid), false, 'no role runs in the supervisor process');
      await until(() => second.children.ingest?.process.subscriptionState === 'acknowledged', {
        label: 'the recovery run to establish',
      });
      await until(() => accepting.connected, { label: 'the recovery venue socket' });
      accepting.sendSnapshot(100);
      accepting.sendDelta(101);
      await until(() => second.children.book?.process.appliedBoundary?.upToSeq === 2, {
        label: 'the board to serve the recovered frames',
      });
      assert.equal(second.stats.abnormal, false, 'the recovery run is not abnormal');
      const closedAgain = await second.close();
      assert.equal(closedAgain.shutdownSucceeded, true, 'the recovery run closes cleanly');
      assert.equal(second.exclusion.heldCount, 0);
    } finally {
      accepting.close();
    }
  } finally {
    for (const supervisor of [first, second]) {
      try {
        await supervisor?.close();
      } catch {
        /* already closed */
      }
    }
    refusing.close();
    rmSync(dir, { recursive: true, force: true });
  }
});
