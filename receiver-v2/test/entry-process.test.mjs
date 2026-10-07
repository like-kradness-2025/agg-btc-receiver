/**
 * Set 7 / stage 5d: the real process entrance, verified as real processes.
 *
 * The entrance (`bin/receiver.mjs`) is now the supervisor plus three forked role processes (§5.8
 * ruling ⑭). These tests start it as a child process, drive it with signals, and read the run marker
 * back out of *organize's* store (organization writes it) and the board's position out of *the book's*
 * store - so the exit codes and the run record are observed where an operator would observe them.
 *
 * Signals stop the three role processes and close their stores without writing complete. Tests use a
 * real local WebSocket venue through Node's production WebSocket, then restart from the same stores.
 */

import { test } from 'node:test';
import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { mkdtempSync, rmSync, writeFileSync, existsSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { DatabaseSync } from 'node:sqlite';

import { openOrganizeStore } from '../src/organize/store.mjs';
import { startVenueServer } from '../test-support/ws-venue-server.mjs';
import { acquireStoreLock } from '../src/supervisor/store-lock.mjs';
import { loadConfig, adapterFor } from '../src/entry/config.mjs';

const PACKAGE_DIR = fileURLToPath(new URL('..', import.meta.url));
const ENTRY = fileURLToPath(new URL('../bin/receiver.mjs', import.meta.url));
const UNREACHABLE = 'ws://127.0.0.1:1/';
const MARKET = 'kraken_spot';
const STREAM = 'trades';

async function withDir(fn) {
  const dir = mkdtempSync(join(tmpdir(), 'entry-process-'));
  try {
    return await fn(dir);
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
}

function writeConfig(dir, overrides = {}) {
  const config = {
    venue: 'kraken',
    market: MARKET,
    stream: STREAM,
    symbol: 'XBT/USD',
    // Three role databases, one file per role, named explicitly (§5.8 ruling ⑮).
    stores: {
      ingest: join(dir, 'ingest.sqlite'),
      organize: join(dir, 'organize.sqlite'),
      book: join(dir, 'book.sqlite'),
    },
    spoolDir: join(dir, 'spool'),
    url: UNREACHABLE,
    ...overrides,
  };
  const path = join(dir, 'receiver.config.json');
  writeFileSync(path, JSON.stringify(config, null, 2));
  return { path, config };
}

function startChild(configPath, extraArgs = []) {
  const child = spawn(process.execPath, [ENTRY, '--config', configPath, ...extraArgs], {
    cwd: PACKAGE_DIR,
    stdio: ['ignore', 'pipe', 'pipe'],
  });
  let stdout = '';
  let stderr = '';
  child.stdout.on('data', (chunk) => {
    stdout += chunk;
  });
  child.stderr.on('data', (chunk) => {
    stderr += chunk;
  });
  const exited = new Promise((resolve) => child.on('exit', (code, signal) => resolve({ code, signal })));
  return {
    child,
    exited,
    get stdout() {
      return stdout;
    },
    get stderr() {
      return stderr;
    },
  };
}

/** Resolve once the child has printed a marker line; reject if it exits first or never does. */
function waitForStdout(handle, marker, timeoutMs = 20_000) {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error(`timed out waiting for "${marker}"; stdout=${handle.stdout}`)), timeoutMs);
    const check = () => {
      if (handle.stdout.includes(marker)) {
        clearTimeout(timer);
        resolve(handle.stdout);
      }
    };
    handle.child.stdout.on('data', check);
    handle.exited.then(({ code, signal }) => {
      clearTimeout(timer);
      reject(new Error(`the child exited before "${marker}" (code=${code}, signal=${signal}); stdout=${handle.stdout} stderr=${handle.stderr}`));
    });
    check();
  });
}

async function until(predicate, { timeoutMs = 20_000, stepMs = 20, label = 'the condition' } = {}) {
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    if (predicate()) return true;
    if (Date.now() >= deadline) throw new Error(`timed out (${timeoutMs} ms) waiting for ${label}`);
    await new Promise((done) => setTimeout(done, stepMs));
  }
}

function waitForExitOrTimeout(handle, timeoutMs, label) {
  let timer;
  return Promise.race([
    handle.exited,
    new Promise((_, reject) => {
      timer = setTimeout(() => reject(new Error(label)), timeoutMs);
    }),
  ]).finally(() => clearTimeout(timer));
}

/** Read the organize store's marker for one run. */
function markerState(organizePath, runId) {
  const store = openOrganizeStore({ path: organizePath, runId: `${runId}-reader` });
  try {
    return store.runMarkerState(runId);
  } finally {
    store.close();
  }
}

/** The book's applied position for this board, read from the book's own store. */
function bookApplied(bookPath) {
  if (!existsSync(bookPath)) return null;
  const db = new DatabaseSync(bookPath);
  try {
    const row = db
      .prepare('SELECT up_to_receive_seq FROM applied_boundary WHERE market = ? AND stream = ?')
      .get(MARKET, STREAM);
    return row ? row.up_to_receive_seq ?? null : null;
  } finally {
    db.close();
  }
}

/** A live venue: the child processes connect to it through Node's global WebSocket. */
async function runWithVenue(dir, fn) {
  const venue = await startVenueServer();
  const { path, config } = writeConfig(dir, { url: `ws://127.0.0.1:${venue.port}/` });
  const handle = startChild(path);
  try {
    return await fn({ venue, handle, config, configPath: path });
  } finally {
    try {
      handle.child.kill('SIGKILL');
    } catch {
      /* already gone */
    }
    venue.close();
  }
}

/** Establish the subscription and drive `count` frames into the book, proven through the book store. */
async function serve(venue, config, handle, count) {
  await until(() => venue.connected, { label: 'the venue socket to be opened' });
  await waitForStdout(handle, 'receiver: established', 20_000);
  for (let seq = 1; seq <= count; seq += 1) venue.sendFrame(seq);
  await until(() => bookApplied(config.stores.book) === count, { label: 'the board to serve every frame' });
}

test('a config that cannot be read ends the process with code 1', async () => {
  await withDir(async (dir) => {
    const handle = startChild(join(dir, 'does-not-exist.json'));
    const { code } = await handle.exited;
    assert.equal(code, 1, 'a missing config file is a failed run');
    assert.match(handle.stderr, /config file could not be read/);
  });
});

test('the command line accepts only --config and its path', async () => {
  await withDir(async (dir) => {
    const { path, config } = writeConfig(dir, { startupDeadlineMs: 1_000 });
    const handle = startChild(path, ['--verbose']);
    const { code } = await handle.exited;

    assert.equal(code, 1);
    assert.match(handle.stderr, /usage: receiver\.mjs --config/);
    assert.doesNotMatch(handle.stdout, /receiver: started/);
    for (const rolePath of Object.values(config.stores)) {
      assert.equal(existsSync(rolePath), false, 'arguments are rejected before opening stores');
    }
  });
});

test('a config that asks for a raw file is refused, and no raw file is written', async () => {
  await withDir(async (dir) => {
    const rawPath = join(dir, 'raw.log');
    const { path } = writeConfig(dir, { raw: rawPath });
    const handle = startChild(path);
    const { code } = await handle.exited;

    assert.equal(code, 1, `a config with a raw destination is a failed run; stderr=${handle.stderr}`);
    assert.match(handle.stderr, /"raw" is not supported/);
    assert.equal(existsSync(rawPath), false, 'nothing was written to the raw path');
  });
});

test('a config that names bitfinex is accepted only for its board stream', async () => {
  await withDir(async (dir) => {
    const { path } = writeConfig(dir, { venue: 'bitfinex', market: 'bitfinex_spot', stream: 'book', symbol: 'tBTCUSD' });
    const config = loadConfig(path);
    assert.equal(config.stream, 'book');
    const adapter = adapterFor(config);
    assert.equal(adapter.stream, 'book');
    assert.equal(adapter.boundary, 'sequence');
  });
});

test('a startup deadline outside the allowed range is refused before anything is opened', async () => {
  await withDir(async (dir) => {
    for (const bad of [0, -1, 1.5, 3_600_001, '60000']) {
      const { path, config } = writeConfig(dir, { startupDeadlineMs: bad });
      const handle = startChild(path);
      const { code } = await handle.exited;
      assert.equal(code, 1, `"${bad}" is a failed run; stderr=${handle.stderr}`);
      assert.match(handle.stderr, /startupDeadlineMs/, `the refused key is named for "${bad}"`);
      for (const rolePath of Object.values(config.stores)) {
        assert.equal(existsSync(rolePath), false, `nothing was opened for "${bad}"`);
      }
    }
  });
});

test('a config whose stores cannot be opened ends the process with code 1 and leaves no completion', async () => {
  await withDir(async (dir) => {
    // The parent directory of the role databases does not exist: SQLite cannot open them, so the run
    // cannot begin. Each store is named explicitly, so all three share the missing parent.
    const missing = join(dir, 'missing-parent');
    const { path, config } = writeConfig(dir, {
      stores: {
        ingest: join(missing, 'ingest.sqlite'),
        organize: join(missing, 'organize.sqlite'),
        book: join(missing, 'book.sqlite'),
      },
    });
    const handle = startChild(path);
    const { code } = await handle.exited;

    assert.equal(code, 1, `the process ended non-zero; stderr=${handle.stderr}`);
    assert.equal(existsSync(config.stores.organize), false, 'the organize store was never created, so no completion can exist');
  });
});

test('a startup the structure refuses ends the process with code 1 and writes no completion', async () => {
  await withDir(async (dir) => {
    // The spool path is a regular file, so ingest cannot be spawned. Organize and the book come up
    // first (the startup order), so organize's store is opened and left behind by the failed run.
    const spoolAsFile = join(dir, 'spool-is-a-file');
    writeFileSync(spoolAsFile, 'not a directory');
    const { path, config } = writeConfig(dir, { spoolDir: spoolAsFile });

    const handle = startChild(path);
    const { code } = await handle.exited;

    assert.equal(code, 1, `the process ended non-zero; stderr=${handle.stderr}`);
    assert.equal(existsSync(config.stores.organize), true, 'organize store was opened');
    const rows = (() => {
      const db = new DatabaseSync(config.stores.organize);
      try {
        return db.prepare('SELECT COUNT(*) AS n FROM run_marker WHERE state = ?').get('complete').n;
      } finally {
        db.close();
      }
    })();
    assert.equal(rows, 0, 'a run that never started writes no completion');
  });
});

test('an unreachable venue exits 1 at the configured startup deadline, without claiming establishment or completing', async () => {
  await withDir(async (dir) => {
    const { path, config } = writeConfig(dir, { startupDeadlineMs: 1_000 });
    const startedAt = Date.now();
    const handle = startChild(path);
    try {
      const ready = await waitForStdout(handle, 'receiver: started');
      const runId = ready.split('receiver: started ')[1].trim().split(':')[0];
      const { code, signal } = await waitForExitOrTimeout(handle, 8_000, 'startup deadline did not end the run');
      const elapsed = Date.now() - startedAt;

      assert.equal(signal, null, 'the process ended itself, not by a signal');
      assert.equal(code, 1, `the startup deadline ends the run non-zero; stderr=${handle.stderr}`);
      assert.ok(elapsed >= 700 && elapsed < 8_000, `the configured one-second deadline applied (elapsed ${elapsed} ms)`);
      assert.doesNotMatch(handle.stdout, /receiver: established/, 'an unanswered subscription is not established');
      assert.match(handle.stderr, /startup deadline/i, 'the process leaves the failure reason');
      assert.notEqual(markerState(config.stores.organize, runId), 'complete', 'an unestablished run is not complete');
    } finally {
      if (handle.child.exitCode === null) handle.child.kill('SIGKILL');
    }
  });
});

test('a refused venue subscription exits 1 with its reason and no completion', async () => {
  await withDir(async (dir) => {
    const venue = await startVenueServer({ subscriptionStatus: 'error' });
    const { path, config } = writeConfig(dir, {
      url: `ws://127.0.0.1:${venue.port}/`,
      startupDeadlineMs: 5_000,
    });
    const handle = startChild(path);
    try {
      const startedAt = Date.now();
      const first = await Promise.race([
        waitForStdout(handle, 'receiver: started').then((ready) => ({ ready })),
        handle.exited.then((exited) => ({ exited })),
      ]);
      let runId = null;
      let outcome;
      if (first.ready !== undefined) {
        runId = first.ready.split('receiver: started ')[1].trim().split(':')[0];
        outcome = await waitForExitOrTimeout(handle, 8_000, 'subscription failure did not end the run');
      } else {
        outcome = first.exited;
      }
      const elapsed = Date.now() - startedAt;

      assert.equal(outcome.signal, null, 'the process handled the failure rather than dying by signal');
      assert.equal(outcome.code, 1, `a refused subscription is an abnormal end; stderr=${handle.stderr}`);
      assert.ok(elapsed < 5_000, `subscription refusal ends before the startup deadline (${elapsed} ms)`);
      assert.match(handle.stderr, /subscription failed/i, 'the reason reaches stderr');
      assert.match(handle.stderr, /denied/i, 'the venue refusal detail is preserved');
      if (runId !== null) assert.notEqual(markerState(config.stores.organize, runId), 'complete', 'a refused stream is not complete');
    } finally {
      if (handle.child.exitCode === null) handle.child.kill('SIGKILL');
      venue.close();
    }
  });
});

test('SIGTERM stops a live receiver once and its stores restart without claiming complete', async () => {
  await withDir(async (dir) => {
    await runWithVenue(dir, async ({ venue, handle, config, configPath }) => {
      const ready = await waitForStdout(handle, 'receiver: started');
      const connectionId = ready.split('receiver: started ')[1].trim();
      const runId = connectionId.split(':')[0];
      assert.ok(runId.length > 0, 'the run named its connection');

      await serve(venue, config, handle, 3);

      handle.child.kill('SIGTERM');
      const { code, signal } = await waitForExitOrTimeout(handle, 10_000, 'signal shutdown timed out');

      assert.equal(signal, null, 'the process ended on its own, not by the signal default action');
      assert.equal(code, 0, `a clean stop is exit code 0; stderr=${handle.stderr}`);
      assertStoppedMarker(config.stores.organize, runId);
      await assertRestart(configPath, config, venue, runId);
    });
  });
});

test('a second signal does not stop the run twice: still exit 0 without claiming complete', async () => {
  await withDir(async (dir) => {
    await runWithVenue(dir, async ({ venue, handle, config }) => {
      const ready = await waitForStdout(handle, 'receiver: started');
      const runId = ready.split('receiver: started ')[1].trim().split(':')[0];
      await serve(venue, config, handle, 3);

      handle.child.kill('SIGTERM');
      handle.child.kill('SIGTERM');
      const { code, signal } = await waitForExitOrTimeout(handle, 10_000, 'signal shutdown timed out');

      assert.equal(signal, null, 'the process ended on its own');
      assert.equal(code, 0, `a clean stop is exit code 0; stderr=${handle.stderr}`);
      assertStoppedMarker(config.stores.organize, runId);
    });
  });
});

test('SIGINT stops cleanly and restarts the same stores without claiming complete', async () => {
  await withDir(async (dir) => {
    await runWithVenue(dir, async ({ venue, handle, config, configPath }) => {
      const ready = await waitForStdout(handle, 'receiver: started');
      const runId = ready.split('receiver: started ')[1].trim().split(':')[0];
      await serve(venue, config, handle, 3);

      handle.child.kill('SIGINT');
      const { code, signal } = await waitForExitOrTimeout(handle, 10_000, 'signal shutdown timed out');

      assert.equal(signal, null, 'the process ended on its own');
      assert.equal(code, 0, `SIGINT stops cleanly too; stderr=${handle.stderr}`);
      assertStoppedMarker(config.stores.organize, runId);
      await assertRestart(configPath, config, venue, runId);
    });
  });
});

test('normal stop does not attempt complete writes even when the store forbids them', async () => {
  await withDir(async (dir) => {
    await runWithVenue(dir, async ({ venue, handle, config }) => {
      const ready = await waitForStdout(handle, 'receiver: started');
      const runId = ready.split('receiver: started ')[1].trim().split(':')[0];
      await serve(venue, config, handle, 3);

      // Complete is forbidden on both write forms. Ordinary shutdown must never reach either.
      const db = new DatabaseSync(config.stores.organize);
      // The organize child holds the file under WAL; the DDL has to wait its turn for the write lock,
      // which its own `busy_timeout = 0` deliberately does not. This second handle waits instead.
      db.exec('PRAGMA busy_timeout = 5000');
      db.exec(
        "CREATE TRIGGER fail_completion BEFORE UPDATE ON run_marker WHEN NEW.state = 'complete' " +
          "BEGIN SELECT RAISE(FAIL, 'completion IO failure'); END",
      );
      db.exec(
        "CREATE TRIGGER fail_completion_insert BEFORE INSERT ON run_marker WHEN NEW.state = 'complete' " +
          "BEGIN SELECT RAISE(FAIL, 'completion IO failure'); END",
      );
      db.close();

      handle.child.kill('SIGTERM');
      const { code, signal } = await waitForExitOrTimeout(handle, 10_000, 'signal shutdown timed out');

      assert.equal(signal, null, 'the process still ended on its own, not by the signal default action');
      assert.equal(code, 0, `ordinary shutdown never writes complete; stderr=${handle.stderr}`);
      const state = markerState(config.stores.organize, runId);
      assert.notEqual(state, 'complete', 'no completion is on disk');
      assert.notEqual(state, null, 'the run was marked live and left behind');
    });
  });
});


function assertStoppedMarker(path, runId) {
  const db = new DatabaseSync(path, { readOnly: true });
  try {
    const row = db.prepare('SELECT * FROM run_marker WHERE run_id = ?').get(runId);
    assert.equal(row.state, 'running', 'shutdown leaves restart invalidation to startup');
    assert.equal(row.finalize_request_id, null);
    assert.equal(row.finalize_request_payload, null);
    assert.equal(row.finalize_receipt_json, null);
  } finally {
    db.close();
  }
}


async function assertRestart(configPath, config, venue, oldRunId) {
  for (const path of Object.values(config.stores)) {
    // Sidecar files are reusable; actual SQLite ownership must be gone after confirmed child exit.
    for (const scope of ['supervisor', 'writer']) {
      const lock = acquireStoreLock({ path, scope });
      try {
        assert.equal(lock.acquired, true, `${scope} ownership was released for ${path}`);
      } finally {
        if (lock.acquired) lock.release();
      }
    }
    const db = new DatabaseSync(path);
    try {
      assert.equal(db.prepare('PRAGMA quick_check').get().quick_check, 'ok');
    } finally {
      db.close();
    }
  }
  const handle = startChild(configPath);
  try {
    const ready = await waitForStdout(handle, 'receiver: started');
    const newRunId = ready.split('receiver: started ')[1].trim().split(':')[0];
    assert.notEqual(newRunId, oldRunId);
    const db = new DatabaseSync(config.stores.organize, { readOnly: true });
    try {
      assert.equal(db.prepare('SELECT state FROM run_marker WHERE run_id = ?').get(oldRunId).state, 'invalidated');
    } finally {
      db.close();
    }
    await serve(venue, config, handle, 4);
    handle.child.kill('SIGTERM');
    const exited = await waitForExitOrTimeout(handle, 10_000, 'the restarted receiver did not stop');
    assert.deepEqual(exited, { code: 0, signal: null }, handle.stderr);
    assertStoppedMarker(config.stores.organize, newRunId);
  } finally {
    if (handle.child.exitCode === null) handle.child.kill('SIGKILL');
  }
}

test('a receive-tail IO failure does not turn a local stop into a completeness claim', async () => {
  await withDir(async (dir) => {
    await runWithVenue(dir, async ({ venue, handle, config }) => {
      const ready = await waitForStdout(handle, 'receiver: started');
      const runId = ready.split('receiver: started ')[1].trim().split(':')[0];
      await serve(venue, config, handle, 3);
      const db = new DatabaseSync(config.stores.ingest);
      try {
        db.exec('PRAGMA busy_timeout = 5000');
        db.exec("CREATE TRIGGER fail_tail BEFORE UPDATE ON received_tail BEGIN SELECT RAISE(FAIL, 'tail IO failure'); END");
      } finally {
        db.close();
      }
      venue.sendFrame(4);
      await until(() => bookApplied(config.stores.book) === 4, { label: 'the frame with a failed tail write to reach the book' });
      handle.child.kill('SIGTERM');
      const exited = await waitForExitOrTimeout(handle, 10_000, 'the failed receiver did not stop');
      assert.deepEqual(exited, { code: 0, signal: null }, handle.stderr);
      assertStoppedMarker(config.stores.organize, runId);
    });
  });
});

test('an established quiet stream can stop successfully before receiving its first data frame', async () => {
  await withDir(async (dir) => {
    await runWithVenue(dir, async ({ handle, config }) => {
      const ready = await waitForStdout(handle, 'receiver: started');
      const runId = ready.split('receiver: started ')[1].trim().split(':')[0];
      await waitForStdout(handle, 'receiver: established');
      handle.child.kill('SIGTERM');
      const exited = await waitForExitOrTimeout(handle, 10_000, 'the quiet receiver did not stop');
      assert.deepEqual(exited, { code: 0, signal: null }, handle.stderr);
      assertStoppedMarker(config.stores.organize, runId);
    });
  });
});
