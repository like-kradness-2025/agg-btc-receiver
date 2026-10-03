/**
 * Set 7: the real process entrance, verified as a real process.
 *
 * The unit tests drive `createSupervisor` directly, with spies for the exit code. What they cannot
 * show is that a *process* gives the answers §5.7 asks for: a config it cannot run ends with code 1 and
 * writes no completion, and a signal stops it once, cleanly, with code 0 and a completion written. These
 * tests start `bin/receiver.mjs` as a child process, drive it with signals, and read the run marker back
 * out of the store it was told to use - so the exit code and the run record are observed where an
 * operator would observe them.
 *
 * The venue URL is unreachable on purpose: the point is the process's own lifecycle, not a venue. The
 * entry holds the process open while the run is live, so a socket between reconnects cannot make it exit
 * 0 silently; that is what lets a signal be delivered and observed here.
 */

import { test } from 'node:test';
import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { mkdtempSync, rmSync, writeFileSync, existsSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { DatabaseSync } from 'node:sqlite';

import { openDurability } from '../src/durability.mjs';

const PACKAGE_DIR = fileURLToPath(new URL('..', import.meta.url));
const ENTRY = fileURLToPath(new URL('../bin/receiver.mjs', import.meta.url));
const UNREACHABLE = 'ws://127.0.0.1:1/';

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
    market: 'kraken_spot',
    stream: 'trades',
    symbol: 'XBT/USD',
    database: join(dir, 'state.sqlite'),
    spoolDir: join(dir, 'spool'),
    url: UNREACHABLE,
    ...overrides,
  };
  const path = join(dir, 'receiver.config.json');
  writeFileSync(path, JSON.stringify(config, null, 2));
  return { path, config };
}

function startChild(configPath) {
  const child = spawn(process.execPath, [ENTRY, '--config', configPath], {
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

/** Resolve once the child has printed its readiness line; reject if it exits first or never does. */
function waitForReady(handle, marker = 'receiver: started', timeoutMs = 15_000) {
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
      reject(new Error(`the child exited before readiness (code=${code}, signal=${signal}); stdout=${handle.stdout} stderr=${handle.stderr}`));
    });
    check();
  });
}

function completionOf(dbPath) {
  const reader = openDurability({ path: dbPath, runId: 'test-reader' });
  try {
    return reader.lastCompleteRun();
  } finally {
    reader.close();
  }
}

test('a config whose store cannot be opened ends the process with code 1 and leaves no completion', async () => {
  await withDir(async (dir) => {
    // The database's parent directory does not exist: SQLite cannot open it, so the run cannot begin.
    const missing = join(dir, 'missing-parent');
    const { path, config } = writeConfig(dir, { database: join(missing, 'state.sqlite') });
    const handle = startChild(path);
    const { code } = await handle.exited;

    assert.equal(code, 1, `the process ended non-zero; stderr=${handle.stderr}`);
    assert.equal(existsSync(config.database), false, 'the store was never created, so no completion can exist');
  });
});

test('a config that cannot be read ends the process with code 1', async () => {
  await withDir(async (dir) => {
    const handle = startChild(join(dir, 'does-not-exist.json'));
    const { code } = await handle.exited;
    assert.equal(code, 1, 'a missing config file is a failed run');
    assert.match(handle.stderr, /config file could not be read/);
  });
});

test('a config that asks for a raw file is refused, and no raw file is written', async () => {
  await withDir(async (dir) => {
    // There is no raw writer in the entrance any more. A `"raw"` key is not silently ignored - it is
    // refused before anything is opened, so a deployment cannot believe a raw file was saved.
    const rawPath = join(dir, 'raw.log');
    const { path } = writeConfig(dir, { raw: rawPath });
    const handle = startChild(path);
    const { code } = await handle.exited;

    assert.equal(code, 1, `a config with a raw destination is a failed run; stderr=${handle.stderr}`);
    assert.match(handle.stderr, /"raw" is not supported/);
    assert.equal(existsSync(rawPath), false, 'nothing was written to the raw path');
  });
});

test('a startup the structure refuses ends the process with code 1 and writes no completion', async () => {
  await withDir(async (dir) => {
    // The spool path is a regular file, so the spool cannot be created. The store *is* opened first and
    // left behind by the failed construction, so the completion can be checked on a real store.
    const spoolAsFile = join(dir, 'spool-is-a-file');
    writeFileSync(spoolAsFile, 'not a directory');
    const { path, config } = writeConfig(dir, { spoolDir: spoolAsFile });

    const handle = startChild(path);
    const { code } = await handle.exited;

    assert.equal(code, 1, `the process ended non-zero; stderr=${handle.stderr}`);
    assert.equal(existsSync(config.database), true, 'the store was opened');
    assert.equal(completionOf(config.database), null, 'a run that never started writes no completion');
  });
});

test('SIGTERM stops a live receiver once: exit 0 with the completion written', async () => {
  await withDir(async (dir) => {
    const { path, config } = writeConfig(dir);
    const handle = startChild(path);
    const ready = await waitForReady(handle);
    // The readiness line carries the run's connection id, whose first field is the run id.
    const connectionId = ready.split('receiver: started ')[1].trim();
    const runId = connectionId.split(':')[0];
    assert.ok(runId.length > 0, 'the run named its connection');

    handle.child.kill('SIGTERM');
    const { code, signal } = await handle.exited;

    assert.equal(signal, null, 'the process ended on its own, not by the signal default action');
    assert.equal(code, 0, 'a clean stop is exit code 0');
    const completion = completionOf(config.database);
    assert.notEqual(completion, null, 'a clean stop writes the completion');
    assert.equal(completion.runId, runId, 'and the completion names the run that just stopped');
  });
});

test('a second signal does not stop the run twice: still exit 0 with one completion', async () => {
  await withDir(async (dir) => {
    const { path, config } = writeConfig(dir);
    const handle = startChild(path);
    const ready = await waitForReady(handle);
    const runId = ready.split('receiver: started ')[1].trim().split(':')[0];

    // Two signals back to back. The first owns the shutdown; the second finds it already under way. The
    // process must leave with 0 and exactly the one completion the first signal's clean stop writes.
    handle.child.kill('SIGTERM');
    handle.child.kill('SIGTERM');
    const { code, signal } = await handle.exited;

    assert.equal(signal, null, 'the process ended on its own');
    assert.equal(code, 0, 'a clean stop is exit code 0');
    const completion = completionOf(config.database);
    assert.notEqual(completion, null, 'the completion was written');
    assert.equal(completion.runId, runId, 'and it names the run, so it was written once for this run');
  });
});

test('SIGINT is the same orderly shutdown as SIGTERM', async () => {
  await withDir(async (dir) => {
    const { path, config } = writeConfig(dir);
    const handle = startChild(path);
    await waitForReady(handle);

    handle.child.kill('SIGINT');
    const { code, signal } = await handle.exited;

    assert.equal(signal, null, 'the process ended on its own');
    assert.equal(code, 0, 'SIGINT stops cleanly too');
    assert.notEqual(completionOf(config.database), null, 'and writes the completion');
  });
});

test('a completion the store refuses turns the stop into exit 1 with no completion', async () => {
  await withDir(async (dir) => {
    const { path, config } = writeConfig(dir);
    const handle = startChild(path);
    const ready = await waitForReady(handle);
    const runId = ready.split('receiver: started ')[1].trim().split(':')[0];

    // A store that refuses the completion: the marker insert for `state = 'complete'` is rejected, the
    // same way a full disk or a failing write would refuse it. The signal still drives the clean stop,
    // so the process reaches close()'s completion write - and that write is exactly what must not be
    // reported as success. Before the fix the exception was swallowed and the process left with 0 while
    // the run record still said `running`.
    const db = new DatabaseSync(config.database);
    db.exec(
      "CREATE TRIGGER fail_completion BEFORE INSERT ON run_marker WHEN NEW.state = 'complete' " +
        "BEGIN SELECT RAISE(FAIL, 'completion IO failure'); END",
    );
    db.close();

    handle.child.kill('SIGTERM');
    const { code, signal } = await handle.exited;

    assert.equal(signal, null, 'the process still ended on its own, not by the signal default action');
    assert.equal(code, 1, `a run whose completion could not be written is not a clean end; stderr=${handle.stderr}`);
    assert.equal(completionOf(config.database), null, 'and no completion is on disk');

    // The run record is not complete either: the next start must still be able to tell this from a crash.
    const reader = new DatabaseSync(config.database);
    const marker = reader.prepare('SELECT state FROM run_marker WHERE run_id = ?').get(runId);
    reader.close();
    assert.notEqual(marker, undefined, 'the run was marked live and left behind');
    assert.notEqual(marker.state, 'complete', 'the run is not marked complete');
  });
});
