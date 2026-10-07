import { test } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';

import { createForkSpawner } from '../src/supervisor/process-spawner.mjs';

const UNRESPONSIVE_ROLE = fileURLToPath(new URL('../test-support/unresponsive-role.mjs', import.meta.url));
const NO_READY_ROLE = fileURLToPath(new URL('../test-support/no-ready-role.mjs', import.meta.url));

test('a close reports termination only after the child actually exits', async () => {
  const dir = mkdtempSync(join(tmpdir(), 'spawner-exit-'));
  let child = null;
  try {
    const spawner = createForkSpawner({ roleModule: UNRESPONSIVE_ROLE });
    child = await spawner('book', {
      instance: 'book-test',
      options: {
        market: 'test_market',
        stream: 'trades',
        runId: 'run-test',
        roleInstance: 'book-test',
        storePath: join(dir, 'book.sqlite'),
      },
    });

    const closed = await child.close();
    assert.equal(closed?.terminated, true, 'close returns only after actual exit is observed');
    assert.equal(closed.clean, false, 'forced termination cannot claim that stores closed normally');
    assert.equal(closed.exitInfo.signal, 'SIGKILL', 'the test child ignored close and SIGTERM, so SIGKILL ended it');
    assert.deepEqual(child.exitInfo, closed.exitInfo, 'the returned proof matches the observed exit event');
  } finally {
    if (child?.exitInfo == null && child?.pid) {
      try {
        process.kill(child.pid, 'SIGKILL');
      } catch {
        /* already exited */
      }
    }
    rmSync(dir, { recursive: true, force: true });
  }
});

test('a role that never announces ready is timed out and confirmed gone before spawn fails', async () => {
  const dir = mkdtempSync(join(tmpdir(), 'spawner-ready-timeout-'));
  try {
    const spawner = createForkSpawner({ roleModule: NO_READY_ROLE });
    const startedAt = Date.now();
    await assert.rejects(
      spawner('book', {
        startupTimeoutMs: 100,
        instance: 'book-no-ready',
        options: {
          market: 'test_market',
          stream: 'trades',
          runId: 'run-test',
          roleInstance: 'book-no-ready',
          storePath: join(dir, 'book.sqlite'),
        },
      }),
      (error) => {
        assert.match(error.message, /did not become ready/);
        assert.equal(error.terminatedConfirmed, true, 'the failed spawn returns only after its child exited');
        return true;
      },
    );
    assert.ok(Date.now() - startedAt < 10_000, 'a missing ready message cannot hang startup indefinitely');
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

