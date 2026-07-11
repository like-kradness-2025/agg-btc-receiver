// test/burst-reducer/lock.test.mjs — Subprocess flock contention test (P1-2 Task 5)
import assert from 'node:assert/strict';
import { describe, it, before, after } from 'node:test';
import { spawnSync, spawn } from 'node:child_process';
import { mkdirSync, rmSync } from 'node:fs';
import { join } from 'node:path';

const TEST_ROOT = 'data/derived/burst_features_v1_test_lock';
const MARKET = 'test_lock';
const LOCK_DIR = join(TEST_ROOT, 'locks');
const LOCK_FILE = join(LOCK_DIR, `${MARKET}.lock`);

/**
 * Try to acquire a flock lock. Returns true if lock was acquired.
 * Uses a shell script because flock is a Linux syscall not exposed in Node.js fs.
 */
function tryLock() {
  const result = spawnSync('/bin/bash', ['-c',
    `exec 42>"${LOCK_FILE}" && flock -x -n 42 2>/dev/null && echo ACQUIRED || echo FAIL`,
  ], { timeout: 3000, stdio: ['ignore', 'pipe', 'pipe'] });
  const out = result.stdout.toString().trim();
  return out === 'ACQUIRED';
}

/**
 * Start a background process that holds the lock for `holdMs` milliseconds.
 * Returns an object with { pid } to track the process.
 */
function holdLock(holdMs = 2000) {
  const child = spawn('/bin/bash', ['-c',
    `exec 42>"${LOCK_FILE}" && flock -x 42 && echo ACQUIRED && sleep ${holdMs / 1000}`,
  ], { stdio: ['ignore', 'pipe', 'pipe'], timeout: holdMs + 3000 });
  return child;
}

describe('MarketLock (flock)', () => {
  before(() => {
    rmSync(TEST_ROOT, { recursive: true, force: true });
    mkdirSync(LOCK_DIR, { recursive: true });
  });

  after(() => {
    // Kill any lingering processes
    try { rmSync(TEST_ROOT, { recursive: true, force: true }); } catch (_) {}
  });

  it('lock acquisition succeeds when free', () => {
    const acquired = tryLock();
    assert.ok(acquired, 'should acquire lock when free');
  });

  it('second acquisition fails when lock is held', async () => {
    // Remove old lock file
    try { rmSync(LOCK_FILE, { force: true }); } catch (_) {}

    // Start process A that holds the lock
    const holder = holdLock(2000);
    // Wait for it to acquire
    await new Promise(r => setTimeout(r, 300));

    // Process B tries to acquire — should fail because A holds it
    const acquiredB = tryLock();
    assert.ok(!acquiredB, 'second lock should fail when lock is held');

    // Wait for holder to finish
    await new Promise(r => setTimeout(r, 2500));
  });

  it('lock can be re-acquired after release', async () => {
    try { rmSync(LOCK_FILE, { force: true }); } catch (_) {}

    // Hold then release
    const holder = holdLock(1000);
    await new Promise(r => setTimeout(r, 1500)); // wait for hold + release

    // Lock should now be free
    const acquired = tryLock();
    assert.ok(acquired, 'lock should be re-acquirable after release');
  });
});
