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

// ── lock-helper.sh integration tests ──

const TEST_ROOT2 = 'data/derived/burst_features_v1_test_lock_helper';
const LOCK_DIR2 = join(TEST_ROOT2, 'locks');

/**
 * Run a bash snippet that sources lock-helper.sh and calls a command.
 * Returns { exitCode, stdout, stderr }.
 */
function runLockHelper(code, env = {}) {
  const result = spawnSync('/bin/bash', ['-c',
    `source scripts/lock-helper.sh\n${code}`,
  ], { timeout: 5000, stdio: ['ignore', 'pipe', 'pipe'], env: { ...process.env, ...env } });
  return {
    exitCode: result.status,
    stdout: result.stdout.toString().trim(),
    stderr: result.stderr.toString().trim(),
  };
}

describe('MarketLock (lock-helper.sh integration)', () => {
  before(() => {
    rmSync(TEST_ROOT2, { recursive: true, force: true });
    mkdirSync(LOCK_DIR2, { recursive: true });
  });

  after(() => {
    try { rmSync(TEST_ROOT2, { recursive: true, force: true }); } catch (_) {}
  });

  it('acquire_market_lock succeeds when free', () => {
    const r = runLockHelper(
      `acquire_market_lock "test_int" "${TEST_ROOT2}"`,
    );
    assert.equal(r.exitCode, 0, `should exit 0, got ${r.exitCode} stderr: ${r.stderr}`);
    // Should have structured INFO log on stderr
    const info = JSON.parse(r.stderr.split('\n').pop());
    assert.equal(info.level, 'INFO');
    assert.equal(info.msg, 'lock-acquired');
    assert.equal(info.market, 'test_int');
  });

  it('acquire_market_lock returns 1 when lock is held', () => {
    // Single bash process: background job acquires and holds lock, foreground job fails
    const r = spawnSync('/bin/bash', ['-c',
      `source scripts/lock-helper.sh
# Background job: acquire lock and hold for 5s
( acquire_market_lock "test_contend" "${TEST_ROOT2}" && sleep 5 ) &
HOLDER_PID=$!
sleep 0.5  # wait for background job to acquire

# Now try to acquire in foreground — should fail with structured SKIP
acquire_market_lock "test_contend" "${TEST_ROOT2}"
RC=$?

# Cleanup: kill holder
kill $HOLDER_PID 2>/dev/null
wait $HOLDER_PID 2>/dev/null
exit $RC`,
    ], { timeout: 8000, stdio: ['ignore', 'pipe', 'pipe'] });

    // Should return 1 (contention)
    assert.equal(r.status, 1, `should exit 1 on contention, got ${r.status} stdout="${r.stdout.toString().trim()}" stderr="${r.stderr.toString().trim()}"`);

    // Verify structured skip JSON on stderr
    const stderrText = r.stderr.toString().trim();
    const stderrLines = stderrText.split('\n').filter(l => l.trim());
    assert.ok(stderrLines.length >= 1, `expected stderr output, got: "${stderrText}"`);
    const lastLine = stderrLines[stderrLines.length - 1];
    let skip;
    try { skip = JSON.parse(lastLine); } catch (e) {
      assert.fail(`last stderr line is not valid JSON: "${lastLine}"`);
    }
    assert.equal(skip.level, 'SKIP', `expected level=SKIP, got ${JSON.stringify(skip)}`);
    assert.equal(skip.reason, 'lock-contention');
    assert.equal(skip.market, 'test_contend');
    assert.ok(skip.lock_file, 'should include lock_file path');
  });

  it('release_market_lock allows re-acquisition', () => {
    const r1 = runLockHelper(
      `acquire_market_lock "test_release" "${TEST_ROOT2}"\nrelease_market_lock`,
    );
    assert.equal(r1.exitCode, 0, `acquire+release should succeed, got ${r1.exitCode}`);

    // Immediately re-acquire — should succeed
    const r2 = runLockHelper(
      `acquire_market_lock "test_release" "${TEST_ROOT2}"`,
    );
    assert.equal(r2.exitCode, 0, `re-acquire after release should succeed, got ${r2.exitCode}`);
  });

  it('different markets do not interfere', () => {
    // Acquire lock for market A
    const r1 = runLockHelper(
      `acquire_market_lock "test_market_a" "${TEST_ROOT2}"`,
    );
    assert.equal(r1.exitCode, 0);

    // Hold market A in background while trying market B
    const holder = spawn('/bin/bash', ['-c',
      `source scripts/lock-helper.sh\nacquire_market_lock "test_market_a" "${TEST_ROOT2}" || exit 1\nsleep 2`,
    ], { stdio: ['ignore', 'pipe', 'pipe'], timeout: 4000 });

    // Market B should still succeed
    const r2 = spawnSync('/bin/bash', ['-c',
      `source scripts/lock-helper.sh\nacquire_market_lock "test_market_b" "${TEST_ROOT2}" || exit 1\necho ACQUIRED_B`,
    ], { timeout: 3000, stdio: ['ignore', 'pipe', 'pipe'] });
    assert.equal(r2.status, 0, 'different market lock should succeed');
    assert.ok(r2.stdout.toString().includes('ACQUIRED_B'), 'should acquire market B');
  });

  it('acquire_market_lock || exit 0 pattern exits 0 on contention', () => {
    // Hold lock in background
    const holder = spawn('/bin/bash', ['-c',
      `source scripts/lock-helper.sh\nacquire_market_lock "test_pattern" "${TEST_ROOT2}" || exit 1\nsleep 3`,
    ], { stdio: ['ignore', 'pipe', 'pipe'], timeout: 5000 });

    // Second process uses the canonical "|| exit 0" pattern
    const r = spawnSync('/bin/bash', ['-c',
      `source scripts/lock-helper.sh\nacquire_market_lock "test_pattern" "${TEST_ROOT2}" || exit 0\necho SHOULD_NOT_REACH`,
    ], { timeout: 3000, stdio: ['ignore', 'pipe', 'pipe'] });
    assert.equal(r.status, 0, '|| exit 0 pattern should exit 0 on contention');
    assert.ok(!r.stdout.toString().includes('SHOULD_NOT_REACH'), 'should not reach past || exit 0');
  });
});
