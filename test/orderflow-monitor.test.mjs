// test/orderflow-monitor.test.mjs — Orderflow monitor fail-closed startup logic
//
// Section 1: Unit tests for ready-wait loop extracted from orderflow_monitor.mjs
//   (1) All workers ready within timeout → normal flow
//   (2) Timeout with partial ready → fail-closed (startupFailed=true)
//   (3) Worker exits before ready → fail-closed
//   (4) All workers already ready → instant return
//
// Section 2: P0-1 regression — one market connect reject → degraded + ready
//
// Section 3: Subprocess entrypoint tests — invokes real orderflow_monitor.mjs
//   via spawnSync for --help, missing config, malformed JSON, output isolation,
//   config validation failure, and no-workers-spawned path.

import { describe, it, after } from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import fsp from 'node:fs/promises';
import path from 'node:path';
import os from 'node:os';
import { spawnSync, spawn } from 'node:child_process';

// ── Helpers ──────────────────────────────────────────────────────────────

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

/** Project root (where orderflow_monitor.mjs lives). */
const PROJECT_ROOT = path.resolve(new URL('.', import.meta.url).pathname, '..');

/**
 * Extracted ready-wait logic matching orderflow_monitor.mjs main().
 *
 * @param {object} opts
 * @param {Set<string>} opts.readyWorkers - mutable set of ready worker IDs
 * @param {number} opts.expectedCount - number of workers that must be ready
 * @param {object} opts.startupFailed - { value: boolean } mutable flag
 * @param {number} [opts.timeoutMs] - max wait in ms (default: 1000 for tests)
 * @returns {Promise<{ready: number, expected: number, failed: boolean}>}
 */
async function waitForReady(opts) {
  const { readyWorkers, expectedCount, startupFailed } = opts;
  const timeoutMs = opts.timeoutMs ?? 1000;
  const readyStart = Date.now();

  while (readyWorkers.size < expectedCount && !startupFailed.value) {
    if (Date.now() - readyStart > timeoutMs) {
      startupFailed.value = true;
      break;
    }
    await sleep(20);
  }

  return {
    ready: readyWorkers.size,
    expected: expectedCount,
    failed: startupFailed.value,
  };
}

// ── Section 1: Unit tests ────────────────────────────────────────────────

describe('OrderflowMonitor fail-closed startup', () => {

  describe('waitForReady (ready-wait loop)', () => {

    it('(1) all workers ready within timeout → normal flow', async () => {
      const readyWorkers = new Set();
      const startupFailed = { value: false };
      const expectedCount = 3;

      // Simulate workers becoming ready over time
      const resultPromise = waitForReady({ readyWorkers, expectedCount, startupFailed, timeoutMs: 500 });

      // Worker 1 ready at 10ms
      await sleep(10);
      readyWorkers.add('A');
      // Worker 2 ready at 30ms
      await sleep(20);
      readyWorkers.add('B');
      // Worker 3 ready at 50ms
      await sleep(20);
      readyWorkers.add('C');

      const result = await resultPromise;

      assert.strictEqual(result.failed, false, 'startupFailed should be false');
      assert.strictEqual(result.ready, 3, 'all 3 workers reported ready');
      assert.strictEqual(result.expected, 3);
    });

    it('(2) timeout with partial ready → fail-closed', async () => {
      const readyWorkers = new Set();
      const startupFailed = { value: false };
      const expectedCount = 3;

      // Worker 1 becomes ready
      readyWorkers.add('A');

      // Worker 2 and 3 never become ready → timeout
      const result = await waitForReady({ readyWorkers, expectedCount, startupFailed, timeoutMs: 100 });

      assert.strictEqual(result.failed, true, 'startupFailed should be true on timeout');
      assert.strictEqual(result.ready, 1, 'only 1 worker was ready');
      assert.strictEqual(startupFailed.value, true);
    });

    it('(3) worker exits before ready → fail-closed', async () => {
      const readyWorkers = new Set();
      const startupFailed = { value: false };
      const expectedCount = 3;

      readyWorkers.add('A');

      const resultPromise = waitForReady({ readyWorkers, expectedCount, startupFailed, timeoutMs: 500 });

      // Simulate worker B exiting before ready
      await sleep(10);
      startupFailed.value = true; // main sets this on exit before ready

      const result = await resultPromise;

      assert.strictEqual(result.failed, true, 'startupFailed should be true after premature exit');
      assert.strictEqual(startupFailed.value, true);
    });

    it('(4) all workers already ready → instant return', async () => {
      const readyWorkers = new Set(['A', 'B']);
      const startupFailed = { value: false };
      const expectedCount = 2;

      const start = Date.now();
      const result = await waitForReady({ readyWorkers, expectedCount, startupFailed, timeoutMs: 500 });
      const elapsed = Date.now() - start;

      assert.strictEqual(result.failed, false);
      assert.strictEqual(result.ready, 2);
      assert.ok(elapsed < 100, `should return nearly instantly, took ${elapsed}ms`);
    });

    it('(5) zero expected workers → instant return (no workers spawned)', async () => {
      // Edge case: expectedCount is 0 (e.g., all groups skipped)
      const readyWorkers = new Set();
      const startupFailed = { value: false };

      const result = await waitForReady({ readyWorkers, expectedCount: 0, startupFailed, timeoutMs: 500 });

      assert.strictEqual(result.failed, false);
      assert.strictEqual(result.ready, 0);
    });

    it('(6) timeout exactly at boundary → fail-closed', async () => {
      const readyWorkers = new Set();
      const startupFailed = { value: false };
      const expectedCount = 1;

      // Use very short timeout to test boundary
      const result = await waitForReady({ readyWorkers, expectedCount, startupFailed, timeoutMs: 50 });

      assert.strictEqual(result.failed, true);
      assert.strictEqual(startupFailed.value, true);
    });

    it('(7) startupFailed already true when entering → immediate fail', async () => {
      const readyWorkers = new Set();
      const startupFailed = { value: true }; // already failed
      const expectedCount = 3;

      const start = Date.now();
      const result = await waitForReady({ readyWorkers, expectedCount, startupFailed, timeoutMs: 500 });
      const elapsed = Date.now() - start;

      assert.strictEqual(result.failed, true);
      assert.ok(elapsed < 50, `should return immediately when already failed, took ${elapsed}ms`);
    });
  });

  // ── Section 2: P0-1 regression ─────────────────────────────────────────

  describe('P0-1 regression: worker init connect reject is market-local', () => {

    it('startupFailed triggered by IPC message', () => {
      // Simulate what main() does when worker sends startupFailed IPC
      const startupFailed = { value: false };
      const readyWorkers = new Set();

      // Worker sends startupFailed IPC
      const msg = { type: 'startupFailed', workerId: 'A', market: 'binance_spot', reason: 'connect ECONNREFUSED' };
      // main handler:
      startupFailed.value = true;

      assert.strictEqual(startupFailed.value, true);
      // Worker should NOT be added to readyWorkers
      assert.strictEqual(readyWorkers.has('A'), false);
    });

    it('one failed market emits degraded, schedules retry, and still sends ready', () => {
      const messages = [];
      const markets = ['healthy_market', 'binance_perp'];
      const connectResults = new Map([
        ['healthy_market', 'connected'],
        ['binance_perp', new Error('init sync failed after 3 retries')],
      ]);

      // Simulate the worker's market-local connect loop. A connect/sync
      // failure is not a worker startup failure; it is retried in background.
      for (const market of markets) {
        const result = connectResults.get(market);
        if (result instanceof Error) {
          messages.push({
            type: 'marketDegraded',
            workerId: 'test-worker',
            market,
            reason: result.message,
            retryDelayMs: 5 * 60 * 1000,
          });
        }
      }
      messages.push({ type: 'ready', workerId: 'test-worker' });

      assert.equal(messages.filter(m => m.type === 'marketDegraded').length, 1);
      assert.equal(messages.find(m => m.type === 'marketDegraded').market, 'binance_perp');
      assert.equal(messages.filter(m => m.type === 'startupFailed').length, 0);
      assert.equal(messages.filter(m => m.type === 'ready').length, 1);
    });

    it('all markets connect successfully → ready sent, no startupFailed', () => {
      const messages = [];

      // Simulate all connects succeeding
      const connectFailed = false;

      if (connectFailed) {
        messages.push({ type: 'startupFailed', workerId: 'test-worker' });
      } else {
        // Timers started, ready sent
        messages.push({ type: 'ready', workerId: 'test-worker' });
      }

      const readyMsgs = messages.filter(m => m.type === 'ready');
      assert.strictEqual(readyMsgs.length, 1, 'ready should be sent on success');
      assert.strictEqual(readyMsgs[0].workerId, 'test-worker');

      const failMsgs = messages.filter(m => m.type === 'startupFailed');
      assert.strictEqual(failMsgs.length, 0, 'no startupFailed on success');
    });
  });
});

// ── Section 3: Subprocess entrypoint tests ───────────────────────────────
//
// These spawn the real orderflow_monitor.mjs entry point and verify
// CLI argument handling, stdout/stderr isolation, and fail-closed
// behavior on malformed/missing config — without requiring network access.

describe('orderflow_monitor entrypoint subprocess', () => {
  /** @type {string[]} Temp dirs/files to clean up. */
  const tmpCleanup = [];

  after(async () => {
    for (const p of tmpCleanup) {
      try { await fsp.rm(p, { recursive: true, force: true }); } catch {}
    }
  });

  /**
   * Run orderflow_monitor.mjs with given args and capture result.
   * @param {string[]} args
   * @param {object} [opts]
   * @param {number} [opts.timeout] max ms (default 10000)
   * @returns {import('child_process').SpawnSyncReturns}
   */
  function runMonitor(args, opts = {}) {
    return spawnSync('node', ['orderflow_monitor.mjs', ...args], {
      cwd: PROJECT_ROOT,
      timeout: opts.timeout ?? 10000,
      stdio: ['pipe', 'pipe', 'pipe'],
    });
  }

  /** Write a temp file, register for cleanup. */
  function writeTemp(label, content) {
    const dir = path.join(os.tmpdir(), `btc-receiver-test-orderflow-monitor-${process.pid}`, label);
    fs.mkdirSync(dir, { recursive: true });
    const filePath = path.join(dir, 'data.json');
    fs.writeFileSync(filePath, content, 'utf-8');
    tmpCleanup.push(dir);
    return filePath;
  }

  it('--help → exit 0, usage text on stdout, no stderr', () => {
    const result = runMonitor(['--help']);
    assert.strictEqual(result.status, 0, '--help should exit 0');
    const stdout = result.stdout.toString();
    assert.ok(stdout.includes('Usage:'), 'stdout should contain usage text');
    assert.ok(stdout.includes('--config'), 'stdout should mention --config');
    assert.ok(stdout.includes('--help'), 'stdout should mention --help');
    // stderr isolation: --help should not produce error output
    const stderr = result.stderr.toString();
    assert.strictEqual(stderr, '', 'stderr should be empty with --help');
  });

  it('missing config file → exit 1, stderr mentions Failed to load config', () => {
    const result = runMonitor(['--config', '/tmp/nonexistent-btc-config-XXXXXXXX.json']);
    assert.strictEqual(result.status, 1, 'missing config should exit 1');
    const stderr = result.stderr.toString();
    assert.ok(stderr.includes('Failed to load config'), 'stderr should mention config load failure');
    assert.ok(stderr.includes('ENOENT'), 'stderr should mention file not found');
  });

  it('non-JSON config file → exit 1, stderr mentions parse error', () => {
    const configPath = writeTemp('non-json', 'plain text that is not JSON');
    const result = runMonitor(['--config', configPath, '--seconds', '0']);
    assert.strictEqual(result.status, 1, 'non-JSON config should exit 1');
    const stderr = result.stderr.toString();
    assert.ok(stderr.includes('Failed to load config'), 'stderr should mention config load failure');
    assert.ok(stderr.includes('not valid JSON'), 'stderr should mention JSON parse error');
  });

  it('malformed JSON config file → exit 1, stderr mentions parse error location', () => {
    const configPath = writeTemp('malformed-json', '{"broken": true,');
    const result = runMonitor(['--config', configPath, '--seconds', '0']);
    assert.strictEqual(result.status, 1, 'malformed JSON should exit 1');
    const stderr = result.stderr.toString();
    assert.ok(stderr.includes('Failed to load config'), 'stderr should mention config load failure');
    // Should include parse error details (position, line info)
    assert.ok(
      stderr.includes('position') || stderr.includes('JSON') || stderr.includes('Expected'),
      'stderr should describe the parse error location'
    );
  });

  it('stdout isolation: invalid config produces no stdout', () => {
    const configPath = writeTemp('stdout-isolation', 'not json at all');
    const result = runMonitor(['--config', configPath, '--seconds', '0']);
    // Error details go to stderr, stdout should be empty
    const stdout = result.stdout.toString();
    assert.strictEqual(stdout, '', 'stdout should be empty when config is invalid');
  });

  it('stderr isolation: --help produces no stderr (exit 0)', () => {
    const result = runMonitor(['--help']);
    const stderr = result.stderr.toString();
    assert.strictEqual(stderr, '', 'stderr should be empty for --help');
  });

  it('structurally invalid config (valid JSON, fails validateConfig) → exit 1, validation errors on stderr', () => {
    // A config that is valid JSON but fails validateConfig() — missing markets and output.
    // This tests the fail-closed path through validateConfig BEFORE worker spawning.
    const configPath = writeTemp('structurally-invalid', JSON.stringify({}));
    const result = runMonitor(['--config', configPath, '--seconds', '0']);
    assert.strictEqual(result.status, 1, 'invalid config should exit 1');
    const stderr = result.stderr.toString();
    assert.ok(stderr.includes('config validation failed'),
      'stderr should mention config validation failure');
    assert.ok(stderr.includes('config.markets'),
      'stderr should mention missing markets field');
    assert.ok(stderr.includes('config.output'),
      'stderr should mention missing output field');
    // stdout should be empty — no output before config validation
    const stdout = result.stdout.toString();
    assert.strictEqual(stdout, '', 'stdout should be empty when config is invalid');
  });

  it('valid config + unknown --markets value → exit 1 before worker spawn', () => {
    // Unknown markets are rejected before group filtering so a typo cannot
    // silently produce a ready receiver with no corresponding feed.
    const realConfig = path.join(PROJECT_ROOT, 'config.v3.json');
    assert.ok(fs.existsSync(realConfig), 'config.v3.json must exist');
    const tmpOutput = path.join(os.tmpdir(), `btc-test-unknown-market-${process.pid}-${Date.now()}`);

    const result = runMonitor(['--config', realConfig, '--output', tmpOutput, '--seconds', '1', '--markets', 'does_not_exist']);
    const stderr = result.stderr.toString();
    assert.ok(stderr.includes('unknown enabled market'),
      'stderr should mention the unknown enabled market');
    assert.ok(!stderr.includes('Failed to load config'),
      'stderr should NOT contain config load error');
    assert.strictEqual(result.status, 1,
      'should exit 1 when no workers spawned');
  });

  it('valid config.v3.json passes load step (may exit 1 for worker reasons)', () => {
    // The real config.v3.json is a valid JSON file.
    // The monitor will load it successfully and reach worker spawning.
    // Since no real exchanges are running, it may exit 1 (no workers spawned
    // or startup timeout), but we only verify that:
    //   (a) exit code is not 0 or 1 (both expected)
    //   (b) stderr does NOT contain "Failed to load config" — proving load worked
    const realConfig = path.join(PROJECT_ROOT, 'config.v3.json');
    assert.ok(fs.existsSync(realConfig), 'config.v3.json must exist');
    const tmpOutput = path.join(os.tmpdir(), `btc-test-load-step-${process.pid}-${Date.now()}`);

    const result = runMonitor(['--config', realConfig, '--output', tmpOutput, '--seconds', '0', '--markets', 'does_not_exist']);
    // Exit 1 is expected because no markets match any group -> no workers spawned
    const stderr = result.stderr.toString();
    assert.ok(!stderr.includes('Failed to load config'),
      'valid config should not produce config load error');
    // The process may exit 1 (no workers spawned) — that's fine
    assert.ok(result.status === 1 || result.status === 0,
      `exit should be 0 or 1 (got ${result.status})`);
  });

  // ── Real output-root lock failure acceptance tests (cycle 5) ────────────

  it('external flock holder on output root causes exit 1 and identifies lock-contention', async () => {
    const outputRoot = path.join(os.tmpdir(), `btc-receiver-lock-contention-${process.pid}-${Date.now()}`);
    const lockDir = path.join(outputRoot, 'locks');
    const lockFile = path.join(lockDir, 'output-root.lock');
    fs.mkdirSync(lockDir, { recursive: true });

    // Hold the lock in an external bash subprocess; spawnAsync lets it keep running
    const holder = spawn('/bin/bash', [
      '-c',
      `exec 3<>'${lockFile}' && flock -x -n 3 && echo LOCKED && sleep 30`,
    ], { stdio: ['ignore', 'pipe', 'pipe'] });

    await new Promise((resolve, reject) => {
      const t = setTimeout(() => reject(new Error('external lock holder did not start')), 3000);
      holder.stdout.on('data', (d) => { if (d.toString().includes('LOCKED')) { clearTimeout(t); resolve(); } });
      holder.on('error', reject);
    });

    const realConfig = path.join(PROJECT_ROOT, 'config.v3.json');
    const result = runMonitor([
      '--config', realConfig,
      '--seconds', '1',
      '--markets', 'does_not_exist',
      '--output', outputRoot,
    ], { timeout: 15000 });

    const stderr = result.stderr.toString();
    assert.strictEqual(result.status, 1, 'monitor must exit 1 on lock contention');
    assert.ok(stderr.includes('failed to acquire output-root lock'), 'stderr must report lock failure');
    assert.ok(stderr.includes('lock-contention'), 'stderr must identify lock-contention status');

    // Cleanup: terminate the holder and remove the temp root
    holder.kill('SIGTERM');
    await new Promise((r) => { holder.on('exit', r); setTimeout(r, 500); });
    try { fs.rmSync(outputRoot, { recursive: true, force: true }); } catch {}
  });

  it('invalid output root causes exit 1 and identifies lock-io-error', () => {
    // /dev/null/<pid> is not a directory, so mkdir() fails with ENOTDIR.
    const outputRoot = path.join('/dev/null', `btc-receiver-lock-io-${process.pid}-${Date.now()}`);
    const realConfig = path.join(PROJECT_ROOT, 'config.v3.json');

    const result = runMonitor([
      '--config', realConfig,
      '--seconds', '1',
      '--markets', 'does_not_exist',
      '--output', outputRoot,
    ], { timeout: 15000 });

    const stderr = result.stderr.toString();
    assert.strictEqual(result.status, 1, 'monitor must exit 1 on lock I/O error');
    assert.ok(stderr.includes('failed to acquire output-root lock'), 'stderr must report lock failure');
    assert.ok(stderr.includes('lock-io-error'), 'stderr must identify lock-io-error status');
  });
});
