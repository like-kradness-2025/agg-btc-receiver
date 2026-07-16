// test/receiver-lock.test.mjs — output-root lock tests (flock-based)
//
// Tests the kernel-backed flock lock implementation from lib/lock.mjs.
// Cross-process mutual exclusion using flock(1) via detached subprocess.
// No stale-recovery TOCTOU race: kernel releases flock on process death.

import { describe, it, before, after } from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import os from 'node:os';
import { spawn } from 'node:child_process';

import {
  acquireOutputRootLock,
  releaseOutputRootLock,
} from '../lib/lock.mjs';

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function tmpDir(label) {
  const dir = path.join(os.tmpdir(), 'receiver-lock', `${label}-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`);
  fs.mkdirSync(dir, { recursive: true });
  return dir;
}

async function rmDir(dir) {
  try { await fs.promises.rm(dir, { recursive: true, force: true }); } catch (_) {}
}

// ---------------------------------------------------------------------------
// Suite
// ---------------------------------------------------------------------------

describe('flock output-root lock', { concurrency: false }, () => {
  let dir;

  before(() => { dir = tmpDir('main'); });
  after(async () => { await rmDir(dir); });

  // ── Acquire & release ─────────────────────────────────────────────────

  it('acquires lock when free', async () => {
    const r = await acquireOutputRootLock(dir);
    assert.equal(r.ok, true);
    assert.equal(r.status, 'acquired');
    await releaseOutputRootLock(dir);
  });

  it('allows re-acquire after release', async () => {
    const a = await acquireOutputRootLock(dir);
    assert.equal(a.ok, true);
    await releaseOutputRootLock(dir);

    const b = await acquireOutputRootLock(dir);
    assert.equal(b.ok, true);
    await releaseOutputRootLock(dir);
  });

  it('detects same-process contention', async () => {
    const a = await acquireOutputRootLock(dir);
    assert.equal(a.ok, true);

    const b = await acquireOutputRootLock(dir);
    assert.equal(b.ok, false);
    assert.equal(b.status, 'lock-contention');
    assert.equal(b.holder, 'self');

    await releaseOutputRootLock(dir);
  });

  // ── Cross-process contention ──────────────────────────────────────────

  it('detects cross-process contention (external flock holder)', async () => {
    const lockDir = path.join(dir, 'locks');
    const lockFile = path.join(lockDir, 'output-root.lock');
    fs.mkdirSync(lockDir, { recursive: true });

    // Spawn a bash subprocess that grabs and holds the flock
    const holder = spawn('/bin/bash', [
      '-c',
      `exec 3<>'${lockFile}' && flock -x -n 3 && echo LOCKED && sleep 30`,
    ], { stdio: ['ignore', 'pipe', 'pipe'] });

    // Wait for holder to acquire
    await new Promise((resolve, reject) => {
      const t = setTimeout(() => reject(new Error('timeout waiting for holder lock')), 5000);
      holder.stdout.on('data', (d) => {
        if (d.toString().includes('LOCKED')) { clearTimeout(t); resolve(); }
      });
      holder.on('error', reject);
    });

    // Now try to acquire from Node — must fail
    const r = await acquireOutputRootLock(dir);
    assert.equal(r.ok, false, 'must NOT acquire lock when another process holds it');
    assert.equal(r.status, 'lock-contention');

    // Cleanup
    holder.kill('SIGTERM');
    await new Promise(r => { holder.on('exit', () => r()); setTimeout(r, 2000); });
  });

  // ── Release semantics ─────────────────────────────────────────────────

  it('release is no-op for unheld path', async () => {
    const r = await releaseOutputRootLock(path.join(dir, 'nonexistent'));
    assert.equal(r.ok, true);
    assert.equal(r.status, 'noop-not-held');
  });

  it('release is no-op after already released', async () => {
    const a = await acquireOutputRootLock(dir);
    assert.equal(a.ok, true);

    const r1 = await releaseOutputRootLock(dir);
    assert.equal(r1.ok, true);
    assert.equal(r1.status, 'released');

    const r2 = await releaseOutputRootLock(dir);
    assert.equal(r2.ok, true);
    assert.equal(r2.status, 'noop-not-held');
  });

  // ── Isolation ─────────────────────────────────────────────────────────

  it('distinct paths are independent', async () => {
    const dir2 = path.join(dir, 'other');
    fs.mkdirSync(dir2, { recursive: true });

    const a = await acquireOutputRootLock(dir);
    const b = await acquireOutputRootLock(dir2);
    assert.equal(a.ok, true);
    assert.equal(b.ok, true, 'second path should also acquire');

    await releaseOutputRootLock(dir2);
    await releaseOutputRootLock(dir);
  });

  // ── Stale-proof (kernel auto-release) ─────────────────────────────────

  it('automatically releases lock when holder dies (no stale lock)', async () => {
    const lockDir = path.join(dir, 'locks');
    const lockFile = path.join(lockDir, 'output-root.lock');
    fs.mkdirSync(lockDir, { recursive: true });

    // Spawn process that acquires lock then exits
    const holder = spawn('/bin/bash', [
      '-c',
      `exec 3<>'${lockFile}' && flock -x -n 3 && echo LOCKED`,
    ], { stdio: ['ignore', 'pipe', 'pipe'] });

    // Wait for it to acquire (will exit immediately after, releasing flock)
    await new Promise((resolve, reject) => {
      const t = setTimeout(() => reject(new Error('timeout')), 5000);
      holder.stdout.on('data', (d) => {
        if (d.toString().includes('LOCKED')) { clearTimeout(t); resolve(); }
      });
      holder.on('error', reject);
    });

    // Wait for the subprocess to fully exit
    await new Promise(r => { holder.on('exit', () => r()); setTimeout(r, 2000); });

    // The lock should now be free — we can acquire it
    const r = await acquireOutputRootLock(dir);
    assert.equal(r.ok, true, 'should acquire after holder exits');
    assert.equal(r.status, 'acquired');
    await releaseOutputRootLock(dir);
  });

  // ── Error handling ────────────────────────────────────────────────────

  it('returns lock-io-error on non-writable path', async () => {
    // /dev/null exists as a char device, not a directory — mkdir fails
    const r = await acquireOutputRootLock('/dev/null/locks');
    assert.equal(r.ok, false);
    assert.equal(r.status, 'lock-io-error');
  });

  it('returns lock-io-error when lock file is a directory (open failure)', async () => {
    // Use a distinct output root to avoid collision with other tests' lock file
    const testDir = tmpDir('lock-dir');
    try {
      // Make the lock file path a directory so exec 3<> fails
      const lockDir = path.join(testDir, 'locks');
      const lockFile = path.join(lockDir, 'output-root.lock');
      fs.mkdirSync(lockDir, { recursive: true });
      fs.mkdirSync(lockFile);

      const r = await acquireOutputRootLock(testDir);
      assert.equal(r.ok, false);
      assert.equal(r.status, 'lock-io-error');
    } finally {
      await rmDir(testDir);
    }
  });

  // ── Parent-SIGKILL regression (lifecycle tie) ──────────────────────────

  it('releases lock when holder parent is SIGKILL\'d (no stale lock)', async () => {
    // Use a subprocess that acquires the lock via the real acquire function,
    // then send it SIGKILL and verify the lock can be reacquired (pipe-based
    // lifecycle detection).
    const testDir = tmpDir('sigkill-parent');
    const helperPath = path.join(testDir, 'lock-helper.mjs');
    const lockModuleAbs = path.resolve('lib', 'lock.mjs');

    try {
      const helperCode = [
        `import { acquireOutputRootLock } from ${JSON.stringify(lockModuleAbs)};`,
        `const dir = ${JSON.stringify(testDir)};`,
        `const lock = await acquireOutputRootLock(dir);`,
        `if (!lock.ok) { console.error('FAIL:' + (lock.holder || lock.status)); process.exit(1); }`,
        `process.stdout.write('LOCKED\\n');`,
        `// Hold lock indefinitely — parent holds pipe write end`,
        `await new Promise(() => {});`,
      ].join('\n');
      fs.writeFileSync(helperPath, helperCode);

      const child = spawn(process.execPath, [helperPath], {
        stdio: ['ignore', 'pipe', 'pipe'],
      });

      // Wait for child to acquire the lock
      await new Promise((resolve, reject) => {
        const t = setTimeout(() => reject(new Error('timeout waiting for lock')), 5000);
        child.stdout.on('data', (d) => {
          if (d.toString().includes('LOCKED')) { clearTimeout(t); resolve(); }
        });
        child.on('error', reject);
      });

      // SIGKILL the child (simulates parent crash — no exit handler runs)
      child.kill('SIGKILL');

      // Wait for child to die
      await new Promise(r => { child.on('exit', () => r()); setTimeout(r, 2000); });

      // The lock should have been released because the child's death closed
      // the pipe, which caused the bash holder to get EOF on its stdin read.
      const r = await acquireOutputRootLock(testDir);
      assert.equal(r.ok, true, 'should reacquire lock after parent SIGKILL');
      assert.equal(r.status, 'acquired');
      await releaseOutputRootLock(testDir);
    } finally {
      await rmDir(testDir);
    }
  });
});
