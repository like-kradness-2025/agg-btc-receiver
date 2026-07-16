// lib/lock.mjs — output-root lock for Receiver multi-instance exclusion
//
// Cross-process mutual exclusion for an output root directory.
// Uses flock(1) via a subprocess — the kernel releases the advisory lock
// automatically when the holder process dies, so there are no stale locks
// to recover and no TOCTOU races in stale recovery.
//
// Lifecycle tie to the parent process (including SIGKILL):
//   A pipe connects this process (write end) to the holder subprocess
//   (read end / stdin).  The subprocess blocks reading from stdin after
//   acquiring the flock.  When the parent dies (even SIGKILL) the kernel
//   closes the pipe write end; the subprocess gets EOF on its read and
//   exits, releasing the flock.  No detached subprocess can outlive the
//   Receiver parent.
//
// Lock lifecycle:
//   1. acquireOutputRootLock(outputRoot):
//      - ensure <outputRoot>/locks/ exists
//      - spawn a bash subprocess that opens + flock()es the lock file
//        and then blocks on stdin (pipe held by parent)
//      - on success (flock acquired) -> { ok: true, status: 'acquired' }
//      - on contention (flock -n fails) -> { ok: false, status: 'lock-contention' }
//      - on I/O / spawn errors -> { ok: false, status: 'lock-io-error' }
//   2. releaseOutputRootLock(outputRoot):
//      - SIGTERM the holder subprocess (kernel releases flock on death)
//      - fail-closed: no-op if path is not held by this process
//   3. process exit:
//      - SIGKILL any still-held subprocesses (belt-and-suspenders)

import { spawn } from 'node:child_process';
import fsp from 'node:fs/promises';
import path from 'node:path';

const LOCK_DIR = 'locks';
const LOCK_FILE = 'output-root.lock';
const LOCK_ACQUIRE_TIMEOUT_MS = 10_000;
const RELEASE_KILL_TIMEOUT_MS = 3_000;

// ---------------------------------------------------------------------------
// Per-path held-lock tracker
// ---------------------------------------------------------------------------

/** @type {Map<string, { child: import('child_process').ChildProcess, lockFile: string }>} */
const _heldLocks = new Map();

// Release all held locks on process exit (belt-and-suspenders — the kernel
// also releases flock on subprocess death).
process.on('exit', () => {
  for (const [, entry] of _heldLocks) {
    try { process.kill(-entry.child.pid, 'SIGKILL'); } catch (_) {}
    try { entry.child.kill('SIGKILL'); } catch (_) {}
  }
  _heldLocks.clear();
});

// ---------------------------------------------------------------------------
// acquireOutputRootLock
// ---------------------------------------------------------------------------

/**
 * Acquire an exclusive non-blocking output-root lock via flock.
 *
 * Spawns a bash subprocess connected via pipe that opens the lock file and
 * holds an exclusive flock.  Contention is reported without blocking.
 *
 * @param {string} outputRoot - output root directory
 * @returns {Promise<{ ok: boolean, status: string, holder?: string }>}
 *   { ok: true, status: 'acquired' }                — lock acquired
 *   { ok: false, status: 'lock-contention' }         — another process holds it
 *   { ok: false, status: 'lock-io-error' }           — mkdir / spawn / timeout
 */
export async function acquireOutputRootLock(outputRoot) {
  // Already held by this process
  if (_heldLocks.has(outputRoot)) {
    return { ok: false, status: 'lock-contention', holder: 'self' };
  }

  // Ensure lock directory exists
  const lockDir = path.join(outputRoot, LOCK_DIR);
  try {
    await fsp.mkdir(lockDir, { recursive: true });
  } catch (mkdirErr) {
    return { ok: false, status: 'lock-io-error', holder: `mkdir: ${mkdirErr.message}` };
  }

  const lockFile = path.join(lockDir, LOCK_FILE);

  return new Promise((resolve) => {
    const escaped = lockFile.replace(/'/g, "'\\''");
    const script = [
      `exec 3<>'${escaped}' || { exec 3>&-; exit 2; }`,
      `flock -x -n 3 2>/dev/null || { exec 3>&-; exit 1; }`,
      `echo '{"status":"ACQUIRED"}'`,
      // Hold lock by blocking on stdin — when parent dies the kernel
      // closes the pipe write end, read gets EOF, and the child exits.
      `while read -r _; do :; done`,
    ].join('\n');

    const child = spawn('/bin/bash', ['-c', script], {
      stdio: ['pipe', 'pipe', 'pipe'],
      // Not detached: child stays in parent's process group; the pipe
      // from parent stdin closes on parent death (even SIGKILL), which
      // the blocking read detects as EOF → child exits → flock released.
    });

    let resolved = false;

    const timer = setTimeout(() => {
      if (resolved) return;
      resolved = true;
      const pid = child.pid;
      try { process.kill(-pid, 'SIGTERM'); } catch (_) {}
      try { child.kill('SIGTERM'); } catch (_) {}
      setTimeout(() => {
        try { process.kill(-pid, 'SIGKILL'); } catch (_) {}
        try { child.kill('SIGKILL'); } catch (_) {}
      }, RELEASE_KILL_TIMEOUT_MS);
      resolve({ ok: false, status: 'lock-io-error', holder: 'timeout' });
    }, LOCK_ACQUIRE_TIMEOUT_MS);

    child.stdout.on('data', (data) => {
      const line = data.toString().trim();
      try {
        const msg = JSON.parse(line);
        if (msg.status === 'ACQUIRED' && !resolved) {
          resolved = true;
          clearTimeout(timer);
          _heldLocks.set(outputRoot, { child, lockFile });
          resolve({ ok: true, status: 'acquired' });
        }
      } catch (_) {
        // Not complete JSON yet — wait for more data
      }
    });

    child.on('exit', (code) => {
      if (resolved) return;
      resolved = true;
      clearTimeout(timer);
      // exit code 1 from the script = flock contention or file-open failure
      if (code === 1) {
        resolve({ ok: false, status: 'lock-contention', holder: 'external' });
      } else {
        resolve({ ok: false, status: 'lock-io-error', holder: `exit-code-${code}` });
      }
    });

    child.on('error', (err) => {
      if (resolved) return;
      resolved = true;
      clearTimeout(timer);
      _heldLocks.delete(outputRoot);
      resolve({ ok: false, status: 'lock-io-error', holder: `spawn: ${err.message}` });
    });
  });
}

// ---------------------------------------------------------------------------
// releaseOutputRootLock
// ---------------------------------------------------------------------------

/**
 * Release a previously acquired output-root lock by killing the holder
 * subprocess.  The kernel releases the advisory flock on process death.
 *
 * Safe to call on a path with no held lock (no-op).
 *
 * @param {string} outputRoot - output root directory
 * @returns {Promise<{ ok: boolean, status: string }>}
 */
export async function releaseOutputRootLock(outputRoot) {
  const entry = _heldLocks.get(outputRoot);
  if (!entry || !entry.child || entry.child.killed) {
    return { ok: true, status: 'noop-not-held' };
  }

  const pid = entry.child.pid;

  // Kill the child process (bash) — SIGTERM first, SIGKILL fallback
  try { process.kill(-pid, 'SIGTERM'); } catch (_) {}
  try { entry.child.kill('SIGTERM'); } catch (_) {}

  return new Promise((resolve) => {
    const timer = setTimeout(() => {
      try { process.kill(-pid, 'SIGKILL'); } catch (_) {}
      try { entry.child.kill('SIGKILL'); } catch (_) {}
      resolve();
    }, RELEASE_KILL_TIMEOUT_MS);

    entry.child.on('exit', () => { clearTimeout(timer); resolve(); });
    entry.child.on('error', () => { clearTimeout(timer); resolve(); });
  }).then(() => {
    _heldLocks.delete(outputRoot);
    return { ok: true, status: 'released' };
  });
}
