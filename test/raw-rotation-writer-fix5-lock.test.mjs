// test/raw-rotation-writer-fix5-lock.test.mjs — FIX5: output-root multi-instance lock unit tests
// + FIX9a: strict PID validation + fail-closed release
//
// acquireOutputRootLock / releaseOutputRootLock のユニットテスト。
// mkdir atomic 性、PID 一致判定、stale 検出、I/O エラー、release を網羅する。

import { describe, it, before, after, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import fsp from 'node:fs/promises';
import path from 'node:path';
import os from 'node:os';

import {
  acquireOutputRootLock,
  releaseOutputRootLock,
  _setTestLockPid,
  _setTestLockFsError,
  _validatePidStrict,
} from '../lib/raw-rotation-writer.mjs';

// ── Helpers ────────────────────────────────────────────────────────────────

function tmpDir(label) {
  const dir = path.join(os.tmpdir(), 'rrw-lock-test', `${label}-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`);
  fs.mkdirSync(dir, { recursive: true });
  return dir;
}

async function rmDir(dir) {
  try { await fsp.rm(dir, { recursive: true, force: true }); } catch {}
}

/** ロックディレクトリのパスを計算する (acquireOutputRootLock と同じロジック) */
function lockDir(outputRoot) {
  return path.join(outputRoot, 'locks', 'receiver.lock');
}

/** ロックディレクトリが存在するか */
function lockExists(outputRoot) {
  return fs.existsSync(lockDir(outputRoot));
}

/** PID ファイルの内容を読む */
function readLockPid(outputRoot) {
  const pidFile = path.join(lockDir(outputRoot), 'pid');
  if (!fs.existsSync(pidFile)) return null;
  return parseInt(fs.readFileSync(pidFile, 'utf-8').trim(), 10);
}

/** 外部プロセスに似せて手動でロックディレクトリを作る */
function manuallyLock(outputRoot, pid) {
  const ld = lockDir(outputRoot);
  fs.mkdirSync(ld, { recursive: true });
  fs.writeFileSync(path.join(ld, 'pid'), String(pid), 'utf-8');
}

/** 手動ロックを削除 */
function manuallyUnlock(outputRoot) {
  fs.rmSync(lockDir(outputRoot), { recursive: true, force: true });
}

// ── Tests: _validatePidStrict ──────────────────────────────────────────────

describe('FIX9a: _validatePidStrict', () => {
  it('accepts valid decimal positive integers', () => {
    assert.equal(_validatePidStrict('1'), 1);
    assert.equal(_validatePidStrict('999999'), 999999);
    assert.equal(_validatePidStrict('4194304'), 4194304);
  });

  it('rejects empty string', () => {
    assert.equal(_validatePidStrict(''), null);
  });

  it('rejects whitespace-only strings', () => {
    assert.equal(_validatePidStrict('   '), null);
    assert.equal(_validatePidStrict('\t'), null);
    assert.equal(_validatePidStrict('\n'), null);
  });

  it('accepts leading/trailing whitespace (trimmed)', () => {
    assert.equal(_validatePidStrict(' 123'), 123);
    assert.equal(_validatePidStrict('123 '), 123);
    assert.equal(_validatePidStrict(' 123 '), 123);
  });

  it('rejects trailing alpha/non-digit garbage', () => {
    assert.equal(_validatePidStrict('123abc'), null);
    assert.equal(_validatePidStrict('999x'), null);
    assert.equal(_validatePidStrict('0dead'), null);
  });

  it('rejects sign prefix', () => {
    assert.equal(_validatePidStrict('+1'), null);
    assert.equal(_validatePidStrict('-1'), null);
    assert.equal(_validatePidStrict('--1'), null);
  });

  it('rejects decimal point', () => {
    assert.equal(_validatePidStrict('1.5'), null);
    assert.equal(_validatePidStrict('3.0'), null);
    assert.equal(_validatePidStrict('.1'), null);
  });

  it('rejects zero', () => {
    assert.equal(_validatePidStrict('0'), null);
  });

  it('rejects non-numeric strings', () => {
    assert.equal(_validatePidStrict('abc'), null);
    assert.equal(_validatePidStrict('pid123'), null);
    assert.equal(_validatePidStrict('NaN'), null);
    assert.equal(_validatePidStrict('Infinity'), null);
  });

  it('rejects overflow beyond MAX_SAFE_INTEGER', () => {
    assert.equal(_validatePidStrict('99999999999999999999'), null);
  });

  it('rejects non-string types', () => {
    assert.equal(_validatePidStrict(null), null);
    assert.equal(_validatePidStrict(undefined), null);
    assert.equal(_validatePidStrict(123), null);
    assert.equal(_validatePidStrict({}), null);
  });
});

// ── Tests: acquireOutputRootLock ───────────────────────────────────────────

describe('FIX5: acquireOutputRootLock', () => {
  /** @type {string} */
  let testRoot;

  before(() => {
    _setTestLockPid(null);
    _setTestLockFsError(null);
  });

  afterEach(() => {
    _setTestLockPid(null);
    _setTestLockFsError(null);
    try { manuallyUnlock(testRoot); } catch {}
  });

  after(() => {
    _setTestLockPid(null);
    _setTestLockFsError(null);
  });

  // ── 1. 空きロックの獲得 ──

  it('G1-1: acquires lock when free', () => {
    testRoot = tmpDir('free');
    const result = acquireOutputRootLock(testRoot);
    assert.ok(result.ok, `should succeed, got ${JSON.stringify(result)}`);
    assert.equal(result.acquired, true, 'acquired should be true on first acquisition');
    assert.ok(lockExists(testRoot), 'lock directory should exist');
    assert.equal(readLockPid(testRoot), process.pid, 'pid file should contain our PID');
  });

  // ── 2. 同一プロセス内 idempotent ──

  it('G1-2: same process idempotent — second call returns acquired=false', () => {
    testRoot = tmpDir('idempotent');
    const r1 = acquireOutputRootLock(testRoot);
    assert.ok(r1.ok);
    assert.equal(r1.acquired, true);

    const r2 = acquireOutputRootLock(testRoot);
    assert.ok(r2.ok, 'second call should also succeed');
    assert.equal(r2.acquired, false, 'acquired should be false on repeated call');
    assert.ok(lockExists(testRoot), 'lock should still exist');
  });

  // ── 3. 別プロセス競合 ──

  it('G1-3: returns lock-contention when another process holds the lock', () => {
    testRoot = tmpDir('contention');
    manuallyLock(testRoot, process.pid);
    _setTestLockPid(999999); // 自分は別のPIDとして振る舞う

    const result = acquireOutputRootLock(testRoot);
    assert.ok(!result.ok, 'should fail on contention');
    assert.equal(result.reason, 'lock-contention');
    assert.equal(result.holderPid, process.pid);
  });

  // ── 4. Stale ロックの自動回収 ──

  it('G1-4: stale lock (dead PID) is cleaned up and acquired', () => {
    testRoot = tmpDir('stale');
    manuallyLock(testRoot, 999999);

    const result = acquireOutputRootLock(testRoot);
    // stale 検出 → 片付け → 再獲得
    assert.ok(result.ok, 'stale lock should be cleaned up and acquired');
    assert.equal(result.acquired, true);
    assert.equal(readLockPid(testRoot), process.pid);
  });

  // ── 5. 複数出力ルートは干渉しない ──

  it('G1-5: different output roots do not interfere', () => {
    const rootA = tmpDir('rootA');
    const rootB = tmpDir('rootB');
    try {
      const rA = acquireOutputRootLock(rootA);
      assert.ok(rA.ok);
      assert.equal(rA.acquired, true);

      const rB = acquireOutputRootLock(rootB);
      assert.ok(rB.ok, 'separate output root should succeed');
      assert.equal(rB.acquired, true);
    } finally {
      releaseOutputRootLock(rootA);
      releaseOutputRootLock(rootB);
      rmDir(rootA);
      rmDir(rootB);
    }
  });
});

// ── Tests: releaseOutputRootLock ───────────────────────────────────────────

describe('FIX5: releaseOutputRootLock', () => {
  /** @type {string} */
  let testRoot;

  afterEach(() => {
    _setTestLockPid(null);
    _setTestLockFsError(null);
    try { manuallyUnlock(testRoot); } catch {}
  });

  after(() => {
    _setTestLockPid(null);
    _setTestLockFsError(null);
  });

  // ── 1. 取得→解放→再取得 ──

  it('G2-1: acquire → release → re-acquire succeeds', () => {
    testRoot = tmpDir('release-reacq');
    const r1 = acquireOutputRootLock(testRoot);
    assert.ok(r1.ok);
    assert.equal(r1.acquired, true);
    assert.ok(lockExists(testRoot));

    releaseOutputRootLock(testRoot);
    assert.ok(!lockExists(testRoot), 'lock should be removed after release');

    const r2 = acquireOutputRootLock(testRoot);
    assert.ok(r2.ok, 're-acquire after release should succeed');
    assert.equal(r2.acquired, true);
  });

  // ── 2. 他プロセスのロックを解放しない ──

  it('G2-2: does NOT release lock owned by another process', () => {
    testRoot = tmpDir('foreign-lock');
    manuallyLock(testRoot, 999999);
    assert.ok(lockExists(testRoot));

    releaseOutputRootLock(testRoot);
    // 他プロセスのロックなので削除されない
    assert.ok(lockExists(testRoot), 'foreign lock should NOT be released');
    assert.equal(readLockPid(testRoot), 999999, 'pid should be unchanged');
  });

  // ── 3. 解放していないロックに対して安全 ──

  it('G2-3: release on non-existent lock is safe (no-op)', () => {
    testRoot = tmpDir('noop-release');
    // lock を取得してない状態で release → 何も起こらない
    releaseOutputRootLock(testRoot);
    assert.ok(!lockExists(testRoot));
  });
});

// ── Tests: I/O error injection ─────────────────────────────────────────────

describe('FIX5: I/O error injection', () => {
  /** @type {string} */
  let testRoot;

  afterEach(() => {
    _setTestLockPid(null);
    _setTestLockFsError(null);
    try { manuallyUnlock(testRoot); } catch {}
  });

  after(() => {
    _setTestLockPid(null);
    _setTestLockFsError(null);
  });

  // ── 1. mkdir 失敗 → lock-io-error ──

  it('G3-1: mkdir I/O error returns lock-io-error', () => {
    testRoot = tmpDir('mkdir-io-error');
    _setTestLockFsError({ mkdirSync: 'EACCES' });
    const result = acquireOutputRootLock(testRoot);
    assert.ok(!result.ok);
    assert.equal(result.reason, 'lock-io-error');
    assert.ok(result.error.includes('[EACCES]'), `error should mention EACCES, got: ${result.error}`);
  });

  // ── 2. PID 書き込み失敗 → lock-io-error ──

  it('G3-2: writeFileSync I/O error after mkdir returns lock-io-error', () => {
    testRoot = tmpDir('write-io-error');
    _setTestLockFsError({ writeFileSync: 'ENOSPC' });
    const result = acquireOutputRootLock(testRoot);
    assert.ok(!result.ok);
    assert.equal(result.reason, 'lock-io-error');
    assert.ok(result.error.includes('[ENOSPC]'), `error should mention ENOSPC, got: ${result.error}`);
  });

  // ── 3. EEXIST + PID 読取エラー → fail-closed ──

  it('G3-3: EEXIST with empty PID file → lock-contention (NOT stale cleanup)', () => {
    testRoot = tmpDir('empty-pid');
    const ld = lockDir(testRoot);
    fs.mkdirSync(ld, { recursive: true });
    fs.writeFileSync(path.join(ld, 'pid'), '', 'utf-8');

    const result = acquireOutputRootLock(testRoot);
    assert.ok(!result.ok, 'empty PID must NOT be treated as stale');
    assert.equal(result.reason, 'lock-contention');
  });

  // ── 4. EEXIST + stale cleanup rmSync 失敗 → lock-io-error (tightened) ──

  it('G3-4: EEXIST with stale cleanup rmSync failure → lock-io-error (tightened)', () => {
    testRoot = tmpDir('rm-fail');
    manuallyLock(testRoot, 999999);
    _setTestLockFsError({ rmSync: 'EACCES', kill: 'ESRCH' });
    const result = acquireOutputRootLock(testRoot);
    assert.ok(!result.ok);
    // FIX9a: tightened — stale cleanup kill=ESRCH + rmSync=EACCES
    // triggers fail-closed lock-io-error (not lock-contention)
    assert.equal(result.reason, 'lock-io-error');
    assert.ok(lockExists(testRoot), 'lock must survive failed stale cleanup');
  });

  // ── 5. kill ESRCH を注入 → stale 検出 → 自動回収 ──

  it('G3-5: kill ESRCH triggers stale cleanup and re-acquire', () => {
    testRoot = tmpDir('kill-esrch');
    manuallyLock(testRoot, 999999);
    _setTestLockFsError({ kill: 'ESRCH' });

    const result = acquireOutputRootLock(testRoot);
    assert.ok(result.ok, 'stale lock should be auto-recovered');
    assert.equal(result.acquired, true);
    assert.equal(readLockPid(testRoot), process.pid);
  });

  // ── FIX8: fail-closed I/O error + TOCTOU-safe stale detection ──

  it('G3-6: existsSync I/O error during EEXIST returns lock-io-error (not stale cleanup)', () => {
    testRoot = tmpDir('exists-io-error');
    manuallyLock(testRoot, 999999);
    _setTestLockFsError({ existsSync: 'EACCES' });
    const result = acquireOutputRootLock(testRoot);
    assert.ok(!result.ok);
    assert.equal(result.reason, 'lock-io-error');
    assert.ok(lockExists(testRoot), 'lock must survive read I/O error');
  });

  it('G3-7: readFileSync I/O error during EEXIST returns lock-io-error (not stale cleanup)', () => {
    testRoot = tmpDir('read-io-error');
    manuallyLock(testRoot, 999999);
    _setTestLockFsError({ readFileSync: 'EACCES' });
    const result = acquireOutputRootLock(testRoot);
    assert.ok(!result.ok);
    assert.equal(result.reason, 'lock-io-error');
    assert.ok(lockExists(testRoot), 'lock must survive read I/O error');
  });

  it('G3-8: lock dir without PID file → claim-based stale detection → acquired', () => {
    testRoot = tmpDir('no-pid-claim');
    const ld = lockDir(testRoot);
    fs.mkdirSync(ld, { recursive: true });

    const result = acquireOutputRootLock(testRoot);
    assert.ok(result.ok, 'stale lock without PID should be cleanable via claim');
    assert.equal(result.acquired, true);
    assert.equal(readLockPid(testRoot), process.pid);
  });

  it('G3-9: lock dir without PID with competing claim dir → lock-contention', () => {
    testRoot = tmpDir('claim-contention');
    const ld = lockDir(testRoot);
    fs.mkdirSync(ld, { recursive: true });
    const myClaim = path.join(ld, `.claim-${process.pid}`);
    fs.mkdirSync(myClaim);

    const result = acquireOutputRootLock(testRoot);
    assert.ok(!result.ok);
    assert.equal(result.reason, 'lock-contention');
  });
});

// ── FIX9a: releaseOutputRootLock fail-closed ──────────────────────────────

describe('FIX9a: release fail-closed (ownership proof required)', () => {
  /** @type {string} */
  let testRoot;

  afterEach(() => {
    _setTestLockPid(null);
    _setTestLockFsError(null);
    try { manuallyUnlock(testRoot); } catch {}
  });

  after(() => {
    _setTestLockPid(null);
    _setTestLockFsError(null);
  });

  it('G4-1: release when pidFile absent → lockDir survives (no-op)', () => {
    testRoot = tmpDir('no-pidfile-release');
    // Lock dir exists but pid file is missing (simulates crash before PID write)
    const ld = lockDir(testRoot);
    fs.mkdirSync(ld, { recursive: true });
    // No pid file written
    assert.ok(lockExists(testRoot));

    releaseOutputRootLock(testRoot);
    // FIX9a: must NOT delete lockDir without proof of ownership
    assert.ok(lockExists(testRoot), 'lock dir must survive release when pidFile absent');
  });

  it('G4-2: release with unreadable pidFile → lockDir survives (no-op)', () => {
    testRoot = tmpDir('unreadable-release');
    manuallyLock(testRoot, process.pid);
    assert.ok(lockExists(testRoot));
    _setTestLockFsError({ readFileSync: 'EACCES' });

    releaseOutputRootLock(testRoot);
    // I/O error on read → fail-closed, must not delete
    assert.ok(lockExists(testRoot), 'lock dir must survive release when pidFile unreadable');
  });

  it('G4-3: release with malformed PID "123abc" → lockDir survives (no-op)', () => {
    testRoot = tmpDir('malformed-release');
    const ld = lockDir(testRoot);
    fs.mkdirSync(ld, { recursive: true });
    fs.writeFileSync(path.join(ld, 'pid'), '123abc', 'utf-8');

    releaseOutputRootLock(testRoot);
    assert.ok(lockExists(testRoot), 'lock dir must survive release with malformed PID');
    assert.equal(readLockPid(testRoot), 123, 'raw read still shows parsed 123');
  });

  it('G4-4: release with whitespace-only pidFile → lockDir survives (no-op)', () => {
    testRoot = tmpDir('blank-pid-release');
    const ld = lockDir(testRoot);
    fs.mkdirSync(ld, { recursive: true });
    fs.writeFileSync(path.join(ld, 'pid'), '   ', 'utf-8');

    releaseOutputRootLock(testRoot);
    assert.ok(lockExists(testRoot), 'lock dir must survive release with whitespace-only PID');
  });

  it('G4-5: release with empty pidFile → lockDir survives (no-op)', () => {
    testRoot = tmpDir('empty-pid-release');
    const ld = lockDir(testRoot);
    fs.mkdirSync(ld, { recursive: true });
    fs.writeFileSync(path.join(ld, 'pid'), '', 'utf-8');

    releaseOutputRootLock(testRoot);
    assert.ok(lockExists(testRoot), 'lock dir must survive release with empty PID');
  });

  it('G4-6: release with negative PID → lockDir survives (no-op)', () => {
    testRoot = tmpDir('negative-pid-release');
    const ld = lockDir(testRoot);
    fs.mkdirSync(ld, { recursive: true });
    fs.writeFileSync(path.join(ld, 'pid'), '-1', 'utf-8');

    releaseOutputRootLock(testRoot);
    assert.ok(lockExists(testRoot), 'lock dir must survive release with negative PID');
  });

  it('G4-7: release with valid own PID succeeds', () => {
    testRoot = tmpDir('own-pid-release');
    const r1 = acquireOutputRootLock(testRoot);
    assert.ok(r1.ok);

    releaseOutputRootLock(testRoot);
    assert.ok(!lockExists(testRoot), 'own lock must be released on valid PID match');
  });
});

// ── FIX9a: acquireOutputRootLock strict PID validation ────────────────────

describe('FIX9a: acquire strict PID validation (malformed → contention)', () => {
  /** @type {string} */
  let testRoot;

  afterEach(() => {
    _setTestLockPid(null);
    _setTestLockFsError(null);
    try { manuallyUnlock(testRoot); } catch {}
  });

  after(() => {
    _setTestLockPid(null);
    _setTestLockFsError(null);
  });

  it('G5-1: pidFile with "123abc" → lock-contention (NOT stale)', () => {
    testRoot = tmpDir('alpha-garbage');
    const ld = lockDir(testRoot);
    fs.mkdirSync(ld, { recursive: true });
    // Write "123abc" — must NOT be parsed as PID 123; must NOT trigger stale cleanup
    fs.writeFileSync(path.join(ld, 'pid'), '123abc', 'utf-8');

    const result = acquireOutputRootLock(testRoot);
    assert.ok(!result.ok);
    assert.equal(result.reason, 'lock-contention');
    // Lock must survive — no stale cleanup on malformed PID
    assert.ok(lockExists(testRoot), 'lock must survive malformed PID (no stale cleanup)');
    assert.equal(readLockPid(testRoot), 123, 'raw file content still readable as 123');
  });

  it('G5-2: pidFile with "+5" → lock-contention (sign prefix)', () => {
    testRoot = tmpDir('sign-pid');
    const ld = lockDir(testRoot);
    fs.mkdirSync(ld, { recursive: true });
    fs.writeFileSync(path.join(ld, 'pid'), '+5', 'utf-8');

    const result = acquireOutputRootLock(testRoot);
    assert.ok(!result.ok);
    assert.equal(result.reason, 'lock-contention');
  });

  it('G5-3: pidFile with whitespace-only → lock-contention', () => {
    testRoot = tmpDir('whitespace-pid');
    const ld = lockDir(testRoot);
    fs.mkdirSync(ld, { recursive: true });
    fs.writeFileSync(path.join(ld, 'pid'), '   ', 'utf-8');

    const result = acquireOutputRootLock(testRoot);
    assert.ok(!result.ok);
    assert.equal(result.reason, 'lock-contention');
  });

  it('G5-4: pidFile with "0" → lock-contention (zero PID)', () => {
    testRoot = tmpDir('zero-pid');
    const ld = lockDir(testRoot);
    fs.mkdirSync(ld, { recursive: true });
    fs.writeFileSync(path.join(ld, 'pid'), '0', 'utf-8');

    const result = acquireOutputRootLock(testRoot);
    assert.ok(!result.ok);
    assert.equal(result.reason, 'lock-contention');
  });

  it('G5-5: pidFile with "3.14" → lock-contention (float)', () => {
    testRoot = tmpDir('float-pid');
    const ld = lockDir(testRoot);
    fs.mkdirSync(ld, { recursive: true });
    fs.writeFileSync(path.join(ld, 'pid'), '3.14', 'utf-8');

    const result = acquireOutputRootLock(testRoot);
    assert.ok(!result.ok);
    assert.equal(result.reason, 'lock-contention');
  });
});

// ── FIX9a: distinct-PID safety (Codex reconciliation) ──────────────────────

describe('FIX9a: distinct PID safety (claim/recovery/release cannot delete foreign lock)', () => {
  /** @type {string} */
  let testRoot;

  afterEach(() => {
    _setTestLockPid(null);
    _setTestLockFsError(null);
    try { manuallyUnlock(testRoot); } catch {}
  });

  after(() => {
    _setTestLockPid(null);
    _setTestLockFsError(null);
  });

  it('G6-1: release with distinct simulated PID never deletes foreign lock', () => {
    testRoot = tmpDir('distinct-release');
    // Lock owned by PID 77777
    manuallyLock(testRoot, 77777);
    // Pretend we are a different PID
    _setTestLockPid(99999);

    releaseOutputRootLock(testRoot);
    // Foreign lock must survive
    assert.ok(lockExists(testRoot), 'foreign lock must survive release from different PID');
    assert.equal(readLockPid(testRoot), 77777, 'pid file content must be unchanged');
  });

  it('G6-2: acquire with distinct PID never yields false ownership', () => {
    testRoot = tmpDir('distinct-acquire');
    // Lock owned by PID 77777 (which does not exist — stale eligible)
    manuallyLock(testRoot, 77777);
    // Pretend we are yet another different PID, not the holder
    _setTestLockPid(88888);

    // Since 77777 is dead, stale recovery kicks in — but with our own PID (88888)
    const result = acquireOutputRootLock(testRoot);
    assert.ok(result.ok, 'stale recovery should succeed');
    assert.equal(result.acquired, true);
    // The lock should now contain the simulated PID (88888), NOT the holder
    assert.equal(readLockPid(testRoot), 88888, 'lock should be recovered with our simulated PID');
  });

  it('G6-3: stale recovery with live foreign PID never deletes active lock', () => {
    testRoot = tmpDir('live-foreign');
    // Lock owned by a real process (our own process.pid)
    manuallyLock(testRoot, process.pid);
    // Pretend we are someone else who cannot stale-recover
    _setTestLockPid(12345);

    const result = acquireOutputRootLock(testRoot);
    // process.pid is alive → lock-contention, NOT stale cleanup
    assert.ok(!result.ok);
    assert.equal(result.reason, 'lock-contention');
    assert.equal(result.holderPid, process.pid);
    // Lock must survive
    assert.ok(lockExists(testRoot), 'active foreign lock must survive contention check');
    assert.equal(readLockPid(testRoot), process.pid, 'pid file must be unchanged');
  });

  it('G6-4: missing-pid-file claim race with distinct PID never deletes competing claim', () => {
    testRoot = tmpDir('claim-race-distinct');
    const ld = lockDir(testRoot);
    fs.mkdirSync(ld, { recursive: true });
    _setTestLockPid(55555);
    // Pre-create a competing claim directory (as if another contender got there first)
    const competingClaim = path.join(ld, '.claim-55555');
    fs.mkdirSync(competingClaim);

    const result = acquireOutputRootLock(testRoot);
    // Our claim mkdir fails with EEXIST → lock-contention
    assert.ok(!result.ok);
    assert.equal(result.reason, 'lock-contention');
  });

  it('G6-5: G3-9 assertion strengthened — assert equal not includes', () => {
    testRoot = tmpDir('claim-strict');
    const ld = lockDir(testRoot);
    fs.mkdirSync(ld, { recursive: true });
    const myClaim = path.join(ld, `.claim-${process.pid}`);
    fs.mkdirSync(myClaim);

    const result = acquireOutputRootLock(testRoot);
    assert.ok(!result.ok);
    // Already strict: assert.equal
    assert.equal(result.reason, 'lock-contention');
  });
});
