// lib/raw-rotation-writer.mjs — 30-second window file rotation for raw data writes
//
// Provides:
//   - Timestamp normalization across second/ms/μs/ns units
//   - 30-second window calculation (UTC)
//   - No-clobber file rename + quarantine primitives
//   - RawRotationWriter: per-market/kind serialized writer with
//     current + previous window tolerance and startup recovery.
//
// Uses BufferedWriter from ./buffered-writer.mjs for actual file I/O.
// The rotation writer manages its own flush lifecycle — BufferedWriterPool
// auto-flush is disabled.

import fsp from 'node:fs/promises';
import path from 'node:path';
import fs from 'node:fs';
import { BufferedWriter } from './buffered-writer.mjs';

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const WINDOW_MS = 30000;
const MAX_QUARANTINE_ATTEMPTS = 100;

// ---------------------------------------------------------------------------
// 1. Timestamp normalization
// ---------------------------------------------------------------------------

/**
 * Normalize a numeric timestamp to integer milliseconds.
 *
 * Unit detection (by absolute value):
 *   < 1e11  → seconds      → multiply by 1000
 *   < 1e14  → milliseconds → as-is
 *   < 1e17  → microseconds → divide by 1000
 *   < 1e20  → nanoseconds  → divide by 1_000_000
 *   else    → invalid      → return null
 *
 * Rejects: non-number, NaN, ±Infinity, numeric strings.
 * Always floors to integer ms before returning.
 *
 * @param {*} ts - input timestamp
 * @returns {number|null} integer milliseconds, or null if invalid
 */
export function normalizeTimestampMs(ts) {
  if (typeof ts !== 'number') return null;
  if (!Number.isFinite(ts)) return null;

  const abs = Math.abs(ts);
  let ms;

  if (abs < 1e11) {
    // Seconds
    ms = ts * 1000;
  } else if (abs < 1e14) {
    // Milliseconds
    ms = ts;
  } else if (abs < 1e17) {
    // Microseconds
    ms = ts / 1000;
  } else if (abs < 1e20) {
    // Nanoseconds
    ms = ts / 1_000_000;
  } else {
    return null;
  }

  const floored = Math.floor(ms);
  if (!Number.isFinite(floored)) return null;
  return floored;
}

// ---------------------------------------------------------------------------
// 2. Window calculation
// ---------------------------------------------------------------------------

/**
 * Compute the 30-second window start timestamp in milliseconds.
 *
 * @param {number} tsMs - integer milliseconds timestamp
 * @returns {number} window start in ms (multiple of 30000)
 */
export function windowStartMs(tsMs) {
  return Math.floor(tsMs / WINDOW_MS) * WINDOW_MS;
}

/**
 * Convert a window start timestamp (ms) to UTC date directory and file base names.
 *
 * @param {number} windowMs - window start in ms
 * @returns {{ dateDir: string, fileBase: string }}
 *   dateDir  — 'YYYY-MM-DD'
 *   fileBase — 'HH-MM-SS'
 */
export function windowStartToDateStr(windowMs) {
  const d = new Date(windowMs);
  const Y = String(d.getUTCFullYear()).padStart(4, '0');
  const M = String(d.getUTCMonth() + 1).padStart(2, '0');
  const D = String(d.getUTCDate()).padStart(2, '0');
  const h = String(d.getUTCHours()).padStart(2, '0');
  const m = String(d.getUTCMinutes()).padStart(2, '0');
  const s = String(d.getUTCSeconds()).padStart(2, '0');

  return {
    dateDir: `${Y}-${M}-${D}`,
    fileBase: `${h}-${m}-${s}`,
  };
}

// ---------------------------------------------------------------------------
// 3. No-clobber file operations
// ---------------------------------------------------------------------------

// Provided as an export so tests can inject link failures (e.g. EXDEV)
// without requiring cross-filesystem mounts.  Replaces fsp.link when set.
// Use _setTestLinkFn(fn) to set, _setTestLinkFn(null) to restore.
let _testLinkFn = null;
export function _setTestLinkFn(fn) { _testLinkFn = fn; }

// FIX4: test seam — writer ファクトリを上書きして I/O エラーを注入する
let _testMakeWriterFn = null;
/**
 * @param {((windowMs: number) => object)|null} fn
 * writer の代わりに任意のオブジェクトを返すファクトリ。null で解除。
 */
export function _setTestMakeWriterFn(fn) { _testMakeWriterFn = fn; }

/**
 * Atomically rename src → dest without overwriting an existing destination.
 *
 * Uses link(2) + unlink(2) as the no-clobber primitive:
 *   - link() fails with EEXIST if dest already exists (atomic check).
 *   - On success, unlink() removes the source.
 *
 * On EEXIST: the source is quarantined in-place by renaming to
 * `src.conflict`.  Returns { ok: false, reason: 'EEXIST' }.
 *
 * Both src and dest must reside on the same filesystem for the
 * link/unlink fast path.  Cross-device (EXDEV) falls back to
 * copyFile + unlink.
 *
 * @param {string} src  - source path
 * @param {string} dest - destination path
 * @returns {Promise<{ ok: boolean, reason?: string }>}
 */
export async function noClobberRename(src, dest) {
  try {
    // Attempt atomic no-clobber via hard link
    // Use test seam if provided, otherwise use real fsp.link
    if (_testLinkFn) {
      await _testLinkFn(src, dest);
    } else {
      await fsp.link(src, dest);
    }
    await fsp.unlink(src);
    return { ok: true };
  } catch (err) {
    if (err.code === 'EEXIST') {
      // Destination exists — quarantine the source in-place
      const conflictPath = src + '.conflict';
      try {
        await fsp.rename(src, conflictPath);
      } catch (e2) {
        // Best-effort: log but don't throw from the primitive
        console.error(
          `[noClobberRename] failed to quarantine ${src} → ${conflictPath}:`,
          e2.message,
        );
      }
      return { ok: false, reason: 'EEXIST' };
    }

    if (err.code === 'EXDEV') {
      // Cross-device fallback: copy + delete
      try {
        // Use COPYFILE_EXCL to avoid overwriting dest
        // Access via fs.constants (not top-level export) for ESM compatibility
        const { constants } = await import('node:fs');
        await fsp.copyFile(src, dest, constants.COPYFILE_EXCL);
        await fsp.unlink(src);
        return { ok: true };
      } catch (e2) {
        if (e2.code === 'EEXIST') {
          const conflictPath = src + '.conflict';
          await fsp.rename(src, conflictPath).catch(() => {});
          return { ok: false, reason: 'EEXIST' };
        }
        throw e2;
      }
    }

    throw err;
  }
}

/**
 * Move a file into a quarantine directory.  Never overwrites existing
 * quarantine artifacts — appends a uniqueness suffix on name collision.
 *
 * @param {string} filePath      - path of the file to quarantine
 * @param {string} quarantineDir - target directory (created if needed)
 * @returns {Promise<{ ok: boolean, dest: string }>}
 */
export async function noClobberQuarantine(filePath, quarantineDir) {
  await fsp.mkdir(quarantineDir, { recursive: true });

  const baseName = path.basename(filePath);
  const baseDest = path.join(quarantineDir, baseName + '.conflict');

  for (let attempt = 0; attempt < MAX_QUARANTINE_ATTEMPTS; attempt++) {
    const destPath =
      attempt === 0
        ? baseDest
        : path.join(quarantineDir, `${baseName}.conflict.${attempt}`);

    try {
      await fsp.link(filePath, destPath);
      await fsp.unlink(filePath);
      return { ok: true, dest: destPath };
    } catch (err) {
      if (err.code === 'EEXIST') continue; // name collision, try next suffix

      if (err.code === 'EXDEV') {
        // Cross-device: copy + delete with EXCL
        try {
          const { constants } = await import('node:fs');
          await fsp.copyFile(filePath, destPath, constants.COPYFILE_EXCL);
          await fsp.unlink(filePath);
          return { ok: true, dest: destPath };
        } catch (e2) {
          if (e2.code === 'EEXIST') continue;
          throw e2;
        }
      }

      throw err;
    }
  }

  throw new Error(
    `noClobberQuarantine: exceeded ${MAX_QUARANTINE_ATTEMPTS} attempts for ${filePath}`,
  );
}

// ---------------------------------------------------------------------------
// 5. Output root multi-instance lock (FIX5)
// ---------------------------------------------------------------------------
//
// 同一出力ルートへの複数 Receiver インスタンスの同時書き込みを防止する。
// ロック機構:
//   <outputRoot>/locks/receiver.lock/   ディレクトリ（mkdir の atomic 性を利用）
//   <outputRoot>/locks/receiver.lock/pid  保持プロセス PID
//
// 原則:
//   - mkdir が成功したプロセスのみがロックを保持する（atomic create）
//   - 同一プロセス内の複数 worker は PID 一致判定で idempotent 動作
//   - 保持プロセス異常終了時は PID 生存確認 (kill -0) で stale 検出し自動回収
//   - process.on('exit') で確実に後片付け

const LOCK_DIR_NAME = 'receiver.lock';
const PID_FILE_NAME = 'pid';

// FIX5: test seams — ロック関連の fs 操作を注入可能にする
let _testLockPid = null;
/**
 * テスト用: process.pid を上書きする。null で解除。
 * @param {number|null} pid
 */
export function _setTestLockPid(pid) { _testLockPid = pid; }

let _testLockFsError = null;
/**
 * テスト用: fs 操作エラーを注入する。
 * { mkdirSync?, writeFileSync?, readFileSync?, existsSync?, rmSync?, kill? }
 * 各キーに対応する操作が指定された code で失敗する。
 * null で解除。
 * @param {Object|null} errMap
 */
export function _setTestLockFsError(errMap) { _testLockFsError = errMap; }

function _getPid() {
  return _testLockPid !== null ? _testLockPid : process.pid;
}

function _mkdirSync(dir) {
  if (_testLockFsError?.mkdirSync) {
    const err = new Error('simulated mkdir error');
    err.code = _testLockFsError.mkdirSync;
    throw err;
  }
  fs.mkdirSync(dir);
}

function _writeFileSync(file, data) {
  if (_testLockFsError?.writeFileSync) {
    const err = new Error('simulated write error');
    err.code = _testLockFsError.writeFileSync;
    throw err;
  }
  fs.writeFileSync(file, data, 'utf-8');
}

function _readFileSync(file) {
  if (_testLockFsError?.readFileSync) {
    const err = new Error('simulated read error');
    err.code = _testLockFsError.readFileSync;
    throw err;
  }
  return fs.readFileSync(file, 'utf-8');
}

function _existsSync(file) {
  if (_testLockFsError?.existsSync) {
    const err = new Error('simulated exists error');
    err.code = _testLockFsError.existsSync;
    throw err;
  }
  return fs.existsSync(file);
}

function _rmSync(dir) {
  if (_testLockFsError?.rmSync) {
    const err = new Error('simulated rm error');
    err.code = _testLockFsError.rmSync;
    throw err;
  }
  fs.rmSync(dir, { recursive: true, force: true });
}

function _kill(pid) {
  if (_testLockFsError?.kill) {
    const err = new Error('simulated kill error');
    err.code = _testLockFsError.kill;
    throw err;
  }
  process.kill(pid, 0);
}

/**
 * Strict PID validation: only decimal positive integer strings are accepted.
 *
 * Returns the parsed PID number on success, null on any invalid input:
 *   - empty / whitespace-only string
 *   - non-digit characters (sign, decimal, alpha, etc.)
 *   - overflow (Infinity)
 *   - zero / negative
 *
 * Trims leading/trailing whitespace before validation (common in text files).
 *
 * @param {*} str - input to validate
 * @returns {number|null}
 */
export function _validatePidStrict(str) {
  if (typeof str !== 'string') return null;
  const trimmed = str.trim();
  if (trimmed.length === 0) return null;
  if (!/^\d+$/.test(trimmed)) return null;
  const n = parseInt(trimmed, 10);
  if (!Number.isFinite(n) || n <= 0 || n > Number.MAX_SAFE_INTEGER) return null;
  return n;
}

/**
 * 出力ルートのマルチインスタンスロックを取得する。
 *
 * mkdir の atomic 性を利用: ディレクトリ作成に成功したプロセスがロック保持者。
 * PID ファイルで同一プロセス内の重複取得を idempotent にする。
 *
 * I/O エラーは fail-closed: exists/read/kill/rm/mkdir/write の内部 I/O 失敗は
 * 常に lock-io-error を返し、ロックを削除しない。PID が読めない状態を stale と
 * 見なさず、TOCTOU 競合を防止する。
 *
 * @param {string} outputRoot - 出力ルートの絶対パス
 * @returns {{ ok: true, acquired?: boolean } | { ok: false, reason: string, holderPid?: number, error?: string }}
 */
export function acquireOutputRootLock(outputRoot) {
  const lockDir = path.join(outputRoot, 'locks', LOCK_DIR_NAME);
  const pidFile = path.join(lockDir, PID_FILE_NAME);
  const myPid = _getPid();

  // 0) locks/ 親ディレクトリを確保（atomic mkdir の前に必要）
  //    この mkdir は競合しない（全インスタンスが同じ dir を作る）
  try {
    fs.mkdirSync(path.dirname(lockDir), { recursive: true });
  } catch (e) {
    return { ok: false, reason: 'lock-io-error', error: `mkdir parent: ${e.message}` };
  }

  // 1) 同一プロセスがすでにロックを保持 → idempotent
  try {
    if (_existsSync(pidFile)) {
      const pidStr = _readFileSync(pidFile);
      const existingPid = _validatePidStrict(pidStr);
      if (existingPid !== null && existingPid === myPid) {
        return { ok: true, acquired: false };
      }
    }
  } catch (_) {
    // existsSync/readFileSync のエラー → mkdir にフォールスルー
  }

  // 2) Atomic mkdir でロック獲得
  try {
    _mkdirSync(lockDir);
    _writeFileSync(pidFile, String(myPid));
    return { ok: true, acquired: true };
  } catch (err) {
    if (err.code !== 'EEXIST') {
      // mkdir/書き込みの I/O エラー
      return { ok: false, reason: 'lock-io-error', error: `[${err.code}] ${err.message}` };
    }

    // 3) EEXIST — 別プロセスが保持中 (FIX8: fail-closed + TOCTOU-safe)
    return _handleLockContention(lockDir, pidFile, myPid);
  }
}

/**
 * EEXIST 発生時のロック競合ハンドラ (FIX8).
 *
 * 原則:
 *   - exists/read I/O エラー → lock-io-error (ロックを削除しない)
 *   - PID ファイルが空/不正 → lock-contention (保持者を確定できない)
 *   - PID ファイルなし → claim ディレクトリで TOCTOU-safe に stale 検出
 *   - kill ESRCH → stale cleanup (唯一のロック削除許可パス)
 *
 * @param {string} lockDir
 * @param {string} pidFile
 * @param {number} myPid
 * @returns {object}
 */
function _handleLockContention(lockDir, pidFile, myPid) {
  try {
    if (!_existsSync(pidFile)) {
      // ロックディレクトリはあるが PID ファイルがない。
      //   可能性 (a) 作成者が mkdir 後 PID 書き込み前にクラッシュ → stale
      //   可能性 (b) 作成者が PID 書き込み中 (TOCTOU)
      // claim ディレクトリの atomic mkdir で排他権を得る。
      return _handleMissingPidFile(lockDir, pidFile, myPid);
    }

    // PID ファイルが存在する → 内容を読む
    const pidStr = _readFileSync(pidFile);
    const holderPid = _validatePidStrict(pidStr);

    // 空/不正な PID → 保持者を特定できない → contention (ロックを削除しない)
    if (holderPid === null) {
      return { ok: false, reason: 'lock-contention' };
    }

    if (holderPid === myPid) {
      // 同一プロセス（別 worker thread）→ idempotent
      return { ok: true, acquired: false };
    }

    // 保持プロセスの生存確認
    try {
      _kill(holderPid);
      // 生存中 → 競合
      return { ok: false, reason: 'lock-contention', holderPid };
    } catch (e) {
      if (e.code === 'ESRCH') {
        // プロセス不在 → stale。唯一のロック削除パス。
        try {
          _rmSync(lockDir);
          _mkdirSync(lockDir);
          _writeFileSync(pidFile, String(myPid));
          return { ok: true, acquired: true };
        } catch (e2) {
          if (e2.code === 'EEXIST') {
            return { ok: false, reason: 'lock-contention' };
          }
          return { ok: false, reason: 'lock-io-error', error: `[${e2.code}] ${e2.message}` };
        }
      }
      // kill のその他のエラー (EPERM など)
      return { ok: false, reason: 'lock-io-error', error: `[${e.code}] ${e.message}` };
    }
  } catch (e) {
    // existsSync / readFileSync の I/O エラー → ロックを削除せず fail-closed
    return { ok: false, reason: 'lock-io-error', error: `[${e.code}] ${e.message}` };
  }
}

/**
 * PID ファイル不在時のハンドラ: claim ディレクトリで TOCTOU-safe に stale 検出.
 *
 *  1) mkdir <lockDir>/.claim-<myPid> で排他権を取得
 *  2) PID ファイルを再確認 (TOCTOU 競合が閉じた状態)
 *  3) 存在すれば → contention (作成者が書き込んだ)
 *  4) 不在なら → 真の stale → rm lockDir → 再獲得
 *
 * @param {string} lockDir
 * @param {string} pidFile
 * @param {number} myPid
 * @returns {object}
 */
function _handleMissingPidFile(lockDir, pidFile, myPid) {
  const claimDir = path.join(lockDir, `.claim-${myPid}`);

  // Atomic mkdir で cleanup 権を獲得
  try {
    _mkdirSync(claimDir);
  } catch (e) {
    if (e.code === 'EEXIST') {
      return { ok: false, reason: 'lock-contention' };
    }
    return { ok: false, reason: 'lock-io-error', error: `[${e.code}] ${e.message}` };
  }

  // Claim 取得済み — PID ファイルを再確認 (TOCTOU は閉じている)
  try {
    if (_existsSync(pidFile)) {
      // 作成者が PID を書き終えた → contention
      try { _rmSync(claimDir); } catch {}
      const pidStr = _readFileSync(pidFile);
      const hp = _validatePidStrict(pidStr);
      if (hp !== null && hp === myPid) {
        return { ok: true, acquired: false };
      }
      return {
        ok: false,
        reason: 'lock-contention',
        holderPid: (hp !== null && hp > 0) ? hp : undefined,
      };
    }

    // 依然 PID ファイルなし → 真の stale
    _rmSync(lockDir);

    // 新規ロック獲得
    try {
      _mkdirSync(lockDir);
      _writeFileSync(pidFile, String(myPid));
      return { ok: true, acquired: true };
    } catch (e2) {
      if (e2.code === 'EEXIST') {
        return { ok: false, reason: 'lock-contention' };
      }
      return { ok: false, reason: 'lock-io-error', error: `[${e2.code}] ${e2.message}` };
    }
  } catch (e) {
    // I/O エラー — cleanup 権を解放して fail-closed
    try { _rmSync(claimDir); } catch {}
    return { ok: false, reason: 'lock-io-error', error: `[${e.code}] ${e.message}` };
  }
}

/**
 * 出力ルートのロックを解放する。
 * ロック保持プロセスの場合のみ実際に削除する。
 * 他プロセスのロックは触らない（安全）。
 *
 * @param {string} outputRoot
 */
export function releaseOutputRootLock(outputRoot) {
  // FIX7a: テスト後 cleanup で outputRoot が null になる可能性があるため guard
  if (!outputRoot) return;
  const lockDir = path.join(outputRoot, 'locks', LOCK_DIR_NAME);
  const pidFile = path.join(lockDir, PID_FILE_NAME);
  const myPid = _getPid();

  try {
    if (!_existsSync(lockDir)) return;
    // FIX9a: PID file absent → cannot prove ownership → fail-closed no-op
    if (!_existsSync(pidFile)) return;
    const pidStr = _readFileSync(pidFile);
    const holderPid = _validatePidStrict(pidStr);
    // FIX9a: malformed PID → cannot prove ownership → fail-closed no-op
    if (holderPid === null) return;
    if (holderPid === myPid) {
      _rmSync(lockDir);
    }
    // 他プロセスのロックは無視
  } catch (_) {
    // I/O エラー → fail-closed (no-op)
  }
}

// ---------------------------------------------------------------------------
// 4. RawRotationWriter
// ---------------------------------------------------------------------------

/**
 * Per-market/kind writer that rotates raw data into 30-second window files.
 *
 * File path convention:
 *   .open:      <basePath>/<kind>/<market>/<YYYY-MM-DD>/<HH-MM-SS>.jsonl.open
 *   finalized:  <basePath>/<kind>/<market>/<YYYY-MM-DD>/<HH-MM-SS>.jsonl
 *   quarantine: <basePath>/_quarantine/<market>/<kind>/<YYYY-MM-DD>/<name>.conflict
 *
 * Maintains up to two writable windows (current + previous) to tolerate
 * late-arriving events.  All mutations are serialized through an internal
 * promise queue, guaranteeing per-market/kind ordering.
 */
export class RawRotationWriter {
  /**
   * @param {string} basePath - root directory, e.g. 'raw/'
   * @param {string} market   - market identifier, e.g. 'binance_spot'
   * @param {string} kind     - data kind, e.g. 'trades', 'book_updates', 'liquidations', 'snapshots'
   * @param {object} [options]
   * @param {number} [options.flushIntervalMs=1000]   - passed to BufferedWriter
   * @param {number} [options.maxBufferLines=4096]    - passed to BufferedWriter
   * @param {string} [options.quarantineDir]          - override quarantine root;
   *                                                     defaults to <basePath>/_quarantine/<market>/<kind>
   */
  constructor(basePath, market, kind, options = {}) {
    this._basePath = basePath;
    this._market = market;
    this._kind = kind;
    this._flushIntervalMs = options.flushIntervalMs ?? 1000;
    this._maxBufferLines = options.maxBufferLines ?? 4096;
    this._quarantineDir =
      options.quarantineDir ??
      path.join(basePath, '_quarantine', market, kind);

    // State
    /** @type {number|null} */
    this._currentWindowMs = null;
    /** @type {BufferedWriter|null} */
    this._currentWriter = null;

    /** @type {number|null} */
    this._previousWindowMs = null;
    /** @type {BufferedWriter|null} */
    this._previousWriter = null;

    /** @type {number|null} — highest finalized window start ms */
    this._finalizedWatermarkMs = null;

    /** @type {Promise<void>} — serialized mutation queue */
    this._queue = Promise.resolve();

    // FIX4: raw I/O エラー追跡 — 書き込み/フラッシュ/stale check の失敗をカウント
    /** @type {number} */
    this._writeErrorCount = 0;
    /** @type {string|null} */
    this._lastWriteErrorMsg = null;
    /** @type {number|null} */
    this._lastWriteErrorAt = null;
  }

  // ---- Public API ---------------------------------------------------------

  /**
   * Write a JSON-serializable object to the appropriate 30-second window file.
   *
   * Errors are caught and logged; this method never throws.
   *
   * @param {object} obj              - data to write (will be JSON-stringified)
   * @param {number} eventTimestampMs - raw event timestamp (any accepted unit)
   * @returns {Promise<void>}
   */
  async write(obj, eventTimestampMs) {
    try {
      await this._enqueue(() => this._writeImpl(obj, eventTimestampMs));
    } catch (err) {
      // FIX4: エラーカウンタを増やし、エビデンスを保持
      this._writeErrorCount++;
      this._lastWriteErrorMsg = err.message;
      this._lastWriteErrorAt = Date.now();
      console.error(
        `[RawRotationWriter] ${this._market}/${this._kind}: write error:`,
        err.message,
      );
    }
  }

  /**
   * Flush, close, and finalize all open writers.  Updates the finalized
   * watermark on success.
   *
   * @returns {Promise<void>}
   */
  async finalize() {
    try {
      await this._enqueue(() => this._finalizeImpl());
    } catch (err) {
      // FIX4: finalize のエラーも追跡
      this._writeErrorCount++;
      this._lastWriteErrorMsg = `finalize: ${err.message}`;
      this._lastWriteErrorAt = Date.now();
      console.error(
        `[RawRotationWriter] ${this._market}/${this._kind}: finalize error:`,
        err.message,
      );
    }
  }

  /**
   * Scan existing files and recover state after a restart.
   *
   * Must be called before any write() for this market/kind.  The call is
   * serialized through the same queue so it completes before any live
   * mutation.
   *
   * @param {number} nowMs - current wall-clock time in ms (Date.now())
   * @returns {Promise<void>}
   */
  async startupRecovery(nowMs, onRecoveredTrade) {
    return this._enqueue(() => this._startupRecoveryImpl(nowMs, onRecoveredTrade));
  }

  /**
   * Finalize any windows that are more than 60 seconds behind wall clock.
   * Call periodically (e.g. every 5–15 s) to prevent stranded .open files
   * when a market goes quiet.
   *
   * @param {number} [nowMs] - current wall-clock time (defaults to Date.now())
   * @returns {Promise<void>}
   */
  async checkStale(nowMs) {
    const now = nowMs ?? Date.now();
    try {
      await this._enqueue(() => this._checkStaleImpl(now));
    } catch (err) {
      // FIX4: checkStale のエラーも追跡
      this._writeErrorCount++;
      this._lastWriteErrorMsg = `checkStale: ${err.message}`;
      this._lastWriteErrorAt = Date.now();
      console.error(
        `[RawRotationWriter] ${this._market}/${this._kind}: checkStale error:`,
        err.message,
      );
    }
  }

  /** @returns {number|null} highest finalized window start ms */
  getWatermark() {
    return this._finalizedWatermarkMs;
  }

  /** @returns {number|null} current open window start ms */
  getCurrentWindowMs() {
    return this._currentWindowMs;
  }

  // ---- FIX4: I/O エラー追跡 ------------------------------------------------

  /** @returns {number} 累積 I/O エラー回数 */
  getWriteErrorCount() {
    return this._writeErrorCount;
  }

  /** @returns {{ message: string, at: number }|null} 最後のエラー情報 */
  getLastWriteError() {
    return this._lastWriteErrorMsg
      ? { message: this._lastWriteErrorMsg, at: this._lastWriteErrorAt }
      : null;
  }

  /** エラーカウンタをリセット（retry/recovery 後に呼ぶ） */
  clearWriteError() {
    this._writeErrorCount = 0;
    this._lastWriteErrorMsg = null;
    this._lastWriteErrorAt = null;
  }

  // ---- Internal: queue ----------------------------------------------------

  /**
   * Enqueue an async function so that all mutations run sequentially.
   * If a previous task rejected the chain still advances.
   *
   * @param {() => Promise<void>} fn
   * @returns {Promise<void>}
   */
  _enqueue(fn) {
    const next = this._queue.then(
      () => fn(),
      () => fn(), // continue chain even after rejection
    );
    // Prevent unhandled rejections from breaking future tasks
    this._queue = next.catch(() => {});
    return next;
  }

  // ---- Internal: write ----------------------------------------------------

  /**
   * Core write logic (runs inside the queue).
   */
  async _writeImpl(obj, eventTimestampMs) {
    // 1. Normalize timestamp
    const tsMs = normalizeTimestampMs(eventTimestampMs);
    if (tsMs === null) {
      console.error(
        `[RawRotationWriter] ${this._market}/${this._kind}: non-numeric/invalid timestamp, dropping event`,
      );
      return;
    }

    // 2. Compute window
    const wMs = windowStartMs(tsMs);

    // 3. Validate
    if (tsMs < 0 || wMs < 0) {
      console.error(
        `[RawRotationWriter] ${this._market}/${this._kind}: negative timestamp ${tsMs}, dropping event`,
      );
      return;
    }

    const nowMs = Date.now();
    const currentWallWindow = windowStartMs(nowMs);
    if (wMs > currentWallWindow) {
      console.error(
        `[RawRotationWriter] ${this._market}/${this._kind}: future window ${wMs} > wall ${currentWallWindow}, dropping event`,
      );
      return;
    }

    if (
      this._finalizedWatermarkMs !== null &&
      wMs <= this._finalizedWatermarkMs
    ) {
      console.error(
        `[RawRotationWriter] ${this._market}/${this._kind}: window ${wMs} <= watermark ${this._finalizedWatermarkMs}, dropping event`,
      );
      return;
    }

    // 4. Route to correct writer (first attempt)
    if (wMs === this._currentWindowMs) {
      await this._currentWriter.write(obj);
      return;
    }

    if (wMs === this._previousWindowMs && this._previousWriter) {
      await this._previousWriter.write(obj);
      return;
    }

    // 5. Window change — rotate writers to accommodate the new window
    await this._rotateWriters(wMs);

    // 6. Route again after rotation (window must now match current or previous)
    if (wMs === this._currentWindowMs) {
      await this._currentWriter.write(obj);
    } else if (wMs === this._previousWindowMs && this._previousWriter) {
      await this._previousWriter.write(obj);
    } else {
      // Should not reach here — rotation should have handled it
      console.error(
        `[RawRotationWriter] ${this._market}/${this._kind}: internal error — ` +
        `window ${wMs} not matched after rotation (current=${this._currentWindowMs}, previous=${this._previousWindowMs})`,
      );
    }
  }

  /**
   * Rotate writers when a new window appears.
   *
   * @param {number} wMs - target window start ms
   */
  async _rotateWriters(wMs) {
    // If this is the very first window, just open it
    if (this._currentWindowMs === null) {
      this._currentWindowMs = wMs;
      this._currentWriter = this._createWriter(wMs);
      return;
    }

    // wMs > currentWindowMs: forward progression (normal case)
    if (wMs > this._currentWindowMs) {
      // Finalize the previous window (if any)
      if (this._previousWriter) {
        await this._finalizeWriter(this._previousWriter, this._previousWindowMs);
        this._previousWriter = null;
        this._previousWindowMs = null;
      }

      // Shift current → previous
      this._previousWriter = this._currentWriter;
      this._previousWindowMs = this._currentWindowMs;

      // Open new current
      this._currentWindowMs = wMs;
      this._currentWriter = this._createWriter(wMs);
      return;
    }

    // wMs < currentWindowMs but > finalized watermark (late event)
    // and wMs != previousWindowMs (already checked in _writeImpl).
    //
    // This means wMs falls between previous and current —
    // e.g. previous=W, current=W+2, event=W+1.
    // Treat it as the new previous: finalize old previous, set wMs as previous.

    if (this._previousWriter) {
      await this._finalizeWriter(this._previousWriter, this._previousWindowMs);
    }

    this._previousWindowMs = wMs;
    this._previousWriter = this._createWriter(wMs);

    // current stays unchanged
  }

  // ---- Internal: finalize -------------------------------------------------

  async _finalizeImpl() {
    if (this._previousWriter) {
      await this._finalizeWriter(this._previousWriter, this._previousWindowMs);
      this._previousWriter = null;
      this._previousWindowMs = null;
    }
    if (this._currentWriter) {
      await this._finalizeWriter(this._currentWriter, this._currentWindowMs);
      this._currentWriter = null;
      this._currentWindowMs = null;
    }
  }

  /**
   * Flush, close, and no-clobber rename a single writer's file
   * from .open → .jsonl.  Updates the finalized watermark on success.
   *
   * @param {BufferedWriter} writer
   * @param {number} windowMs
   */
  async _finalizeWriter(writer, windowMs) {
    if (!writer) return;

    const openPath = writer._filePath;

    // Drain & close
    await writer.flush();
    await writer.close();

    // If no data was ever written, the .open file may not exist on disk.
    // Skip the rename in that case — nothing to finalize.
    try {
      await fsp.access(openPath);
    } catch {
      // File doesn't exist — nothing to do
      return;
    }

    // No-clobber rename .open → .jsonl
    const finalPath = openPath.replace(/\.open$/, '');
    const result = await noClobberRename(openPath, finalPath);

    if (result.ok) {
      // Success — advance watermark
      if (
        this._finalizedWatermarkMs === null ||
        windowMs > this._finalizedWatermarkMs
      ) {
        this._finalizedWatermarkMs = windowMs;
      }
    } else {
      // EEXIST — source was renamed to openPath + '.conflict' in-place.
      // Move the conflict artifact to the proper quarantine directory.
      const conflictPath = openPath + '.conflict';
      const dateDir = path.basename(path.dirname(openPath));
      const quarantineTargetDir = path.join(this._quarantineDir, dateDir);

      try {
        await noClobberQuarantine(conflictPath, quarantineTargetDir);
      } catch (e2) {
        console.error(
          `[RawRotationWriter] ${this._market}/${this._kind}: failed to quarantine conflict for window ${windowMs}:`,
          e2.message,
        );
      }

      console.error(
        `[RawRotationWriter] ${this._market}/${this._kind}: EEXIST conflict finalizing window ${windowMs} (${path.basename(openPath)}), quarantined — watermark NOT advanced`,
      );
    }
  }

  // ---- Internal: stale check ----------------------------------------------

  async _checkStaleImpl(nowMs) {
    const currentWallWindow = windowStartMs(nowMs);

    // Finalize previous window if it's ≥ 60 s behind wall clock
    if (
      this._previousWindowMs !== null &&
      currentWallWindow >= this._previousWindowMs + 60000
    ) {
      if (this._previousWriter) {
        await this._finalizeWriter(this._previousWriter, this._previousWindowMs);
        this._previousWriter = null;
        this._previousWindowMs = null;
      }
    }

    // Finalize current window if it's ≥ 60 s behind wall clock
    if (
      this._currentWindowMs !== null &&
      currentWallWindow >= this._currentWindowMs + 60000
    ) {
      if (this._currentWriter) {
        await this._finalizeWriter(this._currentWriter, this._currentWindowMs);
        this._currentWriter = null;
        this._currentWindowMs = null;
      }
    }
  }

  // ---- Internal: startup recovery -----------------------------------------

  async _startupRecoveryImpl(nowMs) {
    const dataRoot = path.join(this._basePath, this._kind, this._market);
    const nowWindowMs = windowStartMs(nowMs);

    // 1. Scan .jsonl files → compute initial watermark
    const jsonlFiles = await this._scanFiles(dataRoot, /\.jsonl$/);
    const openFilesAll = await this._scanFiles(dataRoot, /\.jsonl\.open$/);

    for (const fp of jsonlFiles) {
      const w = this._extractWindowMs(fp);
      if (w !== null) {
        if (
          this._finalizedWatermarkMs === null ||
          w > this._finalizedWatermarkMs
        ) {
          this._finalizedWatermarkMs = w;
        }
      }
    }

    // 2. Group .open files by windowStartMs
    /** @type {Map<number, string[]>} */
    const byWindow = new Map();
    for (const fp of openFilesAll) {
      const w = this._extractWindowMs(fp);
      if (w === null) continue;
      if (!byWindow.has(w)) byWindow.set(w, []);
      byWindow.get(w).push(fp);
    }

    // 3. Process in ascending window order
    const sortedWindows = [...byWindow.keys()].sort((a, b) => a - b);

    /** @type {number|null} */
    let keepCurrentWindow = null;
    /** @type {number|null} */
    let keepPreviousWindow = null;

    for (const wMs of sortedWindows) {
      const files = byWindow.get(wMs);

      // Deduplicate: keep one, quarantine the rest
      const [keep, ...extras] = files;
      for (const extra of extras) {
        const dateDir = path.basename(path.dirname(extra));
        const quarantineTarget = path.join(this._quarantineDir, dateDir);
        console.error(
          `[RawRotationWriter] ${this._market}/${this._kind}: duplicate .open for window ${wMs}, quarantining ${path.basename(extra)}`,
        );
        await noClobberQuarantine(extra, quarantineTarget).catch((err) =>
          console.error(
            `[RawRotationWriter] quarantine error:`,
            err.message,
          ),
        );
      }

      // Decide what to do with the kept file
      if (
        this._finalizedWatermarkMs !== null &&
        wMs <= this._finalizedWatermarkMs
      ) {
        // Already behind watermark → quarantine
        const dateDir = path.basename(path.dirname(keep));
        const quarantineTarget = path.join(this._quarantineDir, dateDir);
        console.error(
          `[RawRotationWriter] ${this._market}/${this._kind}: stale .open for window ${wMs} (<= watermark ${this._finalizedWatermarkMs}), quarantining`,
        );
        await noClobberQuarantine(keep, quarantineTarget).catch((err) =>
          console.error(`[RawRotationWriter] quarantine error:`, err.message),
        );
        continue;
      }

      if (wMs > nowWindowMs + WINDOW_MS) {
        // Future-named → quarantine with .future suffix
        const dateDir = path.basename(path.dirname(keep));
        const quarantineTarget = path.join(this._quarantineDir, dateDir);
        console.error(
          `[RawRotationWriter] ${this._market}/${this._kind}: future .open for window ${wMs}, quarantining as future`,
        );
        // Rename to indicate "future" before quarantining
        const futurePath = keep + '.future';
        try {
          await fsp.rename(keep, futurePath);
          await noClobberQuarantine(futurePath, quarantineTarget).catch(
            (err) =>
              console.error(
                `[RawRotationWriter] quarantine error:`,
                err.message,
              ),
          );
        } catch (e) {
          // Fallback: try to quarantine directly
          await noClobberQuarantine(keep, quarantineTarget).catch(() => {});
        }
        continue;
      }

      // Check if this window is "current" or "previous" relative to now
      if (wMs === nowWindowMs) {
        keepCurrentWindow = wMs;
      } else if (wMs === nowWindowMs - WINDOW_MS) {
        keepPreviousWindow = wMs;
      } else {
        // Older than previous but not behind watermark → recovery-finalize
        const finalPath = keep.replace(/\.open$/, '');
        const result = await noClobberRename(keep, finalPath);
        if (result.ok) {
          if (
            this._finalizedWatermarkMs === null ||
            wMs > this._finalizedWatermarkMs
          ) {
            this._finalizedWatermarkMs = wMs;
          }
        } else {
          // Conflict — already handled by noClobberRename (quarantined in-place).
          // Move to proper quarantine dir.
          const conflictPath = keep + '.conflict';
          const dateDir = path.basename(path.dirname(keep));
          const quarantineTarget = path.join(this._quarantineDir, dateDir);
          await noClobberQuarantine(conflictPath, quarantineTarget).catch(
            () => {},
          );
        }
      }
    }

    // 4. Set up writers for kept windows
    if (keepPreviousWindow !== null) {
      this._previousWindowMs = keepPreviousWindow;
      // Don't create a writer for previous if it equals current (edge case)
      if (keepPreviousWindow !== keepCurrentWindow) {
        this._previousWriter = this._createWriter(keepPreviousWindow);
      }
    }

    if (keepCurrentWindow !== null) {
      this._currentWindowMs = keepCurrentWindow;
      this._currentWriter = this._createWriter(keepCurrentWindow);

      // If previous wasn't set but we found one that should be, ensure it's set
      if (this._previousWindowMs === null && keepPreviousWindow === null) {
        // Check if there's a window W-1 that could serve as previous
        const candidatePrev = keepCurrentWindow - WINDOW_MS;
        if (sortedWindows.includes(candidatePrev)) {
          // The candidate was already processed above; if it wasn't kept,
          // it was either finalized or quarantined. Don't reopen.
        }
      }
    }

    console.error(
      `[RawRotationWriter] ${this._market}/${this._kind}: recovery complete. ` +
        `watermark=${this._finalizedWatermarkMs}, ` +
        `current=${this._currentWindowMs}, previous=${this._previousWindowMs}`,
    );
  }

  // ---- Internal: helpers --------------------------------------------------

  /**
   * Create a BufferedWriter for the given window.
   * Auto-flush is disabled — the rotation writer manages flushing.
   *
   * @param {number} windowMs
   * @returns {BufferedWriter}
   */
  _createWriter(windowMs) {
    // FIX4: test seam が設定されていればそれを使う
    if (_testMakeWriterFn) return _testMakeWriterFn(windowMs);

    const { dateDir, fileBase } = windowStartToDateStr(windowMs);
    const dir = path.join(this._basePath, this._kind, this._market, dateDir);
    const filePath = path.join(dir, `${fileBase}.jsonl.open`);

    return new BufferedWriter(filePath, {
      flushIntervalMs: this._flushIntervalMs,
      maxBufferLines: this._maxBufferLines,
      autoFlush: false, // We manage flush lifecycle explicitly
      idleCloseMs: 86_400_000, // 24 h — effectively disable pool idle close
    });
  }

  /**
   * Recursively scan a directory for files matching a regex.
   * Compatible with Node 18 (manual recursion — no `recursive: true`).
   *
   * @param {string} dir
   * @param {RegExp} pattern - tested against basename
   * @returns {Promise<string[]>}
   */
  async _scanFiles(dir, pattern) {
    /** @type {string[]} */
    const results = [];
    try {
      const entries = await fsp.readdir(dir, { withFileTypes: true });
      for (const entry of entries) {
        const fullPath = path.join(dir, entry.name);
        if (entry.isDirectory()) {
          const sub = await this._scanFiles(fullPath, pattern);
          results.push(...sub);
        } else if (entry.isFile()) {
          if (pattern.test(entry.name)) {
            results.push(fullPath);
          }
        }
      }
    } catch (err) {
      // ENOENT is fine — directory doesn't exist yet
      if (err.code !== 'ENOENT') throw err;
    }
    return results;
  }

  /**
   * Extract the window start ms from a file path.
   *
   * Path format: .../<YYYY-MM-DD>/<HH-MM-SS>.jsonl[.open]
   *
   * @param {string} filePath
   * @returns {number|null}
   */
  _extractWindowMs(filePath) {
    const name = path.basename(filePath);
    const dirName = path.basename(path.dirname(filePath));

    const timeMatch = name.match(/^(\d{2})-(\d{2})-(\d{2})/);
    const dateMatch = dirName.match(/^(\d{4})-(\d{2})-(\d{2})$/);

    if (!timeMatch || !dateMatch) return null;

    const h = parseInt(timeMatch[1], 10);
    const m = parseInt(timeMatch[2], 10);
    const s = parseInt(timeMatch[3], 10);
    const Y = parseInt(dateMatch[1], 10);
    const M = parseInt(dateMatch[2], 10);
    const D = parseInt(dateMatch[3], 10);

    const ts = Date.UTC(Y, M - 1, D, h, m, s, 0);
    return ts;
  }
}
