// test/raw-rotation-writer.test.mjs — RawRotationWriter unit tests
// Aligned to lib/raw-rotation-writer.mjs API

import { describe, it, before, after, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import fsp from 'node:fs/promises';
import fs from 'node:fs';
import path from 'node:path';
import os from 'node:os';

import {
  normalizeTimestampMs,
  windowStartMs,
  windowStartToDateStr,
  noClobberRename,
  noClobberQuarantine,
  RawRotationWriter,
  _setTestLinkFn,
  _setTestMakeWriterFn,
} from '../lib/raw-rotation-writer.mjs';

// ─── Helpers ────────────────────────────────────────────────────────────────

function tmpDir(label) {
  const dir = path.join(os.tmpdir(), 'rrw-test', `${label}-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`);
  fs.mkdirSync(dir, { recursive: true });
  return dir;
}

async function rmDir(dir) {
  try { await fsp.rm(dir, { recursive: true, force: true }); } catch {}
}

// ─── 1. normalizeTimestampMs ────────────────────────────────────────────────

describe('normalizeTimestampMs', () => {
  it('returns null for non-number types', () => {
    assert.strictEqual(normalizeTimestampMs('12345'), null);
    assert.strictEqual(normalizeTimestampMs(true), null);
    assert.strictEqual(normalizeTimestampMs(null), null);
    assert.strictEqual(normalizeTimestampMs(undefined), null);
    assert.strictEqual(normalizeTimestampMs({}), null);
  });

  it('returns null for NaN', () => {
    assert.strictEqual(normalizeTimestampMs(NaN), null);
  });

  it('returns null for Infinity / -Infinity', () => {
    assert.strictEqual(normalizeTimestampMs(Infinity), null);
    assert.strictEqual(normalizeTimestampMs(-Infinity), null);
  });

  it('returns null for numeric strings', () => {
    assert.strictEqual(normalizeTimestampMs('1690000000'), null);
    assert.strictEqual(normalizeTimestampMs('1690000000000'), null);
  });

  describe('seconds (< 1e11)', () => {
    it('converts 0 → 0', () => assert.strictEqual(normalizeTimestampMs(0), 0));
    it('converts 1 → 1000', () => assert.strictEqual(normalizeTimestampMs(1), 1000));
    it('converts 1690000000 → ms', () => assert.strictEqual(normalizeTimestampMs(1690000000), 1690000000000));
  });

  describe('milliseconds (1e11..1e14)', () => {
    it('passes through 1690000000000', () => assert.strictEqual(normalizeTimestampMs(1690000000000), 1690000000000));
  });

  describe('microseconds (1e14..1e17)', () => {
    it('converts 1690000000000000 → ms', () => assert.strictEqual(normalizeTimestampMs(1690000000000000), 1690000000000));
  });

  describe('nanoseconds (1e17..1e20)', () => {
    it('converts 1690000000000000000 → ms', () => assert.strictEqual(normalizeTimestampMs(1690000000000000000), 1690000000000));
  });

  describe('out of range', () => {
    it('returns null for >= 1e20', () => assert.strictEqual(normalizeTimestampMs(1e20), null));
    it('returns null for <= -1e20', () => assert.strictEqual(normalizeTimestampMs(-1e20), null));
  });

  describe('floor to integer ms', () => {
    it('floors fractional seconds', () => assert.strictEqual(normalizeTimestampMs(1.7), 1700));
    it('floors fractional ms', () => assert.strictEqual(normalizeTimestampMs(1690000000000.9), 1690000000000));
  });
});

// ─── 2. windowStartMs ───────────────────────────────────────────────────────

describe('windowStartMs', () => {
  it('0 → 0', () => assert.strictEqual(windowStartMs(0), 0));
  it('29999 → 0', () => assert.strictEqual(windowStartMs(29999), 0));
  it('30000 → 30000', () => assert.strictEqual(windowStartMs(30000), 30000));
  it('60001 → 60000', () => assert.strictEqual(windowStartMs(60001), 60000));
  it('59999 → 30000', () => assert.strictEqual(windowStartMs(59999), 30000));
});

// ─── 3. windowStartToDateStr ────────────────────────────────────────────────

describe('windowStartToDateStr', () => {
  it('returns correct UTC date/file for a known timestamp', () => {
    const d = new Date(Date.UTC(2026, 6, 5, 12, 0, 30));
    const r = windowStartToDateStr(d.getTime());
    assert.strictEqual(r.dateDir, '2026-07-05');
    assert.strictEqual(r.fileBase, '12-00-30');
  });

  it('handles epoch 0', () => {
    const r = windowStartToDateStr(0);
    assert.strictEqual(r.dateDir, '1970-01-01');
    assert.strictEqual(r.fileBase, '00-00-00');
  });

  it('UTC midnight: 23:59:30 stays in same day', () => {
    const d = new Date(Date.UTC(2026, 0, 1, 23, 59, 30));
    const r = windowStartToDateStr(d.getTime());
    assert.strictEqual(r.dateDir, '2026-01-01');
    assert.strictEqual(r.fileBase, '23-59-30');
  });

  it('UTC midnight: 00:00:00 goes to new day', () => {
    const d = new Date(Date.UTC(2026, 0, 2, 0, 0, 0));
    const r = windowStartToDateStr(d.getTime());
    assert.strictEqual(r.dateDir, '2026-01-02');
    assert.strictEqual(r.fileBase, '00-00-00');
  });

  it('pads single digits', () => {
    const d = new Date(Date.UTC(2026, 0, 1, 1, 2, 3));
    const r = windowStartToDateStr(d.getTime());
    assert.strictEqual(r.fileBase, '01-02-03');
  });
});

// ─── 4. noClobberRename ─────────────────────────────────────────────────────

describe('noClobberRename', () => {
  let dir;
  before(() => { dir = tmpDir('noclobber'); });
  after(async () => { await rmDir(dir); });

  it('renames when dest does not exist', async () => {
    const src = path.join(dir, 'src.txt');
    const dest = path.join(dir, 'dest.txt');
    fs.writeFileSync(src, 'hello');
    const result = await noClobberRename(src, dest);
    assert.deepStrictEqual(result, { ok: true });
    assert.ok(!fs.existsSync(src));
    assert.ok(fs.existsSync(dest));
    assert.strictEqual(fs.readFileSync(dest, 'utf-8'), 'hello');
  });

  it('quarantines source in-place when dest exists (EEXIST)', async () => {
    const src = path.join(dir, 'src2.txt');
    const dest = path.join(dir, 'dest2.txt');
    fs.writeFileSync(src, 'new data');
    fs.writeFileSync(dest, 'existing data');
    const result = await noClobberRename(src, dest);
    assert.deepStrictEqual(result, { ok: false, reason: 'EEXIST' });
    // Source should have been renamed to src2.txt.conflict
    assert.ok(!fs.existsSync(src), 'original src should be gone');
    assert.ok(fs.existsSync(src + '.conflict'), 'conflict file should exist');
    assert.strictEqual(fs.readFileSync(dest, 'utf-8'), 'existing data');
  });
});

// ─── 5. noClobberRename — EXDEV cross-device fallback ──────────────────────

describe('noClobberRename EXDEV fallback', () => {
  let dir;
  before(() => { dir = tmpDir('exdev'); });
  after(async () => { await rmDir(dir); });

  /** Force EXDEV by replacing the link function via test seam. */
  function forceEXDEV() {
    _setTestLinkFn(async () => {
      throw Object.assign(new Error('cross-device link'), { code: 'EXDEV' });
    });
  }

  function restoreLink() {
    _setTestLinkFn(null);
  }

  it('link+unlink path succeeds on same filesystem (normal rename)', async () => {
    restoreLink();
    const src = path.join(dir, 'exdev-norm-src.txt');
    const dest = path.join(dir, 'exdev-norm-dest.txt');
    fs.writeFileSync(src, 'exdev data');
    const result = await noClobberRename(src, dest);
    assert.deepStrictEqual(result, { ok: true });
    assert.ok(!fs.existsSync(src));
    assert.ok(fs.existsSync(dest));
    assert.strictEqual(fs.readFileSync(dest, 'utf-8'), 'exdev data');
  });

  it('EXDEV copy+unlink fallback succeeds when dest does not exist', async () => {
    forceEXDEV();
    try {
      const src = path.join(dir, 'exdev-copy-src.txt');
      const dest = path.join(dir, 'exdev-copy-dest.txt');
      fs.writeFileSync(src, 'exdev copy data');
      const result = await noClobberRename(src, dest);
      assert.deepStrictEqual(result, { ok: true });
      assert.ok(!fs.existsSync(src), 'src should be unlinked after copy');
      assert.ok(fs.existsSync(dest), 'dest should exist after copy');
      assert.strictEqual(fs.readFileSync(dest, 'utf-8'), 'exdev copy data');
    } finally {
      restoreLink();
    }
  });

  it('EXDEV copy+unlink quarantines src when dest already exists (EEXIST during copyFile)', async () => {
    forceEXDEV();
    try {
      const src = path.join(dir, 'exdev-conflict-src.txt');
      const dest = path.join(dir, 'exdev-conflict-dest.txt');
      fs.writeFileSync(src, 'new data');
      fs.writeFileSync(dest, 'existing data');
      const result = await noClobberRename(src, dest);
      assert.deepStrictEqual(result, { ok: false, reason: 'EEXIST' });
      // Dest must still contain original content (no-clobber invariant)
      assert.strictEqual(fs.readFileSync(dest, 'utf-8'), 'existing data');
      // Source should be quarantined
      assert.ok(!fs.existsSync(src), 'src should be quarantined');
      assert.ok(fs.existsSync(src + '.conflict'), 'conflict file should exist');
      // Verify conflict contains new data
      assert.strictEqual(fs.readFileSync(src + '.conflict', 'utf-8'), 'new data');
    } finally {
      restoreLink();
    }
  });

  it('EXDEV copy+unlink no-clobber always protects dest', async () => {
    // Verify that even when link fails with EXDEV, the no-clobber invariant
    // is maintained — dest is never overwritten.
    forceEXDEV();
    try {
      const src = path.join(dir, 'exdev-protect-src.txt');
      const dest = path.join(dir, 'exdev-protect-dest.txt');
      fs.writeFileSync(src, 'should not overwrite');
      fs.writeFileSync(dest, 'protected content');
      const result = await noClobberRename(src, dest);
      assert.deepStrictEqual(result, { ok: false, reason: 'EEXIST' });
      assert.strictEqual(fs.readFileSync(dest, 'utf-8'), 'protected content');
    } finally {
      restoreLink();
    }
  });

  it('EXDEV copy+unlink re-throws non-EXDEV, non-EEXIST errors', async () => {
    // Force EXDEV on link AND make copyFile throw EACCES
    forceEXDEV();
    const origCopy = fsp.copyFile;
    try {
      // Override copyFile to throw EACCES
      fsp.copyFile = async () => {
        throw Object.assign(new Error('permission denied'), { code: 'EACCES' });
      };
      const src = path.join(dir, 'exdev-error-src.txt');
      const dest = path.join(dir, 'exdev-error-dest.txt');
      fs.writeFileSync(src, 'error test');
      await assert.rejects(
        () => noClobberRename(src, dest),
        /permission denied/,
      );
    } finally {
      fsp.copyFile = origCopy;
      restoreLink();
    }
  });
});

// ─── 6. noClobberQuarantine — suffix escalation ───────────────────────────

describe('noClobberQuarantine suffix escalation', () => {
  let dir;
  before(() => { dir = tmpDir('quar-escalation'); });
  after(async () => { await rmDir(dir); });

  it('first conflict produces .conflict (no numeric suffix)', async () => {
    const src = path.join(dir, 'file-a.jsonl.open');
    fs.mkdirSync(path.dirname(src), { recursive: true });
    fs.writeFileSync(src, 'first');
    const qdir = path.join(dir, '_quarantine');
    const result = await noClobberQuarantine(src, qdir);
    assert.strictEqual(result.ok, true);
    assert.ok(result.dest.endsWith('.conflict'), `expected .conflict suffix, got: ${result.dest}`);
    assert.ok(fs.existsSync(result.dest));
    assert.strictEqual(fs.readFileSync(result.dest, 'utf-8'), 'first');
  });

  it('second conflict produces .conflict.1', async () => {
    const baseName = 'file-b.jsonl.open';
    const src = path.join(dir, baseName);
    fs.writeFileSync(src, 'second');
    const qdir = path.join(dir, '_quarantine');
    // Pre-occupy .conflict
    const firstConflict = path.join(qdir, `${baseName}.conflict`);
    fs.mkdirSync(qdir, { recursive: true });
    fs.writeFileSync(firstConflict, 'first occupant');
    const result = await noClobberQuarantine(src, qdir);
    assert.strictEqual(result.ok, true);
    assert.ok(result.dest.endsWith('.conflict.1'), `expected .conflict.1, got: ${result.dest}`);
    assert.strictEqual(fs.readFileSync(result.dest, 'utf-8'), 'second');
    // First occupant still intact
    assert.strictEqual(fs.readFileSync(firstConflict, 'utf-8'), 'first occupant');
  });

  it('can escalate to double-digit suffix', async () => {
    const baseName = 'file-c.jsonl.open';
    const src = path.join(dir, baseName);
    fs.writeFileSync(src, 'triple digit test');
    const qdir = path.join(dir, '_quarantine');
    fs.mkdirSync(qdir, { recursive: true });
    // Pre-occupy .conflict through .conflict.9
    for (let i = 0; i <= 9; i++) {
      const p = i === 0
        ? path.join(qdir, `${baseName}.conflict`)
        : path.join(qdir, `${baseName}.conflict.${i}`);
      fs.writeFileSync(p, 'occupied');
    }
    const result = await noClobberQuarantine(src, qdir);
    assert.strictEqual(result.ok, true);
    assert.ok(result.dest.endsWith('.conflict.10'), `expected .conflict.10, got: ${result.dest}`);
  });

  it('stops at MAX_QUARANTINE_ATTEMPTS (100) and throws', async () => {
    const baseName = 'file-d.jsonl.open';
    const src = path.join(dir, baseName);
    fs.writeFileSync(src, 'overflow');
    const qdir = path.join(dir, '_quarantine');
    fs.mkdirSync(qdir, { recursive: true });
    // Occupy .conflict through .conflict.99
    for (let i = 0; i <= 99; i++) {
      const p = i === 0
        ? path.join(qdir, `${baseName}.conflict`)
        : path.join(qdir, `${baseName}.conflict.${i}`);
      fs.writeFileSync(p, 'occupied');
    }
    await assert.rejects(
      () => noClobberQuarantine(src, qdir),
      /exceeded.*100/,
    );
  });
});

// ─── 6. RawRotationWriter — basic flow ─────────────────────────────────────

describe('RawRotationWriter basic flow', () => {
  let dir;

  before(() => { dir = tmpDir('basic-flow'); });
  after(async () => { await rmDir(dir); });

  it('writes to .open file and advances watermark on finalize', async () => {
    const writer = new RawRotationWriter(dir, 'binance_spot', 'trades', {
      flushIntervalMs: 50,
    });

    // Use ms-range timestamps (>1e11) so normalizeTimestampMs doesn't treat as seconds
    const base = 1690000000000; // unambiguous ms timestamp
    const wsCurrent = windowStartMs(base);       // e.g. 1690000000000 rounded to 30000
    const wsPrevious = wsCurrent - 30000;

    await writer.write({ price: 100 }, wsCurrent);  // current window
    await writer.write({ price: 99 }, wsPrevious);  // previous window

    // Finalize all
    await writer.finalize();

    // Watermark should be set to the higher window
    assert.strictEqual(writer.getWatermark(), wsCurrent);

    // .jsonl should exist for both windows
    const { dateDir: d1, fileBase: f1 } = windowStartToDateStr(wsPrevious);
    const { dateDir: d2, fileBase: f2 } = windowStartToDateStr(wsCurrent);
    const jsonl1 = path.join(dir, 'trades', 'binance_spot', d1, `${f1}.jsonl`);
    const jsonl2 = path.join(dir, 'trades', 'binance_spot', d2, `${f2}.jsonl`);
    assert.ok(fs.existsSync(jsonl1), `expected ${jsonl1}`);
    assert.ok(fs.existsSync(jsonl2), `expected ${jsonl2}`);
  });

  it('drops events with window <= watermark', async () => {
    const writer = new RawRotationWriter(dir, 'binance_spot', 'trades', {
      flushIntervalMs: 50,
    });

    // Recover: watermark should be set from previous test's .jsonl files
    await writer.startupRecovery(Date.now());
    const wm = writer.getWatermark();
    assert.ok(wm !== null, 'watermark should be restored from previous test');

    // Try to write to an earlier window — should be dropped (<= watermark)
    await writer.write({ late: true }, wm - 30000);
    // No error thrown — just silently dropped

    await writer.finalize();
  });

  it('drops events with window > current wall-clock window', async () => {
    const writer = new RawRotationWriter(dir, 'coinbase_spot', 'trades', {
      flushIntervalMs: 50,
    });

    // Wall clock is now, but event timestamp maps to far future
    const futureTs = Date.now() + 120000; // 2 min in future → next window
    await writer.write({ future: true }, futureTs);
    // Should be silently dropped

    await writer.finalize();
  });

  it('drops events with invalid timestamps', async () => {
    const writer = new RawRotationWriter(dir, 'kraken_spot', 'trades', {
      flushIntervalMs: 50,
    });
    await writer.write({ bad: true }, 'not-a-number');
    await writer.write({ bad: true }, NaN);
    await writer.write({ bad: true }, Infinity);
    await writer.finalize();
  });
});

// ─── 7. Startup recovery ────────────────────────────────────────────────────

describe('Startup recovery', () => {
  let dir;

  before(() => { dir = tmpDir('recovery'); });
  after(async () => { await rmDir(dir); });

  it('restores watermark from existing .jsonl files', async () => {
    // Create finalized .jsonl for window 0 and 30000
    for (const ws of [0, 30000]) {
      const { dateDir, fileBase } = windowStartToDateStr(ws);
      const d = path.join(dir, 'trades', 'binance_spot', dateDir);
      fs.mkdirSync(d, { recursive: true });
      fs.writeFileSync(path.join(d, `${fileBase}.jsonl`), `{"window":${ws}}\n`);
    }

    const writer = new RawRotationWriter(dir, 'binance_spot', 'trades');
    await writer.startupRecovery(Date.now());
    assert.strictEqual(writer.getWatermark(), 30000);
    await writer.finalize();
  });

  it('retains .open within keepable range (current + previous)', async () => {
    const nowMs = Date.now();
    const currentWindow = windowStartMs(nowMs);
    const prevWindow = currentWindow - 30000;

    for (const ws of [prevWindow, currentWindow]) {
      const { dateDir, fileBase } = windowStartToDateStr(ws);
      const d = path.join(dir, 'trades', 'okx_spot', dateDir);
      fs.mkdirSync(d, { recursive: true });
      fs.writeFileSync(path.join(d, `${fileBase}.jsonl.open`), `{"window":${ws}}\n`);
    }

    const writer = new RawRotationWriter(dir, 'okx_spot', 'trades');
    await writer.startupRecovery(nowMs);
    assert.strictEqual(writer.getCurrentWindowMs(), currentWindow);
    await writer.finalize();
  });

  it('finalizes .open beyond keepable range', async () => {
    // Window 0 is definitely beyond keepable
    const { dateDir, fileBase } = windowStartToDateStr(0);
    const d = path.join(dir, 'trades', 'bybit_perp', dateDir);
    fs.mkdirSync(d, { recursive: true });
    const openPath = path.join(d, `${fileBase}.jsonl.open`);
    fs.writeFileSync(openPath, '{"old":true}\n');

    const writer = new RawRotationWriter(dir, 'bybit_perp', 'trades');
    await writer.startupRecovery(Date.now());
    assert.ok(writer.getWatermark() !== null);
    assert.ok(writer.getWatermark() >= 0);
    assert.ok(!fs.existsSync(openPath), '.open should be gone');
    const jsonlPath = path.join(d, `${fileBase}.jsonl`);
    assert.ok(fs.existsSync(jsonlPath), '.jsonl should exist');
    await writer.finalize();
  });

  it('quarantines future .open files', async () => {
    const futureWs = Date.now() + 300000; // 5 min in future
    const { dateDir, fileBase } = windowStartToDateStr(futureWs);
    const d = path.join(dir, 'trades', 'bitmex_perp', dateDir);
    fs.mkdirSync(d, { recursive: true });
    const openPath = path.join(d, `${fileBase}.jsonl.open`);
    fs.writeFileSync(openPath, '{"future":true}\n');

    const writer = new RawRotationWriter(dir, 'bitmex_perp', 'trades');
    await writer.startupRecovery(Date.now());
    assert.ok(!fs.existsSync(openPath), 'future .open should be quarantined');
    await writer.finalize();
  });

  it('handles non-existent directories gracefully', async () => {
    const writer = new RawRotationWriter(path.join(dir, 'nonexistent'), 'ghost', 'trades');
    await writer.startupRecovery(Date.now());
    assert.strictEqual(writer.getWatermark(), null);
    assert.strictEqual(writer.getCurrentWindowMs(), null);
    await writer.finalize();
  });
});

// ─── FIX4: raw I/O error propagation ────────────────────────────────────────

describe('FIX4 raw I/O error propagation', () => {
  let dir;

  before(() => { dir = tmpDir('fix4'); });
  after(async () => { await rmDir(dir); });

  /** 指定したメソッドで throw する擬似 writer を返すファクトリ */
  function createFailingWriter(failOn = 'write') {
    return {
      _filePath: '/tmp/fake-path',
      async write() {
        if (failOn === 'write' || failOn === 'all') throw new Error('injected: write failed');
      },
      async flush() {
        if (failOn === 'flush' || failOn === 'all') throw new Error('injected: flush failed');
      },
      async close() {
        if (failOn === 'close' || failOn === 'all') throw new Error('injected: close failed');
      },
    };
  }

  afterEach(() => {
    _setTestMakeWriterFn(null);
  });

  it('F4-1: write() エラーで errorCount が増加する', async () => {
    _setTestMakeWriterFn(() => createFailingWriter('write'));
    const writer = new RawRotationWriter(dir, 'market_a', 'trades', { flushIntervalMs: 50 });

    assert.strictEqual(writer.getWriteErrorCount(), 0, '初期状態は 0');
    // 最初の write で _createWriter → 失敗 writer が使われ、write が throw する
    await writer.write({ price: 100 }, Date.now());
    assert.strictEqual(writer.getWriteErrorCount(), 1, 'write エラー後は 1');

    const lastErr = writer.getLastWriteError();
    assert.ok(lastErr !== null, 'lastError がセットされている');
    assert.ok(lastErr.message.includes('injected'), `メッセージに "injected" を含む: ${lastErr.message}`);
    assert.ok(typeof lastErr.at === 'number', `at は数値: ${typeof lastErr.at}`);

    await writer.finalize();
  });

  it('F4-2: 複数エラーが累積する', async () => {
    _setTestMakeWriterFn(() => createFailingWriter('write'));
    const writer = new RawRotationWriter(dir, 'market_b', 'trades', { flushIntervalMs: 50 });

    await writer.write({ a: 1 }, Date.now());
    await writer.write({ a: 2 }, Date.now() + 1);
    await writer.write({ a: 3 }, Date.now() + 2);
    assert.strictEqual(writer.getWriteErrorCount(), 3, '3回のエラーが累積');

    await writer.finalize();
  });

  it('F4-3: finalize() エラーも errorCount にカウントされる', async () => {
    // write は成功させるが finalize 時の close で失敗させる
    _setTestMakeWriterFn(() => createFailingWriter('close'));
    const writer = new RawRotationWriter(dir, 'market_c', 'trades', { flushIntervalMs: 50 });

    // 最初の write で writer 作成、write は成功（failOn='close'）
    await writer.write({ price: 200 }, Date.now());
    assert.strictEqual(writer.getWriteErrorCount(), 0, 'write 段階ではエラーなし');

    // finalize で close が throw → エラーカウント増加
    await writer.finalize();
    assert.strictEqual(writer.getWriteErrorCount(), 1, 'finalize エラー後は 1');
  });

  it('F4-4: checkStale() エラーも errorCount にカウントされる', async () => {
    // flush で失敗させる writer
    _setTestMakeWriterFn(() => createFailingWriter('flush'));
    const writer = new RawRotationWriter(dir, 'market_d', 'trades', { flushIntervalMs: 50 });

    // まず write で writer を生成（flush は呼ばれない）
    await writer.write({ price: 300 }, Date.now());
    assert.strictEqual(writer.getWriteErrorCount(), 0, 'write 段階ではエラーなし');

    // far-future で checkStale → 60s 条件を満たし _finalizeWriter → flush() が throw
    await writer.checkStale(Date.now() + 120_000);
    assert.strictEqual(writer.getWriteErrorCount(), 1, 'checkStale エラー後は 1');
  });

  it('F4-5: clearWriteError() でリセットされる', async () => {
    _setTestMakeWriterFn(() => createFailingWriter('write'));
    const writer = new RawRotationWriter(dir, 'market_e', 'trades', { flushIntervalMs: 50 });

    await writer.write({ price: 400 }, Date.now());
    assert.strictEqual(writer.getWriteErrorCount(), 1);

    writer.clearWriteError();
    assert.strictEqual(writer.getWriteErrorCount(), 0, 'clear 後は 0');
    assert.strictEqual(writer.getLastWriteError(), null, 'lastError も null');

    await writer.finalize();
  });

  it('F4-6: エラー後も後続の write はキューを継続する（未処理例外なし）', async () => {
    _setTestMakeWriterFn(() => createFailingWriter('write'));
    const writer = new RawRotationWriter(dir, 'market_f', 'trades', { flushIntervalMs: 50 });

    // 連続 write — すべて失敗するが未処理例外にならないことを確認
    await writer.write({ x: 1 }, Date.now());
    await writer.write({ x: 2 }, Date.now() + 1);
    // ここに到達 = 未処理例外なし
    assert.ok(true, '未処理例外なく継続');
    assert.strictEqual(writer.getWriteErrorCount(), 2);

    await writer.finalize();
  });
});
