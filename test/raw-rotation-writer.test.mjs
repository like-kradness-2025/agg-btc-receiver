// test/raw-rotation-writer.test.mjs — RawRotationWriter unit tests
// Aligned to lib/raw-rotation-writer.mjs API

import { describe, it, before, after } from 'node:test';
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

// ─── 5. noClobberQuarantine ─────────────────────────────────────────────────

describe('noClobberQuarantine', () => {
  let dir;
  before(() => { dir = tmpDir('quarantine'); });
  after(async () => { await rmDir(dir); });

  it('moves file to quarantine with suffix', async () => {
    const src = path.join(dir, '12-00-00.jsonl.open');
    fs.mkdirSync(path.dirname(src), { recursive: true });
    fs.writeFileSync(src, 'quarantine test');
    const qdir = path.join(dir, '_quarantine');
    const result = await noClobberQuarantine(src, qdir);
    assert.strictEqual(result.ok, true);
    assert.ok(fs.existsSync(result.dest));
    assert.ok(!fs.existsSync(src));
    assert.strictEqual(fs.readFileSync(result.dest, 'utf-8'), 'quarantine test');
  });

  it('generates unique name when quarantine dest exists', async () => {
    const src = path.join(dir, '12-00-30.jsonl.open');
    fs.mkdirSync(path.dirname(src), { recursive: true });
    fs.writeFileSync(src, 'second quarantine');
    const qdir = path.join(dir, '_quarantine');
    // Pre-create the first conflict file
    const firstDest = path.join(qdir, '12-00-30.jsonl.open.conflict');
    fs.mkdirSync(qdir, { recursive: true });
    fs.writeFileSync(firstDest, 'existing');
    const result = await noClobberQuarantine(src, qdir);
    assert.strictEqual(result.ok, true);
    assert.notStrictEqual(result.dest, firstDest);
    assert.ok(fs.existsSync(result.dest));
    assert.ok(!fs.existsSync(src));
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

  it('writes a batch in order without one queue task per event', async () => {
    const writer = new RawRotationWriter(dir, 'batch_flow', 'trades');
    const ts = Date.now();
    const events = Array.from({ length: 100 }, (_, i) => [{ i }, ts]);

    await writer.writeBatch(events);
    await writer.finalize();

    const { dateDir, fileBase } = windowStartToDateStr(windowStartMs(ts));
    const file = path.join(dir, 'trades', 'batch_flow', dateDir, `${fileBase}.jsonl`);
    const rows = fs.readFileSync(file, 'utf8').trim().split('\n').map(JSON.parse);
    assert.equal(rows.length, 100);
    assert.deepEqual(rows.map((row) => row.i), [...Array(100).keys()]);
  });

  it('flushes active windows without finalizing them', async () => {
    const writer = new RawRotationWriter(dir, 'flush_probe', 'trades');
    const ts = Date.now();
    const { dateDir, fileBase } = windowStartToDateStr(windowStartMs(ts));
    const openPath = path.join(
      dir, 'trades', 'flush_probe', dateDir, `${fileBase}.jsonl.open`,
    );

    await writer.write({ probe: true }, ts);
    assert.ok(!fs.existsSync(openPath), 'line should still be buffered before flush');

    await writer.flush();

    assert.ok(fs.existsSync(openPath), 'flush should create the active .open file');
    assert.equal(fs.readFileSync(openPath, 'utf8'), '{"probe":true}\n');
    assert.equal(writer.getWatermark(), null, 'flush must not finalize the window');

    await writer.finalize();
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

  it('reopens finalized active windows after restart', async () => {
    const nowMs = Date.now();
    const currentWindow = windowStartMs(nowMs);
    const { dateDir, fileBase } = windowStartToDateStr(currentWindow);
    const d = path.join(dir, 'trades', 'restart_append', dateDir);
    fs.mkdirSync(d, { recursive: true });
    const finalPath = path.join(d, `${fileBase}.jsonl`);
    fs.writeFileSync(finalPath, '{"before_restart":true}\n');

    const writer = new RawRotationWriter(dir, 'restart_append', 'trades');
    await writer.startupRecovery(nowMs);
    assert.strictEqual(writer.getWatermark(), null);
    await writer.write({ after_restart: true }, nowMs);
    await writer.finalize();

    const rows = fs.readFileSync(finalPath, 'utf8').trim().split('\n').map(JSON.parse);
    assert.deepStrictEqual(rows, [
      { before_restart: true },
      { after_restart: true },
    ]);
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

// ─── 8. I/O failure propagation ──────────────────────────────────────────────

describe('I/O failure propagation', () => {
  let dir;

  before(() => { dir = tmpDir('io-fail'); });
  after(async () => { await rmDir(dir); });

  it('records error from write() and exposes via getIoFailure()', async () => {
    const writer = new RawRotationWriter(dir, 'test_market', 'trades', {
      flushIntervalMs: 500,
    });

    // Fresh writer: no failures
    assert.deepStrictEqual(writer.getIoFailure(), { count: 0, message: null });

    // Replace _writeImpl to simulate an I/O failure
    const origImpl = writer._writeImpl.bind(writer);
    writer._writeImpl = async () => { throw new Error('ENOSPC: no space left'); };

    // Write fails internally, error is recorded (method doesn't throw)
    await writer.write({ test: true }, Date.now());

    const fail = writer.getIoFailure();
    assert.strictEqual(fail.count, 1, 'should record 1 I/O failure');
    assert.ok(fail.message.includes('ENOSPC'), `message should mention ENOSPC, got: ${fail.message}`);

    // Restore original impl so finalize works
    writer._writeImpl = origImpl;
    await writer.finalize();
  });

  it('records error from checkStale() via getIoFailure()', async () => {
    const writer = new RawRotationWriter(dir, 'test_market_2', 'book_updates', {
      flushIntervalMs: 500,
    });

    // Set up a valid writer first (write once so rotation happens)
    await writer.write({ init: true }, Date.now());
    const before = writer.getIoFailure();
    assert.strictEqual(before.count, 0, 'no failures before stale check');

    // Replace _checkStaleImpl to simulate failure
    const origStale = writer._checkStaleImpl.bind(writer);
    writer._checkStaleImpl = async () => { throw new Error('EACCES: permission denied'); };

    await writer.checkStale(Date.now());

    const fail = writer.getIoFailure();
    assert.strictEqual(fail.count, 1, 'should record 1 checkStale failure');
    assert.ok(fail.message.includes('EACCES'), `message should mention EACCES, got: ${fail.message}`);

    writer._checkStaleImpl = origStale;
    await writer.finalize();
  });

  it('cascading failures increment count', async () => {
    const writer = new RawRotationWriter(dir, 'test_market_3', 'liquidations', {
      flushIntervalMs: 500,
    });

    const origImpl = writer._writeImpl.bind(writer);
    writer._writeImpl = async () => { throw new Error('EIO: I/O error'); };

    // Three consecutive write failures
    await writer.write({ a: 1 }, Date.now());
    await writer.write({ a: 2 }, Date.now() + 10);
    await writer.write({ a: 3 }, Date.now() + 20);

    const fail = writer.getIoFailure();
    assert.strictEqual(fail.count, 3, 'count should reflect all 3 failures');
    assert.ok(fail.message.includes('EIO'), `message should mention EIO, got: ${fail.message}`);

    writer._writeImpl = origImpl;
    await writer.finalize();
  });

  it('normal writes do not produce false failures', async () => {
    const writer = new RawRotationWriter(dir, 'test_market_4', 'trades', {
      flushIntervalMs: 50,
    });

    const base = Date.now();
    for (let i = 0; i < 5; i++) {
      await writer.write({ idx: i }, base + i * 100);
    }

    await writer.finalize();

    assert.deepStrictEqual(
      writer.getIoFailure(),
      { count: 0, message: null },
      'normal writes should not produce I/O failures',
    );
  });

  it('records error from finalize() when _finalizeWriter throws', async () => {
    const writer = new RawRotationWriter(dir, 'finalize_fail_1', 'trades', {
      flushIntervalMs: 500,
    });

    // Fresh writer: no failures
    assert.deepStrictEqual(writer.getIoFailure(), { count: 0, message: null });

    // Write to create a current writer
    await writer.write({ init: true }, Date.now());

    // Mock _finalizeWriter to throw (simulates flush/close/rename failure)
    const origFinalizeWriter = writer._finalizeWriter.bind(writer);
    writer._finalizeWriter = async () => { throw new Error('EIO: finalize flush failed'); };

    await writer.finalize();

    const fail = writer.getIoFailure();
    assert.strictEqual(fail.count, 1, 'should record 1 finalize I/O failure');
    assert.ok(fail.message.includes('EIO'), `message should mention EIO, got: ${fail.message}`);

    // Restore original — no cleanup finalize needed (tmp dir handles it)
    writer._finalizeWriter = origFinalizeWriter;
  });

  it('records error from finalize() with both writers failing', async () => {
    const writer = new RawRotationWriter(dir, 'finalize_fail_2', 'trades', {
      flushIntervalMs: 500,
    });

    const ts = Date.now();
    const currentWs = windowStartMs(ts);
    const prevWs = currentWs - 30000;

    // Write to current window
    await writer.write({ a: 1 }, currentWs + 100);
    // Write to previous window — triggers late-event path, creates _previousWriter
    await writer.write({ b: 2 }, prevWs + 100);

    // Both writers should now be populated
    assert.ok(writer._previousWriter !== null, 'previous writer should exist');
    assert.ok(writer._currentWriter !== null, 'current writer should exist');

    // Mock _finalizeWriter to throw on both calls
    const origFinalizeWriter = writer._finalizeWriter.bind(writer);
    let callCount = 0;
    writer._finalizeWriter = async () => {
      callCount++;
      throw new Error(`EIO: finalize error #${callCount}`);
    };

    await writer.finalize();

    const fail = writer.getIoFailure();
    assert.strictEqual(
      fail.count, 2,
      'should record 2 I/O failures (one per writer)',
    );
    assert.ok(
      fail.message.includes('error #2'),
      `message should reflect last error, got: ${fail.message}`,
    );

    writer._finalizeWriter = origFinalizeWriter;
  });

  it('finalize does not produce false I/O failures on success', async () => {
    const writer = new RawRotationWriter(dir, 'finalize_success', 'trades', {
      flushIntervalMs: 50,
    });

    await writer.write({ ok: true }, Date.now());
    await writer.finalize();

    assert.deepStrictEqual(
      writer.getIoFailure(),
      { count: 0, message: null },
      'successful finalize should not produce I/O failures',
    );
  });
});
