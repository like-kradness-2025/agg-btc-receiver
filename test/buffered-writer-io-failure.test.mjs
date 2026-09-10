// test/buffered-writer-io-failure.test.mjs — R-05 regression tests
//
// Round-3 audit R-05 (P1): BufferedWriter swallowed flush failures.  Before the
// fix a blocked destination path (EISDIR) produced a console error while
// flush()/close() reported success, getIoFailure() did not exist, and buffered
// lines were dropped with no counter — silent data loss, including for
// raw-rotation-writer whose health report (getIoFailure) stayed count:0.
//
// These tests pin the fixed contract:
//   - flush() rejects on I/O failure and records it (getIoFailure().count > 0)
//   - close() rejects instead of silently discarding buffered lines
//   - write() after close is counted (droppedAfterClose) instead of a no-op
//   - the buffer survives the failure so a later flush persists it
//   - RawRotationWriter surfaces the buffered failure through getIoFailure()

import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import os from 'node:os';
import { BufferedWriter } from '../lib/buffered-writer.mjs';
import { RawRotationWriter } from '../lib/raw-rotation-writer.mjs';

function tmpDir(name) {
  const dir = path.join(os.tmpdir(), `bw-io-${process.pid}-${name}-${Date.now()}`);
  fs.mkdirSync(dir, { recursive: true });
  return dir;
}

/** Occupy the destination file path with a directory so any open() fails. */
function blockPath(filePath) {
  fs.mkdirSync(filePath, { recursive: true });
}

function unblockPath(filePath) {
  fs.rmSync(filePath, { recursive: true, force: true });
}

describe('BufferedWriter I/O failure visibility (R-05)', () => {
  it('records a failed flush and rejects instead of reporting success', async () => {
    const dir = tmpDir('flush');
    const filePath = path.join(dir, 'out.jsonl');
    blockPath(filePath);
    const w = new BufferedWriter(filePath, { autoFlush: false });
    try {
      await w.write({ a: 1 });

      await assert.rejects(() => w.flush(), /EISDIR|illegal|directory/i);

      const stats = w.getStats();
      assert.equal(stats.pendingFlushes, 1, 'buffered line must survive the failure');
      assert.equal(stats.totalFlushes, 0);
      const io = w.getIoFailure();
      assert.ok(io.count > 0, `getIoFailure().count must be > 0 (got ${io.count})`);
      assert.match(String(io.message), /EISDIR|illegal|directory/i);
    } finally {
      unblockPath(filePath);
      await w.close().catch(() => {});
      fs.rmSync(dir, { recursive: true, force: true });
    }
  });

  it('close() rejects and keeps the buffer when lines cannot be flushed', async () => {
    const dir = tmpDir('close');
    const filePath = path.join(dir, 'out.jsonl');
    blockPath(filePath);
    const w = new BufferedWriter(filePath, { autoFlush: false });
    try {
      await w.write({ hello: 'world' });

      await assert.rejects(() => w.close(), /could not flush 1 buffered line/i);

      assert.equal(w.getStats().pendingFlushes, 1, 'close must not discard buffered lines');
      assert.equal(w.getStats().closeFailures, 1);
      assert.ok(w.getIoFailure().count > 0);

      // A failed close keeps the writer usable: the retained line is still
      // buffered, and further writes are accepted rather than dropped.
      await w.write({ second: true });
      assert.equal(w.getStats().pendingFlushes, 2);
      assert.equal(w.getStats().droppedAfterClose, 0);
    } finally {
      unblockPath(filePath);
      await w.close().catch(() => {});
      fs.rmSync(dir, { recursive: true, force: true });
    }
  });

  it('persists the retained buffer once the destination is writable again', async () => {
    const dir = tmpDir('recover');
    const filePath = path.join(dir, 'out.jsonl');
    blockPath(filePath);
    const w = new BufferedWriter(filePath, { autoFlush: false });
    await w.write({ a: 1 });
    await assert.rejects(() => w.close());

    unblockPath(filePath);
    await w.close();

    const lines = fs.readFileSync(filePath, 'utf-8').trim().split('\n');
    assert.equal(lines.length, 1, 'the failed-close buffer must still be written');
    assert.deepEqual(JSON.parse(lines[0]), { a: 1 });
    assert.equal(w.getStats().pendingFlushes, 0);
    fs.rmSync(dir, { recursive: true, force: true });
  });

  it('write after a successful close is counted as a drop (not silent)', async () => {
    const dir = tmpDir('afterclose');
    const filePath = path.join(dir, 'out.jsonl');
    const w = new BufferedWriter(filePath, { autoFlush: false });
    await w.write({ a: 1 });
    await w.close();

    await w.write({ b: 2 });
    assert.equal(w.getStats().droppedAfterClose, 1);
    assert.ok(w.getIoFailure().count > 0, 'a dropped line must be observable');
    fs.rmSync(dir, { recursive: true, force: true });
  });
});

describe('RawRotationWriter surfaces buffered I/O failures (R-05)', () => {
  it('reports getIoFailure().count > 0 when the window file cannot be written', async () => {
    const dir = tmpDir('rotation');
    const base = path.join(dir, 'raw');
    const rw = new RawRotationWriter(base, 'binance_spot', 'trades', { flushIntervalMs: 10 });
    try {
      await rw.write({ x: 1 }, Date.now());
      const openPath = rw._currentWriter._filePath;
      unblockPath(openPath);
      blockPath(openPath);

      await rw.flush();
      await rw.flush();

      const io = rw.getIoFailure();
      assert.ok(io.count > 0, `RawRotationWriter.getIoFailure().count must be > 0 (got ${io.count})`);
      assert.match(String(io.message), /EISDIR|illegal|directory|buffered writer/i);

      // The buffered line is retained, so clearing the obstruction still persists it.
      unblockPath(openPath);
      await rw.flush();
      const written = fs.readFileSync(openPath, 'utf-8').trim().split('\n');
      assert.deepEqual(JSON.parse(written[0]), { x: 1 });
    } finally {
      unblockPath(path.join(dir, 'raw'));
      fs.rmSync(dir, { recursive: true, force: true });
    }
  });
});
