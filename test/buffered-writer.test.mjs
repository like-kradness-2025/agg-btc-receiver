// test/buffered-writer.test.mjs — BufferedWriter unit tests

import { describe, it, before, after } from 'node:test';
import assert from 'node:assert';
import fs from 'node:fs';
import path from 'node:path';
import os from 'node:os';
import { BufferedWriter } from '../lib/buffered-writer.mjs';

/**
 * Helper: resolve tmpdir within this project (safe cleanup).
 */
function tmpPath(name) {
  const dir = path.join(os.tmpdir(), `btc-receiver-test-${process.pid}`, `bw-${name}-${Date.now()}`);
  fs.mkdirSync(dir, { recursive: true });
  return path.join(dir, 'test.jsonl');
}

describe('BufferedWriter', () => {
  describe('constructor', () => {
    it('should create output directory and open stream', () => {
      const p = tmpPath('construct');
      const w = new BufferedWriter(p, { autoFlush: false });
      assert.strictEqual(w._closed, false);
      assert.ok(fs.existsSync(path.dirname(p)));
      w.close();
    });
  });

  describe('write and flush', () => {
    it('should buffer and flush lines', async () => {
      const p = tmpPath('write');
      const w = new BufferedWriter(p, { autoFlush: false });
      await w.write({ test: 'data', num: 42 });
      assert.strictEqual(w._numLines, 1);
      await w.flush();
      assert.strictEqual(w._numLines, 0);
      await w.close();

      // Read back
      const content = fs.readFileSync(p, 'utf-8');
      const parsed = JSON.parse(content.trim());
      assert.strictEqual(parsed.test, 'data');
      assert.strictEqual(parsed.num, 42);
    });

    it('should flush multiple lines', async () => {
      const p = tmpPath('multi');
      const w = new BufferedWriter(p, { autoFlush: false });
      await w.write({ a: 1 });
      await w.write({ a: 2 });
      await w.write({ a: 3 });
      await w.flush();
      await w.close();

      const lines = fs.readFileSync(p, 'utf-8').trim().split('\n');
      assert.strictEqual(lines.length, 3);
      assert.strictEqual(JSON.parse(lines[0]).a, 1);
      assert.strictEqual(JSON.parse(lines[1]).a, 2);
      assert.strictEqual(JSON.parse(lines[2]).a, 3);
    });

    it('should auto-flush when maxBufferLines reached', async () => {
      const p = tmpPath('autobuf');
      const w = new BufferedWriter(p, { autoFlush: false, maxBufferLines: 2 });
      await w.write({ x: 1 });
      assert.strictEqual(w._numLines, 1);
      await w.write({ x: 2 }); // auto-flush triggered
      assert.strictEqual(w._numLines, 0);
      await w.close();

      const lines = fs.readFileSync(p, 'utf-8').trim().split('\n');
      assert.strictEqual(lines.length, 2);
    });
  });

  describe('close', () => {
    it('should flush on close', async () => {
      const p = tmpPath('close');
      const w = new BufferedWriter(p, { autoFlush: false });
      await w.write({ close: 'test' });
      await w.close();
      assert.strictEqual(w._closed, true);

      const content = fs.readFileSync(p, 'utf-8');
      assert.ok(content.includes('close'));
    });

    it('should be idempotent', async () => {
      const p = tmpPath('idem');
      const w = new BufferedWriter(p, { autoFlush: false });
      await w.close();
      await w.close(); // second close should no-op
      assert.strictEqual(w._closed, true);
    });

    it('should not write after close', async () => {
      const p = tmpPath('writeclose');
      const w = new BufferedWriter(p, { autoFlush: false });
      await w.close();
      await w.write({ should: 'not appear' });
      // File should be empty or non-existent — either is acceptable
      let content = '';
      try { content = fs.readFileSync(p, 'utf-8').trim(); } catch { /* ENOENT means no data written */ }
      assert.strictEqual(content, '');
    });
  });

  describe('write to closed', () => {
    it('should log error but not throw', async () => {
      const p = tmpPath('closedlog');
      const w = new BufferedWriter(p, { autoFlush: false });
      await w.close();
      // Should not throw
      await w.write({ test: 'ok' });
    });
  });

  describe('getStats', () => {
    it('should return correct counters', async () => {
      const p = tmpPath('stats');
      const w = new BufferedWriter(p, { autoFlush: false });
      await w.write({ a: 1 });
      await w.write({ a: 2 });
      await w.flush();

      const stats = w.getStats();
      assert.strictEqual(stats.totalWrites, 2);
      assert.strictEqual(stats.totalFlushes, 1);
      assert.strictEqual(stats.pendingFlushes, 0);
      assert.ok(stats.totalBytes > 0);

      w.close();
    });
  });
});
