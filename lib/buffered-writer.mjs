// lib/buffered-writer.mjs — Buffered JSONL writer with shared pool scheduler
// v3.01: lazy stream open, 1 shared pool timer, idle close for low-volume writers

import fs from 'node:fs';
import fsp from 'node:fs/promises';
import path from 'node:path';

const DEFAULT_FLUSH_INTERVAL_MS = 1000;
const DEFAULT_MAX_BUFFER_LINES = 4096;
const DEFAULT_MAX_LOSS_MS = 1000;
const DEFAULT_POOL_TICK_MS = 1000;
const DEFAULT_IDLE_CLOSE_MS = 30000;

/**
 * Shared scheduler: one interval, flushes only writers with data.
 */
export class BufferedWriterPool {
  constructor(options = {}) {
    this._tickMs = options.tickMs ?? DEFAULT_POOL_TICK_MS;
    this._idleCloseMs = options.idleCloseMs ?? DEFAULT_IDLE_CLOSE_MS;
    this._writers = new Set();
    this._dirty = new Set();
    this._open = new Set();
    this._timer = null;
  }

  register(w) { this._writers.add(w); this._ensureTimer(); }
  unregister(w) { this._writers.delete(w); this._dirty.delete(w); this._open.delete(w); if (!this._writers.size) this._stopTimer(); }
  markDirty(w) { if (this._writers.has(w)) { this._dirty.add(w); this._ensureTimer(); } }
  markClean(w) { this._dirty.delete(w); }
  markOpen(w) { if (this._writers.has(w)) { this._open.add(w); this._ensureTimer(); } }
  markClosed(w) { this._open.delete(w); }

  _ensureTimer() {
    if (this._timer || !this._writers.size) return;
    this._timer = setInterval(() => this._tick().catch(e => console.error('[BufferedWriterPool]', e.message)), this._tickMs);
    if (this._timer.unref) this._timer.unref();
  }

  _stopTimer() {
    if (!this._timer) return;
    clearInterval(this._timer);
    this._timer = null;
  }

  async _tick() {
    const now = Date.now();
    // Flush dirty writers whose flush-due time has passed
    for (const w of [...this._dirty]) {
      if (w._closed || w._closing) { this._dirty.delete(w); continue; }
      if (!w._autoFlush) continue;
      if (w._flushDueAt !== null && now >= w._flushDueAt) {
        // R-05: one writer's I/O failure must not abort the whole tick (the
        // error is recorded on that writer and observable via getIoFailure()).
        try { await w._flushInternal(); } catch { /* recorded on the writer */ }
      }
      if (!w._numLines) this._dirty.delete(w);
    }
    // Close idle streams (30s no activity)
    for (const w of [...this._open]) {
      if (w._closed || w._closing) { this._open.delete(w); continue; }
      if (w._numLines || w._isFlushing) continue;
      if (now - w._lastActivityAt >= this._idleCloseMs) {
        await w._closeStream();
      }
    }
  }
}

export const defaultPool = new BufferedWriterPool();

/**
 * @typedef {Object} BufferedWriterOptions
 * @property {number} [flushIntervalMs=1000]
 * @property {number} [maxBufferLines=4096]
 * @property {number} [maxLossMs=1000]
 * @property {boolean} [autoFlush=true]
 * @property {BufferedWriterPool} [pool]
 * @property {number} [idleCloseMs=30000]
 */
export class BufferedWriter {
  constructor(filePath, options = {}) {
    this._filePath = filePath;
    this._dir = path.dirname(filePath);
    this._stream = null;
    this._lines = [];
    this._numLines = 0;
    this._bufferedBytes = 0;
    this._flushIntervalMs = options.flushIntervalMs ?? DEFAULT_FLUSH_INTERVAL_MS;
    this._maxBufferLines = options.maxBufferLines ?? DEFAULT_MAX_BUFFER_LINES;
    this._maxLossMs = options.maxLossMs ?? DEFAULT_MAX_LOSS_MS;
    this._autoFlush = options.autoFlush ?? true;
    this._pool = options.pool ?? defaultPool;
    this._idleCloseMs = options.idleCloseMs ?? DEFAULT_IDLE_CLOSE_MS;
    this._totalWrites = 0;
    this._totalBytes = 0;
    this._totalFlushes = 0;
    this._backpressureHits = 0;
    this._isFlushing = false;
    this._closed = false;
    this._closing = false;
    this._lastActivityAt = 0;
    this._flushDueAt = null;
    this._drainWaiters = [];
    // R-05: I/O failures must never be swallowed.  Every failed flush (and
    // every write dropped after close) is counted here so callers can observe
    // silent data loss through getIoFailure() — the same shape
    // RawRotationWriter already reports to HealthMonitor.
    this._ioFailureCount = 0;
    this._lastIoErrorMessage = null;
    this._closeFailures = 0;
    this._droppedAfterClose = 0;
    this._lastIoErrorLogAt = 0;
    fs.mkdirSync(this._dir, { recursive: true });
    this._pool.register(this);
  }

  async write(obj) {
    if (this._closed || this._closing) {
      // R-05: a write after close is a dropped line, not a no-op.  Keep the
      // non-throwing contract (callers append fire-and-forget) but make the
      // loss observable instead of silent.
      this._droppedAfterClose++;
      this._recordIoFailure(new Error(`write after close dropped (${this._filePath})`));
      return;
    }
    const line = JSON.stringify(obj) + '\n';
    const wasEmpty = !this._numLines;
    this._lines.push(line);
    this._numLines++;
    this._bufferedBytes += Buffer.byteLength(line, 'utf-8');
    this._totalWrites++;
    this._lastActivityAt = Date.now();
    // Schedule first flush
    if (wasEmpty && this._autoFlush) {
      this._flushDueAt = this._lastActivityAt + Math.min(this._flushIntervalMs, this._maxLossMs);
      this._pool.markDirty(this);
    }
    // Overflow flush.  The failure is already recorded by _flushInternal();
    // write() itself keeps its non-throwing contract so buffered producers
    // (health/derivatives collectors) cannot turn a disk problem into an
    // unhandled rejection.
    if (this._numLines >= this._maxBufferLines) {
      try { await this.flush(); } catch { /* recorded in getIoFailure() */ }
    }
  }

  async flush() {
    if (this._closed || this._closing) return;
    if (this._isFlushing) return;
    if (!this._numLines) return;
    await this._flushInternal();
  }

  async _flushInternal() {
    if (this._isFlushing || !this._numLines || this._closed) return;
    this._isFlushing = true;
    const batch = this._lines;
    const count = this._numLines;
    const bytes = this._bufferedBytes;
    this._lines = [];
    this._numLines = 0;
    this._bufferedBytes = 0;
    this._flushDueAt = null;
    try {
      if (!this._stream || this._stream.destroyed) await this._ensureStream();
      const chunk = batch.join('');
      await new Promise((resolve, reject) => {
        const ok = this._stream.write(chunk, 'utf-8', err => err ? reject(err) : resolve());
        if (!ok) this._backpressureHits++;
      });
      this._totalBytes += bytes;
      this._totalFlushes++;
      this._lastActivityAt = Date.now();
    } catch (err) {
      // Restore batch in front of any concurrent writes
      this._lines = batch.concat(this._lines);
      this._numLines += count;
      this._bufferedBytes += bytes;
      this._flushDueAt = Date.now() + Math.min(this._flushIntervalMs, this._maxLossMs);
      // R-05: record the failure (observable + logged, throttled) and rethrow
      // so flush()/close() callers see it instead of a silent success.
      this._recordIoFailure(err);
      this._logIoFailure(err);
      if (this._autoFlush) this._pool.markDirty(this);
      this._isFlushing = false;
      if (!this._numLines) this._pool.markClean(this);
      throw err;
    } finally {
      this._isFlushing = false;
      if (!this._numLines) this._pool.markClean(this);
    }
  }

  async rotate() {
    if (this._closed) return;
    await this.flush();
    await this._closeStream();
    const ext = path.extname(this._filePath);
    const base = this._filePath.slice(0, -ext.length);
    const ts = new Date().toISOString().replace(/[:.]/g, '-').slice(0, 19);
    await fsp.rename(this._filePath, `${base}_${ts}${ext}`);
    // Reopen lazily
    this._stream = null;
    this._pool.markClosed(this);
    if (this._numLines > 0) await this.flush();
  }

  async close() {
    if (this._closed || this._closing) return;
    this._closing = true;
    this._pool.unregister(this);
    let flushError = null;
    try {
      await this._flushInternal();
    } catch (err) {
      flushError = err;
    }
    if (this._numLines > 0) {
      // R-05: closing must never silently discard buffered lines.  Keep the
      // writer open (buffer + stream intact) so a later flush()/close() can
      // still persist them once the I/O problem clears, and throw so the
      // caller records the failure instead of losing data quietly.
      this._closeFailures++;
      this._closing = false;
      this._pool.register(this);
      if (this._autoFlush) this._pool.markDirty(this);
      this._recordIoFailure(flushError ?? new Error('unflushed lines at close'));
      const lost = this._numLines;
      throw new Error(
        `BufferedWriter close could not flush ${lost} buffered line(s) (${this._filePath}): `
        + `${this._lastIoErrorMessage}`,
      );
    }
    this._closed = true;
    await this._closeStream();
  }

  getStats() {
    return {
      bufferedBytes: this._bufferedBytes,
      pendingFlushes: this._numLines,
      totalWrites: this._totalWrites,
      totalBytes: this._totalBytes,
      totalFlushes: this._totalFlushes,
      backpressureHits: this._backpressureHits,
      streamOpen: !!(this._stream && !this._stream.destroyed),
      // R-05 observability: dropped writes and close failures are data loss
      // and must be visible in stats as well as getIoFailure().
      droppedAfterClose: this._droppedAfterClose,
      closeFailures: this._closeFailures,
    };
  }

  /**
   * R-05: accumulated I/O failure info (same shape as RawRotationWriter's).
   *
   * @returns {{count: number, message: string|null, droppedAfterClose: number, closeFailures: number}}
   */
  getIoFailure() {
    return {
      count: this._ioFailureCount,
      message: this._lastIoErrorMessage,
      droppedAfterClose: this._droppedAfterClose,
      closeFailures: this._closeFailures,
    };
  }

  // ====== Internal ======

  /**
   * R-05: count an I/O failure so it is observable via getIoFailure().
   * @param {Error} err
   */
  _recordIoFailure(err) {
    this._ioFailureCount++;
    this._lastIoErrorMessage = err?.message ?? String(err);
  }

  /**
   * R-05: log a flush failure, throttled per writer (a failing destination
   * would otherwise emit one line per pool tick).
   * @param {Error} err
   */
  _logIoFailure(err) {
    const now = Date.now();
    if (now - this._lastIoErrorLogAt < 5000) return;
    this._lastIoErrorLogAt = now;
    console.error(
      `[BufferedWriter] flush failed (${this._filePath}): ${err?.message ?? err} `
      + `[ioFailures=${this._ioFailureCount}, pending=${this._numLines}]`,
    );
  }

  async _ensureStream() {
    if (this._stream && !this._stream.destroyed) return;
    await fsp.mkdir(this._dir, { recursive: true });
    this._stream = fs.createWriteStream(this._filePath, { flags: 'a', encoding: 'utf-8', highWaterMark: 64 * 1024 });
    this._pool.markOpen(this);
    this._stream.on('drain', () => this._fireDrainWaiters());
    this._stream.on('error', (err) => {
      console.error(`[BufferedWriter] stream error (${this._filePath}):`, err.message);
      this._stream = null;
      this._pool.markClosed(this);
    });
  }

  _closeStream() {
    return new Promise(resolve => {
      const s = this._stream;
      if (!s || s.destroyed) { this._stream = null; this._pool.markClosed(this); resolve(); return; }
      let done = false;
      const fin = () => { if (done) return; done = true; this._stream = null; this._pool.markClosed(this); resolve(); };
      s.end(fin);
      const t = setTimeout(fin, 2000); if (t.unref) t.unref();
    });
  }

  _fireDrainWaiters() {
    const w = this._drainWaiters; this._drainWaiters = [];
    for (const cb of w) cb();
  }
}
