// lib/health-monitor.mjs — Health state monitor for btc-receiver v3.00

import { BufferedWriter } from './buffered-writer.mjs';
import fsp from 'node:fs/promises';
import crypto from 'node:crypto';
import path from 'node:path';

export const HEALTH_ROTATE_BYTES = 64 * 1024 * 1024;
export const HEALTH_ROTATE_GENERATIONS = 2;

/**
 * Simplified health monitor for Phase 1.
 * Tracks connector states and writes health.jsonl.
 */
export class HealthMonitor {
  /**
   * @param {string} outputPath
   * @param {Object} [options]
   * @param {number} [options.intervalMs=1000]
   * @param {number} [options.rotateBytes=HEALTH_ROTATE_BYTES] testable byte limit
   */
  constructor(outputPath, options = {}) {
    this._outputPath = path.resolve(outputPath);
    this._intervalMs = options.intervalMs ?? 1000;
    this._rotateBytes = options.rotateBytes ?? HEALTH_ROTATE_BYTES;
    /** @type {Map<string, import('./events.mjs').ConnectorStats>} */
    this._connectorStats = new Map();
    /** @type {Map<string, { count: number, message: string|null }>} */
    this._writerIoFailures = new Map();
    this._writer = new BufferedWriter(outputPath, {
      flushIntervalMs: 1000,
      maxBufferLines: 100,
      maxLossMs: 30000,
    });
    this._timer = null;
    this._closed = false;
    this._tickPromise = Promise.resolve();
    this._startTime = Date.now();
  }

  /** Register or update connector stats. */
  updateConnector(market, stats) {
    this._connectorStats.set(market, { ...stats });
  }

  /**
   * Register or clear writer I/O failure status for a market.
   *
   * @param {string} market
   * @param {{ count: number, message: string|null }} ioFailure
   */
  updateWriterHealth(market, ioFailure) {
    if (ioFailure.count > 0) {
      this._writerIoFailures.set(market, { ...ioFailure });
    } else {
      this._writerIoFailures.delete(market);
    }
  }

  /** Start periodic writing. */
  start() {
    if (this._timer) return;
    this._timer = setInterval(() => this._tick(), this._intervalMs);
    if (this._timer.unref) this._timer.unref();
  }

  stop() {
    if (this._timer) {
      clearInterval(this._timer);
      this._timer = null;
    }
  }

  async close() {
    this.stop();
    await this._tickPromise;
    this._closed = true;
    await this._writer.close();
    await this._writeManifest();
  }

  /** @returns {Object} */
  getHealthSummary() {
    const markets = {};
    for (const [market, stats] of this._connectorStats) {
      markets[market] = {
        state: stats.state,
        connectedAt: stats.connectedAt,
        lastDepthMsgAt: stats.lastDepthMsgAt,
        lastTradeMsgAt: stats.lastTradeMsgAt,
        depthMsgCount: stats.depthMsgCount,
        tradeMsgCount: stats.tradeMsgCount,
        droppedDepthCount: stats.droppedDepthCount,
        droppedTradeCount: stats.droppedTradeCount,
        droppedLiquidationCount: stats.droppedLiquidationCount,
        reconnectCount: stats.reconnectCount,
        resyncCount: stats.resyncCount,
        lastSeq: stats.lastSeq,
      };
    }

    // Merge writer I/O failure info into market state
    for (const [market, ioF] of this._writerIoFailures) {
      if (markets[market]) {
        markets[market].ioFailure = { count: ioF.count, message: ioF.message };
      } else {
        markets[market] = { ioFailure: { count: ioF.count, message: ioF.message } };
      }
    }

    const states = Object.values(markets).map(m => m.state).filter(Boolean);
    let overallState = 'normal';
    if (states.some(s => s === 'error' || s === 'reconnecting')) {
      overallState = 'critical';
    } else if (Object.values(markets).some(m => m.ioFailure)) {
      overallState = 'critical';
    } else if (states.some(s => s !== 'running')) {
      overallState = 'warning';
    }

    return {
      ts: Date.now(),
      state: overallState,
      markets,
    };
  }

  _tick() {
    if (this._closed) return;
    const report = this.getHealthSummary();
    this._tickPromise = this._tickPromise
      .then(async () => {
        await this._writer.flush();
        await this._rotateIfNeeded();
        await this._writer.write(report);
        await this._writer.flush();
      })
      .catch(error => console.error(`[HealthMonitor] ${error.message}`));
  }

  async _rotateIfNeeded() {
    let size;
    try { size = (await fsp.stat(this._outputPath)).size; } catch { return; }
    if (size < this._rotateBytes) return;

    await this._writer.close();
    const previous = `${this._outputPath}.1`;
    try { await fsp.rename(this._outputPath, previous); } catch (error) {
      console.error(`[HealthMonitor] rotation failed: ${error.message}`);
      this._writer = new BufferedWriter(this._outputPath, {
        flushIntervalMs: 1000, maxBufferLines: 100, maxLossMs: 30000,
      });
      return;
    }
    this._writer = new BufferedWriter(this._outputPath, {
      flushIntervalMs: 1000, maxBufferLines: 100, maxLossMs: 30000,
    });
    await this._writeManifest();
  }

  async _writeManifest() {
    const files = [];
    for (const filePath of [this._outputPath, `${this._outputPath}.1`]) {
      let stat;
      try { stat = await fsp.stat(filePath); } catch { continue; }
      const hash = crypto.createHash('sha256');
      let rows = 0;
      const content = await fsp.readFile(filePath);
      for (const line of content.toString('utf8').split('\n')) {
        if (line.trim()) { JSON.parse(line); rows++; }
      }
      hash.update(content);
      files.push({ file: filePath, bytes: stat.size, rows, sha256: hash.digest('hex') });
    }
    const manifest = {
      schema_version: 'health_generation_manifest_v1',
      rotate_bytes: this._rotateBytes,
      generations: HEALTH_ROTATE_GENERATIONS,
      updated_at: new Date().toISOString(),
      files,
    };
    const manifestPath = `${this._outputPath}.manifest.json`;
    const tempPath = `${manifestPath}.tmp-${process.pid}`;
    await fsp.writeFile(tempPath, `${JSON.stringify(manifest, null, 2)}\n`);
    await fsp.rename(tempPath, manifestPath);
  }
}
