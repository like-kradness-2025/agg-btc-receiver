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
    /** PR #20 re-audit: JSON key of the completeness_transitions written on the last row (change-only emission). */
    this._lastTransitionsKey = null;
    /**
     * Issue #16 aggregate completeness view, pushed by the main thread via
     * setCompleteness(). Merged into every report so health.jsonl separates
     * per-market running state from aggregate data completeness.
     * @type {null | {expected_markets: string[], running_markets: string[], degraded_markets: Object<string,string>, data_complete: boolean|null, transitions: Array<Object>}}
     */
    this._completeness = null;
  }

  /** Register or update connector stats. */
  updateConnector(market, stats) {
    this._connectorStats.set(market, { ...stats });
  }

  /** Issue #16: store the latest aggregate completeness view (see market-status.mjs). */
  setCompleteness(summary) {
    this._completeness = summary
      ? {
        expected_markets: [...(summary.expected_markets ?? [])],
        running_markets: [...(summary.running_markets ?? [])],
        degraded_markets: { ...(summary.degraded_markets ?? {}) },
        data_complete: summary.data_complete ?? null,
        transitions: (summary.transitions ?? []).slice(-50),
      }
      : null;
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

    // Issue #16: merge the aggregate completeness view. Degraded markets are
    // authoritative over a stale per-connector 'running' stats snapshot: a
    // degraded market is NOT streaming data even if the last stats tick said
    // running moments before the isolation.
    const completeness = this._completeness;
    if (completeness) {
      for (const [market, reason] of Object.entries(completeness.degraded_markets ?? {})) {
        if (!markets[market]) markets[market] = {};
        markets[market].degraded_reason = reason;
        markets[market].state = 'degraded';
      }
    }
    // Isolated/degraded markets keep the receiver alive but the aggregate is
    // incomplete — never present that as a 'normal' state. data_complete is
    // authoritative once a completeness view was pushed: `false` (or not yet
    // true) with NO degraded market — e.g. a required market still
    // 'unknown'/'reconnecting' before its first running report — must also
    // surface as warning. 'normal' + data_complete=false is self-contradictory.
    if (overallState === 'normal' && completeness
      && (Object.keys(completeness.degraded_markets ?? {}).length > 0 || completeness.data_complete !== true)) {
      overallState = 'warning';
    }

    return {
      ts: Date.now(),
      state: overallState,
      expected_markets: completeness?.expected_markets ?? [],
      running_markets: completeness?.running_markets ?? [],
      degraded_markets: completeness?.degraded_markets ?? {},
      data_complete: completeness?.data_complete ?? null,
      completeness_transitions: completeness?.transitions ?? [],
      markets,
    };
  }

  _tick() {
    if (this._closed) return;
    const report = this.getHealthSummary();
    // PR #20 re-audit: completeness_transitions can carry up to 50 entries; writing
    // the full history on EVERY row bloats health.jsonl. Emit the list only
    // when it changed since the previous row ([] = no new transition since
    // the last report) — the field stays stable for parsers while rows stay
    // small between actual state transitions.
    const transitionsKey = JSON.stringify(report.completeness_transitions ?? []);
    if (transitionsKey === this._lastTransitionsKey) report.completeness_transitions = [];
    else this._lastTransitionsKey = transitionsKey;
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
