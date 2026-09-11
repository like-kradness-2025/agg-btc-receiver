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
    /**
     * R-13/R14 counted raw-DB losses, armed by the shutdown path only.
     * @type {null | {dropped_events: number, reason: string|null, schema?: string, ts_ms?: number}}
     */
    this._rawDbDrop = null;
    /**
     * R14 envelopes rejected at the pending-queue cap (see noteRawDbPendingOverflow).
     * @type {null | {dropped_events: number, cap_events: number|null, mode: string|null, raw: number, canonical: number, open_interest: number, first_ts_ms: number|null}}
     */
    this._rawDbPendingOverflow = null;
    /**
     * O-02 canonical frames that arrived after the shutdown drain had run
     * (see noteRawDbPostDrainDroppedEvents).
     * @type {null | {dropped_events: number, frames: number, first_ts_ms: number|null, last_ts_ms: number|null}}
     */
    this._rawDbPostDrainDrop = null;
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
    /**
     * R-13: counted raw-DB events that could not be written during the
     * shutdown drain ({schema, ts_ms, dropped_events, reason, queues}) — null
     * when nothing was dropped. Kept so close() can persist one final
     * health.jsonl row: the drop is decided AFTER the last periodic tick, so
     * without that row the loss would not be durable anywhere but the report
     * file.
     * @type {null | {dropped_events: number, reason: string|null}}
     */
    this._rawDbDrop = null;
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

  /**
   * R-13: record counted raw-DB events that the shutdown drain could not
   * write. Non-zero counts make close() emit one final health.jsonl row
   * carrying the count, so a raw-write failure that loses queued events is
   * observable instead of looking like a no-trade interval.
   *
   * @param {number} count dropped event count (> 0 to arm the final row)
   * @param {{schema?: string, ts_ms?: number, reason?: string|null}} [report]
   */
  noteRawDbDroppedEvents(count, report = null) {
    if (!Number.isFinite(count) || count <= 0) return;
    this._rawDbDrop = {
      dropped_events: count,
      reason: report?.reason ?? null,
      ...(report?.schema ? { schema: report.schema } : {}),
      ...(Number.isFinite(report?.ts_ms) ? { ts_ms: report.ts_ms } : {}),
    };
  }

  /**
   * R14: record envelopes that were rejected at the raw-DB pending-queue cap
   * (the queue never accepted them, so they are a loss disjoint from
   * noteRawDbDroppedEvents()). Non-zero counts arm one final health.jsonl row,
   * so a queue overflow is distinguishable from "the exchange sent nothing".
   *
   * @param {number} count overflowed envelope count (> 0 to arm the final row)
   * @param {{cap_events?: number|null, mode?: string|null, raw?: number, canonical?: number, open_interest?: number, first_ts_ms?: number|null}} [detail]
   */
  noteRawDbPendingOverflow(count, detail = null) {
    if (!Number.isFinite(count) || count <= 0) return;
    this._rawDbPendingOverflow = {
      dropped_events: count,
      cap_events: Number.isFinite(detail?.cap_events) ? detail.cap_events : null,
      mode: detail?.mode ?? null,
      raw: Number.isFinite(detail?.raw) ? detail.raw : 0,
      canonical: Number.isFinite(detail?.canonical) ? detail.canonical : 0,
      open_interest: Number.isFinite(detail?.open_interest) ? detail.open_interest : 0,
      first_ts_ms: Number.isFinite(detail?.first_ts_ms) ? detail.first_ts_ms : null,
    };
  }

  /**
   * O-02: record canonical frames that could not be persisted because they
   * arrived after the shutdown drain had already run. They are disjoint from
   * both noteRawDbDroppedEvents() (queued events a drain could not write) and
   * noteRawDbPendingOverflow() (envelopes a queue rejected at its cap): these
   * frames never reached a queue and no drain remains to retry them, which is
   * exactly the population the pre-fix `rawDbFailure` guard dropped silently.
   * Non-zero counts arm one final health.jsonl row.
   *
   * @param {number} count dropped frame count (> 0 to arm the final row)
   * @param {{first_ts_ms?: number|null, last_ts_ms?: number|null}} [detail]
   */
  noteRawDbPostDrainDroppedEvents(count, detail = null) {
    if (!Number.isFinite(count) || count <= 0) return;
    this._rawDbPostDrainDrop = {
      dropped_events: count,
      frames: count,
      first_ts_ms: Number.isFinite(detail?.first_ts_ms) ? detail.first_ts_ms : null,
      last_ts_ms: Number.isFinite(detail?.last_ts_ms) ? detail.last_ts_ms : null,
    };
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
    // R-13/R14/O-02: the shutdown drain (and therefore the counted drop /
    // overflow / post-drain arrival) happens AFTER the last periodic tick, so a
    // noted loss needs one explicit final row to be durable in health.jsonl.
    // Only emitted when something was lost — the normal path keeps its previous
    // row counts.
    if (this._rawDbDrop || this._rawDbPendingOverflow || this._rawDbPostDrainDrop) {
      try {
        await this._writeReport(this.getHealthSummary());
      } catch (error) {
        console.error(`[HealthMonitor] final drop report row failed: ${error.message}`);
      }
    }
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
      // R-13/R14/O-02 raw-DB loss accounting. These three `*_events` fields are
      // the CANONICAL loss surface (O-03): they are the only place all three
      // populations appear together, and they are what a consumer must read.
      // `raw-db-drop-report.json` mirrors the first two under different names
      // (`dropped_events`, `pending_queue_overflow_events`) and cannot express
      // the third — never sum the row's fields together with the report's
      // fields, or the mirrored populations are counted twice. Total them with
      // sumRawDbLoss() from lib/raw-db-pending.mjs (the registry
      // RAW_DB_LOSS_POPULATIONS maps each population to both field names).
      //
      // R-13: 0 on every normal row; > 0 only on the final row after a
      // shutdown drain lost queued raw events (raw_db_drop carries the why).
      raw_db_dropped_events: this._rawDbDrop?.dropped_events ?? 0,
      ...(this._rawDbDrop ? { raw_db_drop: { ...this._rawDbDrop } } : {}),
      // R14: 0 on every normal row; > 0 on the final row when the pending queue
      // rejected envelopes at the cap (raw_db_pending_overflow carries the split
      // by queue, the cap and the mode).
      raw_db_pending_overflow_events: this._rawDbPendingOverflow?.dropped_events ?? 0,
      ...(this._rawDbPendingOverflow ? { raw_db_pending_overflow: { ...this._rawDbPendingOverflow } } : {}),
      // O-02: 0 on every normal row; > 0 on the final row when canonical frames
      // arrived after the shutdown drain had run (raw_db_post_drain_drop carries
      // their first/last ts). They are disjoint from the two fields above: these
      // frames never reached a queue and no drain remains to retry them.
      raw_db_post_drain_dropped_events: this._rawDbPostDrainDrop?.dropped_events ?? 0,
      ...(this._rawDbPostDrainDrop ? { raw_db_post_drain_drop: { ...this._rawDbPostDrainDrop } } : {}),
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
      .then(() => this._writeReport(report))
      .catch(error => console.error(`[HealthMonitor] ${error.message}`));
  }

  /**
   * Write one health row (buffer flush → rotation check → row → flush).
   * Shared by the periodic tick and the R-13 final drop-report row in close().
   */
  async _writeReport(report) {
    await this._writer.flush();
    await this._rotateIfNeeded();
    await this._writer.write(report);
    await this._writer.flush();
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
