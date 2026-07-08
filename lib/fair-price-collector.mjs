// lib/fair-price-collector.mjs — Raw trade/depth/snapshot recorder for agg-btc-receiver
//
// Raw Hot Storage Tier:
//   data/raw_hot/{UTC-date}/depth/{market}.jsonl     — WS depth incremental updates
//   data/raw_hot/{UTC-date}/snapshot/{market}.jsonl  — Full book checkpoints
//   data/raw_hot/{UTC-date}/trade/{market}.jsonl     — Raw trade ticks

import fs from 'node:fs';
import path from 'node:path';
import { BufferedWriter } from './buffered-writer.mjs';

const DEFAULT_SNAPSHOT_INTERVAL_MS = 1000;
const DEFAULT_BOOK_SNAPSHOT_MS = 30000;
const RECOVERY_SNAPSHOT_DELAY_MS = 3000;

/** UTC date string YYYY-MM-DD */
function utcDateStr(d) {
  const y = d.getUTCFullYear();
  const m = String(d.getUTCMonth() + 1).padStart(2, '0');
  const day = String(d.getUTCDate()).padStart(2, '0');
  return `${y}-${m}-${day}`;
}

/** Extract exchange name from canonical market id: 'binance_spot' → 'binance' */
function exchangeFromMarket(market) {
  if (market === 'binance_coinm_perp') return 'binance';
  if (market === 'binance_perp_btcusdc') return 'binance';
  if (market === 'coinbase_international_perp') return 'coinbase';
  const idx = market.lastIndexOf('_');
  return idx > 0 ? market.slice(0, idx) : market;
}

export class FairPriceCollector {
  constructor(outputBase, options = {}) {
    this._outputBase = outputBase;
    this._snapshotIntervalMs = options.snapshotIntervalMs ?? DEFAULT_SNAPSHOT_INTERVAL_MS;
    this._bookSnapshotMs = options.bookSnapshotMs ?? DEFAULT_BOOK_SNAPSHOT_MS;

    /** @type {Map<string, { connector: any, book: any }>} */
    this._markets = new Map();
    /** @type {Map<string, BufferedWriter>} key: "dateStr/stream/market" */
    this._writers = new Map();
    /** @type {Map<string, number>} */
    this._lastBookSnapshotAt = new Map();
    /** @type {Map<string, number|null>} */
    this._lastSeqs = new Map();
    /** @type {Map<string, number>} */
    this._lastSnapshotBoundaries = new Map();
    /** @type {Map<string, number>} */
    this._snapshotInFlight = new Map();

    this._currentDate = utcDateStr(new Date());
    this._timer = null;
    this._closed = false;
    this._pendingWrites = new Set();
  }

  _schedule(task) {
    const p = Promise.resolve().then(task).finally(() => {
      this._pendingWrites.delete(p);
    });
    this._pendingWrites.add(p);
    return p;
  }

  _getWriter(stream, market, dateStr) {
    const key = `${dateStr}/${stream}/${market}`;
    let w = this._writers.get(key);
    if (!w) {
      const dir = path.join(this._outputBase, dateStr, stream);
      fs.mkdirSync(dir, { recursive: true });
      w = new BufferedWriter(path.join(dir, `${market}.jsonl`), {
        flushIntervalMs: stream === 'snapshot' ? 1000 : 200,
      });
      this._writers.set(key, w);
    }
    return w;
  }

  _closeWriters() {
    const promises = [];
    for (const w of this._writers.values()) promises.push(w.close());
    this._writers.clear();
    return Promise.allSettled(promises);
  }

  async _ensureDate(dateStr) {
    if (dateStr === this._currentDate) return;
    const oldDate = this._currentDate;
    this._currentDate = dateStr;
    const promises = [];
    for (const [key, w] of this._writers) {
      if (key.startsWith(oldDate + '/')) promises.push(w.close());
    }
    await Promise.allSettled(promises);
    for (const key of this._writers.keys()) {
      if (key.startsWith(oldDate + '/')) this._writers.delete(key);
    }
    console.log(`[agg-btc] date partition: ${oldDate} → ${dateStr}`);
  }

  async _writeDepth(market, exchange, depthEvent) {
    if (this._closed) return;
    const now = Date.now();
    const dateStr = utcDateStr(new Date());
    await this._ensureDate(dateStr);
    const writer = this._getWriter('depth', market, dateStr);
    const seq = depthEvent.seq ?? null;
    const prevSeq = this._lastSeqs.get(market) ?? null;
    if (seq != null) this._lastSeqs.set(market, seq);
    await writer.write({
      schemaVersion: '1.0',
      stream: 'depth',
      type: 'update',
      ts: depthEvent.ts ?? now,
      recvTs: now,
      market,
      exchange,
      seq,
      prevSeq,
      bids: depthEvent.bids,
      asks: depthEvent.asks,
    });
  }

  async _writeSnapshot(market, book, reason) {
    if (this._closed) return;
    const now = Date.now();
    const dateStr = utcDateStr(new Date());
    await this._ensureDate(dateStr);
    const snap = book.toSnapshot(now);
    const writer = this._getWriter('snapshot', market, dateStr);
    await writer.write({
      schemaVersion: '1.0',
      stream: 'snapshot',
      reason,
      ts: now,
      recvTs: now,
      market,
      exchange: exchangeFromMarket(market),
      seq: snap.seq ?? null,
      bids: snap.bids,
      asks: snap.asks,
      bidLevelCount: snap.bidLevelCount,
      askLevelCount: snap.askLevelCount,
    });
  }

  _hasUsableBook(book) {
    if (!book || book.isEmpty()) return false;
    const counts = typeof book.getLevelCount === 'function'
      ? book.getLevelCount()
      : { bids: book.bids?.size ?? 0, asks: book.asks?.size ?? 0 };
    return counts.bids > 0 && counts.asks > 0;
  }

  _scheduleRecoverySnapshot(market, connector, book, reason = 'reconnect') {
    const boundary = Math.floor(Date.now() / this._bookSnapshotMs);
    const inFlight = this._snapshotInFlight.get(market) ?? -1;
    const lastBoundary = this._lastSnapshotBoundaries.get(market) ?? -1;
    if (inFlight >= boundary || lastBoundary >= boundary) return;
    this._snapshotInFlight.set(market, boundary);

    const timer = setTimeout(() => {
      if (this._closed) {
        this._snapshotInFlight.delete(market);
        return;
      }
      if (!connector || connector.getState() !== 'running' || !this._hasUsableBook(book)) {
        this._snapshotInFlight.delete(market);
        return;
      }

      const snapshotAt = Date.now();
      this._schedule(() => this._writeSnapshot(market, book, reason)).then(() => {
        this._lastBookSnapshotAt.set(market, snapshotAt);
        this._lastSnapshotBoundaries.set(market, boundary);
      }).catch((err) => {
        console.error(`[agg-btc] ${market} recovery snapshot error: ${err.message}`);
      }).finally(() => {
        this._snapshotInFlight.delete(market);
      });
    }, RECOVERY_SNAPSHOT_DELAY_MS);
    if (timer.unref) timer.unref();
  }

  registerMarket(market, { connector, book }) {
    if (this._markets.has(market)) return;

    const exchange = exchangeFromMarket(market);
    this._markets.set(market, { connector, book });
    this._lastBookSnapshotAt.set(market, 0);

    connector.on('trade', (tradeEvent) => {
      if (this._closed) return;
      this._schedule(async () => {
        const dateStr = utcDateStr(new Date());
        await this._ensureDate(dateStr);
        await this._getWriter('trade', market, dateStr).write({ type: 'trade', ...tradeEvent });
      });
    });

    connector.on('depth', (depthEvent) => {
      if (this._closed) return;
      if (depthEvent.type === 'snapshot') return;
      this._schedule(() => this._writeDepth(market, exchange, depthEvent));
    });

    connector.on('stateChange', (from, to) => {
      if (from === 'running' && to !== 'running') {
        this._lastSeqs.delete(market);
      }
      if (to === 'running') {
        const isStartup = from === 'init' || this._lastBookSnapshotAt.get(market) === 0;
        if (isStartup) {
          this._schedule(() => this._writeSnapshot(market, book, 'startup')).then(() => {
            this._lastBookSnapshotAt.set(market, Date.now());
          }).catch((err) => {
            console.error(`[agg-btc] ${market} startup snapshot error: ${err.message}`);
          });
        } else {
          this._scheduleRecoverySnapshot(market, connector, book, 'reconnect');
        }
      }
    });
  }

  start() {
    if (this._timer) return;
    this._timer = setInterval(() => {
      this._tickSnapshots().catch(err => {
        console.error(`[agg-btc] snapshot tick error: ${err.message}`);
      });
    }, this._snapshotIntervalMs);
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
    this._closed = true;
    await Promise.allSettled([...this._pendingWrites]);
    await this._closeWriters();
    console.log('[agg-btc] closed');
  }

  async _tickSnapshots() {
    if (this._closed) return;
    const now = Date.now();
    const dateStr = utcDateStr(new Date());
    await this._ensureDate(dateStr);

    for (const [market, entry] of this._markets) {
      const { connector, book } = entry;
      if (!connector || connector.getState() !== 'running' || !this._hasUsableBook(book)) {
        continue;
      }

      const lastSnap = this._lastBookSnapshotAt.get(market) ?? 0;
      if (now - lastSnap >= this._bookSnapshotMs) {
        const snapInFlight = this._snapshotInFlight.get(market);
        if (snapInFlight == null) {
          this._lastBookSnapshotAt.set(market, now);
          this._snapshotInFlight.set(market, now);
          const snapshotTask = this._schedule(() => this._writeSnapshot(market, book, 'periodic'));
          snapshotTask.finally(() => {
            this._snapshotInFlight.delete(market);
          });
        }
      }
    }
  }
}
