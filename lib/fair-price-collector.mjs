// lib/fair-price-collector.mjs — Fair price / book diff+snapshot / raw trades recorder for btc-receiver
//
// Raw Hot Storage Tier (SPEC.md):
//   data/raw_hot/{UTC-date}/depth/{market}.jsonl     — WS depth incremental updates
//   data/raw_hot/{UTC-date}/snapshot/{market}.jsonl   — Full book checkpoints
//   data/raw_hot/{UTC-date}/trade/{market}.jsonl      — Raw trade ticks
//   data/raw_hot/{UTC-date}/fairprice/{market}.jsonl  — Fair price features (1s)

import fs from 'node:fs';
import path from 'node:path';
import { BufferedWriter } from './buffered-writer.mjs';

const DEFAULT_TICK_INTERVAL_MS = 1000;
const DEFAULT_BOOK_SNAPSHOT_MS = 600000; // 10 min
const DEFAULT_MARK_FETCH_MS = 5000;
const FETCH_TIMEOUT_MS = 10000;

/**
 * Pick fair price in priority order: mark price -> book mid -> last price.
 */
export function selectFairPrice({ markPrice, bookMid, lastPrice }) {
  if (Number.isFinite(markPrice) && markPrice > 0) {
    return { fairPrice: markPrice, source: 'mark_price' };
  }
  if (Number.isFinite(bookMid) && bookMid > 0) {
    return { fairPrice: bookMid, source: 'book_mid' };
  }
  if (Number.isFinite(lastPrice) && lastPrice > 0) {
    return { fairPrice: lastPrice, source: 'last_price' };
  }
  return null;
}

async function fetchJSON(url, options = {}) {
  const res = await fetch(url, {
    ...options,
    signal: AbortSignal.timeout(FETCH_TIMEOUT_MS),
  });
  if (!res.ok) throw new Error(`HTTP ${res.status}`);
  return res.json();
}

function toPairPrice(value) {
  if (value == null) return null;
  const parsed = typeof value === 'number' ? value : parseFloat(value);
  return Number.isFinite(parsed) ? parsed : null;
}

/** UTC date string YYYY-MM-DD */
function utcDateStr(d) {
  const y = d.getUTCFullYear();
  const m = String(d.getUTCMonth() + 1).padStart(2, '0');
  const day = String(d.getUTCDate()).padStart(2, '0');
  return `${y}-${m}-${day}`;
}

/**
 * Create a market-specific mark price fetcher.
 */
export function createMarkPriceFetcher(market, cfg = {}) {
  const symbol = cfg.symbol || 'BTCUSDT';

  if (market === 'binance_perp') {
    const url = cfg.markPriceUrl || `https://fapi.binance.com/fapi/v1/premiumIndex?symbol=${symbol}`;
    return async () => {
      try {
        const d = await fetchJSON(url);
        return toPairPrice(d?.markPrice);
      } catch { return null; }
    };
  }

  if (market === 'bybit_perp') {
    const url = cfg.markPriceUrl || `https://api.bybit.com/v5/market/tickers?category=linear&symbol=${symbol}`;
    return async () => {
      try {
        const d = await fetchJSON(url, { headers: { 'User-Agent': 'btc-receiver/v3.00' } });
        return toPairPrice(d?.result?.list?.[0]?.markPrice);
      } catch { return null; }
    };
  }

  if (market === 'okx_perp') {
    const instId = cfg.symbol || 'BTC-USDT-SWAP';
    const url = cfg.markPriceUrl || `https://www.okx.com/api/v5/public/mark-price?instType=SWAP&instId=${instId}`;
    return async () => {
      try {
        const d = await fetchJSON(url, { headers: { 'User-Agent': 'btc-receiver/v3.00' } });
        return toPairPrice(d?.data?.[0]?.markPx);
      } catch { return null; }
    };
  }

  if (market === 'hyperliquid_perp') {
    const url = cfg.markPriceUrl || 'https://api.hyperliquid.xyz/info';
    return async () => {
      try {
        const d = await fetchJSON(url, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ type: 'metaAndAssetCtxs' }),
        });
        const ctxs = Array.isArray(d) ? d[1] : null;
        return toPairPrice(ctxs?.[0]?.markPx);
      } catch { return null; }
    };
  }

  return null;
}

/** Extract exchange name from canonical market id: 'binance_spot' → 'binance' */
function exchangeFromMarket(market) {
  // Special cases first
  if (market === 'binance_coinm_perp') return 'binance';
  if (market === 'binance_perp_btcusdc') return 'binance';
  if (market === 'coinbase_international_perp') return 'coinbase';
  // General: strip last suffix
  const idx = market.lastIndexOf('_');
  return idx > 0 ? market.slice(0, idx) : market;
}

export class FairPriceCollector {
  constructor(outputBase, options = {}) {
    this._outputBase = outputBase;
    this._tickIntervalMs = options.tickIntervalMs ?? DEFAULT_TICK_INTERVAL_MS;
    this._bookSnapshotMs = options.bookSnapshotMs ?? DEFAULT_BOOK_SNAPSHOT_MS;
    this._markFetchMs = options.markFetchMs ?? DEFAULT_MARK_FETCH_MS;

    /** @type {Map<string, { connector: any, book: any, markPriceFetcher: null | (() => Promise<number|null>) }>} */
    this._markets = new Map();

    /** @type {Map<string, BufferedWriter>} key: "dateStr/stream/market" */
    this._writers = new Map();

    /** @type {Map<string, number|null>} */
    this._lastMarkPrices = new Map();
    /** @type {Map<string, number>} */
    this._lastTradePrices = new Map();
    /** @type {Map<string, number>} */
    this._lastBookSnapshotAt = new Map();
    /** @type {Map<string, number>} */
    this._lastMarkFetchAt = new Map();

    this._currentDate = utcDateStr(new Date());
    /** @type {Map<string, number|null>} Track last seq per market for prevSeq */
    this._lastSeqs = new Map();
    /** @type {Map<string, number>} */
    this._lastSnapshotBoundaries = new Map();
    /** @type {Map<string, number>} Track in-flight periodic snapshot boundaries to prevent duplicate enqueue */
    this._snapshotInFlight = new Map();
    this._timer = null;
    this._ticking = false;
    this._closed = false;
  }

  // ====== Writer management ======

  /** Get or create a writer for the given stream/market/date. */
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

  /** Close all current writers and clear the map. */
  _closeWriters() {
    const promises = [];
    for (const w of this._writers.values()) promises.push(w.close());
    this._writers.clear();
    return Promise.allSettled(promises);
  }

  /** Rotate to new date partition if needed. Closes old writers. */
  async _ensureDate(dateStr) {
    if (dateStr === this._currentDate) return;
    const oldDate = this._currentDate;
    this._currentDate = dateStr;
    // Close all writers for the old date partition
    const promises = [];
    for (const [key, w] of this._writers) {
      if (key.startsWith(oldDate + '/')) promises.push(w.close());
    }
    await Promise.allSettled(promises);
    // Remove closed writer entries
    for (const key of this._writers.keys()) {
      if (key.startsWith(oldDate + '/')) this._writers.delete(key);
    }
    console.log(`[fairprice] date partition: ${oldDate} → ${dateStr}`);
  }

  // ====== Depth diff / snapshot writing ======

  /** Write a depth diff event (incremental WS update). */
  async _writeDepth(market, exchange, depthEvent) {
    if (this._closed) return;
    const now = Date.now();
    const dateStr = utcDateStr(new Date());
    await this._ensureDate(dateStr);
    const writer = this._getWriter('depth', market, dateStr);
    const seq = depthEvent.seq ?? null;
    const prevSeq = this._lastSeqs.get(market) ?? null;
    if (seq != null) this._lastSeqs.set(market, seq);
    writer.write({
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

  /** Write a full book snapshot checkpoint. */
  async _writeSnapshot(market, book, reason) {
    if (this._closed) return;
    const now = Date.now();
    const dateStr = utcDateStr(new Date());
    await this._ensureDate(dateStr);
    const snap = book.toSnapshot(now);
    const writer = this._getWriter('snapshot', market, dateStr);
    writer.write({
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

  // ====== Market registration ======

  registerMarket(market, { connector, book, markPriceFetcher = null }) {
    if (this._markets.has(market)) return;

    const exchange = exchangeFromMarket(market);
    this._markets.set(market, { connector, book, markPriceFetcher });
    this._lastBookSnapshotAt.set(market, 0);
    this._lastMarkFetchAt.set(market, 0);

    // ── Trade events ──
    connector.on('trade', (tradeEvent) => {
      if (this._closed) return;
      this._lastTradePrices.set(market, tradeEvent.price);
      const dateStr = utcDateStr(new Date());
      this._ensureDate(dateStr);
      this._getWriter('trade', market, dateStr).write({ type: 'trade', ...tradeEvent });
    });

    // ── Depth events (WS incremental updates) ──
    connector.on('depth', (depthEvent) => {
      if (this._closed) return;
      // Only persist incremental updates; full snapshots from WS go to snapshot stream via _writeSnapshot
      if (depthEvent.type === 'snapshot') return;
      this._writeDepth(market, exchange, depthEvent);
    });

    // ── State change → trigger snapshot on successful recovery ──
    connector.on('stateChange', (from, to) => {
      if (to === 'running' && book && !book.isEmpty()) {
        let reason = 'startup';
        if (from === 'reconnecting') reason = 'reconnect';
        else if (from === 'error') reason = 'gap_recovery';
        else if (from === 'syncing') {
          reason = this._lastBookSnapshotAt.get(market) === 0 ? 'startup' : 'reconnect';
        }
        // Capture boundary at fire time so a slow write that crosses a UTC
        // boundary doesn't misalign the tracked boundary.  Set in-flight
        // guard BEFORE the write to prevent a concurrent periodic tick from
        // queueing a duplicate snapshot for the same boundary.
        const recoveryBoundary = Math.floor(Date.now() / this._bookSnapshotMs);
        const recoverySnapshotAt = Date.now();
        // Guard: skip if periodic already covered or is in-flight for this boundary
        const lastBoundary = this._lastSnapshotBoundaries.get(market) ?? -1;
        const inFlight = this._snapshotInFlight.get(market) ?? -1;
        if (inFlight >= recoveryBoundary || lastBoundary >= recoveryBoundary) return;
        this._snapshotInFlight.set(market, recoveryBoundary);
        this._writeSnapshot(market, book, reason).then(() => {
          this._lastBookSnapshotAt.set(market, recoverySnapshotAt);
          this._lastSnapshotBoundaries.set(market, recoveryBoundary);
          this._snapshotInFlight.delete(market);
        }).catch(err => {
          this._snapshotInFlight.delete(market);
          console.error(`[fairprice] ${market} recovery snapshot error: ${err.message}`);
        });
      }
    });
  }

  // ====== Lifecycle ======

  start() {
    if (this._timer) return;
    this._timer = setInterval(() => {
      this._tick().catch(err => {
        console.error(`[fairprice] tick error: ${err.message}`);
      });
    }, this._tickIntervalMs);
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

    // Wait for any in-flight _tick to finish
    while (this._ticking) {
      await new Promise(resolve => setTimeout(resolve, 10));
    }

    // Write final shutdown snapshots for markets with valid books
    const now = Date.now();
    const dateStr = utcDateStr(new Date());
    await this._ensureDate(dateStr);
    for (const [market, entry] of this._markets) {
      const { book } = entry;
      if (book && !book.isEmpty()) {
        const snap = book.toSnapshot(now);
        const writer = this._getWriter('snapshot', market, dateStr);
        writer.write({
          schemaVersion: '1.0',
          stream: 'snapshot',
          reason: 'shutdown',
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
    }

    await this._closeWriters();
  }

  // ====== Tick ======

  async _tick() {
    if (this._closed || this._ticking) return;
    this._ticking = true;
    try {
      const now = Date.now();
      const dateStr = utcDateStr(new Date());
      this._ensureDate(dateStr);

      await this._refreshMarks(now);

      for (const [market, entry] of this._markets) {
        const { connector, book } = entry;
        if (!connector || connector.getState() !== 'running' || !book || book.isEmpty()) {
          continue;
        }

        // Periodic full snapshot aligned to UTC bookSnapshotMs boundaries.
        // Track per market with in-flight guard to prevent duplicate enqueue
        // when _writeSnapshot takes longer than one tick interval.
        const currentBoundary = Math.floor(now / this._bookSnapshotMs);
        const lastBoundary = this._lastSnapshotBoundaries.get(market) ?? -1;
        if (currentBoundary > lastBoundary) {
          const inFlight = this._snapshotInFlight.get(market) ?? -1;
          if (inFlight < currentBoundary) {
            this._snapshotInFlight.set(market, currentBoundary);
            this._writeSnapshot(market, book, 'periodic').then(() => {
              this._snapshotInFlight.delete(market);
              this._lastSnapshotBoundaries.set(market, currentBoundary);
              this._lastBookSnapshotAt.set(market, now);
            }).catch(err => {
              this._snapshotInFlight.delete(market);
              console.error(`[fairprice] ${market} periodic snapshot error: ${err.message}`);
            });
          }
        }

        // Fair price computation (1s interval, written to fairprice stream)
        const bookMid = book.getMid();
        const lastPrice = this._lastTradePrices.get(market) ?? null;
        const markPrice = this._lastMarkPrices.get(market) ?? null;
        const picked = selectFairPrice({ markPrice, bookMid, lastPrice });
        if (!picked) continue;

        this._getWriter('fairprice', market, dateStr).write({
          type: 'fair_price',
          ts: now,
          market,
          fair_price: picked.fairPrice,
          fair_price_source: picked.source,
          mark_price: markPrice,
          book_mid: bookMid,
          last_price: lastPrice,
        });
      }
    } finally {
      this._ticking = false;
    }
  }

  async _refreshMarks(now) {
    const tasks = [];
    for (const [market, entry] of this._markets) {
      const { markPriceFetcher } = entry;
      if (!markPriceFetcher) continue;

      const lastFetch = this._lastMarkFetchAt.get(market) ?? 0;
      if (now - lastFetch < this._markFetchMs) continue;

      this._lastMarkFetchAt.set(market, now);
      tasks.push(
        markPriceFetcher().then((price) => {
          this._lastMarkPrices.set(market, price);
        }).catch(() => {
          this._lastMarkPrices.set(market, null);
        }),
      );
    }
    await Promise.allSettled(tasks);
  }
}
