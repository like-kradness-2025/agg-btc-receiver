// lib/derivatives-helper.mjs — Perp auxiliary data collector for btc-receiver v3.00
// Periodically fetches mark price, funding rate, and open interest from perp exchanges.
// Writes formatted rows to derivatives/{market}.jsonl

import { BufferedWriter } from './buffered-writer.mjs';
import { getOICapability, normalizeOpenInterest } from './oi-schema.mjs';

const DEFAULT_INTERVAL_MS = 10000;
const FETCH_TIMEOUT_MS = 10000;
const DEFAULT_MAX_RETRIES = 2;
const DEFAULT_BACKOFF_MS = 250;

const finite = (value) => {
  // Number(null) is 0, so null/undefined/'' must be rejected explicitly.
  // Otherwise an OI REST error row's source_ts:null silently becomes 0 and
  // downstream `source_ts ?? ts` fallbacks (orderflow_monitor) never fire,
  // producing event_ts_ms=0 placeholder rows.
  if (value === null || value === undefined || value === '') return null;
  const number = Number(value);
  return Number.isFinite(number) ? number : null;
};

const responseTimestamp = (value) => {
  const timestamp = finite(value);
  if (timestamp !== null) return timestamp;
  if (typeof value === 'string') {
    const trimmed = value.trim();
    if (trimmed === '') return null;
    // OKX returns millisecond timestamps as numeric strings (e.g. "1597026383085").
    if (/^\d+(\.\d+)?$/.test(trimmed)) {
      const parsed = Number(trimmed);
      return Number.isFinite(parsed) ? parsed : null;
    }
    const parsed = Date.parse(trimmed);
    return Number.isFinite(parsed) ? parsed : null;
  }
  return null;
};

class HttpResponseError extends Error {
  constructor(url, status) {
    super(`HTTP ${status} from ${url}`);
    this.name = 'HttpResponseError';
    this.status = status;
  }
}

/**
 * @typedef {Object} DerivativeRow
 * @property {number} ts
 * @property {string} market
 * @property {number|null} mark_price
 * @property {number|null} funding_rate
 * @property {number|null} open_interest
 * @property {number|null} next_funding_time
 * @property {string} source
 */

export class DerivativesHelper {
  /**
   * @param {string} outputBase - base directory for output files
   * @param {Object} [options]
   * @param {number} [options.intervalMs=5000]
   */
  constructor(outputBase, options = {}) {
    this._intervalMs = options.intervalMs ?? DEFAULT_INTERVAL_MS;
    this._maxRetries = options.maxRetries ?? DEFAULT_MAX_RETRIES;
    this._backoffBaseMs = options.backoffBaseMs ?? DEFAULT_BACKOFF_MS;
    this._sleep = options.sleep ?? ((ms) => new Promise(resolve => setTimeout(resolve, ms)));
    this._onRow = options.onRow ?? null;
    /** @type {Map<string, BufferedWriter>} */
    this._writers = new Map();
    this._timer = null;
    this._inFlight = false;
    this._tickPromise = null;
    this._closed = false;
    this._outputBase = outputBase;
  }

  /**
   * Register a perp market for auxiliary data collection.
   * @param {string} market  e.g. 'binance_perp'
   * @param {Object} [restUrls] - REST endpoint URLs for auxiliary data
   * @param {string} [restUrls.premiumIndex] - Binance perp premiumIndex URL
   * @param {string} [restUrls.openInterest] - Binance perp / OKX open interest URL
   * @param {string} [restUrls.tickers] - Bybit tickers URL
   * @param {string} [restUrls.fundingRate] - OKX funding rate URL
   */
  registerMarket(market, restUrls) {
    if (!getOICapability(market)) return false;
    if (this._writers.has(market)) return;
    const writer = this._onRow ? null : new BufferedWriter(
      `${this._outputBase}/derivatives/${market}.jsonl`,
      { flushIntervalMs: 1000, maxBufferLines: 100 },
    );
    this._writers.set(market, { writer, restUrls: restUrls || {} });
    return true;
  }

  /** Start periodic collection. */
  start() {
    if (this._timer) return;
    this._timer = setInterval(() => {
      this._tickPromise = this._tick().finally(() => { this._tickPromise = null; });
    }, this._intervalMs);
    if (this._timer.unref) this._timer.unref();
  }

  /** Stop periodic collection. */
  stop() {
    if (this._timer) {
      clearInterval(this._timer);
      this._timer = null;
    }
  }

  async close() {
    this.stop();
    this._closed = true;
    if (this._tickPromise) await this._tickPromise;
    const promises = [];
    for (const { writer } of this._writers.values()) {
      if (writer) promises.push(writer.close());
    }
    await Promise.allSettled(promises);
  }

  async _tick() {
    if (this._closed || this._inFlight) return false;
    this._inFlight = true;
    try {
      const now = Date.now();
      const promises = [];
      for (const [market, { writer, restUrls }] of this._writers) {
        promises.push(
          this._fetchMarket(market, writer, now, restUrls).catch(err => {
            console.error(`[derivatives] ${market} fetch error: ${err.message}`);
          }),
        );
      }
      await Promise.allSettled(promises);
    } finally {
      this._inFlight = false;
    }
    return true;
  }

  async _fetchMarket(market, writer, now, restUrls = {}) {
    let sample;
    try {
      if (market === 'binance_perp' || market === 'binance_perp_btcusdc') {
        sample = await this._fetchBinancePerp(now, market, restUrls);
      } else if (market === 'bybit_perp') {
        sample = await this._fetchBybit(now, restUrls);
      } else if (market === 'okx_perp') {
        sample = await this._fetchOkx(now, restUrls);
      } else if (market === 'hyperliquid_perp') {
        sample = await this._fetchHyperliquid(now, restUrls);
      } else if (market === 'bitmex_perp') {
        sample = await this._fetchBitmex(now, restUrls);
      } else {
        sample = { error: `no OI collector for ${market}`, error_code: 'oi_not_supported' };
      }
    } catch (err) {
      sample = { error: err.message, error_code: 'source_error' };
    }

    const sourceTs = responseTimestamp(sample?.source_ts);
    const normalized = normalizeOpenInterest({
      market,
      ts: sourceTs,
      open_interest: sample?.open_interest,
      oi_native: sample?.oi_native,
      oiCcy: sample?.oiCcy,
      oiUsd: sample?.oiUsd,
      mark_price: sample?.mark_price,
      error: sample?.error,
      error_code: sample?.error_code,
    }, { nowMs: now });
    const row = {
      ts: now,
      source_ts: sourceTs,
      market,
      mark_price: sample?.mark_price ?? null,
      funding_rate: sample?.funding_rate ?? null,
      open_interest: sample?.open_interest ?? null,
      next_funding_time: sample?.next_funding_time ?? null,
      source: market.replace(/_perp$|_.*$/, ''),
      ...normalized,
      // `ts` is collector time; `source_ts` is the exchange sample time.
      ts: now,
      source_ts: sourceTs,
    };
    if (this._onRow) await this._onRow(row);
    else await writer.write(row);
  }

  async _fetchJson(url, options = {}) {
    if (!url) throw new Error('missing REST URL');
    const fetchOptions = { signal: AbortSignal.timeout(FETCH_TIMEOUT_MS), ...options };
    for (let attempt = 0; attempt <= this._maxRetries; attempt++) {
      try {
        const response = await fetch(url, fetchOptions);
        if (response.ok) return await response.json();
        const error = new HttpResponseError(url, response.status);
        const retryable = response.status === 429 || response.status >= 500;
        if (!retryable || attempt >= this._maxRetries) throw error;
        const retryAfter = response.headers?.get?.('retry-after');
        const retryAfterMs = retryAfter && Number.isFinite(Number(retryAfter))
          ? Number(retryAfter) * 1000
          : this._backoffBaseMs * 2 ** attempt;
        await this._sleep(Math.max(0, retryAfterMs));
      } catch (err) {
        if (err instanceof HttpResponseError) {
          if (err.status !== 429 && err.status < 500) throw err;
          if (attempt >= this._maxRetries) throw err;
          continue;
        }
        if (attempt >= this._maxRetries) throw err;
        await this._sleep(this._backoffBaseMs * 2 ** attempt);
      }
    }
    throw new Error(`request failed: ${url}`);
  }

  _url(restUrls, key, fallback) {
    return restUrls?.[key] || fallback;
  }

  // ====== Binance perp ======

  async _fetchBinancePerp(now, market = 'binance_perp', restUrls = {}) {
    const symbol = getOICapability(market).symbol;
    const apiRoot = 'https://fapi.binance.com/fapi/v1';
    // premiumIndex gives markPrice + lastFundingRate
    let markPrice = null, fundingRate = null, nextFundingTime = null;
    let openInterest = null;
    let sourceTs = null;
    let oiError = null;

    try {
      const d = await this._fetchJson(this._url(restUrls, 'premiumIndex', `${apiRoot}/premiumIndex?symbol=${symbol}`));
      markPrice = finite(d.markPrice);
      fundingRate = finite(d.lastFundingRate);
      nextFundingTime = finite(d.nextFundingTime);
      sourceTs = responseTimestamp(d.time);
    } catch (_err) {
      // OI remains independently usable if only premiumIndex failed.
    }

    try {
      const d = await this._fetchJson(this._url(restUrls, 'openInterest', `${apiRoot}/openInterest?symbol=${symbol}`));
      openInterest = finite(d.openInterest);
      sourceTs = responseTimestamp(d.time) ?? sourceTs ?? now;
    } catch (err) {
      oiError = err;
    }

    return {
      mark_price: markPrice,
      funding_rate: fundingRate,
      open_interest: openInterest,
      next_funding_time: nextFundingTime,
      source_ts: oiError ? null : (sourceTs ?? now),
      error: oiError?.message ?? null,
      error_code: oiError ? 'source_error' : null,
    };
  }

  // ====== Bybit perp ======

  async _fetchBybit(now, restUrls = {}) {
    let markPrice = null, fundingRate = null, openInterest = null, nextFundingTime = null;
    let sourceTs = null;
    let error = null;

    try {
      const d = await this._fetchJson(this._url(restUrls, 'tickers', 'https://api.bybit.com/v5/market/tickers?category=linear&symbol=BTCUSDT'));
      if (!d.result?.list?.length) throw new Error('Bybit ticker response has no BTCUSDT row');
      const t = d.result.list[0];
      markPrice = finite(t.markPrice);
      fundingRate = finite(t.fundingRate);
      openInterest = finite(t.openInterest);
      nextFundingTime = finite(t.nextFundingTime);
      sourceTs = responseTimestamp(d.time) ?? now;
    } catch (err) {
      error = err.message;
    }

    return {
      mark_price: markPrice,
      funding_rate: fundingRate,
      open_interest: openInterest,
      next_funding_time: nextFundingTime,
      source_ts: error ? null : sourceTs,
      error,
      error_code: error ? 'source_error' : null,
    };
  }

  // ====== OKX perp ======

  async _fetchOkx(now, restUrls = {}) {
    let markPrice = null, fundingRate = null, openInterest = null, oiCcy = null, oiUsd = null, nextFundingTime = null;
    let sourceTs = null;
    let oiError = null;

    try {
      const d = await this._fetchJson(this._url(restUrls, 'fundingRate', 'https://www.okx.com/api/v5/public/funding-rate?instId=BTC-USDT-SWAP'));
      const row = d.data?.[0];
      if (row) {
        fundingRate = finite(row.fundingRate);
        nextFundingTime = finite(row.fundingTime);
      }
    } catch (_err) {
      // Funding is optional; keep OI independent.
    }

    // open interest: OKX provides oi (contracts), oiCcy (BTC), and oiUsd (USD).
    // Pass all three through so the schema can prefer source-provided notional
    // values and keep contracts for audit.
    try {
      const d = await this._fetchJson(this._url(restUrls, 'openInterest', 'https://www.okx.com/api/v5/public/open-interest?instType=SWAP&instId=BTC-USDT-SWAP'));
      const row = d.data?.[0];
      if (!row) throw new Error('OKX open-interest response has no BTC-USDT-SWAP row');
      openInterest = finite(row.oi);
      oiCcy = finite(row.oiCcy);
      oiUsd = finite(row.oiUsd);
      sourceTs = responseTimestamp(row.ts) ?? now;
    } catch (err) {
      oiError = err;
    }

    // mark price from dedicated mark-price endpoint (more reliable than ticker.markPx)
    try {
      const d = await this._fetchJson(this._url(restUrls, 'markPrice', 'https://www.okx.com/api/v5/public/mark-price?instType=SWAP&instId=BTC-USDT-SWAP'));
      markPrice = finite(d.data?.[0]?.markPx);
    } catch (_err) {
      // Mark is optional for the source row; schema will report missing_price.
    }

    return {
      mark_price: markPrice,
      funding_rate: fundingRate,
      open_interest: openInterest,
      oiCcy,
      oiUsd,
      next_funding_time: nextFundingTime,
      source_ts: oiError ? null : (sourceTs ?? now),
      error: oiError?.message ?? null,
      error_code: oiError ? 'source_error' : null,
    };
  }

  // ====== Hyperliquid perp ======
  // Uses info POST type=metaAndAssetCtxs which returns all asset contexts in one call.
  // Index 0 = BTC (first asset in the universe).

  async _fetchHyperliquid(now, restUrls = {}) {
    let markPrice = null, fundingRate = null, openInterest = null, nextFundingTime = null;

    try {
      const d = await this._fetchJson(this._url(restUrls, 'info', 'https://api.hyperliquid.xyz/info'), {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ type: 'metaAndAssetCtxs' }),
      });
      // d is [universe[], assetCtxs[]] — assetCtxs[0] = BTC
      // asset ctx fields: { markPx, funding, openInterest, ... }
      if (Array.isArray(d) && d.length >= 2 && Array.isArray(d[1]) && d[1][0]) {
        const btc = d[1][0];
        markPrice = finite(btc.markPx);
        fundingRate = finite(btc.funding);
        openInterest = finite(btc.openInterest);
      } else {
        throw new Error('Hyperliquid response has no BTC asset context');
      }
    } catch (err) {
      return {
        mark_price: markPrice,
        funding_rate: fundingRate,
        open_interest: openInterest,
        next_funding_time: nextFundingTime,
        source_ts: null,
        error: err.message,
        error_code: 'source_error',
      };
    }

    return {
      mark_price: markPrice,
      funding_rate: fundingRate,
      open_interest: openInterest,
      next_funding_time: nextFundingTime,
      source_ts: now,
      error: null,
      error_code: null,
    };
  }

  // BitMEX instrument API returns the live XBTUSD contract state, including
  // openInterest, markPrice, and timestamp. Funding is intentionally null
  // unless the response explicitly provides a fundingRate field.
  async _fetchBitmex(now, restUrls = {}) {
    let row;
    try {
      const d = await this._fetchJson(this._url(restUrls, 'instrument', 'https://www.bitmex.com/api/v1/instrument?symbol=XBTUSD'));
      row = Array.isArray(d) ? d[0] : null;
      if (!row || row.symbol !== 'XBTUSD') throw new Error('BitMEX response has no XBTUSD instrument');
      return {
        mark_price: finite(row.markPrice),
        funding_rate: finite(row.fundingRate),
        open_interest: finite(row.openInterest),
        next_funding_time: null,
        source_ts: responseTimestamp(row.timestamp) ?? now,
        error: null,
        error_code: null,
      };
    } catch (err) {
      return {
        mark_price: finite(row?.markPrice),
        funding_rate: finite(row?.fundingRate),
        open_interest: finite(row?.openInterest),
        next_funding_time: null,
        source_ts: null,
        error: err.message,
        error_code: 'source_error',
      };
    }
  }
}
