// lib/market-data-collector.mjs — REST-based market data collector for btc-receiver v3.05
//
// Collects additional free data from all exchanges at configurable intervals:
//   - OHLCV  (1m candles)      → data/live_v3/ohlcv/{market}.jsonl
//   - 24h Ticker               → data/live_v3/ticker/{market}.jsonl
//   - Long/Short Ratio         → data/live_v3/lsratio/{market}.jsonl      (perp only)
//   - Taker Buy/Sell Volume    → data/live_v3/takervol/{market}.jsonl     (perp only)
//   - Premium Index            → data/live_v3/premium/coinbase_premium.jsonl
//
// All data is fetched via REST (no auth required) and written as JSONL.
// Exchange-specific parsing is driven by PARSER_CONFIGS for easy extensibility.

import { BufferedWriter } from './buffered-writer.mjs';

// ======================================================================
// Constants
// ======================================================================

const FETCH_TIMEOUT_MS = 10000;
const FETCH_RETRIES = 1;

// ======================================================================
// Ticker parser configuration per exchange
//
// Each entry defines:
//   needsUA    — whether to send User-Agent header
//   extract    — function to get the raw data row from response JSON
//   validate   — predicate that ensures the response/extracted data is valid
//   fields     — mapping { standardizedName: 'responseField' | fn }
//   transforms — optional { fieldName: (rawValue) => transformedValue }
//
// To add a new exchange ticker, just add an entry here + its URL in config.v3.json.
// ======================================================================

const TICKER_PARSERS = {
  binance_spot: {
    needsUA: false,
    extract: (d) => d,
    validate: (d) => d && d.lastPrice !== undefined,
    fields: {
      last_price: 'lastPrice',
      volume_24h: 'volume',
      quote_vol_24h: 'quoteVolume',
      high_24h: 'highPrice',
      low_24h: 'lowPrice',
      price_change_24h: 'priceChange',
      price_change_pct: 'priceChangePercent',
    },
  },
  binance_spot_usdc: {
    needsUA: false,
    extract: (d) => d,
    validate: (d) => d && d.lastPrice !== undefined,
    fields: {
      last_price: 'lastPrice',
      volume_24h: 'volume',
      quote_vol_24h: 'quoteVolume',
      high_24h: 'highPrice',
      low_24h: 'lowPrice',
      price_change_24h: 'priceChange',
      price_change_pct: 'priceChangePercent',
    },
  },
  binance_spot_fdusd: {
    needsUA: false,
    extract: (d) => d,
    validate: (d) => d && d.lastPrice !== undefined,
    fields: {
      last_price: 'lastPrice',
      volume_24h: 'volume',
      quote_vol_24h: 'quoteVolume',
      high_24h: 'highPrice',
      low_24h: 'lowPrice',
      price_change_24h: 'priceChange',
      price_change_pct: 'priceChangePercent',
    },
  },
  // Binance perp ticker has identical shape to spot
  binance_perp: {
    needsUA: false,
    extract: (d) => d,
    validate: (d) => d && d.lastPrice !== undefined,
    fields: {
      last_price: 'lastPrice',
      volume_24h: 'volume',
      quote_vol_24h: 'quoteVolume',
      high_24h: 'highPrice',
      low_24h: 'lowPrice',
      price_change_24h: 'priceChange',
      price_change_pct: 'priceChangePercent',
    },
  },
  bybit_perp: {
    needsUA: true,
    extract: (d) => d.result?.list?.[0],
    validate: (d) => d.retCode === 0 && !!d.result?.list?.length,
    fields: {
      last_price: 'lastPrice',
      volume_24h: 'volume24h',
      quote_vol_24h: 'turnover24h',
      high_24h: 'highPrice24h',
      low_24h: 'lowPrice24h',
      open_24h: 'prevPrice24h',
      mark_price: 'markPrice',
      funding_rate: 'fundingRate',
      open_interest: 'openInterest',
      next_funding_time: 'nextFundingTime',
    },
    transforms: {
      next_funding_time: (v) => (v ? parseInt(v, 10) : null),
    },
  },
  okx_perp: {
    needsUA: true,
    extract: (d) => d.data?.[0],
    validate: (d) => !!d.data?.length,
    fields: {
      last_price: 'last',
      volume_24h: 'vol24h',
      quote_vol_24h: 'volCcy24h',
      high_24h: 'high24h',
      low_24h: 'low24h',
      open_24h: 'open24h',
      ts: 'ts',
    },
    transforms: {
      ts: (v) => (v ? parseInt(v, 10) : null),
    },
  },
  okx_spot: {
    needsUA: true,
    extract: (d) => d.data?.[0],
    validate: (d) => !!d.data?.length,
    fields: {
      last_price: 'last',
      volume_24h: 'vol24h',
      quote_vol_24h: 'volCcy24h',
      high_24h: 'high24h',
      low_24h: 'low24h',
      open_24h: 'open24h',
      ts: 'ts',
    },
    transforms: {
      ts: (v) => (v ? parseInt(v, 10) : null),
    },
  },
  bybit_spot: {
    needsUA: true,
    extract: (d) => d.result?.list?.[0],
    validate: (d) => d.retCode === 0 && !!d.result?.list?.length,
    fields: {
      last_price: 'lastPrice',
      volume_24h: 'volume24h',
      quote_vol_24h: 'turnover24h',
      high_24h: 'highPrice24h',
      low_24h: 'lowPrice24h',
      open_24h: 'prevPrice24h',
    },
  },
  binance_coinm_perp: {
    needsUA: false,
    extract: (d) => d,
    validate: (d) => d && d.lastPrice !== undefined,
    fields: {
      last_price: 'lastPrice',
      volume_24h: 'volume',
      quote_vol_24h: 'quoteVolume',
      high_24h: 'highPrice',
      low_24h: 'lowPrice',
      price_change_24h: 'priceChange',
      price_change_pct: 'priceChangePercent',
    },
  },
  binance_perp_btcusdc: {
    needsUA: false,
    extract: (d) => d,
    validate: (d) => d && d.lastPrice !== undefined,
    fields: {
      last_price: 'lastPrice',
      volume_24h: 'volume',
      quote_vol_24h: 'quoteVolume',
      high_24h: 'highPrice',
      low_24h: 'lowPrice',
      price_change_24h: 'priceChange',
      price_change_pct: 'priceChangePercent',
    },
  },
  coinbase_spot: {
    needsUA: true,
    extract: (d) => d,
    validate: (d) => d && d.price !== undefined,
    fields: {
      last_price: 'price',
      volume_24h: 'volume',
      high_24h: 'high',
      low_24h: 'low',
      open_24h: 'open',
    },
  },
  kraken_spot: {
    needsUA: false,
    extract: (d) => d.result?.XBTUSD || d.result?.XXBTZUSD || Object.values(d.result || {})[0],
    validate: (d) => d?.error?.length === 0 && !!d.result,
    fields: {
      last_price: 'c',
      volume_24h: 'v',
      quote_vol_24h: null,
      high_24h: 'h',
      low_24h: 'l',
      price_change_24h: null,
      price_change_pct: null,
    },
    transforms: {
      last_price: (v) => Array.isArray(v) ? parseFloat(v[0]) : null,
      volume_24h: (v) => Array.isArray(v) ? parseFloat(v[0]) : null,
      high_24h: (v) => Array.isArray(v) ? parseFloat(v[0]) : null,
      low_24h: (v) => Array.isArray(v) ? parseFloat(v[0]) : null,
    },
  },
};

// ======================================================================
// LS-Ratio parser configuration per exchange
// ======================================================================

const LSRATIO_PARSERS = {
  binance_perp: {
    needsUA: false,
    extract: (d) => Array.isArray(d) && d.length > 0 ? d[0] : null,
    validate: (d) => !!d,
    fields: {
      long_account_ratio: 'longAccount',
      short_account_ratio: 'shortAccount',
      long_short_ratio: 'longShortRatio',
      period: (raw) => raw.period || '1h',
    },
  },
  bybit_perp: {
    needsUA: true,
    extract: (d) => d.result?.list?.[0],
    validate: (d) => d.retCode === 0 && !!d.result?.list?.length,
    fields: {
      long_account_ratio: 'buyRatio',
      short_account_ratio: 'sellRatio',
      long_short_ratio: null, // computed below
      period: (raw) => raw.period || '1h',
      ts_str: null, // raw string via postProcess
    },
    defaults: { period: '1h' },
    postProcess(row, raw) {
      const la = parseFloat(raw.buyRatio);
      const sa = parseFloat(raw.sellRatio);
      row.long_account_ratio = la;
      row.short_account_ratio = sa;
      row.long_short_ratio = sa > 0 ? la / sa : null;
      // Preserve ts_str as raw string (not parseFloat'd)
      row.ts_str = raw.timestamp || null;
    },
  },
  okx_perp: {
    needsUA: true,
    extract: (d) => Array.isArray(d.data) && d.data.length > 0 ? d.data[0] : null,
    validate: (d) => Array.isArray(d.data) && d.data.length > 0,
    fields: {
      long_short_ratio: null, // index-based
      ts_str: null,           // index-based
    },
    defaults: { period: '1H', long_account_ratio: null, short_account_ratio: null },
    postProcess(row, raw) {
      if (Array.isArray(raw)) {
        row.ts_str = raw[0];
        row.long_short_ratio = raw[1] ? parseFloat(raw[1]) : null;
      }
    },
  },
};

// ======================================================================
// MarketDataCollector class
// ======================================================================

export class MarketDataCollector {
  /**
   * @param {string} outputBase - base directory
   * @param {Object} [options]
   * @param {number} [options.intervalMs=60000]
   */
  constructor(outputBase, options = {}) {
    this._intervalMs = options.intervalMs ?? 60000;
    /** @type {Map<string, BufferedWriter>} */
    this._writers = new Map();
    /** @type {Array<{market: string, type: string, fetchFn: () => Promise<Object>}>} */
    this._fetchers = [];
    this._timer = null;
    this._closed = false;
    this._outputBase = outputBase;

    // Cache of last ticker values per market (for premium computation)
    /** @type {Object<string, Object>} */
    this._lastTickers = {};
  }

  /**
   * Register a market for OHLCV + Ticker collection.
   * @param {string} market
   * @param {Object} cfg
   * @param {'spot'|'perp'} cfg.type
   * @param {Object} cfg.urls
   * @param {string} [cfg.urls.ohlcv]   - OHLCV REST URL
   * @param {string} [cfg.urls.ticker]  - 24h ticker REST URL
   * @param {Object} [cfg.collect]
   * @param {boolean} [cfg.collect.lsratio]
   * @param {boolean} [cfg.collect.takervol]
   */
  registerMarket(market, cfg) {
    // ── OHLCV ──
    if (cfg.urls?.ohlcv) {
      this._registerOhlcv(market, cfg.urls.ohlcv);
    }

    // ── 24h Ticker ──
    if (cfg.urls?.ticker) {
      this._registerTicker(market, cfg.urls.ticker);
    }

    // ── Long/Short Ratio (perp only) ──
    if (cfg.collect?.lsratio) {
      this._registerLsRatio(market, cfg.urls.lsratio);
    }

    // ── Taker Buy/Sell Volume (Binance perp only) ──
    if (cfg.collect?.takervol && market === 'binance_perp') {
      this._addFetcher('takervol', market, () => this._fetchBinanceTakerVol(cfg.urls.takervol));
    }
  }

  /**
   * Register the Coinbase Premium Index fetcher.
   * Depends on ticker data from binance_spot + coinbase_spot being registered.
   */
  registerPremium() {
    this._doPremium = true;
  }

  /** Start periodic collection. */
  start() {
    if (this._timer) return;
    this._tick();
    this._timer = setInterval(() => this._tick(), this._intervalMs);
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
    const promises = [];
    for (const writer of this._writers.values()) {
      promises.push(writer.close());
    }
    await Promise.allSettled(promises);
  }

  // ======================================================================
  // Internal: data-driven registration helpers
  // ======================================================================

  /** @private */
  _registerOhlcv(market, url) {
    switch (market) {
      case 'binance_spot':
      case 'binance_spot_usdc':
      case 'binance_perp':
        this._addFetcher('ohlcv', market, () => this._fetchBinanceKlines(url));
        break;
      case 'bybit_perp':
        this._addFetcher('ohlcv', market, () => this._fetchBybitKlines(url));
        break;
      case 'okx_perp':
        this._addFetcher('ohlcv', market, () => this._fetchOkxCandles(url));
        break;
      case 'bybit_spot':
        this._addFetcher('ohlcv', market, () => this._fetchBybitKlines(url));
        break;
      case 'okx_spot':
        this._addFetcher('ohlcv', market, () => this._fetchOkxCandles(url));
        break;
      case 'binance_coinm_perp':
      case 'binance_perp_btcusdc':
        this._addFetcher('ohlcv', market, () => this._fetchBinanceKlines(url));
        break;
      case 'kraken_spot':
        this._addFetcher('ohlcv', market, () => this._fetchKrakenCandles(url));
        break;
      case 'coinbase_spot':
        this._addFetcher('ohlcv', market, () => this._fetchCoinbaseCandles(url));
        break;
      case 'hyperliquid_perp':
        this._addFetcher('ohlcv', market, () => this._fetchHyperliquidCandles(url));
        break;
      default:
        console.warn(`[market-data] no OHLCV parser for market: ${market}`);
    }
  }

  /** @private */
  _registerTicker(market, url) {
    if (market === 'hyperliquid_perp') {
      // Hyperliquid uses a generic passthrough (no structured ticker endpoint)
      this._addFetcher('ticker', market, () => this._fetchGenericTicker(url));
      return;
    }
    // All other exchanges use the unified _fetchTicker with TICKER_PARSERS
    if (TICKER_PARSERS[market]) {
      this._addFetcher('ticker', market, () => this._fetchTicker(url, market));
    } else {
      console.warn(`[market-data] no ticker parser for market: ${market}`);
    }
  }

  /** @private */
  _registerLsRatio(market, url) {
    if (LSRATIO_PARSERS[market]) {
      this._addFetcher('lsratio', market, () => this._fetchLsRatio(url, market));
    } else {
      console.warn(`[market-data] no LS ratio parser for market: ${market}`);
    }
  }

  // ======================================================================
  // Internal: generic fetch with retry
  // ======================================================================

  /**
   * Fetch JSON from a URL with timeout and optional retry.
   * @param {string} url
   * @param {Object} [options]
   * @param {number} [options.retries=FETCH_RETRIES]
   * @param {Object} [options.headers]
   * @param {Object} [options.fetchOpts] - additional fetch options (method, body, etc.)
   * @returns {Promise<Object|null>} parsed JSON or null on failure
   */
  async _fetchJSON(url, { retries = FETCH_RETRIES, headers, fetchOpts } = {}) {
    for (let attempt = 0; attempt <= retries; attempt++) {
      try {
        const res = await fetch(url, {
          ...fetchOpts,
          headers: { ...headers },
          signal: AbortSignal.timeout(FETCH_TIMEOUT_MS),
        });
        if (!res.ok) {
          if (attempt === retries) {
            console.error(`[market-data] HTTP ${res.status} fetching ${url}`);
          }
          return null;
        }
        return await res.json();
      } catch (err) {
        if (attempt === retries) {
          console.error(`[market-data] fetch error (${attempt + 1}/${retries + 1}): ${url} — ${err.message}`);
          return null;
        }
      }
    }
    return null;
  }

  // ======================================================================
  // Internal: generic fetch + write dispatcher
  // ======================================================================

  _getWriter(subpath) {
    if (!this._writers.has(subpath)) {
      const writer = new BufferedWriter(
        `${this._outputBase}/${subpath}`,
        { flushIntervalMs: 1000, maxBufferLines: 100 },
      );
      this._writers.set(subpath, writer);
    }
    return this._writers.get(subpath);
  }

  _addFetcher(type, market, fetchFn) {
    this._fetchers.push({ type, market, fetchFn });
  }

  async _tick() {
    if (this._closed) return;
    const now = Date.now();
    const promises = [];

    for (const fetcher of this._fetchers) {
      promises.push(
        this._executeFetcher(fetcher, now).catch(err => {
          console.error(`[market-data] ${fetcher.market}/${fetcher.type} error: ${err.message}`);
        }),
      );
    }

    await Promise.allSettled(promises);

    // ── Premium Index (Coinbase - Binance) ──
    if (this._doPremium) {
      await this._computePremium(now).catch(err => {
        console.error(`[market-data] premium error: ${err.message}`);
      });
    }
  }

  async _executeFetcher(fetcher, now) {
    const row = await fetcher.fetchFn();
    if (!row) return;
    row.ts = now;
    row.market = fetcher.market;
    row.type = fetcher.type;
    const writer = this._getWriter(`${fetcher.type}/${fetcher.market}.jsonl`);
    await writer.write(row);
  }

  // ======================================================================
  // Premium computation
  // ======================================================================

  async _computePremium(now) {
    const binanceSpot = this._lastTickers['binance_spot'];
    const coinbase = this._lastTickers['coinbase_spot'];
    if (!binanceSpot?.last_price || !coinbase?.last_price) return;

    const premium = coinbase.last_price - binanceSpot.last_price;
    const premiumBps = binanceSpot.last_price > 0
      ? (premium / binanceSpot.last_price) * 10000
      : 0;

    const writer = this._getWriter('premium/coinbase_premium.jsonl');
    await writer.write({
      ts: now,
      type: 'premium',
      market: 'coinbase_spot',
      binance_price: binanceSpot.last_price,
      coinbase_price: coinbase.last_price,
      premium,
      premium_bps: Math.round(premiumBps * 100) / 100,
      binance_vol_24h: binanceSpot.volume_24h,
      coinbase_vol_24h: coinbase.volume_24h,
    });
  }

  // ======================================================================
  // Unified Ticker fetcher (config-driven)
  // ======================================================================

  /**
   * Unified ticker fetch. Uses TICKER_PARSERS config for field mapping.
   * @param {string} url
   * @param {string} market
   * @returns {Promise<Object|null>}
   */
  async _fetchTicker(url, market) {
    const parser = TICKER_PARSERS[market];
    if (!parser) return null;

    const opts = parser.needsUA
      ? { headers: { 'User-Agent': 'btc-receiver/v3.00' } }
      : {};
    const data = await this._fetchJSON(url, opts);
    if (!data) return null;

    const raw = parser.extract(data);
    if (!raw || !parser.validate(data)) return null;

    const row = {};
    for (const [key, field] of Object.entries(parser.fields)) {
      if (typeof field === 'function') {
        row[key] = field(raw);
      } else if (field !== null) {
        row[key] = parseFloat(raw[field]);
      }
    }

    // Apply transforms (parseInt etc.)
    if (parser.transforms) {
      for (const [key, fn] of Object.entries(parser.transforms)) {
        if (key in row) {
          row[key] = fn(raw[parser.fields[key]]);
        }
      }
    }

    this._lastTickers[market] = row;
    return row;
  }

  // ======================================================================
  // Unified LS-Ratio fetcher (config-driven)
  // ======================================================================

  /**
   * Unified LS-ratio fetch. Uses LSRATIO_PARSERS config.
   * @param {string} url
   * @param {string} market
   * @returns {Promise<Object|null>}
   */
  async _fetchLsRatio(url, market) {
    const parser = LSRATIO_PARSERS[market];
    if (!parser) return null;

    const opts = parser.needsUA
      ? { headers: { 'User-Agent': 'btc-receiver/v3.00' } }
      : {};
    const data = await this._fetchJSON(url, opts);
    if (!data) return null;

    const raw = parser.extract(data);
    if (!raw || !parser.validate(data)) return null;

    const row = {};
    for (const [key, field] of Object.entries(parser.fields)) {
      if (typeof field === 'function') {
        row[key] = field(raw);
      } else if (field !== null) {
        row[key] = parseFloat(raw[field]);
      }
    }

    // Apply defaults for missing fields
    if (parser.defaults) {
      for (const [key, val] of Object.entries(parser.defaults)) {
        if (!(key in row)) row[key] = val;
      }
    }

    // Apply post-process hook
    if (parser.postProcess) {
      parser.postProcess(row, raw);
    }

    return row;
  }

  // ======================================================================
  // Exchange-specific OHLCV fetchers
  // ======================================================================

  async _fetchBinanceKlines(url) {
    const data = await this._fetchJSON(url);
    if (!Array.isArray(data) || data.length === 0) return null;
    const k = data[0];
    return {
      open_time: k[0],
      open: parseFloat(k[1]),
      high: parseFloat(k[2]),
      low: parseFloat(k[3]),
      close: parseFloat(k[4]),
      volume: parseFloat(k[5]),
      quote_vol: parseFloat(k[7]),
      taker_buy_vol: parseFloat(k[9]),
      taker_buy_quote: parseFloat(k[10]),
    };
  }

  async _fetchBybitKlines(url) {
    const data = await this._fetchJSON(url);
    if (!data || data.retCode !== 0 || !data.result?.list?.length) return null;
    const k = data.result.list[0];
    return {
      open_time: parseInt(k[0]),
      open: parseFloat(k[1]),
      high: parseFloat(k[2]),
      low: parseFloat(k[3]),
      close: parseFloat(k[4]),
      volume: parseFloat(k[5]),
      turnover: parseFloat(k[6]),
    };
  }

  async _fetchOkxCandles(url) {
    const data = await this._fetchJSON(url);
    if (!data?.data?.length) return null;
    const k = data.data[0];
    return {
      open_time: parseInt(k[0]),
      open: parseFloat(k[1]),
      high: parseFloat(k[2]),
      low: parseFloat(k[3]),
      close: parseFloat(k[4]),
      vol: parseFloat(k[5]),
      vol_currency: parseFloat(k[6]),
      vol_currency_pair: parseFloat(k[7]),
      confirm: k[8],
    };
  }

  async _fetchKrakenCandles(url) {
    const d = await this._fetchJSON(url);
    if (!d?.result) return null;
    const pair = d.result.pair || Object.keys(d.result).find(k => k !== 'last');
    const rows = d.result[pair] || [];
    const k = rows.at?.(-1) || rows[rows.length - 1];
    if (!Array.isArray(k)) return null;
    return {
      open_time: parseInt(k[0], 10),
      open: parseFloat(k[1]),
      high: parseFloat(k[2]),
      low: parseFloat(k[3]),
      close: parseFloat(k[4]),
      volume: parseFloat(k[6]),
    };
  }

  async _fetchCoinbaseCandles(url) {
    const data = await this._fetchJSON(url, {
      headers: { 'User-Agent': 'btc-receiver/v3.00' },
    });
    if (!Array.isArray(data) || data.length === 0) return null;
    const k = data[0];
    return {
      open_time: k[0] * 1000,
      low: parseFloat(k[1]),
      high: parseFloat(k[2]),
      open: parseFloat(k[3]),
      close: parseFloat(k[4]),
      volume: parseFloat(k[5]),
    };
  }

  async _fetchHyperliquidCandles(url) {
    const now = Date.now();
    const data = await this._fetchJSON(url, {
      fetchOpts: {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          type: 'candleSnapshot',
          req: {
            coin: 'BTC',
            interval: '1m',
            startTime: now - 120000,
            endTime: now,
          },
        }),
      },
    });
    if (!Array.isArray(data) || data.length === 0) return null;
    const k = data[data.length - 1];
    return {
      open_time: parseInt(k.t),
      open: parseFloat(k.o),
      high: parseFloat(k.h),
      low: parseFloat(k.l),
      close: parseFloat(k.c),
      volume: parseFloat(k.v),
    };
  }

  // ======================================================================
  // Legacy exchange-specific Ticker fetcher aliases
  // Kept for backward compatibility with existing tests.
  // ======================================================================

  async _fetchBinanceTicker(url, market = 'binance_spot') {
    return this._fetchTicker(url, market);
  }

  async _fetchBybitTicker(url, market = 'bybit_perp') {
    return this._fetchTicker(url, market);
  }

  async _fetchOkxTicker(url, market = 'okx_perp') {
    return this._fetchTicker(url, market);
  }

  async _fetchKrakenTicker(url, market = 'kraken_spot') {
    return this._fetchTicker(url, market);
  }

  async _fetchCoinbaseTicker(url, market = 'coinbase_spot') {
    return this._fetchTicker(url, market);
  }

  async _fetchGenericTicker(url) {
    const d = await this._fetchJSON(url);
    if (!d) return null;
    return { raw: d };
  }

  // ======================================================================
  // Legacy LS-Ratio fetcher aliases
  // ======================================================================

  async _fetchBinanceLSRatio(url) {
    return this._fetchLsRatio(url, 'binance_perp');
  }

  async _fetchBybitLSRatio(url) {
    return this._fetchLsRatio(url, 'bybit_perp');
  }

  async _fetchOkxLSRatio(url) {
    return this._fetchLsRatio(url, 'okx_perp');
  }

  // ======================================================================
  // Taker Buy/Sell Volume fetchers (perp only)
  // ======================================================================

  async _fetchBinanceTakerVol(url) {
    const data = await this._fetchJSON(url);
    if (!Array.isArray(data) || data.length === 0) return null;
    const r = data[0];
    return {
      buy_vol_24h: parseFloat(r.buyVol),
      sell_vol_24h: parseFloat(r.sellVol),
      buy_sell_ratio: parseFloat(r.buySellRatio),
      period: r.period || '1h',
    };
  }
}
