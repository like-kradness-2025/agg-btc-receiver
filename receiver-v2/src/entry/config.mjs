/**
 * The receiver's configuration: the file the process is started with, and the adapter the venue names.
 *
 * The process is configured from a file and only from a file (§5.7's minimal shape): `--config <JSON>`
 * names the venue, the market, the stream and where this run's data goes. Nothing is read from
 * the environment and nothing is passed on the command line beyond the path - a deployment describes
 * itself in one place, and a command line that could override a destination is a second source of truth
 * for where the canonical data goes.
 *
 * Since the receiver is three role processes (stage 5d, §5.8 ruling ⑭), the canonical data lives in
 * three databases - one per role, each opened and held by exactly one process (ruling ⑮). The config
 * therefore names all three explicitly, under `stores: { ingest, organize, book }`. The three paths
 * are named rather than derived from one base path on purpose: a derived sibling would be a naming
 * convention this file invented, and a second owner of the canonical data must be a path the
 * deployment chose, not one this package guessed. The spool belongs to ingest and keeps its own key,
 * `spoolDir`.
 *
 * There is deliberately no raw destination. The old six-column world had a raw writer
 * (`lib/raw-sqlite-writer.mjs`); this package carries only its reception half, and the entrance has no
 * raw writer at all. A `"raw"` key is therefore not supported: it is refused outright rather than
 * ignored, so a deployment that asks for a raw file fails loudly instead of being told a run was
 * recorded when nothing was saved.
 */

import { readFileSync } from 'node:fs';

import { createKrakenAdapter } from '../ingest/venues/kraken.mjs';
import { createBitfinexAdapter } from '../ingest/venues/bitfinex.mjs';
import { createBinanceSpotAdapter, createBinanceFuturesAdapter, createBinanceCoinMFuturesAdapter } from '../ingest/venues/binance-spot.mjs';
import { createBybitPerpAdapter, createBybitSpotAdapter } from '../ingest/venues/bybit.mjs';
import { createOkxPerpAdapter, createOkxSpotAdapter } from '../ingest/venues/okx.mjs';
import { createCoinbaseAdapter } from '../ingest/venues/coinbase.mjs';
import { createHyperliquidAdapter } from '../ingest/venues/hyperliquid.mjs';
import { createBitstampAdapter } from '../ingest/venues/bitstamp.mjs';

/** The venues this package can receive from. Adding a venue is adding its adapter here. */
const ADAPTERS = Object.freeze({
  kraken: createKrakenAdapter,
  bitfinex: createBitfinexAdapter,
  binance_spot: createBinanceSpotAdapter,
  binance_spot_usdc: (options) => createBinanceSpotAdapter({ ...options, symbol: options.symbol ?? 'BTCUSDC' }),
  binance_spot_fdusd: (options) => createBinanceSpotAdapter({ ...options, symbol: options.symbol ?? 'BTCFDUSD' }),
  binance_perp: createBinanceFuturesAdapter,
  binance_perp_btcusdc: (options) => createBinanceFuturesAdapter({ ...options, symbol: options.symbol ?? 'BTCUSDC' }),
  binance_coinm_perp: createBinanceCoinMFuturesAdapter,
  bybit_perp: createBybitPerpAdapter,
  bybit_spot: createBybitSpotAdapter,
  okx_perp: createOkxPerpAdapter,
  okx_spot: createOkxSpotAdapter,
  coinbase_spot: createCoinbaseAdapter,
  hyperliquid_perp: createHyperliquidAdapter,
  bitstamp_spot: createBitstampAdapter,
});

/**
 * Venues that are built but must not be operated, each with the reason. C11: the Bitfinex adapter is
 * "当面使用禁止" until it is rewritten against the official spec (chanId management, book/trade
 * separation) - `parse()` drops `[chanId,[price,count,amount]]` book frames and misreads a trade
 * snapshot as a book, treating the trade id as a price. A deployment that named it would corrupt the
 * board, so a config that names it is refused here, the same way the `"raw"` key is: a clear error
 * before anything is opened, not a run that records data nobody can trust.
 */
const DISABLED_VENUES = Object.freeze({
  binance_coinm_perp: 'COIN-M is implemented read-only but remains disabled until production admission is verified',
});

const REQUIRED = Object.freeze(['venue', 'market', 'spoolDir']);

/** The three roles whose databases this run owns: one file per role, one process per file (ruling ⑮). */
export const STORE_ROLES = Object.freeze(['ingest', 'organize', 'book']);

/**
 * The bounds a config's `startupDeadlineMs` must sit within. §5.7 fixes 60_000 ms as the default and
 * leaves the value changeable by configuration; the bounds keep a change from reading as "no deadline"
 * (0 or negative) or as "no startup window at all". One second is the shortest window that can still
 * contain a connection attempt; one hour is the longest a startup should be allowed to sit before the
 * run ends non-zero.
 */
export const MIN_STARTUP_DEADLINE_MS = 1_000;
export const MAX_STARTUP_DEADLINE_MS = 3_600_000;

/** The venues a config may name, for a caller that wants to report the choice. */
export function knownVenues() {
  return Object.keys(ADAPTERS).filter((venue) => !Object.prototype.hasOwnProperty.call(DISABLED_VENUES, venue));
}

/** Refuse a venue that is built but must not be operated. Shared so loadConfig and adapterFor agree. */
function refuseIfDisabled(venue) {
  if (Object.prototype.hasOwnProperty.call(DISABLED_VENUES, venue)) {
    throw new TypeError(`the venue "${venue}" is not supported: ${DISABLED_VENUES[venue]}`);
  }
}

/**
 * Read and validate the config file. Every refusal is raised before anything is opened, so a config
 * that cannot run leaves no store and no spool behind for the next attempt to trip over.
 */
export function loadConfig(configPath) {
  if (typeof configPath !== 'string' || configPath.length === 0) {
    throw new TypeError('a config path is required (--config <path-to-json>)');
  }
  let text;
  try {
    text = readFileSync(configPath, 'utf8');
  } catch (error) {
    throw new Error(`the config file could not be read (${configPath}): ${error.message}`);
  }
  let raw;
  try {
    raw = JSON.parse(text);
  } catch (error) {
    throw new Error(`the config file is not valid JSON (${configPath}): ${error.message}`);
  }
  if (raw === null || typeof raw !== 'object' || Array.isArray(raw)) {
    throw new TypeError('the config must be a JSON object');
  }
  // A raw destination is not supported: this package's entrance no longer saves a raw file. The key is
  // refused rather than ignored, so a deployment cannot carry a `"raw"` path and be told a run was
  // recorded while nothing was written to it. Removing the write (rather than coordinating two writers on
  // it) is the point: there is no raw path here to be dangerous.
  if (Object.prototype.hasOwnProperty.call(raw, 'raw')) {
    throw new TypeError(
      'the config\u2019s "raw" is not supported: this receiver does not save a raw file (remove the "raw" key)',
    );
  }
  for (const key of REQUIRED) {
    if (typeof raw[key] !== 'string' || raw[key].length === 0) {
      throw new TypeError(`the config needs a non-empty "${key}"`);
    }
  }
  // The three role databases. Named explicitly: each is opened and held by exactly one role process
  // (ruling ⑮), so the deployment - not a naming convention this file invents - says where each goes.
  if (raw.stores === null || typeof raw.stores !== 'object' || Array.isArray(raw.stores)) {
    throw new TypeError('the config needs a "stores" object naming the three role databases (ingest, organize, book)');
  }
  const stores = {};
  for (const role of STORE_ROLES) {
    const rolePath = raw.stores[role];
    if (typeof rolePath !== 'string' || rolePath.length === 0) {
      throw new TypeError(`the config needs a non-empty "stores.${role}"`);
    }
    stores[role] = rolePath;
  }
  const stream = raw.stream ?? (raw.venue === 'bitfinex' ? 'book' : 'trades');
  if (typeof stream !== 'string' || stream.length === 0) {
    throw new TypeError('the config\'s "stream" must be a non-empty string');
  }
  if (!Object.prototype.hasOwnProperty.call(ADAPTERS, raw.venue)) {
    throw new TypeError(`unknown venue "${raw.venue}"; known venues: ${knownVenues().join(', ')}`);
  }
  refuseIfDisabled(raw.venue);
  // The startup deadline is the one timing knob a deployment may set (§5.7: 60_000 ms is the default and
  // the value stays changeable). It is validated here, before anything is opened, so an out-of-range value
  // fails loudly rather than silently running with no deadline.
  let startupDeadlineMs;
  if (Object.prototype.hasOwnProperty.call(raw, 'startupDeadlineMs')) {
    const value = raw.startupDeadlineMs;
    if (
      typeof value !== 'number' ||
      !Number.isInteger(value) ||
      value < MIN_STARTUP_DEADLINE_MS ||
      value > MAX_STARTUP_DEADLINE_MS
    ) {
      throw new TypeError(
        `the config's "startupDeadlineMs" must be an integer between ${MIN_STARTUP_DEADLINE_MS} and ${MAX_STARTUP_DEADLINE_MS} (got ${JSON.stringify(value)})`,
      );
    }
    startupDeadlineMs = value;
  }
  // The supervisor's router socket. Optional: a deployment that does not name one has it derived next to
  // the spool (see the entrance). A value that is present must be a non-empty string, so a half-written
  // key fails loudly rather than silently falling back to the derived path.
  let routerPath;
  if (Object.prototype.hasOwnProperty.call(raw, 'routerPath')) {
    if (typeof raw.routerPath !== 'string' || raw.routerPath.length === 0) {
      throw new TypeError('the config\'s "routerPath" must be a non-empty string when present');
    }
    routerPath = raw.routerPath;
  }
  // Set 7a: where the canonical raw is written. Optional - a deployment that names no `rawDir` runs
  // without a raw writer (the pre-Set-7 behaviour), so an existing config keeps working unchanged. A
  // value that is present must be a non-empty string so a half-written key fails loudly rather than
  // silently writing the raw nowhere.
  let rawDir;
  if (Object.prototype.hasOwnProperty.call(raw, 'rawDir')) {
    if (typeof raw.rawDir !== 'string' || raw.rawDir.length === 0) {
      throw new TypeError('the config\'s "rawDir" must be a non-empty string when present');
    }
    rawDir = raw.rawDir;
  }
  // Set 8: the auxiliary open-interest REST poll interval. Optional - absent means the poller is off
  // (the pre-Set-8 behaviour); a positive integer starts the 30 s-style poll. A present value must be
  // a positive integer so a half-written key fails loudly rather than silently disabling the poll.
  let oiPollIntervalMs;
  if (Object.prototype.hasOwnProperty.call(raw, 'oiPollIntervalMs')) {
    const value = raw.oiPollIntervalMs;
    if (typeof value !== 'number' || !Number.isInteger(value) || value <= 0) {
      throw new TypeError('the config\'s "oiPollIntervalMs" must be a positive integer when present');
    }
    oiPollIntervalMs = value;
  }
  return Object.freeze({
    venue: raw.venue,
    market: raw.market,
    stream,
    stores: Object.freeze(stores),
    spoolDir: raw.spoolDir,
    // Only present when the file names it: absent means the supervisor applies its own default, so the
    // entrance does not turn a missing key into a value the deployment never chose.
    ...(startupDeadlineMs === undefined ? {} : { startupDeadlineMs }),
    ...(routerPath === undefined ? {} : { routerPath }),
    ...(rawDir === undefined ? {} : { rawDir }),
    ...(oiPollIntervalMs === undefined ? {} : { oiPollIntervalMs }),
    // The venue-specific extras, only when the file carries them. Kraken needs a symbol; Bitfinex
    // defaults to tBTCUSD. A url is optional and defaults inside the adapter.
    symbol: typeof raw.symbol === 'string' && raw.symbol.length > 0 ? raw.symbol : undefined,
    url: typeof raw.url === 'string' && raw.url.length > 0 ? raw.url : undefined,
    restUrl: typeof raw.restUrl === 'string' && raw.restUrl.length > 0 ? raw.restUrl : undefined,
  });
}

/** Build the adapter the config names, refusing a stream the adapter cannot actually carry. */
export function adapterFor(config) {
  // Refused here too: a caller that built a config object by hand must not be able to reach the disabled
  // adapter through this function either. Same treatment as loadConfig.
  refuseIfDisabled(config.venue);
  const build = ADAPTERS[config.venue];
  const options = { market: config.market };
  if (config.symbol !== undefined) options.symbol = config.symbol;
  if (config.url !== undefined) options.url = config.url;
  if (config.restUrl !== undefined) options.restUrl = config.restUrl;
  const adapter = build(options);
  // A structure that organizes one stream while its adapter carries another can only produce frames the
  // board refuses - after they have been written to the raw. Refuse here, before anything is opened.
  if (adapter.stream !== config.stream) {
    const detail = config.venue === 'bitfinex'
      ? 'not supported: Bitfinex is book-only; the configured stream must be book'
      : `the ${config.venue} adapter carries the ${adapter.stream} stream, but the config names ${config.stream}`;
    throw new Error(detail);
  }
  return adapter;
}
