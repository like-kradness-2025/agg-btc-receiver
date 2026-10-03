/**
 * The receiver's configuration: the file the process is started with, and the adapter the venue names.
 *
 * The process is configured from a file and only from a file (§5.7's minimal shape): `--config <JSON>`
 * names the store, the spool, the venue, the market and the stream this run is for. Nothing is read from
 * the environment and nothing is passed on the command line beyond the path - a deployment describes
 * itself in one place, and a command line that could override a destination is a second source of truth
 * for where the canonical data goes.
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

/** The venues this package can receive from. Adding a venue is adding its adapter here. */
const ADAPTERS = Object.freeze({
  kraken: createKrakenAdapter,
  bitfinex: createBitfinexAdapter,
});

const REQUIRED = Object.freeze(['venue', 'market', 'database', 'spoolDir']);

/** The venues a config may name, for a caller that wants to report the choice. */
export function knownVenues() {
  return Object.keys(ADAPTERS);
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
  const stream = raw.stream ?? 'trades';
  if (typeof stream !== 'string' || stream.length === 0) {
    throw new TypeError('the config\'s "stream" must be a non-empty string');
  }
  if (!Object.prototype.hasOwnProperty.call(ADAPTERS, raw.venue)) {
    throw new TypeError(`unknown venue "${raw.venue}"; known venues: ${knownVenues().join(', ')}`);
  }
  return Object.freeze({
    venue: raw.venue,
    market: raw.market,
    stream,
    database: raw.database,
    spoolDir: raw.spoolDir,
    // The venue-specific extras, only when the file carries them. Kraken needs a symbol; Bitfinex
    // defaults to tBTCUSD. A url is optional and defaults inside the adapter.
    symbol: typeof raw.symbol === 'string' && raw.symbol.length > 0 ? raw.symbol : undefined,
    url: typeof raw.url === 'string' && raw.url.length > 0 ? raw.url : undefined,
  });
}

/** Build the adapter the config names, refusing a stream the adapter cannot actually carry. */
export function adapterFor(config) {
  const build = ADAPTERS[config.venue];
  const options = { market: config.market };
  if (config.symbol !== undefined) options.symbol = config.symbol;
  if (config.url !== undefined) options.url = config.url;
  const adapter = build(options);
  // A structure that organizes one stream while its adapter carries another can only produce frames the
  // board refuses - after they have been written to the raw. Refuse here, before anything is opened.
  if (adapter.stream !== config.stream) {
    throw new Error(
      `the ${config.venue} adapter carries the ${adapter.stream} stream, but the config names ${config.stream}`,
    );
  }
  return adapter;
}
