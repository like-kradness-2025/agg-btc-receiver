/**
 * The real built-in kraken adapter, exposed under the module seam `bin/role.mjs` resolves
 * (`createAdapter`), for the real-process through test (stage 5c).
 *
 * The child process builds its adapter from a module path. The built-in module exports
 * `createKrakenAdapter`, which is not the seam's name, so this one-line shim renames it - it adds no
 * behaviour of its own, so the through test runs the *same code* a real deployment would.
 */

import { createKrakenAdapter } from '../src/ingest/venues/kraken.mjs';

export function createAdapter({ symbol = 'XBT/USD', url, ...rest } = {}) {
  return createKrakenAdapter({ symbol, url, ...rest });
}
