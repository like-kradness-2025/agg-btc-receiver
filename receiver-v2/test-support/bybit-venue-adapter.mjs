/**
 * The real built-in bybit adapter, exposed under the module seam `bin/role.mjs` resolves
 * (`createAdapter`), for run-level tests.
 *
 * The child process builds its adapter from a module path; the built-in module exports
 * `createBybitPerpAdapter`, which is not the seam's name, so this one-line shim renames it - it adds
 * no behaviour of its own, so the test runs the *same code* a real deployment would.
 */

import { createBybitPerpAdapter } from '../src/ingest/venues/bybit.mjs';

export function createAdapter({ market = 'bybit_perp', symbol = 'BTCUSDT', url, ...rest } = {}) {
  return createBybitPerpAdapter({ market, symbol, url, ...rest });
}
