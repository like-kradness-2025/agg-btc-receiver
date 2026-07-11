// lib/burst-reducer/pending-block-manager.mjs — Loading/saving checkpoint state
// Follows plan Task 8

import { existsSync, readFileSync } from 'node:fs';
import { join } from 'node:path';

/**
 * Load checkpoint for a market.
 * @param {string} market
 * @param {string} [derivedDir] - override derived dir (default from schema)
 * @returns {Object|null} checkpoint or null
 */
export function loadCheckpoint(market, derivedDir) {
  const base = derivedDir || 'data/derived/burst_features_v1';
  const path = join(base, 'manifests/checkpoints', `${market}.json`);
  if (!existsSync(path)) return null;
  try {
    return JSON.parse(readFileSync(path, 'utf8'));
  } catch {
    return null;
  }
}
