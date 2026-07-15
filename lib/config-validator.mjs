// lib/config-validator.mjs — Runtime config structural validation
//
// Receiver-only fail-closed boundary: validates required object/array/string/
// number/URL shapes, disabled market structure, output paths/flush intervals,
// and malformed/null/missing fields before worker startup.
//
// Must be called immediately after JSON.parse, before any config access.
// On failure the caller MUST NOT spawn workers, create output directories,
// or write any raw data — error is deterministic, actionable, non-zero exit.
//
// Schema derived from config.v3.json and the C1 receiver contract.
// Do NOT infer new downstream schema beyond what the existing code accesses.

/**
 * Validate runtime config structure.
 *
 * @param {unknown} config - Parsed JSON config object (from config.v3.json).
 * @returns {{ valid: true } | { valid: false, errors: string[] }}
 */
export function validateConfig(config) {
  /** @type {string[]} */
  const errors = [];

  // ── Top-level ──────────────────────────────────────────────────────────
  if (config === null || config === undefined || typeof config !== 'object' || Array.isArray(config)) {
    errors.push('config must be a non-null object');
    return { valid: false, errors };
  }

  // ── config.markets ─────────────────────────────────────────────────────
  if (config.markets === null || config.markets === undefined || typeof config.markets !== 'object' || Array.isArray(config.markets)) {
    errors.push('config.markets must be a non-null object');
  } else {
    const marketKeys = Object.keys(config.markets);
    if (marketKeys.length === 0) {
      errors.push('config.markets must have at least one market entry');
    }

    for (const key of marketKeys) {
      const m = config.markets[key];
      const prefix = `config.markets.${key}`;

      if (m === null || m === undefined || typeof m !== 'object' || Array.isArray(m)) {
        errors.push(`${prefix} must be a non-null object`);
        continue;
      }

      // symbol: required non-empty string for every market (enabled or disabled)
      if (typeof m.symbol !== 'string' || m.symbol.length === 0) {
        errors.push(`${prefix}.symbol must be a non-empty string`);
      }

      // wsUrl: required non-empty string for every market
      // WebSocket URLs start with ws:// or wss://
      if (typeof m.wsUrl !== 'string' || m.wsUrl.length === 0) {
        errors.push(`${prefix}.wsUrl must be a non-empty string`);
      } else if (!m.wsUrl.startsWith('ws://') && !m.wsUrl.startsWith('wss://')) {
        errors.push(`${prefix}.wsUrl must start with ws:// or wss://`);
      }

      // restUrl: optional string (may be empty per existing config)
      if ('restUrl' in m && typeof m.restUrl !== 'string') {
        errors.push(`${prefix}.restUrl must be a string`);
      }

      // enabled: optional boolean field — if present must be boolean
      if ('enabled' in m && typeof m.enabled !== 'boolean') {
        errors.push(`${prefix}.enabled must be a boolean`);
      }

      // depthLimit: optional number — if present must be a non-negative integer
      if ('depthLimit' in m) {
        if (!Number.isInteger(m.depthLimit) || m.depthLimit < 0) {
          errors.push(`${prefix}.depthLimit must be a non-negative integer`);
        }
      }
    }
  }

  // ── config.output ──────────────────────────────────────────────────────
  if (config.output === null || config.output === undefined || typeof config.output !== 'object' || Array.isArray(config.output)) {
    errors.push('config.output must be a non-null object');
  } else {
    // base_path: required non-empty string
    if (typeof config.output.base_path !== 'string' || config.output.base_path.length === 0) {
      errors.push('config.output.base_path must be a non-empty string');
    }

    // Flush interval fields: required positive integers
    const flushFields = ['flush_trades_ms', 'flush_book_ms', 'flush_liquidations_ms', 'flush_health_ms'];
    for (const field of flushFields) {
      if (!(field in config.output)) {
        errors.push(`config.output.${field} is required`);
      } else if (!Number.isInteger(config.output[field]) || config.output[field] <= 0) {
        errors.push(`config.output.${field} must be a positive integer, got ${JSON.stringify(config.output[field])}`);
      }
    }
  }

  // ── config.tick (optional) ─────────────────────────────────────────────
  if (config.tick !== undefined) {
    if (config.tick === null || Array.isArray(config.tick) || typeof config.tick !== 'object') {
      errors.push('config.tick must be a non-null object when present');
    } else if (config.tick.market_data_ms !== undefined) {
      if (!Number.isInteger(config.tick.market_data_ms) || config.tick.market_data_ms <= 0) {
        errors.push('config.tick.market_data_ms must be a positive integer when present');
      }
    }
  }

  // ── config.fairprice (optional, per C1 contract) ──────────────────────
  if (config.fairprice !== undefined) {
    if (config.fairprice === null || Array.isArray(config.fairprice) || typeof config.fairprice !== 'object') {
      errors.push('config.fairprice must be a non-null object when present');
    } else {
      if ('snapshot_interval_ms' in config.fairprice) {
        if (!Number.isInteger(config.fairprice.snapshot_interval_ms) || config.fairprice.snapshot_interval_ms <= 0) {
          errors.push('config.fairprice.snapshot_interval_ms must be a positive integer when present');
        }
      }
      if ('book_snapshot_ms' in config.fairprice) {
        if (!Number.isInteger(config.fairprice.book_snapshot_ms) || config.fairprice.book_snapshot_ms <= 0) {
          errors.push('config.fairprice.book_snapshot_ms must be a positive integer when present');
        }
      }
    }
  }

  return errors.length === 0
    ? { valid: true }
    : { valid: false, errors };
}
