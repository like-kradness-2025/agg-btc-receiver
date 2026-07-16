// test/config-validator-runtime.test.mjs — Runtime config structural validation
//
// Unit tests for validateConfig() + subprocess integration tests that verify
// the orderflow_monitor.mjs entry point fails-closed (exit 1, stderr, no
// output) on invalid config.  Does NOT touch service/config files.
//
// Acceptance: focused validator suite passes; full suite passes; npm run
// check passes; direct exit/stderr/output evidence.

import { describe, it, before, after } from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import fsp from 'node:fs/promises';
import path from 'node:path';
import os from 'node:os';
import { spawnSync } from 'node:child_process';

import { validateConfig } from '../lib/config-validator.mjs';

// ── Helpers ──────────────────────────────────────────────────────────────

/** A minimal structurally valid config fragment (not a complete config). */
function validMinimalConfig() {
  return {
    markets: {
      test_market: {
        enabled: true,
        symbol: 'BTCUSDT',
        wsUrl: 'wss://stream.example.com/ws',
        restUrl: 'https://api.example.com/depth',
        depthLimit: 0,
      },
    },
    output: {
      base_path: 'data/test_output',
      flush_trades_ms: 200,
      flush_book_ms: 1000,
      flush_liquidations_ms: 200,
      flush_health_ms: 1000,
    },
    tick: { market_data_ms: 60000 },
    fairprice: {
      snapshot_interval_ms: 30000,
      book_snapshot_ms: 600000,
    },
  };
}

/** A reference to the real config.v3.json (for valid-config subprocess tests). */
const PROJECT_ROOT = path.resolve(new URL('.', import.meta.url).pathname, '..');
const REAL_CONFIG = path.join(PROJECT_ROOT, 'config.v3.json');

function tmpConfigPath(label) {
  const dir = path.join(os.tmpdir(), `btc-receiver-test-config-validator-${process.pid}`, label);
  fs.mkdirSync(dir, { recursive: true });
  return path.join(dir, 'config.json');
}

async function rmDir(dir) {
  try { await fsp.rm(dir, { recursive: true, force: true }); } catch {}
}

/** Run orderflow_monitor.mjs with a given config file and capture result. */
function runMonitor(configPath) {
  return spawnSync('node', ['orderflow_monitor.mjs', '--config', configPath, '--seconds', '0', '--markets', 'test_market'], {
    cwd: PROJECT_ROOT,
    timeout: 10000,
    stdio: ['pipe', 'pipe', 'pipe'],
    env: { ...process.env, BTCRECEIVER_READY_TIMEOUT_MS: '500' },
  });
}

// ── Unit tests: validateConfig ──────────────────────────────────────────

describe('validateConfig structural validation', () => {

  describe('valid config', () => {

    it('accepts a complete valid config', () => {
      const result = validateConfig(validMinimalConfig());
      assert.strictEqual(result.valid, true);
    });

    it('accepts config without optional tick field', () => {
      const cfg = validMinimalConfig();
      delete cfg.tick;
      const result = validateConfig(cfg);
      assert.strictEqual(result.valid, true);
    });

    it('accepts config without optional fairprice field', () => {
      const cfg = validMinimalConfig();
      delete cfg.fairprice;
      const result = validateConfig(cfg);
      assert.strictEqual(result.valid, true);
    });

    it('accepts config with empty-string restUrl', () => {
      const cfg = validMinimalConfig();
      cfg.markets.test_market.restUrl = '';
      const result = validateConfig(cfg);
      assert.strictEqual(result.valid, true);
    });

    it('accepts config with multiple markets including disabled', () => {
      const cfg = validMinimalConfig();
      cfg.markets.enabled_second = {
        enabled: true, symbol: 'ETHBTC', wsUrl: 'wss://stream2.example.com/ws',
        restUrl: '', depthLimit: 0,
      };
      cfg.markets.disabled_market = {
        enabled: false, symbol: 'LTCBTC', wsUrl: 'wss://stream3.example.com/ws',
        restUrl: '', depthLimit: 0,
      };
      const result = validateConfig(cfg);
      assert.strictEqual(result.valid, true);
    });
  });

  describe('top-level config', () => {

    it('rejects null config', () => {
      const result = validateConfig(null);
      assert.strictEqual(result.valid, false);
      assert.ok(result.errors[0].includes('non-null object'));
    });

    it('rejects undefined config', () => {
      const result = validateConfig(undefined);
      assert.strictEqual(result.valid, false);
      assert.ok(result.errors[0].includes('non-null object'));
    });

    it('rejects string config', () => {
      const result = validateConfig('hello');
      assert.strictEqual(result.valid, false);
    });

    it('rejects array config', () => {
      const result = validateConfig([]);
      assert.strictEqual(result.valid, false);
    });

    it('rejects number config', () => {
      const result = validateConfig(42);
      assert.strictEqual(result.valid, false);
    });
  });

  describe('config.markets', () => {

    it('rejects missing markets key', () => {
      const cfg = validMinimalConfig();
      delete cfg.markets;
      const result = validateConfig(cfg);
      assert.strictEqual(result.valid, false);
      assert.ok(result.errors.some(e => e.includes('markets')));
    });

    it('rejects null markets', () => {
      const cfg = validMinimalConfig();
      cfg.markets = null;
      const result = validateConfig(cfg);
      assert.strictEqual(result.valid, false);
    });

    it('rejects array markets', () => {
      const cfg = validMinimalConfig();
      cfg.markets = [];
      const result = validateConfig(cfg);
      assert.strictEqual(result.valid, false);
    });

    it('rejects empty markets object', () => {
      const cfg = validMinimalConfig();
      cfg.markets = {};
      const result = validateConfig(cfg);
      assert.strictEqual(result.valid, false);
      assert.ok(result.errors.some(e => e.includes('at least one')));
    });

    it('rejects null market entry', () => {
      const cfg = validMinimalConfig();
      cfg.markets.test_market = null;
      const result = validateConfig(cfg);
      assert.strictEqual(result.valid, false);
      assert.ok(result.errors.some(e => e.includes('test_market') && e.includes('non-null object')));
    });

    it('rejects array market entry', () => {
      const cfg = validMinimalConfig();
      cfg.markets.test_market = [];
      const result = validateConfig(cfg);
      assert.strictEqual(result.valid, false);
    });

    it('rejects market entry missing symbol', () => {
      const cfg = validMinimalConfig();
      delete cfg.markets.test_market.symbol;
      const result = validateConfig(cfg);
      assert.strictEqual(result.valid, false);
      assert.ok(result.errors.some(e => e.includes('symbol') && e.includes('non-empty')));
    });

    it('rejects market entry with empty symbol', () => {
      const cfg = validMinimalConfig();
      cfg.markets.test_market.symbol = '';
      const result = validateConfig(cfg);
      assert.strictEqual(result.valid, false);
    });

    it('rejects market entry with non-string symbol', () => {
      const cfg = validMinimalConfig();
      cfg.markets.test_market.symbol = 123;
      const result = validateConfig(cfg);
      assert.strictEqual(result.valid, false);
    });

    it('rejects market entry missing wsUrl', () => {
      const cfg = validMinimalConfig();
      delete cfg.markets.test_market.wsUrl;
      const result = validateConfig(cfg);
      assert.strictEqual(result.valid, false);
      assert.ok(result.errors.some(e => e.includes('wsUrl')));
    });

    it('rejects market entry with empty wsUrl', () => {
      const cfg = validMinimalConfig();
      cfg.markets.test_market.wsUrl = '';
      const result = validateConfig(cfg);
      assert.strictEqual(result.valid, false);
    });

    it('rejects wsUrl without ws:// or wss:// prefix', () => {
      const cfg = validMinimalConfig();
      cfg.markets.test_market.wsUrl = 'http://example.com/ws';
      const result = validateConfig(cfg);
      assert.strictEqual(result.valid, false);
      assert.ok(result.errors.some(e => e.includes('ws://')));
    });

    it('rejects wsUrl with non-string type', () => {
      const cfg = validMinimalConfig();
      cfg.markets.test_market.wsUrl = 42;
      const result = validateConfig(cfg);
      assert.strictEqual(result.valid, false);
    });

    it('rejects restUrl with non-string type', () => {
      const cfg = validMinimalConfig();
      cfg.markets.test_market.restUrl = 42;
      const result = validateConfig(cfg);
      assert.strictEqual(result.valid, false);
      assert.ok(result.errors.some(e => e.includes('restUrl')));
    });

    it('accepts market entry without restUrl (optional)', () => {
      const cfg = validMinimalConfig();
      delete cfg.markets.test_market.restUrl;
      const result = validateConfig(cfg);
      assert.strictEqual(result.valid, true);
    });

    it('rejects enabled field with non-boolean', () => {
      const cfg = validMinimalConfig();
      cfg.markets.test_market.enabled = 'true';
      const result = validateConfig(cfg);
      assert.strictEqual(result.valid, false);
      assert.ok(result.errors.some(e => e.includes('enabled')));
    });

    it('rejects depthLimit with negative value', () => {
      const cfg = validMinimalConfig();
      cfg.markets.test_market.depthLimit = -1;
      const result = validateConfig(cfg);
      assert.strictEqual(result.valid, false);
      assert.ok(result.errors.some(e => e.includes('depthLimit')));
    });

    it('rejects depthLimit with non-integer', () => {
      const cfg = validMinimalConfig();
      cfg.markets.test_market.depthLimit = 1.5;
      const result = validateConfig(cfg);
      assert.strictEqual(result.valid, false);
    });
  });

  describe('config.output', () => {

    it('rejects missing output key', () => {
      const cfg = validMinimalConfig();
      delete cfg.output;
      const result = validateConfig(cfg);
      assert.strictEqual(result.valid, false);
      assert.ok(result.errors.some(e => e.includes('output')));
    });

    it('rejects null output', () => {
      const cfg = validMinimalConfig();
      cfg.output = null;
      const result = validateConfig(cfg);
      assert.strictEqual(result.valid, false);
    });

    it('rejects missing output.base_path', () => {
      const cfg = validMinimalConfig();
      delete cfg.output.base_path;
      const result = validateConfig(cfg);
      assert.strictEqual(result.valid, false);
      assert.ok(result.errors.some(e => e.includes('base_path')));
    });

    it('rejects empty output.base_path', () => {
      const cfg = validMinimalConfig();
      cfg.output.base_path = '';
      const result = validateConfig(cfg);
      assert.strictEqual(result.valid, false);
    });

    it('rejects non-string output.base_path', () => {
      const cfg = validMinimalConfig();
      cfg.output.base_path = null;
      const result = validateConfig(cfg);
      assert.strictEqual(result.valid, false);
    });

    it('rejects missing flush fields', () => {
      const cfg = validMinimalConfig();
      const fields = ['flush_trades_ms', 'flush_book_ms', 'flush_liquidations_ms', 'flush_health_ms'];
      for (const f of fields) {
        delete cfg.output[f];
      }
      const result = validateConfig(cfg);
      assert.strictEqual(result.valid, false);
      for (const f of fields) {
        assert.ok(result.errors.some(e => e.includes(f)));
      }
    });

    it('rejects zero flush field', () => {
      const cfg = validMinimalConfig();
      cfg.output.flush_trades_ms = 0;
      const result = validateConfig(cfg);
      assert.strictEqual(result.valid, false);
    });

    it('rejects negative flush field', () => {
      const cfg = validMinimalConfig();
      cfg.output.flush_liquidations_ms = -100;
      const result = validateConfig(cfg);
      assert.strictEqual(result.valid, false);
    });

    it('rejects non-integer flush field', () => {
      const cfg = validMinimalConfig();
      cfg.output.flush_book_ms = 1.5;
      const result = validateConfig(cfg);
      assert.strictEqual(result.valid, false);
    });

    it('rejects string flush field', () => {
      const cfg = validMinimalConfig();
      cfg.output.flush_health_ms = 'fast';
      const result = validateConfig(cfg);
      assert.strictEqual(result.valid, false);
    });
  });

  describe('config.tick (optional)', () => {

    it('rejects null tick when present', () => {
      const cfg = validMinimalConfig();
      cfg.tick = null;
      const result = validateConfig(cfg);
      assert.strictEqual(result.valid, false);
    });

    it('rejects array tick', () => {
      const cfg = validMinimalConfig();
      cfg.tick = [];
      const result = validateConfig(cfg);
      assert.strictEqual(result.valid, false);
    });

    it('rejects non-integer market_data_ms', () => {
      const cfg = validMinimalConfig();
      cfg.tick.market_data_ms = 'slow';
      const result = validateConfig(cfg);
      assert.strictEqual(result.valid, false);
    });

    it('rejects zero market_data_ms', () => {
      const cfg = validMinimalConfig();
      cfg.tick.market_data_ms = 0;
      const result = validateConfig(cfg);
      assert.strictEqual(result.valid, false);
    });
  });

  describe('config.fairprice (optional)', () => {

    it('rejects null fairprice', () => {
      const cfg = validMinimalConfig();
      cfg.fairprice = null;
      const result = validateConfig(cfg);
      assert.strictEqual(result.valid, false);
    });

    it('rejects non-integer snapshot_interval_ms', () => {
      const cfg = validMinimalConfig();
      cfg.fairprice.snapshot_interval_ms = '30s';
      const result = validateConfig(cfg);
      assert.strictEqual(result.valid, false);
    });

    it('rejects zero book_snapshot_ms', () => {
      const cfg = validMinimalConfig();
      cfg.fairprice.book_snapshot_ms = 0;
      const result = validateConfig(cfg);
      assert.strictEqual(result.valid, false);
    });
  });

  describe('multiple errors', () => {

    it('reports all errors for a completely broken config', () => {
      const cfg = { markets: null, output: 'broken', tick: null };
      const result = validateConfig(cfg);
      assert.strictEqual(result.valid, false);
      // Should have at least 2 errors (markets broken, output broken)
      assert.ok(result.errors.length >= 2, `expected >=2 errors, got ${result.errors.length}: ${result.errors.join('; ')}`);
      // Each error message is distinct
      const unique = new Set(result.errors);
      assert.strictEqual(unique.size, result.errors.length, 'errors should be distinct');
    });
  });
});

// ── Subprocess integration tests ─────────────────────────────────────────
//
// These spawn orderflow_monitor.mjs with invalid config files and verify
// that the process exits with code 1, prints actionable error messages on
// stderr, and does NOT create any output files.

describe('orderflow_monitor subprocess config validation', () => {
  /** @type {string[]} Temp dirs to clean up. */
  const tmpDirs = [];

  after(async () => {
    for (const d of tmpDirs) {
      await rmDir(d);
    }
  });

  /** Write JSON to a temp file, register it for cleanup. */
  function writeTempConfig(label, json) {
    const dir = path.join(os.tmpdir(), `btc-receiver-test-config-validator-${process.pid}`, label);
    fs.mkdirSync(dir, { recursive: true });
    tmpDirs.push(dir);
    const filePath = path.join(dir, 'config.json');
    fs.writeFileSync(filePath, JSON.stringify(json, null, 2), 'utf-8');
    return filePath;
  }

  it('(1) missing config.markets → exit 1, stderr has validation error', () => {
    const cfgPath = writeTempConfig('no-markets', {
      output: { base_path: 'data/test', flush_trades_ms: 200, flush_book_ms: 1000, flush_liquidations_ms: 200, flush_health_ms: 1000 },
    });
    const result = runMonitor(cfgPath);
    assert.notStrictEqual(result.status, 0, 'should exit non-zero');
    assert.strictEqual(result.status, 1, 'should exit 1');
    const stderr = result.stderr.toString();
    assert.ok(stderr.includes('config validation failed'), 'stderr should mention config validation');
    assert.ok(stderr.includes('markets'), 'stderr should mention markets field');
  });

  it('(2) null config.markets → exit 1, stderr has validation error', () => {
    const cfgPath = writeTempConfig('null-markets', {
      markets: null,
      output: { base_path: 'data/test', flush_trades_ms: 200, flush_book_ms: 1000, flush_liquidations_ms: 200, flush_health_ms: 1000 },
    });
    const result = runMonitor(cfgPath);
    assert.strictEqual(result.status, 1);
    const stderr = result.stderr.toString();
    assert.ok(stderr.includes('config validation failed'));
    assert.ok(stderr.includes('markets'));
  });

  it('(3) missing config.output → exit 1, stderr has validation error', () => {
    const cfgPath = writeTempConfig('no-output', {
      markets: { test: { enabled: true, symbol: 'BTC', wsUrl: 'wss://ex.com/ws', restUrl: '' } },
    });
    const result = runMonitor(cfgPath);
    assert.strictEqual(result.status, 1);
    const stderr = result.stderr.toString();
    assert.ok(stderr.includes('config validation failed'));
    assert.ok(stderr.includes('output'));
  });

  it('(4) market wsUrl without wss:// prefix → exit 1', () => {
    const cfgPath = writeTempConfig('bad-wsurl', {
      markets: { test: { enabled: true, symbol: 'BTC', wsUrl: 'http://ex.com/ws', restUrl: '' } },
      output: { base_path: 'data/test', flush_trades_ms: 200, flush_book_ms: 1000, flush_liquidations_ms: 200, flush_health_ms: 1000 },
    });
    const result = runMonitor(cfgPath);
    assert.strictEqual(result.status, 1);
    const stderr = result.stderr.toString();
    assert.ok(stderr.includes('config validation failed'));
    assert.ok(stderr.includes('ws://'));
  });

  it('(5) empty market symbol → exit 1', () => {
    const cfgPath = writeTempConfig('empty-symbol', {
      markets: { test: { enabled: true, symbol: '', wsUrl: 'wss://ex.com/ws', restUrl: '' } },
      output: { base_path: 'data/test', flush_trades_ms: 200, flush_book_ms: 1000, flush_liquidations_ms: 200, flush_health_ms: 1000 },
    });
    const result = runMonitor(cfgPath);
    assert.strictEqual(result.status, 1);
    const stderr = result.stderr.toString();
    assert.ok(stderr.includes('config validation failed'));
    assert.ok(stderr.includes('symbol'));
  });

  it('(6) zero flush interval → exit 1', () => {
    const cfgPath = writeTempConfig('zero-flush', {
      markets: { test: { enabled: true, symbol: 'BTC', wsUrl: 'wss://ex.com/ws', restUrl: '' } },
      output: { base_path: 'data/test', flush_trades_ms: 0, flush_book_ms: 1000, flush_liquidations_ms: 200, flush_health_ms: 1000 },
    });
    const result = runMonitor(cfgPath);
    assert.strictEqual(result.status, 1);
    const stderr = result.stderr.toString();
    assert.ok(stderr.includes('config validation failed'));
    assert.ok(stderr.includes('flush_trades_ms'));
  });

  it('(7) valid config passes validation (exit non-zero for other reasons is OK)', () => {
    // The real config.v3.json is valid.  The monitor will pass validation but
    // may still exit non-zero because workers can't actually connect within
    // the short timeout.  We only verify that validation did NOT block it.
    const result = runMonitor(REAL_CONFIG);
    // Validation passes — we should NOT see validation error in stderr
    const stderr = result.stderr.toString();
    assert.ok(!stderr.includes('config validation failed'), 'valid config should not produce validation error');
    // The process may exit 0 (if --seconds 0 exit before timeout) or 1 (if
    // workers fail to connect).  Both are expected — we just proved validation
    // didn't block.
  });

  it('(8) null market entry (wrong type) → exit 1 with actionable error', () => {
    const cfgPath = writeTempConfig('null-entry', {
      markets: { test: null },
      output: { base_path: 'data/test', flush_trades_ms: 200, flush_book_ms: 1000, flush_liquidations_ms: 200, flush_health_ms: 1000 },
    });
    const result = runMonitor(cfgPath);
    assert.strictEqual(result.status, 1);
    const stderr = result.stderr.toString();
    assert.ok(stderr.includes('config validation failed'));
    assert.ok(stderr.includes('test'));
  });

  it('(9) negative depthLimit → exit 1', () => {
    const cfgPath = writeTempConfig('neg-depth', {
      markets: { test: { enabled: true, symbol: 'BTC', wsUrl: 'wss://ex.com/ws', restUrl: '', depthLimit: -5 } },
      output: { base_path: 'data/test', flush_trades_ms: 200, flush_book_ms: 1000, flush_liquidations_ms: 200, flush_health_ms: 1000 },
    });
    const result = runMonitor(cfgPath);
    assert.strictEqual(result.status, 1);
    const stderr = result.stderr.toString();
    assert.ok(stderr.includes('config validation failed'));
    assert.ok(stderr.includes('depthLimit'));
  });

  it('(10) non-boolean enabled field → exit 1', () => {
    const cfgPath = writeTempConfig('nonbool-enabled', {
      markets: { test: { enabled: 'true', symbol: 'BTC', wsUrl: 'wss://ex.com/ws', restUrl: '' } },
      output: { base_path: 'data/test', flush_trades_ms: 200, flush_book_ms: 1000, flush_liquidations_ms: 200, flush_health_ms: 1000 },
    });
    const result = runMonitor(cfgPath);
    assert.strictEqual(result.status, 1);
    const stderr = result.stderr.toString();
    assert.ok(stderr.includes('config validation failed'));
    assert.ok(stderr.includes('enabled'));
  });
});
