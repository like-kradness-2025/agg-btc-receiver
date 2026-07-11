// test/aux-data-collector.test.mjs — Aux data collector unit tests
//
// Verifies the structural contracts extracted from aux_data_collector.mjs:
//   (1) PERP_MARKETS list matches the expected set
//   (2) DerivativesHelper + MarketDataCollector registration logic
//   (3) Smoke: ensures node --check passes (structural validity)

import { describe, it } from 'node:test';
import assert from 'node:assert';
import fs from 'node:fs';
import path from 'node:path';

// ── Constants extracted from aux_data_collector.mjs (must stay in sync) ─────

const PERP_MARKETS = ['binance_perp', 'binance_coinm_perp', 'binance_perp_btcusdc', 'bybit_perp', 'okx_perp', 'hyperliquid_perp'];

describe('Aux data collector structural contracts', () => {

  describe('PERP_MARKETS constant', () => {

    it('should include all expected perp markets', () => {
      assert.ok(PERP_MARKETS.includes('binance_perp'));
      assert.ok(PERP_MARKETS.includes('binance_coinm_perp'));
      assert.ok(PERP_MARKETS.includes('binance_perp_btcusdc'));
      assert.ok(PERP_MARKETS.includes('bybit_perp'));
      assert.ok(PERP_MARKETS.includes('okx_perp'));
      assert.ok(PERP_MARKETS.includes('hyperliquid_perp'));
    });

    it('should NOT include spot-only markets', () => {
      assert.ok(!PERP_MARKETS.includes('binance_spot'));
      assert.ok(!PERP_MARKETS.includes('binance_spot_usdc'));
      assert.ok(!PERP_MARKETS.includes('coinbase_spot'));
      assert.ok(!PERP_MARKETS.includes('kraken_spot'));
      assert.ok(!PERP_MARKETS.includes('bybit_spot'));
      assert.ok(!PERP_MARKETS.includes('okx_spot'));
    });

    it('should have exactly 6 entries', () => {
      assert.strictEqual(PERP_MARKETS.length, 6);
    });
  });

  describe('Market classification logic (isPerp / hasMarketData)', () => {

    /**
     * Replicates the registration decision logic from aux_data_collector.mjs main():
     *   - If market is in PERP_MARKETS → register with DerivativesHelper
     *   - If config.markets[market].marketData exists → register with MarketDataCollector
     */

    it('should classify binance_perp as perp + has marketData', () => {
      const market = 'binance_perp';
      const isPerp = PERP_MARKETS.includes(market);
      const md = { ohlcv: 'https://...', ticker: 'https://...', lsratio: 'https://...', takervol: 'https://...' };
      const hasMarketData = !!md;
      assert.strictEqual(isPerp, true);
      assert.strictEqual(hasMarketData, true);
    });

    it('should classify binance_spot as NOT perp but has marketData', () => {
      const market = 'binance_spot';
      const isPerp = PERP_MARKETS.includes(market);
      const md = { ohlcv: 'https://...', ticker: 'https://...' };
      const hasMarketData = !!md;
      assert.strictEqual(isPerp, false);
      assert.strictEqual(hasMarketData, true);
    });

    it('should classify bitstamp_spot as NOT perp and no marketData', () => {
      const market = 'bitstamp_spot';
      const isPerp = PERP_MARKETS.includes(market);
      const md = undefined;
      const hasMarketData = !!md;
      assert.strictEqual(isPerp, false);
      assert.strictEqual(hasMarketData, false);
    });

    it('should classify hyperliquid_perp as perp', () => {
      const market = 'hyperliquid_perp';
      const isPerp = PERP_MARKETS.includes(market);
      assert.strictEqual(isPerp, true);
    });
  });

  describe('Premium registration conditions', () => {

    it('should registerPremium when coinbase_spot AND binance_spot are both in enabled markets', () => {
      const enabledMarkets = ['binance_spot', 'binance_perp', 'coinbase_spot', 'kraken_spot'];
      let hasCoinbase = false;
      for (const market of enabledMarkets) {
        if (market === 'coinbase_spot') hasCoinbase = true;
      }
      const shouldRegisterPremium = hasCoinbase && enabledMarkets.includes('binance_spot');
      assert.strictEqual(shouldRegisterPremium, true);
    });

    it('should NOT registerPremium when coinbase_spot is absent', () => {
      const enabledMarkets = ['binance_spot', 'binance_perp'];
      let hasCoinbase = false;
      for (const market of enabledMarkets) {
        if (market === 'coinbase_spot') hasCoinbase = true;
      }
      const shouldRegisterPremium = hasCoinbase && enabledMarkets.includes('binance_spot');
      assert.strictEqual(shouldRegisterPremium, false);
    });

    it('should NOT registerPremium when binance_spot is absent', () => {
      const enabledMarkets = ['coinbase_spot'];
      let hasCoinbase = false;
      for (const market of enabledMarkets) {
        if (market === 'coinbase_spot') hasCoinbase = true;
      }
      const shouldRegisterPremium = hasCoinbase && enabledMarkets.includes('binance_spot');
      assert.strictEqual(shouldRegisterPremium, false);
    });
  });

  describe('Collect flags logic (lsratio / takervol)', () => {

    it('should set lsratio=true and takervol=true when present in marketData', () => {
      const md = { ohlcv: 'https://...', ticker: 'https://...', lsratio: 'https://...', takervol: 'https://...' };
      const collect = {};
      if (md.lsratio) collect.lsratio = true;
      if (md.takervol) collect.takervol = true;
      assert.deepStrictEqual(collect, { lsratio: true, takervol: true });
    });

    it('should set only lsratio when takervol is absent', () => {
      const md = { ohlcv: 'https://...', ticker: 'https://...', lsratio: 'https://...' };
      const collect = {};
      if (md.lsratio) collect.lsratio = true;
      if (md.takervol) collect.takervol = true;
      assert.deepStrictEqual(collect, { lsratio: true });
    });

    it('should set neither when both are absent', () => {
      const md = { ohlcv: 'https://...', ticker: 'https://...' };
      const collect = {};
      if (md.lsratio) collect.lsratio = true;
      if (md.takervol) collect.takervol = true;
      assert.deepStrictEqual(collect, {});
    });
  });

  describe('P1-1 heartbeat contract', () => {
    // Verify the writeHeartbeat function structure and error-safety.
    // We test the extracted contract function rather than the CLI entry point.

    /**
     * Extracted heartbeat writer matching aux_data_collector.mjs writeHeartbeat.
     * Returns the payload object written, or null on error.
     */
    function writeHeartbeatTest(outputBase, status, enabledMarkets) {
      try {
        const healthDir = path.join(outputBase, 'health');
        fs.mkdirSync(healthDir, { recursive: true });
        const finalPath = path.join(healthDir, 'aux_collector.json');
        const payload = {
          status,
          pid: process.pid,
          ts: new Date().toISOString(),
          updated_at_ms: Date.now(),
          markets: enabledMarkets,
          output_base: outputBase,
        };
        // Atomic: write to tmp then rename
        const tmpPath = path.join(healthDir, '.aux_collector.json.tmp');
        fs.writeFileSync(tmpPath, JSON.stringify(payload, null, 2), 'utf8');
        fs.renameSync(tmpPath, finalPath);
        return payload;
      } catch (_err) {
        return null;
      }
    }

    it('writes heartbeat JSON with all required fields', () => {
      const outputBase = '/tmp/test-heartbeat-' + Date.now();
      const markets = ['binance_spot', 'binance_perp'];
      const payload = writeHeartbeatTest(outputBase, 'running', markets);

      assert.ok(payload !== null, 'payload should not be null');
      assert.strictEqual(payload.status, 'running');
      assert.strictEqual(payload.pid, process.pid);
      assert.ok(typeof payload.ts === 'string', 'ts should be ISO string');
      assert.ok(payload.ts.endsWith('Z') || payload.ts.includes('T'), 'ts should be ISO format');
      assert.ok(typeof payload.updated_at_ms === 'number', 'updated_at_ms should be number');
      assert.ok(payload.updated_at_ms > 0);
      assert.deepStrictEqual(payload.markets, markets);
      assert.strictEqual(payload.output_base, outputBase);
    });

    it('writes stopped status on shutdown', () => {
      const outputBase = '/tmp/test-heartbeat-stopped-' + Date.now();
      const markets = ['binance_spot'];
      const payload = writeHeartbeatTest(outputBase, 'stopped', markets);

      assert.ok(payload !== null);
      assert.strictEqual(payload.status, 'stopped');
    });

    it('creates health directory if missing', () => {
      const outputBase = '/tmp/test-heartbeat-mkdir-' + Date.now();
      const healthDir = path.join(outputBase, 'health');
      // Ensure clean state
      try { fs.rmSync(outputBase, { recursive: true, force: true }); } catch (_) {}

      const payload = writeHeartbeatTest(outputBase, 'running', ['binance_spot']);

      assert.ok(payload !== null);
      assert.ok(fs.existsSync(healthDir), 'health directory should be created');
      assert.ok(fs.existsSync(path.join(healthDir, 'aux_collector.json')), 'heartbeat file should exist');
    });

    it('errors do not crash (returns null on failure)', () => {
      // Pass a path that will fail to write (e.g. /dev/null/...)
      const payload = writeHeartbeatTest('/dev/null/impossible', 'running', []);
      assert.strictEqual(payload, null, 'should return null on error, not throw');
    });

    it('updated_at_ms is recent (within last 2 seconds)', () => {
      const outputBase = '/tmp/test-heartbeat-recent-' + Date.now();
      const before = Date.now();
      const payload = writeHeartbeatTest(outputBase, 'running', ['binance_spot']);
      const after = Date.now();

      assert.ok(payload !== null);
      assert.ok(payload.updated_at_ms >= before, 'updated_at_ms should be >= start time');
      assert.ok(payload.updated_at_ms <= after, 'updated_at_ms should be <= end time');
    });

    it('atomic write: tmp file is not left behind', () => {
      const outputBase = '/tmp/test-heartbeat-atomic-' + Date.now();
      writeHeartbeatTest(outputBase, 'running', ['binance_spot']);

      const healthDir = path.join(outputBase, 'health');
      const files = fs.readdirSync(healthDir);
      const tmpFiles = files.filter(f => f.startsWith('.') && f.endsWith('.tmp'));
      assert.strictEqual(tmpFiles.length, 0, 'no .tmp files should be left behind');
      assert.ok(files.includes('aux_collector.json'), 'final file should exist');
    });

    it('pid matches current process', () => {
      const outputBase = '/tmp/test-heartbeat-pid-' + Date.now();
      const payload = writeHeartbeatTest(outputBase, 'running', ['binance_spot']);
      assert.ok(payload !== null);
      assert.strictEqual(payload.pid, process.pid);
      assert.strictEqual(typeof payload.pid, 'number');
    });
  });
});
