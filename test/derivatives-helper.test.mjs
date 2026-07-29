// test/derivatives-helper.test.mjs — DerivativesHelper unit tests
// Tests data row format, writer integration, and market registration.
// Network-dependent _fetch* methods are not tested here (requires live API).

import { describe, it } from 'node:test';
import assert from 'node:assert';
import fs from 'node:fs';
import path from 'node:path';
import os from 'node:os';
import { DerivativesHelper } from '../lib/derivatives-helper.mjs';

function uniqueTmpDir() {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'btc-rec-derivatives-'));
  // Pre-create the derivatives subdir so BufferedWriter stream won't fail
  fs.mkdirSync(path.join(dir, 'derivatives'), { recursive: true });
  return dir;
}

describe('DerivativesHelper row format', () => {
  it('sends rows to a callback without creating derivative files', async () => {
    const tmpDir = uniqueTmpDir();
    let captured = null;
    const helper = new DerivativesHelper(tmpDir, {
      intervalMs: 10000,
      onRow: async (row) => { captured = row; },
    });
    helper.registerMarket('binance_perp', {});
    const entry = helper._writers.get('binance_perp');
    assert.equal(entry.writer, null);
    helper._fetchBinancePerp = async () => ({
      mark_price: 65000,
      funding_rate: 0.0001,
      open_interest: 2.5,
      source_ts: 1_000,
    });
    await helper._fetchMarket('binance_perp', null, 2_000, {});
    assert.equal(captured.market, 'binance_perp');
    assert.equal(captured.open_interest, 2.5);
    assert.equal(captured.source_ts, 1_000);
    assert.equal(fs.existsSync(path.join(tmpDir, 'derivatives', 'binance_perp.jsonl')), false);
    await helper.close();
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  it('should register market and create writer', async () => {
    const tmpDir = uniqueTmpDir();
    const helper = new DerivativesHelper(tmpDir, { intervalMs: 10000 });
    helper.registerMarket('binance_perp', {});
    assert.ok(helper._writers.has('binance_perp'));
    assert.ok(helper._writers.get('binance_perp').writer);
    await helper.close();
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  it('should write derivative row and flush to JSONL file', async () => {
    const tmpDir = uniqueTmpDir();
    const helper = new DerivativesHelper(tmpDir, { intervalMs: 10000 });
    helper.registerMarket('binance_perp', {});
    const entry = helper._writers.get('binance_perp');

    const row = {
      ts: Date.now(),
      market: 'binance_perp',
      mark_price: 65000.5,
      funding_rate: 0.0001,
      open_interest: 12345.67,
      next_funding_time: Date.now() + 28800000,
    };

    await entry.writer.write(row);
    await entry.writer.flush();

    const filePath = path.join(tmpDir, 'derivatives', 'binance_perp.jsonl');
    assert.ok(fs.existsSync(filePath));

    const content = fs.readFileSync(filePath, 'utf-8').trim();
    const parsed = JSON.parse(content);
    assert.strictEqual(parsed.market, 'binance_perp');
    assert.strictEqual(parsed.mark_price, 65000.5);
    assert.strictEqual(parsed.funding_rate, 0.0001);
    assert.strictEqual(parsed.open_interest, 12345.67);
    assert.ok(parsed.ts);
    assert.ok(parsed.next_funding_time);

    await helper.close();
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  it('should write row with null fields for missing data', async () => {
    const tmpDir = uniqueTmpDir();
    const helper = new DerivativesHelper(tmpDir, { intervalMs: 10000 });
    helper.registerMarket('okx_perp', {});

    const row = {
      ts: Date.now(),
      market: 'okx_perp',
      mark_price: null,
      funding_rate: -0.00005,
      open_interest: null,
      next_funding_time: null,
    };

    const entry = helper._writers.get('okx_perp');
    await entry.writer.write(row);
    await entry.writer.flush();

    const filePath = path.join(tmpDir, 'derivatives', 'okx_perp.jsonl');
    assert.ok(fs.existsSync(filePath));

    const content = fs.readFileSync(filePath, 'utf-8').trim();
    const parsed = JSON.parse(content);
    assert.strictEqual(parsed.market, 'okx_perp');
    assert.strictEqual(parsed.mark_price, null);
    assert.strictEqual(parsed.funding_rate, -0.00005);
    assert.strictEqual(parsed.open_interest, null);
    assert.strictEqual(parsed.next_funding_time, null);

    await helper.close();
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  it('should support multiple markets with separate writers', async () => {
    const tmpDir = uniqueTmpDir();
    const helper = new DerivativesHelper(tmpDir, { intervalMs: 10000 });
    helper.registerMarket('bybit_perp', {});
    helper.registerMarket('hyperliquid_perp', {});

    assert.ok(helper._writers.has('bybit_perp'));
    assert.ok(helper._writers.has('hyperliquid_perp'));

    const bybitEntry = helper._writers.get('bybit_perp');
    const hyperEntry = helper._writers.get('hyperliquid_perp');
    assert.notStrictEqual(bybitEntry.writer, hyperEntry.writer);

    await helper.close();
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  it('should handle multiple writes and read them back', async () => {
    const tmpDir = uniqueTmpDir();
    const helper = new DerivativesHelper(tmpDir, { intervalMs: 10000 });
    helper.registerMarket('binance_perp', {});

    const entry = helper._writers.get('binance_perp');
    await entry.writer.write({ ts: 1, market: 'binance_perp', mark_price: 100, funding_rate: 0, open_interest: null, next_funding_time: null });
    await entry.writer.write({ ts: 2, market: 'binance_perp', mark_price: 200, funding_rate: 0.001, open_interest: 5000, next_funding_time: null });
    await entry.writer.flush();

    const filePath = path.join(tmpDir, 'derivatives', 'binance_perp.jsonl');
    assert.ok(fs.existsSync(filePath));

    const content = fs.readFileSync(filePath, 'utf-8').trim();
    const lines = content.split('\n');
    assert.strictEqual(lines.length, 2);
    const first = JSON.parse(lines[0]);
    const second = JSON.parse(lines[1]);
    assert.strictEqual(first.mark_price, 100);
    assert.strictEqual(second.mark_price, 200);
    assert.strictEqual(second.funding_rate, 0.001);

    await helper.close();
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });
});

describe('DerivativesHelper Hyperliquid parse (network-less mock)', () => {
  it('should parse metaAndAssetCtxs response from mock fetch', async () => {
    const mockData = [
      [{ name: 'BTC', tokens: [] }],
      [{
        markPx: '67890.5',
        funding: '0.000095',
        openInterest: '12345.678',
      }],
    ];

    const origFetch = global.fetch;
    try {
      global.fetch = async (url, opts) => {
        assert.ok(url === 'https://api.hyperliquid.xyz/info');
        assert.ok(opts?.body?.includes('metaAndAssetCtxs'));
        return {
          ok: true,
          json: async () => mockData,
        };
      };

      const helper = new DerivativesHelper('/tmp/non-existent', { intervalMs: 60000 });
      const row = await helper._fetchHyperliquid(Date.now());
      assert.ok(row);
      assert.strictEqual(row.mark_price, 67890.5);
      assert.strictEqual(row.funding_rate, 0.000095);
      assert.strictEqual(row.open_interest, 12345.678);
      assert.strictEqual(row.next_funding_time, null);
    } finally {
      global.fetch = origFetch;
    }
  });
});

describe('DerivativesHelper OKX parse (network-less mock)', () => {
  it('should parse OKX _fetchOkx response with correct OI endpoint', async () => {
    const origFetch = global.fetch;
    try {
      let callCount = 0;
      global.fetch = async (url) => {
        const urlStr = typeof url === 'string' ? url : url.toString();
        if (urlStr.includes('funding-rate')) {
          return {
            ok: true,
            json: async () => ({
              data: [{ fundingRate: '0.0001', fundingTime: '1700000000000' }],
            }),
          };
        }
        if (urlStr.includes('open-interest')) {
          callCount++;
          // Verify correct endpoint
          assert.ok(urlStr.includes('/api/v5/public/open-interest'));
          assert.ok(urlStr.includes('instType=SWAP'));
          assert.ok(urlStr.includes('instId=BTC-USDT-SWAP'));
          // Old endpoint should NOT be used
          assert.ok(!urlStr.includes('/api/v5/market/open-interest'));
          return {
            ok: true,
            json: async () => ({
              data: [{ oi: '98765.432', oiCcy: '987.65432', oiUsd: '64197607.079' }],
            }),
          };
        }
        if (urlStr.includes('mark-price')) {
          return {
            ok: true,
            json: async () => ({
              data: [{ markPx: '65000.5' }],
            }),
          };
        }
        return { ok: false };
      };

      const helper = new DerivativesHelper('/tmp/non-existent', { intervalMs: 60000 });
      const row = await helper._fetchOkx(Date.now());
      assert.ok(row);
      assert.strictEqual(row.open_interest, 98765.432);
      assert.strictEqual(row.oiCcy, 987.65432);
      assert.strictEqual(row.oiUsd, 64197607.079);
      assert.strictEqual(row.funding_rate, 0.0001);
      assert.strictEqual(row.mark_price, 65000.5);
      assert.strictEqual(callCount, 1, 'should call open-interest endpoint exactly once');
    } finally {
      global.fetch = origFetch;
    }
  });

  it('normalizes OKX OI from source oiCcy/oiUsd through _fetchMarket', async () => {
    const tmpDir = uniqueTmpDir();
    const helper = new DerivativesHelper(tmpDir, { intervalMs: 60000 });
    helper.registerMarket('okx_perp', {});
    const now = 1_700_000_000_000;
    helper._fetchOkx = async () => ({
      mark_price: 65000,
      funding_rate: 0.0001,
      open_interest: 100,
      oiCcy: 1,
      oiUsd: 65000,
      next_funding_time: null,
      source_ts: now,
      error: null,
      error_code: null,
    });
    try {
      await helper._fetchMarket('okx_perp', helper._writers.get('okx_perp').writer, now, {});
      await helper._writers.get('okx_perp').writer.flush();
      const filePath = path.join(tmpDir, 'derivatives', 'okx_perp.jsonl');
      const line = JSON.parse(fs.readFileSync(filePath, 'utf-8').trim());
      assert.strictEqual(line.oi_native, 100);
      assert.strictEqual(line.oi_btc, 1);
      assert.strictEqual(line.oi_usd, 65000);
      assert.strictEqual(line.open_interest, 100);
      assert.strictEqual(line.status, 'fresh');
      assert.strictEqual(line.price_usd, 65000);
    } finally {
      await helper.close();
      fs.rmSync(tmpDir, { recursive: true, force: true });
    }
  });
});

describe('DerivativesHelper v4 safeguards', () => {
  it('uses the BTCUSDC endpoint for the BTCUSDC capability', async () => {
    const origFetch = global.fetch;
    const urls = [];
    try {
      global.fetch = async (url) => {
        urls.push(String(url));
        return { ok: true, json: async () => ({ markPrice: '65000', lastFundingRate: '0', nextFundingTime: 1, time: Date.now(), openInterest: '2' }) };
      };
      const helper = new DerivativesHelper('/tmp/non-existent', { maxRetries: 0 });
      await helper._fetchBinancePerp(Date.now(), 'binance_perp_btcusdc');
      assert.ok(urls.some(url => url.includes('symbol=BTCUSDC')));
      assert.ok(urls.every(url => !url.includes('symbol=BTCUSDT')));
    } finally {
      global.fetch = origFetch;
    }
  });

  it('does not register spot or unsupported COIN-M OI', () => {
    const helper = new DerivativesHelper('/tmp/non-existent');
    assert.equal(helper.registerMarket('binance_spot', {}), false);
    assert.equal(helper.registerMarket('binance_coinm_perp', {}), false);
    assert.equal(helper._writers.size, 0);
  });
});

describe('DerivativesHelper polling and concurrency', () => {
  it('defaults to a 10 second poll interval', () => {
    const helper = new DerivativesHelper('/tmp/non-existent');
    assert.strictEqual(helper._intervalMs, 10000);
    helper.stop();
  });

  it('skips overlapping ticks with an in-flight guard', async () => {
    const helper = new DerivativesHelper('/tmp/non-existent', { intervalMs: 60000 });
    helper.registerMarket('binance_perp', {});
    let release;
    helper._fetchMarket = () => new Promise(resolve => { release = resolve; });
    const first = helper._tick();
    assert.strictEqual(helper._inFlight, true);
    const second = await helper._tick();
    assert.strictEqual(second, false);
    release();
    await first;
    assert.strictEqual(helper._inFlight, false);
    await helper.close();
  });

  it('respects Retry-After on 429 and backs off before retrying', async () => {
    const sleeps = [];
    const helper = new DerivativesHelper('/tmp/non-existent', {
      maxRetries: 2,
      backoffBaseMs: 100,
      sleep: (ms) => { sleeps.push(ms); return Promise.resolve(); },
    });
    const origFetch = global.fetch;
    try {
      let calls = 0;
      global.fetch = async () => {
        calls += 1;
        if (calls === 1) {
          return {
            ok: false,
            status: 429,
            headers: { get: (h) => h.toLowerCase() === 'retry-after' ? '2' : null },
          };
        }
        return { ok: true, json: async () => ({}) };
      };
      await helper._fetchJson('https://example.com/x');
      assert.strictEqual(calls, 2);
      assert.deepStrictEqual(sleeps, [2000]);
    } finally {
      global.fetch = origFetch;
    }
  });

  it('falls back to exponential backoff on 503 without Retry-After', async () => {
    const sleeps = [];
    const helper = new DerivativesHelper('/tmp/non-existent', {
      maxRetries: 2,
      backoffBaseMs: 100,
      sleep: (ms) => { sleeps.push(ms); return Promise.resolve(); },
    });
    const origFetch = global.fetch;
    try {
      let calls = 0;
      global.fetch = async () => {
        calls += 1;
        if (calls <= 2) {
          return { ok: false, status: 503, headers: { get: () => null } };
        }
        return { ok: true, json: async () => ({}) };
      };
      await helper._fetchJson('https://example.com/x');
      assert.strictEqual(calls, 3);
      assert.deepStrictEqual(sleeps, [100, 200]);
    } finally {
      global.fetch = origFetch;
    }
  });
});

describe('DerivativesHelper source_ts/age/status', () => {
  it('writes source_ts, age_ms, status and normalized OI fields', async () => {
    const tmpDir = uniqueTmpDir();
    const helper = new DerivativesHelper(tmpDir, { intervalMs: 60000 });
    helper.registerMarket('binance_perp', {});
    const now = 1_700_000_000_000;
    const sourceTs = now - 5_000;
    helper._fetchBinancePerp = async () => ({
      mark_price: 65000,
      funding_rate: 0.0001,
      open_interest: 2.5,
      next_funding_time: null,
      source_ts: sourceTs,
      error: null,
      error_code: null,
    });
    await helper._fetchMarket('binance_perp', helper._writers.get('binance_perp').writer, now, {});
    await helper._writers.get('binance_perp').writer.flush();
    const filePath = path.join(tmpDir, 'derivatives', 'binance_perp.jsonl');
    const line = JSON.parse(fs.readFileSync(filePath, 'utf-8').trim());
    assert.strictEqual(line.ts, now);
    assert.strictEqual(line.source_ts, sourceTs);
    assert.strictEqual(line.status, 'fresh');
    assert.strictEqual(line.age_ms, 5000);
    assert.strictEqual(line.oi_native, 2.5);
    assert.strictEqual(line.oi_btc, 2.5);
    assert.strictEqual(line.oi_usd, 162500);
    assert.strictEqual(line.price_usd, 65000);
    await helper.close();
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });
});

describe('DerivativesHelper BitMEX parse (network-less mock)', () => {
  it('parses XBTUSD inverse response', async () => {
    const origFetch = global.fetch;
    try {
      const ts = '2024-01-01T00:00:00.000Z';
      global.fetch = async () => ({
        ok: true,
        json: async () => [{ symbol: 'XBTUSD', markPrice: '40000', openInterest: '100000', timestamp: ts }],
      });
      const helper = new DerivativesHelper('/tmp/non-existent');
      const row = await helper._fetchBitmex(Date.now());
      assert.strictEqual(row.mark_price, 40000);
      assert.strictEqual(row.open_interest, 100000);
      assert.strictEqual(row.source_ts, Date.parse(ts));
    } finally {
      global.fetch = origFetch;
    }
  });
});

describe('DerivativesHelper OKX timestamp and unit regression', () => {
  it('parses OKX millisecond timestamp strings into source_ts', async () => {
    const origFetch = global.fetch;
    try {
      const now = 1_700_000_000_000;
      global.fetch = async (url) => {
        const urlStr = String(url);
        if (urlStr.includes('open-interest')) {
          return {
            ok: true,
            json: async () => ({ data: [{ oi: '100', oiCcy: '1', oiUsd: '50000', ts: String(now - 1000) }] }),
          };
        }
        if (urlStr.includes('mark-price')) {
          return { ok: true, json: async () => ({ data: [{ markPx: '50000' }] }) };
        }
        if (urlStr.includes('funding-rate')) {
          return { ok: true, json: async () => ({ data: [{ fundingRate: '0' }] }) };
        }
        return { ok: false };
      };
      const helper = new DerivativesHelper('/tmp/non-existent', { maxRetries: 0 });
      const row = await helper._fetchOkx(now);
      assert.strictEqual(row.source_ts, now - 1000);
      assert.strictEqual(row.open_interest, 100);
      assert.strictEqual(row.oiCcy, 1);
      assert.strictEqual(row.oiUsd, 50000);
    } finally {
      global.fetch = origFetch;
    }
  });
});
