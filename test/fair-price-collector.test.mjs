import { describe, it, after } from 'node:test';
import assert from 'node:assert';
import fs from 'node:fs';
import path from 'node:path';
import { FairPriceCollector } from '../lib/fair-price-collector.mjs';
import { FullBook } from '../lib/full-book.mjs';

const TEST_BASE = path.join('data', 'test_raw_hot');

class MockConnector {
  constructor() {
    this._handlers = new Map();
    this._state = 'init';
  }

  on(event, handler) {
    if (!this._handlers.has(event)) this._handlers.set(event, []);
    this._handlers.get(event).push(handler);
  }

  emit(event, ...args) {
    for (const handler of this._handlers.get(event) || []) handler(...args);
  }

  getState() {
    return this._state;
  }

  setState(next) {
    const prev = this._state;
    this._state = next;
    this.emit('stateChange', prev, next);
  }
}

describe('FairPriceCollector raw-only v2', () => {
  let collector;

  after(async () => {
    try { if (collector) await collector.close(); } catch {}
    try { fs.rmSync(TEST_BASE, { recursive: true, force: true }); } catch {}
  });

  it('writes raw trade events to date-partitioned trade jsonl', async () => {
    collector = new FairPriceCollector(TEST_BASE, { snapshotIntervalMs: 5, bookSnapshotMs: 1000 });
    const connector = new MockConnector();
    const book = new FullBook();
    collector.registerMarket('binance_spot', { connector, book });

    connector.emit('trade', { ts: 1700000000000, price: 50000, qty: 0.1, side: 'buy' });
    await collector.close();
    collector = null;

    const dateStr = new Date().toISOString().slice(0, 10);
    const tradePath = path.join(TEST_BASE, dateStr, 'trade', 'binance_spot.jsonl');
    assert.ok(fs.existsSync(tradePath), `trade file missing: ${tradePath}`);
    const lines = fs.readFileSync(tradePath, 'utf-8').trim().split('\n');
    assert.strictEqual(lines.length, 1);
    const row = JSON.parse(lines[0]);
    assert.strictEqual(row.type, 'trade');
    assert.strictEqual(row.price, 50000);
    assert.strictEqual(row.qty, 0.1);
    assert.strictEqual(row.side, 'buy');
  });

  it('writes raw depth updates with seq linkage', async () => {
    collector = new FairPriceCollector(TEST_BASE, { snapshotIntervalMs: 5, bookSnapshotMs: 1000 });
    const connector = new MockConnector();
    const book = new FullBook();
    collector.registerMarket('kraken_spot', { connector, book });

    connector.emit('depth', { ts: 1700000000000, seq: 10, bids: [['50000', '1.0']], asks: [['50010', '1.2']] });
    connector.emit('depth', { ts: 1700000001000, seq: 11, bids: [['50001', '1.1']], asks: [['50011', '1.3']] });
    await new Promise(resolve => setTimeout(resolve, 20));
    await collector.close();
    collector = null;

    const dateStr = new Date().toISOString().slice(0, 10);
    const depthPath = path.join(TEST_BASE, dateStr, 'depth', 'kraken_spot.jsonl');
    assert.ok(fs.existsSync(depthPath), `depth file missing: ${depthPath}`);
    const rows = fs.readFileSync(depthPath, 'utf-8').trim().split('\n').map(JSON.parse);
    assert.strictEqual(rows.length, 2);
    assert.strictEqual(rows[0].seq, 10);
    assert.strictEqual(rows[0].prevSeq, null);
    assert.strictEqual(rows[1].seq, 11);
    assert.strictEqual(rows[1].prevSeq, 10);
    assert.strictEqual(rows[1].exchange, 'kraken');
  });

  it('writes startup snapshot immediately when market first reaches running', async () => {
    collector = new FairPriceCollector(TEST_BASE, { snapshotIntervalMs: 1000, bookSnapshotMs: 1000 });
    const connector = new MockConnector();
    const book = new FullBook();
    book.applySnapshot([['50000', '1.0']], [['50010', '1.2']]);
    collector.registerMarket('coinbase_spot', { connector, book });
    connector.setState('running');

    await new Promise(resolve => setTimeout(resolve, 20));
    await collector.close();
    collector = null;

    const dateStr = new Date().toISOString().slice(0, 10);
    const snapPath = path.join(TEST_BASE, dateStr, 'snapshot', 'coinbase_spot.jsonl');
    assert.ok(fs.existsSync(snapPath), `snapshot file missing: ${snapPath}`);
    const rows = fs.readFileSync(snapPath, 'utf-8').trim().split('\n').map(JSON.parse);
    assert.ok(rows.some(r => r.reason === 'startup'));
  });

  it('writes reconnect snapshot after returning to running', async () => {
    collector = new FairPriceCollector(TEST_BASE, { snapshotIntervalMs: 1000, bookSnapshotMs: 1000 });
    const connector = new MockConnector();
    const book = new FullBook();
    book.applySnapshot([['50000', '1.0']], [['50010', '1.2']]);
    collector.registerMarket('okx_spot', { connector, book });
    connector.setState('running');
    await new Promise(resolve => setTimeout(resolve, 30));
    connector.setState('reconnecting');
    connector.setState('running');
    await new Promise(resolve => setTimeout(resolve, 3200));
    await collector.close();
    collector = null;

    const dateStr = new Date().toISOString().slice(0, 10);
    const snapPath = path.join(TEST_BASE, dateStr, 'snapshot', 'okx_spot.jsonl');
    const rows = fs.readFileSync(snapPath, 'utf-8').trim().split('\n').map(JSON.parse);
    assert.ok(rows.some(r => r.reason === 'reconnect'));
  });

  it('filters depth events flagged as snapshot', async () => {
    collector = new FairPriceCollector(TEST_BASE, { snapshotIntervalMs: 1000, bookSnapshotMs: 1000 });
    const connector = new MockConnector();
    const book = new FullBook();
    collector.registerMarket('bybit_perp', { connector, book });

    connector.emit('depth', { type: 'snapshot', ts: 1700000000000, seq: 77, bids: [['50000', '1.0']], asks: [['50010', '1.2']] });
    await new Promise(resolve => setTimeout(resolve, 20));
    await collector.close();
    collector = null;

    const dateStr = new Date().toISOString().slice(0, 10);
    const depthPath = path.join(TEST_BASE, dateStr, 'depth', 'bybit_perp.jsonl');
    assert.ok(!fs.existsSync(depthPath), `snapshot-typed depth event should not write file: ${depthPath}`);
  });

  it('writes periodic snapshot when running book is usable', async () => {
    collector = new FairPriceCollector(TEST_BASE, { snapshotIntervalMs: 1000, bookSnapshotMs: 1 });
    const connector = new MockConnector();
    const book = new FullBook();
    book.applySnapshot([['50000', '1.0']], [['50010', '1.2']]);
    collector.registerMarket('coinbase_spot', { connector, book });
    connector.setState('running');
    await new Promise(resolve => setTimeout(resolve, 20));
    collector._lastBookSnapshotAt.set('coinbase_spot', 0);
    collector._snapshotInFlight.delete('coinbase_spot');

    await collector._tickSnapshots();
    await new Promise(resolve => setTimeout(resolve, 20));
    await collector.close();
    collector = null;

    const dateStr = new Date().toISOString().slice(0, 10);
    const snapPath = path.join(TEST_BASE, dateStr, 'snapshot', 'coinbase_spot.jsonl');
    assert.ok(fs.existsSync(snapPath), `snapshot file missing: ${snapPath}`);
    const rows = fs.readFileSync(snapPath, 'utf-8').trim().split('\n').map(JSON.parse);
    assert.ok(rows.some(r => r.reason === 'periodic'));
    const periodicRows = rows.filter(r => r.reason === 'periodic');
    assert.ok(periodicRows.length >= 1, 'expected at least one periodic snapshot row');
    const latest = periodicRows[periodicRows.length - 1];
    assert.strictEqual(latest.exchange, 'coinbase');
    assert.deepStrictEqual(latest.bids, [['50000', '1.0']]);
    assert.deepStrictEqual(latest.asks, [['50010', '1.2']]);
  });

  it('ignores trade/depth/state-change work after close guard is set', async () => {
    collector = new FairPriceCollector(TEST_BASE, { snapshotIntervalMs: 1000, bookSnapshotMs: 1 });
    const connector = new MockConnector();
    const book = new FullBook();
    book.applySnapshot([['50000', '1.0']], [['50010', '1.2']]);
    collector.registerMarket('bitstamp_spot', { connector, book });

    await collector.close();
    collector = null;

    connector.emit('trade', { ts: 1700000000000, price: 50000, qty: 0.1, side: 'buy' });
    connector.emit('depth', { ts: 1700000001000, seq: 10, bids: [['50000', '1.0']], asks: [['50010', '1.2']] });
    connector.setState('running');
    await new Promise(resolve => setTimeout(resolve, 50));

    const dateStr = new Date().toISOString().slice(0, 10);
    const tradePath = path.join(TEST_BASE, dateStr, 'trade', 'bitstamp_spot.jsonl');
    const depthPath = path.join(TEST_BASE, dateStr, 'depth', 'bitstamp_spot.jsonl');
    const snapPath = path.join(TEST_BASE, dateStr, 'snapshot', 'bitstamp_spot.jsonl');
    assert.ok(!fs.existsSync(tradePath), `trade file should not exist after close guard: ${tradePath}`);
    assert.ok(!fs.existsSync(depthPath), `depth file should not exist after close guard: ${depthPath}`);
    assert.ok(!fs.existsSync(snapPath), `snapshot file should not exist after close guard: ${snapPath}`);
  });
});
