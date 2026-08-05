import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, readFile, readdir, writeFile, mkdir } from 'node:fs/promises';
import { existsSync } from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { materializeOrderHeatmapIncremental, cursorPath } from '../scripts/materialize-orderheatmap.mjs';

const BLOCK_MS = 30_000;
const MARKET = 'test_market';

async function tempDir() {
  return mkdtemp(path.join(os.tmpdir(), 'ohmap-inc-'));
}

function snapshotRow(ts, bidPrice, askPrice) {
  return {
    schema_version: 'book_snapshot_1s_v2',
    market: MARKET,
    ts,
    finalized: true,
    seeded: true,
    gap: false,
    crossed: false,
    stale: false,
    book_status: 'seeded',
    sequence_status: 'sequenced',
    best_bid: bidPrice,
    best_ask: askPrice,
    mid: (bidPrice + askPrice) / 2,
    bid_prices: [bidPrice],
    bid_qtys: [0.5],
    ask_prices: [askPrice],
    ask_qtys: [1.0],
    base_price_bin_usd: 1,
  };
}

async function writeSnapshotBlock(snapshotRoot, market, blockStartMs) {
  const iso = new Date(blockStartMs).toISOString();
  const dir = path.join(snapshotRoot, `market=${market}`, `date=${iso.slice(0, 10)}`);
  await mkdir(dir, { recursive: true });
  const rows = [];
  for (let i = 0; i < 30; i++) {
    rows.push(snapshotRow(blockStartMs + i * 1000, 50000 + i, 50010 + i));
  }
  const file = path.join(dir, `${iso.slice(11, 19).replaceAll(':', '-')}.jsonl`);
  await writeFile(file, rows.map((r) => JSON.stringify(r)).join('\n') + '\n');
  return file;
}

async function marketFiles(root, market) {
  const dir = path.join(root, `market=${market}`);
  if (!existsSync(dir)) return [];
  const out = [];
  for (const date of await readdir(dir)) {
    const dateDir = path.join(dir, date);
    for (const name of await readdir(dateDir)) {
      out.push(path.join(dateDir, name));
    }
  }
  return out.sort();
}

async function readJsonlLines(filePath) {
  if (!existsSync(filePath)) return [];
  const text = await readFile(filePath, 'utf8');
  return text.split('\n').filter(Boolean).map((line) => JSON.parse(line));
}

describe('materialize-orderheatmap.mjs incremental', () => {
  it('processes only new complete 30-row blocks and advances durable cursor', async () => {
    const snapshotRoot = await tempDir();
    const outRoot = await tempDir();
    const start = Date.UTC(2024, 0, 1, 0, 0, 0);

    await writeSnapshotBlock(snapshotRoot, MARKET, start);
    await writeSnapshotBlock(snapshotRoot, MARKET, start + BLOCK_MS);

    const first = await materializeOrderHeatmapIncremental({
      snapshotRoot,
      outputRoot: outRoot,
      markets: [MARKET],
      from: start,
    });
    assert.equal(first.written_blocks, 2);
    assert.equal(first.blocked_markets, 0);

    const files = await marketFiles(outRoot, MARKET);
    assert.equal(files.length, 2);
    const checkpoint = JSON.parse(await readFile(cursorPath(outRoot, MARKET), 'utf8'));
    assert.equal(checkpoint.schema_version, 'orderheatmap_cursor_v1');
    assert.equal(checkpoint.next_block_ms, start + 2 * BLOCK_MS);

    await writeSnapshotBlock(snapshotRoot, MARKET, start + 2 * BLOCK_MS);
    const second = await materializeOrderHeatmapIncremental({
      snapshotRoot,
      outputRoot: outRoot,
      markets: [MARKET],
    });
    assert.equal(second.written_blocks, 1);
    assert.equal((await marketFiles(outRoot, MARKET)).length, 3);
    const checkpoint2 = JSON.parse(await readFile(cursorPath(outRoot, MARKET), 'utf8'));
    assert.equal(checkpoint2.next_block_ms, start + 3 * BLOCK_MS);
  });

  it('aligns an unaligned initial live horizon to the next 30s block', async () => {
    const snapshotRoot = await tempDir();
    const outRoot = await tempDir();
    const start = Date.UTC(2024, 0, 1, 0, 0, 0);

    await writeSnapshotBlock(snapshotRoot, MARKET, start + BLOCK_MS);
    const result = await materializeOrderHeatmapIncremental({
      snapshotRoot,
      outputRoot: outRoot,
      markets: [MARKET],
      from: start + 1234,
      to: start + 2 * BLOCK_MS,
    });

    assert.equal(result.written_blocks, 1);
    assert.equal(result.blocked_markets, 0);
    const checkpoint = JSON.parse(await readFile(cursorPath(outRoot, MARKET), 'utf8'));
    assert.equal(checkpoint.next_block_ms, start + 2 * BLOCK_MS);
  });

  it('is fail-closed and does not advance cursor on an invalid block', async () => {
    const snapshotRoot = await tempDir();
    const outRoot = await tempDir();
    const start = Date.UTC(2024, 0, 1, 0, 0, 0);

    await writeSnapshotBlock(snapshotRoot, MARKET, start);
    const iso = new Date(start + BLOCK_MS).toISOString();
    const dir = path.join(snapshotRoot, `market=${MARKET}`, `date=${iso.slice(0, 10)}`);
    await mkdir(dir, { recursive: true });
    await writeFile(
      path.join(dir, `${iso.slice(11, 19).replaceAll(':', '-')}.jsonl`),
      [snapshotRow(start + BLOCK_MS, 50000, 50010)].map((r) => JSON.stringify(r)).join('\n') + '\n',
    );

    let threw = false;
    try {
      await materializeOrderHeatmapIncremental({
        snapshotRoot,
        outputRoot: outRoot,
        markets: [MARKET],
        from: start,
      });
    } catch (error) {
      threw = true;
      assert.ok(error.message.includes('expected 30 rows'));
    }
    assert.equal(threw, true);

    const files = await marketFiles(outRoot, MARKET);
    assert.equal(files.length, 1);
    const checkpoint = JSON.parse(await readFile(cursorPath(outRoot, MARKET), 'utf8'));
    assert.equal(checkpoint.next_block_ms, start + BLOCK_MS);
  });

  it('skips only the initial unseeded interval when explicitly enabled', async () => {
    const snapshotRoot = await tempDir();
    const outRoot = await tempDir();
    const start = Date.UTC(2024, 0, 1, 0, 0, 0);

    const first = await writeSnapshotBlock(snapshotRoot, MARKET, start);
    const firstRows = await readJsonlLines(first);
    for (const row of firstRows) {
      row.seeded = false;
      row.finalized = false;
      row.book_status = 'unseeded';
    }
    await writeFile(first, firstRows.map((row) => JSON.stringify(row)).join('\n') + '\n');
    await writeSnapshotBlock(snapshotRoot, MARKET, start + BLOCK_MS);

    const result = await materializeOrderHeatmapIncremental({
      snapshotRoot, outputRoot: outRoot, markets: [MARKET], from: start,
      to: start + 2 * BLOCK_MS, skipInitialUnseeded: true,
    });
    assert.equal(result.written_blocks, 1);
    const checkpoint = JSON.parse(await readFile(cursorPath(outRoot, MARKET), 'utf8'));
    assert.deepEqual(checkpoint.initial_unseeded_gap, {
      start_ms: start,
      end_ms: start + BLOCK_MS,
    });
  });

  it('does not skip a missing block to process a later file', async () => {
    const snapshotRoot = await tempDir();
    const outRoot = await tempDir();
    const start = Date.UTC(2024, 0, 1, 0, 0, 0);

    await writeSnapshotBlock(snapshotRoot, MARKET, start + 2 * BLOCK_MS);
    const result = await materializeOrderHeatmapIncremental({
      snapshotRoot, outputRoot: outRoot, markets: [MARKET], from: start,
    });

    assert.equal(result.written_blocks, 0);
    assert.equal(result.blocked_markets, 1);
    assert.equal((await marketFiles(outRoot, MARKET)).length, 0);
    assert.equal(existsSync(cursorPath(outRoot, MARKET)), false);
  });

  for (const field of ['finalized', 'seeded', 'gap', 'crossed', 'stale']) {
    it(`rejects snapshot blocks with ${field} quality flag`, async () => {
      const snapshotRoot = await tempDir();
      const outRoot = await tempDir();
      const start = Date.UTC(2024, 0, 1, 0, 0, 0);
      const file = await writeSnapshotBlock(snapshotRoot, MARKET, start);
      const rows = await readJsonlLines(file);
      rows[0][field] = field === 'gap' || field === 'crossed' || field === 'stale';
      await writeFile(file, rows.map((row) => JSON.stringify(row)).join('\n') + '\n');

      await assert.rejects(
        materializeOrderHeatmapIncremental({ snapshotRoot, outputRoot: outRoot, markets: [MARKET], from: start }),
        new RegExp(`${field}=`),
      );
      assert.equal(existsSync(cursorPath(outRoot, MARKET)), false);
      assert.equal((await marketFiles(outRoot, MARKET)).length, 0);
    });
  }
});
