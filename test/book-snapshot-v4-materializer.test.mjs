import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { mkdir, mkdtemp, readFile, readdir, writeFile } from 'node:fs/promises';
import { existsSync } from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { RawV4Writer } from '../lib/raw-v4-writer.mjs';
import { materializeBookSnapshotsV4, v4CursorPath } from '../scripts/materialize-book-snapshots.mjs';
import { discoverV4Segments, verifyV4ClosedSegment } from '../scripts/cleanup-raw.mjs';

const BLOCK_MS = 30_000;
const MARKET = 'test_market';

async function tempDir() {
  return mkdtemp(path.join(os.tmpdir(), 'book-snap-v4-'));
}

function bookSnapshot(ts, seq, bids, asks) {
  return {
    market: MARKET,
    type: 'snapshot',
    ts,
    seq,
    bids: bids.map(([p, q]) => [String(p), String(q)]),
    asks: asks.map(([p, q]) => [String(p), String(q)]),
  };
}

function bookUpdate(ts, seq, prevSeq, bids, asks) {
  return {
    market: MARKET,
    type: 'update',
    ts,
    seq,
    prev_seq: prevSeq,
    bids: bids.map(([p, q]) => [String(p), String(q)]),
    asks: asks.map(([p, q]) => [String(p), String(q)]),
  };
}

async function readJsonlLines(filePath) {
  if (!existsSync(filePath)) return [];
  const text = await readFile(filePath, 'utf8');
  return text.split('\n').filter(Boolean).map((line) => JSON.parse(line));
}

async function marketFiles(root, market) {
  const dir = path.join(root, `market=${market}`);
  if (!existsSync(dir)) return [];
  const out = [];
  for (const date of await readdir(dir)) {
    const dateDir = path.join(dir, date);
    for (const name of await readdir(dateDir)) {
      if (name.endsWith('.jsonl')) out.push(path.join(dateDir, name));
    }
  }
  return out.sort();
}

async function writeBlock(writer, start, count = 29) {
  // Strict pre-second: snapshot before the block anchors the first row.
  await writer.append(bookSnapshot(start - 1, 1, [[50000, 1]], [[50100, 2]]));
  for (let i = 1; i <= count; i++) {
    const ts = start + i * 1000;
    await writer.append(bookUpdate(ts, 1 + i, i, [[50000 + i, 1]], [[50100 + i, 2]]));
  }
}

describe('materialize-book-snapshots.mjs v4 incremental', () => {
  it('commits a closed 30s block and leaves active partial uncommitted', async () => {
    const rawRoot = await tempDir();
    const outRoot = await tempDir();
    const start = Date.UTC(2024, 0, 1, 0, 0, 0);

    const writer = new RawV4Writer({ root: rawRoot, market: MARKET, kind: 'book_updates', now: () => start });
    await writeBlock(writer, start, 29);
    // Active partial in the next block.
    await writer.append(bookUpdate(start + BLOCK_MS + 1000, 31, 30, [[50030, 1]], [[50200, 2]]));
    await writer.flush();

    const result = await materializeBookSnapshotsV4({
      data: rawRoot,
      outputRoot: outRoot,
      markets: [MARKET],
      from: start,
    });
    assert.equal(result.written_blocks, 1);
    assert.equal(result.active_partial_markets, 1);
    assert.equal(result.blocked_markets, 0);

    const files = await marketFiles(outRoot, MARKET);
    assert.equal(files.length, 1);
    const rows = await readJsonlLines(files[0]);
    assert.equal(rows.length, 30);
    assert.equal(rows[0].ts, start);
    assert.equal(rows[29].ts, start + 29 * 1000);
    assert.equal(rows[0].seeded, true);
    assert.equal(rows[0].finalized, true);
    assert.equal(rows[0].book_status, 'seeded');
    // Row 1 still uses the snapshot because the update at start+1000 is excluded.
    assert.equal(rows[0].bid_prices[0], 50000);
    assert.equal(rows[1].bid_prices[0], 50000);
    assert.equal(rows[2].bid_prices[0], 50001);

    const checkpoint = JSON.parse(await readFile(v4CursorPath(outRoot, MARKET), 'utf8'));
    assert.equal(checkpoint.schema_version, 'book_snapshot_v4_cursor_v1');
    assert.equal(checkpoint.next_block_ms, start + BLOCK_MS);
    assert.ok(checkpoint.raw_v4_cursor);
    assert.ok(checkpoint.carry_seed);

    const manifest = JSON.parse(await readFile(`${files[0]}.manifest.json`, 'utf8'));
    const proof = manifest.source.raw_v4_segment_proof;
    assert.equal(proof.length, 1);
    assert.equal(proof[0].sourceLogicalPath, 'book_updates/test_market/2024-01-01/00-00.jsonl');
    assert.equal(proof[0].source_path, path.join(rawRoot, proof[0].sourceLogicalPath));
    assert.equal(proof[0].sourcePath, proof[0].source_path);
    assert.ok(proof[0].source_size > 0);
    assert.match(proof[0].source_hash, /^[0-9a-f]{64}$/);
    assert.equal(proof[0].active, true);
    assert.equal(proof[0].status, 'active');
    assert.equal(proof[0].source_prefix_size, proof[0].byte_offset_end);
    assert.match(proof[0].source_prefix_hash, /^[0-9a-f]{64}$/);
    assert.equal(proof[0].byte_offset, proof[0].byte_offset_end);
    assert.equal(proof[0].status, 'active');
    assert.equal(manifest.source.status, 'active');
    assert.deepEqual(checkpoint.raw_v4_segment_proof, proof);
    const activeSegment = discoverV4Segments(rawRoot).find((segment) => segment.active);
    assert.deepEqual(verifyV4ClosedSegment(activeSegment, {
      cursorRoots: [path.join(outRoot, '.v4-cursors')],
      manifestRoots: [outRoot],
    }), { ok: false, reason: 'active-segment' });

    // Append to the active segment after the first proof, then close it. The
    // logical path remains stable and the next block gets a closed proof.
    await writer.append(bookUpdate(start + BLOCK_MS + 2000, 32, 31, [[50031, 1]], [[50201, 2]]));
    await writer.close();
    const resumed = await materializeBookSnapshotsV4({
      data: rawRoot,
      outputRoot: outRoot,
      markets: [MARKET],
    });
    assert.equal(resumed.written_blocks, 1);
    const closedFiles = await marketFiles(outRoot, MARKET);
    const closedManifest = JSON.parse(await readFile(`${closedFiles[1]}.manifest.json`, 'utf8'));
    assert.equal(closedManifest.source.raw_v4_segment_proof[0].source_path, proof[0].source_path);
    assert.equal(closedManifest.source.raw_v4_segment_proof[0].status, 'committed');
    assert.equal(closedManifest.source.raw_v4_segment_proof[0].active, false);
    assert.equal(closedManifest.source.raw_v4_segment_proof[0].source_prefix_size, closedManifest.source.raw_v4_segment_proof[0].byte_offset_end);
    const closedSegment = discoverV4Segments(rawRoot).find((segment) => !segment.active);
    const closedProof = verifyV4ClosedSegment(closedSegment, {
      cursorRoots: [path.join(outRoot, '.v4-cursors')],
      manifestRoots: [outRoot],
    });
    assert.ok(closedProof.ok, JSON.stringify(closedProof));
  });

  it('resumes from durable byte cursor and carries the seed to the next block', async () => {
    const rawRoot = await tempDir();
    const outRoot = await tempDir();
    const start = Date.UTC(2024, 0, 1, 0, 0, 0);

    const writer = new RawV4Writer({ root: rawRoot, market: MARKET, kind: 'book_updates', now: () => start });
    await writeBlock(writer, start, 29);
    // Partial in second block so the first block commits.
    await writer.append(bookUpdate(start + BLOCK_MS + 1000, 31, 30, [[50030, 1]], [[50040, 2]]));
    await writer.flush();
    await materializeBookSnapshotsV4({ data: rawRoot, outputRoot: outRoot, markets: [MARKET], from: start });

    // Complete the second block and start a third active partial.
    await writer.append(bookUpdate(start + BLOCK_MS + 2000, 32, 31, [[50031, 1]], [[50201, 2]]));
    await writer.append(bookUpdate(start + BLOCK_MS + 3000, 33, 32, [[50032, 1]], [[50202, 2]]));
    await writer.append(bookUpdate(start + 2 * BLOCK_MS + 1000, 34, 33, [[50033, 1]], [[50203, 2]]));
    await writer.flush();

    const result = await materializeBookSnapshotsV4({
      data: rawRoot,
      outputRoot: outRoot,
      markets: [MARKET],
    });
    assert.equal(result.written_blocks, 1);
    assert.equal(result.active_partial_markets, 1);

    const files = await marketFiles(outRoot, MARKET);
    assert.equal(files.length, 2);
    const secondBlock = files.find((p) => p.includes(`${new Date(start + BLOCK_MS).toISOString().slice(11, 19).replaceAll(':', '-')}`));
    assert.ok(secondBlock, 'second block output missing');
    const rows = await readJsonlLines(secondBlock);
    assert.equal(rows.length, 30);
    assert.equal(rows[0].seeded, true);
    assert.equal(rows[0].finalized, true);
    assert.equal(rows[0].book_status, 'seeded');

    const checkpoint = JSON.parse(await readFile(v4CursorPath(outRoot, MARKET), 'utf8'));
    assert.equal(checkpoint.next_block_ms, start + 2 * BLOCK_MS);
  });

  it('seeds update-only book_updates from the latest external snapshot', async () => {
    const rawRoot = await tempDir();
    const outRoot = await tempDir();
    const start = Date.UTC(2024, 0, 1, 0, 0, 0);
    const snapshotPath = path.join(rawRoot, 'snapshots', MARKET, '2023-12-31', 'snapshot.jsonl');
    await mkdir(path.dirname(snapshotPath), { recursive: true });
    await writeFile(snapshotPath, `${JSON.stringify({
      schemaVersion: '1.0',
      stream: 'snapshot',
      reason: 'startup',
      ts: start - 1000,
      recvTs: start - 999,
      market: MARKET,
      seq: 100,
      bids: [['50000', '1']],
      asks: [['50100', '2']],
    })}\n`);

    const writer = new RawV4Writer({ root: rawRoot, market: MARKET, kind: 'book_updates', now: () => start });
    for (let i = 1; i <= 29; i++) {
      await writer.append(bookUpdate(start + i * 1000, 100 + i, 99 + i, [[50000 + i, 1]], [[50100 + i, 2]]));
    }
    await writer.close();

    const result = await materializeBookSnapshotsV4({
      data: rawRoot,
      outputRoot: outRoot,
      markets: [MARKET],
      from: start,
    });
    assert.equal(result.written_blocks, 1);

    const files = await marketFiles(outRoot, MARKET);
    const rows = await readJsonlLines(files[0]);
    assert.equal(rows.length, 30);
    assert.equal(rows[0].seeded, true);
    assert.equal(rows[0].gap, false);
    assert.equal(rows[0].sequence_status, 'ok');
    assert.equal(rows[0].last_seq, 100);
    assert.equal(rows[0].bid_prices[0], 50000);
    assert.equal(rows[2].last_seq, 101);
  });

  it('is fail-closed and does not advance cursor on invalid payload', async () => {
    const rawRoot = await tempDir();
    const outRoot = await tempDir();
    const start = Date.UTC(2024, 0, 1, 0, 0, 0);

    const writer = new RawV4Writer({ root: rawRoot, market: MARKET, kind: 'book_updates', now: () => start });
    await writeBlock(writer, start, 29);
    await writer.flush();
    // Append an invalid raw_v4 payload (missing type) to the active segment.
    await writer.append({ market: MARKET, ts: start + BLOCK_MS + 1000, bids: [], asks: [] });
    await writer.close();

    let threw = false;
    try {
      await materializeBookSnapshotsV4({
        data: rawRoot,
        outputRoot: outRoot,
        markets: [MARKET],
        from: start,
      });
    } catch (error) {
      threw = true;
      assert.ok(error.message.includes('type:') || error.message.includes('raw_v4 payload invalid'));
    }
    assert.equal(threw, true);

    // The first (closed) block should still be committed before the invalid row.
    const files = await marketFiles(outRoot, MARKET);
    assert.equal(files.length, 1);
    const checkpoint = JSON.parse(await readFile(v4CursorPath(outRoot, MARKET), 'utf8'));
    assert.equal(checkpoint.next_block_ms, start + BLOCK_MS);
  });

  it('processes a final record returned with done=true from a closed segment', async () => {
    const rawRoot = await tempDir();
    const outRoot = await tempDir();
    const start = Date.UTC(2024, 0, 1, 0, 0, 0);

    const writer = new RawV4Writer({ root: rawRoot, market: MARKET, kind: 'book_updates', now: () => start });
    await writeBlock(writer, start, 29);
    await writer.close();

    const result = await materializeBookSnapshotsV4({
      data: rawRoot,
      outputRoot: outRoot,
      markets: [MARKET],
      from: start,
    });
    assert.equal(result.written_blocks, 1);

    const files = await marketFiles(outRoot, MARKET);
    assert.equal(files.length, 1);
    assert.equal((await readJsonlLines(files[0])).length, 30);
  });
});
