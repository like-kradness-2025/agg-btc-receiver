// test/burst-reducer/raw-v4-pipeline.test.mjs — Focused v4 integration tests for TFP
import assert from 'node:assert/strict';
import { describe, it, before, after } from 'node:test';
import { appendFileSync, existsSync, mkdirSync, renameSync, writeFileSync, rmSync, readFileSync } from 'node:fs';
import { join } from 'node:path';
import { runPipeline } from '../../lib/burst-reducer/pipeline.mjs';
import { loadManifest, loadCheckpoint } from '../../lib/burst-reducer/manifest-manager.mjs';
import { RawV4BlockSource } from '../../lib/downstream/raw-v4-block-source.mjs';
import { RAW_V4_SCHEMA } from '../../lib/downstream/raw-v4-segment-reader.mjs';
import { discoverV4Segments, verifyV4ClosedSegment } from '../../scripts/cleanup-raw.mjs';

const TEST_DIR = join('test', 'fixtures', 'burst-v1', 'tmp-raw-v4-pipeline');
const DERIVED_DIR = join('test', 'fixtures', 'burst-v1', 'tmp-raw-v4-derived');
const MARKET = 'test_raw_v4_pipeline';

function envelope(payload, eventTs, extra = {}) {
  return {
    schema: RAW_V4_SCHEMA,
    market: MARKET,
    stream: 'trades',
    event_ts_ms: eventTs,
    recv_ts_ms: eventTs,
    writer_session_id: 'test',
    ingest_seq: null,
    source_id: null,
    payload,
    ...extra,
  };
}

function trade(ts, side, price, qty) {
  return { ts, side, price, qty, market: MARKET };
}

function writeSegment(root, kind, market, date, segment, records, active = false) {
  const dir = join(root, kind, market, date);
  mkdirSync(dir, { recursive: true });
  const text = records.map(r => `${JSON.stringify(r)}\n`).join('');
  const ext = active ? '.jsonl.active' : '.jsonl';
  writeFileSync(join(dir, `${segment}${ext}`), text);
}

function clean() {
  rmSync(TEST_DIR, { recursive: true, force: true });
  rmSync(DERIVED_DIR, { recursive: true, force: true });
}

describe('RawV4 TFP integration', () => {
  before(clean);
  after(clean);

  it('processes closed v4 segments and blocks EOF-finalization of active trailing block', async () => {
    // Closed segment: block 0 and block 1 (30s each)
    writeSegment(TEST_DIR, 'trades', MARKET, '1970-01-01', '00-00', [
      envelope(trade(500, 'buy', 100, 1), 500),
      envelope(trade(520, 'buy', 100, 2), 520),
      envelope(trade(30500, 'sell', 101, 1), 30500),
    ]);
    // Active segment: block 2 (incomplete)
    writeSegment(TEST_DIR, 'trades', MARKET, '1970-01-01', '00-01', [
      envelope(trade(60500, 'buy', 102, 1), 60500),
    ], true);

    const result = await runPipeline({
      dataDir: TEST_DIR,
      market: MARKET,
      fromMs: 0,
      toMs: 90000,
      runId: 'v4-active-1',
      outputRoot: DERIVED_DIR,
      rawLayout: 'v4',
    });

    // Should process block 0 and block 1, leaving block 2 pending (active)
    assert.equal(result.processed, 2, `expected 2 processed, got ${result.processed}; blocked=${result.blocked} reason=${result.blockedReason}`);
    assert.equal(result.blocked, true);
    assert.equal(result.blockedReason, 'active-incomplete-block');

    const cp = loadCheckpoint(MARKET, DERIVED_DIR, 'trades');
    assert.ok(cp);
    assert.equal(cp.pending_block.block_start_ms, 60000);
    assert.equal(cp.pending_block.active, true);
    assert.ok(cp.pending_block.replay_identity.v4_cursor);

    const manifest = loadManifest(MARKET, DERIVED_DIR);
    assert.ok(manifest);
    const committed = Object.values(manifest.processed_blocks).filter(r => r.status === 'committed');
    assert.equal(committed.length, 2);
    for (const record of committed) {
      assert.ok(record.raw_v4_segment_proof, 'committed record should carry segment proof');
      assert.ok(Array.isArray(record.raw_v4_segment_proof));
      assert.ok(record.raw_v4_segment_proof[0].segmentLogicalId);
      assert.ok(typeof record.raw_v4_segment_proof[0].byteOffsetStart === 'number');
      assert.ok(typeof record.raw_v4_segment_proof[0].byteOffsetEnd === 'number');
      assert.equal(record.raw_v4_segment_proof[0].sourceLogicalPath, 'trades/test_raw_v4_pipeline/1970-01-01/00-00.jsonl');
      assert.equal(record.raw_v4_segment_proof[0].sourceSize > 0, true);
      assert.match(record.raw_v4_segment_proof[0].sourceSha256, /^[0-9a-f]{64}$/);
      assert.equal(record.raw_v4_segment_proof[0].active, false);
      assert.equal(record.raw_v4_segment_proof[0].status, 'committed');
      assert.equal(record.raw_v4_segment_proof[0].sourcePrefixSize, record.raw_v4_segment_proof[0].byteOffsetEnd);
      assert.match(record.raw_v4_segment_proof[0].sourcePrefixSha256, /^[0-9a-f]{64}$/);
      assert.equal(record.raw_v4_segment_proof[0].byte_offset, record.raw_v4_segment_proof[0].byteOffsetEnd);
    }

    const activePath = join(TEST_DIR, 'trades', MARKET, '1970-01-01', '00-01.jsonl.active');
    const closedPath = activePath.replace(/\.active$/, '');
    const extra = envelope(trade(60510, 'sell', 103, 1), 60510);
    appendFileSync(activePath, `${JSON.stringify(extra)}\n`);
    renameSync(activePath, closedPath);
    const resumed = await runPipeline({
      dataDir: TEST_DIR,
      market: MARKET,
      fromMs: 0,
      toMs: 90000,
      runId: 'v4-active-close-2',
      outputRoot: DERIVED_DIR,
      rawLayout: 'v4',
      finalizedThroughMs: 90000,
    });
    assert.equal(resumed.processed, 1);
    const closedProof = verifyV4ClosedSegment(
      discoverV4Segments(TEST_DIR).find((item) => item.segmentName === '00-01.jsonl'),
      { cursorRoots: [join(DERIVED_DIR, 'manifests', 'checkpoints')], manifestRoots: [join(DERIVED_DIR, 'manifests')] },
    );
    assert.ok(closedProof.ok, JSON.stringify(closedProof));
  });

  it('treats empty 30s windows as valid blocks and resumes from v4 cursor', async () => {
    clean();
    // Closed segment: block 0 has trades, block 1 empty (no envelopes), block 2 has trades
    writeSegment(TEST_DIR, 'trades', MARKET, '1970-01-01', '00-00', [
      envelope(trade(500, 'buy', 100, 1), 500),
      envelope(trade(60500, 'sell', 101, 1), 60500),
    ]);

    const first = await runPipeline({
      dataDir: TEST_DIR,
      market: MARKET,
      fromMs: 0,
      toMs: 90000,
      runId: 'v4-empty-1',
      outputRoot: DERIVED_DIR,
      rawLayout: 'v4',
      finalizedThroughMs: 90000,
    });

    // Should process block 0, block 1 (empty), and EOF finalize block 2
    assert.equal(first.processed, 3, `expected 3 processed, got ${first.processed}; blocked=${first.blocked}`);
    assert.equal(first.blocked, undefined);

    const cp = loadCheckpoint(MARKET, DERIVED_DIR, 'trades');
    assert.equal(cp.pending_block, null);
    assert.equal(cp.last_committed_block_start, 60000);
    assert.ok(cp.raw_v4_cursor?.byte_offset >= 0);

    // Resume from checkpoint: no new data, should be idle/no-op
    const second = await runPipeline({
      dataDir: TEST_DIR,
      market: MARKET,
      fromMs: 0,
      toMs: 120000,
      runId: 'v4-empty-2',
      outputRoot: DERIVED_DIR,
      rawLayout: 'v4',
    });
    assert.equal(second.processed, 0);
  });

  it('builds #12 notional lookup and trade history from v4 source', async () => {
    clean();
    writeSegment(TEST_DIR, 'trades', MARKET, '1970-01-01', '00-00', [
      envelope(trade(500, 'buy', 100, 1), 500),
      envelope(trade(30500, 'sell', 101, 1), 30500),
      envelope(trade(60500, 'buy', 102, 1), 60500),
    ]);

    const source = new RawV4BlockSource({ root: TEST_DIR, kind: 'trades', market: MARKET });
    await source.open();
    const { blocks } = await source.loadBlocks({ fromMs: 0, toMs: 90000 });
    await source.close();

    assert.equal(blocks.length, 3);
    const lookup = source.buildTradedNotionalLookup(30000);
    assert.equal(lookup.get(30000), 100 * 1); // only block 0 trade in window [0, 30000)
    assert.equal(lookup.get(31000), 101 * 1); // only block 1 trade in window [1000, 31000)

    const history = source.buildTradeHistory(60000);
    assert.equal(history.trades.length, 3); // block 0, block 1 and block 2
  });

  it('uses byte cursor to suppress rescan on resume', async () => {
    clean();
    writeSegment(TEST_DIR, 'trades', MARKET, '1970-01-01', '00-00', [
      envelope(trade(500, 'buy', 100, 1), 500),
      envelope(trade(30500, 'sell', 101, 1), 30500),
    ]);

    // First run commits block 0 and leaves block 1 pending
    const first = await runPipeline({
      dataDir: TEST_DIR,
      market: MARKET,
      fromMs: 0,
      toMs: 60000,
      runId: 'v4-cursor-1',
      outputRoot: DERIVED_DIR,
      rawLayout: 'v4',
    });
    assert.equal(first.processed, 1);
    assert.equal(first.blocked, true);

    // Append next closed segment so block 1 can be committed and block 2 finalized
    writeSegment(TEST_DIR, 'trades', MARKET, '1970-01-01', '00-01', [
      envelope(trade(60500, 'buy', 102, 1), 60500),
    ]);

    const second = await runPipeline({
      dataDir: TEST_DIR,
      market: MARKET,
      fromMs: 0,
      toMs: 90000,
      runId: 'v4-cursor-2',
      outputRoot: DERIVED_DIR,
      rawLayout: 'v4',
      finalizedThroughMs: 90000,
    });
    assert.equal(second.processed, 2);
    assert.equal(second.blocked, undefined);

    const cp = loadCheckpoint(MARKET, DERIVED_DIR, 'trades');
    assert.equal(cp.pending_block, null);
    assert.equal(cp.last_committed_block_start, 60000);
  });

  it('blocks EOF-finalization of active block even with finalized-through', async () => {
    clean();
    writeSegment(TEST_DIR, 'trades', MARKET, '1970-01-01', '00-00', [
      envelope(trade(500, 'buy', 100, 1), 500),
    ], true);

    const result = await runPipeline({
      dataDir: TEST_DIR,
      market: MARKET,
      fromMs: 0,
      toMs: 60000,
      runId: 'v4-active-horizon',
      outputRoot: DERIVED_DIR,
      rawLayout: 'v4',
      finalizedThroughMs: 60000,
    });

    assert.equal(result.processed, 0);
    assert.equal(result.blocked, true);
    assert.equal(result.blockedReason, 'active-incomplete-block');
  });

  it('tracks book_updates ordering over v4 segments and blocks active trailing block', async () => {
    clean();
    writeSegment(TEST_DIR, 'book_updates', MARKET, '1970-01-01', '00-00', [
      envelope({ market: MARKET, type: 'snapshot', ts: 500, bids: [[100, 1]], asks: [[101, 1]] }, 500),
      envelope({ market: MARKET, type: 'update', ts: 30500, bids: [[101, 1]], asks: [[102, 1]] }, 30500),
    ], true);

    const result = await runPipeline({
      dataDir: TEST_DIR,
      market: MARKET,
      fromMs: 0,
      toMs: 60000,
      runId: 'v4-book-1',
      outputRoot: DERIVED_DIR,
      rawLayout: 'v4',
      kind: 'book_updates',
    });

    assert.equal(result.processed, 0); // non-trade only counts verified-missing gap blocks
    assert.equal(result.blocked, true);
    assert.equal(result.blockedReason, 'active-incomplete-block');
    // Blocked non-trade runs do not persist a checkpoint.
  });

  it('fails closed for missing raw_v4 book windows', async () => {
    clean();
    writeSegment(TEST_DIR, 'book_updates', MARKET, '1970-01-01', '00-00', [
      envelope({ market: MARKET, type: 'snapshot', ts: 60500, bids: [[100, 1]], asks: [[101, 1]] }, 60500),
    ]);

    const result = await runPipeline({
      dataDir: TEST_DIR,
      market: MARKET,
      fromMs: 0,
      toMs: 90000,
      runId: 'v4-book-gap',
      outputRoot: DERIVED_DIR,
      rawLayout: 'v4',
      kind: 'book_updates',
    });

    assert.equal(result.blocked, true);
    assert.equal(result.blockedReason, 'verified-missing');
  });
});
