// test/burst-reducer/b5-checkpoint.test.mjs — B5 book checkpoint, verified-missing, kind-aware recovery tests

import { describe, it, before, after } from 'node:test';
import assert from 'node:assert/strict';
import { existsSync, mkdirSync, rmSync, writeFileSync, readFileSync } from 'node:fs';
import { join } from 'node:path';
import { runPipeline } from '../../lib/burst-reducer/pipeline.mjs';
import { writeCheckpoint, loadCheckpoint, writeVerifiedMissingRecord, loadManifest } from '../../lib/burst-reducer/manifest-manager.mjs';
import { reconcileMarketState } from '../../lib/burst-reducer/recovery.mjs';

const TEST_DIR = join('test', 'fixtures', 'b5-checkpoint');
const MARKET = 'test_b5';
const RUN_ID = 'b5-test';
const DERIVED_DIR = join('test', 'fixtures', 'b5-checkpoint', 'derived');

function cleanDerived() {
  rmSync(DERIVED_DIR, { recursive: true, force: true });
}

function setupBookFixtures() {
  rmSync(TEST_DIR, { recursive: true, force: true });
  mkdirSync(join(TEST_DIR, 'book_updates', MARKET, '1970-01-01'), { recursive: true });
  writeFileSync(join(TEST_DIR, 'book_updates', MARKET, '1970-01-01', '00-00-00.jsonl'),
    '{"schema_version":"book_updates_v1","market":"' + MARKET + '","type":"snapshot","event_ts_ms":500,"seq":1,"prev_seq":null,"bids":[["100","1"]],"asks":[["101","1"]],"source":{"exchange":"test","channel":"book"}}\n');
  writeFileSync(join(TEST_DIR, 'book_updates', MARKET, '1970-01-01', '00-01-00.jsonl'),
    '{"schema_version":"book_updates_v1","market":"' + MARKET + '","type":"update","event_ts_ms":60500,"seq":2,"prev_seq":1,"bids":[["100","2"]],"asks":[["101","2"]],"source":{"exchange":"test","channel":"book"}}\n');
}

describe('B5: Book checkpoint write/load', () => {
  before(() => { setupBookFixtures(); cleanDerived(); });
  after(() => { rmSync(TEST_DIR, { recursive: true, force: true }); });

  it('writes and loads book_updates checkpoint with kind-qualified filename', () => {
    writeCheckpoint({
      market: MARKET,
      pending_block: {
        block_start_ms: 30000,
        replay_identity: { market: MARKET, block_start_ms: 30000, input_path: `book_updates/${MARKET}/1970-01-01/00-00-30.jsonl` },
      },
      last_committed_block_start: null,
      generation: 0,
      open_burst: null,
      kind: 'book_updates',
      derivedDir: DERIVED_DIR,
    });

    // Kind-qualified filename
    const cpPath = join(DERIVED_DIR, 'manifests/checkpoints', `${MARKET}.book_updates.json`);
    assert.ok(existsSync(cpPath), 'checkpoint should exist with kind-qualified filename');
    const cp = JSON.parse(readFileSync(cpPath, 'utf8'));
    assert.equal(cp.kind, 'book_updates');
    assert.equal(cp.pending_block.block_start_ms, 30000);
    assert.equal(cp.generation, 0);

    // loadCheckpoint with kind
    const loaded = loadCheckpoint(MARKET, DERIVED_DIR, 'book_updates');
    assert.ok(loaded);
    assert.equal(loaded.kind, 'book_updates');
    assert.equal(loaded.pending_block.block_start_ms, 30000);

    // trades checkpoint should not exist
    const tradesCp = loadCheckpoint(MARKET, DERIVED_DIR, 'trades');
    assert.equal(tradesCp, null, 'trades checkpoint should not exist when only book_updates was written');
  });

  it('kind-aware recovery returns book_updates checkpoint', () => {
    cleanDerived();
    writeCheckpoint({
      market: MARKET,
      pending_block: { block_start_ms: 60000, replay_identity: { market: MARKET, block_start_ms: 60000, input_path: `book_updates/${MARKET}/1970-01-01/00-01-00.jsonl` } },
      last_committed_block_start: null,
      generation: 1,
      open_burst: null,
      kind: 'book_updates',
      derivedDir: DERIVED_DIR,
    });

    const result = reconcileMarketState(MARKET, DERIVED_DIR, 'book_updates');
    assert.ok(result.cursor);
    assert.equal(result.cursor.kind, 'book_updates');
    assert.equal(result.cursor.generation, 1);
    assert.equal(result.cursor.pending_block.block_start_ms, 60000);
    assert.equal(result.errors.length, 0);
  });

  it('trades recovery still works for trades kind (regression)', () => {
    cleanDerived();
    // Write trades checkpoint
    writeCheckpoint({
      market: MARKET,
      pending_block: { block_start_ms: 0, replay_identity: { market: MARKET, block_start_ms: 0, input_path: `trades/${MARKET}/1970-01-01/00-00-00.jsonl` }, trade_input_sha256: 'abc' },
      last_committed_block_start: null,
      generation: 1,
      open_burst: null,
      kind: 'trades',
      derivedDir: DERIVED_DIR,
    });

    const result = reconcileMarketState(MARKET, DERIVED_DIR, 'trades');
    assert.ok(result.cursor);
    assert.equal(result.cursor.kind, 'trades');
    assert.equal(result.cursor.generation, 1);
    assert.equal(result.cursor.pending_block.block_start_ms, 0);
  });
});

describe('B5: Verified-missing persistence', () => {
  before(() => { setupBookFixtures(); cleanDerived(); });
  after(() => { rmSync(TEST_DIR, { recursive: true, force: true }); });

  it('writeVerifiedMissingRecord persists to manifest', () => {
    writeVerifiedMissingRecord(MARKET, 30000, {
      kind: 'book_updates',
      reason: 'verified-missing-gap',
      gap_range: { start_ms: 30000, end_ms_exclusive: 60000 },
    }, DERIVED_DIR);

    const manifest = loadManifest(MARKET, DERIVED_DIR);
    assert.ok(manifest, 'manifest should exist');
    const key = `verified_missing:${MARKET}:30000`;
    assert.ok(manifest.processed_blocks[key], 'verified_missing record should be in manifest');
    assert.equal(manifest.processed_blocks[key].reason, 'verified-missing');
    assert.equal(manifest.processed_blocks[key].details.reason, 'verified-missing-gap');
    assert.equal(manifest.processed_blocks[key].status, 'verified_missing');
    assert.equal(manifest.processed_blocks[key].market, MARKET);
    assert.equal(manifest.processed_blocks[key].block_start_ms, 30000);
  });

  it('multiple verified-missing blocks are all tracked', () => {
    writeVerifiedMissingRecord(MARKET, 0, { kind: 'book_updates', reason: 'pending-block-file-not-found' }, DERIVED_DIR);
    writeVerifiedMissingRecord(MARKET, 30000, { kind: 'book_updates', reason: 'verified-missing-gap' }, DERIVED_DIR);
    writeVerifiedMissingRecord(MARKET, 60000, { kind: 'book_updates', reason: 'verified-missing-gap' }, DERIVED_DIR);

    const manifest = loadManifest(MARKET, DERIVED_DIR);
    assert.ok(manifest.processed_blocks[`verified_missing:${MARKET}:0`]);
    assert.ok(manifest.processed_blocks[`verified_missing:${MARKET}:30000`]);
    assert.ok(manifest.processed_blocks[`verified_missing:${MARKET}:60000`]);
    assert.equal(Object.keys(manifest.processed_blocks).length, 3);
  });
});
