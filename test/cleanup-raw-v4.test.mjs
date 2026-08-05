import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { appendFile, mkdtemp, mkdir, readFile, rename, rm, stat, utimes, writeFile } from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import { spawnSync } from 'node:child_process';
import {
  detectRawLayout,
  discoverV4Segments,
  verifyV4ClosedSegment,
} from '../scripts/cleanup-raw.mjs';

const SCRIPT = path.resolve('scripts/cleanup-raw.mjs');

function sha256(content) {
  return createHash('sha256').update(content).digest('hex');
}

async function fixture() {
  const root = await mkdtemp(path.join(os.tmpdir(), 'cleanup-raw-v4-'));
  const segment = path.join(root, 'trades', 'm', '2026-07-19', '12-00.jsonl');
  const active = `${segment}.active`;
  const cursors = path.join(root, 'proof', 'cursors');
  const manifests = path.join(root, 'proof', 'manifests');
  await mkdir(path.dirname(segment), { recursive: true });
  await mkdir(cursors, { recursive: true });
  await mkdir(manifests, { recursive: true });
  const segmentContent = '{"schema":"raw_v4"}\n';
  await writeFile(segment, segmentContent);
  await writeFile(active, segmentContent);
  const old = new Date(Date.now() - 60_000);
  await utimes(segment, old, old);
  await utimes(active, old, old);
  const bytes = (await stat(segment)).size;
  const hash = sha256(segmentContent);
  await writeFile(path.join(cursors, 'm-trades.json'), JSON.stringify({
    schema_version: 'incremental_cursor_v1',
    source_path: segment,
    byte_offset: bytes,
    partial_line: '',
    source_hash: hash,
  }));
  await writeFile(path.join(manifests, 'm-trades.json'), JSON.stringify({
    schema_version: 'raw_consumer_manifest_v1',
    source_path: segment,
    status: 'committed',
    source_size: bytes,
    source_hash: hash,
  }));
  return { root, segment, active, cursors, manifests, hash, bytes };
}

describe('v4 raw cleanup proof gate', () => {
  it('recognizes v4 segments and never accepts an active segment', async () => {
    const f = await fixture();
    try {
      assert.equal(detectRawLayout(f.root), 'v4');
      const segments = discoverV4Segments(f.root);
      assert.equal(segments.length, 2);
      const active = segments.find((item) => item.active);
      assert.deepEqual(verifyV4ClosedSegment(active, {
        cursorRoots: [f.cursors],
        manifestRoots: [f.manifests],
      }), { ok: false, reason: 'active-segment' });
    } finally {
      await rm(f.root, { recursive: true, force: true });
    }
  });

  it('requires both EOF cursor and committed manifest proof for a closed segment', async () => {
    const f = await fixture();
    try {
      const segment = discoverV4Segments(f.root).find((item) => !item.active);
      const proof = verifyV4ClosedSegment(segment, {
        cursorRoots: [f.cursors],
        manifestRoots: [f.manifests],
      });
      assert.equal(proof.ok, true);

      await rm(f.manifests, { recursive: true, force: true });
      const missingManifest = verifyV4ClosedSegment(segment, {
        cursorRoots: [f.cursors],
        manifestRoots: [f.manifests],
      });
      assert.deepEqual(missingManifest, { ok: false, reason: 'missing-consumer-manifest-proof' });
    } finally {
      await rm(f.root, { recursive: true, force: true });
    }
  });

  it('auto-detects v4 and dry-run prints proof-gated deletion without deleting', async () => {
    const f = await fixture();
    try {
      const result = spawnSync(process.execPath, [
        SCRIPT, '--data', f.root, '--dry-run', '--safety-margin', '0',
        '--consumer-cursors', f.cursors,
        '--consumer-manifests', f.manifests,
      ], { encoding: 'utf8' });
      assert.equal(result.status, 0, result.stderr);
      assert.match(result.stdout, /Raw layout:\s+v4/);
      assert.match(result.stdout, /v4 closed \+ cursor EOF \+ manifest committed/);
      await stat(f.segment);
      await stat(f.active);
    } finally {
      await rm(f.root, { recursive: true, force: true });
    }
  });

  it('keeps the v3 auto-detect result for v3-shaped files', async () => {
    const root = await mkdtemp(path.join(os.tmpdir(), 'cleanup-raw-v3-'));
    try {
      const dir = path.join(root, 'trades', 'm', '2026-07-19');
      await mkdir(dir, { recursive: true });
      await writeFile(path.join(dir, '12-00-00.jsonl'), '{}\n');
      assert.equal(detectRawLayout(root), 'v3');
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });

  it('tracks active-to-closed rename in cursor proof', async () => {
    const f = await fixture();
    try {
      const segment = discoverV4Segments(f.root).find((item) => !item.active);
      // Cursor still points to the .active path even though the file was renamed to closed.
      await writeFile(path.join(f.cursors, 'm-trades.json'), JSON.stringify({
        schema_version: 'incremental_cursor_v1',
        source_path: f.active,
        byte_offset: f.bytes,
        partial_line: '',
        source_hash: f.hash,
      }));
      const proof = verifyV4ClosedSegment(segment, {
        cursorRoots: [f.cursors],
        manifestRoots: [f.manifests],
      });
      assert.equal(proof.ok, true);
    } finally {
      await rm(f.root, { recursive: true, force: true });
    }
  });

  it('accepts a consumed prefix proof after active append and close', async () => {
    const f = await fixture();
    try {
      await rm(f.segment);
      const appended = 'x'.repeat(100 * 1024);
      await appendFile(f.active, appended);
      await rename(f.active, f.segment);
      const finalBytes = (await stat(f.segment)).size;
      const finalHash = sha256(`${'{"schema":"raw_v4"}\n'}${appended}`);
      await writeFile(path.join(f.cursors, 'm-trades.json'), JSON.stringify({
        schema_version: 'incremental_cursor_v1',
        source_path: f.active,
        byte_offset: finalBytes,
        partial_line: '',
        source_hash: finalHash,
        raw_v4_segment_proof: [{
          source_path: f.active,
          byte_offset_end: f.bytes,
          source_prefix_size: f.bytes,
          source_prefix_hash: f.hash,
        }],
      }));
      await writeFile(path.join(f.manifests, 'm-trades.json'), JSON.stringify({
        schema_version: 'raw_consumer_manifest_v1',
        source_path: f.active,
        status: 'committed',
        source_size: f.bytes,
        source_hash: f.hash,
        byte_offset_end: f.bytes,
        source_prefix_size: f.bytes,
        source_prefix_hash: f.hash,
      }));
      const proof = verifyV4ClosedSegment(discoverV4Segments(f.root).find((item) => !item.active), {
        cursorRoots: [f.cursors],
        manifestRoots: [f.manifests],
      });
      assert.equal(proof.ok, true, JSON.stringify(proof));
    } finally {
      await rm(f.root, { recursive: true, force: true });
    }
  });

  it('ignores unrelated cursor/manifest JSON in the cursor root', async () => {
    const f = await fixture();
    try {
      const segment = discoverV4Segments(f.root).find((item) => !item.active);
      const unrelatedSegment = path.join(f.root, 'trades', 'm', '2026-07-19', '12-01.jsonl');
      await writeFile(path.join(f.cursors, 'unrelated-cursor.json'), JSON.stringify({
        schema_version: 'incremental_cursor_v1',
        source_path: unrelatedSegment,
        byte_offset: 0,
        partial_line: '',
        source_hash: '0'.repeat(64),
      }));
      await writeFile(path.join(f.cursors, 'unrelated-manifest.json'), JSON.stringify({
        schema_version: 'raw_consumer_manifest_v1',
        source_path: unrelatedSegment,
        status: 'committed',
        source_size: 999,
        source_hash: '0'.repeat(64),
      }));
      const proof = verifyV4ClosedSegment(segment, {
        cursorRoots: [f.cursors],
        manifestRoots: [f.manifests],
      });
      assert.equal(proof.ok, true);
    } finally {
      await rm(f.root, { recursive: true, force: true });
    }
  });

  it('rejects deletion when no cursor references the target segment', async () => {
    const f = await fixture();
    try {
      const segment = discoverV4Segments(f.root).find((item) => !item.active);
      await writeFile(path.join(f.cursors, 'm-trades.json'), JSON.stringify({
        schema_version: 'incremental_cursor_v1',
        source_path: path.join(f.root, 'trades', 'm', '2026-07-19', '12-01.jsonl'),
        byte_offset: 999,
        partial_line: '',
        source_hash: '0'.repeat(64),
      }));
      const proof = verifyV4ClosedSegment(segment, {
        cursorRoots: [f.cursors],
        manifestRoots: [f.manifests],
      });
      assert.equal(proof.ok, false);
      assert.equal(proof.reason, 'missing-consumer-cursor-proof');
    } finally {
      await rm(f.root, { recursive: true, force: true });
    }
  });

  it('computes SHA256 for large segments using fixed-size chunks', async () => {
    const f = await fixture();
    try {
      const segment = discoverV4Segments(f.root).find((item) => !item.active);
      const bigContent = '{"schema":"raw_v4"}\n' + 'x'.repeat(100 * 1024);
      await writeFile(f.segment, bigContent);
      const bytes = (await stat(f.segment)).size;
      const hash = sha256(bigContent);
      await writeFile(path.join(f.cursors, 'm-trades.json'), JSON.stringify({
        schema_version: 'incremental_cursor_v1',
        source_path: f.segment,
        byte_offset: bytes,
        partial_line: '',
        source_hash: hash,
      }));
      await writeFile(path.join(f.manifests, 'm-trades.json'), JSON.stringify({
        schema_version: 'raw_consumer_manifest_v1',
        source_path: f.segment,
        status: 'committed',
        source_size: bytes,
        source_hash: hash,
      }));
      const proof = verifyV4ClosedSegment(segment, {
        cursorRoots: [f.cursors],
        manifestRoots: [f.manifests],
      });
      assert.equal(proof.ok, true);
    } finally {
      await rm(f.root, { recursive: true, force: true });
    }
  });

  it('requires every consumer cursor to be at EOF', async () => {
    const f = await fixture();
    try {
      const segment = discoverV4Segments(f.root).find((item) => !item.active);
      // One cursor at EOF, one lagging behind.
      await writeFile(path.join(f.cursors, 'a.json'), JSON.stringify({
        schema_version: 'incremental_cursor_v1',
        source_path: segment.fullPath,
        byte_offset: f.bytes,
        partial_line: '',
        source_hash: f.hash,
      }));
      await writeFile(path.join(f.cursors, 'b.json'), JSON.stringify({
        schema_version: 'incremental_cursor_v1',
        source_path: segment.fullPath,
        byte_offset: 0,
        partial_line: '',
        source_hash: f.hash,
      }));
      const proof = verifyV4ClosedSegment(segment, {
        cursorRoots: [f.cursors],
        manifestRoots: [f.manifests],
      });
      assert.equal(proof.ok, false);
      assert.equal(proof.reason, 'incomplete-consumer-cursor-proof');
    } finally {
      await rm(f.root, { recursive: true, force: true });
    }
  });

  it('rejects deletion when source hash in manifest does not match', async () => {
    const f = await fixture();
    try {
      const segment = discoverV4Segments(f.root).find((item) => !item.active);
      await writeFile(path.join(f.manifests, 'm-trades.json'), JSON.stringify({
        schema_version: 'raw_consumer_manifest_v1',
        source_path: segment.fullPath,
        status: 'committed',
        source_size: f.bytes,
        source_hash: '0'.repeat(64),
      }));
      const proof = verifyV4ClosedSegment(segment, {
        cursorRoots: [f.cursors],
        manifestRoots: [f.manifests],
      });
      assert.equal(proof.ok, false);
      assert.equal(proof.reason, 'missing-consumer-manifest-proof');
    } finally {
      await rm(f.root, { recursive: true, force: true });
    }
  });

  it('rejects deletion when a quarantine report references the segment', async () => {
    const f = await fixture();
    try {
      const segment = discoverV4Segments(f.root).find((item) => !item.active);
      const quarantine = path.join(f.root, 'quarantine', 'trades', 'm', '2026-07-19');
      await mkdir(quarantine, { recursive: true });
      await writeFile(path.join(quarantine, '12-00.json'), JSON.stringify({
        schema_version: 'quarantine_v1',
        source_path: segment.fullPath,
        reason: 'SEQUENCE_GAP',
      }));
      const proof = verifyV4ClosedSegment(segment, {
        cursorRoots: [f.cursors],
        manifestRoots: [f.manifests],
        quarantineCheckpointRoots: [path.join(f.root, 'quarantine'), path.join(f.root, 'checkpoints')],
      });
      assert.equal(proof.ok, false);
      assert.equal(proof.reason, 'quarantine-or-checkpoint-reference');
    } finally {
      await rm(f.root, { recursive: true, force: true });
    }
  });

  it('rejects deletion when a checkpoint references the segment', async () => {
    const f = await fixture();
    try {
      const segment = discoverV4Segments(f.root).find((item) => !item.active);
      const checkpoints = path.join(f.root, 'checkpoints');
      await mkdir(checkpoints, { recursive: true });
      await writeFile(path.join(checkpoints, 'm.json'), JSON.stringify({
        schema_version: 'checkpoint_v1',
        market: 'm',
        last_checkpoint_block_start: 1752873600000,
        pending_block: { source_path: segment.fullPath },
      }));
      const proof = verifyV4ClosedSegment(segment, {
        cursorRoots: [f.cursors],
        manifestRoots: [f.manifests],
        quarantineCheckpointRoots: [path.join(f.root, 'quarantine'), checkpoints],
      });
      assert.equal(proof.ok, false);
      assert.equal(proof.reason, 'quarantine-or-checkpoint-reference');
    } finally {
      await rm(f.root, { recursive: true, force: true });
    }
  });

  it('applies a 24-hour safety margin by default for v4 segments', async () => {
    const f = await fixture();
    try {
      // Make the closed segment only 1 hour old.
      const recent = new Date(Date.now() - 60 * 60 * 1000);
      await utimes(f.segment, recent, recent);
      const result = spawnSync(process.execPath, [
        SCRIPT, '--data', f.root, '--dry-run',
        '--consumer-cursors', f.cursors,
        '--consumer-manifests', f.manifests,
      ], { encoding: 'utf8' });
      assert.equal(result.status, 0, result.stderr);
      assert.match(result.stdout, /Safety margin:\s+86400s/);
      assert.match(result.stdout, /skipped\(age\)=1/);
    } finally {
      await rm(f.root, { recursive: true, force: true });
    }
  });
});
