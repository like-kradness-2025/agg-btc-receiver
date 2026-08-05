import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, readFile, readdir } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { SnapshotWriter } from '../lib/snapshot-writer.mjs';

describe('SnapshotWriter', () => {
  it('writes an atomic seed artifact outside rotating raw windows', async () => {
    const root = await mkdtemp(join(tmpdir(), 'agg-btc-snapshot-'));
    const writer = new SnapshotWriter(root, 'okx_perp');
    const snapshot = { market: 'okx_perp', type: 'snapshot', ts: 1700000000123, seq: 10, bids: [], asks: [] };
    assert.equal(await writer.write(snapshot), true);
    const files = await readdir(join(root, 'snapshots', 'okx_perp', '2023-11-14'));
    assert.equal(files.length, 1);
    assert.deepEqual(JSON.parse(await readFile(join(root, 'snapshots', 'okx_perp', '2023-11-14', files[0]), 'utf8')), snapshot);
  });
});
