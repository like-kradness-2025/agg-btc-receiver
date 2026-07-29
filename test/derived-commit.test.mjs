import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, readFile } from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import { commitDerived } from '../lib/downstream/derived-commit.mjs';

describe('derived commit', () => {
  it('commits atomically and retries idempotently', async () => {
    const root = await mkdtemp(path.join(os.tmpdir(), 'derived-commit-'));
    const outputPath = path.join(root, 'features.jsonl');
    const first = await commitDerived({ outputPath, content: '{"ts":1}\n', source: { cursor: 1 } });
    const second = await commitDerived({ outputPath, content: '{"ts":1}\n', source: { cursor: 1 } });
    assert.equal(first.status, 'committed');
    assert.equal(second.status, 'idempotent');
    assert.equal(await readFile(outputPath, 'utf8'), '{"ts":1}\n');
  });

  it('quarantines conflicting output instead of overwriting', async () => {
    const root = await mkdtemp(path.join(os.tmpdir(), 'derived-commit-'));
    const outputPath = path.join(root, 'features.jsonl');
    await commitDerived({ outputPath, content: 'old' });
    const result = await commitDerived({ outputPath, content: 'new' });
    assert.equal(result.status, 'quarantine');
    assert.equal(await readFile(outputPath, 'utf8'), 'old');
  });
});
