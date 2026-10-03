/**
 * The raw writer's durability: it must not answer true for a frame whose bytes did not all reach the
 * file. `writeSync` may write fewer bytes than asked - a short write is not an error - and a writer that
 * takes the first short write as the whole line makes the raw claim a durability it does not have: the
 * frame's bytes are lost, the torn line cannot be read back on the next start, and the frame is gone.
 *
 * These tests drive the writer with an injected `fs` whose `writeSync` writes short on purpose, so the
 * exact failure the review reproduced in a child process is examined here as a unit.
 */

import { test } from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import { mkdtempSync, readFileSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

import { createFileRawWriter } from '../src/raw/file-writer.mjs';
import { makeEnvelope, dedupeKey } from '../src/envelope.mjs';

function envelope(seq) {
  return makeEnvelope({
    market: 'kraken_spot',
    stream: 'trades',
    connectionId: 'conn-1',
    runId: 'run-1',
    venue: 'kraken',
    generation: 1,
    receiveSeq: seq,
    recvTsMs: 1_792_000_000_000 + seq,
    recvMonoNs: 1_000_000 + seq,
    raw: JSON.stringify({ seq }),
    meta: { first_seq: 1 },
  });
}

async function withDir(fn) {
  const dir = mkdtempSync(join(tmpdir(), 'raw-writer-'));
  try {
    return await fn(dir);
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
}

test('a write that makes no progress answers false and does not claim the key was held', async () => {
  await withDir(async (dir) => {
    const filePath = join(dir, 'raw.log');
    const fsModule = { ...fs };
    let calls = 0;
    fsModule.writeSync = (fd, buf, off, len) => {
      calls += 1;
      // The first call writes half the line; every later call writes nothing at all - the disk is full
      // after the tear. A writer that trusted the first short write would answer true over lost bytes.
      if (calls === 1) return fs.writeSync(fd, buf, off, Math.max(1, Math.floor(len / 2)));
      return 0;
    };

    const write = createFileRawWriter({ path: filePath, fsModule });
    const frame = envelope(1);
    assert.equal(write(frame), false, 'a line that could not be finished is not durable');
    // The key was not recorded: the same frame is retried rather than answered true from memory.
    assert.equal(write(frame), false, 'the frame stays owed; nothing claims the raw holds it');
    write.close();
  });
});

test('a short write that eventually completes is written whole, not truncated', async () => {
  await withDir(async (dir) => {
    const filePath = join(dir, 'raw.log');
    const fsModule = { ...fs };
    fsModule.writeSync = (fd, buf, off, len) => fs.writeSync(fd, buf, off, Math.min(len, 3));

    const write = createFileRawWriter({ path: filePath, fsModule });
    const frame = envelope(1);
    assert.equal(write(frame), true, 'written in full, so the answer is true');

    const lines = readFileSync(filePath, 'utf8').trim().split('\n');
    assert.equal(lines.length, 1, 'exactly the one line');
    const record = JSON.parse(lines[0]);
    assert.equal(record.key, dedupeKey(frame), 'the whole line survived the short writes');
    assert.equal(record.envelope.raw, Buffer.from(JSON.stringify({ seq: 1 })).toString('base64'));
    write.close();
  });
});
