// ⚠️  LEGACY — Seed snapshot writer for old JSONL-based paths. The live
//    production pipeline does not generate per-second snapshot JSONL files.
//    See docs/current/canonical-pipeline.md for the canonical architecture.
//
// Durable synchronization snapshots.  Unlike rotating book updates, a seed
// snapshot is an independent recovery artifact and must remain writable when
// the current raw 30s window has already been finalized.

import { mkdir, open, rename, writeFile } from 'node:fs/promises';
import { join } from 'node:path';

export class SnapshotWriter {
  constructor(basePath, market) {
    this.basePath = basePath;
    this.market = market;
    this.counter = 0;
  }

  async write(snapshot) {
    const ts = Number(snapshot?.ts);
    if (!Number.isFinite(ts) || ts < 0) return false;
    const d = new Date(ts);
    const date = d.toISOString().slice(0, 10);
    const dir = join(this.basePath, 'snapshots', this.market, date);
    await mkdir(dir, { recursive: true });
    const id = `${Math.floor(ts)}-${process.pid}-${this.counter++}`;
    const target = join(dir, `${id}.jsonl`);
    const temp = `${target}.tmp`;
    await writeFile(temp, JSON.stringify(snapshot) + '\n', 'utf8');
    const handle = await open(temp, 'r+');
    try { await handle.sync(); } finally { await handle.close(); }
    await rename(temp, target);
    return true;
  }
}
