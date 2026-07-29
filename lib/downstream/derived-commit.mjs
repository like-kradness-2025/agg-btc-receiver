// Durable, idempotent commit primitive for derived artifacts.
// Cursor advancement must happen only after this function returns committed.

import { createHash, randomUUID } from 'node:crypto';
import { link, mkdir, open, readFile, rename, unlink, writeFile } from 'node:fs/promises';
import path from 'node:path';

async function durableFile(file, bytes) {
  await mkdir(path.dirname(file), { recursive: true });
  const temp = `${file}.tmp-${process.pid}-${randomUUID()}`;
  await writeFile(temp, bytes, { flag: 'wx' });
  const fd = await open(temp, 'r');
  try { await fd.sync(); } finally { await fd.close(); }
  return temp;
}

async function syncDir(dir) {
  const fd = await open(dir, 'r');
  try { await fd.sync(); } finally { await fd.close(); }
}

async function installNoClobber(temp, destination) {
  try {
    await link(temp, destination);
    await unlink(temp);
    return true;
  } catch (error) {
    if (error.code === 'EEXIST') {
      await unlink(temp).catch(() => {});
      return false;
    }
    throw error;
  }
}

const hashBytes = bytes => createHash('sha256').update(bytes).digest('hex');

export async function commitDerived({ outputPath, content, source = {}, manifestPath = `${outputPath}.manifest.json` } = {}) {
  if (!outputPath) throw new TypeError('outputPath is required');
  const bytes = Buffer.isBuffer(content) ? content : Buffer.from(typeof content === 'string' ? content : JSON.stringify(content));
  const contentSha256 = hashBytes(bytes);
  const manifest = {
    schema_version: 'derived_manifest_v1',
    output_path: path.resolve(outputPath),
    content_sha256: contentSha256,
    byte_length: bytes.length,
    source,
  };
  try {
    const existing = await readFile(outputPath);
    if (hashBytes(existing) === contentSha256) return { status: 'idempotent', ...manifest };
    return { status: 'quarantine', ...manifest, reason: 'output_hash_conflict' };
  } catch (e) {
    if (e.code !== 'ENOENT') throw e;
  }
  const temp = await durableFile(outputPath, bytes);
  if (!await installNoClobber(temp, outputPath)) {
    return { status: 'quarantine', ...manifest, reason: 'output_race_conflict' };
  }
  await syncDir(path.dirname(outputPath));
  const manifestTemp = await durableFile(manifestPath, Buffer.from(`${JSON.stringify(manifest, null, 2)}\n`));
  if (!await installNoClobber(manifestTemp, manifestPath)) {
    return { status: 'quarantine', ...manifest, reason: 'manifest_conflict' };
  }
  await syncDir(path.dirname(manifestPath));
  return { status: 'committed', ...manifest, manifest_path: path.resolve(manifestPath) };
}

export { hashBytes };
