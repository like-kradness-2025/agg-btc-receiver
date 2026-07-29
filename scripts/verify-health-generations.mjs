#!/usr/bin/env node

// Read-only verifier for health.jsonl and its retained rotation generation.
import fs from 'node:fs';
import crypto from 'node:crypto';
import path from 'node:path';
import { HEALTH_ROTATE_BYTES, HEALTH_ROTATE_GENERATIONS } from '../lib/health-monitor.mjs';

export function validateHealthGenerations(healthPath) {
  healthPath = path.resolve(healthPath);
  const manifestPath = `${healthPath}.manifest.json`;
  const manifest = JSON.parse(fs.readFileSync(manifestPath, 'utf8'));
  if (manifest.schema_version !== 'health_generation_manifest_v1') throw new Error('unsupported health manifest');
  if (manifest.generations !== HEALTH_ROTATE_GENERATIONS) throw new Error('unexpected generation count');
  if (!Number.isSafeInteger(manifest.rotate_bytes) || manifest.rotate_bytes <= 0) throw new Error('invalid rotate_bytes');

  const files = manifest.files.map(entry => {
    const file = path.resolve(entry.file);
    if (file !== healthPath && file !== `${healthPath}.1`) throw new Error(`manifest path outside health generations: ${entry.file}`);
    const stat = fs.statSync(file);
    const content = fs.readFileSync(file);
    const lines = content.toString('utf8').split('\n').filter(Boolean);
    const parsed = lines.map(line => JSON.parse(line));
    for (let i = 1; i < parsed.length; i++) {
      if (parsed[i].ts < parsed[i - 1].ts) throw new Error(`timestamps decrease in ${file}`);
    }
    const sha256 = crypto.createHash('sha256').update(content).digest('hex');
    if (entry.bytes !== stat.size || entry.rows !== parsed.length || entry.sha256 !== sha256) {
      throw new Error(`manifest mismatch: ${file}`);
    }
    return { file, bytes: stat.size, rows: parsed.length };
  });
  return { ok: true, health: healthPath, rotate_bytes: manifest.rotate_bytes, files };
}

function main(argv) {
  const index = argv.indexOf('--health');
  const healthPath = index >= 0 ? argv[index + 1] : path.resolve('data/live_v3/health.jsonl');
  if (!healthPath || healthPath.startsWith('--')) throw new Error('usage: verify-health-generations.mjs --health PATH');
  const result = validateHealthGenerations(path.resolve(healthPath));
  console.log(JSON.stringify(result, null, 2));
}

if (import.meta.url === `file://${process.argv[1]}`) {
  try { main(process.argv.slice(2)); }
  catch (error) { console.error(`[health-verify] ${error.message}`); process.exitCode = 1; }
}
