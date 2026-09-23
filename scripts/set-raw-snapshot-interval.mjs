// Roll the 60s periodic-snapshot anchor out to every enabled market.
//
// A correction of a snapshot stage reads raw from the newest `snapshots` batch
// at/before the span start, so the smallest possible correction read is every byte
// written since that snapshot. With `raw_snapshot_interval_ms` unset (or 0)
// snapshots only appear on session start/reconnect: measured gaps 112-159 min made
// the smallest read 28-347MiB against the 32MiB bounded-replay cap, i.e. 13 of 15
// markets could not serve a correction at all. A 60s cadence makes the smallest
// read ~0.7-5MiB for every market, at +1-3% raw volume (measured average snapshot
// 9.6-86KB, 1-2 rows).
//
// Only markets that are enabled are touched; disabled entries keep whatever they
// have. Idempotent: an entry already at the target value is left alone.
//
//   node scripts/set-raw-snapshot-interval.mjs [--config config.v3.json] [--interval 60000] [--dry-run]
import { readFileSync, writeFileSync } from 'node:fs';
import path from 'node:path';

const arg = (name, fallback = null) => {
  const index = process.argv.indexOf(`--${name}`);
  return index >= 0 && process.argv[index + 1] ? process.argv[index + 1] : fallback;
};
const file = path.resolve(arg('config', 'config.v3.json'));
const interval = Number(arg('interval', '60000'));
const dryRun = process.argv.includes('--dry-run');

if (!Number.isInteger(interval) || interval <= 0) {
  console.error(`interval must be a positive integer, got ${arg('interval')}`);
  process.exit(2);
}

const original = readFileSync(file, 'utf8');
const config = JSON.parse(original);
const markets = config.markets ?? {};
const changed = [];
for (const [market, entry] of Object.entries(markets)) {
  if (!entry || typeof entry !== 'object' || !('wsUrl' in entry)) continue;
  if (entry.enabled === false) continue;
  if (entry.raw_snapshot_interval_ms === interval) continue;
  changed.push(`${market}: ${entry.raw_snapshot_interval_ms ?? 'unset'} -> ${interval}`);
  entry.raw_snapshot_interval_ms = interval;
}

if (!changed.length) { console.log('nothing to change (already at the target interval)'); process.exit(0); }
const next = `${JSON.stringify(config, null, 2)}\n`;
if (dryRun) { console.log(`dry run, ${changed.length} market(s):`); for (const line of changed) console.log(' ', line); process.exit(0); }
if (next === original) { console.log('the serialized form is identical; nothing written'); process.exit(0); }
writeFileSync(file, next);
console.log(`updated ${file}: ${changed.length} market(s)`);
for (const line of changed) console.log(' ', line);
