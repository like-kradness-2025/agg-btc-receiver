#!/usr/bin/env node
/**
 * screenshot.mjs — Take dashboard screenshot via headless Chrome
 *
 * Usage: node scripts/screenshot.mjs [--port 3847] [--output /tmp/agg-receiver-dash.png]
 *
 * Requires: google-chrome-stable on PATH, dashboard running on --port
 */

import { spawnSync } from 'node:child_process';
import { existsSync } from 'node:fs';
import { resolve } from 'node:path';

const PORT = process.argv.includes('--port')
  ? process.argv[process.argv.indexOf('--port') + 1] : '3847';
const OUT = process.argv.includes('--output')
  ? resolve(process.argv[process.argv.indexOf('--output') + 1]) : '/tmp/agg-receiver-dash.png';

const url = `http://127.0.0.1:${PORT}/`;
const chrome = 'google-chrome-stable';

// Verify chrome is available
if (!existsSync('/usr/bin/' + chrome) && !existsSync('/usr/local/bin/' + chrome)) {
  console.error(`[screenshot] ERROR: ${chrome} not found`);
  process.exit(2);
}

console.error(`[screenshot] Capturing ${url} → ${OUT}`);

const result = spawnSync(chrome, [
  '--headless=new',
  '--disable-gpu',
  '--no-sandbox',
  `--screenshot=${OUT}`,
  '--window-size=1600,1200',
  '--hide-scrollbars',
  url,
], { timeout: 30000, stdio: 'pipe' });

if (result.error) {
  console.error(`[screenshot] ERROR: ${result.error.message}`);
  process.exit(1);
}

const stderr = result.stderr.toString();
const match = stderr.match(/^(\d+) bytes written to file/m);
if (match) {
  console.log(`[screenshot] OK ${match[1]} bytes → ${OUT}`);
  process.exit(0);
}

// Some Chrome versions write to stdout instead
const stdout = result.stdout.toString();
const match2 = stdout.match(/^(\d+) bytes written to file/m);
if (match2) {
  console.log(`[screenshot] OK ${match2[1]} bytes → ${OUT}`);
  process.exit(0);
}

console.error('[screenshot] WARN: no size line in output');
console.error('stderr:', stderr);
console.error('stdout:', stdout);
process.exit(0);
