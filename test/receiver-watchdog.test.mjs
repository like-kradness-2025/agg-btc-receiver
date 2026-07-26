import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { spawnSync } from 'node:child_process';

const ROOT = path.resolve(import.meta.dirname, '..');
const WATCHDOG = path.join(ROOT, 'scripts', 'receiver-auto-restart-watchdog.py');

function runWatchdog(healthPath, runtimeDir) {
  return spawnSync('python3', [WATCHDOG, '--dry-run'], {
    encoding: 'utf8',
    env: {
      ...process.env,
      XDG_RUNTIME_DIR: runtimeDir,
      RECEIVER_HEALTH_PATH: healthPath,
      RECEIVER_REQUIRED_MARKETS: 'binance_perp',
    },
  });
}

describe('receiver watchdog', () => {
  it('uses health signals instead of raw price distance', () => {
    const source = fs.readFileSync(WATCHDOG, 'utf8');
    assert.doesNotMatch(source, /raw_outlier_batches|raw_book_outliers|statistics\.median|sqlite3/);

    const runtimeDir = fs.mkdtempSync(path.join(os.tmpdir(), 'agg-watchdog-'));
    const healthPath = path.join(runtimeDir, 'health.jsonl');
    const now = Date.now();
    fs.writeFileSync(healthPath, `${JSON.stringify({
      ts: now,
      markets: {
        binance_perp: {
          state: 'running',
          lastDepthMsgAt: now,
          lastTradeMsgAt: now,
        },
      },
    })}\n`);
    const healthy = runWatchdog(healthPath, runtimeDir);
    assert.equal(healthy.status, 0, healthy.stderr);
    assert.deepEqual(JSON.parse(healthy.stdout).reasons, []);

    fs.writeFileSync(healthPath, `${JSON.stringify({
      ts: now,
      markets: {
        binance_perp: {
          state: 'error',
          lastDepthMsgAt: now,
          lastTradeMsgAt: now,
        },
      },
    })}\n`);
    let unhealthy;
    for (let i = 0; i < 3; i += 1) {
      unhealthy = runWatchdog(healthPath, runtimeDir);
      assert.equal(unhealthy.status, 0, unhealthy.stderr);
    }
    const result = JSON.parse(unhealthy.stdout);
    assert.equal(result.action, 'would_restart');
    assert.deepEqual(result.reasons, ['market_state:binance_perp:error']);
  });

  it('routes restart requests to one market worker', () => {
    const main = fs.readFileSync(path.join(ROOT, 'orderflow_monitor.mjs'), 'utf8');
    const worker = fs.readFileSync(path.join(ROOT, 'lib', 'orderflow-worker.mjs'), 'utf8');
    assert.match(main, /process\.on\('SIGUSR2'/);
    assert.match(main, /cmd: 'restartMarket'/);
    assert.match(worker, /async function restartMarket/);
    assert.match(worker, /case 'restartMarket'/);
  });
});
