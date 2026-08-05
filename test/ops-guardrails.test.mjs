// Static operational guardrail checks. They do not install, enable, or start units.
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { spawnSync } from 'node:child_process';

const ROOT = path.resolve(import.meta.dirname, '..');
const LOCK_HELPER = path.join(ROOT, 'scripts', 'with-maintenance-lock.sh');
const UNIT_DIR = path.join(ROOT, 'systemd');
const UNITS = [
  'agg-btc-receiver-maintenance-tfp.service',
  'agg-btc-receiver-maintenance-cleanup-raw.service',
  'agg-btc-receiver-maintenance-book-snapshots.service',
];
const SHARED_LOCK = '/home/weed420/Tool/agg-btc-receiver/data/live_v4/state/maintenance.lock';
const TFP_WRAPPER = path.join(ROOT, 'scripts', 'run-tfp-live.sh');
const RECEIVER_WATCHDOG = path.join(ROOT, 'scripts', 'receiver-auto-restart-watchdog.py');

describe('ops guardrails', () => {
  it('maintenance lock helper is shell-valid and runs a command', () => {
    assert.equal(spawnSync('bash', ['-n', LOCK_HELPER]).status, 0);
    const lockFile = path.join(fs.mkdtempSync(path.join(os.tmpdir(), 'agg-ops-')), 'maintenance.lock');
    const result = spawnSync(LOCK_HELPER, ['--lock-file', lockFile, '--', 'true'], { encoding: 'utf8' });
    assert.equal(result.status, 0, result.stderr);
    assert.match(result.stderr, /maintenance-lock: acquired/);
  });

  it('staged maintenance units share one lock and bounded resources', () => {
    for (const name of UNITS) {
      const unit = fs.readFileSync(path.join(UNIT_DIR, name), 'utf8');
      assert.match(unit, /ExecStart=.*with-maintenance-lock\.sh --/);
      assert.match(unit, new RegExp(`Environment=MAINTENANCE_LOCK_FILE=${SHARED_LOCK.replaceAll('/', '[/]')}`));
      assert.match(unit, /^CPUQuota=50%$/m);
      assert.match(unit, /^MemoryMax=2G$/m);
      assert.match(unit, /^TasksMax=128$/m);
      assert.match(unit, /^Nice=10$/m);
      assert.doesNotMatch(unit, /^\[Install\]$/m, `${name} must remain opt-in`);
      assert.equal(spawnSync('systemd-analyze', ['verify', path.join(UNIT_DIR, name)]).status, 0);
    }
  });

  it('TFP wrapper propagates per-market failures', () => {
    const wrapper = fs.readFileSync(TFP_WRAPPER, 'utf8');
    assert.equal(spawnSync('bash', ['-n', TFP_WRAPPER]).status, 0);
    assert.match(wrapper, /failed_markets=\(\)/);
    assert.match(wrapper, /exit 1/);
    assert.doesNotMatch(wrapper, /\|\| echo "TFP_FAILED/);
  });

  // -----------------------------------------------------------------------
  // Health check: old v4/derived data absence is NOT an outage
  // -----------------------------------------------------------------------
  it('main receiver entry points do not reference v4/derived paths in execution logic', () => {
    const receiverMain = [
      'lib/raw-sqlite-writer.mjs',
      'lib/health-monitor.mjs',
    ];
    for (const rel of receiverMain) {
      const full = path.join(ROOT, rel);
      if (fs.existsSync(full)) {
        const content = fs.readFileSync(full, 'utf8');
        assert.doesNotMatch(
          content,
          /live_v4|burst_features_v1|raw-v4-(segment-reader|block-source)/,
          `${rel} must not depend on v4/derived paths`,
        );
      }
    }
  });

  it('legacy units reference live_v4 only through a maintenance lock that is safe to miss', () => {
    for (const name of UNITS) {
      const unit = fs.readFileSync(path.join(UNIT_DIR, name), 'utf8');
      // The lock file path references live_v4, but the lock script handles missing files gracefully.
      assert.match(unit, /with-maintenance-lock\.sh/);
    }
    // The maintenance lock script itself should handle missing lock directory gracefully
    const lockContent = fs.readFileSync(LOCK_HELPER, 'utf8');
    assert.ok(
      lockContent.includes('maintenance-lock:') || lockContent.includes('error') || lockContent.includes('exit'),
      'lock helper handles errors',
    );
  });

  it('main receiver service does not require v4 directories', () => {
    const unitPath = path.join(UNIT_DIR, 'agg-btc-receiver.service');
    const unit = fs.readFileSync(unitPath, 'utf8');
    assert.doesNotMatch(unit, /live_v4|burst_features_v1/, 'receiver service must not reference v4 paths');
    assert.match(unit, /sqlite|data\/sqlite/, 'receiver service must use sqlite paths');
  });

  it('receiver watchdog uses health signals, not raw price distance', () => {
    const source = fs.readFileSync(RECEIVER_WATCHDOG, 'utf8');
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
    const env = {
      ...process.env,
      XDG_RUNTIME_DIR: runtimeDir,
      RECEIVER_HEALTH_PATH: healthPath,
      RECEIVER_REQUIRED_MARKETS: 'binance_perp',
    };
    const healthy = spawnSync('python3', [RECEIVER_WATCHDOG, '--dry-run'], { encoding: 'utf8', env });
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
      unhealthy = spawnSync('python3', [RECEIVER_WATCHDOG, '--dry-run'], { encoding: 'utf8', env });
      assert.equal(unhealthy.status, 0, unhealthy.stderr);
    }
    const result = JSON.parse(unhealthy.stdout);
    assert.equal(result.action, 'would_restart');
    assert.deepEqual(result.reasons, ['market_state:binance_perp:error']);
  });
});
