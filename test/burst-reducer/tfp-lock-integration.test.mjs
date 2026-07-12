// test/burst-reducer/tfp-lock-integration.test.mjs — Gate A: TFP direct-entry market lock integration test
// RED→GREEN: Tests that tfp.mjs acquires per-market flock, emits SKIP on contention,
// releases lock on normal/exception exit, and preserves cursor on skip.
//
// Phase: RED first — current tfp.mjs has NO lock; these tests MUST FAIL until implementation.

import assert from 'node:assert/strict';
import { describe, it, before, after } from 'node:test';
import { spawnSync, spawn } from 'node:child_process';
import { mkdirSync, rmSync, readFileSync, existsSync, writeFileSync } from 'node:fs';
import { join } from 'node:path';

const TEST_OUTPUT_ROOT = 'data/derived/burst_features_v1_test_tfp_lock';
const TEST_DATA = join('test', 'fixtures', 'burst-v1', 'tmp-tfp-lock');
const MARKET = 'test_market_lock';
const LOCK_FILE = join(TEST_OUTPUT_ROOT, 'locks', `${MARKET}.lock`);

// ── helpers ──

/** Spawn a process that holds the lock file via raw flock for holdMs ms. */
function holdRawLock(holdMs = 5000) {
  mkdirSync(join(TEST_OUTPUT_ROOT, 'locks'), { recursive: true });
  const child = spawn('/bin/bash', ['-c',
    `exec 42>"${LOCK_FILE}" && flock -x 42 && sleep ${holdMs / 1000}`,
  ], { stdio: ['ignore', 'pipe', 'pipe'] });
  return child;
}

/** Run tfp.mjs synchronously. Returns { status, stdout, stderr }. */
function runTfp(args = []) {
  return spawnSync('node', ['scripts/tfp.mjs', ...args], {
    timeout: 15000,
    stdio: ['ignore', 'pipe', 'pipe'],
    cwd: join(process.cwd()),  // repo root
  });
}

/** Extract JSON objects from stderr lines. */
function parseStderrLines(stderr) {
  return stderr.split('\n')
    .map(l => l.trim())
    .filter(l => l)
    .map(l => { try { return JSON.parse(l); } catch (_) { return null; } })
    .filter(Boolean);
}

// ── setup / teardown ──

describe('TFP direct-entry market lock (Gate A integration)', () => {
  before(() => {
    // Create minimal test data: market dir with a date dir, no jsonl files
    // → scanBlocks returns [] → pipeline runs but processes 0 blocks
    rmSync(TEST_DATA, { recursive: true, force: true });
    mkdirSync(join(TEST_DATA, 'trades', MARKET, '1970-01-01'), { recursive: true });

    rmSync(TEST_OUTPUT_ROOT, { recursive: true, force: true });
    mkdirSync(join(TEST_OUTPUT_ROOT, 'locks'), { recursive: true });
  });

  after(() => {
    rmSync(TEST_DATA, { recursive: true, force: true });
    rmSync(TEST_OUTPUT_ROOT, { recursive: true, force: true });
  });

  // ── Test 1: Lock acquired when free, released on exit ──
  it('RED-1: acquires market lock and releases on exit', () => {
    const r = runTfp([
      '--markets', MARKET,
      '--from', '1970-01-01T00:00:00Z',
      '--to', '1970-01-01T00:05:00Z',
      '--data', TEST_DATA,
      '--output-root', TEST_OUTPUT_ROOT,
    ]);

    const lines = parseStderrLines(r.stderr.toString());

    // Must contain lock-acquired INFO
    const acquired = lines.find(l => l.level === 'INFO' && l.msg === 'lock-acquired' && l.market === MARKET);
    assert.ok(acquired, `Expected lock-acquired INFO in stderr. Got: ${JSON.stringify(lines.map(l => l.level + ':' + (l.msg || l.reason)))}`);

    // Exit code 0 (not an error)
    assert.equal(r.status, 0, `Expected exit 0, got ${r.status}`);
  });

  // ── Test 2: Contention → structured SKIP, cursor unchanged, exit 0 ──
  it('RED-2: emits structured SKIP on lock contention, preserves cursor, exits 0', async () => {
    // Pre-acquire raw lock
    const holder = holdRawLock(6000);
    // Wait for holder to acquire the lock
    await new Promise(resolve => setTimeout(resolve, 600));

    // Run tfp.mjs for the SAME market — should fail to acquire, emit SKIP
    const r = runTfp([
      '--markets', MARKET,
      '--from', '1970-01-01T00:00:00Z',
      '--to', '1970-01-01T00:05:00Z',
      '--data', TEST_DATA,
      '--output-root', TEST_OUTPUT_ROOT,
    ]);

    const lines = parseStderrLines(r.stderr.toString());

    // Must contain structured SKIP
    const skip = lines.find(l => l.level === 'SKIP' && l.reason === 'lock-contention' && l.market === MARKET);
    assert.ok(skip, `Expected SKIP lock-contention in stderr. Got: ${JSON.stringify(lines.map(l => l.level + ':' + (l.msg || l.reason)))}`);
    assert.ok(skip.lock_file, 'SKIP must include lock_file path');
    assert.ok(skip.ts, 'SKIP must include ts');

    // Must NOT have lock-acquired for this market
    const acquired = lines.find(l => l.level === 'INFO' && l.msg === 'lock-acquired' && l.market === MARKET);
    assert.ok(!acquired, 'Must NOT acquire lock when contended');

    // Exit 0 (not an error)
    assert.equal(r.status, 0, `Expected exit 0 on contention, got ${r.status}`);

    // Verify no output was committed (cursor unchanged)
    const manifestPath = join(TEST_OUTPUT_ROOT, 'manifests', `${MARKET}.json`);
    // After SKIP, manifest should not exist (pipeline was never reached)
    assert.ok(!existsSync(manifestPath), `Manifest must NOT exist after SKIP (cursor unchanged). Found: ${manifestPath}`);

    // Cleanup: wait for holder to exit
    await new Promise(resolve => setTimeout(resolve, 6000));
  });

  // ── Test 3: Lock released after normal exit → re-acquirable ──
  it('RED-3: lock released after normal exit, re-acquirable by subsequent run', () => {
    // Run 1: acquires, processes, releases
    const r1 = runTfp([
      '--markets', MARKET,
      '--from', '1970-01-01T00:00:00Z',
      '--to', '1970-01-01T00:05:00Z',
      '--data', TEST_DATA,
      '--output-root', TEST_OUTPUT_ROOT,
    ]);
    assert.equal(r1.status, 0, `Run 1 exit 0, got ${r1.status}`);

    const lines1 = parseStderrLines(r1.stderr.toString());
    assert.ok(lines1.find(l => l.msg === 'lock-acquired'), 'Run 1 must acquire lock');

    // Run 2: should also be able to acquire (lock was released)
    const r2 = runTfp([
      '--markets', MARKET,
      '--from', '1970-01-01T00:00:00Z',
      '--to', '1970-01-01T00:05:00Z',
      '--data', TEST_DATA,
      '--output-root', TEST_OUTPUT_ROOT,
    ]);
    assert.equal(r2.status, 0, `Run 2 exit 0, got ${r2.status}`);

    const lines2 = parseStderrLines(r2.stderr.toString());
    assert.ok(lines2.find(l => l.msg === 'lock-acquired'), 'Run 2 must re-acquire lock after Run 1 released');
  });

  // ── Test 4: Different markets do not contend ──
  it('RED-4: different markets do not contend with each other', () => {
    const OTHER_MARKET = 'test_other_market';
    mkdirSync(join(TEST_DATA, 'trades', OTHER_MARKET, '1970-01-01'), { recursive: true });

    // Run with both markets
    const r = runTfp([
      '--markets', `${MARKET},${OTHER_MARKET}`,
      '--from', '1970-01-01T00:00:00Z',
      '--to', '1970-01-01T00:05:00Z',
      '--data', TEST_DATA,
      '--output-root', TEST_OUTPUT_ROOT,
    ]);
    assert.equal(r.status, 0, `Dual-market exit 0, got ${r.status}`);

    const lines = parseStderrLines(r.stderr.toString());
    const acquired = lines.filter(l => l.msg === 'lock-acquired');
    assert.equal(acquired.length, 2, `Expected 2 lock-acquired (one per market), got ${acquired.length}: ${JSON.stringify(acquired)}`);
  });
});

// ── Fix A regression: cwd-independent lock-helper sourcing ──

const TEST_CWD_ROOT = 'data/derived/burst_features_v1_test_cwd_indep';
const MARKET_CWD = 'test_cwd_indep';

describe('Fix A: cwd-independent lock-helper sourcing', () => {
  before(() => {
    rmSync(TEST_CWD_ROOT, { recursive: true, force: true });
    mkdirSync(join(TEST_CWD_ROOT, 'locks'), { recursive: true });
    mkdirSync(join('test', 'fixtures', 'burst-v1', 'tmp-tfp-cwd', 'trades', MARKET_CWD, '1970-01-01'), { recursive: true });
  });

  after(() => {
    try { rmSync(TEST_CWD_ROOT, { recursive: true, force: true }); } catch (_) {}
    try { rmSync(join('test', 'fixtures', 'burst-v1', 'tmp-tfp-cwd'), { recursive: true, force: true }); } catch (_) {}
  });

  it('acquires lock when run from different working directory (absolute tfp.mjs path)', () => {
    // Resolve absolute path to tfp.mjs
    const absTfp = join(process.cwd(), 'scripts/tfp.mjs');
    // Run from /tmp — different cwd than repo root
    const r = spawnSync('node', [absTfp,
      '--markets', MARKET_CWD,
      '--from', '1970-01-01T00:00:00Z',
      '--to', '1970-01-01T00:05:00Z',
      '--data', 'test/fixtures/burst-v1/tmp-tfp-cwd',
      '--output-root', TEST_CWD_ROOT,
    ], {
      timeout: 15000,
      stdio: ['ignore', 'pipe', 'pipe'],
      cwd: '/tmp',
    });

    const lines = (r.stderr.toString()).split('\n')
      .map(l => l.trim()).filter(l => l)
      .map(l => { try { return JSON.parse(l); } catch (_) { return null; } })
      .filter(Boolean);

    const acquired = lines.find(l => l.level === 'INFO' && l.msg === 'lock-acquired' && l.market === MARKET_CWD);
    assert.ok(acquired, `Expected lock-acquired INFO when run from /tmp (abs path). Got exit=${r.status} stderr=${r.stderr.toString().slice(0, 400)}`);
    assert.equal(r.status, 0, `Expected exit 0, got ${r.status}`);
  });
});

// ── Fix C regression: timeout cleanup with process-group TERM→KILL ──

const TEST_TIMEOUT_ROOT = 'data/derived/burst_features_v1_test_timeout_cleanup';
const MARKET_TIMEOUT = 'test_timeout_cleanup';
const LOCK_FILE_TIMEOUT = join(TEST_TIMEOUT_ROOT, 'locks', `${MARKET_TIMEOUT}.lock`);

describe('Fix C: timeout cleanup releases lock FD', () => {
  before(() => {
    rmSync(TEST_TIMEOUT_ROOT, { recursive: true, force: true });
    mkdirSync(join(TEST_TIMEOUT_ROOT, 'locks'), { recursive: true });
    mkdirSync(join('test', 'fixtures', 'burst-v1', 'tmp-tfp-timeout', 'trades', MARKET_TIMEOUT, '1970-01-01'), { recursive: true });
  });

  after(() => {
    try { rmSync(TEST_TIMEOUT_ROOT, { recursive: true, force: true }); } catch (_) {}
    try { rmSync(join('test', 'fixtures', 'burst-v1', 'tmp-tfp-timeout'), { recursive: true, force: true }); } catch (_) {}
  });

  it('timeout produces FATAL (not SKIP) and releases lock FD', () => {
    // Run tfp.mjs with short timeout and a pre-acquire delay so the child
    // takes longer than the timeout → timer fires before flock attempt.
    const r = spawnSync('node', ['scripts/tfp.mjs',
      '--markets', MARKET_TIMEOUT,
      '--from', '1970-01-01T00:00:00Z',
      '--to', '1970-01-01T00:05:00Z',
      '--data', 'test/fixtures/burst-v1/tmp-tfp-timeout',
      '--output-root', TEST_TIMEOUT_ROOT,
    ], {
      timeout: 20000,
      stdio: ['ignore', 'pipe', 'pipe'],
      env: {
        ...process.env,
        TFP_LOCK_ACQUIRE_TIMEOUT_MS: '3000',       // 3s timeout seam
        TFP_LOCK_PRE_ACQUIRE_DELAY_MS: '10000',     // 10s pre-sleep → timeout fires first
      },
    });

    const stderr = r.stderr.toString();
    const stderrLines = stderr.split('\n')
      .map(l => l.trim()).filter(l => l)
      .map(l => { try { return JSON.parse(l); } catch (_) { return null; } })
      .filter(Boolean);

    // Must exit 1 (FATAL), not 0 (SKIP)
    assert.equal(r.status, 1, `Expected exit 1 (FATAL from timeout), got ${r.status}. stderr: ${stderr.slice(0, 400)}`);

    // Must have FATAL level with timeout-related reason
    const fatal = stderrLines.find(l => l.level === 'FATAL');
    assert.ok(fatal, `Expected FATAL in stderr. Got: ${stderr.slice(0, 400)}`);
    assert.ok(fatal.error.includes('timed out'), `FATAL error must mention 'timed out'. Got: ${fatal.error}`);

    // Verify lock file is released — should be re-acquirable now
    // (child was killed by process-group TERM→KILL in timeout cleanup)
    // First ensure lock dir exists (tfp.mjs may not have created it due to pre-sleep)
    mkdirSync(join(TEST_TIMEOUT_ROOT, 'locks'), { recursive: true });
    const reAcq = spawnSync('/bin/bash', ['-c',
      `exec 42>"${LOCK_FILE_TIMEOUT}" && flock -x -n 42 2>/dev/null && echo ACQUIRED || echo FAIL`
    ], { timeout: 3000, stdio: ['ignore', 'pipe', 'pipe'] });
    assert.equal(reAcq.stdout.toString().trim(), 'ACQUIRED',
      `Lock must be re-acquirable after timeout cleanup. Got: ${reAcq.stdout.toString().trim()}`);

    // Verify no orphan sleep processes hold this lock
    const lsofCheck = spawnSync('/bin/bash', ['-c',
      `lsof "${LOCK_FILE_TIMEOUT}" 2>/dev/null || true`
    ], { timeout: 3000, stdio: ['ignore', 'pipe', 'pipe'] });
    assert.equal(lsofCheck.stdout.toString().trim(), '',
      `Lock file must have no holding processes after timeout. lsof: ${lsofCheck.stdout.toString().trim()}`);
  });
});

// ── Cron wrapper regression tests ──
// Verify cron-reduce-burst-v1.sh exit-code capture, idempotency fast-path, and no double-lock.

const TEST_CRON_ROOT = 'data/derived/burst_features_v1_test_cron_wrapper';

describe('Cron wrapper regression (cron-reduce-burst-v1.sh)', () => {
  before(() => {
    rmSync(TEST_CRON_ROOT, { recursive: true, force: true });
    mkdirSync(join(TEST_CRON_ROOT, 'manifests'), { recursive: true });
    mkdirSync(join(TEST_CRON_ROOT, 'locks'), { recursive: true });
  });

  after(() => {
    try { rmSync(TEST_CRON_ROOT, { recursive: true, force: true }); } catch (_) {}
  });

  /**
   * Run a minimal shell snippet that mimics the cron wrapper's per-market loop.
   * Returns { exitCode, stdout, stderr }.
   */
  function runCronSnippet(extraCode, env = {}) {
    const script = `#!/usr/bin/env bash
set -euo pipefail
ALL_OUTPUT_ROOT="${TEST_CRON_ROOT}"
FROM_TS=0
FROM_ISO="1970-01-01T00:00:00+00:00"
TO_ISO="1970-01-01T00:05:00+00:00"
TOTAL_PROCESSED=0
TOTAL_ERRORS=0
FAILED_MARKETS=""
MARKET="test_cron"
${extraCode}
echo "FINAL processed=\${TOTAL_PROCESSED} errors=\${TOTAL_ERRORS}"`;
    const r = spawnSync('/bin/bash', ['-c', script], {
      timeout: 10000,
      stdio: ['ignore', 'pipe', 'pipe'],
      env: { ...process.env, ...env },
    });
    return {
      exitCode: r.status,
      stdout: r.stdout.toString().trim(),
      stderr: r.stderr.toString().trim(),
    };
  }

  it('exit code is properly captured (non-zero from tfp.mjs)', () => {
    // Simulate tfp.mjs exiting 1 by using a node -e that exits 1
    const r = runCronSnippet(`
set +e
OUTPUT=$(node -e 'console.error(JSON.stringify({level:"FATAL",market:"test_cron",error:"simulated"})); process.exit(1)' 2>&1)
EXIT_CODE=$?
set -euo pipefail
if [ "$EXIT_CODE" -ne 0 ] || echo "$OUTPUT" | grep -q '"level":"FATAL"'; then
  TOTAL_ERRORS=$((TOTAL_ERRORS + 1))
  FAILED_MARKETS="\${FAILED_MARKETS} \${MARKET}"
fi
`);
    // The snippet itself should exit 0 (errors are tracked, not fatal to wrapper)
    assert.ok(r.stdout.includes('errors=1'), `Expected errors=1, got: ${r.stdout}`);
  });

  it('exit code 0 with FATAL in output still counts as error', () => {
    // Simulate tfp.mjs exiting 0 but with FATAL in output (regression guard)
    const r = runCronSnippet(`
set +e
OUTPUT=$(node -e 'console.error(JSON.stringify({level:"FATAL",market:"test_cron",error:"late"})); process.exit(0)' 2>&1)
EXIT_CODE=$?
set -euo pipefail
if [ "$EXIT_CODE" -ne 0 ] || echo "$OUTPUT" | grep -q '"level":"FATAL"'; then
  TOTAL_ERRORS=$((TOTAL_ERRORS + 1))
  FAILED_MARKETS="\${FAILED_MARKETS} \${MARKET}"
fi
`);
    assert.ok(r.stdout.includes('errors=1'), `FATAL in output should count as error. Got: ${r.stdout}`);
  });

  it('exit code 0 with no FATAL and no processed counts as normal no-work', () => {
    const r = runCronSnippet(`
set +e
OUTPUT=$(node -e 'process.exit(0)' 2>&1)
EXIT_CODE=$?
set -euo pipefail
if echo "$OUTPUT" | grep -q '"processed":0'; then
  :
elif echo "$OUTPUT" | grep -q '"processed":'; then
  : # would parse
fi
if [ "$EXIT_CODE" -ne 0 ] || echo "$OUTPUT" | grep -q '"level":"FATAL"'; then
  TOTAL_ERRORS=$((TOTAL_ERRORS + 1))
fi
`);
    assert.ok(r.stdout.includes('errors=0'), `Normal no-work should have 0 errors. Got: ${r.stdout}`);
  });

  it('idempotency fast-path skips when manifest block >= FROM_MS', () => {
    // Create a manifest with last_checkpoint_block_start = 60000 (epoch ms)
    const manifest = { last_checkpoint_block_start: 60000, processed_blocks: {} };
    writeFileSync(
      join(TEST_CRON_ROOT, 'manifests', 'test_cron_fp.json'),
      JSON.stringify(manifest)
    );

    const r = runCronSnippet(`
MANIFEST_PATH="\${ALL_OUTPUT_ROOT}/manifests/test_cron_fp.json"
FROM_MS=0
SKIPPED="no"
if [ -f "$MANIFEST_PATH" ]; then
  LATEST_COMMITTED=$(python3 -c "
import json
try:
    m = json.load(open('$MANIFEST_PATH'))
    v = m.get('last_checkpoint_block_start')
    if v: print(v)
except: pass
" 2>/dev/null || true)
  if [ -n "$LATEST_COMMITTED" ] && [ "$LATEST_COMMITTED" -ge "$FROM_MS" ] 2>/dev/null; then
    SKIPPED="yes"
  fi
fi
echo "SKIPPED=\${SKIPPED}"
`);
    assert.ok(r.stdout.includes('SKIPPED=yes'), `Fast-path should skip. Got: ${r.stdout}`);
  });

  it('idempotency fast-path does NOT skip when manifest block < FROM_MS', () => {
    const manifest = { last_checkpoint_block_start: 1000, processed_blocks: {} };
    writeFileSync(
      join(TEST_CRON_ROOT, 'manifests', 'test_cron_fp2.json'),
      JSON.stringify(manifest)
    );

    const r = runCronSnippet(`
MANIFEST_PATH="\${ALL_OUTPUT_ROOT}/manifests/test_cron_fp2.json"
FROM_MS=60000
SKIPPED="no"
if [ -f "$MANIFEST_PATH" ]; then
  LATEST_COMMITTED=$(python3 -c "
import json
try:
    m = json.load(open('$MANIFEST_PATH'))
    v = m.get('last_checkpoint_block_start')
    if v: print(v)
except: pass
" 2>/dev/null || true)
  if [ -n "$LATEST_COMMITTED" ] && [ "$LATEST_COMMITTED" -ge "$FROM_MS" ] 2>/dev/null; then
    SKIPPED="yes"
  fi
fi
echo "SKIPPED=\${SKIPPED}"
`);
    assert.ok(r.stdout.includes('SKIPPED=no'), `Should NOT skip when manifest < FROM_MS. Got: ${r.stdout}`);
  });

  it('wrapper does NOT double-lock (no lock-helper source or acquire_market_lock)', () => {
    // Read the actual cron script and verify it doesn't source lock-helper
    const content = readFileSync('scripts/cron-reduce-burst-v1.sh', 'utf8');
    assert.ok(!content.includes('lock-helper.sh'), 'cron-reduce-burst-v1.sh must NOT source lock-helper.sh');
    assert.ok(!content.includes('acquire_market_lock'), 'cron-reduce-burst-v1.sh must NOT call acquire_market_lock');
  });
});

// ── P0 Gate A regression: malformed checkpoint fatal → lock released ──
// Isolated root to avoid shared test root contention.

const TEST_MALFORMED_ROOT = 'data/derived/burst_features_v1_test_tfp_malformed';
const TEST_DATA_MALFORMED = join('test', 'fixtures', 'burst-v1', 'tmp-tfp-malformed');
const MARKET_MALFORMED = 'test_malformed_cp';

describe('P0: malformed checkpoint releases lock (Gate A regression)', () => {
  before(() => {
    rmSync(TEST_MALFORMED_ROOT, { recursive: true, force: true });
    rmSync(TEST_DATA_MALFORMED, { recursive: true, force: true });
    mkdirSync(join(TEST_DATA_MALFORMED, 'trades', MARKET_MALFORMED, '1970-01-01'), { recursive: true });
    mkdirSync(join(TEST_MALFORMED_ROOT, 'manifests', 'checkpoints'), { recursive: true });
    mkdirSync(join(TEST_MALFORMED_ROOT, 'locks'), { recursive: true });
    // Malformed checkpoint: invalid JSON
    writeFileSync(
      join(TEST_MALFORMED_ROOT, 'manifests', 'checkpoints', `${MARKET_MALFORMED}.json`),
      'NOT VALID JSON {{{',
      'utf8'
    );
  });

  after(() => {
    try { rmSync(TEST_DATA_MALFORMED, { recursive: true, force: true }); } catch (_) {}
    try { rmSync(TEST_MALFORMED_ROOT, { recursive: true, force: true }); } catch (_) {}
  });

  it('exits 1 on malformed checkpoint, lock released, no orphan holder/sleep remains', () => {
    // Run tfp.mjs — fatal error from malformed checkpoint
    const r = spawnSync('node', ['scripts/tfp.mjs',
      '--markets', MARKET_MALFORMED,
      '--from', '1970-01-01T00:00:00Z',
      '--to', '1970-01-01T00:05:00Z',
      '--data', TEST_DATA_MALFORMED,
      '--output-root', TEST_MALFORMED_ROOT,
    ], { timeout: 15000, stdio: ['ignore', 'pipe', 'pipe'] });

    // Assert exit code 1 (fatal error from malformed checkpoint)
    assert.equal(r.status, 1, `Expected exit 1 (fatal), got ${r.status}\nstderr: ${r.stderr.toString().slice(0, 300)}`);

    // Verify FATAL + corrupt-checkpoint was emitted on stderr
    const stderr = r.stderr.toString();
    const hasFatal = stderr.includes('FATAL') && stderr.includes('corrupt-checkpoint');
    assert.ok(hasFatal, `Expected FATAL + corrupt-checkpoint in stderr. Output: ${stderr.slice(0, 400)}`);

    // Verify lock is released and can be re-acquired
    const lockFile = join(TEST_MALFORMED_ROOT, 'locks', `${MARKET_MALFORMED}.lock`);
    const reAcq = spawnSync('/bin/bash', ['-c',
      `exec 42>"${lockFile}" && flock -x -n 42 2>/dev/null && echo ACQUIRED || echo FAIL`
    ], { timeout: 3000, stdio: ['ignore', 'pipe', 'pipe'] });
    assert.equal(reAcq.stdout.toString().trim(), 'ACQUIRED',
      `Lock must be re-acquirable after fatal exit. Got: ${reAcq.stdout.toString().trim()}`);

    // Verify no orphan sleep 86400 process holds this lock
    const lsofCheck = spawnSync('/bin/bash', ['-c',
      `lsof "${lockFile}" 2>/dev/null || true`
    ], { timeout: 3000, stdio: ['ignore', 'pipe', 'pipe'] });
    assert.equal(lsofCheck.stdout.toString().trim(), '',
      `Lock file must have no holding processes. lsof: ${lsofCheck.stdout.toString().trim()}`);
  });
});

// ── P0: output-root as regular file → lock-dir/open fails → FATAL exit 1 ──

const TEST_FILE_ROOT = 'data/derived/burst_features_v1_test_tfp_fileroot';
const TEST_DATA_FILEROOT = join('test', 'fixtures', 'burst-v1', 'tmp-tfp-fileroot');
const MARKET_FILEROOT = 'test_fileroot';

describe('P0: output-root as regular file → FATAL exit 1', () => {
  before(() => {
    rmSync(TEST_FILE_ROOT, { recursive: true, force: true });
    rmSync(TEST_DATA_FILEROOT, { recursive: true, force: true });
    mkdirSync(join(TEST_DATA_FILEROOT, 'trades', MARKET_FILEROOT, '1970-01-01'), { recursive: true });
    // Create output-root as regular file → mkdir -p fails → acquire_market_lock returns 75
    // → shell case *) exit 3 → acquireLock rejects → FATAL exit 1
    writeFileSync(TEST_FILE_ROOT, '', 'utf8');
  });

  after(() => {
    try { rmSync(TEST_FILE_ROOT, { recursive: true, force: true }); } catch (_) {}
    try { rmSync(TEST_DATA_FILEROOT, { recursive: true, force: true }); } catch (_) {}
  });

  it('exits 1 with FATAL when output-root is a regular file', () => {
    const r = spawnSync('node', ['scripts/tfp.mjs',
      '--markets', MARKET_FILEROOT,
      '--from', '1970-01-01T00:00:00Z',
      '--to', '1970-01-01T00:05:00Z',
      '--data', TEST_DATA_FILEROOT,
      '--output-root', TEST_FILE_ROOT,
    ], { timeout: 15000, stdio: ['ignore', 'pipe', 'pipe'] });

    assert.equal(r.status, 1, `Expected exit 1 (FATAL), got ${r.status}`);

    const stderr = r.stderr.toString();
    // lock-helper.sh emits FATAL with lock-dir-create-failed or lock-file-open-failed
    // tfp.mjs catch block also emits FATAL with error message
    const hasLockHelperFatal = stderr.includes('FATAL') &&
      (stderr.includes('lock-dir-create-failed') || stderr.includes('lock-file-open-failed'));
    assert.ok(hasLockHelperFatal,
      `Expected FATAL with lock-dir-create-failed or lock-file-open-failed in stderr. Output: ${stderr.slice(0, 400)}`);
  });
});

// ── P0 (c): Contention preserves pre-existing manifest/checkpoint/output unchanged ──
// When lock contention occurs, any existing manifest, checkpoint, or output files
// must remain bit-for-bit identical (bytes, hash, mtime).

import { statSync } from 'node:fs';
import { createHash } from 'node:crypto';

const TEST_CONTENTION_PREEXIST_ROOT = 'data/derived/burst_features_v1_test_tfp_preexist';
const TEST_DATA_PREEXIST = join('test', 'fixtures', 'burst-v1', 'tmp-tfp-preexist');
const MARKET_PREEXIST = 'test_preexist';

/** Return a snapshot hash (sha256 hex) and mtimeMs for a file, or null if missing. */
function fileSnapshot(filePath) {
  if (!existsSync(filePath)) return null;
  const buf = readFileSync(filePath);
  const hash = createHash('sha256').update(buf).digest('hex');
  const mtimeMs = statSync(filePath).mtimeMs;
  return { size: buf.length, hash, mtimeMs };
}

describe('P0 (c): Contention preserves pre-existing manifest/checkpoint/output unchanged', () => {
  /** @type {Map<string, ReturnType<typeof fileSnapshot>>} */
  let beforeSnap = new Map();

  before(() => {
    rmSync(TEST_CONTENTION_PREEXIST_ROOT, { recursive: true, force: true });
    rmSync(TEST_DATA_PREEXIST, { recursive: true, force: true });
    mkdirSync(join(TEST_DATA_PREEXIST, 'trades', MARKET_PREEXIST, '1970-01-01'), { recursive: true });
    mkdirSync(join(TEST_CONTENTION_PREEXIST_ROOT, 'locks'), { recursive: true });
    mkdirSync(join(TEST_CONTENTION_PREEXIST_ROOT, 'manifests', 'checkpoints'), { recursive: true });

    // Pre-create manifest with known bytes
    const manifestDir = join(TEST_CONTENTION_PREEXIST_ROOT, 'manifests');
    const manifestPath = join(manifestDir, `${MARKET_PREEXIST}.json`);
    const manifestContent = JSON.stringify({ last_checkpoint_block_start: 30000, processed_blocks: { '30000': 1 } });
    writeFileSync(manifestPath, manifestContent, 'utf8');

    // Pre-create checkpoint with known bytes
    const cpPath = join(TEST_CONTENTION_PREEXIST_ROOT, 'manifests', 'checkpoints', `${MARKET_PREEXIST}.json`);
    const cpContent = JSON.stringify({ cursor: 30000, state: { burst_count: 0 } });
    writeFileSync(cpPath, cpContent, 'utf8');

    // Pre-create a dummy output file (feature output)
    const outDir = join(TEST_CONTENTION_PREEXIST_ROOT, 'features', MARKET_PREEXIST);
    mkdirSync(outDir, { recursive: true });
    const outPath = join(outDir, '1970-01-01.jsonl');
    writeFileSync(outPath, '{"block_start_ms":0}\n', 'utf8');

    // Snapshot all three before contention
    beforeSnap.set('manifest', fileSnapshot(manifestPath));
    beforeSnap.set('checkpoint', fileSnapshot(cpPath));
    beforeSnap.set('output', fileSnapshot(outPath));
  });

  after(() => {
    try { rmSync(TEST_CONTENTION_PREEXIST_ROOT, { recursive: true, force: true }); } catch (_) {}
    try { rmSync(TEST_DATA_PREEXIST, { recursive: true, force: true }); } catch (_) {}
  });

  it('manifest bytes/hash/mtime unchanged after contention SKIP', async () => {
    // Pre-acquire raw lock to force contention
    const lockFile = join(TEST_CONTENTION_PREEXIST_ROOT, 'locks', `${MARKET_PREEXIST}.lock`);
    const holder = spawn('/bin/bash', ['-c',
      `exec 42>"${lockFile}" && flock -x 42 && sleep 6`
    ], { stdio: ['ignore', 'pipe', 'pipe'] });
    await new Promise(r => setTimeout(r, 600));

    // Run tfp.mjs — must hit contention → SKIP
    const r = spawnSync('node', ['scripts/tfp.mjs',
      '--markets', MARKET_PREEXIST,
      '--from', '1970-01-01T00:00:00Z',
      '--to', '1970-01-01T00:30:00Z',   // covers block 30000
      '--data', TEST_DATA_PREEXIST,
      '--output-root', TEST_CONTENTION_PREEXIST_ROOT,
    ], { timeout: 15000, stdio: ['ignore', 'pipe', 'pipe'] });

    // Must exit 0 (SKIP, not error)
    assert.equal(r.status, 0, `Expected exit 0 on contention SKIP, got ${r.status}`);
    const stderr = r.stderr.toString();
    assert.ok(stderr.includes('lock-contention'), `Expected lock-contention in stderr. Got: ${stderr.slice(0, 400)}`);

    // Verify pre-existing files are UNCHANGED
    const manifestPath = join(TEST_CONTENTION_PREEXIST_ROOT, 'manifests', `${MARKET_PREEXIST}.json`);
    const cpPath = join(TEST_CONTENTION_PREEXIST_ROOT, 'manifests', 'checkpoints', `${MARKET_PREEXIST}.json`);
    const outPath = join(TEST_CONTENTION_PREEXIST_ROOT, 'features', MARKET_PREEXIST, '1970-01-01.jsonl');

    for (const [label, filePath] of [['manifest', manifestPath], ['checkpoint', cpPath], ['output', outPath]]) {
      const before = beforeSnap.get(label);
      assert.ok(before, `${label} snapshot must exist before test`);
      const after = fileSnapshot(filePath);
      assert.ok(after, `${label} file must still exist after contention SKIP`);
      assert.equal(after.size, before.size, `${label}: size must be unchanged (was ${before.size}, got ${after.size})`);
      assert.equal(after.hash, before.hash, `${label}: sha256 must be unchanged (was ${before.hash}, got ${after.hash})`);
      assert.equal(after.mtimeMs, before.mtimeMs, `${label}: mtime must be unchanged (was ${before.mtimeMs}, got ${after.mtimeMs})`);
    }

    // Cleanup holder
    await new Promise(r => setTimeout(r, 6000));
  });
});

// ── P0 (d): Epoch 0 fast path — null checkpoint must NOT skip ──
// When last_checkpoint_block_start is null/missing, the cron idempotency
// fast-path must fall through (not skip), so epoch 0 data gets processed.

const TEST_EPOCH0_ROOT = 'data/derived/burst_features_v1_test_tfp_epoch0';

describe('P0 (d): Epoch 0 fast path — null checkpoint does NOT skip', () => {
  before(() => {
    rmSync(TEST_EPOCH0_ROOT, { recursive: true, force: true });
    mkdirSync(join(TEST_EPOCH0_ROOT, 'manifests'), { recursive: true });
    mkdirSync(join(TEST_EPOCH0_ROOT, 'locks'), { recursive: true });
  });

  after(() => {
    try { rmSync(TEST_EPOCH0_ROOT, { recursive: true, force: true }); } catch (_) {}
  });

  it('manifest with null/missing last_checkpoint_block_start → fast-path does NOT skip (epoch 0)', () => {
    // Create manifest where last_checkpoint_block_start is null/explicit null
    const manifest = { last_checkpoint_block_start: null, processed_blocks: {} };
    writeFileSync(
      join(TEST_EPOCH0_ROOT, 'manifests', 'test_epoch0_null.json'),
      JSON.stringify(manifest)
    );

    // Run the same Python snippet the cron wrapper uses
    const r = spawnSync('/bin/bash', ['-c', `
MANIFEST_PATH="${TEST_EPOCH0_ROOT}/manifests/test_epoch0_null.json"
FROM_MS=0
SKIPPED="no"
if [ -f "$MANIFEST_PATH" ]; then
  LATEST_COMMITTED=$(python3 -c "
import json
try:
    m = json.load(open('$MANIFEST_PATH'))
    v = m.get('last_checkpoint_block_start')
    if v is not None: print(v)
except: pass
" 2>/dev/null || true)
  if [ -n "$LATEST_COMMITTED" ] && [ "$LATEST_COMMITTED" -ge "$FROM_MS" ] 2>/dev/null; then
    SKIPPED="yes"
  fi
fi
echo "SKIPPED=$SKIPPED"
echo "LATEST_COMMITTED=[$LATEST_COMMITTED]"
`], { timeout: 5000, stdio: ['ignore', 'pipe', 'pipe'] });

    const stdout = r.stdout.toString();
    assert.ok(stdout.includes('SKIPPED=no'), `Epoch 0 with null cp must NOT skip. Got: ${stdout}`);
    assert.ok(stdout.includes('LATEST_COMMITTED=[]'), `LATEST_COMMITTED must be empty for null cp. Got: ${stdout}`);
  });

  it('manifest with missing last_checkpoint_block_start key → fast-path does NOT skip', () => {
    // Manifest without the key at all
    const manifest = { processed_blocks: {} };
    writeFileSync(
      join(TEST_EPOCH0_ROOT, 'manifests', 'test_epoch0_missing.json'),
      JSON.stringify(manifest)
    );

    const r = spawnSync('/bin/bash', ['-c', `
MANIFEST_PATH="${TEST_EPOCH0_ROOT}/manifests/test_epoch0_missing.json"
FROM_MS=0
SKIPPED="no"
if [ -f "$MANIFEST_PATH" ]; then
  LATEST_COMMITTED=$(python3 -c "
import json
try:
    m = json.load(open('$MANIFEST_PATH'))
    v = m.get('last_checkpoint_block_start')
    if v is not None: print(v)
except: pass
" 2>/dev/null || true)
  if [ -n "$LATEST_COMMITTED" ] && [ "$LATEST_COMMITTED" -ge "$FROM_MS" ] 2>/dev/null; then
    SKIPPED="yes"
  fi
fi
echo "SKIPPED=$SKIPPED"
echo "LATEST_COMMITTED=[$LATEST_COMMITTED]"
`], { timeout: 5000, stdio: ['ignore', 'pipe', 'pipe'] });

    const stdout = r.stdout.toString();
    assert.ok(stdout.includes('SKIPPED=no'), `Epoch 0 with missing cp key must NOT skip. Got: ${stdout}`);
    assert.ok(stdout.includes('LATEST_COMMITTED=[]'), `LATEST_COMMITTED must be empty for missing cp key. Got: ${stdout}`);
  });

  it('manifest with last_checkpoint_block_start=0 → fast-path DOES skip (already processed)', () => {
    // Explicit checkpoint at 0 — means epoch 0 was already processed
    const manifest = { last_checkpoint_block_start: 0, processed_blocks: { '0': 1 } };
    writeFileSync(
      join(TEST_EPOCH0_ROOT, 'manifests', 'test_epoch0_zero.json'),
      JSON.stringify(manifest)
    );

    const r = spawnSync('/bin/bash', ['-c', `
MANIFEST_PATH="${TEST_EPOCH0_ROOT}/manifests/test_epoch0_zero.json"
FROM_MS=0
SKIPPED="no"
if [ -f "$MANIFEST_PATH" ]; then
  LATEST_COMMITTED=$(python3 -c "
import json
try:
    m = json.load(open('$MANIFEST_PATH'))
    v = m.get('last_checkpoint_block_start')
    if v is not None: print(v)
except: pass
" 2>/dev/null || true)
  if [ -n "$LATEST_COMMITTED" ] && [ "$LATEST_COMMITTED" -ge "$FROM_MS" ] 2>/dev/null; then
    SKIPPED="yes"
  fi
fi
echo "SKIPPED=$SKIPPED"
`], { timeout: 5000, stdio: ['ignore', 'pipe', 'pipe'] });

    const stdout = r.stdout.toString();
    assert.ok(stdout.includes('SKIPPED=yes'), `Epoch 0 with checkpoint=0 must skip (already processed). Got: ${stdout}`);
  });
});
