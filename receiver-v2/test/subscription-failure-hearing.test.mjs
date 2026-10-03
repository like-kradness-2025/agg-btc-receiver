/**
 * A-2: a subscription failure is heard, distinguished from a pending one, and the run leaves its reason
 * behind before it exits.
 *
 * C3 already ends the run non-zero on a failed subscription. Astra's refinement asks for three more
 * things: the reason is carried on the diagnostic path before the exit; a temporary wait (pending) is
 * kept apart from a settled failure (failed); and `receiver: started` is the process-start line, not a
 * claim that the stream was established. These tests observe all three - the first two at the supervisor,
 * the last two as a real child process.
 */

import { test } from 'node:test';
import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { mkdtempSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';

import { createSupervisor } from '../src/supervisor/supervisor.mjs';

const PACKAGE_DIR = fileURLToPath(new URL('..', import.meta.url));
const ENTRY = fileURLToPath(new URL('../bin/receiver.mjs', import.meta.url));
const UNREACHABLE = 'ws://127.0.0.1:1/';

function withDir(fn) {
  const dir = mkdtempSync(join(tmpdir(), 'subrefine-'));
  try {
    return fn(dir);
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
}

function fakeSockets() {
  const sockets = [];
  const impl = function fakeSocket(url) {
    const socket = {
      url,
      sent: [],
      closed: false,
      onopen: null,
      onmessage: null,
      onclose: null,
      onerror: null,
      send(message) {
        socket.sent.push(message);
      },
      close() {
        socket.closed = true;
      },
    };
    sockets.push(socket);
    return socket;
  };
  return { sockets, impl };
}

function ackAdapter({ expectedSubscriptions } = {}) {
  return {
    url: 'ws://venue.test/ws',
    stream: 'trades',
    expectedSubscriptions,
    subscribeMessages: () => ['{"subscribe":"trades"}'],
    heartbeatMessage: () => null,
    parse: (raw) => {
      const parsed = JSON.parse(String(raw));
      if (parsed.rejected !== undefined) {
        return { kind: 'subscription', key: parsed.rejected, ok: false, detail: 'denied' };
      }
      if (parsed.subscribed !== undefined) {
        return { kind: 'subscription', key: parsed.subscribed, ok: true };
      }
      return { kind: 'data' };
    },
    changesFor: () => [{ side: 'bid', price: 100, size: 1 }],
  };
}

function build(dir, { adapter }) {
  const { sockets, impl } = fakeSockets();
  const timers = [];
  const exits = [];
  const stops = [];
  const diagnostics = [];
  const supervisor = createSupervisor({
    market: 'kraken_spot',
    stream: 'trades',
    adapter,
    path: join(dir, 'state.sqlite'),
    venue: 'kraken',
    runId: 'run-1',
    webSocketImpl: impl,
    spoolDir: join(dir, 'spool'),
    setTimer: (fn, ms) => {
      const timer = { fn, ms, cleared: false, unref() {} };
      timers.push(timer);
      return timer;
    },
    clearTimer: (timer) => {
      timer.cleared = true;
    },
    exit: (code) => exits.push(code),
    onStop: (stop) => stops.push(stop),
    onDiagnostic: (diagnostic) => diagnostics.push(diagnostic),
  });
  return { supervisor, sockets, timers, exits, stops, diagnostics };
}

test('a refused subscription leaves its reason on the diagnostic path before the run ends', () => {
  withDir((dir) => {
    const h = build(dir, { adapter: ackAdapter() });
    h.supervisor.start();
    h.sockets[0].onopen();
    h.sockets[0].onmessage({ data: '{"rejected":"trades"}' });

    assert.deepEqual(h.exits, [1], 'the failed subscription ends the run non-zero');
    assert.equal(h.stops.length, 1, 'the stop names a reason through onStop');
    assert.match(String(h.stops[0].reason), /subscription failed: denied/);
    assert.equal(
      h.diagnostics.some((d) => /subscription failed/.test(d.reason) && /denied/.test(d.reason)),
      true,
      'and the reason is on the diagnostic path, not only the exit code',
    );
    h.supervisor.close();
  });
});

test('a pending subscription is a wait, not a failure: nothing ends, nothing is reported as failed', () => {
  withDir((dir) => {
    const adapter = ackAdapter({ expectedSubscriptions: () => ['a', 'b'] });
    const h = build(dir, { adapter });
    h.supervisor.start();
    h.sockets[0].onopen();
    h.sockets[0].onmessage({ data: '{"subscribed":"a"}' });

    assert.equal(h.supervisor.stats.subscriptionState, 'pending', 'one of two expected keys is a wait');
    assert.deepEqual(h.exits, [], 'a wait does not end the run');
    assert.equal(h.stops.length, 0, 'and is not reported as a stop');
    assert.equal(
      h.diagnostics.some((d) => /subscription failed/.test(d.reason)),
      false,
      'and never as a settled failure',
    );
    assert.equal(h.supervisor.ready, false, 'while the stream is not established');

    // The second key settles it: the same state machine now reads acknowledged, and the run is alive.
    h.sockets[0].onmessage({ data: '{"subscribed":"b"}' });
    assert.equal(h.supervisor.stats.subscriptionState, 'acknowledged');
    assert.deepEqual(h.exits, []);
    h.supervisor.stop();
    h.supervisor.close();
  });
});

/** Spawn the real entrance and collect stdout/stderr until it exits. */
function runChild(configPath) {
  const child = spawn(process.execPath, [ENTRY, '--config', configPath], {
    cwd: PACKAGE_DIR,
    stdio: ['ignore', 'pipe', 'pipe'],
  });
  let stdout = '';
  let stderr = '';
  child.stdout.on('data', (chunk) => {
    stdout += chunk;
  });
  child.stderr.on('data', (chunk) => {
    stderr += chunk;
  });
  const exited = new Promise((resolve) => child.on('exit', (code, signal) => resolve({ code, signal })));
  return {
    get stdout() {
      return stdout;
    },
    get stderr() {
      return stderr;
    },
    exited,
  };
}

test('the process reports its reason before exiting, and start is not establishment', async () => {
  const dir = mkdtempSync(join(tmpdir(), 'subrefine-proc-'));
  try {
    const config = {
      venue: 'kraken',
      market: 'kraken_spot',
      stream: 'trades',
      symbol: 'XBT/USD',
      database: join(dir, 'state.sqlite'),
      spoolDir: join(dir, 'spool'),
      url: UNREACHABLE,
      // The venue is unreachable, so the run can only end at its deadline; the smallest allowed one keeps
      // the test fast.
      startupDeadlineMs: 1000,
    };
    const path = join(dir, 'receiver.config.json');
    writeFileSync(path, JSON.stringify(config, null, 2));

    const handle = runChild(path);
    const { code } = await handle.exited;

    assert.equal(code, 1, `the run ended non-zero; stderr=${handle.stderr}`);
    assert.match(handle.stdout, /receiver: started /, 'the process-start line was printed');
    assert.equal(
      /receiver: established/.test(handle.stdout),
      false,
      'but establishment was never claimed for a venue that never answered',
    );
    assert.match(handle.stderr, /receiver: stopping/, 'the failure reason was written before the exit');
    assert.match(handle.stderr, /startup deadline/i, 'and it names what went wrong');
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});
