#!/usr/bin/env node
/**
 * The role process entrance (stage 5c: real process separation, docs/fix-plan-sets.md §5.8).
 *
 * This is the per-role CLI the run supervisor forks as a *separate OS process*. One invocation runs
 * exactly one role - `ingest`, `organize` or `book` - and it is handed a single serializable spec as
 * its second argument (nothing is read from the environment, and the spec is the whole configuration
 * for that child). The spec is the serializable projection of what `src/supervisor/run.mjs`'s
 * `specFor()` builds in-process; the parts that cannot cross a process boundary - the venue adapter
 * object and the websocket implementation - are resolved *here*, in the child:
 *
 *   - the adapter: the built-in adapter the venue names, built by `adapterFor` (the same function the
 *     single-process entrance `bin/receiver.mjs` uses), unless the spec names an `adapterModule`.
 *   - the websocket: Node's global `WebSocket` - the exact mechanism `bin/receiver.mjs` uses - unless
 *     the spec names a `websocketModule` (a general seam: a deployment can wrap the socket, and a
 *     test can supply a fake one without a real venue).
 *
 * The child then speaks two channels. Its *business* channel is the supervisor's router socket (the
 * same IPC the in-process roles use). Its *control* channel is the fork's built-in IPC, over which
 * the supervisor calls the handful of role operations the wiring needs (start/stop/
 * drainSpool/beginRun/... ) and reads a cached state snapshot. Business traffic never travels the
 * fork channel, and no business payload is interpreted here.
 *
 * The child exits when the supervisor closes it, when a signal arrives, or when the parent's channel
 * goes away - so a supervisor that dies does not leave three orphaned roles behind.
 */

import process from 'node:process';
import { pathToFileURL } from 'node:url';

import { openIngestProcess } from '../src/ingest/main.mjs';
import { openOrganizeProcess } from '../src/organize/main.mjs';
import { openBookProcess } from '../src/book/main.mjs';
import { adapterFor } from '../src/entry/config.mjs';
import { acquireStoreLock } from '../src/supervisor/store-lock.mjs';

/** The operations the supervisor may call on each role, exactly the ones `run.mjs` uses. */
export const ROLE_METHODS = Object.freeze({
  ingest: Object.freeze(['start', 'stop', 'drainSpool', 'receivedTails', 'close']),
  organize: Object.freeze([
    'beginRun',
    'resumeFromBoundary',
    'deliverOwed',
    'recoveryStatus',
    'stop',
    'close',
  ]),
  book: Object.freeze(['stop', 'close']),
});

/** The properties `run.mjs` reads synchronously; the child pushes a snapshot the parent caches. */
export const ROLE_PROPS = Object.freeze({
  ingest: Object.freeze(['connectionId', 'state', 'generation', 'subscriptionState', 'spool']),
  organize: Object.freeze(['serving']),
  book: Object.freeze(['appliedBoundary', 'isRunning']),
});

/** The state snapshot the parent keeps. Plain data only: it is structured-cloned across the IPC. */
function snapshotOf(role, proc) {
  if (role === 'ingest') {
    const spool = proc.spool;
    return {
      connectionId: proc.connectionId ?? null,
      state: proc.state ?? null,
      generation: proc.generation ?? null,
      subscriptionState: proc.subscriptionState ?? null,
      spool:
        spool === null || spool === undefined
          ? null
          : { isOverBound: spool.isOverBound === true, failed: spool.failed === true, bytes: spool.bytes ?? 0 },
    };
  }
  if (role === 'organize') {
    return { serving: proc.serving === true };
  }
  return { appliedBoundary: proc.appliedBoundary ?? null, isRunning: proc.isRunning === true };
}

/** Resolve the venue adapter for the ingest child: a module override, or the built-in one. */
async function resolveAdapter(spec) {
  if (spec.adapterModule) {
    const module = await import(pathToFileURL(spec.adapterModule).href);
    const factory = module.createAdapter ?? module.default;
    if (typeof factory !== 'function') {
      throw new Error('the adapter module exports no createAdapter/default function');
    }
    return factory({ market: spec.market, stream: spec.stream, ...(spec.adapterSpec ?? {}) });
  }
  const config = {
    venue: spec.venue,
    market: spec.market,
    stream: spec.stream,
    ...(spec.adapterSpec ?? {}),
  };
  return adapterFor(config);
}

/** Resolve the websocket implementation: a module override, or Node's global `WebSocket`. */
async function resolveWebSocket(spec) {
  if (spec.websocketModule) {
    const module = await import(pathToFileURL(spec.websocketModule).href);
    const Impl = module.default ?? module.WebSocket;
    if (typeof Impl !== 'function') {
      throw new Error('the websocket module exports no constructor');
    }
    return Impl;
  }
  const Impl = globalThis.WebSocket;
  if (typeof Impl !== 'function') {
    throw new Error('this node has no global WebSocket, so the role has no way to open a socket');
  }
  return Impl;
}

function forwardDiagnostic(send, role) {
  return (diagnostic) => {
    try {
      send({ kind: 'diag', role, reason: diagnostic?.reason ?? String(diagnostic) });
    } catch {
      /* a diagnostic is best-effort by contract */
    }
  };
}

async function buildRole(spec, send) {
  const { role, market, stream, runId, roleInstance, channelOptions = {} } = spec;
  const onDiagnostic = forwardDiagnostic(send, role);
  if (role === 'ingest') {
    const [adapter, webSocketImpl] = await Promise.all([resolveAdapter(spec), resolveWebSocket(spec)]);
    return openIngestProcess({
      market,
      stream,
      adapter,
      venue: spec.venue,
      takeoverFor: () => spec.takeover === true,
      runId,
      roleInstance,
      webSocketImpl,
      organizeSocketPath: spec.routerSocketPath,
      ingestStorePath: spec.storePath,
      spoolDir: spec.spoolDir,
      channelOptions,
      readinessIntervalMs: spec.readinessIntervalMs ?? 0,
      onDiagnostic,
    });
  }
  if (role === 'organize') {
    return openOrganizeProcess({
      market,
      stream,
      runId,
      roleInstance,
      routerSocketPath: spec.routerSocketPath,
      storePath: spec.storePath,
      markRunning: spec.markRunning === true,
      channelOptions,
      readinessIntervalMs: spec.readinessIntervalMs ?? 0,
      onDiagnostic,
    });
  }
  if (role === 'book') {
    return openBookProcess({
      market,
      stream,
      runId,
      roleInstance,
      organizeSocketPath: spec.routerSocketPath,
      adapter: null,
      storePath: spec.storePath,
      channelOptions,
      readinessIntervalMs: spec.readinessIntervalMs ?? 0,
      onDiagnostic,
    });
  }
  throw new Error(`no role named ${JSON.stringify(role)}`);
}

async function main(argv) {
  const role = argv[0];
  if (!['ingest', 'organize', 'book'].includes(role)) {
    process.stderr.write('usage: role.mjs <ingest|organize|book> <spec-json>\n');
    process.exit(2);
  }
  let spec;
  try {
    spec = JSON.parse(argv[1]);
  } catch (error) {
    process.stderr.write(`role: the spec is not valid JSON: ${error.message}\n`);
    process.exit(2);
  }

  const send = (msg) => {
    try {
      process.send?.(msg);
    } catch {
      /* the parent may already be gone */
    }
  };

  let proc;
  let writerLock;
  try {
    writerLock = acquireStoreLock({
      path: spec.storePath,
      role,
      instance: spec.roleInstance,
      scope: 'writer',
    });
    if (!writerLock.acquired) throw new Error(writerLock.reason);
    proc = await buildRole(spec, send);
  } catch (error) {
    send({ kind: 'fatal', role, reason: error.message });
    process.stderr.write(`role ${role}: ${error.message}\n`);
    process.exit(1);
  }

  const methods = ROLE_METHODS[role];
  const props = ROLE_PROPS[role];
  send({ kind: 'ready', role, methods, props });

  let stateTimer = setInterval(() => send({ kind: 'state', state: snapshotOf(role, proc) }), 15);
  if (typeof stateTimer.unref === 'function') stateTimer.unref();

  let closing = false;
  const shutdown = (code = 0) => {
    if (closing) return;
    closing = true;
    clearInterval(stateTimer);
    stateTimer = null;
    let storeClosed = false;
    try {
      proc.close();
      storeClosed = true;
    } catch {
      code = 1;
      /* if store close is uncertain, keep the writer lock until process.exit closes every handle */
    }
    if (storeClosed && writerLock !== null) {
      try {
        writerLock.release();
        writerLock = null;
      } catch {
        /* process.exit below is the final OS-level release */
      }
    }
    process.exit(code);
  };

  process.on('message', (msg) => {
    if (!msg || msg.kind !== 'cmd') return;
    if (msg.method === 'close') {
      // Reply first so the supervisor sees the result, then let the process leave.
      try {
        proc.close();
      } catch (error) {
        send({ kind: 'result', id: msg.id, ok: false, error: error.message });
        shutdown(1);
        return;
      }
      send({ kind: 'result', id: msg.id, ok: true, value: null });
      shutdown(0);
      return;
    }
    let ok = true;
    let value;
    let error;
    try {
      const fn = proc[msg.method];
      if (typeof fn !== 'function') throw new TypeError(`the ${role} process has no ${msg.method}`);
      value = fn.apply(proc, Array.isArray(msg.args) ? msg.args : []);
      if (value !== undefined && value !== null && typeof value.then === 'function') {
        value.then(
          (resolved) => {
            send({ kind: 'result', id: msg.id, ok: true, value: resolved ?? null });
            send({ kind: 'state', state: snapshotOf(role, proc) });
          },
          (reason) => send({ kind: 'result', id: msg.id, ok: false, error: reason?.message ?? String(reason) }),
        );
        return;
      }
    } catch (caught) {
      ok = false;
      error = caught?.message ?? String(caught);
    }
    if (ok) value = value ?? null;
    send(ok ? { kind: 'result', id: msg.id, ok: true, value } : { kind: 'result', id: msg.id, ok: false, error });
    send({ kind: 'state', state: snapshotOf(role, proc) });
  });

  process.on('SIGTERM', () => shutdown(0));
  process.on('SIGINT', () => shutdown(0));
  // A supervisor that dies must not leave this role running: the fork channel closing is the signal.
  process.on('disconnect', () => shutdown(0));
}

main(process.argv.slice(2));
