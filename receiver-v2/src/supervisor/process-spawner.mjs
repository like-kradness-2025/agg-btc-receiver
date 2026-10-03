/**
 * The real process spawner (stage 5c).
 *
 * `src/supervisor/run.mjs` starts each role through an injected `spawner(role, spec)`. The default one
 * runs the role *in this process*; this module is the other one - it forks `bin/role.mjs` as a
 * separate OS process per role and returns a remote handle whose shape is the one the wiring above
 * already uses (`start`, `stop`, `sealTails`, `drainSpool`, `beginRun`, ... plus the handful of
 * properties read synchronously).
 *
 * Two channels exist, deliberately apart:
 *   - the business channel is the supervisor's router socket, which the child opens itself (the same
 *     IPC the in-process roles speak). No business payload crosses this module.
 *   - the control channel is the fork's built-in IPC. Method calls are request/response over it, and
 *     the child pushes a small state snapshot on an interval so the wiring's synchronous property
 *     reads (`connectionId`, `state`, `generation`, `allAcked`, `appliedBoundary`, `spool`) have a
 *     value to read.
 *
 * The abnormal-exit path is the point of a real process: when a child dies, its `exit` gives the code
 * and signal, and the registered `onExit` is called with them - which is what lets the supervisor's
 * condition-by-condition failure policy (ruling ⑬) act on a real death rather than a simulated one.
 * An intentional `close()` marks the child as closing, so a clean stop never reads as a failure.
 */

import { fork } from 'node:child_process';
import { fileURLToPath } from 'node:url';

const DEFAULT_ROLE_MODULE = fileURLToPath(new URL('../../bin/role.mjs', import.meta.url));
const EXIT_GRACE_MS = 4_000;

/** Map one in-process spec (as `run.mjs`'s `specFor` builds it) to the serializable child spec. */
export function toChildSpec(
  role,
  spec,
  { adapterSpec = null, adapterModule = null, websocketModule = null, readinessIntervalMs = 0 } = {},
) {
  const options = spec.options ?? {};
  const common = {
    role,
    market: options.market,
    stream: options.stream,
    runId: options.runId,
    roleInstance: spec.instance,
    channelOptions: options.channelOptions ?? {},
    readinessIntervalMs,
  };
  if (role === 'ingest') {
    return {
      ...common,
      venue: options.venue,
      routerSocketPath: options.organizeSocketPath,
      storePath: options.ingestStorePath,
      spoolDir: options.spoolDir,
      websocketModule,
      adapterModule,
      adapterSpec,
    };
  }
  if (role === 'organize') {
    return {
      ...common,
      routerSocketPath: options.routerSocketPath,
      storePath: options.storePath,
      markRunning: options.markRunning === true,
    };
  }
  return {
    ...common,
    routerSocketPath: options.organizeSocketPath,
    storePath: options.storePath,
  };
}

/**
 * Build a spawner that forks each role as its own OS process. The returned function has the same
 * signature the supervisor's default spawner has.
 *
 * `adapterSpec` carries what the child needs to build the built-in venue adapter (a `symbol` and an
 * optional `url`); `websocketModule` names a module exporting a WebSocket constructor, for a caller
 * that wants something other than Node's global `WebSocket`.
 */
export function createForkSpawner({
  roleModule = DEFAULT_ROLE_MODULE,
  adapterSpec = null,
  adapterModule = null,
  websocketModule = null,
  readinessIntervalMs = 1_000,
  execArgv = [],
  env = process.env,
  onDiagnostic = () => {},
} = {}) {
  return async function forkSpawner(role, spec) {
    const childSpec = toChildSpec(role, spec, { adapterSpec, adapterModule, websocketModule, readinessIntervalMs });
    const child = fork(roleModule, [role, JSON.stringify(childSpec)], {
      stdio: ['ignore', 'pipe', 'pipe', 'ipc'],
      env,
      execArgv,
    });

    let exited = false;
    let exitInfo = null;
    let closing = false;
    let manifest = null;
    let exitCallback = null;
    let fatal = null;
    const state = {};
    const pending = new Map();
    let nextId = 1;
    let stderrTail = '';

    child.stderr?.on('data', (chunk) => {
      const text = chunk.toString('utf8');
      stderrTail = (stderrTail + text).slice(-2_000);
    });

    let settleReady;
    let rejectReady;
    const ready = new Promise((resolve, reject) => {
      settleReady = resolve;
      rejectReady = reject;
    });

    function rpc(method, args) {
      if (exited) return Promise.reject(new Error(`the ${role} process has exited`));
      const id = nextId++;
      return new Promise((resolve, reject) => {
        pending.set(id, { resolve, reject });
        try {
          child.send({ kind: 'cmd', id, method, args });
        } catch (error) {
          pending.delete(id);
          reject(error);
        }
      });
    }

    function waitForExit(timeoutMs = EXIT_GRACE_MS) {
      if (exited) return Promise.resolve(exitInfo);
      return new Promise((resolve) => {
        const timer = setTimeout(() => {
          try {
            child.kill('SIGKILL');
          } catch {
            /* already gone */
          }
          resolve(exitInfo ?? { code: null, signal: 'SIGKILL' });
        }, timeoutMs);
        if (typeof timer.unref === 'function') timer.unref();
        child.once('exit', () => {
          clearTimeout(timer);
          resolve(exitInfo);
        });
      });
    }

    const proxy = {
      onExit(callback) {
        exitCallback = callback;
        return proxy;
      },
      get pid() {
        return child.pid;
      },
      get exitInfo() {
        return exitInfo;
      },
      async close() {
        if (exited) return;
        closing = true;
        try {
          await rpc('close', []);
        } catch {
          /* the child may already be gone */
        }
        await waitForExit();
      },
    };

    child.on('message', (message) => {
      if (message?.kind === 'ready') {
        manifest = message;
        for (const method of message.methods ?? []) {
          proxy[method] = (...args) => rpc(method, args);
        }
        for (const property of message.props ?? []) {
          Object.defineProperty(proxy, property, {
            get: () => state[property],
            enumerable: true,
            configurable: true,
          });
        }
        settleReady(proxy);
      } else if (message?.kind === 'state') {
        Object.assign(state, message.state ?? {});
      } else if (message?.kind === 'result') {
        const slot = pending.get(message.id);
        if (slot) {
          pending.delete(message.id);
          if (message.ok) slot.resolve(message.value);
          else slot.reject(new Error(message.error));
        }
      } else if (message?.kind === 'diag') {
        try {
          onDiagnostic(message);
        } catch {
          /* a diagnostic is best-effort */
        }
      } else if (message?.kind === 'fatal') {
        fatal = new Error(`${role} failed to start: ${message.reason}`);
        rejectReady(fatal);
      }
    });

    child.on('exit', (code, signal) => {
      exited = true;
      exitInfo = { code, signal };
      for (const slot of pending.values()) slot.reject(new Error(`the ${role} process exited (${code}/${signal})`));
      pending.clear();
      if (fatal === null && manifest === null) {
        fatal = new Error(`${role} exited before it was ready (${code}/${signal})${stderrTail ? `: ${stderrTail.trim()}` : ''}`);
        rejectReady(fatal);
      }
      if (!closing && exitCallback) {
        try {
          exitCallback({ code, signal, reason: `the ${role} process exited (code ${code}, signal ${signal ?? 'none'})` });
        } catch {
          /* an observation that throws is not a fact about the death */
        }
      }
    });

    await ready;
    return proxy;
  };
}
