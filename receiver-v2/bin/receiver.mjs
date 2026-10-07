#!/usr/bin/env node
/**
 * The receiver's process entrance (stage 5d, docs/fix-plan-sets.md §5.8 ruling ⑭).
 *
 * This is the one place that turns "a run" into "processes". Since the receiver is three role
 * processes behind a supervisor (C11, §5.8), the entrance no longer builds a structure itself: it
 * reads the config file, builds the supervisor's *fork* spawner - the one that starts `bin/role.mjs`
 * as a separate OS process per role - and drives the run supervisor (`src/supervisor/run.mjs`), which
 * owns the router, the startup sequence and the two stop orders. There is no operating switch: this
 * is the entrance, and the single-process wiring (`supervisor.mjs` / `structure.mjs`) stays behind as
 * the seam the tests drive, not as an alternative deployment.
 *
 * SIGINT/SIGTERM stop reception once, then stop downstream roles and close the children/stores.
 * Exit 0 means shutdown succeeded; it does not claim the venue's final state is complete. Pending
 * durable work and the running marker remain for the existing startup invalidation/recovery path.
 * Data-processing failures and unconfirmed stops/termination end non-zero.
 *
 * The socket implementation is Node's global `WebSocket` (present since Node 22, which this package
 * requires); it is checked here before any child is forked, and each role process resolves its own
 * copy the same way. There is no config key for it: a process started without a socket must say so
 * rather than quietly running with no way to receive.
 *
 * The only argument is `--config <path-to-json>`. Nothing is read from the environment and no
 * destination may be overridden on the command line: the file is the one source of truth for where the
 * canonical data goes. There is no raw writer here: the entrance builds no raw destination, and a
 * config that carries a `"raw"` key is refused rather than ignored (see `../src/entry/config.mjs`).
 * Success of a run therefore never means "a raw file holds the data".
 *
 * `receiver: started` (process start: the startup sequence completed and reception was admitted) and
 * `receiver: established` (delivery: the venue acknowledged this connection's subscription) are kept
 * apart, exactly as A-2 requires. Diagnostics and every stop reason go to stderr, so a process that
 * ends non-zero is not silent about why.
 */

import process from 'node:process';
import { dirname, join } from 'node:path';

import { loadConfig, adapterFor } from '../src/entry/config.mjs';
import { createRunSupervisor } from '../src/supervisor/run.mjs';
import { createForkSpawner } from '../src/supervisor/process-spawner.mjs';

function configPathFrom(argv) {
  if (argv.length !== 2 || argv[0] !== '--config') return null;
  const value = argv[1];
  if (value === undefined || value.length === 0 || value.startsWith('--')) return null;
  return value;
}

let supervisor = null;
let shuttingDown = false;
// A handle that keeps the event loop alive while the run is live. Reception's timers are unref'd by
// design, and the forks' IPC channels alone are not a guarantee that the loop stays open, so without
// this a run whose socket is between reconnects could exit 0 *silently* while reception had been
// admitted and the run had not been asked to stop - neither the clean stop nor the failure the
// supervisor reports. The run is held open until a signal arrives or the supervisor ends it.
let keepAlive = null;
// The interval that watches the admission's subscription state and reports establishment once per
// transition, so `receiver: established` is a fact about delivery and not about process start.
let establishedTimer = null;
let lastSubscriptionState = null;

function holdOpen() {
  if (keepAlive === null) keepAlive = setInterval(() => {}, 0x7fffffff);
}

function releaseHold() {
  if (keepAlive !== null) {
    clearInterval(keepAlive);
    keepAlive = null;
  }
  if (establishedTimer !== null) {
    clearInterval(establishedTimer);
    establishedTimer = null;
  }
}

/**
 * Report establishment when the admitted connection's subscription becomes acknowledged. It reads only
 * the cached snapshot the fork spawner already publishes for `run.mjs` (`subscriptionState` is one of
 * the ingest role's declared properties), so it is an observation of the run, not a new capability.
 */
function watchEstablishment() {
  establishedTimer = setInterval(() => {
    if (shuttingDown || supervisor === null) return;
    const ingest = supervisor.children.ingest?.process;
    if (!ingest) return;
    const state = ingest.subscriptionState ?? null;
    if (state === lastSubscriptionState) return;
    lastSubscriptionState = state;
    if (state === 'acknowledged') {
      process.stdout.write(`receiver: established ${ingest.connectionId ?? ''}\n`);
    }
  }, 100);
  if (typeof establishedTimer.unref === 'function') establishedTimer.unref();
}

/** A signal owns shutdown once. Success is local stop and close, without a complete marker. */
async function shutdown() {
  if (shuttingDown) return;
  shuttingDown = true;
  let failed = false;
  if (supervisor !== null && supervisor.started === true) {
    try {
      const result = await supervisor.stop();
      if (result?.stopped !== true || result?.abnormal === true) failed = true;
    } catch (error) {
      process.stderr.write(`receiver: stop failed: ${error.message}\n`);
      failed = true;
    }
  } else {
    // Reception was never admitted: this is not a clean end, whatever arrives next.
    failed = true;
  }
  try {
    const closed = await supervisor?.close();
    if (closed?.shutdownSucceeded !== true) failed = true;
  } catch (error) {
    process.stderr.write(`receiver: close failed: ${error.message}\n`);
    failed = true;
  }
  releaseHold();
  process.exit(failed ? 1 : 0);
}

/**
 * A failed run ends without a completion. `terminate` closes every child and releases its store
 * without running the processing stop. The marker remains available to startup invalidation/recovery.
 */
async function fail(reason) {
  releaseHold();
  try {
    await supervisor?.terminate();
  } catch {
    // the children may already be gone
  }
  if (reason) process.stderr.write(`receiver: ${reason}\n`);
  process.exit(1);
}

async function main(argv) {
  const configPath = configPathFrom(argv);
  if (configPath === null) {
    process.stderr.write('usage: receiver.mjs --config <path-to-json>\n');
    process.exit(1);
  }

  // The signals are taken before anything is built, so a signal that arrives during a slow open is an
  // orderly shutdown rather than the shell's default action.
  process.on('SIGINT', () => void shutdown());
  process.on('SIGTERM', () => void shutdown());

  let config;
  let adapter;
  let WebSocketImpl;
  try {
    config = loadConfig(configPath);
    // Pre-flight: the venue's adapter must be buildable (this refuses the disabled bitfinex adapter
    // and a stream the adapter cannot carry) and this node must have a way to open a socket - before
    // any child is forked or any store is opened.
    adapter = adapterFor(config);
    void adapter;
    WebSocketImpl = globalThis.WebSocket;
    if (typeof WebSocketImpl !== 'function') {
      throw new Error('this node has no global WebSocket, so the receiver has no way to open a socket');
    }
  } catch (error) {
    process.stderr.write(`receiver: ${error.message}\n`);
    process.exit(1);
  }

  // The router's socket: named by the config, or derived next to the spool when the deployment did not
  // choose one. The socket must live somewhere the deployment owns, and the spool's directory is that
  // place; a second source of truth (an environment variable) is what this entrance exists to avoid.
  const routerPath = config.routerPath ?? join(dirname(config.spoolDir), 'router.sock');

  try {
    supervisor = createRunSupervisor({
      market: config.market,
      stream: config.stream,
      venue: config.venue,
      ingestStorePath: config.stores.ingest,
      organizeStorePath: config.stores.organize,
      bookStorePath: config.stores.book,
      spoolDir: config.spoolDir,
      routerListenPath: routerPath,
      // The startup deadline is carried only when the config named one: absent means the supervisor's own
      // default applies, so the entrance does not pass a value the deployment never chose.
      ...(config.startupDeadlineMs === undefined ? {} : { startupDeadlineMs: config.startupDeadlineMs }),
      // The real process separation (stage 5c): each role runs in its own OS process, forked from
      // `bin/role.mjs`. The venue adapter and the socket implementation are resolved *inside* the child
      // (the built-in adapter the venue names, and Node's global WebSocket) unless a module override is
      // given - which this entrance never gives: the production path uses only the defaults.
      spawner: createForkSpawner({
        onDiagnostic: (diagnostic) => {
          process.stderr.write(`receiver: ${diagnostic.role}: ${diagnostic.reason}\n`);
        },
        adapterSpec: {
          ...(config.url === undefined ? {} : { url: config.url }),
          ...(config.restUrl === undefined ? {} : { restUrl: config.restUrl }),
          ...(config.symbol === undefined ? {} : { symbol: config.symbol }),
        },
      }),
      // A-2: the process's own reporting. A failure that ends the run has to leave its reason behind
      // *before* the exit, which is what these lines are for: a stop (a refused subscription, a
      // deadline, a connection the board did not admit) writes its reason to stderr, and so does every
      // diagnostic reception, organization and the board raise.
      onDiagnostic: (diagnostic) => {
        process.stderr.write(`receiver: diagnostic${diagnostic?.reason ? `: ${diagnostic.reason}` : ''}\n`);
      },
      // A child that died for a reason the supervisor did not ask for is reported with its code and
      // signal - the news that a condition-by-condition restart (or a fatal end) is being decided.
      onChildExit: (info) => {
        process.stderr.write(`receiver: ${info?.role ?? 'a role'} exited${info?.reason ? `: ${info.reason}` : ''}\n`);
      },
      // The supervisor chooses the code; a process turns it into its own exit code. This is the fatal
      // path (a child failure with no continue rule, an exhausted restart budget): the run is over, so
      // hold nothing open and leave with the code. The forked children see the IPC channel close and
      // shut themselves down.
      exit: (code) => {
        process.exitCode = code;
        if (code !== 0) {
          releaseHold();
          process.exit(code);
        }
      },
    });
  } catch (error) {
    process.stderr.write(`receiver: ${error.message}\n`);
    process.exit(1);
  }

  let started;
  try {
    started = await supervisor.start();
  } catch (error) {
    await fail(error.message);
    return;
  }
  if (started?.started !== true) {
    // A run that did not start is a failed run. `terminate` ends the children that did come up and
    // writes no completion, which is exactly "the run never reached reception".
    await fail(`the run could not start${started?.reason ? `: ${started.reason}` : ''}`);
    return;
  }

  // Reception was admitted: hold the process open until a signal or a failure ends the run.
  holdOpen();
  watchEstablishment();
  // A-2: the process-start line. It says the startup sequence completed and reception was admitted - a
  // socket is open and the board took the connection - which is what an operator (or a test) needs to
  // know before a signal means "an orderly stop". It is deliberately NOT the delivery-established line:
  // whether the stream was established is a separate fact, reported by `receiver: established` when the
  // venue acknowledges this connection's subscription (and a failed subscription ends the run through
  // the supervisor's stop path above).
  process.stdout.write(`receiver: started ${started.connectionId ?? ''}\n`);
}

void main(process.argv.slice(2));
