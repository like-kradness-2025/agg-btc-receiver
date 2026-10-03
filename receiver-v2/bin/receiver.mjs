#!/usr/bin/env node
/**
 * The receiver's process entrance.
 *
 * This is the one place that turns "a run" into "a process": it reads the config file, builds the two
 * things the supervisor cannot build for itself (the venue adapter and the socket implementation), drives
 * the startup sequence, and gives the process the two answers §5.7 asks a real process to give:
 *
 *   - a clean end is exit 0 with the completion written: SIGINT and SIGTERM stop reception once, close
 *     the structure, write the run's completion, and leave. A second signal does not stop it again - the
 *     first signal already owns the shutdown.
 *   - a failure is exit 1 with no completion: a config that cannot be read, an adapter that cannot be
 *     built, a store that cannot be opened, or a startup step the supervisor refuses all end the process
 *     non-zero, and because the end is abnormal the completion marker is not written - which is exactly
 *     what lets the next start tell "stopped on purpose" from "ended mid-stream".
 *
 * The socket implementation is Node's global `WebSocket` (present since Node 22, which this package
 * requires). There is no config key for it: a process started without a socket must say so rather than
 * quietly running with no way to receive.
 *
 * The only argument is `--config <path-to-json>`. Nothing is read from the environment and no destination
 * may be overridden on the command line: the file is the one source of truth for where the canonical data
 * goes. There is no raw writer here: the entrance builds no raw destination, and a config that carries a
 * `"raw"` key is refused rather than ignored (see `../src/entry/config.mjs`). Success of a run therefore
 * never means "a raw file holds the data".
 */

import process from 'node:process';

import { loadConfig, adapterFor } from '../src/entry/config.mjs';
import { createSupervisor } from '../src/supervisor/supervisor.mjs';

function configPathFrom(argv) {
  const index = argv.indexOf('--config');
  if (index === -1) return null;
  const value = argv[index + 1];
  if (value === undefined || value.startsWith('--')) return null;
  return value;
}

let supervisor = null;
let shuttingDown = false;
// A handle that keeps the event loop alive while the run is live. Reception's timers are unref'd by
// design - so that a stopped run can let the process go - which means a run whose socket is between
// reconnects has nothing holding the loop open. Without this, the process would exit 0 *silently*
// while reception had been admitted and the run had not been asked to stop: neither the clean stop
// (exit 0 with a completion) nor the failure the supervisor reports. The run is held open until a
// signal arrives or the supervisor ends it.
let keepAlive = null;

function holdOpen() {
  if (keepAlive === null) keepAlive = setInterval(() => {}, 0x7fffffff);
}

function releaseHold() {
  if (keepAlive !== null) {
    clearInterval(keepAlive);
    keepAlive = null;
  }
}

/**
 * A signal, once. Reception ends, the structure is closed and the completion is written by the clean
 * close, and the process leaves with zero. A second signal finds `shuttingDown` set and does nothing:
 * the shutdown the first one started is the only one this process performs.
 */
function shutdown() {
  if (shuttingDown) return;
  shuttingDown = true;
  // Whether the clean end actually happened. A stop or a close that failed - or a close that was refused
  // the completion rather than writing it - is not a clean end, and the process must not report success
  // (exit 0) for a run whose completion is not on disk. The next start would then read `running` and
  // have no way to tell this stop from a crash. So the failure is carried to the exit code instead of
  // being swallowed here.
  let failed = false;
  if (supervisor !== null) {
    // `stop` is the clean stop: it does not mark the run abnormal, so `close` writes the completion.
    // What `stop` does *not* do is throw for a re-entrant call: a stop that arrives while another
    // operation holds the store's execution right is answered with a refusal value, and an exception
    // from the structure's own stop is already absorbed by the supervisor's `stopStructure()`. So the
    // catch below is a guard, not a routine path - but an unexpected throw is still a stop that did not
    // cleanly happen, and it must not be reported as success.
    try {
      supervisor.stop();
    } catch {
      failed = true;
    }
    try {
      const outcome = supervisor.close();
      // `close` reports its end as a value, not only by throwing: a refusal object (`completed: false`)
      // means the completion was not written, which is exactly as much a failure as a throw.
      if (outcome !== null && typeof outcome === 'object' && outcome.completed === false) failed = true;
    } catch {
      failed = true;
    }
  }
  releaseHold();
  process.exit(failed ? 1 : 0);
}

function main(argv) {
  const configPath = configPathFrom(argv);
  if (configPath === null) {
    process.stderr.write('usage: receiver.mjs --config <path-to-json>\n');
    process.exit(1);
  }

  // The signals are taken before anything is built, so a signal that arrives during a slow open is an
  // orderly shutdown rather than the shell's default action.
  process.on('SIGINT', shutdown);
  process.on('SIGTERM', shutdown);

  let config;
  let adapter;
  let WebSocketImpl;
  try {
    config = loadConfig(configPath);
    adapter = adapterFor(config);
    WebSocketImpl = globalThis.WebSocket;
    if (typeof WebSocketImpl !== 'function') {
      throw new Error('this node has no global WebSocket, so the receiver has no way to open a socket');
    }
  } catch (error) {
    process.stderr.write(`receiver: ${error.message}\n`);
    process.exit(1);
  }

  try {
    // The startup deadline is carried only when the config named one: absent means the supervisor's own
    // default applies, so the entrance does not pass a value the deployment never chose.
    const supervisorOptions = {
      market: config.market,
      stream: config.stream,
      adapter,
      path: config.database,
      venue: config.venue,
      webSocketImpl: WebSocketImpl,
      spoolDir: config.spoolDir,
      ...(config.startupDeadlineMs === undefined ? {} : { startupDeadlineMs: config.startupDeadlineMs }),
      // A-2: the process's own reporting. A failure that ends the run has to leave its reason behind
      // *before* the exit, which is what these two lines are for: a stop (a refused subscription, a
      // deadline, a raw that refused) writes its reason to stderr, and so does every diagnostic reception
      // and the board raise. Nothing here is a capability - they are observations the supervisor already
      // makes; the point is that a process that ends non-zero is not silent about why.
      onStop: (stop) => {
        process.stderr.write(`receiver: stopping${stop?.reason ? `: ${stop.reason}` : ''}\n`);
      },
      onDiagnostic: (diagnostic) => {
        process.stderr.write(`receiver: diagnostic${diagnostic?.reason ? `: ${diagnostic.reason}` : ''}\n`);
      },
      // A-2: establishment is its own display, kept apart from `receiver: started`. `started` says the
      // process reached reception (a socket is open and admitted); it says nothing about the stream being
      // established. A venue whose subscription is acknowledged is what this line reports - and a venue
      // whose subscription failed ends the run through `onStop` above rather than hiding behind `started`.
      onSubscriptions: (info) => {
        if (info?.state === 'acknowledged') {
          process.stdout.write(`receiver: established ${info.connectionId ?? ''}\n`);
        }
      },
      // The supervisor chooses the code; a process turns it into its own exit code. A non-zero code is
      // the end of the run: reception is already stopped, so close (which writes no completion for an
      // abnormal end) and leave with the code the supervisor chose.
      exit: (code) => {
        process.exitCode = code;
        if (code !== 0) {
          if (supervisor !== null && !shuttingDown) {
            shuttingDown = true;
            try {
              supervisor.close();
            } catch {
              // The store may already be gone.
            }
          }
          releaseHold();
          process.exit(code);
        }
      },
    };
    supervisor = createSupervisor(supervisorOptions);
  } catch (error) {
    process.stderr.write(`receiver: ${error.message}\n`);
    process.exit(1);
  }

  const started = supervisor.start();
  if (started.started !== true) {
    // A run that did not start is a failed run. The supervisor's own exit route reported a non-zero
    // code; if for any reason it did not, one is still the honest answer for "reception was not reached".
    try {
      supervisor.close();
    } catch {
      // The store may already be gone.
    }
    releaseHold();
    process.exit(process.exitCode && process.exitCode !== 0 ? process.exitCode : 1);
  }
  // Reception was admitted: hold the process open until a signal or a failure ends the run.
  holdOpen();
  // A-2: the process-start line. It says the startup sequence completed and reception was admitted - a
  // socket is open and the book took the connection - which is what an operator (or a test) needs to know
  // before a signal means "an orderly stop". It is deliberately NOT the delivery-established line: whether
  // the stream was established is a separate fact, reported by `receiver: established` when the venue
  // acknowledges the subscription (and a failed one ends the run through `receiver: stopping` above).
  process.stdout.write(`receiver: started ${supervisor.connection?.connectionId ?? ''}\n`);
}

main(process.argv.slice(2));
