#!/usr/bin/env node
/**
 * The receiver's process entrance.
 *
 * This is the one place that turns "a run" into "a process": it reads the config file, builds the three
 * things the supervisor cannot build for itself (the venue adapter, the raw writer, and the socket
 * implementation), drives the startup sequence, and gives the process the two answers §5.7 asks a real
 * process to give:
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
 * goes.
 */

import process from 'node:process';

import { loadConfig, adapterFor } from '../src/entry/config.mjs';
import { createFileRawWriter } from '../src/raw/file-writer.mjs';
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
  if (supervisor !== null) {
    // `stop` is the clean stop: it does not mark the run abnormal, so `close` writes the completion.
    try {
      supervisor.stop();
    } catch {
      // A stop that throws is still the stop this process is making; the close below still ends it.
    }
    try {
      supervisor.close();
    } catch {
      // The store may already be gone; the process is leaving either way.
    }
  }
  releaseHold();
  process.exit(0);
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
  let rawWriter;
  let WebSocketImpl;
  try {
    config = loadConfig(configPath);
    adapter = adapterFor(config);
    rawWriter = createFileRawWriter({ path: config.raw });
    WebSocketImpl = globalThis.WebSocket;
    if (typeof WebSocketImpl !== 'function') {
      throw new Error('this node has no global WebSocket, so the receiver has no way to open a socket');
    }
  } catch (error) {
    process.stderr.write(`receiver: ${error.message}\n`);
    process.exit(1);
  }

  try {
    supervisor = createSupervisor({
      market: config.market,
      stream: config.stream,
      adapter,
      path: config.database,
      venue: config.venue,
      webSocketImpl: WebSocketImpl,
      rawWriter,
      spoolDir: config.spoolDir,
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
    });
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
  // The readiness line: a run that reached reception. It is how an operator - or a test - knows the
  // process is past its startup sequence and a signal now means an orderly stop.
  process.stdout.write(`receiver: started ${supervisor.connection?.connectionId ?? ''}\n`);
}

main(process.argv.slice(2));
