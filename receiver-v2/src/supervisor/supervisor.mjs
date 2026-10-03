/**
 * The supervisor: one run of the receiver, from the moment it starts to the report that it ended badly.
 *
 * The structure below this is the wiring of reception, organization and the board. What it cannot do for
 * itself is the part that belongs to *running*: decide that this process is a new run, carry out the
 * startup sequence in the one order a restart is safe in, and turn "reception has stopped" into a
 * non-zero exit rather than a process that sits there looking healthy.
 *
 * The startup sequence, and why the order is not a matter of taste (C2, §5.7):
 *
 *   (a) mark this run as the live one, before anything else can be mistaken for the previous run's work;
 *   (b) restore the board's recorded boundary into the organizer - which connection it follows, where that
 *       connection's numbering starts, and what the raw already holds;
 *   (c) walk the old spool oldest first, re-injecting each spilled frame through the ordinary path;
 *   (d) redeliver what the raw holds and the board does not have, which is what fixes where the old
 *       connection's numbering can start;
 *   (e) issue the connection and, in the real generation announcement, authorize and admit it - the socket
 *       is opened only once that succeeds.
 *
 * (b) is not skipped when the ledger is empty: a restart whose ledger is empty but whose spool still holds
 * frames has to organize them on the connection the board recorded, and the organizer learns that
 * connection nowhere else. (d) before (e) is the same fact from the other side - a redelivered frame fixes
 * the ceiling, and a completion heard before it could carry a start above a frame the store already holds.
 *
 * Notifications are requests, not actions. `onStop` / `onRefetch` record what is wanted and return; the
 * supervisor carries the request out only after the operation that raised it has finished and the store's
 * execution right is back, because a structure asked to stop or start from inside its own hook refuses
 * as re-entrant and the request would be lost. A notification that returns normally is not a recovery:
 * what is held is the request, and only carrying it out clears it. A failed recovery is retried a bounded
 * number of times and then becomes an abnormal end.
 *
 * The run is marked complete only when it ends cleanly. An abnormal end writes no completion, because the
 * marker exists precisely so that the next start can tell "stopped on purpose" from "ended mid-stream".
 */

import { randomUUID } from 'node:crypto';

import { createStructure } from './structure.mjs';

export const DEFAULT_MAX_RECOVERY_ATTEMPTS = 3;

export function createSupervisor(options = {}) {
  const {
    market,
    stream = 'trades',
    adapter,
    path,
    venue,
    runId = randomUUID(),
    webSocketImpl,
    rawWriter,
    spoolDir = null,
    Database = null,
    nowMs,
    issueGeneration = null,
    onStop = () => {},
    onRefetch = () => {},
    onGap = () => {},
    onAck = () => {},
    onDiagnostic = () => {},
    // Where the news that the run ended badly goes. A test hands in a spy; a process sets its own code.
    exit = (code) => {
      process.exitCode = code;
    },
    // Which step of the startup sequence is being carried out. An observation, not a capability: it is told
    // what happened and cannot change it.
    onStep = () => {},
    maxRecoveryAttempts = DEFAULT_MAX_RECOVERY_ATTEMPTS,
    ...receiveOptions
  } = options;

  if (!path) throw new TypeError('the supervisor needs a path for its store');
  if (!venue) throw new TypeError('the supervisor needs a venue to name its connections');
  // The operational entrance is the path and only the path: a caller does not hand in an opened store, so
  // no caller holds a way to write around the structure it drives. A store object here would be exactly the
  // private route the set exists to remove from the operational path.
  if ('durability' in options) {
    throw new TypeError('the supervisor opens its own store from a path; it does not accept one');
  }

  // One issuer for the whole run, handed to every connection the run builds. Two sockets of the same run,
  // venue and market must not both number themselves from 1: their connection names would then collide and
  // one run's frames would be read under the other's identity (C2).
  let generationCounter = 0;
  const generationIssuer =
    issueGeneration ?? (() => generationCounter += 1);

  // Requests, recorded where the notification arrives and carried out where the operation has ended. The
  // first request wins: the later ones are the same need, and a queue of them would try to stop something
  // already stopped or start a socket already being replaced.
  let stopRequest = null;
  let refetchRequest = null;
  let stopped = false;
  let ended = false;
  let closed = false;
  // Whether the run ended for a reason that is not a clean stop. A clean stop still writes the completion;
  // an abnormal end must not, or the next start cannot tell the crash from the shutdown.
  let abnormal = false;
  let ready = false;
  let recoveries = 0;

  const step = (name, detail) => {
    try {
      onStep({ step: name, ...detail });
    } catch {
      // An observation that throws is not a fact about the run.
    }
  };

  function recordStop(info) {
    // Recorded before the caller's hook runs: a hook that throws must not lose the news that reception has
    // stopped, and the news is what makes the process end non-zero instead of looking quiet.
    if (stopRequest === null) stopRequest = info ?? { market, reason: 'reception stopped' };
    try {
      onStop(info);
    } catch {
      // the request is held regardless
    }
  }

  function recordRefetch(info) {
    if (refetchRequest === null) refetchRequest = info ?? { market, reason: 'a re-anchor was requested' };
    try {
      onRefetch(info);
    } catch {
      // the request is held regardless
    }
  }

  const structure = createStructure({
    market,
    stream,
    adapter,
    path,
    venue,
    runId,
    webSocketImpl,
    rawWriter,
    spoolDir,
    Database,
    nowMs,
    issueGeneration: generationIssuer,
    // The supervisor drives the startup sequence itself; the structure must not restore the board before the
    // run has been marked live.
    deferRecovery: true,
    onStop: (info) => recordStop(info),
    onRefetch: (info) => recordRefetch(info),
    onGap,
    onAck,
    onDiagnostic: (diagnostic) => {
      try {
        onDiagnostic(diagnostic);
      } catch {
        // diagnostics are best-effort by contract
      }
    },
    ...receiveOptions,
  });

  /**
   * Carry out what the notifications asked for, now that the operation that raised them has finished and
   * the store's execution right is back. It loops because a recovery can itself raise a request (a new
   * connection the book refuses), and that request is owed the same answer.
   *
   * A request raised from inside a hook finds the right still held, and the structure refuses to stop or
   * start for it. That is not a lost request: the refusal leaves it recorded, and the operation that owns
   * the right calls this again on its way out. Nothing is carried out in the middle of a frame, and nothing
   * asked for is dropped because it asked too early.
   */
  function settle() {
    for (;;) {
      if (ended) return;
      if (stopRequest !== null) {
        if (!carryStop()) return; // an operation still holds the right: its own exit will carry it out
        return;
      }
      if (refetchRequest !== null) {
        if (!carryRefetch()) return;
        continue;
      }
      return;
    }
  }

  /** The re-entrancy refusal a structure window answers with when another operation holds the right. */
  const refused = (result) => result !== null && typeof result === 'object' && result.code === 'REENTRANT_OPERATION';

  function carryStop() {
    const stoppedIt = stopStructure();
    if (refused(stoppedIt)) return false;
    stopRequest = null;
    refetchRequest = null;
    ended = true;
    stopped = true;
    ready = false;
    abnormal = true;
    exit(1);
    return true;
  }

  function stopStructure() {
    try {
      return structure.stop();
    } catch {
      // A stop that throws is still a stop: the flag is set, and the verdict does not change.
      return undefined;
    }
  }

  /**
   * Answer a re-anchor request by replacing the socket: drop the connection that broke, then start a new
   * one, which issues a new generation and asks the venue for a fresh snapshot. The old socket's timers and
   * the old connection's snapshot die with the connection it belonged to. Bounded: a venue that never
   * recovers ends the run abnormally rather than spinning.
   */
  function carryRefetch() {
    const info = refetchRequest;
    if (recoveries >= maxRecoveryAttempts) {
      recordStop({ market, reason: `the board could not be re-anchored after ${recoveries} attempts` });
      return true; // the request is now a stop, carried out by the loop
    }
    // The old connection is abandoned first: its silence and stability timers, and any event it queued,
    // are checked against a socket that is already gone, so nothing of it can re-anchor the board.
    const stoppedIt = structure.stop();
    if (refused(stoppedIt)) return false;
    refetchRequest = null;
    recoveries += 1;
    ready = false;
    step('recover', { attempt: recoveries, connectionId: info?.connectionId ?? null });
    try {
      const announced = structure.start();
      if (refused(announced)) return true; // the right came back; the start is owed another attempt next time
      // The connection reports success by returning nothing: it is admitted unless a stop was raised in its
      // announcement.
      if (stopRequest === null && !structure.stats.stopped) ready = true;
    } catch (error) {
      recordStop({ market, reason: `the re-anchor failed: ${error.message}` });
    }
    return true;
  }

  function runOperation(operation) {
    try {
      return operation();
    } finally {
      settle();
    }
  }

  /**
   * The startup sequence. Every step that can fail stops the run and exits non-zero; the drain retries only
   * a transient raw refusal, and stops on a corrupt spool, a failed save or a refused admission.
   */
  function start() {
    return runOperation(() => {
      if (ended) return { started: false, reason: 'this supervisor has ended' };
      step('beginRun', {});
      const begun = structure.beginRun();
      if (begun?.begun === false) {
        recordStop({ market, reason: begun.reason ?? 'the run could not be marked live' });
        return { started: false, reason: 'the run could not be marked live' };
      }
      step('restore', {});
      structure.restore();
      step('drain', {});
      drainWithRetry();
      if (ended || structure.stats.stopped) {
        return { started: false, reason: 'recovery stopped the structure' };
      }
      // (d) and (e): start() redelivers what the raw holds and the board does not have, then announces the
      // connection; the socket opens only if the book admits it in the real onGeneration.
      step('start', {});
      const announced = structure.start();
      if (announced && announced.started === false) return announced;
      // The connection reports its own start by returning nothing; whether it was admitted is visible in the
      // stop a refused connection raises, and in the structure having stopped.
      if (stopRequest !== null || structure.stats.stopped) {
        return { started: false, reason: 'the connection was not admitted' };
      }
      ready = true;
      return { started: true, connectionId: structure.connection.connectionId };
    });
  }

  function drainWithRetry() {
    let attempts = 0;
    for (;;) {
      const drained = structure.drainSpool();
      if (!drained?.stopped) return drained;
      // Only a raw that refused a record is the transient case: the spool may be handed back once the raw
      // can take it. A spool that cannot be read, a failed save or a refused admission is not something a
      // retry improves, and those stop the run.
      const transient = /raw still refused/.test(String(drained.stopped));
      if (transient && attempts < maxRecoveryAttempts) {
        attempts += 1;
        continue;
      }
      recordStop({ market, reason: `the old spool could not be drained: ${drained.stopped}` });
      return drained;
    }
  }

  const api = {
    market,
    start,
    feed: (envelope) => runOperation(() => structure.feed(envelope)),
    /**
     * A clean stop: reception ends, and the completion is written. A stop that came from a failure goes
     * through `settle` and is abnormal, so this is the way a caller ends a run that finished its work.
     */
    stop: () =>
      runOperation(() => {
        if (closed) return { stopped: false, reason: 'this supervisor is closed' };
        stopped = true;
        ready = false;
        if (!ended) {
          ended = true;
          stopStructure();
        }
        return { stopped: true };
      }),
    close: () => {
      if (closed) return;
      closed = true;
      stopStructure();
      // The marker is written only on a clean end: an abnormal end leaves no completion, so the next start
      // can tell that this one did not stop on purpose.
      if (!abnormal) structure.completeRun();
      structure.close();
    },
    /** A caller - or a notification - that wants reception stopped without reaching into the structure. */
    requestStop: (reason) => {
      recordStop({ market, reason: reason ?? 'a stop was requested' });
      settle();
    },
    /** A caller - or a notification - that wants a re-anchor; carried out once the operation has ended. */
    requestRefetch: (info) => {
      recordRefetch(info);
      settle();
    },
    get ready() {
      return ready;
    },
    get stopped() {
      return stopped;
    },
    get ended() {
      return ended;
    },
    get abnormal() {
      return abnormal;
    },
    get pending() {
      return { stop: stopRequest, refetch: refetchRequest };
    },
    get stats() {
      return structure.stats;
    },
    get appliedBoundary() {
      return structure.book.appliedBoundary;
    },
    get book() {
      return structure.book;
    },
    get ledger() {
      return structure.ledger;
    },
    get spool() {
      return structure.spool;
    },
    get organizer() {
      return structure.organizer;
    },
    get connection() {
      return structure.connection;
    },
  };

  return api;
}
