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

/**
 * How long a run may spend trying to reach reception before it gives up. §5.7 wants (e)'s connection
 * retries to follow the existing backoff *and* a startup deadline: a venue that never lets a socket in
 * is not a run that should reconnect for ever, quietly looking alive. Past the deadline the run ends
 * non-zero, which is the only honest answer left.
 */
export const DEFAULT_STARTUP_DEADLINE_MS = 60_000;

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
    startupDeadlineMs = DEFAULT_STARTUP_DEADLINE_MS,
    ...receiveOptions
  } = options;

  // The startup deadline runs on the same injected timers the connection uses, so a test can drive it.
  // They stay in `receiveOptions` and reach the connection too; only their names are borrowed here, with
  // the same defaults the connection would apply.
  //
  // The deadline's *elapsed* is measured on a monotonic clock, not the wall clock - the same treatment the
  // envelopes already give recv_mono_ns. A wall clock can be corrected backwards; a deadline measured
  // against it would then read a negative elapsed, judge the deadline unmet and - the timer already spent -
  // drop the monitoring for good, leaving a run with a venue that never lets a socket in looking alive for
  // ever. The monotonic clock cannot go backwards, so the elapsed is always honest. `wallClockMs` is left
  // in `receiveOptions` (the connection still stamps its records with it) and is deliberately not used
  // here.
  const {
    setTimer = setTimeout,
    clearTimer = clearTimeout,
    monotonicMs = () => Number(process.hrtime.bigint()) / 1e6,
  } = receiveOptions;

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
  // Whether reception has been admitted and its socket opened. This is *not* readiness: a socket that
  // is open has proved nothing about the board. Readiness (the `ready` getter below) is derived from
  // this plus the board actually serving on the connection that is current - see `serving()`. Keeping
  // the two apart is what stops the supervisor reporting a board nobody may read as ready.
  let started = false;
  let recoveries = 0;
  // The startup deadline timer, armed when the sequence begins and never re-armed; null once fired or
  // disarmed. `hasServed` records that the board has actually served at some point, which is what tells
  // the deadline the run really did reach reception - a later re-anchor is a different question, with
  // its own bounded recovery, and must not be mistaken for a startup that never happened.
  let startupDeadlineTimer = null;
  let hasServed = false;
  // Whether a settle is already in progress. Carrying a recovery out runs structure operations, each of
  // which reports its own end and would otherwise call settle again from inside this one; the outer loop
  // is already owed the same answer, so a nested call returns and lets it finish.
  let settling = false;
  // How many of the supervisor's own operations are on the stack. A notification that arrives while one
  // is running is recorded, not carried out: §5.7 wants the request answered after the *operation it
  // arrived in* has ended, and an operation the supervisor is running is not over until it returns. The
  // startup sequence is the case that matters - a re-anchor raised while (d) redelivers must not run its
  // nested stop/start in the middle of the sequence, or the sequence's own (e) connection is issued over
  // it and the run opens more than the one connection it should. A socket arrival never comes through a
  // supervisor call, so its depth is zero and its request is carried out the moment the arrival ends.
  let operationDepth = 0;

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
    // The structure calls this once the execution right is back, after any operation it ran - a socket
    // arrival in particular, which never comes through a supervisor API call. That is the moment a stop
    // or a re-anchor recorded from inside the operation can be carried out.
    onOperationEnd: () => settle(),
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
    // A request raised while one of the supervisor's own operations is running waits for that operation:
    // its exit calls this again, at depth zero. This is what keeps a request from being carried out in the
    // middle of the startup sequence - and the depth check sits before the re-entrancy guard, so a nested
    // settle inside a supervisor operation does not mark the outer loop as running.
    if (operationDepth > 0) return;
    // Reception has actually happened: the run reached the board, so the startup deadline has done its
    // job and a later re-anchor must not be read as a startup that never completed.
    if (!hasServed && serving()) hasServed = true;
    // Re-entrant calls are answered by the loop already running: the operations a recovery runs report
    // their own end, and that end must not start a second pass over the same requests. A request raised
    // by such a nested operation stays recorded and the outer loop picks it up on its next turn.
    if (settling) return;
    settling = true;
    try {
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
    } finally {
      settling = false;
    }
  }

  /** The re-entrancy refusal a structure window answers with when another operation holds the right. */
  const refused = (result) => result !== null && typeof result === 'object' && result.code === 'REENTRANT_OPERATION';

  /**
   * Readiness: reception is admitted and the board is actually serving on the connection that is
   * current. A structure that has merely opened a socket has proved nothing - the board is serving
   * only once the current connection's frames have been applied and its boundary is proven (§5.7,
   * C6/C7). `book.appliedBoundary.connectionId` is read live, so a re-anchor's new connection must
   * have its own frames applied before this returns true again; and a stop, an end, or a stopped
   * structure is never ready whatever the board's phase says.
   */
  function serving() {
    if (!started || stopped || ended || closed) return false;
    if (structure.stats.stopped) return false;
    return structure.book.appliedBoundary.connectionId !== null && structure.book.isRunning === true;
  }

  /**
   * Arm the startup deadline once, when the sequence begins. It fires on the run's own timers; if the run
   * still has not reached reception - and never did - the deadline ends it non-zero rather than letting
   * the reconnect backoff run for ever.
   *
   * The elapsed check runs on the monotonic clock, not the wall clock. That is what makes the deadline
   * survive a clock correction: a wall clock that is set back cannot make the elapsed shrink below the
   * deadline and leave the run unmonitored. And a timer that fires *early* - because a test drove it by
   * hand, or the timer ran ahead of the clock - does not drop the monitoring either: the firing re-arms
   * the timer for exactly the time left, so the deadline is never lost and still acts the moment it is
   * really due. The state guard (an ended, stopped, or already-serving run) is checked first and spends the
   * timer: a run that reached reception must never be ended by a deadline for a startup that did happen.
   */
  function armStartupDeadline() {
    if (startupDeadlineTimer !== null || ended || closed) return;
    const armedAtMs = monotonicMs();
    const fire = () => {
      if (ended || stopped || closed || hasServed) {
        startupDeadlineTimer = null;
        return;
      }
      const remaining = startupDeadlineMs - (monotonicMs() - armedAtMs);
      if (remaining > 0) {
        // Fired before the deadline had really passed: keep the monitoring by re-arming for the remainder.
        startupDeadlineTimer = setTimer(fire, remaining);
        if (typeof startupDeadlineTimer?.unref === 'function') startupDeadlineTimer.unref();
        return;
      }
      startupDeadlineTimer = null;
      recordStop({
        market,
        reason: `reception was not reached within the startup deadline (${startupDeadlineMs} ms)`,
      });
      settle();
    };
    startupDeadlineTimer = setTimer(fire, startupDeadlineMs);
    if (typeof startupDeadlineTimer?.unref === 'function') startupDeadlineTimer.unref();
  }

  function carryStop() {
    const stoppedIt = stopStructure();
    if (refused(stoppedIt)) return false;
    stopRequest = null;
    refetchRequest = null;
    ended = true;
    stopped = true;
    started = false;
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
    if (refetchRequest === null) return true;
    if (recoveries >= maxRecoveryAttempts) {
      recordStop({ market, reason: `the board could not be re-anchored after ${recoveries} attempts` });
      return true; // the request is now a stop, carried out by the loop
    }
    // The old connection is abandoned first: its silence and stability timers, and any event it queued,
    // are checked against a socket that is already gone, so nothing of it can re-anchor the board.
    const stoppedIt = structure.stop();
    if (refused(stoppedIt)) return false; // the request stays: it is only answered once it is carried out
    recoveries += 1;
    started = false;
    step('recover', { attempt: recoveries, connectionId: info?.connectionId ?? null });
    try {
      const announced = structure.start();
      // A refusal here means another operation holds the store's right again. The request is *not*
      // cleared: it has not been carried out, and whoever holds the right will call settle() on the
      // way out and owe it the same answer.
      if (refused(announced)) return false;
    } catch (error) {
      recordStop({ market, reason: `the re-anchor failed: ${error.message}` });
      return true;
    }
    // The replacement started, so the request we answered is cleared - but only that one. A newer
    // request recorded from inside the recovery (a connection the book again refused) is a different
    // need and stays pending for the loop's next pass.
    if (refetchRequest === info) refetchRequest = null;
    if (stopRequest === null && !structure.stats.stopped) started = true;
    return true;
  }

  function runOperation(operation) {
    operationDepth += 1;
    try {
      return operation();
    } finally {
      // The operation has ended: only now may a request it raised be carried out, and the depth is back to
      // what it was before, so a request raised inside settles here rather than from inside the operation.
      operationDepth -= 1;
      settle();
    }
  }

  /**
   * Run one step of the startup sequence, turning a failure at that step into a stop and a non-zero
   * end. A step that throws is exactly as fatal as one that reports failure: either the run did not
   * reach reception, and a process that carried on from here would be one that never opened a socket
   * while looking alive. The stop is recorded first, so a notification hook cannot swallow it, and
   * because the end is abnormal the completion is never written.
   */
  function stepOrStop(name, run) {
    try {
      const value = run();
      // A step that returns a re-entrancy refusal did not run either: another operation held the
      // store's right, and treating that as success would carry on past work that never happened.
      if (refused(value)) {
        recordStop({ market, reason: `the ${name} step was refused as re-entrant: ${value.reason}` });
        return { ok: false, reason: `the ${name} step was refused` };
      }
      return { ok: true, value };
    } catch (error) {
      recordStop({ market, reason: `the ${name} step failed: ${error.message}` });
      return { ok: false, reason: `the ${name} step failed: ${error.message}` };
    }
  }

  /**
   * The startup sequence. Every step that can fail stops the run and exits non-zero; the drain retries only
   * a transient raw refusal, and stops on a corrupt spool, a failed save or a refused admission.
   */
  function start() {
    return runOperation(() => {
      if (ended) return { started: false, reason: 'this supervisor has ended' };
      // The deadline covers (a)-(e): from the moment the run tries to start until it actually receives.
      armStartupDeadline();
      step('beginRun', {});
      const begun = stepOrStop('beginRun', () => structure.beginRun());
      if (!begun.ok) return { started: false, reason: begun.reason };
      if (begun.value?.begun === false) {
        recordStop({ market, reason: begun.value.reason ?? 'the run could not be marked live' });
        return { started: false, reason: 'the run could not be marked live' };
      }
      step('restore', {});
      const restored = stepOrStop('restore', () => structure.restore());
      if (!restored.ok) return { started: false, reason: restored.reason };
      if (stopRequest !== null || ended) return { started: false, reason: 'the store could not be restored' };
      // (c): the walk is bounded per call, and the sequence must not reach (e) while the spool still holds
      // records - the old connection is retired the moment the new one is admitted, so anything left here
      // would never be read again. The step is wrapped like the others, so a save that throws on the way
      // out is a stop and a non-zero end rather than an exception escaping the sequence.
      step('drain', {});
      const drained = stepOrStop('drain', () => drainWithRetry());
      if (!drained.ok) return { started: false, reason: drained.reason };
      if (drained.value?.ok === false || stopRequest !== null || ended || structure.stats.stopped) {
        return { started: false, reason: 'the old spool could not be drained' };
      }
      // (d): redeliver what the raw holds and the board does not have, which fixes where the old
      // connection's numbering can start - before the connection is announced, because a completion
      // heard before these frames could carry a start above a frame the store already holds (§2.2).
      step('redeliver', {});
      const redelivered = stepOrStop('redeliver', () => structure.redeliverPending());
      if (!redelivered.ok) return { started: false, reason: redelivered.reason };
      if (stopRequest !== null || ended || structure.stats.stopped) {
        return { started: false, reason: 'redelivery stopped the structure' };
      }
      // (e): announce the connection; the socket opens only if the book admits it in the real
      // onGeneration.
      step('start', {});
      const announced = stepOrStop('start', () => structure.start());
      if (!announced.ok) return { started: false, reason: announced.reason };
      if (announced.value && announced.value.started === false) return announced.value;
      // The connection reports its own start by returning nothing; whether it was admitted is visible in the
      // stop a refused connection raises, and in the structure having stopped. A run the hook already ended
      // (the refusal was carried out the moment the announcement's operation finished) is not a start either.
      if (stopRequest !== null || ended || structure.stats.stopped) {
        return { started: false, reason: 'the connection was not admitted' };
      }
      started = true;
      return { started: true, connectionId: structure.connection.connectionId };
    });
  }

  /** Bytes the spool still holds, or 0 when there is no spool. The sequence's own read of "is the walk
   *  done", so a bounded batch that left records behind is never mistaken for a finished drain. */
  function spoolRemainingBytes() {
    return structure.spool?.bytes ?? 0;
  }

  /**
   * Drain the old spool until it is empty. Each walk is bounded (`drainSpool`'s own limit), so one call
   * is not the same as a finished drain: the sequence keeps walking while records remain, and only a
   * transient raw refusal is retried - a corrupt spool, a failed save or a refused admission stops.
   */
  function drainWithRetry() {
    let attempts = 0;
    for (;;) {
      const drained = structure.drainSpool();
      if (drained?.stopped) {
        // The machine-readable code decides what a retry can improve. Only a raw that refused a record is
        // the transient case: the spool may be handed back once the raw can take it. A spool that cannot
        // be read, a frame the store could not save, or a connection the board has not accepted is not
        // something a retry improves, and those stop the run. The report keeps the string form for people;
        // the classification is the code.
        const transient = drained.stoppedCode === 'raw-refused';
        if (transient && attempts < maxRecoveryAttempts) {
          attempts += 1;
          continue;
        }
        recordStop({ market, reason: `the old spool could not be drained: ${drained.stopped}` });
        return { ...drained, ok: false };
      }
      // Not stopped, but progress is not the same as completion: a bounded walk can return with records
      // still waiting. Handing the board over now would retire the old connection with those records
      // unread, so the walk continues until the spool is actually empty.
      if (spoolRemainingBytes() === 0) return { ...drained, ok: true };
      if (!(drained?.consumed > 0)) {
        // Bytes remain and this walk consumed none: no retry can move it, and carrying on would loop.
        const reason = 'the old spool could not be drained to empty';
        recordStop({ market, reason });
        return { ...drained, stopped: reason, stoppedCode: 'not-consumed', ok: false };
      }
      attempts = 0; // progress was made; the retry budget applies to a stuck batch, not to the spool
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
        started = false;
        if (!ended) {
          ended = true;
          stopStructure();
        }
        return { stopped: true };
      }),
    close: () => {
      if (closed) return { closed: true, completed: false, reason: 'this supervisor is closed' };
      closed = true;
      stopStructure();
      // The marker is written only on a clean end: an abnormal end leaves no completion, so the next start
      // can tell that this one did not stop on purpose. The completion's outcome is *returned* rather than
      // swallowed, and a completion that throws is re-thrown after the structure is still closed: a process
      // turning this into its exit code has to be able to tell a written completion from one the store
      // refused, and only the former is a clean end.
      let completion = null;
      let failure = null;
      try {
        if (!abnormal) completion = structure.completeRun();
      } catch (error) {
        failure = error;
      } finally {
        structure.close();
      }
      if (failure !== null) throw failure;
      if (abnormal) {
        return { closed: true, completed: false, reason: 'the run ended abnormally, so no completion was written' };
      }
      // A refusal (a store that holds the execution right) comes back as a value with `completed: false`,
      // not as a throw; both are a completion that is not on disk.
      return {
        closed: true,
        completed: completion?.completed !== false,
        code: completion?.code,
        reason: completion?.reason,
      };
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
      return serving();
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
