/**
 * Stage 5b: the supervisor's wiring - one run of the three-process receiver.
 *
 * The pieces below (ingest, organize, book) exist as their own processes with their own stores,
 * speaking the stage-1 vocabulary over the supervisor's router. What was missing is the part that
 * *runs* them: spawning the three children, carrying out the startup sequence in the one safe order,
 * stopping processing in one order and the processes in another, restarting a child by condition,
 * aggregating readiness, and holding each store against a second owner. This module is that part.
 *
 * The startup sequence (§5.7, rulings ① and the stage-5 corrections), and why the order is not a
 * matter of taste:
 *
 *   (a) mark this run live - organize's `beginRun`. Nothing else may be mistaken for the previous
 *       run's work.
 *   (b) hand organize the book's recorded applied boundary. It is NOT skipped when the ledger is
 *       empty: a restart whose ledger is empty but whose spool still holds frames must organize them
 *       on the connection the board recorded, and organize learns that nowhere else.
 *   (c) walk the old spool oldest-first, re-injecting each spilled frame through the ordinary path.
 *   (d) deliver what is durable and owed to the board. (d) before (e) is the same fact from the other
 *       side: a delivered frame fixes where the old connection's numbering can start, and a completion
 *       heard before it could carry a start above a frame the store already holds. This module keeps
 *       the invariant - `start()` runs `deliverOwed()` and only then issues the connection.
 *   (e) issue the connection. The accept is ingest's; the book authorizes; organize adopts; and only
 *       the adoption opens the socket. The book's success alone opens nothing.
 *
 * Stop order: processing stops reception -> organization -> board. Accepted receive callbacks are
 * synchronous in the forked role, so its stop RPC runs after the current callback. No final-tail
 * proof or completion marker is part of ordinary shutdown; durable pending work resumes at startup.
 * Children then close board -> organization -> reception, releasing stores after confirmed exit.
 * Stop/close failures remain abnormal even if later termination succeeds.
 *
 * Child failure is by condition (ruling ⑬): the book alone is restarted, without renewing the receive
 * generation; organize failure keeps reception going within the spool's capacity, stops reception and
 * records the missing when it cannot be held, and ends non-zero when it cannot be recorded.
 *
 * Readiness is aggregated: a role loses readiness on disconnect, on an instance (generation) mismatch,
 * and when its last report is older than the report deadline.
 *
 * Store exclusion is by database, not by `OPEN_STORES` (ruling ⑮): before a role is spawned the
 * supervisor claims that role's database file, refusing a second owner that is still alive, and only
 * releases the file once the old owner's termination is confirmed. Each store also sets
 * `busy_timeout = 0`, so a real busy surfaces at once as an anomaly.
 */

import { randomUUID } from 'node:crypto';
import { resolve } from 'node:path';

import { acquireStoreLock } from './store-lock.mjs';

import { openRouter } from './router.mjs';
import { openIngestProcess } from '../ingest/main.mjs';
import { openOrganizeProcess } from '../organize/main.mjs';
import { openBookProcess } from '../book/main.mjs';

export const RUN_ROLES = Object.freeze(['ingest', 'organize', 'book']);

/** Stop intake before its downstream consumers. */
export const PROCESSING_STOP_ORDER = Object.freeze(['ingest', 'organize', 'book']);

/** The process termination order: the board is let go first, reception last (it owns the socket). */
export const TERMINATION_ORDER = Object.freeze(['book', 'organize', 'ingest']);

const ROUTE_OPTIONS = Object.freeze({ batchFrames: 1 });

async function until(predicate, { timeoutMs = 10_000, stepMs = 5, label = 'the condition', nowMs = () => Date.now() } = {}) {
  const deadline = nowMs() + timeoutMs;
  for (;;) {
    if (predicate()) return true;
    if (nowMs() >= deadline) {
      throw new Error(`timed out (${timeoutMs} ms) waiting for ${label}`);
    }
    await new Promise((done) => setTimeout(done, stepMs));
  }
}

// ---------------------------------------------------------------------------------------------------
// Store exclusion: one database, one owner across processes (ruling ⑮).
// ---------------------------------------------------------------------------------------------------

/** Whether a pid is a live process here. EPERM means it exists but belongs to another user. */
export function defaultProcessAlive(pid, { self = process.pid } = {}) {
  if (!Number.isInteger(pid) || pid <= 0) return false;
  if (pid === self) return true;
  try {
    process.kill(pid, 0);
    return true;
  } catch (error) {
    return error?.code === 'EPERM';
  }
}

/**
 * A process-wide lease for one role database, backed by SQLite's atomic `BEGIN IMMEDIATE` lock rather
 * than a read-then-write owner file. The lock is a sidecar held open by the supervisor while roles start
 * and run. The role process also holds its own writer-scope lease (see `bin/role.mjs`), so a supervisor
 * crash cannot admit another writer before a disconnected child has actually exited.
 */
export function createStoreExclusion({ isProcessAlive = defaultProcessAlive, nowMs = () => Date.now() } = {}) {
  const held = new Map(); // resolved store path -> { owner, lease }

  return {
    claim({ path, role, instance, pid = process.pid }) {
      if (!path) return { claimed: false, code: 'NO_PATH', reason: 'a store claim needs a path' };
      const key = resolve(path);
      const existing = held.get(key);
      if (existing) {
        return {
          claimed: false,
          code: 'STORE_ALREADY_OWNED',
          reason: `${key} is held by ${existing.owner.role}/${existing.owner.instance} (pid ${existing.owner.pid})`,
          owner: existing.owner,
        };
      }
      const lease = acquireStoreLock({ path: key, role, instance, scope: 'supervisor' });
      if (!lease.acquired) {
        return {
          claimed: false,
          code: 'STORE_ALREADY_OWNED',
          reason: lease.reason,
        };
      }
      const owner = { role, instance, pid, claimed_at_ms: nowMs(), file: lease.path };
      held.set(key, { owner, lease });
      return { claimed: true, path: lease.path, tookOverStale: false, owner };
    },

    /**
     * Release only after the child is confirmed gone (or its pid is no longer alive). A timeout or a
     * SIGKILL request without an observed exit is not confirmation and must leave the SQLite lease held.
     */
    release({ path, instance, confirmedTerminated = false }) {
      const key = resolve(path);
      const record = held.get(key);
      if (!record) return { released: false, reason: 'this store was not claimed by this supervisor' };
      if (instance !== undefined && record.owner.instance !== instance) {
        return { released: false, refused: true, reason: 'a different instance owns this store' };
      }
      if (!confirmedTerminated && isProcessAlive(record.owner.pid)) {
        return { released: false, refused: true, reason: `the owner ${record.owner.instance} is still alive` };
      }
      try {
        const result = record.lease.release();
        if (result.released) held.delete(key);
        return result;
      } catch (error) {
        return { released: false, refused: true, reason: `the SQLite owner lock could not be released: ${error.message}` };
      }
    },

    heldBy(path) {
      return held.get(resolve(path))?.owner ?? null;
    },

    lockPath(path) {
      return `${resolve(path)}.supervisor-lock.sqlite`;
    },

    get heldCount() {
      return held.size;
    },
  };
}

// ---------------------------------------------------------------------------------------------------
// Readiness aggregation (ruling ⑬).
// ---------------------------------------------------------------------------------------------------

/**
 * Readiness for the whole run, from three facts per role: it is connected to the supervisor, the
 * instance bound to it is the one the supervisor spawned (a mismatch is another generation's channel),
 * and its last report is fresh within the report deadline. A missing report is not itself a loss - a
 * role that has never spoken is judged by the other two facts - but a report that has aged past the
 * deadline loses readiness, which is the point of the deadline.
 */
export function createReadinessAggregator({ nowMs = () => Date.now(), reportDeadlineMs = 60_000 } = {}) {
  const connected = new Map();
  const expectedInstance = new Map();
  const boundInstance = new Map();
  const reports = new Map(); // role -> { at, payload }

  return {
    setConnected(role, value) {
      connected.set(role, value === true);
    },
    setExpectedInstance(role, instance) {
      expectedInstance.set(role, instance);
    },
    setBoundInstance(role, instance) {
      boundInstance.set(role, instance ?? null);
    },
    noteReport(role, payload) {
      reports.set(role, { at: nowMs(), payload: payload ?? {} });
    },
    clearReport(role) {
      reports.delete(role);
    },
    get reportDeadlineMs() {
      return reportDeadlineMs;
    },
    snapshot() {
      const roles = {};
      const reasons = [];
      let ready = true;
      for (const role of RUN_ROLES) {
        const isConnected = connected.get(role) === true;
        const expected = expectedInstance.get(role) ?? null;
        const bound = boundInstance.get(role) ?? null;
        const mismatch = expected !== null && bound !== expected;
        const report = reports.get(role);
        const fresh = report !== undefined && nowMs() - report.at <= reportDeadlineMs;
        const parts = [];
        if (!isConnected) parts.push('disconnected');
        if (mismatch) parts.push('instance-mismatch');
        if (report !== undefined && !fresh) parts.push('report-deadline');
        if (report !== undefined && fresh && report.payload?.ready === false) parts.push('reported-not-ready');
        const ok = parts.length === 0;
        roles[role] = {
          ok,
          connected: isConnected,
          expectedInstance: expected,
          boundInstance: bound,
          mismatch,
          reported: report !== undefined,
          reportFresh: fresh,
        };
        if (!ok) {
          ready = false;
          for (const reason of parts) reasons.push({ role, reason });
        }
      }
      return { ready, roles, reasons };
    },
  };
}

// ---------------------------------------------------------------------------------------------------
// Child-failure policy (ruling ⑬), as a pure decision so it can be fixed independently of the run.
// ---------------------------------------------------------------------------------------------------

export function decideChildFailure(
  role,
  { restarts = 0, maxRestarts = 3, spoolWithinCapacity = true, canRecordMissing = true } = {},
) {
  if (restarts >= maxRestarts) {
    return { action: 'exit', code: 1, reason: `the ${role} failed after ${restarts} restarts` };
  }
  if (role === 'book') {
    // Reception and organization carry on; only the book is restarted, and the receive generation is
    // deliberately not renewed (ruling ⑧).
    return { action: 'restart-book', renewGeneration: false, reason: 'the book alone is restarted' };
  }
  if (role === 'organize') {
    if (spoolWithinCapacity) {
      return { action: 'continue', reason: 'reception continues within the spool capacity' };
    }
    if (canRecordMissing) {
      return { action: 'stop-reception-record', reason: 'reception stops and the missing is recorded' };
    }
    return { action: 'exit', code: 1, reason: 'reception cannot be held and the missing cannot be recorded' };
  }
  return { action: 'exit', code: 1, reason: `the ${role} failed and there is no rule to continue` };
}

// ---------------------------------------------------------------------------------------------------
// The run supervisor.
// ---------------------------------------------------------------------------------------------------

/**
 * The default spawner starts each role in *this* process using its own entrance. That is the seam the
 * design leaves open: the same supervisor should be able to `fork` the role entrances as real OS
 * children, but a fork needs a per-role CLI and a real venue websocket, neither of which exists yet
 * (recorded as stage 5b-2 / open point). The spawner is injectable so a test - or that fork - can
 * replace it; the wiring above it does not change.
 */
async function inProcessSpawner(role, { options }) {
  if (role === 'ingest') return openIngestProcess({ ...options, takeoverFor: () => options.takeover === true });
  if (role === 'organize') return openOrganizeProcess(options);
  if (role === 'book') return openBookProcess(options);
  throw new TypeError(`no entrance for role ${role}`);
}

/**
 * The in-process spawner is kept for tests: it is the seam the through tests use to exercise the
 * wiring above without paying for three OS processes. The real process separation is
 * `createForkSpawner` in `./process-spawner.mjs`, which forks `bin/role.mjs` per role.
 */
export { inProcessSpawner };

export function createRunSupervisor(options = {}) {
  const {
    market,
    stream = 'trades',
    venue,
    adapter,
    runId = randomUUID(),
    ingestStorePath = null,
    organizeStorePath = null,
    bookStorePath = null,
    spoolDir = null,
    // Set 7a: where the canonical raw lives, handed to the ingest child. Absent means no raw writer.
    rawDir = null,
    // Set 8: the auxiliary open-interest REST poll interval, handed to the ingest child. Absent/0
    // means no poller.
    oiPollIntervalMs = 0,
    routerListenPath,
    webSocketImpl,
    rawWriter = null,
    nowMs = () => Date.now(),
    reportDeadlineMs = 60_000,
    // Stage 5c: how often a role reports its own readiness to the aggregator (via the router). Must be
    // well under `reportDeadlineMs` so a live role is not expired by its own reporting interval.
    readinessIntervalMs = 1_000,
    startupDeadlineMs = 60_000,
    stopRpcTimeoutMs = 1_000,
    // The deadline clock is monotonic and injectable, like the receive connection's clock. Timer hooks
    // keep the whole startup/first-serving boundary testable without changing production timing.
    setTimer = setTimeout,
    clearTimer = clearTimeout,
    monotonicMs = () => Number(process.hrtime.bigint()) / 1e6,
    maxRestarts = 3,
    excludeStores = true,
    spawner = inProcessSpawner,
    exit = (code) => {
      process.exitCode = code;
    },
    onDiagnostic = () => {},
    onStep = () => {},
    onReadiness = () => {},
    onChildExit = () => {},
  } = options;

  if (!market) throw new TypeError('the run supervisor needs a market');
  if (!venue) throw new TypeError('the run supervisor needs a venue');

  const exclusion = createStoreExclusion({ nowMs });
  const readiness = createReadinessAggregator({ nowMs, reportDeadlineMs });

  const children = { ingest: null, organize: null, book: null };
  const restarts = { ingest: 0, organize: 0, book: 0 };
  let router = null;
  let started = false;
  let stopped = false;
  let ended = false;
  let closed = false;
  let abnormal = false;
  let stopPromise = null;
  let closeResult = null;
  let operationDepth = 0;
  let startupDeadlineAt = null;
  let startupDeadlineTimer = null;
  let startupInProgress = false;
  let hasServed = false;
  let fatalReason = null;
  let fatalExitPromise = null;

  function clearStartupDeadline() {
    if (startupDeadlineTimer !== null) {
      clearTimer(startupDeadlineTimer);
      startupDeadlineTimer = null;
    }
  }

  function startupRemainingMs() {
    if (startupDeadlineAt === null) return startupDeadlineMs;
    return Math.max(0, startupDeadlineAt - monotonicMs());
  }

  function startupDeadlineError(label) {
    return new Error(`${label} did not finish within the startup deadline (${startupDeadlineMs} ms)`);
  }

  function withStartupDeadline(promise, label) {
    const remaining = startupRemainingMs();
    if (remaining <= 0) return Promise.reject(startupDeadlineError(label));
    let timer = null;
    return Promise.race([
      Promise.resolve(promise),
      new Promise((_, reject) => {
        timer = setTimer(() => reject(startupDeadlineError(label)), remaining);
        if (typeof timer?.unref === 'function') timer.unref();
      }),
    ]).finally(() => {
      if (timer !== null) clearTimer(timer);
    });
  }

  function withStopDeadline(promise, role) {
    if (!Number.isFinite(stopRpcTimeoutMs) || stopRpcTimeoutMs <= 0) {
      return Promise.reject(new Error(`the ${role} stop RPC has no valid timeout`));
    }
    let timer = null;
    return Promise.race([
      Promise.resolve(promise),
      new Promise((_, reject) => {
        timer = setTimer(
          () => reject(new Error(`the ${role} stop RPC timed out after ${stopRpcTimeoutMs} ms`)),
          stopRpcTimeoutMs,
        );
        if (typeof timer?.unref === 'function') timer.unref();
      }),
    ]).finally(() => {
      if (timer !== null) clearTimer(timer);
    });
  }

  function currentBoardIsServing() {
    const connectionId = children.ingest?.process.connectionId;
    const boundary = children.book?.process.appliedBoundary;
    return (
      started &&
      connectionId !== null &&
      connectionId !== undefined &&
      boundary?.connectionId === connectionId &&
      children.book?.process.isRunning === true
    );
  }

  function checkStartupDeadline() {
    if (startupDeadlineTimer !== null) {
      clearTimer(startupDeadlineTimer);
      startupDeadlineTimer = null;
    }
    if (closed || ended || stopped || hasServed) return;
    if (currentBoardIsServing()) {
      hasServed = true;
      startupDeadlineAt = null;
      step('startup-served', { connectionId: children.ingest?.process.connectionId ?? null });
      return;
    }
    const remaining = startupRemainingMs();
    if (remaining <= 0) {
      requestFatal(`reception was not reached within the startup deadline (${startupDeadlineMs} ms)`);
      return;
    }
    startupDeadlineTimer = setTimer(checkStartupDeadline, Math.min(25, remaining));
    if (typeof startupDeadlineTimer?.unref === 'function') startupDeadlineTimer.unref();
  }

  function armServingDeadline() {
    if (hasServed || closed || ended || stopped) return;
    checkStartupDeadline();
  }

  function finishFatalRun() {
    if (fatalExitPromise !== null || closed) return fatalExitPromise;
    abnormal = true;
    clearStartupDeadline();
    fatalExitPromise = (async () => {
      try {
        await terminate();
      } catch (error) {
        diagnostic(`the failed run could not terminate every role: ${error.message}`, { kind: 'terminate' });
      }
      try {
        exit(1);
      } catch {
        /* the fatal exit is best-effort after all role termination attempts */
      }
    })();
    return fatalExitPromise;
  }

  function requestFatal(reason) {
    if (fatalReason === null) {
      fatalReason = reason;
      abnormal = true;
      diagnostic(reason, { kind: 'failure' });
    }
    // While the startup sequence is running, its deadline-bounded awaits will return a failed start and
    // the entrance owns termination. After admission, the run supervisor owns the abnormal end.
    if (!startupInProgress && started) void finishFatalRun();
  }

  function diagnostic(reason, extra = {}) {
    try {
      onDiagnostic({ market, reason, ...extra });
    } catch {
      /* a diagnostic is best-effort by contract */
    }
  }

  function step(name, detail = {}) {
    try {
      onStep({ step: name, ...detail });
    } catch {
      /* an observation that throws is not a fact about the run */
    }
  }

  function runOperation(fn) {
    operationDepth += 1;
    try {
      return fn();
    } finally {
      operationDepth -= 1;
    }
  }

  function storePathFor(role) {
    if (role === 'ingest') return ingestStorePath;
    if (role === 'organize') return organizeStorePath;
    if (role === 'book') return bookStorePath;
    return null;
  }

  function instanceFor(role, suffix = null) {
    return suffix === null ? `${role}-${runId}` : `${role}-${runId}-${suffix}`;
  }

  function specFor(role, suffix = null) {
    const instance = instanceFor(role, suffix);
    if (role === 'ingest') {
      return {
        instance,
        options: {
          market,
          stream,
          adapter,
          venue,
          runId,
          // This supervisor owns the stores. start() admits this run only after old spool/ledger
          // recovery, so its accept may explicitly replace the persisted book owner from a prior run.
          takeover: true,
          roleInstance: instance,
          webSocketImpl,
          organizeSocketPath: router.path,
          ingestStorePath,
          spoolDir,
          ...(rawDir === null ? {} : { rawDir }),
          ...(Number.isFinite(oiPollIntervalMs) && oiPollIntervalMs > 0 ? { oiPollIntervalMs } : {}),
          channelOptions: ROUTE_OPTIONS,
          readinessIntervalMs,
          onStop: (info) => diagnostic(`reception stopped: ${info?.reason ?? 'unknown'}`, { kind: 'stop' }),
          onDiagnostic: (d) => diagnostic(d?.reason ?? String(d), { kind: 'ingest' }),
        },
      };
    }
    if (role === 'organize') {
      return {
        instance,
        options: {
          routerSocketPath: router.path,
          market,
          stream,
          runId,
          roleInstance: instance,
          storePath: organizeStorePath,
          rawWriter: rawWriter ?? null,
          markRunning: false, // (a) is the supervisor's to carry out
          channelOptions: ROUTE_OPTIONS,
          readinessIntervalMs,
          onDiagnostic: (d) => diagnostic(d?.reason ?? String(d), { kind: 'organize' }),
        },
      };
    }
    // The book reads its level changes off the frame and needs no adapter of its own; the supervisor
    // holds none either (ruling ③). A venue with no proof runs as unverifiable, not as proved.
    return {
      instance,
      options: {
        organizeSocketPath: router.path,
        market,
        stream,
        runId,
        roleInstance: instance,
        adapter: null,
        storePath: bookStorePath,
        channelOptions: ROUTE_OPTIONS,
        readinessIntervalMs,
        onDiagnostic: (d) => diagnostic(d?.reason ?? String(d), { kind: 'book' }),
      },
    };
  }

  /**
   * Spawn one role, claiming its database first. The claim is the inter-process exclusion: a live
   * owner means the spawn is refused rather than racing it for the file.
   */
  async function spawnRole(role, suffix = null) {
    const spec = specFor(role, suffix);
    spec.startupTimeoutMs = startupRemainingMs();
    const dbPath = storePathFor(role);
    if (excludeStores && dbPath) {
      const claim = exclusion.claim({ path: dbPath, role, instance: spec.instance });
      if (!claim.claimed) {
        throw new Error(`store exclusion refused the ${role}: ${claim.reason}`);
      }
      if (claim.tookOverStale) diagnostic(`recovered a stale store claim for the ${role}`, { kind: 'exclusion' });
    }
    let process;
    try {
      process = await withStartupDeadline(spawner(role, spec), `the ${role} role to become ready`);
    } catch (error) {
      // The fork spawner only declares a timed-out spawn failed after it has confirmed that the child is
      // gone. Keep the exclusion claim on any unconfirmed failure; releasing it while a writer may still
      // be alive would admit a second owner.
      if (excludeStores && dbPath && error?.terminatedConfirmed === true) {
        exclusion.release({ path: dbPath, instance: spec.instance, confirmedTerminated: true });
      }
      throw error;
    }
    const handle = { role, instance: spec.instance, process, storePath: dbPath };
    children[role] = handle;
    // Stage 5c: a real child process announces its own death (with code and signal) through the
    // spawner. Only a death that is not an intentional close reaches here, and only while this handle
    // is still the current one - so the condition-by-condition failure policy (ruling ⑬) runs against
    // a real process death, not a simulated one.
    if (typeof process?.onExit === 'function') {
      process.onExit((info) => {
        if (closed || ended || children[role] !== handle) return;
        void handleChildFailure(role, info);
      });
    }
    readiness.setExpectedInstance(role, spec.instance);
    readiness.setConnected(role, true);
    step('spawn', { role, instance: spec.instance });
    return handle;
  }

  /** Wind one role's store ownership down: release the file only once its termination is confirmed. */
  function releaseRole(role, { confirmedTerminated }) {
    const handle = children[role];
    const dbPath = handle?.storePath ?? storePathFor(role);
    if (excludeStores && dbPath) {
      exclusion.release({ path: dbPath, instance: handle?.instance, confirmedTerminated });
    }
    readiness.setConnected(role, false);
  }

  async function closeRole(role) {
    const handle = children[role];
    if (!handle) return;
    readiness.setConnected(role, false);
    children[role] = null;
    try {
      await handle.process.close();
    } catch (error) {
      diagnostic(`the ${role} could not be closed cleanly: ${error.message}`, { kind: 'terminate' });
    }
    releaseRole(role, { confirmedTerminated: true });
  }

  async function waitForRoles(timeoutMs = startupRemainingMs()) {
    await until(() => RUN_ROLES.every((role) => router.channels().has(role)), {
      timeoutMs,
      nowMs: monotonicMs,
      label: 'all three roles to announce themselves to the supervisor',
    });
    for (const role of RUN_ROLES) readiness.setBoundInstance(role, router.instances().get(role));
  }

  async function waitForAdmission(timeoutMs = startupRemainingMs()) {
    // Admission is the round trip, not the moment a connection id appears: ingest names its connection
    // as soon as it is announced (before anyone answered), so the id alone would let the startup race
    // past (e). What proves the socket may open is organize's adoption having come back - reception
    // has left `idle`. The supervisor's own pending accept must be settled too.
    //
    // "Left idle" is checked as a state, not as `=== 'connecting'`: a real child process opens its
    // socket and moves on to `subscribing` as soon as the adoption lands, and the supervisor's cached
    // snapshot may never observe the brief `connecting` window. A state that is not `idle` and not a
    // refusal is exactly "admitted and running"; requiring the precise intermediate state would make
    // the supervisor wait for a deadline to pass and only then catch a later generation.
    await until(
      () => {
        if (fatalReason !== null) throw new Error(fatalReason);
        const ingest = children.ingest?.process;
        if (!ingest || ingest.connectionId == null) return false;
        if (router.pendingAcceptCount !== 0) return false;
        const state = ingest.state;
        return typeof state === 'string' && state !== '' && state !== 'idle' && state !== 'refused';
      },
      { timeoutMs, nowMs: monotonicMs, label: 'the connection to be authorized, adopted and admitted' },
    );
  }

  /**
   * §9.2 (the same rule the in-process structure applies through its own wiring): record the
   * interval each unclean earlier run left behind, before the boundary is followed again.
   *
   * The receive tail is a lower bound on what that run heard - it is saved on its own clock now, so
   * the last moments before a crash are not in it - and a run that did not write its completion
   * marker may have received frames that were never made durable. That interval must stay visible as
   * a suspected gap rather than be smoothed over. It is recorded once per restart, and never deleted
   * (C7): a board recovered by new data does not erase the history it could not prove.
   */
  async function recordRestartGaps() {
    let recorded = 0;
    const tails = await withStartupDeadline(children.ingest.process.receivedTails(), 'the receive tails');
    for (const tail of tails) {
      // A connection name begins with the run that issued it (C2), and a run id has no colon, so
      // the first field names the run unambiguously.
      const tailRunId = String(tail.connectionId).split(':')[0];
      // This run's own tail is not a past interval: this run has not ended.
      if (tailRunId === runId) continue;
      // Only a run with no completion marker is one that may have lost frames; a run that closed
      // cleanly has a tail that is a true upper bound, so there is nothing to suspect.
      const state = await withStartupDeadline(
        children.organize.process.runMarkerState(tailRunId),
        'the run marker of an earlier run',
      );
      if (state === 'complete') continue;
      await withStartupDeadline(
        children.organize.process.recordSuspectedGap({
          market: tail.market,
          stream: tail.stream,
          fromMs: tail.updatedAtMs,
          toMs: nowMs(),
          reason: `a run that did not close cleanly received up to sequence ${tail.lastReceivedSeq} on ${tail.connectionId}; frames after it are unaccounted for`,
        }),
        'a suspected gap',
      );
      recorded += 1;
    }
    return { recorded };
  }

  /** Keep the new connection gated until every confirmed ledger row is applied or otherwise resolved. */
  async function waitForDeliveryRecovery() {
    for (;;) {
      const status = await withStartupDeadline(
        children.organize.process.recoveryStatus(),
        'the delivery-ledger recovery check',
      );
      if (status?.resolved === true) return status;
      const remaining = startupRemainingMs();
      if (remaining <= 0) throw startupDeadlineError('delivery-ledger recovery');
      await new Promise((done) => setTimeout(done, Math.min(25, remaining)));
    }
  }

  // -----------------------------------------------------------------------------------------------
  // Startup.
  // -----------------------------------------------------------------------------------------------

  async function start() {
    if (ended) return { started: false, reason: 'this run has ended' };
    if (started) return { started: true, connectionId: children.ingest?.process.connectionId ?? null };

    startupInProgress = true;
    startupDeadlineAt = monotonicMs() + startupDeadlineMs;
    let result;
    try {
      result = await runOperation(async () => {
        try {
          router = await withStartupDeadline(
            openRouter({
              listenPath: routerListenPath,
              channelOptions: ROUTE_OPTIONS,
              onDiagnostic: (d) => diagnostic(d?.reason ?? String(d), { kind: 'router' }),
              // Role readiness reports feed the run-wide aggregator. Errors are separate facts: an
              // ingest subscription refusal is a failed start, not merely a readiness observation.
              onObserved: (observed) => {
                if (observed.type === 'readiness') {
                  readiness.noteReport(observed.from, observed.message?.payload ?? {});
                  if (observed.from === 'book' && observed.message?.payload?.ready === true) {
                    checkStartupDeadline();
                  }
                  return;
                }
                if (observed.type === 'error' && observed.from === 'ingest') {
                  const payload = observed.message?.payload ?? {};
                  if (payload.role !== 'ingest') return;
                  const reason = payload.reason ?? 'the venue refused the subscription';
                  requestFatal(`the ingest subscription failed: ${reason}`);
                }
              },
            }),
            'the supervisor router to open',
          );
          step('router', { path: router.path });

          await spawnRole('organize');
          await spawnRole('book');
          await spawnRole('ingest');
          await waitForRoles(startupRemainingMs());
          step('bound', { instances: RUN_ROLES.map((r) => router.instances().get(r)) });

          // (a) mark this run live.
          await withStartupDeadline(children.organize.process.beginRun(), 'beginRun');
          step('a:beginRun', {});

          // (a2) §9.2: the intervals unclean earlier runs left behind, before the boundary is
          // followed again - the same rule the in-process structure applies.
          const restartGaps = await recordRestartGaps();
          step('a2:restartGaps', restartGaps);

          // (b) hand organize the book's applied boundary - never omitted, even when it is empty.
          const boundary = children.book.process.appliedBoundary;
          const recorded = await withStartupDeadline(
            children.organize.process.resumeFromBoundary(boundary),
            'boundary restore',
          );
          step('b:boundary', { recorded: recorded.recorded, boundary });

          // (c) the old spool, oldest-first.
          const drained = await withStartupDeadline(children.ingest.process.drainSpool(), 'old spool drain');
          step('c:drainSpool', drained);

          // (d) what is durable and owed goes to the board - before (e), the invariant this wiring fixes.
          const delivered = await withStartupDeadline(children.organize.process.deliverOwed(), 'owed delivery');
          step('d:deliverOwed', delivered);
          const recovery = await waitForDeliveryRecovery();
          step('d:deliveryRecoveryResolved', recovery);

          // (e) issue the connection; the socket opens only once the book authorized and organize adopted.
          await withStartupDeadline(children.ingest.process.start(), 'connection admission');
          step('e:accept', {});
          await waitForAdmission(startupRemainingMs());
          if (fatalReason !== null) throw new Error(fatalReason);
          started = true;
          step('admitted', { connectionId: children.ingest.process.connectionId });
          return { started: true, connectionId: children.ingest.process.connectionId };
        } catch (error) {
          abnormal = true;
          clearStartupDeadline();
          diagnostic(`the run could not start: ${error.message}`, { kind: 'startup' });
          return { started: false, reason: error.message };
        }
      });
      if (result?.started === true) armServingDeadline();
      else {
        clearStartupDeadline();
        startupDeadlineAt = null;
      }
      return result;
    } finally {
      startupInProgress = false;
      if (fatalReason !== null && started) void finishFatalRun();
    }
  }

  // -----------------------------------------------------------------------------------------------
  // Stop: processing order, then termination order, kept apart.
  // -----------------------------------------------------------------------------------------------

  /** Stop local processing without asserting that the venue's final state is complete. */
  function stop() {
    if (stopPromise !== null) return stopPromise;
    stopPromise = runOperation(async () => {
      if (closed) return { stopped: false, abnormal: true, reason: 'this run is closed' };
      clearStartupDeadline();
      startupDeadlineAt = null;
      stopped = true;
      const results = { processingOrder: [...PROCESSING_STOP_ORDER] };
      let confirmed = true;
      for (const role of PROCESSING_STOP_ORDER) {
        try {
          const child = children[role];
          results[role] = child
            ? await withStopDeadline(child.process.stop('the run is stopping'), role)
            : { stopped: false, reason: `the ${role} process is absent` };
        } catch (error) {
          results[role] = { stopped: false, reason: error.message };
        }
        step(`stop:${role}`, results[role]);
        if (results[role]?.stopped !== true) confirmed = false;
        if (results[role]?.stopped !== true || results[role]?.abnormal === true) {
          abnormal = true;
          diagnostic(`the ${role} stop was not normal: ${results[role]?.reason ?? 'stop not confirmed'}`, { kind: 'stop' });
        }
      }
      return { stopped: confirmed, ...results, abnormal };
    });
    return stopPromise;
  }

  /** Terminate the child processes in their own order, releasing each store once it is confirmed gone. */
  async function terminate() {
    clearStartupDeadline();
    startupDeadlineAt = null;
    const order = [...TERMINATION_ORDER];
    const unconfirmed = [];
    for (const role of order) {
      const handle = children[role];
      if (!handle) continue;
      readiness.setConnected(role, false);
      // Null the child first: an intentional close must not read as an abnormal death, and the exit
      // guard is the handle's identity.
      children[role] = null;
      let closeCompleted = false;
      try {
        const outcome = await handle.process.close();
        closeCompleted = outcome?.terminated === true || handle.process.exitInfo != null || handle.process.pid == null;
        const info = outcome?.exitInfo ?? handle.process.exitInfo;
        if (outcome?.clean === false || (info != null && (info.code !== 0 || info.signal != null))) {
          abnormal = true;
          diagnostic(`the ${role} did not close normally`, { kind: 'terminate' });
        }
      } catch (error) {
        closeCompleted = handle.process.exitInfo != null;
        abnormal = true;
        diagnostic(`the ${role} could not be closed cleanly: ${error.message}`, { kind: 'terminate' });
      }
      if (closeCompleted) {
        releaseRole(role, { confirmedTerminated: true });
      } else {
        // A timeout or SIGKILL request is not proof of death. Keep the database exclusion claim held.
        abnormal = true;
        unconfirmed.push(role);
        diagnostic(`the ${role} process termination was not confirmed; its store remains claimed`, { kind: 'terminate' });
      }
    }
    try {
      router?.close();
    } catch {
      /* the server may already be closed */
    }
    router = null;
    closed = true;
    ended = true;
    return { terminationOrder: order, unconfirmed };
  }

  /** Close every role/store after processing stops; success describes shutdown, never completeness. */
  async function close() {
    if (closeResult !== null) return closeResult;
    if (closed) return { closed: true, shutdownSucceeded: false, abnormal, reason: 'this run was already closed' };
    const processing = await stop();
    const termination = await terminate();
    closeResult = {
      closed: true,
      terminationOrder: termination.terminationOrder,
      terminationConfirmed: termination.unconfirmed.length === 0,
      unconfirmedTermination: termination.unconfirmed,
      abnormal,
      shutdownSucceeded: processing.stopped === true && !abnormal && termination.unconfirmed.length === 0,
    };
    return closeResult;
  }

  // -----------------------------------------------------------------------------------------------
  // Child failure and restart (ruling ⑬).
  // -----------------------------------------------------------------------------------------------

  function spoolWithinCapacity() {
    const spool = children.ingest?.process.spool;
    if (spool == null) return true;
    return spool.isOverBound !== true && spool.failed !== true;
  }

  /**
   * A child failed. Record it, mark it unready, and decide by condition what to do. The book alone is
   * restarted and the receive generation is left untouched; organize failure keeps reception going
   * within the spool's capacity, stops reception and records the missing when it cannot, and ends the
   * run non-zero when the missing cannot be recorded.
   */
  async function handleChildFailure(role, info = {}) {
    readiness.setConnected(role, false);
    try {
      onChildExit({ role, ...info });
    } catch {
      /* an observation that throws is not a fact about the failure */
    }
    step('child-exit', { role, ...info });
    const verdict = decideChildFailure(role, {
      restarts: restarts[role] ?? 0,
      maxRestarts,
      spoolWithinCapacity: spoolWithinCapacity(),
      canRecordMissing: true,
    });
    if (verdict.action === 'restart-book') {
      restarts.book += 1;
      const generationBefore = children.ingest?.process.generation ?? null;
      await closeRole('book');
      await spawnRole('book', `r${restarts.book}`);
      const generationAfter = children.ingest?.process.generation ?? null;
      // The receive generation must not have moved with a book restart (ruling ⑧).
      return { restarted: 'book', renewGeneration: generationAfter !== generationBefore, verdict };
    }
    if (verdict.action === 'continue') {
      return { continued: true, verdict };
    }
    if (verdict.action === 'stop-reception-record') {
      await children.ingest?.process.stop();
      diagnostic(`reception stopped and the missing recorded: ${verdict.reason}`, { kind: 'child-failure' });
      return { stoppedReception: true, recorded: true, verdict };
    }
    abnormal = true;
    ended = true;
    try {
      exit(verdict.code);
    } catch {
      /* the exit is requested regardless */
    }
    return { exit: verdict.code, verdict };
  }

  const api = {
    market,
    stream,
    venue,
    runId,
    start,
    stop,
    close,
    terminate,
    handleChildFailure,

    /** Readiness for the whole run, and an explicit observation hook when it is read. */
    readiness() {
      const snapshot = readiness.snapshot();
      try {
        onReadiness(snapshot);
      } catch {
        /* best-effort */
      }
      return snapshot;
    },
    noteReadiness(role, payload) {
      readiness.noteReport(role, payload);
    },
    get ready() {
      return readiness.snapshot().ready;
    },

    /** The store exclusion registry, for an operator (and for the tests) to inspect. */
    exclusion,
    get children() {
      return { ...children };
    },
    get router() {
      return router;
    },
    get started() {
      return started;
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
    get stats() {
      return {
        market,
        runId,
        started,
        stopped,
        ended,
        closed,
        abnormal,
        roles: Object.fromEntries(
          RUN_ROLES.map((role) => [
            role,
            children[role] === null ? null : { instance: children[role].instance, storePath: children[role].storePath },
          ]),
        ),
        restarts: { ...restarts },
        router: router?.stats ?? null,
      };
    },
  };
  return api;
}

/**
 * Start a run supervisor and run its startup sequence. A convenience for an operator: build, start,
 * and hand back the live run. The startup order, the exclusion and the readiness are all inside.
 */
export async function startRun(options) {
  const supervisor = createRunSupervisor(options);
  const started = await supervisor.start();
  return { supervisor, started };
}
