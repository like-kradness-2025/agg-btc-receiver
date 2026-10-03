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
 * Stop order - the two orders are separate (ruling, §5.8):
 *
 *   processing stop: reception -> organization -> board. Reception seals its final tails first, so
 *   organize can judge "all acknowledged"; organize is then asked to stop accepting frames but is NOT
 *   yet allowed to write the completion; the board is stopped and its result is confirmed.
 *
 *   process termination: the children are closed in a distinct order, and each store is released only
 *   after that child's termination is confirmed.
 *
 * A run is a normal end only when BOTH hold: organize judged all acknowledged AND the board's stop
 * result was confirmed. "All ACK" alone is never a normal end (rulings ⑨⑩).
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
import { readFileSync, writeFileSync, unlinkSync } from 'node:fs';
import { resolve } from 'node:path';

import { openRouter } from './router.mjs';
import { openIngestProcess } from '../ingest/main.mjs';
import { openOrganizeProcess } from '../organize/main.mjs';
import { openBookProcess } from '../book/main.mjs';

export const RUN_ROLES = Object.freeze(['ingest', 'organize', 'book']);

/** The processing stop order: stop what consumes first, write the record last, the board before it. */
export const PROCESSING_STOP_ORDER = Object.freeze(['ingest', 'organize', 'book']);

/** The process termination order: the board is let go first, reception last (it owns the socket). */
export const TERMINATION_ORDER = Object.freeze(['book', 'organize', 'ingest']);

const ROUTE_OPTIONS = Object.freeze({ batchFrames: 1 });

async function until(predicate, { timeoutMs = 10_000, stepMs = 5, label = 'the condition' } = {}) {
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    if (predicate()) return true;
    if (Date.now() >= deadline) {
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
 * A database-unit claim, held in a sidecar file next to the database. `OPEN_STORES` guards one process;
 * this guards the file, which is the unit the ruling names. A claim is refused while the recorded owner
 * is still alive; a stale claim (owner gone) is taken over, which is what makes a crash recoverable.
 * Release requires the old owner's termination to be confirmed - a live owner is never displaced by a
 * claim or a release, so a database cannot acquire a second writer through this path.
 */
export function createStoreExclusion({ isProcessAlive = defaultProcessAlive, nowMs = () => Date.now() } = {}) {
  const lockPathOf = (dbPath) => `${resolve(dbPath)}.owner`;
  const held = new Map(); // resolved db path -> record

  function readRecord(key) {
    try {
      return JSON.parse(readFileSync(`${key}.owner`, 'utf8'));
    } catch {
      return null;
    }
  }

  function writeRecord(key, record) {
    writeFileSync(`${key}.owner`, JSON.stringify(record), 'utf8');
  }

  function clearRecord(key) {
    try {
      unlinkSync(`${key}.owner`);
    } catch {
      /* already gone */
    }
  }

  return {
    claim({ path, role, instance, pid = process.pid }) {
      if (!path) return { claimed: false, code: 'NO_PATH', reason: 'a store claim needs a path' };
      const key = resolve(path);
      const existing = readRecord(key);
      if (existing !== null && isProcessAlive(existing.pid)) {
        return {
          claimed: false,
          code: 'STORE_ALREADY_OWNED',
          reason: `${key} is held by ${existing.role}/${existing.instance} (pid ${existing.pid})`,
          owner: existing,
        };
      }
      const record = { role, instance, pid, claimed_at_ms: nowMs(), file: key };
      writeRecord(key, record);
      held.set(key, record);
      return { claimed: true, path: key, tookOverStale: existing !== null, owner: record };
    },

    /**
     * Release a claim. A release is refused unless the old owner's termination is confirmed
     * (`confirmedTerminated`) or its pid is no longer alive - the whole point is that a database does
     * not change hands while its old writer may still be writing.
     */
    release({ path, instance, confirmedTerminated = false }) {
      const key = resolve(path);
      const existing = held.get(key) ?? readRecord(key);
      if (existing === null) return { released: false, reason: 'this store was not claimed' };
      if (instance !== undefined && existing.instance !== instance) {
        return { released: false, refused: true, reason: 'a different instance owns this store' };
      }
      if (!confirmedTerminated && isProcessAlive(existing.pid)) {
        return { released: false, refused: true, reason: `the owner ${existing.instance} is still alive` };
      }
      clearRecord(key);
      held.delete(key);
      return { released: true, path: key };
    },

    heldBy(path) {
      return readRecord(resolve(path));
    },

    lockPath: (path) => lockPathOf(path),

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
  if (role === 'ingest') return openIngestProcess(options);
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
    routerListenPath,
    webSocketImpl,
    rawWriter = null,
    nowMs = () => Date.now(),
    reportDeadlineMs = 60_000,
    // Stage 5c: how often a role reports its own readiness to the aggregator (via the router). Must be
    // well under `reportDeadlineMs` so a live role is not expired by its own reporting interval.
    readinessIntervalMs = 1_000,
    startupDeadlineMs = 60_000,
    stopDeadlineMs = 10_000,
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
  let completion = null;
  let operationDepth = 0;

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
          roleInstance: instance,
          webSocketImpl,
          organizeSocketPath: router.path,
          ingestStorePath,
          spoolDir,
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
    const dbPath = storePathFor(role);
    if (excludeStores && dbPath) {
      const claim = exclusion.claim({ path: dbPath, role, instance: spec.instance });
      if (!claim.claimed) {
        throw new Error(`store exclusion refused the ${role}: ${claim.reason}`);
      }
      if (claim.tookOverStale) diagnostic(`recovered a stale store claim for the ${role}`, { kind: 'exclusion' });
    }
    const process = await spawner(role, spec);
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

  async function waitForRoles(timeoutMs) {
    await until(() => RUN_ROLES.every((role) => router.channels().has(role)), {
      timeoutMs,
      label: 'all three roles to announce themselves to the supervisor',
    });
    for (const role of RUN_ROLES) readiness.setBoundInstance(role, router.instances().get(role));
  }

  async function waitForAdmission(timeoutMs) {
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
        const ingest = children.ingest?.process;
        if (!ingest || ingest.connectionId == null) return false;
        if (router.pendingAcceptCount !== 0) return false;
        const state = ingest.state;
        return typeof state === 'string' && state !== '' && state !== 'idle' && state !== 'refused';
      },
      { timeoutMs, label: 'the connection to be authorized, adopted and admitted' },
    );
  }

  async function waitForAllAcked(timeoutMs) {
    await until(() => children.organize?.process.allAcked === true, {
      timeoutMs,
      label: 'organize to judge every sealed tail reached',
    });
  }

  // -----------------------------------------------------------------------------------------------
  // Startup.
  // -----------------------------------------------------------------------------------------------

  async function start() {
    return runOperation(async () => {
      if (ended) return { started: false, reason: 'this run has ended' };
      if (started) return { started: true, connectionId: children.ingest?.process.connectionId ?? null };
      try {
        router = await openRouter({
          listenPath: routerListenPath,
          channelOptions: ROUTE_OPTIONS,
          onDiagnostic: (d) => diagnostic(d?.reason ?? String(d), { kind: 'router' }),
          // Stage 5c: the roles' readiness reports are observed by the router and fed to the
          // aggregator here. This closes stage 5b's reservation (the reports were not wired to it).
          onObserved: (observed) => {
            if (observed.type === 'readiness') readiness.noteReport(observed.from, observed.message?.payload ?? {});
          },
        });
        step('router', { path: router.path });

        await spawnRole('organize');
        await spawnRole('book');
        await spawnRole('ingest');
        await waitForRoles(startupDeadlineMs);
        step('bound', { instances: RUN_ROLES.map((r) => router.instances().get(r)) });

        // (a) mark this run live.
        await children.organize.process.beginRun();
        step('a:beginRun', {});

        // (b) hand organize the book's applied boundary - never omitted, even when it is empty.
        const boundary = children.book.process.appliedBoundary;
        const recorded = await children.organize.process.resumeFromBoundary(boundary);
        step('b:boundary', { recorded: recorded.recorded, boundary });

        // (c) the old spool, oldest-first.
        const drained = await children.ingest.process.drainSpool();
        step('c:drainSpool', drained);

        // (d) what is durable and owed goes to the board - before (e), the invariant this wiring fixes.
        const delivered = await children.organize.process.deliverOwed();
        step('d:deliverOwed', delivered);

        // (e) issue the connection; the socket opens only once the book authorized and organize adopted.
        await children.ingest.process.start();
        step('e:accept', {});
        await waitForAdmission(startupDeadlineMs);
        started = true;
        step('admitted', { connectionId: children.ingest.process.connectionId });
        return { started: true, connectionId: children.ingest.process.connectionId };
      } catch (error) {
        abnormal = true;
        diagnostic(`the run could not start: ${error.message}`, { kind: 'startup' });
        return { started: false, reason: error.message };
      }
    });
  }

  // -----------------------------------------------------------------------------------------------
  // Stop: processing order, then termination order, kept apart.
  // -----------------------------------------------------------------------------------------------

  /**
   * Stop processing in the order reception -> organization -> board. Organize keeps its completion
   * capability until the board's stop result is confirmed; the completion is written only when both
   * "all acknowledged" and that confirmation hold. This is the deliberate refusal to call a run a
   * normal end on all-ACK alone.
   */
  async function stop() {
    return runOperation(async () => {
      if (closed) return { stopped: false, reason: 'this run is closed' };
      if (stopped) return { stopped: true, already: true };
      stopped = true;
      const results = { processingOrder: [...PROCESSING_STOP_ORDER] };

      // reception: seal the final tails first, then stop receiving.
      if (children.ingest) {
        results.sealed = await children.ingest.process.sealTails();
        await children.ingest.process.stop();
        step('stop:ingest', results.sealed);
      }
      // organization: judged on the sealed tails; asked to stop accepting, but not to finalize yet.
      try {
        await waitForAllAcked(stopDeadlineMs);
        results.allAcked = true;
      } catch {
        results.allAcked = false;
        diagnostic('the sealed tails were not judged reached before the deadline', { kind: 'stop' });
      }
      if (children.organize) {
        results.organize = await children.organize.process.requestStop('the run is stopping');
        step('stop:organize', results.organize);
      }
      // the board: stopped, and its result confirmed.
      if (children.book) {
        results.book = await children.book.process.stop();
        step('stop:book', results.book);
      }
      const bookConfirmed = results.book?.stopped === true;
      // The completion needs BOTH: the sealed tails reached, and the board's stop confirmed.
      if (bookConfirmed && results.allAcked && children.organize) {
        results.completion = await children.organize.process.finalize();
      } else {
        results.completion = {
          completed: false,
          reason: !bookConfirmed
            ? 'the board stop was not confirmed, so no completion is written'
            : 'not every sealed tail was judged reached',
        };
      }
      completion = results.completion;
      abnormal = results.completion.completed !== true;
      return { stopped: true, ...results, abnormal };
    });
  }

  /** Terminate the child processes in their own order, releasing each store once it is confirmed gone. */
  async function terminate() {
    const order = [...TERMINATION_ORDER];
    for (const role of order) {
      const handle = children[role];
      if (!handle) continue;
      readiness.setConnected(role, false);
      // Null the child first: an intentional close must not read as an abnormal death, and the exit
      // guard is the handle's identity.
      children[role] = null;
      try {
        await handle.process.close();
      } catch (error) {
        diagnostic(`the ${role} could not be closed cleanly: ${error.message}`, { kind: 'terminate' });
      }
      releaseRole(role, { confirmedTerminated: true });
    }
    try {
      router?.close();
    } catch {
      /* the server may already be closed */
    }
    router = null;
    closed = true;
    ended = true;
    return { terminationOrder: order };
  }

  /**
   * The end of the run: a processing stop (unless one was made) followed by the termination order. The
   * two orders are deliberately distinct; the completion was written by `stop`, never here.
   */
  async function close() {
    if (closed) return { closed: true, completed: false, reason: 'this run was already closed' };
    if (!stopped) await stop();
    const termination = await terminate();
    return {
      closed: true,
      terminationOrder: termination.terminationOrder,
      abnormal,
      completed: !abnormal && completion?.completed === true,
      reason: completion?.reason,
    };
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
    get completion() {
      return completion;
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
        completion,
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
