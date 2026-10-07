import { DatabaseSync } from 'node:sqlite';
import { basename, dirname, join, resolve } from 'node:path';
import { realpathSync } from 'node:fs';

const VALID_SCOPES = new Set(['supervisor', 'writer']);

function canonicalStorePath(path) {
  const absolute = resolve(path);
  try {
    return realpathSync(absolute);
  } catch {
    // A new store does not exist yet. Resolve its parent so different working-directory aliases still
    // contend on the same sidecar lock; the role store itself will create the leaf after acquisition.
    return join(realpathSync(dirname(absolute)), basename(absolute));
  }
}

function isBusy(error) {
  return error?.errcode === 5 || error?.code === 'ERR_SQLITE_BUSY' || /database is locked|database table is locked/i.test(error?.message ?? '');
}

/**
 * Acquire an inter-process exclusive lock backed by SQLite's atomic BEGIN IMMEDIATE lock.
 *
 * The lock lives in a small sidecar database distinct from the role's data database. Its transaction
 * remains open for the lease's lifetime, so acquisition is atomic across processes and the OS releases
 * it on process death. `supervisor` leases protect startup ownership; `writer` leases are held by the
 * actual role process as a second fence, so a supervisor crash cannot admit another writer while a
 * disconnected child is still shutting down.
 */
export function acquireStoreLock({ path, role, instance, scope = 'supervisor', Database = DatabaseSync } = {}) {
  if (typeof path !== 'string' || path.length === 0) throw new TypeError('a store lock needs a path');
  if (!VALID_SCOPES.has(scope)) throw new TypeError(`unknown store lock scope ${JSON.stringify(scope)}`);
  const storePath = canonicalStorePath(path);
  const lockPath = `${storePath}.${scope}-lock.sqlite`;
  const db = new Database(lockPath);
  try {
    db.exec('PRAGMA busy_timeout = 0');
    db.exec('BEGIN IMMEDIATE');
  } catch (error) {
    try {
      db.close();
    } catch {
      // Preserve the lock failure; a failed close cannot turn it into a successful acquisition.
    }
    if (isBusy(error)) {
      return {
        acquired: false,
        code: 'STORE_ALREADY_OWNED',
        path: storePath,
        lockPath,
        reason: `${storePath} is held by another ${scope} owner`,
      };
    }
    throw error;
  }

  let released = false;
  return {
    acquired: true,
    path: storePath,
    lockPath,
    role: role ?? null,
    instance: instance ?? null,
    release() {
      if (released) return { released: false, already: true, path: storePath };
      // Only mark it released after SQLite confirms both rollback and close. If either fails the caller
      // keeps the handle and must not hand the store to another owner.
      db.exec('ROLLBACK');
      db.close();
      released = true;
      return { released: true, path: storePath };
    },
  };
}
