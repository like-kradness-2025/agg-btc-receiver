/**
 * A durability store whose chosen writes fail, so that a change applied in halves can be looked for.
 *
 * Used by the tests that pin what a failed write must leave behind: nothing in memory moved, nothing in
 * the store moved, and the operation can be attempted again. The wrapper intercepts `db` - the statements
 * only - and re-binds itself to the store's private routes: a module takes the execution right, the
 * transaction discipline and the observation of the right from the wiring, never from the object it is
 * handed, so a wrapper that is not bound cannot be opened by a module at all.
 */
import { bindInternals, internalsOf } from '../src/internal/wiring.mjs';

export function withInjectableWrites(store) {
  const armed = { pattern: null };
  const watchers = { onWrite: null };
  const wrapped = {
    ...store,
    db: {
      exec: (sql, ...rest) => store.db.exec(sql, ...rest),
      prepare: (sql, ...rest) => {
        const statement = store.db.prepare(sql, ...rest);
        return {
          run: (...args) => {
            if (armed.pattern && armed.pattern.test(sql)) {
              armed.pattern = null;
              throw new Error('injected write failure');
            }
            // A write is the innermost point a test can reach: it is where the store is already inside the
            // operation that owns it, so it is where a re-entrant call can be driven from.
            if (watchers.onWrite) watchers.onWrite(sql, args);
            return statement.run(...args);
          },
          get: (...args) => statement.get(...args),
          all: (...args) => statement.all(...args),
        };
      },
    },
  };
  bindInternals(wrapped, internalsOf(store));
  return {
    durability: wrapped,
    /** Fail the next write whose SQL matches, once. */
    armWriteFailure: (pattern) => {
      armed.pattern = pattern;
    },
    /** Watch every write. The callback runs inside the transaction that owns it. */
    onWrite: (fn) => {
      watchers.onWrite = fn;
    },
  };
}

export const APPLIED_BOUNDARY_WRITE = /INSERT OR REPLACE INTO applied_boundary/;
export const RETIRED_RUN_WRITE = /INSERT OR REPLACE INTO retired_run/;
export const WATERMARK_WRITE = /INSERT OR REPLACE INTO organized_watermark/;
export const LEDGER_INTENT_WRITE = /INSERT OR IGNORE INTO delivery_ledger/;
export const LEDGER_CONFIRM_WRITE = /UPDATE delivery_ledger SET state/;
