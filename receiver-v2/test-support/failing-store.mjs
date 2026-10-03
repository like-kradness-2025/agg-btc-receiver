/**
 * A store whose chosen writes fail, so that a change applied in halves can be looked for.
 *
 * The interception is at the database, but it is installed through the wiring: a module runs its statements
 * through the database it is handed on the private side (`internalsOf(store).db`), so a test that wrapped
 * the store object alone would be wrapping something nothing reads. The wrapper re-binds itself to the same
 * private side with that database replaced, and the store's own transaction helper still runs on the same
 * connection.
 */
import { bindInternals, internalsOf } from '../src/internal/wiring.mjs';

export function withInjectableWrites(store) {
  const armed = { pattern: null, commit: null };
  const watchers = { onWrite: null };
  const internal = internalsOf(store);
  const db = {
    exec: (sql, ...rest) => internal.db.exec(sql, ...rest),
    prepare: (sql, ...rest) => {
      const statement = internal.db.prepare(sql, ...rest);
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
  };
  // The commit itself, refused on request. An INSERT failure cannot show what a rollback does to the
  // process's own memory: there the write never landed, so nothing was ever followed. Refusing the commit
  // is the case where the writes did land inside the transaction and are taken back afterwards.
  const inTransaction = (fn) => {
    const refuse = armed.commit;
    armed.commit = null;
    internal.db.exec('BEGIN IMMEDIATE');
    try {
      const result = fn();
      if (refuse) throw new Error('injected write failure');
      internal.db.exec('COMMIT');
      return result;
    } catch (error) {
      try {
        internal.db.exec('ROLLBACK');
      } catch {
        // the original failure is the one to report
      }
      throw error;
    }
  };
  const wrapped = { ...store };
  bindInternals(wrapped, { ...internal, db, inTransaction });
  return {
    durability: wrapped,
    /** Fail the next write whose SQL matches, once. */
    armWriteFailure: (pattern) => {
      armed.pattern = pattern;
    },
    /** Refuse the next commit, once: the transaction's writes are rolled back after they ran. */
    armCommitFailure: () => {
      armed.commit = true;
    },
    /** Watch every write. The callback runs inside the transaction that owns it. */
    onWrite: (fn) => {
      watchers.onWrite = fn;
    },
  };
}

export const APPLIED_BOUNDARY_WRITE = /INSERT OR REPLACE INTO applied_boundary/;
export const BOOK_MISSING_WRITE = /INSERT OR REPLACE INTO book_missing_record/;
export const RETIRED_RUN_WRITE = /INSERT OR REPLACE INTO retired_run/;
export const WATERMARK_WRITE = /INSERT OR REPLACE INTO organized_watermark/;
export const LEDGER_INTENT_WRITE = /INSERT OR IGNORE INTO delivery_ledger/;
export const LEDGER_CONFIRM_WRITE = /UPDATE delivery_ledger SET state/;
