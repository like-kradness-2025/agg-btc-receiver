/**
 * A durability store whose chosen writes fail, so that a change applied in halves can be looked for.
 *
 * Used by the tests that pin what a failed write must leave behind: nothing in memory moved, nothing in
 * the store moved, and the operation can be attempted again. The wrapper only intercepts `db`, so the
 * store's own transaction helper still runs on the same connection.
 */
export function withInjectableWrites(store) {
  const armed = { pattern: null };
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
            return statement.run(...args);
          },
          get: (...args) => statement.get(...args),
          all: (...args) => statement.all(...args),
        };
      },
    },
  };
  return {
    durability: wrapped,
    /** Fail the next write whose SQL matches, once. */
    armWriteFailure: (pattern) => {
      armed.pattern = pattern;
    },
  };
}

export const APPLIED_BOUNDARY_WRITE = /INSERT OR REPLACE INTO applied_boundary/;
export const RETIRED_RUN_WRITE = /INSERT OR REPLACE INTO retired_run/;
export const WATERMARK_WRITE = /INSERT OR REPLACE INTO organized_watermark/;
