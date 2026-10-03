/**
 * A minimal contract-shaped venue adapter for the real-process through test (stage 5c).
 *
 * The child process builds this adapter from a module path (the same seam the role CLI offers for a
 * real deployment). It answers a subscription on the literal `sub-ack` and derives one level change
 * per data frame from the frame's receive sequence, so the through test exercises the whole
 * ingest -> organize -> book path without needing a real venue protocol or a boundary proof.
 *
 * The built-in kraken adapter is deliberately not used here: its `changesFor` still returns a bare
 * level array, which the split's `src/changes.mjs` (v1 contract) refuses - a real, separate gap.
 */

export function createAdapter({ url, stream = 'trades' } = {}) {
  return {
    url: url ?? 'fake://venue',
    stream,
    ackMode: 'explicit',
    expectedSubscriptions: ['trades'],
    subscribeMessages: () => ['{"subscribe":"trades"}'],
    heartbeatMessage: () => null,
    parse(raw) {
      const text = Buffer.isBuffer(raw) ? raw.toString('utf8') : String(raw);
      if (text === 'sub-ack') return { kind: 'subscription', key: 'trades', ok: true };
      return { kind: 'data', raw: text };
    },
    changesFor: (envelope) => ({
      replace: false,
      changes: [{ side: 'bid', price: 100 + envelope.receive_seq, size: envelope.receive_seq }],
    }),
  };
}
