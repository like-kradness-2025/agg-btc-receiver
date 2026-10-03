/**
 * A minimal contract-shaped venue adapter for the real-process through test (stage 5c).
 *
 * The child process builds this adapter from a module path (the same seam the role CLI offers for a
 * real deployment). It answers a subscription on the literal `sub-ack` and derives one level change
 * per data frame from the frame's receive sequence, so the through test exercises the whole
 * ingest -> organize -> book path without needing a real venue protocol or a boundary proof.
 *
 * The built-in kraken adapter answers in the split's v1 contract now (a snapshot is a replacement, an
 * update is a diff), so the real-process through test uses it directly (see
 * `test-support/kraken-venue-adapter.mjs`); this synthetic adapter is kept as a minimal
 * contract-shaped example and is no longer a workaround for that gap.
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
