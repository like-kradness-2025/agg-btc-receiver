# TFP Phase B3 Tasks

## B3-0 — Contract/recon (done)

- [x] Trace `pipeline.mjs`, `feature-computer-1s.mjs`, `schema.mjs`, `BookStateMachine.stateAt()`.
- [x] Confirm no production book caller exists.
- [x] Exclude B4 board fields and B5 persistence from this phase.

## B3-1 — Pure join contract (coder)

- [ ] Define explicit same-block trade/book identity and input shape.
- [ ] Define `bookSnapshotAt(anchor_ts_ms)` using strict `<`.
- [ ] Define unavailable/quarantine metadata without zero substitution.
- [ ] Add independent RED verifier before implementation.

## B3-2 — Production wiring (coder)

- [ ] Read paired book input at the existing pipeline boundary.
- [ ] Pass optional book lookup into 1s computation without changing #13/#14.
- [ ] Keep trade-only path/output backward compatible.

## B3-3 — Parent verification

- [ ] Read every changed file and trace caller → consumer.
- [ ] Run focused join/anchor tests, existing book tests, full npm test.
- [ ] Run positive/negative probes for missing, malformed, crossed, and same-anchor events.

## B3-4 — Review gate

- [ ] Independent reviewer delegate, scope-limited to B3.
- [ ] Score >=95; otherwise FIX child and repeat.
