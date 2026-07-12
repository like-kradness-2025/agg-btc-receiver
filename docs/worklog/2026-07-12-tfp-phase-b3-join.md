# Worklog: TFP Phase B3 Same-block Join

## State

B2 is complete and pushed (`612e0e3`, review 98/100 PASS). B3-B0 read-only reconnaissance is complete. A sounding-board contract review is in progress before coder execution.

## Evidence

- `pipeline.mjs:252-340`: non-trade path does not parse/apply book content.
- `pipeline.mjs:427-434, 538-545, 668-675`: trade feature calls have no book lookup argument.
- `feature-computer-1s.mjs:19,27`: no book parameter; `book_seeded` is hardcoded false.
- `book-state-machine.mjs:346-358`: strict `stateAt()` exists but has no production caller.

## Decisions

- B3 is join + bookSnapshotAt wiring only.
- B4 owns board candidate schema/columns.
- B5 owns persistent quarantine/checkpoint/manifest policy.
- Existing #13/#14 and trade-only #1-#12 remain unchanged in B3.

## Delegation

- B3-B0 researcher: completed (`deleg_eb52c804`).
- B3 contract sounding-board: running (`deleg_fcc1618b`).
- B3 coder: not started until contract review is incorporated.
