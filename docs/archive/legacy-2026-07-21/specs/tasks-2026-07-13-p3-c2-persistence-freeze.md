# P3-C2 Persistence Tasks

1. [approval] Freeze features_5min path and idempotency key (hash conflict → quarantine).
2. [approval] Freeze manifest/checkpoint namespace and schemas.
3. [approval] Freeze finalized-through/EOF authority (upstream eof required, inference forbidden).
4. [approval] Freeze recovery rules (orphan cleanup, source-referenced reconciliation, hash verification).
5. [approval] Freeze consumer/index contract (committed-only manifest reader, no dashboard wiring).
6. [approval] Freeze state isolation (never touch 1s/30s namespace).
7. [reviewer] Cross-document contract review ≥95.
8. [coder] Only after approval: dedicated 5min committer + adversarial tests.
9. [coder] Later: pipeline wiring after durable 30s commit.
10. [coder] Later: consumer module after committer is stable.

Blocked until tasks 1–6 are approved.
