# TFP Phase C2 Tasks

1. [researcher] C2-B0 wiring/recovery reconnaissance (complete).
2. [coder] Implement a 30s-only writer with isolated shard and manifest/checkpoint namespace.
3. [coder] Wire only after durable 1s commit; preserve normal/gap/EOF and book_updates paths.
4. [coder] Add focused tests for normal, gap/missing, EOF partial, restart/idempotency, root isolation.
5. [parent] Re-read allowlist; run focused/relevant/full/static checks and real isolated-root probe.
6. [reviewer] Independent adversarial review >=95.
7. [parent] Commit/push only after PASS.

Stop on schema ambiguity, namespace collision, partial output, 1s regression, or default-root mutation.
