# P3-C1 Plan: Pure Market-Local 5min Summary

- Date: 2026-07-13
- Base: Phase C complete, P3-C0 review 97/100 PASS.
- Scope: pure market-local 5min transform from ten complete 30s rows.

## Contract

- One output row per `(market, window_start_ms)`.
- Input: ten consecutive, 30s-aligned `features_30s` rows for one market.
- Start: 5min-aligned; no cross-market join/universe/spot-perp mapping.
- Empty-valid only when all ten rows are valid empty; missing/status errors fail closed.
- Preserve source/provenance/coverage/empty/missing/finalized quality.
- No persistence, pipeline, manifest/checkpoint, consumer, Receiver/raw, cron, or Gateway.
