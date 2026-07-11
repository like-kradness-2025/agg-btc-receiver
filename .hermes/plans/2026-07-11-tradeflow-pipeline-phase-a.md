# TradeFlow Pipeline Phase A Implementation Plan

> **For Hermes:** Use subagent-driven-development skill to implement this plan task-by-task.

この workspace の正式計画は `docs/specs/plan-2026-07-11-tradeflow-pipeline-phase-a.md` を正とする。

**Goal:** Receiver の後工程を一つの **TradeFlow Pipeline (TFP)** worker として整理し、burst/feature/将来 rollup を内部 stage として扱う。その前提となる burst reducer の安全性不足（retention、recovery、manifest、cursor/lock tests）を TDD で解消する。

**Scope:** Phase A のみ。raw input、Receiver、cron、本番 output root、rollup output schema は変更しない。

**Tasks:**
1. closed burst bounded retention
2. unused full-state clone removal
3. fail-closed recovery
4. corrupt manifest preservation
5. cursor skip and lock tests
6. full verification and independent review gate

**Acceptance:** `docs/specs/plan-2026-07-11-tradeflow-pipeline-phase-a.md` の各タスクの受入条件、全テスト、95点レビューを満たすこと。
