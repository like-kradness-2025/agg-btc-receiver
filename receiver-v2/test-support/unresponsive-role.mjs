#!/usr/bin/env node
// Test child for process-spawner shutdown tests: announces readiness, then ignores commands and SIGTERM.
// The parent must not treat its SIGKILL request as proof of exit; only the real `exit` event counts.
import process from 'node:process';

process.on('SIGTERM', () => {});
process.on('message', () => {});
process.send?.({ kind: 'ready', role: process.argv[2], methods: [], props: [] });
setInterval(() => {}, 60_000);
