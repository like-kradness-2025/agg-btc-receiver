#!/usr/bin/env node
// Test child for readiness-timeout cleanup: never announces ready and ignores SIGTERM.
import process from 'node:process';

process.on('SIGTERM', () => {});
process.on('message', () => {});
setInterval(() => {}, 60_000);
