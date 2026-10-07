#!/usr/bin/env node
// Hold a writer-scope lock until the test kills this process.
import process from 'node:process';
import { acquireStoreLock } from '../src/supervisor/store-lock.mjs';

const path = process.argv[2];
const lock = acquireStoreLock({ path, role: 'book', instance: 'book-lock-holder', scope: 'writer' });
if (!lock.acquired) process.exit(2);
process.stdout.write('lock-held\n');
// Keep the lease reachable for the lifetime of this process; the native SQLite handle is owned by it.
setInterval(() => {
  if (!lock.acquired) process.exit(2);
}, 60_000);
