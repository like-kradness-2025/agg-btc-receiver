import { test } from 'node:test';
import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';

import { acquireStoreLock } from '../src/supervisor/store-lock.mjs';

const LOCK_HOLDER = fileURLToPath(new URL('../test-support/store-lock-holder.mjs', import.meta.url));

function withDir(fn) {
  const dir = mkdtempSync(join(tmpdir(), 'store-lock-'));
  try {
    return fn(dir);
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
}

test('the supervisor lock is atomic across independent lock handles and releases only explicitly', () => {
  withDir((dir) => {
    const path = join(dir, 'organize.sqlite');
    const first = acquireStoreLock({ path, role: 'organize', instance: 'organize-1', scope: 'supervisor' });
    assert.equal(first.acquired, true);

    const second = acquireStoreLock({ path, role: 'organize', instance: 'organize-2', scope: 'supervisor' });
    assert.equal(second.acquired, false, 'a second owner cannot pass the database lock');
    assert.equal(second.code, 'STORE_ALREADY_OWNED');

    assert.equal(first.release().released, true);
    const third = acquireStoreLock({ path, role: 'organize', instance: 'organize-3', scope: 'supervisor' });
    assert.equal(third.acquired, true, 'the next owner takes the lock after release');
    third.release();
  });
});

test('a writer-process crash releases its SQLite lock only after the process exits', async () => {
  await new Promise((resolve, reject) => {
    const dir = mkdtempSync(join(tmpdir(), 'store-lock-crash-'));
    const path = join(dir, 'book.sqlite');
    const child = spawn(process.execPath, [LOCK_HOLDER, path], { stdio: ['ignore', 'pipe', 'pipe'] });
    let stdout = '';
    let stderr = '';
    let exited = false;
    let lockChecked = false;
    const cleanup = () => rmSync(dir, { recursive: true, force: true });
    const timer = setTimeout(() => {
      if (!exited) child.kill('SIGKILL');
      cleanup();
      reject(new Error(`lock-holder did not become ready; stdout=${stdout}; stderr=${stderr}`));
    }, 10_000);
    child.stdout.on('data', (chunk) => {
      stdout += chunk;
      if (!lockChecked && stdout.includes('lock-held')) {
        lockChecked = true;
        const blocked = acquireStoreLock({ path, role: 'book', instance: 'book-second', scope: 'writer' });
        assert.equal(blocked.acquired, false, 'the live writer retains the cross-process lock');
        child.kill('SIGKILL');
      }
    });
    child.stderr.on('data', (chunk) => {
      stderr += chunk;
    });
    child.on('error', (error) => {
      clearTimeout(timer);
      cleanup();
      reject(error);
    });
    child.on('exit', (code, signal) => {
      exited = true;
      clearTimeout(timer);
      try {
        assert.equal(signal, 'SIGKILL');
        const recovered = acquireStoreLock({ path, role: 'book', instance: 'book-after-crash', scope: 'writer' });
        assert.equal(recovered.acquired, true, 'the OS releases the lock when the actual writer process exits');
        recovered.release();
        cleanup();
        resolve();
      } catch (error) {
        cleanup();
        reject(error);
      }
    });
  });
});
