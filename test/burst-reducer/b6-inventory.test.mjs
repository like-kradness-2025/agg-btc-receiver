// test/burst-reducer/b6-inventory.test.mjs — frozen inventory kind separation

import assert from 'node:assert/strict';
import { describe, it, before, after } from 'node:test';
import { mkdirSync, rmSync, writeFileSync } from 'node:fs';
import { join } from 'node:path';
import {
  validateInventoryEntry,
  validateInventoryCrossReferences,
  loadAndValidateFrozenInventory,
} from '../../scripts/tfp.mjs';
import { reconcileMarketState } from '../../lib/burst-reducer/recovery.mjs';

const ROOT = join('test', 'fixtures', 'b6-inventory');
const INVENTORY = join(ROOT, 'inventory.json');
const DERIVED = join(ROOT, 'derived');

const entry = (market, kind, block) => ({
  market,
  kind,
  block_start_ms: block,
  path: `${kind}/${market}/1970-01-01/${block === 0 ? '00-00-00' : '00-00-30'}.jsonl`,
  sha256: '',
});

before(() => { rmSync(ROOT, { recursive: true, force: true }); mkdirSync(ROOT, { recursive: true }); });
after(() => { rmSync(ROOT, { recursive: true, force: true }); });

describe('B6 frozen inventory separation', () => {
  it('rejects path-kind mismatch directly', () => {
    const errors = validateInventoryEntry({
      ...entry('alpha', 'book_updates', 0),
      path: 'trades/alpha/1970-01-01/00-00-00.jsonl',
    }, 0);
    assert.ok(errors.some((e) => /leading directory.*does not match kind/i.test(e)));
  });

  it('accepts multiple markets and kinds without cross-kind collision', () => {
    const entries = [
      entry('alpha', 'trades', 0),
      entry('alpha', 'book_updates', 0),
      entry('beta', 'trades', 0),
      entry('beta', 'book_updates', 0),
      entry('alpha', 'trades', 30000),
      entry('alpha', 'book_updates', 30000),
    ];
    assert.deepEqual(validateInventoryCrossReferences(entries), []);
    writeFileSync(INVENTORY, JSON.stringify(entries));
    const loaded = loadAndValidateFrozenInventory(INVENTORY);
    assert.ok(loaded);
    assert.equal(loaded.errors.length, 0);
    assert.equal(loaded.byKindAndMarket.get('trades').get('alpha').get(0).kind, 'trades');
    assert.equal(loaded.byKindAndMarket.get('book_updates').get('alpha').get(0).kind, 'book_updates');
    assert.equal(loaded.byKindAndMarket.get('trades').get('beta').get(0).kind, 'trades');
    assert.equal(loaded.byKindAndMarket.get('book_updates').get('beta').get(0).kind, 'book_updates');
    assert.equal(loaded.byKindAndMarket.get('trades').get('alpha').get(60000), undefined);
    assert.equal(loaded.byKindAndMarket.get('book_updates').get('alpha').get(60000), undefined);
  });

  it('detects duplicate only within the same market-kind-block tuple', () => {
    const sameKind = [entry('alpha', 'trades', 0), entry('alpha', 'trades', 0)];
    assert.equal(validateInventoryCrossReferences(sameKind).length, 1);
    const crossKind = [entry('alpha', 'trades', 0), entry('alpha', 'book_updates', 0)];
    assert.deepEqual(validateInventoryCrossReferences(crossKind), []);
  });

  it('returns corrupt-checkpoint for an invalid book_updates checkpoint', () => {
    const cpDir = join(DERIVED, 'manifests', 'checkpoints');
    mkdirSync(cpDir, { recursive: true });
    writeFileSync(join(cpDir, 'alpha.book_updates.json'), '{not-json');
    const result = reconcileMarketState('alpha', DERIVED, 'book_updates');
    assert.equal(result.status, 'corrupt-checkpoint');
    assert.equal(result.cursor, null);
    assert.equal(result.generation, 0);
    assert.ok(result.errors.some((e) => /corrupt-checkpoint/i.test(e)));
  });

  it('does not use book_updates checkpoint when recovering trades', () => {
    const cpDir = join(DERIVED, 'manifests', 'checkpoints');
    mkdirSync(cpDir, { recursive: true });
    writeFileSync(join(cpDir, 'alpha.book_updates.json'), JSON.stringify({ kind: 'book_updates', generation: 7 }));
    const result = reconcileMarketState('alpha', DERIVED, 'trades');
    assert.equal(result.cursor, null);
    assert.equal(result.generation, 0);
  });
});
