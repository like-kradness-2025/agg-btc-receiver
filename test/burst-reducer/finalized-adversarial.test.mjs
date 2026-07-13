import assert from 'node:assert/strict';
import { describe, it } from 'node:test';
import { aggregate30s } from '../../lib/burst-reducer/rollup.mjs';

function rows(finalized = true) {
  return Array.from({ length: 30 }, (_, i) => ({
    ts: i * 1000,
    market: 'adversarial',
    burst_count_1s: 0,
    total_burst_notional_1s: 0,
    max_burst_notional_1s: 0,
    max_burst_prints_1s: 0,
    max_burst_duration_ms_1s: 0,
    _quality: { finalized, input_status: 'arrived-valid', has_missing_input: false, coverage: 1 },
  }));
}

describe('P3-C3 finalized/provenance adversarial contract', () => {
  it('rejects missing finalized', () => {
    const input = rows();
    delete input[7]._quality.finalized;
    assert.throws(() => aggregate30s(input), { code: 'E_ROLLUP_INVALID_INPUT_STATUS' });
  });

  it('rejects finalized=false', () => {
    const input = rows(false);
    assert.throws(() => aggregate30s(input), { code: 'E_ROLLUP_INVALID_INPUT_STATUS' });
  });

  it('requires every row to be finalized, not only the first row', () => {
    const input = rows();
    input[29]._quality.finalized = false;
    assert.throws(() => aggregate30s(input), { code: 'E_ROLLUP_INVALID_INPUT_STATUS' });
  });

  it('derives finalized=true only from an all-finalized input window', () => {
    const [output] = aggregate30s(rows(true));
    assert.equal(output._quality.finalized, true);
    assert.equal(output._quality.source_window_count, 30);
  });
});
