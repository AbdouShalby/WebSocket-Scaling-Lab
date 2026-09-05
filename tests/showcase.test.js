'use strict';
const { test } = require('node:test');
const assert = require('node:assert/strict');
const { ROOT } = require('../bench/options');
const { verify, inventory } = require('../scripts/verify-evidence');
const { tapTotals } = require('../scripts/validate');
test('curated archive verifies original hashes and regenerates successful matrix tables/curves', () => {
  assert.deepEqual(verify(), { files: 111, matrixTrials: 49, completeMatrices: 1 });
});
test('archive verifier rejects changed hashes, missing entries and reclassified failures', () => {
  const index = { schemaVersion: 1, scopes: ['ws-1a', 'ws-1b', 'ws-1c', 'ws-2', 'ws-3'], files: inventory() };
  const badHash = structuredClone(index); badHash.files[0].sha256 = '0'.repeat(64);
  const missing = structuredClone(index); missing.files.pop();
  const reclassified = structuredClone(index); reclassified.files.find((f) => f.status === 'FAILED').status = 'COMPLETE';
  for (const bad of [badHash, missing, reclassified]) assert.throws(() => verify(ROOT, bad), /archive differs/);
});
test('full-suite gate fails closed on skipped, failing, empty or incomplete TAP', () => {
  const good = '# tests 2\n# pass 2\n# fail 0\n# cancelled 0\n# skipped 0\n# todo 0\n';
  assert.equal(tapTotals(good).pass, 2);
  for (const bad of [good.replace('skipped 0', 'skipped 1'), good.replace('fail 0', 'fail 1'),
    good.replace('pass 2', 'pass 1'), good.replace('tests 2', 'tests 0'), '', good + good]) assert.throws(() => tapTotals(bad));
});
