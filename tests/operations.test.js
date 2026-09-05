'use strict';
const { test } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs'), path = require('node:path');
const { spawn } = require('node:child_process');
const { randomUUID, createHash } = require('node:crypto');
const { ROOT } = require('../bench/options');
const { options } = require('../bench/prove-operations');
function run(scenario, mutation) {
  return new Promise((resolve, reject) => {
    const env = { ...process.env };
    if (mutation) { env.WS2_MUTATION = mutation; env.NODE_OPTIONS = `--require ${JSON.stringify(path.join(ROOT, 'tests/fixtures/ws2-mutant.cjs'))}`; }
    const args = ['bench/prove-operations.js', '--scenario', scenario, '--output-dir', `bench/results/ws2-test-${randomUUID()}`];
    const child = spawn(process.execPath, args, { cwd: ROOT, env, stdio: ['ignore', 'pipe', 'pipe'] });
    let stdout = '', stderr = '';
    child.stdout.on('data', (s) => { stdout += s; }); child.stderr.on('data', (s) => { stderr += s; });
    child.on('error', reject); child.on('close', (code) => resolve({ code, stdout, stderr, args }));
  });
}
function artifact(output) {
  const file = output.stdout.match(/^RESULT (.+)$/m)?.[1]?.trim(); assert.ok(file, output.stdout + output.stderr);
  return { file, r: JSON.parse(fs.readFileSync(file, 'utf8')) };
}
test('operational options reject unsafe paths and unknown scenarios', () => {
  for (const args of [['--output-dir', '../outside'], ['--scenario', 'fake'], ['--scenario'], ['--redis-port', '6379']]) assert.throws(() => options(args));
});
test('mandatory real POSIX Redis/process/drain/backpressure scenarios with raw evidence', { timeout: 90000 }, async () => {
  const out = await run('all'); assert.equal(out.code, 0, out.stdout + out.stderr);
  const { file, r } = artifact(out);
  assert.equal(r.status, 'PASSED'); assert.equal(r.provenance.testMutation, null);
  assert.equal(r.provenance.sourceChangedDuringRun, false); assert.notEqual(r.provenance.controller.platform, 'win32');
  assert.deepEqual(r.provenance.command.argv, out.args);
  assert.deepEqual(r.scenarios.map((s) => s.scenario), ['recovery', 'crash', 'drain', 'forced-drain', 'backpressure']);
  for (const s of r.scenarios) {
    assert.equal(s.status, 'PASSED'); assert.deepEqual(s.failures, []); assert.equal(s.cleanup.allOwnedProcessesExited, true);
    assert.ok(s.processes.every((p) => p.exit));
    assert.ok(s.processes.some((p) => p.role === 'redis' && /^\d+\./.test(p.runtime.version)));
    for (const p of s.publications) {
      const receipts = s.receipts.filter((x) => x.id === p.id);
      assert.equal(new Set(receipts.map((x) => x.client)).size, receipts.length);
      for (const required of p.required) assert.ok(receipts.some((x) => x.client === required));
      assert.ok(receipts.every((x) => p.allowed.includes(x.client)));
    }
  }
  for (const [source, hash] of Object.entries(r.provenance.git.sourceSha256)) assert.equal(createHash('sha256').update(fs.readFileSync(path.join(ROOT, source), 'utf8').replace(/\r\n/g, '\n')).digest('hex'), hash, source);
  assert.ok(fs.readFileSync(file.replace(/\.json$/, '.log'), 'utf8').includes('outage-publish-rejected'));
});
for (const [scenario, reason] of [['recovery', /Redis subscribers=2/], ['drain', /1006.*1001|1006 !== 1001/s], ['backpressure', /paused TCP peer/]]) {
  test(`real negative control detects broken ${scenario}`, { timeout: 60000 }, async () => {
    const out = await run(scenario, scenario); assert.equal(out.code, 1, out.stdout + out.stderr);
    const { r } = artifact(out); assert.equal(r.status, 'FAILED'); assert.equal(r.provenance.testMutation, scenario);
    assert.match(r.scenarios[0].failures.join('; '), reason);
    assert.equal(r.scenarios[0].cleanup.allOwnedProcessesExited, true);
  });
}
