'use strict';
const { test } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs'), path = require('node:path');
const { spawn } = require('node:child_process');
const { randomUUID, createHash } = require('node:crypto');
const { ROOT } = require('../bench/options');
async function run(scenario, mutation) {
  const args = ['bench/prove-guards.js', '--scenario', scenario, '--output-dir', `bench/results/ws3-test-${randomUUID()}`];
  const env = { ...process.env };
  if (mutation) { env.WS3_MUTATION = mutation; env.NODE_OPTIONS = `--require ${JSON.stringify(path.join(ROOT, 'tests/fixtures/ws3-mutant.cjs'))}`; }
  return new Promise((resolve, reject) => {
    const child = spawn(process.execPath, args, { cwd: ROOT, env, stdio: ['ignore', 'pipe', 'pipe'] });
    let stdout = '', stderr = '';
    child.stdout.on('data', (b) => { stdout += b; }); child.stderr.on('data', (b) => { stderr += b; });
    child.on('error', reject); child.on('close', (code) => resolve({ code, stdout, stderr, args }));
  });
}
function artifact(out) {
  const file = out.stdout.match(/^RESULT (.+)$/m)?.[1]?.trim(); assert.ok(file, out.stdout + out.stderr);
  return JSON.parse(fs.readFileSync(file, 'utf8'));
}
test('mandatory real WS-3 protocol, origin, rate, readiness and telemetry proof', { timeout: 60000 }, async () => {
  const out = await run('all'); assert.equal(out.code, 0, out.stdout + out.stderr);
  const r = artifact(out); assert.equal(r.status, 'PASSED');
  assert.equal(r.provenance.testMutation, null); assert.equal(r.provenance.sourceChangedDuringRun, false);
  assert.deepEqual(r.provenance.command.argv, out.args);
  assert.deepEqual(r.scenarios.map((s) => s.scenario), ['ws3-security', 'ws3-rate', 'ws3-readiness', 'ws3-subscription-ack']);
  for (const s of r.scenarios) {
    assert.equal(s.status, 'PASSED'); assert.deepEqual(s.failures, []);
    assert.equal(s.cleanup.allOwnedProcessesExited, true); assert.ok(s.processes.every((p) => p.exit));
    assert.ok(s.processes.some((p) => p.role === 'redis' && /^\d+\./.test(p.runtime.version)));
    for (const publication of s.publications) {
      const seen = s.receipts.filter((x) => x.id === publication.id);
      assert.equal(new Set(seen.map((x) => x.client)).size, seen.length);
      for (const c of publication.required) assert.ok(seen.some((x) => x.client === c));
      assert.ok(seen.every((x) => publication.allowed.includes(x.client)));
    }
  }
  for (const [source, hash] of Object.entries(r.provenance.git.sourceSha256)) {
    assert.equal(createHash('sha256').update(fs.readFileSync(path.join(ROOT, source), 'utf8').replace(/\r\n/g, '\n')).digest('hex'), hash, source);
  }
});
for (const [mutation, scenario, reason] of [
  ['origin', 'security', /upgrade rejection/], ['schema', 'security', /protocol reply|undefined/],
  ['rate', 'rate', /rate-limit close/], ['ready', 'subscription-ack', /readiness=false/],
]) test(`real negative control detects disabled ${mutation}`, { timeout: 30000 }, async () => {
  const out = await run(scenario, mutation); assert.equal(out.code, 1, out.stdout + out.stderr);
  const r = artifact(out); assert.equal(r.status, 'FAILED'); assert.equal(r.provenance.testMutation, mutation);
  assert.match(r.scenarios[0].failures.join(';'), reason);
  assert.equal(r.scenarios[0].cleanup.allOwnedProcessesExited, true);
});
