'use strict';
const { test } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const { spawn } = require('node:child_process');
const { randomUUID, createHash } = require('node:crypto');
const { ROOT } = require('../bench/options');
const { verifyTranscript } = require('../bench/prove-two-node');

// Synthetic transcripts only test the verifier. The integration below always
// launches actual servers, real sockets, and an external Redis process.
function transcript() {
  const clients = [{ id: 'a', instanceId: 'A' }, { id: 'b', instanceId: 'B' }];
  const p = { channel: 'shared', publishedAt: 123, data: { seq: 0, runId: 'test' }, recipients: ['a', 'b'] };
  const receipts = clients.map((c) => ({ client: c.id,
    message: { type: 'event', channel: p.channel, publishedAt: p.publishedAt, data: { ...p.data }, relayedBy: c.instanceId } }));
  return { clients, publications: [p], receipts };
}
test('verifier accepts the exact cross-instance delivery matrix', () => {
  const f = transcript();
  assert.equal(verifyTranscript(f.publications, f.receipts, f.clients).received, 2);
});
for (const [name, mutate, reason] of [
  ['lost second-node delivery', (f) => f.receipts.pop(), /missing delivery/],
  ['duplicate delivery', (f) => f.receipts.push(f.receipts[0]), /duplicate delivery/],
  ['wrong instance', (f) => { f.receipts[1].message.relayedBy = 'A'; }, /wrong relay instance/],
  ['wrong channel', (f) => { f.receipts[0].message.channel = 'private'; }, /strictly equal/],
  ['changed payload', (f) => { f.receipts[0].message.data.runId = 'other'; }, /payload changed/],
  ['unsubscribed client leaked delivery', (f) => { f.publications[0].recipients = ['a']; }, /unexpected recipient/],
]) {
  test(`verifier rejects ${name}`, () => {
    const f = transcript(); mutate(f);
    assert.throws(() => verifyTranscript(f.publications, f.receipts, f.clients), reason);
  });
}

function cli(args, env = process.env) {
  return new Promise((resolve, reject) => {
    const child = spawn(process.execPath, ['bench/prove-two-node.js', ...args], {
      cwd: ROOT, env, windowsHide: true, stdio: ['ignore', 'pipe', 'pipe'] });
    let stdout = '', stderr = '';
    const timer = setTimeout(() => { child.kill('SIGKILL'); reject(new Error(`proof timeout: ${stdout} ${stderr}`)); }, 30000);
    child.stdout.on('data', (s) => { stdout += s; });
    child.stderr.on('data', (s) => { stderr += s; });
    child.on('error', (e) => { clearTimeout(timer); reject(e); });
    child.on('exit', (code) => { clearTimeout(timer); resolve({ code, stdout, stderr }); });
  });
}

test('mandatory real Redis and two-process proof, including raw evidence', { timeout: 35000 }, async () => {
  const args = ['--redis-host', process.env.REDIS_HOST || '127.0.0.1', '--redis-port', process.env.REDIS_PORT || '6379',
    '--timeout', '3', '--output-dir', `bench/results/ws-1b-test-${randomUUID()}`];
  const output = await cli(args);
  assert.equal(output.code, 0, output.stdout + output.stderr);
  const file = output.stdout.match(/^RESULT (.+)$/m)?.[1]?.trim();
  assert.ok(file, output.stdout);
  const r = JSON.parse(fs.readFileSync(file, 'utf8'));
  assert.equal(r.schemaVersion, 1); assert.equal(r.status, 'PASSED');
  assert.equal(r.provenance.testMutation, null);
  assert.deepEqual(r.failures, []); assert.deepEqual(r.transportErrors, []);
  assert.equal(r.servers.length, 2); assert.notEqual(r.servers[0].pid, r.servers[1].pid);
  assert.notEqual(r.servers[0].instanceId, r.servers[1].instanceId);
  assert.notEqual(r.servers[0].url, r.servers[1].url);
  assert.equal(r.redis.subscribersBefore, 2); assert.equal(r.redis.subscribersAfter, 2);
  assert.equal(r.redis.subscribersAfterCleanup, 0);
  assert.match(r.redis.version, /^\d+\.\d+/);
  assert.equal(r.clients.length, 6); assert.ok(r.clients.every((c) => c.opened));
  assert.equal(r.publications.length, 6);
  assert.ok(r.publications.every((p) => p.redisSubscribers === 2));
  assert.deepEqual(r.accounting, { expected: 13, received: 13, missing: 0, duplicates: 0, unexpected: 0 });
  assert.deepEqual(verifyTranscript(r.publications, r.receipts, r.clients), r.accounting);
  assert.equal(r.byNode.A.received, 6); assert.equal(r.byNode.B.received, 7);
  const ready = r.operations.findIndex((o) => o.action === 'all-six-clients-ready');
  const firstPublish = r.operations.findIndex((o) => o.action === 'publish-once');
  assert.ok(ready >= 0 && firstPublish > ready);
  assert.equal(r.operations.filter((o) => o.action === 'control-acknowledged').length, 9);
  assert.equal(r.operations.filter((o) => o.action === 'stage-verified').length, 3);
  assert.deepEqual(r.provenance.command.argv, ['bench/prove-two-node.js', ...args]);
  assert.equal(r.provenance.sourceChangedDuringRun, false);
  for (const [source, hash] of Object.entries(r.provenance.git.sourceSha256)) {
    assert.equal(createHash('sha256').update(fs.readFileSync(path.join(ROOT, source), 'utf8').replace(/\r\n/g, '\n')).digest('hex'), hash, source);
  }
  assert.ok(fs.readFileSync(file.replace(/\.json$/, '.log'), 'utf8').includes('both-processes-and-six-sockets-still-live'));
});

for (const [mutation, expectedFailure] of [['suppress', /timeout: deliveries/], ['duplicate', /duplicate delivery/]]) {
  test(`real two-server negative control rejects ${mutation} fan-out`, { timeout: 15000 }, async () => {
    const preload = path.join(ROOT, 'tests/fixtures/ws1b-mutant.cjs');
    const output = await cli(['--redis-host', process.env.REDIS_HOST || '127.0.0.1',
      '--redis-port', process.env.REDIS_PORT || '6379', '--timeout', '2',
      '--output-dir', `bench/results/ws-1b-negative-${mutation}-${randomUUID()}`],
    { ...process.env, NODE_OPTIONS: `--require ${JSON.stringify(preload)}`, WS1B_MUTATION: mutation });
    assert.equal(output.code, 1, output.stdout + output.stderr);
    const file = output.stdout.match(/^RESULT (.+)$/m)?.[1]?.trim();
    assert.ok(file, output.stdout + output.stderr);
    const r = JSON.parse(fs.readFileSync(file, 'utf8'));
    assert.equal(r.status, 'FAILED');
    assert.equal(r.provenance.testMutation, mutation);
    assert.match(r.failures.join('; '), expectedFailure);
    assert.equal(r.redis.subscribersBefore, 2, 'failure must occur with both real Redis subscribers running');
    assert.equal(r.redis.subscribersAfterCleanup, 0);
  });
}
