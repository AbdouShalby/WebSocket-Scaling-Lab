'use strict';
// Mandatory real Redis, one real server, real simultaneous sockets and CLI artifacts.
// No skip and no in-memory substitute. Test-generated metadata/results stay ignored.
const { test } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const { randomUUID, createHash } = require('node:crypto');
const { spawn, execFileSync } = require('node:child_process');
const { ROOT } = require('../bench/options');

function cli(script, args) {
  return new Promise((resolve, reject) => {
    const child = spawn(process.execPath, [script, ...args], { cwd: ROOT, windowsHide: true, stdio: ['ignore', 'pipe', 'pipe'] });
    let stdout = '', stderr = '';
    const deadline = setTimeout(() => { child.kill('SIGKILL'); reject(new Error(`CLI timeout: ${stdout} ${stderr}`)); }, 20000);
    child.stdout.on('data', (s) => { stdout += s; });
    child.stderr.on('data', (s) => { stderr += s; });
    child.on('error', (e) => { clearTimeout(deadline); reject(e); });
    child.on('exit', (code) => { clearTimeout(deadline); resolve({ code, stdout, stderr }); });
  });
}
function artifact(output) {
  const file = output.stdout.match(/^RESULT (.+)$/m)?.[1]?.trim();
  assert.ok(file, output.stdout + output.stderr);
  return { file, result: JSON.parse(fs.readFileSync(file, 'utf8')) };
}

test('WS-1A CLI: real Redis, complete result/provenance, then refused-target failure artifact', { timeout: 40000 }, async () => {
  const outputDir = `bench/results/integration-${randomUUID()}`;
  const args = ['--redis-host', process.env.REDIS_HOST || '127.0.0.1', '--redis-port', process.env.REDIS_PORT || '6379',
    '--conns', '12', '--channels', '3', '--ramp', '4', '--rate', '30', '--measure', '1',
    '--warmup', '0.2', '--drain', '0.2', '--timeout', '3', '--output-dir', outputDir];
  const output = await cli('bench/run-local.js', args);
  assert.equal(output.code, 0, output.stdout + output.stderr);
  const { file, result: r } = artifact(output);
  assert.equal(r.schemaVersion, 1); assert.equal(r.status, 'COMPLETE');
  assert.deepEqual(r.failures, []);
  assert.equal(r.connections.ready, 12); assert.equal(r.connections.failed, 0);
  assert.equal(r.connections.disconnected, 0);
  assert.equal(r.accounting.expected, r.publisher.issued * 4);
  assert.equal(r.accounting.received, r.accounting.expected);
  assert.equal(r.accounting.missing, 0); assert.equal(r.accounting.duplicates, 0);
  assert.equal(r.accounting.received, r.accounting.inWindow + r.accounting.late);
  assert.equal(r.accounting.deliveriesPerSecond, r.accounting.inWindow * 1000 / r.accounting.elapsedMs);
  assert.equal(r.accounting.latencyMs.samples, r.accounting.inWindow);
  assert.equal(Object.keys(r.accounting.instances).length, 1);
  assert.equal(Object.keys(r.accounting.instances)[0], r.provenance.target.server.config.instanceId);
  assert.match(r.publisher.redisVersion, /^\d+\.\d+/);
  assert.equal(r.provenance.git.sha, execFileSync('git', ['rev-parse', 'HEAD'], { cwd: ROOT, encoding: 'utf8' }).trim());
  for (const [source, hash] of Object.entries(r.provenance.git.sourceSha256)) {
    assert.equal(hash, createHash('sha256').update(fs.readFileSync(path.join(ROOT, source), 'utf8').replace(/\r\n/g, '\n')).digest('hex'), source);
  }
  assert.deepEqual(r.provenance.command.argv, ['bench/run-local.js', ...args]);
  assert.equal(r.provenance.sourceChangedDuringRun, false);
  assert.ok(Date.parse(r.completedAt) >= Date.parse(r.startedAt));
  assert.match(path.basename(file), /^\d{4}-\d{2}-\d{2}T/);
  assert.ok(fs.readFileSync(file.replace(/\.json$/, '.log'), 'utf8').includes('MEASURING'));

  const runtimeFile = path.join(ROOT, outputDir, 'runtime.json');
  fs.writeFileSync(runtimeFile, JSON.stringify(r.provenance.target), { flag: 'wx' });
  // run-local has stopped its server; this explicitly tests a refused target,
  // not a node-kill/recovery experiment.
  const failure = await cli('bench/connect-storm.js', [...args, '--url', r.options.url, '--runtime', runtimeFile]);
  assert.equal(failure.code, 1, failure.stdout + failure.stderr);
  const f = artifact(failure).result;
  assert.equal(f.status, 'FAILED'); assert.equal(f.connections.ready, 0);
  assert.equal(f.connections.failed, 12); assert.equal(f.publisher.issued, 0);
  assert.ok(f.failures.includes('socket-error'));
  const invalid = await cli('bench/connect-storm.js', ['--conns', '0']);
  assert.equal(invalid.code, 1);
});
