'use strict';
const { test } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const { spawn } = require('node:child_process');
const { randomUUID, createHash } = require('node:crypto');
const { ROOT } = require('../bench/options');
const { summarize, markdown, svg } = require('../bench/scaling-report');

function cli(script, args, env = process.env) {
  return new Promise((resolve, reject) => {
    const child = spawn(process.execPath, [script, ...args], { cwd: ROOT, env, windowsHide: true, stdio: ['ignore', 'pipe', 'pipe'] });
    let stdout = '', stderr = '';
    child.stdout.on('data', (s) => { stdout += s; }); child.stderr.on('data', (s) => { stderr += s; });
    child.on('error', reject); child.on('close', (code) => resolve({ code, stdout, stderr }));
  });
}
const redisArgs = () => ['--redis-host', process.env.REDIS_HOST || '127.0.0.1', '--redis-port', process.env.REDIS_PORT || '6379'];

test('mandatory real Redis 1/2/4-process matrix and regression-sensitive evidence aggregation', { timeout: 90000 }, async () => {
  const args = [...redisArgs(), '--connections', '12,24,48', '--channels', '3', '--rate', '30',
    '--repeats', '1', '--ramp', '48', '--warmup', '0.1', '--measure', '0.3', '--drain', '0.1', '--timeout', '3',
    '--output-dir', `bench/results/ws1c-test-${randomUUID()}`];
  const output = await cli('bench/run-scaling.js', args);
  assert.equal(output.code, 0, output.stdout + output.stderr);
  const file = output.stdout.match(/^SUMMARY (.+)$/m)?.[1]?.trim(); assert.ok(file);
  const summary = JSON.parse(fs.readFileSync(file, 'utf8'));
  assert.equal(summary.status, 'COMPLETE'); assert.equal(summary.artifacts.length, 9);
  assert.deepEqual(summary.provenance.command.argv, ['bench/run-scaling.js', ...args]);
  const directory = path.dirname(file);
  const trials = summary.artifacts.map((entry) => {
    const raw = fs.readFileSync(path.join(directory, entry.file), 'utf8');
    assert.equal(createHash('sha256').update(raw.replace(/\r\n/g, '\n')).digest('hex'), entry.sha256);
    const result = JSON.parse(raw);
    assert.deepEqual(result.socketErrors, {});
    for (const [source, hash] of Object.entries(result.provenance.git.sourceSha256)) {
      assert.equal(createHash('sha256').update(fs.readFileSync(path.join(ROOT, source), 'utf8').replace(/\r\n/g, '\n')).digest('hex'), hash, source);
    }
    assert.ok(fs.readFileSync(path.join(directory, entry.file.replace(/\.json$/, '.log')), 'utf8').includes('MEASURING'));
    assert.ok(result.controllerMeasurement.elapsedMs > 0);
    assert.equal(result.routing.length, entry.nodes);
    for (const route of result.routing) assert.equal(route.ready, entry.conns / entry.nodes);
    return { ...entry, result };
  });
  assert.deepEqual(summarize(trials, summary.config), summary.rows);
  assert.equal(fs.readFileSync(path.join(directory, 'summary.md'), 'utf8'), markdown(summary.rows));
  assert.equal(fs.readFileSync(path.join(directory, 'curves.svg'), 'utf8'), svg(summary.rows));
  for (const mutate of [
    (t) => t.pop(),
    (t) => { t[1] = structuredClone(t[0]); },
    (t) => { t[0].result.status = 'FAILED'; },
    (t) => { t[0].result.options.rate++; },
    (t) => { t[0].result.accounting.deliveriesPerSecond++; },
    (t) => { t[0].result.accounting.missing++; },
    (t) => { t[0].result.fleet.subscribersBefore = 0; },
    (t) => { t[0].result.provenance.git.sha = 'changed'; },
    (t) => { t[0].result.accounting.latencyMs.samples++; },
    (t) => { t[0].result.publisher.eventsPerSecond++; },
  ]) {
    const broken = structuredClone(trials); mutate(broken);
    assert.throws(() => summarize(broken, summary.config));
  }
});
test('real four-node fleet rejects duplicated fan-out rather than graphing inflated throughput', { timeout: 20000 }, async () => {
  const output = await cli('bench/run-fleet.js', [...redisArgs(), '--nodes', '4', '--conns', '12', '--channels', '3',
    '--rate', '30', '--warmup', '0.1', '--measure', '0.3', '--drain', '0.1', '--timeout', '3',
    '--output-dir', `bench/results/ws1c-negative-${randomUUID()}`],
  { ...process.env, NODE_OPTIONS: `--require ${JSON.stringify(path.join(ROOT, 'tests/fixtures/ws1b-mutant.cjs'))}`, WS1B_MUTATION: 'duplicate' });
  assert.equal(output.code, 1, output.stdout + output.stderr);
  const file = output.stdout.match(/^RESULT (.+)$/m)?.[1]?.trim(); assert.ok(file);
  const result = JSON.parse(fs.readFileSync(file, 'utf8'));
  assert.equal(result.fleet.subscribersBefore, 4); assert.equal(result.fleet.subscribersAfterCleanup, 0);
  assert.ok(result.failures.includes('duplicate-deliveries')); assert.ok(result.accounting.duplicates > 0);
});
