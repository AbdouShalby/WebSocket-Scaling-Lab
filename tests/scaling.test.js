'use strict';
const { test } = require('node:test');
const assert = require('node:assert/strict');
const { DeliveryLedger } = require('../bench/accounting');
const { runBenchmark } = require('../bench/runner');
const { defaults } = require('../bench/options');
const { fleetArgs } = require('../bench/run-fleet');
const { matrixArgs } = require('../bench/run-scaling');
const { plan, stats, summarize } = require('../bench/scaling-report');

test('matrix covers all 27 cells and rotates topology order across repeats', () => {
  const jobs = plan([240, 1200, 4800], 3);
  assert.equal(jobs.length, 27);
  assert.equal(new Set(jobs.map((j) => `${j.repetition}/${j.conns}/${j.nodes}`)).size, 27);
  for (const conns of [240, 1200, 4800]) {
    assert.deepEqual(jobs.filter((j) => j.conns === conns).map((j) => j.nodes), [1, 2, 4, 2, 4, 1, 4, 1, 2]);
  }
});
test('fleet validation rejects ambiguous identities and imbalanced workloads', () => {
  for (const args of [['--nodes', '3'], ['--nodes', '2', '--nodes', '4'], ['--nodes'],
    ['--nodes', '4', '--conns', '13'], ['--url', 'ws://localhost'], ['--runtime', 'fake.json']]) {
    assert.throws(() => fleetArgs(args));
  }
  assert.equal(fleetArgs(['--nodes', '4', '--conns', '120', '--channels', '3']).nodes, 4);
});
test('matrix options enforce equal balanced loads and bounded repetitions', () => {
  const c = matrixArgs([]);
  assert.deepEqual(c.connections, [240, 1200, 4800]); assert.equal(c.repeats, 3);
  assert.equal(c.options.measure, 15); assert.equal(c.options.rate, 120);
  for (const args of [['--connections', '12,12,48'], ['--connections', '12,24'], ['--repeats', '0'],
    ['--repeats', '4'], ['--connections', '12,24,47'], ['--repeats', '1', '--repeats', '2'],
    ['--conns', '120'], ['--measure', 'NaN'], ['--output-dir', '../outside']]) assert.throws(() => matrixArgs(args));
});
test('ledger rejects a valid-looking event from the wrong assigned instance', () => {
  const ledger = new DeliveryLedger([{ channel: 'a', instanceId: 'B' }], 'run');
  ledger.begin(0); ledger.issue(0, 'a', 0);
  const event = { type: 'event', channel: 'a', relayedBy: 'A', data: { runId: 'run', phase: 'measurement', seq: 0 } };
  ledger.receive(0, event, 1); ledger.endWindow(10);
  assert.equal(ledger.snapshot().unexpected, 1); assert.equal(ledger.snapshot().missing, 1);
});
test('runner rejects inconsistent or duplicate fleet declarations before networking', async () => {
  const runtime = { schemaVersion: 1, environment: 'fixture', topology: { description: 'fixture', wsInstances: 2 },
    server: { node: 'test', ws: 'test', ioredis: 'test', config: {} } };
  for (const targets of [[{ url: 'ws://localhost:1', instanceId: 'A' }],
    [{ url: 'ws://localhost:1', instanceId: 'A' }, { url: 'ws://localhost:1', instanceId: 'B' }],
    [{ url: 'ws://localhost:1', instanceId: 'A' }, { url: 'ws://localhost:2', instanceId: 'A' }]]) {
    await assert.rejects(runBenchmark(defaults, runtime, { targets }), /distinct targets/);
  }
});
test('aggregation uses all repetitions, not maximum/first-run selection', () => {
  assert.deepEqual(stats([100, 1, 4]), { median: 4, min: 1, max: 100 });
  assert.deepEqual(stats([2, 4]), { median: 3, min: 2, max: 4 });
  assert.throws(() => stats([1, NaN]));
});
test('an incomplete matrix cannot produce successful curves', () => {
  assert.throws(() => summarize([], matrixArgs([])), /incomplete matrix/);
});
