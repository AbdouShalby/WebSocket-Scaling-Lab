'use strict';
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const { randomUUID, createHash } = require('node:crypto');
const { performance, monitorEventLoopDelay } = require('node:perf_hooks');
const Redis = require('ioredis');
const { ROOT, parseArgs } = require('./options');
const { provenance } = require('./connect-storm');
const { startServer } = require('./run-local');
const { runBenchmark } = require('./runner');

function fleetArgs(argv) {
  let nodes = 1, found = false;
  const rest = [];
  for (let i = 0; i < argv.length; i += 2) {
    if (argv[i] === '--nodes') {
      if (found || argv[i + 1] === undefined) throw new Error('duplicate/incomplete nodes');
      found = true; nodes = Number(argv[i + 1]);
    } else {
      if (['--url', '--runtime', '--channel-prefix'].includes(argv[i])) throw new Error('fleet owns target/runtime/namespace');
      rest.push(argv[i], argv[i + 1]);
    }
  }
  const options = parseArgs(rest);
  if (![1, 2, 4].includes(nodes) || options.conns % (options.channels * nodes) !== 0) {
    throw new Error('nodes must be 1/2/4 and connections divisible by channels * nodes');
  }
  return { nodes, options };
}

function capture(argv, runtime) {
  const meta = provenance(argv, runtime);
  for (const file of ['tests/scaling.test.js', 'tests/scaling-integration.test.js']) {
    meta.git.sourceSha256[file] = createHash('sha256').update(fs.readFileSync(path.join(ROOT, file), 'utf8').replace(/\r\n/g, '\n')).digest('hex');
  }
  return meta;
}

async function runFleet(nodes, input, argv) {
  const options = { ...input, 'channel-prefix': `ws1c:${randomUUID()}:` };
  const servers = [], owned = [], lines = [];
  const runtime = { schemaVersion: 1, environment: 'same-host Node controller and independent servers; external real Redis',
    topology: { description: 'direct channel-balanced connections; no nginx; all processes share host resources', wsInstances: nodes }, servers };
  const meta = capture(argv, runtime);
  let result = { schemaVersion: 1, runId: randomUUID(), startedAt: new Date().toISOString(), status: 'FAILED', failures: [], options };
  const fleet = { requestedNodes: nodes, servers };
  const controller = {};
  let redis, cpu, elu, clock, delay;
  const onState = (state) => {
    lines.push(`${new Date().toISOString()} ${state}`);
    if (state === 'MEASURING') {
      cpu = process.cpuUsage(); elu = performance.eventLoopUtilization(); clock = performance.now();
      controller.rssStartBytes = process.memoryUsage().rss;
      delay = monitorEventLoopDelay({ resolution: 10 }); delay.enable();
    } else if ((state === 'DRAINING' || state === 'CLEANUP') && delay) {
      const usage = process.cpuUsage(cpu);
      controller.elapsedMs = performance.now() - clock;
      controller.cpuCorePercent = (usage.user + usage.system) / (controller.elapsedMs * 10);
      controller.eventLoopUtilization = performance.eventLoopUtilization(elu).utilization;
      controller.eventLoopDelayP99Ms = delay.percentile(99) / 1e6;
      controller.rssEndBytes = process.memoryUsage().rss;
      delay.disable(); delay = null;
    }
  };
  const errors = [];
  try {
    redis = new Redis({ host: options['redis-host'], port: options['redis-port'], lazyConnect: true,
      enableOfflineQueue: false, retryStrategy: () => null, maxRetriesPerRequest: 0,
      connectTimeout: options.timeout * 1000, commandTimeout: options.timeout * 1000 });
    redis.on('error', () => errors.push('fleet-Redis-transport-error'));
    await redis.connect();
    for (let index = 0; index < nodes; index++) {
      const s = await startServer(options);
      owned.push(s);
      servers.push({ index, pid: s.pid, url: s.url, instanceId: s.runtime.server.config.instanceId, runtime: s.runtime.server });
    }
    assert.equal(new Set(servers.map((s) => s.pid)).size, nodes);
    assert.equal(new Set(servers.map((s) => s.url)).size, nodes);
    for (const s of servers) { process.kill(s.pid, 0); assert.notEqual(s.pid, process.pid); }
    fleet.subscribersBefore = Number((await redis.pubsub('NUMSUB', options['channel-prefix'] + 'all'))[1]);
    assert.equal(fleet.subscribersBefore, nodes);
    runtime.server = servers[0].runtime;
    options.url = servers[0].url;
    result = await runBenchmark(options, runtime, { onState, targets: servers.map(({ url, instanceId }) => ({ url, instanceId })) });
    for (const s of servers) process.kill(s.pid, 0);
    fleet.subscribersAfter = Number((await redis.pubsub('NUMSUB', options['channel-prefix'] + 'all'))[1]);
    assert.equal(fleet.subscribersAfter, nodes);
    if (result.status === 'COMPLETE') {
      assert.equal(Object.keys(result.accounting.instances).length, nodes);
      for (const s of servers) {
        const observed = result.accounting.instances[s.instanceId];
        assert.equal(observed.clients, options.conns / nodes);
        assert.equal(observed.received, result.publisher.issued * options.conns / options.channels / nodes);
      }
      for (const route of result.routing) {
        assert.equal(route.assigned, options.conns / nodes); assert.equal(route.ready, route.assigned);
      }
    }
  } catch (e) { result.status = 'FAILED'; result.failures.push(e.message); }
  finally {
    onState('CLEANUP-FLEET');
    for (const s of owned) {
      try { await s.stop(); } catch (e) { errors.push(`owned-server-cleanup: ${e.message}`); }
    }
    if (redis?.status === 'ready') {
      try {
        fleet.subscribersAfterCleanup = Number((await redis.pubsub('NUMSUB', options['channel-prefix'] + 'all'))[1]);
        assert.equal(fleet.subscribersAfterCleanup, 0);
      } catch (e) { errors.push(`Redis-cleanup: ${e.message}`); }
    }
    redis?.disconnect();
  }
  result.failures.push(...errors);
  const after = capture(argv, runtime);
  meta.sourceChangedDuringRun = meta.git.sha !== after.git.sha || JSON.stringify(meta.git.sourceSha256) !== JSON.stringify(after.git.sourceSha256);
  if (meta.sourceChangedDuringRun) result.failures.push('source-changed-during-run');
  if (result.failures.length) result.status = 'FAILED';
  Object.assign(result, { completedAt: new Date().toISOString(), milestone: 'WS-1C', provenance: meta, fleet, controllerMeasurement: controller });
  result.methodology = {
    routing: 'client floor(id/channels) modulo nodes; each channel equally represented on every node',
    throughput: 'unique in-window deliveries / actual monotonic seconds; late and duplicates excluded',
    latency: 'same-controller pre-PUBLISH to receive; in-window 1ms histogram; includes controller and Redis costs',
    controller: 'measurement lifecycle interval; CPU percent relative to one core, 10ms event-loop-delay sampler; RSS endpoints are not peak memory',
    limits: 'same host/direct sockets, no nginx, no capacity ceiling or production scaling guarantee',
  };
  const directory = path.resolve(ROOT, options['output-dir']);
  fs.mkdirSync(directory, { recursive: true });
  const name = `${result.startedAt.replace(/[:.]/g, '-')}-${result.runId}`;
  const file = path.join(directory, name + '.json');
  fs.writeFileSync(file, JSON.stringify(result, null, 2) + '\n', { flag: 'wx' });
  fs.writeFileSync(path.join(directory, name + '.log'), lines.join('\n') + '\n' + JSON.stringify({ status: result.status, failures: result.failures, accounting: result.accounting }) + '\n', { flag: 'wx' });
  console.log(`RESULT ${file}`);
  console.log(`${result.status}: ${nodes} nodes, ${options.conns} clients, ${result.accounting?.deliveriesPerSecond ?? 0} deliveries/s`);
  return result;
}
if (require.main === module) {
  (async () => {
    const argv = process.argv.slice(2), { nodes, options } = fleetArgs(argv);
    const r = await runFleet(nodes, options, ['bench/run-fleet.js', ...argv]);
    process.exitCode = r.status === 'COMPLETE' ? 0 : 1;
  })().catch((e) => { console.error(e.message); process.exitCode = 1; });
}
module.exports = { fleetArgs, runFleet, capture };
