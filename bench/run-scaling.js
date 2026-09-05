'use strict';
const fs = require('node:fs');
const path = require('node:path');
const { spawn } = require('node:child_process');
const { randomUUID, createHash } = require('node:crypto');
const { ROOT, parseArgs } = require('./options');
const { capture } = require('./run-fleet');
const { plan, summarize, markdown, svg } = require('./scaling-report');

function matrixArgs(argv) {
  let connections = [240, 1200, 4800], repeats = 3;
  const rest = [], seen = new Set();
  for (let i = 0; i < argv.length; i += 2) {
    const key = argv[i], value = argv[i + 1];
    if (value === undefined || seen.has(key)) throw new Error('duplicate/incomplete option');
    seen.add(key);
    if (key === '--connections') connections = value.split(',').map(Number);
    else if (key === '--repeats') repeats = Number(value);
    else {
      if (['--nodes', '--conns', '--url', '--runtime', '--channel-prefix'].includes(key)) throw new Error('matrix owns fleet and connections');
      rest.push(key, value);
    }
  }
  const base = ['--channels', '12', '--rate', '120', '--warmup', '2', '--measure', '15', '--drain', '1', '--ramp', '200'];
  const defaults = [];
  for (let i = 0; i < base.length; i += 2) if (!seen.has(base[i])) defaults.push(base[i], base[i + 1]);
  const options = parseArgs([...defaults, ...rest, '--conns', String(Math.max(...connections))]);
  if (connections.length !== 3 || connections.some((n, i) => !Number.isSafeInteger(n) || n <= 0 ||
      n % (options.channels * 4) !== 0 || (i > 0 && n <= connections[i - 1])) ||
      !Number.isSafeInteger(repeats) || repeats < 1 || repeats > 3) {
    throw new Error('three increasing connection levels divisible by channels * 4; repeats 1..3');
  }
  return { connections, repeats, options };
}
function childTrial(args) {
  // Each trial gets a fresh controller process as well as a fresh server fleet.
  return new Promise((resolve, reject) => {
    const child = spawn(process.execPath, ['bench/run-fleet.js', ...args], { cwd: ROOT, windowsHide: true, stdio: ['ignore', 'pipe', 'pipe'] });
    let stdout = '', stderr = '';
    child.stdout.on('data', (s) => { stdout += s; }); child.stderr.on('data', (s) => { stderr += s; });
    child.on('error', reject);
    child.on('close', (code) => resolve({ code, stdout, stderr }));
  });
}
async function runMatrix(config, argv) {
  const startedAt = new Date().toISOString(), id = randomUUID();
  const directory = path.resolve(ROOT, config.options['output-dir'], `scaling-${startedAt.replace(/[:.]/g, '-')}-${id}`);
  fs.mkdirSync(directory, { recursive: true });
  const meta = capture(argv, { description: 'fresh controller and 1/2/4 independent server processes per trial; same host; no nginx' });
  const trials = [], failures = [];
  const report = { schemaVersion: 1, milestone: 'WS-1C', startedAt, config, provenance: meta,
    order: plan(config.connections, config.repeats), artifacts: [] };
  try {
    for (const job of report.order) {
      const opts = { ...config.options, conns: job.conns, 'output-dir': path.relative(ROOT, directory).replace(/\\/g, '/') };
      const args = ['--nodes', String(job.nodes)];
      for (const key of ['conns', 'channels', 'rate', 'ramp', 'warmup', 'measure', 'drain', 'timeout', 'redis-host', 'redis-port', 'output-dir']) args.push(`--${key}`, String(opts[key]));
      const output = await childTrial(args);
      const file = output.stdout.match(/^RESULT (.+)$/m)?.[1]?.trim();
      if (!file) throw new Error(`trial produced no artifact: ${output.stderr}`);
      const raw = fs.readFileSync(file, 'utf8'), result = JSON.parse(raw);
      report.artifacts.push({ ...job, file: path.basename(file), sha256: createHash('sha256').update(raw.replace(/\r\n/g, '\n')).digest('hex') });
      trials.push({ ...job, result });
      console.log(`TRIAL ${trials.length}/${report.order.length} repeat=${job.repetition} clients=${job.conns} nodes=${job.nodes}: ${result.status} ${result.accounting?.deliveriesPerSecond?.toFixed(2) ?? 0}/s`);
      if (output.code !== 0) throw new Error(`trial failed: ${result.failures.join('; ')}`);
    }
    report.rows = summarize(trials, config);
    const after = capture(argv, meta.target);
    meta.sourceChangedDuringRun = meta.git.sha !== after.git.sha || JSON.stringify(meta.git.sourceSha256) !== JSON.stringify(after.git.sourceSha256);
    if (meta.sourceChangedDuringRun) throw new Error('source changed during matrix');
  } catch (e) { failures.push(e.message); }
  report.status = failures.length ? 'FAILED' : 'COMPLETE'; report.failures = failures;
  report.completedAt = new Date().toISOString();
  report.interpretation = 'Fixed total offered workload across process counts. Same-host curves, not capacity or speedup proof. All repetitions retained; median/min/max, not confidence intervals. p99 is median of trial p99 values.';
  fs.writeFileSync(path.join(directory, 'summary.json'), JSON.stringify(report, null, 2) + '\n', { flag: 'wx' });
  if (report.status === 'COMPLETE') {
    fs.writeFileSync(path.join(directory, 'summary.md'), markdown(report.rows), { flag: 'wx' });
    fs.writeFileSync(path.join(directory, 'curves.svg'), svg(report.rows), { flag: 'wx' });
  }
  console.log(`SUMMARY ${path.join(directory, 'summary.json')}`);
  return report;
}
if (require.main === module) {
  (async () => {
    const argv = process.argv.slice(2), config = matrixArgs(argv);
    const report = await runMatrix(config, ['bench/run-scaling.js', ...argv]);
    process.exitCode = report.status === 'COMPLETE' ? 0 : 1;
  })().catch((e) => { console.error(e.message); process.exitCode = 1; });
}
module.exports = { matrixArgs, runMatrix };
