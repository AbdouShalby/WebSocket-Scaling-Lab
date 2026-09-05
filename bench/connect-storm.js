'use strict';
const fs = require('node:fs');
const path = require('node:path');
const os = require('node:os');
const { createHash } = require('node:crypto');
const { execFileSync } = require('node:child_process');
const { ROOT, parseArgs } = require('./options');
const { runBenchmark } = require('./runner');

function provenance(argv, runtime) {
  const git = (...args) => execFileSync('git', args, { cwd: ROOT, encoding: 'utf8' }).trim();
  const files = git('ls-files', '--cached', '--others', '--exclude-standard').split('\n')
    .filter((f) => /^(src\/|bench\/.*\.js$|package(-lock)?\.json$|Dockerfile$|docker-compose\.yml$|nginx\.conf$)/.test(f));
  // Git checkouts may use CRLF on Windows: hash normalized source text for portability.
  const hashes = Object.fromEntries(files.sort().map((f) => [f, createHash('sha256')
    .update(fs.readFileSync(path.join(ROOT, f), 'utf8').replace(/\r\n/g, '\n')).digest('hex')]));
  return {
    command: { executable: 'node', argv, cwd: 'repository-root' },
    git: { sha: git('rev-parse', 'HEAD'), dirty: Boolean(git('status', '--porcelain')),
      sourceHashEncoding: 'UTF-8 with CRLF normalized to LF', sourceSha256: hashes },
    controller: { node: process.version, ws: require('ws/package.json').version,
      ioredis: require('ioredis/package.json').version, platform: process.platform,
      release: os.release(), arch: process.arch, cpu: os.cpus()[0]?.model,
      logicalCpus: os.cpus().length, totalMemoryBytes: os.totalmem() },
    target: runtime,
  };
}

async function execute(options, runtime, argv) {
  const meta = provenance(argv, runtime);
  const lines = [];
  const result = await runBenchmark(options, runtime, { onState: (s) => {
    const line = `${new Date().toISOString()} ${s}`;
    lines.push(line); console.log(line);
  } });
  const after = provenance(argv, runtime);
  meta.sourceChangedDuringRun = meta.git.sha !== after.git.sha ||
    JSON.stringify(meta.git.sourceSha256) !== JSON.stringify(after.git.sourceSha256);
  if (meta.sourceChangedDuringRun) {
    result.status = 'FAILED';
    result.failures.push('source-changed-during-run');
  }
  result.provenance = meta;
  result.methodology = {
    latency: 'controller monotonic time before Redis publish to the same controller receiving a unique event; in-window only; 1ms histogram',
    throughput: 'unique in-window deliveries / actual monotonic measurement seconds; late/duplicates excluded',
    expected: 'issued measurement events times assigned subscribers, including uncertain/failed publishes',
    boundary: 'warmup reset; bounded post-measurement drain counts late and missing; no latency samples in drain',
    limitations: ['single controller/publisher process shares CPU and event loop with clients',
      'target metadata is declared by operator or captured by run-local; instance IDs are observations, not multi-node proof',
      'complete is a finite benchmark result, not a delivery guarantee; after-drain arrivals are missing'],
  };
  const directory = path.resolve(ROOT, options['output-dir']);
  fs.mkdirSync(directory, { recursive: true });
  const name = `${result.startedAt.replace(/[:.]/g, '-')}-${result.runId}`;
  const jsonPath = path.join(directory, `${name}.json`);
  fs.writeFileSync(jsonPath, JSON.stringify(result, null, 2) + '\n', { flag: 'wx' });
  lines.push(JSON.stringify({ status: result.status, failures: result.failures, accounting: result.accounting }));
  fs.writeFileSync(path.join(directory, `${name}.log`), lines.join('\n') + '\n', { flag: 'wx' });
  console.log(`RESULT ${jsonPath}`);
  console.log(`${result.status}: ${result.accounting.inWindow} in-window deliveries; ${result.accounting.deliveriesPerSecond.toFixed(2)}/s`);
  return result;
}

if (require.main === module) {
  (async () => {
    const options = parseArgs(process.argv.slice(2));
    if (!options.runtime) throw new Error('provide --runtime <metadata.json>, or use bench/run-local.js');
    const runtime = JSON.parse(fs.readFileSync(path.resolve(options.runtime), 'utf8'));
    const result = await execute(options, runtime, ['bench/connect-storm.js', ...process.argv.slice(2)]);
    process.exitCode = result.status === 'COMPLETE' ? 0 : 1;
  })().catch((e) => { console.error(e.message); process.exitCode = 1; });
}
module.exports = { provenance, execute };
