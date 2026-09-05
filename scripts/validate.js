'use strict';
// One entry point for every existing test file; never uses an external Redis daemon.
const assert = require('node:assert/strict');
const fs = require('node:fs'), path = require('node:path');
const { randomUUID } = require('node:crypto');
const { execFileSync } = require('node:child_process');
const { ROOT } = require('../bench/options');
const { Lab } = require('../bench/operational-lab');
const { provenance } = require('../bench/connect-storm');
const { verify, hash, read } = require('./verify-evidence');
function tapTotals(text) {
  const totals = {};
  for (const key of ['tests', 'pass', 'fail', 'cancelled', 'skipped', 'todo']) {
    const found = [...text.matchAll(new RegExp(`^# ${key} (\\d+)\\r?$`, 'gm'))];
    assert.equal(found.length, 1, `missing or ambiguous TAP total: ${key}`);
    totals[key] = Number(found[0][1]);
  }
  assert.ok(totals.tests > 0 && totals.pass === totals.tests, 'not all tests passed');
  for (const key of ['fail', 'cancelled', 'skipped', 'todo']) assert.equal(totals[key], 0, `nonzero ${key}`);
  return totals;
}
function snapshot() {
  const tracked = execFileSync('git', ['ls-files', '--cached', '--others', '--exclude-standard'], { cwd: ROOT, encoding: 'utf8' });
  const files = [...new Set(tracked.trim().split('\n'))].filter((f) =>
    /^(src\/|bench\/.*\.js$|scripts\/|tests\/|package(-lock)?\.json$|Dockerfile$|docker-compose\.yml$|nginx\.conf$|docs\/evidence-index\.json$|\.github\/)/.test(f)).sort();
  return Object.fromEntries(files.map((f) => [f, hash(read(path.join(ROOT, f)))]));
}
async function main() {
  assert.equal(process.argv.length, 2, 'no options; output is always under bench/results/ws4');
  assert.notEqual(process.platform, 'win32', 'run the full POSIX suite in Linux/WSL');
  for (const key of ['NODE_OPTIONS', 'WS1B_MUTATION', 'WS2_MUTATION', 'WS3_MUTATION']) {
    assert.ok(!process.env[key], `unset ${key} before validation`);
  }
  const directory = path.join(ROOT, 'bench/results/ws4', `${new Date().toISOString().replace(/[:.]/g, '-')}-${randomUUID()}`);
  fs.mkdirSync(directory, { recursive: true });
  const lab = new Lab('ws4-full-suite');
  const report = { schemaVersion: 1, milestone: 'WS-4', startedAt: new Date().toISOString(), status: 'FAILED', failures: [] };
  let tap = '';
  try {
    report.provenance = provenance(['scripts/validate.js'], { description: 'same-host full suite; owned ephemeral Redis; direct process tests' });
    report.sourceSha256 = snapshot();
    report.archive = verify();
    const syntax = Object.keys(report.sourceSha256).filter((f) => /\.(js|cjs)$/.test(f));
    for (const file of syntax) execFileSync(process.execPath, ['--check', file], { cwd: ROOT, stdio: 'pipe' });
    report.syntaxFiles = syntax;
    await lab.startRedis();
    report.tests = fs.readdirSync(path.join(ROOT, 'tests')).filter((f) => f.endsWith('.test.js')).sort().map((f) => `tests/${f}`);
    report.argv = ['--test', '--test-reporter=tap', ...report.tests];
    const child = lab.child('full-suite', process.execPath, report.argv, { ...process.env, REDIS_HOST: '127.0.0.1', REDIS_PORT: String(lab.redisPort) });
    child.p.stdout.on('data', (b) => { tap += b; process.stdout.write(b); });
    child.p.stderr.on('data', (b) => process.stderr.write(b));
    const code = await new Promise((resolve, reject) => { child.p.once('error', reject); child.p.once('close', resolve); });
    assert.equal(code, 0, 'test process failed');
    report.totals = tapTotals(tap);
    assert.deepEqual(snapshot(), report.sourceSha256, 'source changed during validation');
    assert.equal(provenance([], {}).git.sha, report.provenance.git.sha, 'HEAD changed during validation');
    report.sourceChangedDuringRun = false;
    report.status = 'PASSED';
  } catch (e) { report.failures.push(e.stack || e.message); }
  finally {
    try { await lab.cleanup(); } catch (e) { report.status = 'FAILED'; report.failures.push(`cleanup: ${e.message}`); }
    report.resources = lab.result;
    report.completedAt = new Date().toISOString();
    report.tapSha256 = hash(tap.replace(/\r\n/g, '\n'));
    fs.writeFileSync(path.join(directory, 'tests.log'), tap, { flag: 'wx' });
    fs.writeFileSync(path.join(directory, 'summary.json'), JSON.stringify(report, null, 2) + '\n', { flag: 'wx' });
  }
  console.log(`VALIDATION ${path.relative(ROOT, directory).replace(/\\/g, '/')}/summary.json`);
  console.log(`${report.status}: ${report.failures.join('; ')}`);
  process.exitCode = report.status === 'PASSED' ? 0 : 1;
}
if (require.main === module) main().catch((e) => { console.error(e.message); process.exitCode = 1; });
module.exports = { tapTotals, snapshot };
