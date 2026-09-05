'use strict';
// Read-only verification of the curated WS-1A..WS-3 archive, not a new live proof.
const assert = require('node:assert/strict');
const fs = require('node:fs'), path = require('node:path');
const { createHash } = require('node:crypto');
const { ROOT } = require('../bench/options');
const { summarize, markdown, svg } = require('../bench/scaling-report');
const scopes = ['ws-1a', 'ws-1b', 'ws-1c', 'ws-2', 'ws-3'];
const read = (file) => fs.readFileSync(file, 'utf8').replace(/\r\n/g, '\n');
const hash = (text) => createHash('sha256').update(text).digest('hex');
function walk(directory) {
  return fs.readdirSync(directory, { withFileTypes: true }).flatMap((e) => {
    assert.ok(!e.isSymbolicLink(), 'evidence symlinks are not supported');
    const file = path.join(directory, e.name);
    return e.isDirectory() ? walk(file) : [file];
  }).sort();
}
function inventory(root = ROOT) {
  return scopes.flatMap((scope) => walk(path.join(root, 'docs/evidence', scope))).map((file) => {
    const text = read(file), json = file.endsWith('.json') ? JSON.parse(text) : null;
    return { file: path.relative(root, file).replace(/\\/g, '/'), sha256: hash(text),
      normalizedBytes: Buffer.byteLength(text), ...(json ? { status: json.status } : {}) };
  });
}
function verify(root = ROOT, index = JSON.parse(read(path.join(root, 'docs/evidence-index.json')))) {
  assert.equal(index.schemaVersion, 1);
  assert.deepEqual(index.scopes, scopes);
  assert.deepEqual(inventory(root), index.files, 'archive differs from the reviewed evidence index');
  let matrices = 0, trials = 0;
  for (const entry of index.files.filter((e) => e.file.endsWith('/summary.json'))) {
    const directory = path.dirname(path.join(root, entry.file));
    const report = JSON.parse(read(path.join(root, entry.file)));
    const inputs = report.artifacts.map((a) => {
      assert.equal(path.basename(a.file), a.file, 'trial must be inside its matrix directory');
      const text = read(path.join(directory, a.file));
      assert.equal(hash(text), a.sha256, 'trial hash differs from its original matrix summary');
      return { ...a, result: JSON.parse(text) };
    });
    trials += inputs.length;
    if (report.status === 'COMPLETE') {
      const rows = summarize(inputs, report.config);
      assert.deepEqual(rows, report.rows);
      assert.equal(read(path.join(directory, 'summary.md')), markdown(rows));
      assert.equal(read(path.join(directory, 'curves.svg')), svg(rows));
      matrices++;
    } else {
      assert.equal(report.status, 'FAILED');
      assert.ok(report.failures.length > 0, 'failed matrix must retain the failure reason');
    }
  }
  return { files: index.files.length, matrixTrials: trials, completeMatrices: matrices };
}
if (require.main === module) {
  try {
    assert.equal(process.argv.length, 2, 'no options: verification never regenerates the index');
    console.log(JSON.stringify({ status: 'PASSED', ...verify() }));
  } catch (e) { console.error(e.message); process.exitCode = 1; }
}
module.exports = { inventory, verify, hash, read };
