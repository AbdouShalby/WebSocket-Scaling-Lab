'use strict';
// Offline, deliberately narrow guard. Not GitGuardian and not a complete secret detector.
const fs = require('node:fs');
const { execFileSync } = require('node:child_process');
const { ROOT } = require('../bench/options');
const rules = [
  ['url-userinfo', /\b[a-z][a-z0-9+.-]*:\/\/[^\s/"'<>]+:[^\s/"'<>]+@/i],
  ['authorization-literal', /["'`](?:Basic|Bearer)\s+[A-Za-z0-9+/_=-]{8,}["'`]/i],
  ['private-key', /-----BEGIN (?:RSA |EC |OPENSSH |DSA )?PRIVATE KEY-----/],
  ['provider-token', /\b(?:gh[pousr]_[A-Za-z0-9]{30,}|github_pat_[A-Za-z0-9_]{35,}|AKIA[A-Z0-9]{16}|sk_live_[A-Za-z0-9]{16,}|xox[baprs]-[A-Za-z0-9-]{15,})\b/],
];
function findCredentialLiterals(text) {
  return text.split('\n').flatMap((line, i) => rules.filter(([, pattern]) => pattern.test(line))
    .map(([rule]) => ({ rule, line: i + 1 })));
}
function scan() {
  const files = [...new Set(execFileSync('git', ['ls-files', '-z', '--cached', '--others', '--exclude-standard'],
    { cwd: ROOT, encoding: 'utf8' }).split('\0').filter(Boolean))];
  const findings = [];
  for (const file of files) {
    if (/(^|\/)\.env(?:$|\.)/.test(file) && !/\.env\.(?:example|sample|template)$/.test(file)) {
      findings.push({ file, rule: 'private-env-file', line: 1 });
    }
    const absolute = require('node:path').join(ROOT, file);
    if (fs.lstatSync(absolute).isSymbolicLink()) throw new Error('credential scan does not follow symlinks');
    const content = fs.readFileSync(absolute);
    if (!content.includes(0)) findings.push(...findCredentialLiterals(content.toString('utf8')).map(f => ({ file, ...f })));
  }
  return { files: files.length, findings };
}
if (require.main === module) {
  try {
    const result = scan();
    console.log(JSON.stringify({ status: result.findings.length ? 'FAILED' : 'PASSED', ...result }));
    process.exitCode = result.findings.length ? 1 : 0;
  } catch { console.error('credential scan failed; inspect repository readability'); process.exitCode = 1; }
}
module.exports = { findCredentialLiterals, scan };
