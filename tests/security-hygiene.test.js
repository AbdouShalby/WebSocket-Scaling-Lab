'use strict';
const { test } = require('node:test');
const assert = require('node:assert/strict');
const { randomUUID } = require('node:crypto');
const { findCredentialLiterals } = require('../scripts/check-credential-literals');

test('credential guard detects URL userinfo and encoded Authorization without echoing values', () => {
  const url = new URL('https://example.test');
  url.username = randomUUID(); url.password = randomUUID();
  const header = ['Basic', Buffer.from([url.username, url.password].join(':')).toString('base64')].join(' ');
  const quotedHeaders = ['"', "'", '`'].map(quote => quote + header + quote);
  const matches = findCredentialLiterals([url.href, ...quotedHeaders].join('\n'));
  assert.deepEqual(matches, [{ rule: 'url-userinfo', line: 1 },
    ...[2, 3, 4].map(line => ({ rule: 'authorization-literal', line }))]);
  assert.ok(!JSON.stringify(matches).includes(url.username));
  assert.ok(!JSON.stringify(matches).includes(url.password));
});

test('credential guard permits credential-free URLs and environment variable references', () => {
  assert.deepEqual(findCredentialLiterals('https://example.test\nws://localhost\nprocess.env.API_KEY'), []);
});
