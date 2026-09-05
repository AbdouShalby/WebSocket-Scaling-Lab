# Synthetic credential incident: 2026-09-05

## Classification and exact source locations

The reported commit is `5583d6bdfdc23ae20bb1a304f34a6661b7e1511d` (WS-3).
Its `tests/guards.test.js:77` contains an HTTPS URL with fixed username/password
userinfo, supplied as an invalid `ALLOWED_ORIGINS` value. Line 80 repeats the
pair in a non-disclosure assertion. Neither value is reproduced here.
This is a **synthetic negative-test credential**, not a provider-issued secret
or generated benchmark evidence. It remains in the subsequent WS-4 commit
`5a0a8a9bb0c3b5b0b660fa5a55bfbd255f711934` at the same locations.

A related fixed WS URL occurs in `tests/benchmark.test.js:63`, introduced in
`23e72ba69a92e5725ec604347cc90b683ba7febd` (WS-1A). It exercises invalid CLI input.
This related case is removed as well, without claiming it is the same vendor alert.

The HTTPS fixture matches the documented shape of GitGuardian's
[Basic Auth String detector](https://docs.gitguardian.com/secrets-detection/secrets-detection-engine/detectors/generics/basic_auth_string).
That detector lists no validity check. The private incident record was not
available, so exact vendor-incident attribution is a high-confidence source
match, not independent verification of GitGuardian's incident metadata.

## Authentication assessment and audit scope

The origin fixture uses `example.test`. Its child process imports only the
configuration module; `src/config.js:18-20` rejects the noncanonical origin
before server startup or networking. The WS fixture targets localhost and
`bench/options.js:37-39` rejects userinfo before any benchmark connection.
There is no corresponding Basic Auth account/backend in this repository.
No external login probes were made and no credentials were revoked or rotated.
There is no evidence that these fixture values can authenticate to a real
service in this project; this does not establish absence of arbitrary reuse elsewhere.

The pre-remediation audit covered all 11 local reachable commits and 219 unique
Git blobs (including refs/reflogs), current publishable files, and a separate
scan of 3,276 local first-party/runtime text files excluding Git internals and
node_modules. Checks included credential-bearing URLs, Authorization literals,
secret assignments, private keys, common provider token shapes, related pairs
and encodings, and tracked `.env` paths. The two test URL fixtures were the
credential-shaped findings. Generic English matches in ignored Node headers
were not Authorization credentials. No tracked `.env` history was found.
This is an evidence-bounded audit, not proof that every possible secret format
or inaccessible remote object has been examined.

## Remediation and history decision

Both tests now construct fresh synthetic userinfo with `randomUUID()` at test
time. They still prove rejection before networking; the origin test separately
checks that neither component nor the full URL appears in errors. Application
code, benchmark behavior, and the existing curated evidence/index are unchanged.

`npm run check:secrets` adds an offline, metadata-only guard over Git-tracked
and nonignored untracked files. CI runs it and its regression tests. It catches
URL userinfo, selected quoted Authorization/token/private-key representations,
and non-template `.env` filenames. It is intentionally narrow, not a substitute
for GitGuardian or a comprehensive secrets scanner. No detector ignore rule
or credential allowlist was added.

No published history rewrite is warranted for this synthetic fixture.
Old commit blobs still contain it, so a history/diff rescan can still flag it.
Removing the current literal does **not** close the historical vendor incident.
GitGuardian documents [Ignore: Test credential](https://docs.gitguardian.com/public-monitoring/remediate/remediate-incidents)
as the appropriate disposition for such cases. No vendor incident was ignored
or marked resolved by this change. Dashboard access/incident identity is needed
to verify that disposition; empty GitHub check/alert lists are not vendor clearance.

## Validation and reproducibility

- Full POSIX suite: **65/65 passed**, zero skipped/cancelled/failed, Node 24.15.0,
  real owned ephemeral Redis and real process tests, including existing negative controls.
- [Raw suite summary](2026-09-05/summary.json) and [TAP output](2026-09-05/tests.txt)
  preserve the validator output verbatim (only the TAP filename extension differs).
  The existing runner labels this `WS-4`; this is a security revalidation, not a
  new milestone. Its precommit HEAD plus source hashes identify the tested patch.
- Existing archive integrity: 111 files, 49 matrix trials, one complete matrix.
- Offline guard: no current findings. Negative controls against the original
  Git blobs detect the origin at line 77 and benchmark URL at line 63.
- Independent Gitleaks 8.30.1, with full redaction: the 11-commit pre-remediation
  history scan returned zero findings. The final 174-file publishable snapshot
  returned one `generic-api-key` match in `2026-09-05/summary.json:81`: the SHA-256
  digest for `scripts/check-credential-literals.js`. Recomputing that file's
  normalized source hash exactly matches the recorded digest; it is a verified
  noncredential false positive. The raw evidence is retained without alteration
  and no suppression was added. **Gitleaks did not detect the original
  synthetic URLs**, so these results are not evidence of GitGuardian clearance.
  The official Windows archive was checked against release SHA-256
  `d29144deff3a68aa93ced33dddf84b7fdc26070add4aa0f4513094c8332afc4e`.
- Downloads, uncurated runtime output and scanner snapshots stay ignored under
  `bench/results/security-incident-20260905/`; no tooling binaries are published.

Re-run `npm run check:secrets`, `npm run verify:evidence`, and `npm run validate`
on Linux with supported Node and `redis-server` installed. For an independent
scanner use `gitleaks git --log-opts="--all --full-history" --redact=100`, and
`gitleaks dir` on a snapshot of the Git-publishable files. Findings must be
reviewed with their detector limitations; never print credential values in reports.
