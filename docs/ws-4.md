# WS-4: showcase finalization

## Read-only audit and scope

Started from clean `5583d6bdfdc23ae20bb1a304f34a6661b7e1511d` (WS-3). Inspected tracked application/harness/test files, package and lockfile, Docker/Compose/nginx, k6, CI, README, milestone documents, Git history/status and all 111 curated evidence artifacts. Ignored runtime files remained excluded; no user changes needed separation and no history rewrite was needed.

The audit found claim drift rather than an application feature gap: README wording suggested stateless sessions and unconditional fan-out; comments implied unmeasured compression gains and a Dockerfile FD-limit change; the Compose benchmark example omitted mandatory runtime provenance; k6 thresholds had no retained evidence. Finalization corrects those claims and classifies historical results without changing executable application or deployment behavior.

## Changes

- README and [evidence index](evidence/README.md) distinguish bounded test evidence, production-inspired choices, assumptions, unsupported claims and missing guarantees. Prior milestone documents are explicitly historical snapshots.
- [Reproduction guide](reproduce.md) supplies portable prerequisites, a single full-suite gate, individual experiment commands and failure/cleanup boundaries.
- `scripts/verify-evidence.js` checks the immutable reviewed archive index and regenerates successful WS-1C derived outputs in memory. It does not write archive files or certify live behavior from logs.
- `scripts/validate.js` reuses the existing owned-resource lab to run every test file against real infrastructure, records provenance/TAP and rejects skips or incomplete summaries. Three tooling regressions test archive drift/reclassification and fail-closed TAP handling. Existing tests and negative controls are unchanged.
- CI gains archive/tooling checks; local validation artifacts are retained separately from the historical checksum index to avoid self-reference. Routine outputs remain ignored.

No authentication, durable replay, broker HA, fleet routing redesign, application features, new project or Career Ops changes are included. Cleanup means clearer tracking/documentation boundaries, not deleting generated artifacts or services.

## Validation and review

Live gate on 2026-09-05: **63 tests passed, zero failed/skipped/cancelled/todo**, in 63.93 seconds of test-runner time. All 60 existing tests remain unchanged; three WS-4 tooling regressions were added. The gate syntax-checked 35 JavaScript/CommonJS files and verified 111 archive files / 49 matrix trials / one complete matrix. This includes the real 1/2/4-process short matrix, two-node fan-out, operational/security scenarios and existing regression-sensitive negative controls.

- [Full-suite JSON](evidence/ws-4/full-suite.json) and [full TAP stdout](evidence/ws-4/full-suite.log): Node v24.15.0, ws 8.21.0, ioredis 5.11.1, Redis 8.0.5, Linux WSL2 x64 on one Ryzen 7 5800X host. The JSON pins 42 source/config/test/index files; normalized hashes were checked against final contents, and the TAP checksum matches. Base SHA is WS-3 with dirty WS-4 content, not an invented future commit hash.
- [Missing-Redis negative smoke](evidence/ws-4/missing-redis.json) and its [empty TAP log](evidence/ws-4/missing-redis.log): launched with `REDIS_SERVER=/nonexistent/ws4-redis-server`, exited **1** with `Redis startup failed: ENOENT`; no tests were silently skipped or falsely reported successful. This intentionally failed prerequisite probe is not a failed positive suite.
- The positive run verified its owned Redis PID 11645 on ephemeral port 35423, with persistence disabled; test runner PID 11655 exited 0. Normal cleanup recorded both owned processes exited. A subsequent process listing showed no remaining lab test processes; pre-existing Redis on 6379 and PostgreSQL remained running and were not touched.
- `docker compose config --quiet` and `git diff --check` passed. This is configuration parsing, **not** Docker runtime or nginx routing evidence. Historical long-run performance results are preserved, not rerun or relabeled as WS-4 capacity evidence. Local results do not establish a remote CI pass.

### Standards review

Independent read-only review found no blocking correctness or documented-rule violations. Documentation findings were resolved: the reproduction guide names the legacy E2E port 18090 prerequisite, dynamic replica discovery is classified as unsupported rather than implemented, and this section replaces the pending result placeholder with actual evidence.

### Spec review

Independent read-only review confirmed comment-only application/deployment changes, unchanged existing tests and historical artifacts, and no speculative runtime features. Its claim-classification finding was resolved as above. No WS-4 requirement was traded for a weaker test or stronger unsupported claim.

Final boundary: one scoped WS-4 checkpoint, no dependency resolution change, no generated routine artifacts or private files staged, no push and no subsequent milestone/project work.
