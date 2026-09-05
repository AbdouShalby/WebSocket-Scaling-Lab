# Evidence index and claim boundaries

This is a production-inspired showcase using simulated events, not a production-proven service. Read [reproduction](../reproduce.md) for the current gate and [WS-4](../ws-4.md) for finalization results. Milestone documents are historical snapshots: their commands, source hashes and runtime details describe those runs, not an assertion that every later checkout has identical performance.

## Curated archive

The [machine-readable index](../evidence-index.json) enumerates all 111 original WS-1A..WS-3 files, including logs, failed attempts and generated reports. Paths are repository-relative; SHA-256 and sizes use UTF-8 with CRLF normalized to LF for Windows checkouts. Run `npm run verify:evidence` from the repository root. Verification is read-only: it rejects changed, added or missing archive files and never silently rewrites the index. A deliberate archive update requires an explicit review and index update.

| Scope | Evidence / retained outcome | What it establishes |
|---|---|---|
| WS-1A | [JSON](ws-1a/2026-09-05T02-41-59-459Z-9786088b-c9f7-4595-b7e7-cf841f256085.json), [log](ws-1a/2026-09-05T02-41-59-459Z-9786088b-c9f7-4595-b7e7-cf841f256085.log) | 24 clients, 90 publications, 720 unique receipts; finite one-server methodology check |
| WS-1B | [JSON](ws-1b/2026-09-05T02-55-24-709Z-73185e26-f74d-464e-b71f-af3ab83ca525.json), [log](ws-1b/2026-09-05T02-55-24-709Z-73185e26-f74d-464e-b71f-af3ab83ca525.log) | Two real processes, six clients, six publications and 13 exact client/event receipts; shared/private channels and subscription transitions |
| WS-1C successful matrix | [summary](ws-1c/scaling-2026-09-05T03-15-56-144Z-00e21f81-9bdc-4e95-bc45-e31821433b2a/summary.json), [table](ws-1c/scaling-2026-09-05T03-15-56-144Z-00e21f81-9bdc-4e95-bc45-e31821433b2a/summary.md), [curves](ws-1c/scaling-2026-09-05T03-15-56-144Z-00e21f81-9bdc-4e95-bc45-e31821433b2a/curves.svg) | 27 runs: 240/1200/2400 total clients, 1/2/4 processes, three repetitions, fixed offered workload on one host |
| WS-1C failed attempt 1 | [summary with all seven trial paths](ws-1c/scaling-2026-09-05T03-05-37-576Z-4a634f2f-aef7-4d53-9572-c282b455d8db/summary.json) | 4800-client trial missing 172,738 receipts at the bounded cutoff; not successful capacity evidence or a server-only limit |
| WS-1C failed attempt 2 | [summary with all 15 trial paths](ws-1c/scaling-2026-09-05T03-08-35-449Z-bffd1c90-5e6a-47a3-b59d-8f3464e19a85/summary.json) | Setup reached 1199/1200 ready clients with one socket error; no measurement publications in the failing trial; root cause not established |
| WS-2 | [JSON](ws-2/2026-09-05T14-55-12-178Z-15bc2891-16a3-4cd5-8126-45d600114127.json), [log](ws-2/2026-09-05T14-55-12-178Z-15bc2891-16a3-4cd5-8126-45d600114127.log) | Five real process/socket scenarios: Redis recovery, server crash, cooperative drain, forced deadline, slow TCP reader; 15 owned processes exited |
| WS-3 | [JSON](ws-3/2026-09-05T15-31-52-483Z-b99b50ac-be39-4f72-940a-519f26eda0ce.json), [log](ws-3/2026-09-05T15-31-52-483Z-b99b50ac-be39-4f72-940a-519f26eda0ce.log) | Four ingress/rate/readiness/subscription-ACK scenarios with real Redis and sockets; 10 owned processes exited |
| WS-4 | [final validation and review](../ws-4.md) | Current full-suite execution and archive reconciliation, not a repeat of the long WS-1C capacity workload |

All three matrix summaries reference their raw trials by basename and original normalized checksum. The verifier checks these references and recomputes the successful matrix rows, Markdown and SVG using `bench/scaling-report.js`; it does not aggregate failed attempts into the successful curve. Across the archive there are 49 matrix trials: 47 complete, two failed. Successful-matrix expected and received totals are both 4,873,340, including 2,145 late receipts excluded from throughput/latency samples.

At 2400 clients the historical 1/2/4-process median received rates are 22,975.60 / 21,933.83 / 22,994.65 per second, with median trial p99 of 14 / 8 / 7 ms. Actual issued rates differ (114.99 / 109.79 / 114.97 source events/s); these are not equal achieved input rates or evidence of a throughput speedup. The table retains min/max and every repetition rather than choosing the fastest run.

## Provenance interpretation

Artifacts were produced before milestone commits, so their Git SHA is the earlier base and `dirty` is true. Recorded normalized source hashes identify tested content; a dirty flag is not permission to attribute it to an arbitrary later checkout. During the WS-4 audit:

- WS-1A hashes matched `23e72ba`; WS-1B matched `842bdd9`; the completed WS-1C matrix matched `b70f6a2`; WS-2 matched `7cd086e`; WS-3 matched `5583d6b`.
- Both failed WS-1C attempts predate the final socket-error diagnostics. Their `bench/runner.js` and `tests/scaling-integration.test.js` hashes differ from `b70f6a2`; other recorded files match. Those intermediate versions were not separately committed. Preserve this distinction rather than claiming exact committed provenance for them.
- WS-4 changes comments in two application files and deployment examples, and adds validation tooling/documentation. Historical source hashes should not be compared indiscriminately with current HEAD. The new full-suite report records its own base and source hashes.
- Archive checksums detect drift, not forged evidence or an independent attestation. Offline verification cannot rerun sockets, redis outages or security exchanges from a log. The live suite is a separate gate.

## Claim classification

| Classification | Boundary |
|---|---|
| Proven by bounded tests | Exact receipt accounting and channel isolation; recovery after explicit Redis/process actions; close 1001/exit 0 and deadline exit 1; slow-reader drop/eviction and healthy-client continuity; malformed input guards, optional exact Origin checks, rate limits, subscriber-ACK readiness and telemetry |
| Production-inspired design | Local socket ownership, pub/sub fan-out, observable lossy backpressure, liveness/readiness separation, bounded input handling. These are design choices, not operational production history. |
| Assumptions | Trusted local broker/network, simulated loss-tolerant data, same-host controller clock, finite observation windows; harness explicitly reconnects/resubscribes clients and starts replacement processes |
| Configured but not evidenced as a deployment | Compose/nginx service-DNS topology, Docker image startup, long-running k6 churn. Configuration presence is not a deployment or capacity test. |
| Unsupported / withdrawn | Dynamic replica discovery/reconfiguration; 30k concurrent clients; old July 5k/10k runs and derived latency/throughput; multi-host speedup, saturation ceiling, universal p99/SLOs, compression gains, production-proven usage |
| Absent / not promised | Durable replay, receipt ACK protocol, exactly-once or guaranteed delivery/recovery, session migration, lossless rolling deployments, arbitrary crash safety, total memory bound, Redis HA, TLS, authentication/channel authorization, DDoS protection, fleet metrics aggregation |

The old numerical claims remain only in Git history at `17f3771` and `e8648b4`, explicitly withdrawn by WS-1A. Negative-control fixtures are test-only source mutations, never fake replacements for the mandatory real-infrastructure proofs. Their expected failures are successful regression detection, not failed positive scenarios.

Routine `bench/results/`, portable tooling, node_modules, `.env` and non-curated logs are local/generated artifacts, not checkpoint contents. Nothing was deleted to make the showcase clean. No private file or credential is required to reproduce the gate.
