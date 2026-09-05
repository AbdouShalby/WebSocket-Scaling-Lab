# Reproduce the showcase

Run from the repository root in a clean Linux or WSL shell. No Career Ops files, cloud account, credentials or existing Redis daemon are required for the full gate. This guide describes the WS-4 checkout; [milestone evidence](evidence/README.md) is historical and must retain its original runtime attribution.

## Prerequisites and full gate

- Git; Node.js 22.x or 24.x and npm. The package engine minimum is 20, but this is not evidence that every allowed version is validated. CI is configured for Node 22; the retained local run uses Node 24.15.0.
- A real POSIX `redis-server` executable on PATH (or `REDIS_SERVER` pointing to that executable). CI config uses a Redis 7 service and an installed system Redis executable; the local retained run uses Redis 8.0.5. These are observed/configured environments, not a compatibility matrix.
- Local loopback networking and permission to spawn/stop owned processes. On Windows, run **Linux Node inside WSL**, not Windows Node via interop: the failure tests use SIGKILL, SIGSTOP/SIGCONT and SIGTERM semantics.
- Enough free file descriptors/ports for the small integration fixtures, including **loopback port 18090 free** for the legacy E2E server. Other fixtures allocate ephemeral ports. Do not run multiple full gates concurrently. The full gate is not the 2400-client long matrix.

```bash
node --version
npm --version
redis-server --version
npm ci
npm run verify:evidence
npm run validate
```

Use the committed lockfile; `npm ci` installs it without updating it. Do not reuse custom app configuration or mutation-preload variables from a previous experiment. `validate` rejects `NODE_OPTIONS` and the known mutation selector variables. Optional `REDIS_SERVER` is a path to a real executable, not a mock. Output arguments/paths and runtime logs are recorded: never put secrets in them.

`validate` checks archive integrity, syntax-checks source/harness/tooling/tests, discovers every `tests/*.test.js`, starts a nonpersistent Redis on a free loopback port and verifies its PID and persistence configuration. Existing suites needing Redis receive that port; WS-2/WS-3 create their own isolated Redis/server children. There is no external-daemon, in-memory Redis or synchronous fake fallback. Expected negative controls run real infrastructure with explicit source mutations.

The command exits nonzero on a failed process, incomplete TAP totals, any skip/todo/cancellation, source drift or normal cleanup failure. On completion it prints `VALIDATION bench/results/ws4/<timestamp-uuid>/summary.json`. That JSON records source hashes, exact test argv, runtime, counts, failures and owned-resource exit evidence; `tests.log` retains the full TAP stdout with a checksum. Supporting scenario JSON/logs remain in their existing ignored `bench/results/` directories. The full-suite summary retains the outer Redis process evidence; inner process cleanup assertions live in the scenario tests/artifacts.

No success is claimed if a prerequisite fails. Missing Redis should report `Redis startup failed` and a nonzero exit, not an E2E skip. A bare `npm test` can skip its legacy E2E test when Redis is absent, so it is **not** the final gate. Do not convert a failed scenario to a skip or relax thresholds to obtain a pass. Inspect the retained failure and rerun only after understanding the change.

Normal cleanup stops only owned processes and leaves artifacts intact. Hard-killing the validator/OS is not an arbitrary process-tree cleanup guarantee. If interrupted, inspect recorded PIDs and command lines before terminating anything; never use broad `pkill`, kill a shared daemon or prune Docker. No persistence files, volumes or images are removed by the gate.

## Individual experiments

WS-2 and WS-3 own Redis and produce independent versioned artifacts:

```bash
npm run proof:operations
npm run proof:guards
```

The following checks require a **dedicated already-running Redis**, unlike the full gate. Choose an unused port; in one terminal start a foreground, nonpersistent Redis (stop that exact foreground process after the experiment):

```bash
redis-server --bind 127.0.0.1 --port 16379 --save '' --appendonly no --protected-mode yes
```

In another terminal at the same checkout:

```bash
node bench/run-local.js --redis-port 16379 --conns 12 --channels 3 --rate 30 --measure 2
npm run proof:two-node -- --redis-port 16379
npm run bench:scaling -- --redis-port 16379 --connections 240,1200,2400 --ramp 50
```

The last command reproduces the **shape** of the 27-trial historical experiment, takes substantially longer than the integration gate, and may fail on a different machine. It must not be presented as reproducing identical numbers. Leave competing publishers/workloads off the dedicated broker. All repetitions and failed attempts matter. Do not use the default 4800-client matrix and then silently label it as the retained 2400-client run.

`bench/connect-storm.js` targets an existing server and requires `--runtime <metadata.json>` with schemaVersion, environment, topology and server versions/config; it cannot discover those facts automatically. Prefer `run-local`/`run-fleet` launchers that capture their owned target. Never invent runtime metadata just to bypass the CLI guard.

## Optional topology, not measured evidence

`docker compose config --quiet` can check Compose syntax without starting Docker services. The Compose fleet publishes loopback ports 6379 and 8080; check for conflicts first. It includes a demo publisher that is not the controlled benchmark publisher. Image builds, nginx routing, Docker startup/recovery and k6 are not part of the retained WS-4 live gate. Do not expose this configuration publicly: no application authentication, channel authorization or TLS is supplied.

## What to retain / compare

Keep raw JSON and logs together. Compare status, failures, actual issued rate, ready/disconnected clients, expected/received/in-window/late/missing/duplicate/unexpected counts and process IDs **before** throughput or p99. For matrices compare the full fixed-workload repetition set, not isolated fastest trials. The archive verifier recomputes the successful table and SVG and detects historical file drift; it does not assert cryptographic authenticity, capacity, production readiness or live health.
