# celld-tck

An independent compatibility and recovery test suite for [celld](https://celld.dev).

Run identical fixtures on celld and Cloudflare’s workerd, check API behavior, and test recovery from crashes, storage outages, and node failures.

**[Latest results →](https://ewhauser.github.io/celld-tck/)** · [Known bugs](docs/BUGS.md) · [Test design](docs/DESIGN.md) · [CI runs](https://github.com/ewhauser/celld-tck/actions/workflows/ci.yml?query=branch%3Amain)

## What it tests

| Area            | Coverage                                                                      |
| --------------- | ----------------------------------------------------------------------------- |
| Runtime APIs    | HTTP, streams, crypto, WebSockets, and Node.js compatibility                  |
| Durable Objects | Storage, SQL, transactions, RPC, concurrency, and alarms                      |
| Services        | KV, D1, R2, Queues, Workflows, and service bindings                           |
| Extensions      | Static assets, WebAssembly, dynamic Workers, and facets                       |
| Recovery        | Restarts, disk loss, failover, and replica recovery                           |
| Faults and load | Network partitions, storage failures, memory pressure, and interrupted writes |

Each runtime must satisfy the case’s assertions independently. Matching incorrect results do not pass. Every API case also has a deliberately incorrect observation that its checker must reject.

Tests target **celld v0.6.0** and **workerd 1.20260730.1**, with compatibility date **2026-07-30**. Container images are [pinned by digest](infra/compose.yaml). See the [case registry and exclusions](docs/coverage.json) for the full scope.

The [v0.6.0 upgrade assessment](docs/RELEASE-0.6.0.md) records new coverage, release-specific gaps, and local validation.

## Run locally

Requires Node.js **24.21.0**, pnpm **11.15.0**, and Docker with Compose. No cloud account is needed.

```sh
git clone https://github.com/ewhauser/celld-tck.git
cd celld-tck
pnpm install --frozen-lockfile
pnpm test:local
```

The suite starts workerd, celld, and MinIO, runs the tests, and removes its Docker resources afterward, including on test failures and handled interrupts. Reports and diagnostic logs are saved under `artifacts/tck-<uuid>/`.

To check the harness using two independent workerd instances, without Docker:

```sh
pnpm test:reference
```

Both `docker compose` and `docker-compose` are supported. Set `TCK_COMPOSE_BIN` if Compose is installed elsewhere.

MinIO's public container images are unavailable. Compose builds the same pinned MinIO server and client releases from official GitHub binaries using SHA-256 checksums and a digest-pinned Alpine base; see [the Dockerfile](infra/minio/Dockerfile). This requires BuildKit and supports amd64 and arm64. The first Docker run downloads the binaries; later runs reuse build layers.

## Reading the results

The [live matrix](https://ewhauser.github.io/celld-tck/) updates from CI on `main`, including failing runs. It separates passes, known bugs, accepted divergences, failures, and missing evidence, with individual observations and report downloads.

**Successful CI can include known bugs and accepted divergences.** Inspect those results before relying on a particular behavior. Known bugs are accepted only for the registered runtime version, compatibility settings, and observations. Unexpected failures exit nonzero.

To treat known bugs as failures:

```sh
pnpm tck --profile local --known-bugs error
```

Local runs produce:

- `report.json` and `junit.xml`: case results and cleanup failures.
- `run.json` and `coverage.json`: configuration, versions, seed, and selected cases.
- Logs and observations for investigating failures.

Use `--output ./artifacts` to choose the output directory. See [findings](docs/FINDINGS.md) and [known bugs](docs/BUGS.md) for documented behavior.

These tests cover the pinned local runtimes. They do not establish managed Cloudflare equivalence or AWS qualification; AWS adapters and provisioning are not implemented.

## Running individual suites

The default API suite runs 66 compatibility cases and, for local runs, 6 deployment checks. Select `core`, `bindings`, `node`, or `extensions`, or run a single case:

```sh
pnpm tck --profile local --suite bindings
pnpm tck --profile local --case storage.transaction-rollback --seed 123
pnpm tck --help
```

Recovery and fault suites run separately and require Docker:

| Command                   | Coverage                                                                                                                                                     |
| ------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `pnpm test:recovery`      | Process restarts, disk loss, and object-store outages                                                                                                        |
| `pnpm test:multinode`     | Two-node bucket-durable failover and fencing                                                                                                                 |
| `pnpm test:fleet`         | Fleet-durable follower recovery                                                                                                                              |
| `pnpm test:resilience`    | Three-node fault scenarios                                                                                                                                   |
| `pnpm test:qualification` | Traffic, dependency recovery, storage faults, capacity, [security boundaries](docs/SECURITY-BOUNDARIES.md), and [fleet operations](docs/FLEET-OPERATIONS.md) |

The optional `repros` suite contains isolated [upstream bug reproductions](docs/upstream/README.md) and is excluded from the default suite.

## Contributing

Contributions can add coverage, reproduce a bug, or improve the harness. Include the case ID, runtime versions, and relevant artifacts in failure reports.

```sh
pnpm check
pnpm test:reference
pnpm test:local
```

Every new case needs a meaningful negative example that its actual checker rejects. See [CONTRIBUTING.md](CONTRIBUTING.md) for case authoring, validation, and implementation conventions.

## Documentation

- [Test design](docs/DESIGN.md)
- [Recovery](docs/RECOVERY.md), [multi-node failover](docs/MULTINODE.md), and [fleet durability](docs/FLEET.md)
- [Resilience](docs/RESILIENCE.md) and [qualification scenarios](docs/QUALIFICATION.md)
- [Storage durability barriers, cursors, and deadlines](docs/STORAGE-DURABILITY.md)
- [Dashboard publishing and local previews](docs/DASHBOARD.md)
- [Coverage roadmap](docs/ROADMAP.md) and [qualification backlog](docs/BACKLOG.md)
- [In-place deployment coverage](docs/IN-PLACE-DEPLOYMENT.md)
- [Assets, dynamic Workers, and facets](docs/EXTENSIONS.md)
- [Runtime and service breadth](docs/RUNTIME-BREADTH.md)
- [Security boundaries](docs/SECURITY-BOUNDARIES.md)

## License

[Apache License 2.0](LICENSE).
