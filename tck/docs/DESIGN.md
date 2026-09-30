# celld API compatibility test kit

Status: implemented local API corpus, September 18, 2026. The Effect v4 runner now has 63 differential cases and 6 celld-specific deployment checks, across base-Web, Node-compatibility, and extension fixture deployments. The executable coverage contract is [coverage.json](coverage.json); [FINDINGS.md](FINDINGS.md) records validation results and differences. API-suite implementation does not mean the candidate conforms. Separate local recovery, two-node bucket/fleet, and three-node resilience suites now implement lifecycle/fault testing; see RECOVERY.md, MULTINODE.md, FLEET.md, and RESILIENCE.md. The full local P0/P1/P2 checklist is implemented by 19 additional traffic, dependency, storage-fault, and capacity scenarios; see [QUALIFICATION.md](QUALIFICATION.md) and [BACKLOG.md](BACKLOG.md). AWS adapters and managed Cloudflare qualification remain separate work. No cloud resources have been created.

## Decision

Use TypeScript with Effect v4 release candidate for all authored runner, scenario, adapter, and Worker/Durable Object fixture code. Pin all Effect packages to the same exact RC (initially `4.0.0-rc.115`). Run the driver on a pinned Node.js LTS release and use `@effect/vitest` with its supported Vitest version. Compile fixture TypeScript into JavaScript once with esbuild and deploy identical application modules, including their bundled Effect runtime, to both engines.

Workflows return `Effect`, dependencies are `Context.Service` values supplied by Layers, untrusted data is decoded with Schema, and owned resources use scopes/finalizers. Use Effect platform services for filesystem, process, and HTTP I/O. Adapt third-party Promise APIs at their boundary. Convert to Promise only at externally imposed handlers/callbacks; preserve synchronous execution inside synchronous platform transaction callbacks. Pin the fixture Effect version in every report because its runtime is part of the tested application.

Use Miniflare, powered by workerd, as the first local reference engine. Run the actual celld release image against MinIO through Docker Compose as the first candidate environment. Add real S3 and then EC2 through adapters without changing cases or expectations. A future managed Cloudflare reference is useful for service behavior that the local emulator cannot establish.

The suite is an independent public contract check. It does not require celld's private tests, private simulation hooks, or Rust internals. Rust would be appropriate for a later in-process protocol simulator, but offers little benefit for an HTTP/WebSocket-driven API suite whose fixtures must execute JavaScript anyway.

## Scope and interpretation

Test observable API behavior: return values, errors, state changes, ordering constraints, and completion of asynchronous work. Cover supported Cloudflare APIs and explicitly test rejection of unsupported configuration/API surfaces.

Keep three result categories distinct:

1. Shared API conformance against a reference engine and documented expectations.
2. Documented celld divergences and celld-only extensions.
3. Lifecycle/recovery checks, which can reuse the harness but have their own requirements.

This first design does not claim distributed durability, performance, or production Cloudflare equivalence from a passing local run. Multi-host fault injection belongs in a subsequent reliability suite.

## Topology

```text
                      Node.js / TypeScript driver
                   cases + assertions + result writer
                         /                   \
                   HTTP / WS              HTTP / WS
                       /                       \
          Miniflare -> workerd              real celld
          isolated reference state           |
                                          MinIO or S3

            identical application module bytes on both sides
```

MinIO is the candidate's object store. Miniflare is the reference runtime launcher/service emulator. They serve different purposes.

All assertions live in the external driver. Each Miniflare runtime runs in a scoped child process because its process-exit signal hooks otherwise bypass the driver's finalizers. Fixtures call platform APIs and expose narrowly scoped observations. The common test path uses actual HTTP/WebSocket listeners on both engines, not Miniflare-only direct binding access or `cloudflare:test` helpers. A Worker endpoint can invoke a DO or service RPC method internally, making that call observable through HTTP without replacing the RPC under test.

Do not load the test runner into the Worker. Do not use Node-hosted mocks as implementations of the API being checked.

## Environment profiles

| Profile       | Candidate compute                           | Candidate storage        | Purpose                                             |
| ------------- | ------------------------------------------- | ------------------------ | --------------------------------------------------- |
| `local`       | One Docker celld node                       | MinIO container          | Default developer/PR conformance                    |
| `local-fleet` | Three Docker celld nodes                    | MinIO container          | Cross-node routing and preliminary lifecycle checks |
| `s3`          | Local Docker celld                          | Real dedicated S3 prefix | Isolate real object-store behavior                  |
| `aws`         | celld containers on dedicated EC2 instances | Real S3                  | Cloud environment qualification                     |
| `attached`    | Existing dedicated test fleet               | Its configured store     | Reuse externally provisioned infrastructure         |

Pin container images by digest, including MinIO, and record architecture. Avoid the MinIO release identified as broken in celld's guarantees documentation. Run celld's storage diagnostics and startup checks before testing; fail setup if required conditional-write or range behavior cannot be verified. A local result is useful API evidence, not certification that MinIO reproduces S3 fault, IAM, or latency behavior.

Start with one node and explicit bucket durability. The fleet profile explicitly selects fleet durability and records node count. One-node fleet mode falls back to bucket proofs, so it does not exercise follower-backed acknowledgments.

Use a unique Compose project, network, volumes, bucket prefix, and run ID. Address peers using names reachable on their Docker network; the driver uses published application ports. Never publish a peer listener beyond the test environment. Readiness requires a request to the deployed fixture and a storage round trip, not just an open port.

For AWS, start with an attached fleet descriptor rather than infrastructure provisioning inside tests. A separate future AWS CDK/TypeScript stack can produce the same descriptor: application endpoints, deployment access, region, bucket/prefix, instance IDs, and node metadata. EC2 with persistent per-node volumes keeps later host-stop and recovery tests possible. Use roles/standard credential providers, not secrets in fixtures or artifacts.

Infrastructure ownership is explicit: the runner cleans resources it created; attached resources are retained. Cleanup of data is constrained to the recorded test prefix. AWS creation/teardown remains a separate explicit command. Attach only to a dedicated test fleet: deploying fixtures replaces the fleet application.

## Adapter boundaries

Separate provisioning from runtime semantics. Tests must not contain `if (aws)` or Docker commands.

Conceptual host-side Effect contracts (error/service names below describe roles):

```ts
interface Provisioner {
  acquire(
    request: EnvironmentRequest,
  ): Effect.Effect<EnvironmentLease, ProvisionError, Scope.Scope>;
}

interface RuntimeAdapter {
  deploy(
    bundle: FixtureBundle,
    bindings: BindingPlan,
  ): Effect.Effect<Deployment, DeployError>;
  ready(deployment: Deployment): Effect.Effect<RuntimeMetadata, ReadinessError>;
  collectArtifacts(): Effect.Effect<ArtifactIndex, ArtifactError>;
  // Registered as a finalizer by the environment scope, including partial setup.
  dispose(): Effect.Effect<void, CleanupError>;
}

interface CaseDefinition {
  id: string;
  fixture: string;
  contract: string; // documentation reference
  requirements: string[]; // explicit required capabilities
  comparison: "exact" | "invariants" | "documented-divergence";
  run(
    target: Target,
    input: CaseInput,
  ): Effect.Effect<Observation, CaseError, Transport>;
  check(
    observation: Observation,
    input: CaseInput,
  ): Effect.Effect<void, AssertionError>;
  compare(
    reference: Observation,
    candidate: Observation,
  ): Effect.Effect<void, AssertionError>;
}
```

These interfaces describe the architectural boundaries; concrete exports are in `src/Domain.ts`, `Build.ts`, `Reference.ts`, and `Local.ts`. `Target` is a transport view with endpoints and authentication; it does not expose deployment credentials. Optional lifecycle control is a separate interface used only by lifecycle cases. Each acquired environment supplies an exclusive deployment lease, cleanup policy, and resource inventory. Adapters may translate binding declarations but cannot change test assertions or application module bytes.

## Fixture packaging and isolation

Group fixtures into small deployment projects by API family/configuration profile. Use separate projects where compatibility flags, bindings, migrations, or intentional deployment failures differ. Avoid one giant worker: an unsupported import must not prevent unrelated API tests from running.

Build each project once. Record hashes for every emitted module, asset, and wasm file. Feed the emitted modules to both runtimes without a second bundling pass; celld supports `no_bundle`. Render backend-specific bindings/configuration from one declarative binding plan and archive both rendered versions. Verify the no-rebundle path in the initial spike.

Pin compatibility dates and flags per fixture profile; do not automatically advance dates. Maintain separate base-Web and Node-compatibility profiles so enabling Node compatibility cannot hide a missing Web API. Record exact Miniflare, workerd, celld, Node, compiler, and package-lock versions.

Each case receives its own logical namespace on both engines. Use the same logical DO names and inputs, isolated by run/project on each target. Prefix KV/R2 keys and allocate independent D1/Queue/Workflow resources or serialize cases where namespace isolation is insufficient. A new run uses fresh persistence; only lifecycle tests intentionally reuse state across restarts.

One celld fleet hosts one application. Deploy projects sequentially per fleet. Parallel cases may share a project only if all state is isolated; parallel projects require separate fleets. Reject concurrent attachment without the deployment lease.

Fixture routes are explicit operations such as `/storage/rollback`, not arbitrary code evaluation. Internal fixture events go into a bounded trace for assertions. External-effect fixtures use an auxiliary HTTP/WebSocket service owned by the test run and reachable from both engines. Keep that service and its observations separate for each target.

## How a case runs

1. Resolve requirements against a versioned capability manifest. Missing required capabilities fail setup; optional out-of-scope coverage is reported explicitly.
2. Provision/acquire environments, deploy the same artifact, and verify readiness.
3. Execute the same seeded inputs against fresh reference and candidate state.
4. Save the raw requests, responses, events, and observations before normalization.
5. Validate each engine independently against documented semantic invariants.
6. Compare the observations using the case's explicit comparison rule.
7. Collect diagnostics and release resources in `finally`, including on interruption where possible.

If the reference fails its own expected invariants, report a reference/harness error; do not label matching failures a compatibility pass. First build a reference-versus-reference self-check and intentionally perturb observations to verify that the comparators detect differences. This checks the harness rather than relying on a consistently green report.

## Comparison rules

- Exact comparison for deterministic API results, binary payloads, lengths, ordered lists, SQL values, and selected semantic headers/statuses.
- Preserve JavaScript type distinctions in observations: `undefined`, missing properties, `null`, BigInt, special numeric values, byte arrays, Map, and Set. A plain JSON round trip cannot represent all API results. Test the observation codec independently.
- Use fixed cryptographic test vectors; validate properties of random outputs instead of expecting identical random bytes. Treat opaque IDs as opaque unless equality across engines is part of the contract.
- Normalize only named fields with a per-case rule, such as a deployment origin or generated identifier. Keep raw data. Never sort a list whose order is under test, blanket-remove errors, or compare only final counters when order matters.
- For concurrent calls, validate allowed histories and partial-order constraints. The two engines need not select the same legal schedule. Coordinate requests with explicit barriers/control endpoints and capture invocation/completion intervals.
- For timers, alarms, queues, and workflows, check eventual semantic outcomes within a bounded profile-specific deadline. Poll observable state; do not rely on fixed sleeps, fake runtime clocks, exact elapsed milliseconds, or manual reference-only alarm firing.
- For streams, compare bytes, ordering, close/error/cancel behavior, and tested backpressure properties. Network chunk boundaries and timing are not stable API outputs.
- For errors, compare rejection category and specified observable fields. Compare message text only where deliberately part of the contract; runtime stack traces are diagnostics.
- No automatic case retry that converts a flaky failure to green. Retry transport readiness separately; any diagnostic rerun remains linked to the initial failure.

Known divergences are narrow records keyed by case, celld version, and compatibility profile, with a source/issue, expected observation, owner, and review date. They remain visible as divergences. An unexpected pass requires review rather than being silently swallowed. Unsupported APIs get rejection tests; feature detection must not dynamically skip a promised API.

## Initial coverage

| Family              | Initial cases                                                                                   | Comparison                                 |
| ------------------- | ----------------------------------------------------------------------------------------------- | ------------------------------------------ |
| HTTP/Web APIs       | Bodies, headers, encoding, cloning/body consumption, redirects, URL/FormData, aborts            | Exact values plus invariants               |
| DO identity/routing | Repeated named identity, separate names, persistence, direct vs forwarded requests              | State and identity relationships           |
| DO storage          | Types, get/put/delete/list, ranges/order, empty values, transactions and rollback               | Values and state                           |
| SQL                 | Bindings, NULL/BLOB, cursors, `RETURNING`, transaction rollback, read/write interleaving        | Typed results and committed state          |
| Concurrency         | Constructor gating, `blockConcurrencyWhile`, storage input gates, concurrent increments         | Allowed histories, no lost updates         |
| RPC                 | DO methods, named service entrypoints, serialization, thrown errors, supported returned targets | Values, errors, reference lifetimes        |
| Alarms              | Set/get/delete, replacement, actual firing, handler failure/retry                               | Eventual state and allowed delivery counts |
| Streams             | Binary transfer, streaming responses, cancellation, errors, backpressure                        | Bytes and event constraints                |
| WebSockets          | Upgrade, text/binary messages, close, attachment state                                          | Protocol/state invariants                  |
| Deployment          | Supported bindings/config, missing bindings, unsupported config/API rejection                   | Success/rejection contract                 |

KV, D1, R2 bindings, Queues, Workflows, Node compatibility, Web Crypto, assets, wasm, and facets now have executable coverage groups. The checked-in manifest lists the exact cases; a family entry does not claim every method or edge case is exhausted. Prioritize Queues and named RPC early for the consuming workflow application. Keep application/framework conformance tests separate from runtime API cases so failures have a clear owner.

Hibernation/restart scenarios extend the lifecycle group. A forced process restart is not proof of WebSocket hibernation behavior; report unsupported lifecycle controls explicitly. Containers/Sandbox require a separate optional profile and are not implied by running the celld server in Docker.

workerd is the reference for runtime behavior. Miniflare-provided services are local implementations of managed services, so label those results accordingly. Managed Cloudflare comparisons can later verify the service-level contract. AWS runs validate celld on AWS; they do not make local reference emulation equivalent to the Cloudflare service.

## Outputs and CI

Produce human-readable results, machine-readable JSON, and JUnit, with separate counts for passes, unexpected failures, known divergences, unsupported/out-of-scope cases, reference errors, and infrastructure errors. Every selected case must have a terminal record; no missing case counts as a pass.

Each evidence bundle includes case IDs, seed, source revision, fixture hashes, runtime versions, image digests/architecture, capability manifest, durability mode, storage backend, node count, rendered non-secret configuration, raw and normalized observations, comparator diff, and runtime logs. Capture diagnostic artifacts before teardown and redact credentials. Report cleanup failures without hiding the original test result.

Current commands:

```sh
pnpm tck --profile local --suite all
pnpm tck --profile reference --suite all
pnpm tck --profile local --suite bindings
pnpm tck --profile local --suite node
pnpm tck --profile local --suite flags
pnpm tck --profile local --suite extensions
pnpm tck --profile local --case storage.transaction-rollback --seed 42
```

PR gate: reference/candidate core conformance on Docker/MinIO and unit tests of the harness itself. Extended local jobs: other API families and multiple node routing. Explicit or scheduled cloud qualification: real S3, then EC2/S3 using the same cases. Baseline updates require review of actual diffs; runtime upgrades never automatically rewrite expectations.

## Implementation sequence and acceptance criteria

1. **Vertical slice:** pinned tools/images, one Worker/DO fixture, Miniflare HTTP reference, Compose celld/MinIO candidate, readiness, isolated state, three cases (HTTP, storage round trip, rollback), raw evidence and comparator self-check. Verify unchanged fixture bytes and deliberate mismatch detection.
2. **Core corpus:** storage/SQL/concurrency/RPC, negative cases, rich-value codec, capability/divergence manifest, deadlines, WebSocket/stream transport. CI must fail on a deliberately broken result or a missing required case.
3. **Portability proof:** execute the same three slice cases with local celld against S3, then against an attached dedicated AWS fleet. No edits to case source or assertions; only environment configuration changes.
4. **Broader API/lifecycle coverage:** add asynchronous service families and lifecycle controls with explicit reference capability limits. Keep reliability/fault testing a separate suite even when it shares provisioning and artifacts.

Milestones 1 and 2 now have a working local implementation, with additional service and extension API cases from milestone 4. Milestone 3 remains blocked on cloud resources. Local lifecycle controls now support restart, disk loss, storage partitions, process pause, and multi-node fleet recovery through separate suites; these do not qualify AWS or managed Cloudflare. The first milestone demonstrated correctness of the harness before growing the corpus. Avoid committing to a test-count target that rewards redundant cases over observable behaviors.

## Source notes

Reviewed September 18, 2026. Upstream source inspected at `12d5b6333fe52717325addcfe1e99e9fd4f77bcd`. Recommendations above are this project's proposed design, not claims that these tests already exist or have passed.

- [celld compatibility contract](https://celld.dev/docs/cloudflare-compat/): supported surfaces, explicit differences, configuration, and unsupported-feature rejection.
- [celld guarantees](https://celld.dev/docs/guarantees/): storage prerequisites, MinIO caveat, and bucket/fleet durability.
- [celld limitations](https://celld.dev/docs/limitations/): one application per fleet and operational boundaries.
- [celld testing strategy](https://celld.dev/docs/testing/): differential execution and separate simulation/fleet validation.
- [Miniflare maintained README](https://github.com/cloudflare/workers-sdk/blob/main/packages/miniflare/README.md): workerd-backed local reference.
- [Cloudflare testing overview](https://developers.cloudflare.com/workers/testing/): testing modes and runtime integration.
- [Cloudflare Workers best practices](https://developers.cloudflare.com/workers/best-practices/workers-best-practices/): Worker API/configuration guidance.
- [AWS S3 conditional writes](https://docs.aws.amazon.com/AmazonS3/latest/userguide/conditional-writes.html): real storage preconditions.
