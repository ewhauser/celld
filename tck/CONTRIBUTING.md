# Contributing

Bug reproductions, new test cases, and improvements to coverage are welcome. For a failure report, include the case ID, runtime versions, and relevant artifacts from the run.

## Validation

The runner and fixtures are written in TypeScript using Effect v4. To validate changes:

```sh
pnpm check
pnpm test:reference
pnpm test:local
```

`pnpm check` runs Oxfmt formatting checks, Oxlint, type checks, and unit tests. Use `pnpm format` to format files and `pnpm lint:fix` to apply automatic lint fixes. Generated Worker declarations are excluded from formatting and linting. Run the relevant recovery or fault suite when changing those areas.

Oxlint loads `@mpsuesser/oxlint-plugin-effect` with eight selected rules in `.oxlintrc.json`: typed generator failures, current service APIs, runtime execution boundaries, tagged domain errors, Effect filesystem/process/HTTP services, and scoped temporary files. We do not enable its full opinionated preset. These checks are syntax-based, not type-aware, and do not replace review of cancellation and resource ownership.

Fixtures and tests may construct and inspect native errors to exercise runtime semantics. The listed Worker and callback adapters may run Effects at platform boundaries. Intentional synchronous rollback throws and the Node HTTP fault-injection proxy have local suppressions explaining why native behavior is required. Keep new exceptions equally narrow. The plugin's optional `msgpackr-extract` native build is disabled in `pnpm-workspace.yaml`; linting uses its JavaScript fallback.

## Adding cases

Add API cases in `src/CoreCases.ts`, `src/ServiceCases.ts`, `src/NodeCases.ts`, `src/FlagCases.ts`, or `src/ExtensionCases.ts`, and register their IDs in `docs/coverage.json`. A case that needs different compatibility flags belongs in a fixture variant: `Build.ts` rebuilds the same worker under the variant's flags, and `Domain.ts`, `Runner.ts`'s fixture loop, `Suites.ts`, and `Catalog.ts` each name it once. Keep fixtures identical across runtimes: fixtures perform operations and return observations; the driver owns assertions.

Every new case needs a targeted negative example exercising its actual oracle. For API cases, add an independently reviewed positive observation to `test/case-oracles/observations.json` and at least one named semantic mutation in `test/case-oracles/Mutations.ts`. `pnpm check` enforces exact coverage of the case registry and tests the real checker against each mutation, including matching wrong reference/candidate observations. Generic HTTP failures, malformed envelopes, and no-op mutations do not count. See [the oracle corpus guide](test/case-oracles/README.md) for the authoring workflow. Lifecycle and qualification cases must likewise include targeted bad observations in their oracle tests.

## Runner architecture

The four runners share `src/SuiteExecutor.ts` for case results, deadlines, cleanup diagnostics, and JSON/JUnit reporting. Runners retain selection, provisioning, and scenario sequencing. Use `record` for results already classified by the API oracle and `runCase` for lifecycle scenarios; scenarios that include provisioning mark `ready` after setup succeeds. Dependent stages stop on failure, while independent cases can continue. Only the API suite accepts reviewed divergences and known bugs. Reports are finalized after resource cleanup, including on cancellation, and all suites include status counts and placeholders for unreached cases. Executor changes should extend `test/SuiteExecutor.test.ts` and run the affected runtime suites.

Qualification cases live in `src/QualificationTraffic.ts`, `src/QualificationDependencies.ts`, `src/QualificationFaults.ts`, and `src/QualificationCapacity.ts`. Each registered case owns its complete fault and recovery sequence; `QualificationContext.ts` supplies shared requests, traffic, and evidence handling. `LocalOptions` names the topology and durability settings and constrains qualification to three nodes.

API reports retain the driver Node.js version in `environment.hostNode`; fixture provenance is grouped under `environment.fixtures.core|node|extensions|repro`. Preserve this metadata when changing reporting.

## Fault controls and polling

Use `FleetControls` for Docker fault operations. Its scoped peer partitions and memory limits capture prior settings, install restoration before mutation, and retain inspection evidence. Memory faults on initially unlimited nodes require an explicit finite recovery budget because Docker update treats zero as unchanged; the capacity scenarios retain their existing 512 MiB recovery budget. Keep the fault scope inside the case so restoration completes before recovery assertions. Use `Polling.ts` only for read probes: specify pending conditions or transient errors, an attempt limit, and a deadline. Readiness checks retry transport failures and HTTP 502/503/504; successful response bodies are asserted outside the retry loop. Shared history readers handle decoding and pagination; scenario oracles still check correctness, and standalone audit stays read-only.

See [AGENTS.md](AGENTS.md) for implementation conventions and [the design document](docs/DESIGN.md) for the harness architecture.

See [dashboard maintenance](docs/DASHBOARD.md) for publishing and preview instructions.
