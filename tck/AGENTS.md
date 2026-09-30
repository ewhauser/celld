# Implementation conventions

- All authored TypeScript is based on Effect v4 release candidate. Keep `effect`, `@effect/platform-node`, and `@effect/vitest` pinned to the same exact RC.
- Model workflows and fallible operations with `Effect`; use typed errors, `Schema` at untrusted boundaries, `Context.Service`/`Layer` for dependencies, and scopes for resource ownership.
- Use Effect platform filesystem, HTTP, process, and CLI facilities where applicable. Adapt third-party Promise APIs once at the boundary. Do not build parallel Promise-based orchestration.
- Fixtures also use Effect. `Effect.runPromise` belongs at Worker handlers or callbacks whose platform contract requires a Promise; synchronous transaction callbacks must stay synchronous.
- Use `@effect/vitest` for Effect tests. Verify error classification, cancellation, cleanup, and deliberate bad observations; matching wrong results must never pass.
- Every new case requires a named, behavior-specific negative example exercising its actual oracle. API cases must register an independent positive observation and semantic mutations in `test/case-oracles/`; `pnpm check` enforces registry coverage. Lifecycle and qualification cases require corresponding targeted oracle tests. Generic transport errors and malformed envelopes do not satisfy this requirement.
- Keep fixtures identical across reference and candidate runtimes. Keep orchestration separate from semantic expectations.
- `pnpm check` is the static/unit gate. `pnpm test:reference` exercises two independent workerd instances. `pnpm test:local` exercises workerd versus actual celld and MinIO.
- Preserve `docs/DESIGN.md`. Report which checks ran and distinguish local validation from AWS qualification.
