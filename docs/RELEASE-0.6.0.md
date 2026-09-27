# celld v0.6.0 upgrade assessment

The TCK pins [celld v0.6.0](https://github.com/denoland/celld/releases/tag/v0.6.0) at OCI index digest `sha256:e188a7f2bb0b8cec9fb04ee4c3d1ed7cca0ea0419519ae2a9ba36e5b6fe5161b`. The base runtime, telemetry CLI, and upgrade overlay use this image. The workerd reference remains 1.20260730.1 at compatibility date 2026-07-30; the Effect packages remain pinned to 4.0.0-rc.115.

## Compatibility review

- `facets.outbound-transaction` now passes the independent workerd oracle. The call inside a root transaction reaches the gateway and preserves the facet write. The previous divergence was retired, and a named semantic mutation rejects the former refusal. Facet isolation and explicit commit/rollback also pass.
- All nine registered known-bug observations match the v0.5.1 observations exactly. Their version scopes, review dates, and evidence runs were updated after comparison. These remain known bugs, not compatibility passes.
- The always-miss cache, secret-key JWK export restriction, and cross-isolate RPC stub restriction remain documented in the [v0.6.0 compatibility contract](https://github.com/denoland/celld/blob/v0.6.0/docs/cloudflare-compat.md). Their exact observations are unchanged; their version scopes were revalidated.
- The rolling upgrade starts all three nodes on the digest-pinned v0.5.1 image and replaces each with v0.6.0. It explicitly uses **bucket durability**: the release requires a whole-fleet stop when upgrading with fleet durability. Both mixed-version stages must accept acknowledged writes through every live node and retain every receipt at completion.

The initial API review is recorded in `tck-3b8dd9ff-6e2d-475c-a43b-46829979609f`, and the isolated bug repros in `tck-45ae6091-9437-4d15-ba25-99890bbf974f`. Those review runs intentionally failed the old version guards; they are evidence for updating expectations, not passing-suite claims.

## Local validation

- `pnpm check`: passed formatting, lint, all three TypeScript projects, and 574 tests across 36 files.
- `pnpm test:reference`: all 95 cases passed in `tck-e0cd1f99-b185-43e2-a88f-35d6065bf335`.
- `pnpm test:local`: passed in `tck-a16cf5c3-8a36-48ab-b1d2-f0c66c366c63` with 91 ordinary passes, three reviewed divergences, seven known bugs, and no failures or infrastructure errors.
- `pnpm tck --profile local --suite repros`: passed in `tck-37431a1f-5445-4ab5-8a94-67a9c8c80815`, reproducing both registered known-bug observations.
- `pnpm test:operations`: all six cases passed in `tck-e5bfbdaa-b62e-495f-8236-e0ec821464e1`.
- `pnpm tck --profile local --suite operations --case operations.binary-upgrade`: passed in `tck-a3ff7ee6-1799-4b3d-9ef8-d80372ec943e`, including both mixed-version stages.
- `pnpm test:fleet`: all five cases passed in `tck-94787c14-a98f-4536-b850-84b782374e9c`, covering routing, owner failover/rejoin, storage partition, and follower recovery.
- `pnpm test:recovery`: all six cases passed in `tck-7fb08e70-9b98-43fe-8d96-cbe116aa327f`, including facet persistence, local disk loss, and storage outage.

These are local Docker, MinIO, and workerd checks. They do not qualify AWS, managed Cloudflare, or a fleet-durability binary upgrade.

## CI provisioning repair

The initial PR checks could not pull either pinned MinIO image from Quay (`401 Unauthorized`), before exercising celld. Cached images masked this locally. Compose now builds the same MinIO and mc releases from official GitHub release binaries, with an explicit SHA-256 checksum for each amd64/arm64 binary and a digest-pinned Alpine base. No runtime assertions or bug waivers change for this repair.

Both architecture builds and their reported release versions were checked. `pnpm check` still passes all 574 tests. A fresh Compose project using the replacement images passed `facets.outbound-transaction` in `tck-013e0666-2b2a-473a-81e7-67606794d250`; setup also exercises conditional object-store writes.

## Release-specific coverage still to add

The pin upgrade revalidates existing cases; it does not fully qualify every release-note claim. Targeted cases remain for:

- Ed25519/NODE-ED25519 interoperability and X25519 raw-key import/export and low-order rejection.
- `ctx.exports` entrypoint fetch and facet creation; first-open migration of a persisted v0.5.1 facet database.
- New `WorkerCode` rejection rules, nested `transactionSync()` calls and callback arity, and relative imports inside Dynamic Worker subdirectories.
- R2 empty path segments, non-ASCII and percent-encoded list keys, and the legacy trailing-slash key behavior.
- Invalid SQLite UTF-8, Unicode header values, close-frame `wasClean`, the root `run_worker_first` route, re-armed alarms with pending timers, and WebSocket delivery during a running message handler.
- Per-cell memory bounds and follower log-tail range evidence under recovery faults.

Each addition needs an independent positive observation and a named behavior-specific negative example as required by [AGENTS.md](../AGENTS.md). The outstanding feature coverage in [RELEASE-0.5.1.md](RELEASE-0.5.1.md) also remains open.
