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

## Additional release coverage

Fifteen new differential cases exercise Ed25519/NODE-ED25519 with RFC 8032 vectors; X25519 raw keys, RFC 7748 derivation and low-order rejection; nested synchronous transactions, callback arity and rollback; invalid SQLite UTF-8; Unicode header construction and fetched response decoding; R2 key identity and list encoding; `ctx.exports` default/named fetch and facet classes; relative Dynamic Worker imports; WorkerCode rejection rules; wrapped WASM; root worker-first routing; alarm rearming with pending timers; close-frame `wasClean`; and outgoing WebSocket streaming during a running hibernatable handler.

The root routing fixture has an actual index asset and no catch-all worker-first rule, so `/` must work independently. The WebSocket case measures three frames on the public listener with one client's monotonic clock. Its handler sleeps 500 ms twice; the oracle requires at least 750 ms from first to last receipt and 250 ms from second to last. Correct payloads delivered in one batch fail. Raw receipt timestamps are saved. Each new case has an independently captured workerd observation and named semantic mutations, including rejection of matching wrong results.

Validation of these additions:

- `pnpm check`: 633 tests across 36 files, plus formatting, lint and TypeScript.
- `pnpm test:reference`: 110 passes in `tck-85100d73-461c-47eb-96af-af2ca7631c6a`.
- `pnpm test:local`: 104 passes, three divergences and nine known-bug results in `tck-d8c0f5ab-dea6-482a-84d5-c2a809c1ab17`.
- The final raw-timestamp WebSocket oracle was then validated against both runtimes in `tck-45a6130d-d82f-4327-9418-7559beda0a4f`. The independent corpus includes that final observation.

The two new API findings are [CELL-009](upstream/x25519-low-order.md), a generic X25519 error class, and [CELL-010](upstream/r2-trailing-slash-list.md), omission of a readable trailing-slash object from R2 listing. Their exact observations are scoped to 0.6.0. They remain compatibility defects, not passes; strict `--known-bugs error` mode rejects them.

## Remaining release-specific gaps

The publicly observable API additions above do not establish every implementation claim:

- First-open migration of persisted v0.5.1 facet databases and legacy R2 trailing-slash keys are not covered by the current suite. The rolling binary-upgrade case continues to verify acknowledged ledger writes.
- Per-cell memory bounds need a native allocation or statement-cache metric. Process RSS alone cannot prove the 128 KiB compiled-statement cap, shared object-store client ownership or absence of per-operation timer leaks. No arbitrary RSS threshold is treated as proof.
- Follower range-evidence rejection needs a controlled failed, incomplete or legacy tail response after sealing. Existing fleet recovery tests prove acknowledged-write retention, but do not inject faults specifically between the seal response and the range-certified tail response. The v0.6.0 internal protocol distinguishes these stages; ordinary whole-node/network outages do not independently exercise the new guard.

Memory and follower-tail verification require additional instrumentation or a targeted peer-protocol fault harness. AWS, managed Cloudflare, the outstanding feature coverage in [RELEASE-0.5.1.md](RELEASE-0.5.1.md), and other distributed schedules remain unqualified. All new evidence above is local workerd/Docker/MinIO evidence.
