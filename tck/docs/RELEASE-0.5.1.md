# celld v0.5.1 upgrade assessment

The TCK pins the [v0.5.1 release](https://github.com/denoland/celld/releases/tag/v0.5.1) at OCI index digest `sha256:9df15352bcbb92a8d73dabadc349383ccd3d7851b2ba4ccffe2c956f2f9fc93f`. The rolling upgrade scenario pins v0.5.0 as its starting release. The workerd reference remains 1.20260730.1 at compatibility date 2026-07-30.

## Covered by this upgrade

| Release behavior                       | TCK evidence                                                                                                                                         |
| -------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------- |
| `WorkerCode.limits` is accepted        | `dynamic.limits` now passes on v0.5.1; its v0.5.0 divergence was retired. This case does not claim limit enforcement.                                |
| KV `put()` accepts a `ReadableStream`  | `kv.stream-put` verifies all binary chunks, metadata, and deletion on both runtimes.                                                                 |
| KV rejects an oversized streamed value | `kv.stream-limit` writes 26 MiB, above the documented 25 MiB maximum, and checks both rejection and absence of a partial value.                      |
| Rolling update from v0.5.0             | `operations.binary-upgrade` moves one node at a time across a three-node MinIO fleet and checks acknowledged writes during both mixed-version steps. |

The reference observations for the KV cases were captured in runs `tck-c1b2e1d0-8efe-4dbf-aecc-4809f74146ba` and `tck-9d1891f6-1f3e-423e-95b0-9dfd4510ab06`. The v0.5.1 candidate passed `kv.stream-limit` in `tck-02b04c50-4b51-4979-9dfc-400908fe7109`. The API sweep `tck-ff703cf6-0646-47ba-bee7-d6e6db1f5f36` passed with 90 ordinary passes, four reviewed divergences, and seven known bugs. The repros `tck-5c8b38f9-3579-4eb7-9f77-95aef1cdc063` reproduced two more known bugs. The rolling upgrade passed in `tck-3b987589-818c-4067-8d73-482511a42434`. These are local Docker, MinIO, and workerd results; they do not qualify AWS or managed Cloudflare.

## Cases to add

1. **Dynamic Worker limit enforcement and tail reports.** Exercise an over-budget subrequest and CPU workload with a within-budget control, then verify the Tail Worker receives request, response, console, exception, and outcome records. The pinned workerd does not enforce a custom `subRequests` budget locally, so enforcement needs a celld-only qualification oracle. Tail delivery should have a distinct negative example for each promised field.
2. **`celld r2` CLI.** Round-trip put/get/list/inspect/delete against an `r2_buckets` binding with the node stopped. Assert HTTP and custom metadata survive, and assert a missing key or invalid binding fails without changing another object.
3. **Deploy bundling.** Add a deployment check for Wrangler `define` and Text, Data, and CompiledWasm `rules`, with an actual served result. An invalid module rule should be rejected without replacing the active deployment.
4. **Eviction and state reporting.** Distinguish `/evict/<cell>` completion from refusal, cancellation, and failure with owner and data checks. Validate `/state` isolate counts and memory counter types before and after an isolated load, without assuming exact allocator values.
5. **Node compatibility.** Qualify mutable built-in modules, module-loading `process` fields, and RSA `KeyObject.toCryptoKey()` algorithm, usages, extractability, and signature verification. The pinned workerd returns `The toCryptoKey method is not implemented`, so this RSA fix needs a celld-only oracle or a newer independent reference.
6. **Memory and compaction.** Hold a slow cell across retained LTX bundle compaction while memory pressure is induced; require acknowledged data and a clean drain. This needs a fault scenario with direct evidence that the old segments left the in-memory index.
7. **Azure R2 metadata rules.** Verify the #209 fix against actual Azure Blob Storage. MinIO cannot qualify Azure's metadata-name restriction.

The first three directly cover new public features; the others cover operational fixes or environment-specific claims. Each new case needs the named bad observation and independent oracle required by [AGENTS.md](../AGENTS.md).
