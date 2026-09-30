# Prepared upstream reports

Investigated September 18, 2026. These are **drafts, not submitted issues or validated fixes**.

The two original failing TCK cases reduce to three independently actionable reports; two further Web Crypto reports were added later:

1. [Body readers permit repeated consumption](body-consumption.md).
2. [Invalid storage list limits throw the wrong error class](storage-list-validation.md).
3. [Uncloneable storage values throw TypeError instead of DataCloneError](storage-clone-errors.md).
4. [EC public key export returns SPKI for the raw format and omits JWK metadata](webcrypto-ec-key-export.md).
5. [SubtleCrypto does not enforce key usages or key lengths](webcrypto-input-validation.md).
6. [`node:stream` Readable.toWeb/fromWeb throw TypeError](node-stream-web-interop.md).
7. [`websocket_standard_binary_type` is not applied](websocket-standard-binary-type.md), observed in `tck-fe61abc7-a30e-41b9-96d7-ca3a68a09757`; reproduce it with `pnpm tck --profile local --suite flags`.

Reports 4 to 6 were found by the runtime API cases in the regular `core` and `node` suites, not by the isolated `repros` fixture. Reproduce them with `pnpm tck --profile local --suite core --case crypto.ecdsa-p256`, `--suite core --case crypto.invalid-input` (evidence run `tck-0c3c37cf-d018-4e54-b741-aeec138d7628`), and `--suite node --case node.stream-timers` (evidence run `tck-3168c226-4b59-4847-b179-739fd47fa9f7`). Their source investigation is not pinned to an upstream commit.

A later report came from the extensions suite rather than the repro fixture:

4. [Static assets do not serve a directory index for a trailing-slash path](asset-directory-index.md), observed in `tck-61e8bd92-7c94-40b3-a868-a9fa40881eec`. It has no source investigation; reproduce it with `pnpm tck --profile local --suite extensions --case assets.html-routing`.

The source investigation is pinned to upstream commit `12d5b6333fe52717325addcfe1e99e9fd4f77bcd`. Live GitHub checks confirmed that commit was still `main` and v0.5.0 was still the latest release. Targeted issue searches for body consumption/bodyUsed/DataCloneError and storage TypeError did not find a matching report; that is not an exhaustive duplicate guarantee. No messages, issues, or PRs have been sent upstream.

## Reproduce

The isolated fixture is [fixtures/repro/worker.ts](../../fixtures/repro/worker.ts), with one SQLite DO binding and no application/service/Node dependencies. It uses the same pinned Effect v4 runtime as the rest of the TCK. Both engines receive identical built JavaScript, verified by SHA-256.

```sh
pnpm install --frozen-lockfile
pnpm tck --profile reference --suite repros
pnpm tck --profile local --suite repros
```

Both commands now exit 0 under the default [known-bug policy](../BUGS.md); celld results are `known-bug`, not passes. Add `--known-bugs error` to reproduce the original nonzero exit. Run one report group with `--suite repros --case repro.body-readers` or `--suite repros --case repro.storage-errors`. The regular `all` suite remains the original 63-case corpus; `repros` is an explicit diagnostic suite, not part of the default corpus.

Read `report.json` and the `repro/case-*.json` records in the printed evidence directory. Raw HTTP and Docker commands are retained. [observations.json](observations.json) contains a checked-in, path-free copy of the actual observations and runtime/module identities; it is evidence, not a snapshot used to set test expectations.

Validated runs:

- Reference self-check: `tck-ddfc1bfb-9f36-4929-bae9-c30c9a2aeb59`, both cases pass.
- Actual celld/MinIO: `tck-fdff5d81-b4bf-45b9-a651-debe3808879d`, both cases fail their independent expectations, no setup/cleanup error.

The body report covers ten reader/type combinations and two null-body controls. The storage report covers asynchronous/synchronous list validation, three serialization paths, a valid-list control, and rejected-write atomicity.

## Handoff

The report files contain concise reproductions, observed/expected behavior, pinned source links, impact, and repair boundaries. Keep the three fixes independently reviewable. Before treating a patch as ready, rerun `repros`, the regular core suite, and upstream's native runtime tests; the public repository's private test includes are not available here. Neither the current source investigation nor a proposed repair direction proves that a runtime patch is correct.
