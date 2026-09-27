# Tracked compatibility bugs

[bugs.json](bugs.json) is the machine-readable registry used by the runner. Its version-scoped expectations were rechecked on celld **0.6.0** on September 26, 2026. The upstream reports document the original v0.5.0 observations and remain local drafts.

| ID       | Status | Bug                                                                                      | Affected cases                                  |
| -------- | ------ | ---------------------------------------------------------------------------------------- | ----------------------------------------------- |
| CELL-001 | Open   | [Repeated body reads and null-body state](upstream/body-consumption.md)                  | `http.body-consumption`, `repro.body-readers`   |
| CELL-002 | Open   | [Negative list limit error class](upstream/storage-list-validation.md)                   | `storage.invalid-input`, `repro.storage-errors` |
| CELL-003 | Open   | [Uncloneable storage value error class](upstream/storage-clone-errors.md)                | `storage.invalid-input`, `repro.storage-errors` |
| CELL-004 | Open   | [Missing directory index for a trailing-slash path](upstream/asset-directory-index.md)   | `assets.html-routing`                           |
| CELL-005 | Open   | [EC public key export formats](upstream/webcrypto-ec-key-export.md)                      | `crypto.ecdsa-p256`                             |
| CELL-006 | Open   | [SubtleCrypto key usage and length validation](upstream/webcrypto-input-validation.md)   | `crypto.invalid-input`                          |
| CELL-007 | Open   | [node:stream web-stream interop](upstream/node-stream-web-interop.md)                    | `node.stream-timers`                            |
| CELL-008 | Open   | [websocket_standard_binary_type not applied](upstream/websocket-standard-binary-type.md) | `flags.enabled-defaults`                        |

## Run policy

By default, all cases still execute. A candidate that violates the semantic contract but exactly matches a registered observation on the registered celld version and compatibility date/flags is reported as **known-bug**. This status does not fail the suite, is counted separately from passes and intentional divergences in JSON, and appears as a JUnit skipped result with bug IDs. Both observations remain in the evidence bundle. Each run saves the registry and policy in `bugs.json`.

```sh
pnpm test:local
pnpm tck --profile local --suite repros
# Fail on all compatibility bugs, including registered ones:
pnpm tck --profile local --known-bugs error
```

`--known-bugs error` disables waivers without changing expectations. Reference runs never receive waivers. Unexpected passes require retiring or revising the expectation; they fail under the default policy. Changed observations, unreviewed celld versions, transport errors, assertion defects, reference failures, and setup/cleanup errors still fail. Existing intentional cache/RPC divergences and the documented secret-key JWK export restriction in `crypto.key-export` retain their separate policy.

## Maintaining the registry

1. Reproduce a bug against a valid reference and celld. Add a report with the contract, reproduction, version, evidence, and impact.
2. Assign a stable ID, owner, review date, and status in `bugs.json`. Add the upstream issue URL when submitted; `null` means no issue has been linked.
3. Register each affected case with its exact candidate observation, celld version, compatibility date, and compatibility flags. Review the entire observation; do not add wildcard errors or regenerate expectations automatically from failures. Shared cases can link multiple bugs, and fixing any one requires reviewing the combined expectation.
4. After a verified fix, remove or revise affected expectations, mark the bug fixed, and update this table. An expectation may only reference open bugs. A runtime upgrade requires revalidation, even if the old behavior persists.

The registry is schema-validated before provisioning. Unknown cases, duplicate registrations/IDs, and missing or fixed bug references are rejected. These narrowly scoped expectations acknowledge bugs; they do not change the semantic oracle or claim API compatibility.

## Infrastructure investigations (not waived)

| ID        | Status                                           | Observation                                                                                                                                                        | Policy                                                                                                                                         |
| --------- | ------------------------------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------ | ---------------------------------------------------------------------------------------------------------------------------------------------- |
| INFRA-001 | Transport workaround; upstream cause unconfirmed | Intermittent connection reset during the MinIO conditional-write diagnostic, including `tck-7afe9b5b-93c9-4be7-96c8-6ea25573fb09`. See [FINDINGS.md](FINDINGS.md). | Setup fails; no automatic retry or registry exemption. The operation may have committed, and this is not a semantic compatibility observation. |

All setup diagnostics now use the storage proxy with a fresh MinIO connection per request. Qualification runtime storage uses the same connection policy; other runtime suites still access MinIO directly. This avoids reusing connections after rejected conditional writes, without retrying or accepting failed diagnostics. It is a harness transport workaround, not a fix to celld or MinIO.

The three-node setup also exposed a celld v0.5.0 operational limitation: a healthy ensemble with one follower does not automatically expand when a second follower joins. This is tracked in [RESILIENCE.md](RESILIENCE.md) as a scenario precondition, not assigned an API waiver or treated as a proven contract violation.
