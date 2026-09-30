# Correctness review fixes

Validated locally on September 18, 2026, based on HEAD `296b795`.
The storage diagnostics, node-local memory pressure, and diagnostics proxy
network fixes from `a82026e` and `296b795` are retained. No semantic
expectations were regenerated.

## Changes

- Persist successful history reads as fsynced `observed` ledger records.
  Subsequent checks and resumed audits enforce their sequence and payload,
  including writes whose original acknowledgment was uncertain. Unobserved
  writes may still legitimately be absent.
- Propagate checker/comparator defects and interruption through divergence
  evaluation; neither can become an accepted waiver.
- Finalize recovery, multinode, and qualification reports after resource
  cleanup using an interruption-safe exit handler. Preserve original failures,
  cancellation, cleanup errors, and unfinished case placeholders.
- Decode independent fixed gzip/zlib vectors in the fixture; independently
  decode emitted compressed bytes in the host oracle. Encoder byte equality
  is not required.
- Keep host Node.js in `environment.hostNode` and fixture metadata under
  `environment.fixtures`. Validate API provenance with `ApiEnvironment`.
- Require reviewed compatibility dates and flags for known bugs and divergences,
  in addition to the existing case/version/observation constraints. Flag order
  does not change a profile.

## Validation

`pnpm check`: 63 tests passed across 22 files, plus formatting and all three
TypeScript checks. Regressions cover disappearing/reassigned observed writes,
resumed audit constraints, checker defects and interruption, changed profiles,
independently broken compression/decompression, full API-run provenance, and
report success/failure/cancellation/cleanup semantics.

| Command               | Result                                 | Evidence under `artifacts/`                            |
| --------------------- | -------------------------------------- | ------------------------------------------------------ |
| `pnpm test:reference` | 63 passes                              | `tck-13192ceb-f4bb-4aa1-905d-1c4c0795022c/report.json` |
| `pnpm test:local`     | 65 passes, 2 divergences, 2 known bugs | `tck-297b800b-6333-4851-9b07-6fa0d96f09a7/report.json` |
| `pnpm test:recovery`  | 5 passes                               | `tck-02aa68e9-ee20-429e-82e9-0a06527246c0/report.json` |
| `pnpm test:multinode` | 4 passes                               | `tck-ae775499-a92c-4d05-ad6a-04d7198309f7/report.json` |
| `pnpm test:traffic`   | 3 passes                               | `tck-f0d7f152-586d-47cd-8f46-216be30c26e7/report.json` |

Local commands used
`TCK_COMPOSE_BIN=/Users/ewhauser/working/celld-tck/.cache/tools/docker-compose`.
Initial attempts without that setting failed provisioning and produced failure
reports. Those attempts are not counted as passing validation.

The two divergences were `cache.documented-miss` and `rpc.returned-target`.
The known bugs were `http.body-consumption` (CELL-001) and
`storage.invalid-input` (CELL-002 and CELL-003). These remain separately
reported, not compatibility passes.

The traffic restart-race run persisted 1,379 read-observation records,
including two distinct uncertain writes, and its independent audit passed.
After the final ledger change, rerunning `pnpm tck --profile local --suite traffic
--case traffic.restart-races` also passed with 212 acknowledged and 214 recovered
writes (`tck-25a23898-637d-4897-b782-dda908ab1f30/report.json`).
Both final API reports retain host Node.js `v26.5.0` alongside separate core,
node, and extensions fixture metadata.

## Real interruption verification

Sent SIGINT directly to the Node CLI process after owned Docker resources
appeared (setup), and after a completed lifecycle stage or a persisted traffic
acknowledgment (execution). All six runs exited 130, wrote both `report.json`
and `junit.xml`, retained terminal placeholders, recorded interruption,
reported `success: false`, and left no owned containers or volumes.

| Suite     | Setup run                                  | Execution run                              |
| --------- | ------------------------------------------ | ------------------------------------------ |
| recovery  | `tck-053cc04c-aa52-470f-82a5-2ea262a3a0b7` | `tck-a9a5356c-7ba2-44cd-8c90-ddc4706bc9e8` |
| multinode | `tck-b5b14242-a21f-422e-aa99-5fdbea7b46ef` | `tck-31c2e714-6dcc-4f0e-ba1b-c636d8bf9c22` |
| traffic   | `tck-fac899c6-7286-499f-9f84-f647dc367f3e` | `tck-fcdc92a5-41ea-4f83-887d-28b77956e89e` |

Reports are under `artifacts/interrupt-<suite>-<phase>/<run>/`.
The local signal harness and machine-readable cleanup assertions are retained
in `artifacts/review-validation/`. Cleanup failure is covered by deterministic
Effect tests; no destructive Docker cleanup failure was induced.

These results establish local validation only. AWS qualification, the complete
fault/capacity matrix, and unhandleable process termination such as SIGKILL were
not tested. No publication, commit, push, or PR was requested or performed.
