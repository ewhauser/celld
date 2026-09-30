# Local compatibility findings

## Hosted CI follow-up

[The first README revision's CI run](https://github.com/ewhauser/celld-tck/actions/runs/35384604319) failed in five jobs. Local API, multinode, resilience, and rolling-deployment setup failed on the conditional update immediately after the diagnostic's expected rejected create. The fault proxy also reproduced this failure with upstream connection pooling enabled. Setup diagnostics now use fresh upstream connections through the proxy; qualification runtime traffic uses the same policy. Other runtime traffic remains direct to MinIO. No write retries, diagnostic waivers, or semantic expectation changes were added. The underlying celld/MinIO interaction remains tracked as INFRA-001.

The insufficient-spare scenario sent allocation requests through a Durable Object, which could route them to its owner instead of the constrained node. CI retained a successful 96 MiB allocation response from a node limited to 8 MiB, while that receiving node remained alive. Memory pressure now runs in the receiving Worker. A workerd regression test rejects any Durable Object dispatch for that endpoint; a separate network test verifies fresh proxy connections, preserved rejection responses and request bodies, and no extra writes. Both regression tests failed before the fixes and passed afterward.

Local validation passed `pnpm check` (51 tests), the full API suite (`tck-a750e7b9-e25d-4e40-9f7b-b0254cd77520`), and the insufficient-spare scenario (`tck-79572b57-1b8d-4230-a8c0-7b78c1f83a52`). API results retain two known-bug cases and two intentional divergences. The 63-case reference self-check also passed (`tck-9a316b94-eb88-41ad-a992-0290d571e8c0`). The first hosted follow-up passed static checks, reference, and local API jobs, but exposed missing proxy connectivity on the older multi-node storage network. After attaching the proxy to that network, all four local multinode cases passed (`tck-a8afcf87-e200-40af-987a-adc5a170b07d`). Full hosted validation is pending; no AWS qualification was performed.

The suite compares pinned celld v0.5.0 with Miniflare 4.20260730.0 / workerd 1.20260730.1 at compatibility date 2026-07-30. All authored TypeScript uses Effect 4.0.0-rc.115. These are local observations, not AWS or managed Cloudflare qualification.

## Full checklist qualification, September 18, 2026

The full local P0/P1/P2 checklist is implemented in [QUALIFICATION.md](QUALIFICATION.md). The initial complete 19-case run passed (`tck-43c1b1ac-fe99-4278-8db6-54f696dcac30`). Final review strengthened exact-object fault evidence, workflow return-value validation, and independent KV enumeration so partial KV-only commits cannot escape the SQL-history oracle. The final run with all these checks passed **19/19**, with no harness errors (`tck-7db577c9-6ae1-406e-ba67-31607c5282e9`).

The regression gates completed with no harness errors:

| Gate                     | Result                                                 | Evidence run                               |
| ------------------------ | ------------------------------------------------------ | ------------------------------------------ |
| Full qualification       | 19 passed                                              | `tck-7db577c9-6ae1-406e-ba67-31607c5282e9` |
| Static and unit checks   | Formatting, three TypeScript projects, 49 tests passed | `pnpm check`                               |
| Reference corpus         | 63 passed                                              | `tck-24b9b35b-1ba4-45d1-9d3f-5af6a834e2fc` |
| Local API and deployment | 65 passed, 2 divergences, 2 known bugs                 | `tck-24bf595d-9337-4e34-ab9e-85ecb2ba119d` |
| Single-node recovery     | 5 passed                                               | `tck-40411ea4-a45c-461c-837c-a1dc03c722ec` |
| Two-node bucket          | 4 passed                                               | `tck-e5afeaa7-ffa6-4b63-b805-88e51e60843f` |
| Two-node fleet           | 5 passed                                               | `tck-95cf9417-d7b5-4dd9-9450-53ab50fd799d` |
| Three-node resilience    | 10 passed; final fault declared data loss              | `tck-b6d5180a-1e42-4332-8d17-d6b4c42ba010` |

The latest all-replica-disk-loss case lost acknowledged operations 150, 151, and 154, with a matching loss declaration. Its passing status verifies declared loss outside the surviving-copy assumption; it is not an acknowledged-write preservation result.

The final traffic cases exercised 781 operations: 412 durable acknowledgments all recovered, plus three fully committed operations whose responses were uncertain. Reopened ledger audits and independent KV scans passed. Paused-owner takeover produced no successful receipt from the old activation after takeover. These numbers describe this seeded local schedule, not a general throughput measurement.

The capacity cases verified 16 MiB of pseudorandom restored payloads, four simultaneous slow 8 MiB readers, all 500 queued message IDs, and recovery after both memory pressure and unavailable spare capacity. This run recorded 266 ms for the post-restart blob verification request (excluding startup and lease wait), 13,096 ms for the slow readers, and 5,270 ms for queue submission/drain. These bounded local observations are not production capacity or latency guarantees. Both memory scenarios preserved all 24 baseline acknowledgments after capacity was restored.

The lost-response storage case recorded 37 successful LTX PUT responses dropped after upstream completion. Every fault case requires evidence on the exact test object's data path; neither unrelated lease traffic nor a generic HTTP error can satisfy this requirement. A separate focused fault run also passed all six cases (`tck-c9e60d32-1d5c-4482-aaae-58493d3c4a89`), and the stricter workflow-output check passed independently (`tck-a7a3947f-844b-4ce5-8a8b-0cd3b2c11cb7`).

Docker inspection after the final run found no remaining Compose-owned containers, volumes, or networks, including resources from retained failed development runs. CI now defines eleven independent jobs but has not run remotely. AWS/managed Cloudflare adapters and execution remain separate work requiring dedicated environments. The two existing API bugs, two documented divergences, and INFRA-001 remain visible; no new qualification waiver was introduced.

### Qualification development failures

Failed runs remain in `artifacts/`; no qualification failures were waived. Development exposed and corrected concurrent fleet startup, Docker port restoration after network reconnection, blob verification, and container memory-limit restoration:

- `tck-42acbe89-6a2d-4962-833e-bffc386a066d` and `tck-fece581a-e70d-42f9-9180-061dc350eb1f`: dependency recovery exposed sequential startup waiting for peers that had not yet been started; the latter also retained a diagnostic setup failure. Recovery now starts all nodes concurrently.
- `tck-994beac9-d1f9-4949-b8eb-e6616c834826`: peer reconnection did not restore Docker's published port. The harness now verifies history through a peer before restarting the reconnected node and refreshing its endpoint. The rolling case also retained a storage-diagnostic failure.
- `tck-36d05b4b-891c-4dc6-8b69-65816a013ec9`: blob validation and the initial spare-capacity assumption failed. The fixture now verifies deterministic pseudorandom bytes; insufficient-spare testing requires both constrained replacements to be stopped.
- `tck-bc5acb8b-b97f-49c1-a344-bf86d1177b6d`: Docker's `--memory 0` did not remove the imposed limit. Recovery now raises and verifies each constrained node's limit to 512 MiB.
- `tck-6709f125-e671-4501-b698-302f9284d995`: Docker reported exit 137 under the verified limit without setting `OOMKilled`. Reports retain both observations separately; the scenario requires a successful verified allocation or an observed termination under the imposed limit, and full acknowledged-data recovery afterward.

Qualification now diagnoses the same proxy route it uses for storage before enabling faults. This does not resolve or waive INFRA-001 on the original direct-MinIO path. Negative unit cases reject wrong-object fault evidence, missing/duplicate/corrupt/orphaned history, stale-owner receipts, malformed ledgers, and incorrect workflow output. Actual network proxy tests exercise latency, timeout, throttling, and response loss after upstream success.

## Earlier local validation, September 18, 2026

| Gate                     | Result                                                 | Evidence run                               |
| ------------------------ | ------------------------------------------------------ | ------------------------------------------ |
| Static and unit checks   | Formatting, three TypeScript projects, 41 tests passed | `pnpm check`                               |
| Reference corpus         | 63 passed                                              | `tck-0e01dfdd-48c7-4fd5-9c0a-04ebb732989e` |
| Local API and deployment | 65 passed, 2 divergences, 2 known bugs                 | `tck-bf4d440f-34e8-41a2-868c-c6f4cbe684c0` |
| Single-node recovery     | 5 passed                                               | `tck-cc3dd0e4-3074-4da5-852d-26f96c2ea785` |
| Two-node bucket          | 4 passed                                               | `tck-76b343e8-2bcf-4954-988f-d6598c9e8b40` |
| Two-node fleet           | 5 passed                                               | `tck-2fe48cbd-ea04-4c0a-81b6-4ff8620f7c83` |
| Three-node resilience    | 10 passed; final fault explicitly declared data loss   | `tck-6905c62a-9c6f-40dd-ae5e-770f398a9be3` |

All six runtime commands exited zero with no harness errors. Docker inspection confirmed that their owned containers, volumes, and networks were removed; the three failed development runs were also clean. The CI matrix is configured but has not run remotely. No AWS or managed Cloudflare execution is claimed. The all-disk-loss result and retained development failures are explained below.

## Known-bug policy validation

- `pnpm check`: formatting, three TypeScript projects, and 41 harness tests passed.
- Full local run: **65 passed, 2 divergences, 2 known bugs**, exit 0 (`tck-bf4d440f-34e8-41a2-868c-c6f4cbe684c0`).
- Reference reproductions: **2 passed**, exit 0 (`tck-e1f63e03-89b2-4b67-a101-206498064bb0`).
- Local reproductions: **2 known bugs**, exit 0 (`tck-7c0e3f3e-47bd-46ed-9a5f-a759e0d255e1`).

- Strict local reproductions (`--known-bugs error`): **2 failures**, exit 1, no harness errors (`tck-6ca002bf-eac4-4440-8c6c-d6af63908d0f`).

## Original strict baseline

- Reference self-check: **63/63 passed** (`tck-2be81d98-acd3-4f22-92e6-eb0a63c09b3d`).
- Full local run: **65 passed, 2 documented divergences, 2 failures** across 63 differential cases and 6 deployment checks (`tck-7c8c6cf9-b2f3-4d2d-b2c0-8c21b7fbca7d`). All cases reached terminal results; no case-level infrastructure/reference errors.
- Exit status is 1 for the local run because the two unexpected differences remain failures. CI has been configured but was not run remotely.

Evidence is under `artifacts/<run-id>/report.json` and accompanying files; artifacts are intentionally ignored by Git. The full local run includes the Workflow retry/event and extension cases, unlike earlier incremental runs.

## Unexpected differences

| Case                    | workerd expectation                                                                             | celld observation                                                             |
| ----------------------- | ----------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------- |
| `http.body-consumption` | Reading an already-consumed Request body rejects with `TypeError`.                              | A second `.text()` call is accepted. Cloning after consumption still rejects. |
| `storage.invalid-input` | Negative list limit rejects with `TypeError`; storing a function rejects with `DataCloneError`. | Both operations reject, but with `Error` and `TypeError`, respectively.       |

Both remain semantic contract violations. The [bug registry](BUGS.md) now classifies their exact celld 0.5.0 observations as `known-bug`; default runs allow these results, while `--known-bugs error` preserves strict failure behavior. Unexpected observations remain failures. The error-type difference is a compatibility observation, not a claim of data loss.

## Isolated upstream reproductions

The dedicated `repros` suite passes both cases on the reference self-check and reproduces both violations against celld v0.5.0, with no setup or cleanup errors. Current default runs classify both as known bugs. It confirms repeated consumption for all five readers on both Request and Response, incorrect `bodyUsed` after reading null bodies, and storage error-class differences in asynchronous, synchronous, and batch operations. The rejected batch produced no partial writes.

[Three prepared upstream reports](upstream/README.md) include minimal reproductions, pinned source analysis, repair boundaries, and checked-in observations. They remain local drafts; no upstream fix or issue submission is claimed.

## Documented divergences

- `cache.documented-miss`: celld deliberately implements an always-miss cache. The candidate must return no match and `false` for deletion after `put`; workerd must retrieve the stored response. [celld cache contract](https://celld.dev/docs/cloudflare-compat/#cache).
- `rpc.returned-target`: workerd can return a live `RpcTarget` and invoke it; celld rejects the stub crossing an isolate boundary. The fixture captures the rejection as an observation. [celld RPC contract](https://celld.dev/docs/cloudflare-compat/#rpc).

These expectations are restricted to celld 0.5.0, have exact candidate checks and review metadata, and are reported separately. An unexpected conformance pass or changed candidate result fails pending review. They do not apply to the reference-versus-reference self-check.

## Infrastructure evidence

Three retained runs encountered a connection reset during celld's conditional-write storage diagnostic, before API cases started:

- `tck-9b34c9d9-de59-4eaf-9fab-41dc76326aae` (extensions setup).
- `tck-fa6996a9-1724-4166-80ea-e848ecb2bebe` (core setup).
- `tck-7afe9b5b-93c9-4be7-96c8-6ea25573fb09` (three-node resilience setup).

The diagnostic failed while updating its probe object, reported that the write may have committed, and returned exit 1. The harness did not bypass or automatically retry it. The raw diagnosis is retained in each run's `commands.jsonl`; cleanup ran and unexecuted cases remained infrastructure errors. Other runs completed storage diagnostics and all selected API cases. This transient local storage/transport issue remains unresolved and is not relabeled as API incompatibility.

## Local process recovery

The separate [recovery suite](RECOVERY.md) passed graceful restart, SIGKILL recovery of acknowledged state, and an alarm due while the process was stopped (`tck-aa23e90c-8fe3-4491-92bd-6dc419459de6`). Those three scenarios retained both local disks. The additional `recovery.disk-loss` scenario passed independently (`tck-54de7fea-fa4c-4ad4-9aa7-fc7b52810358`): the owned celld volume was removed, the replacement was verified empty, MinIO remained unchanged, and data plus the overdue alarm recovered. The full four-case recovery suite also passed (`tck-272a3944-9e22-4284-a132-6d33ee9c32ea`). These single-node runs make no multi-node recovery claim; separate multi-node evidence follows below.

## Object-store outage

The focused `recovery.storage-outage` case passed (`tck-e9c47cb1-60d3-4603-931b-a03ad803806a`). All three outage writes returned transport errors; logs recorded `node_lease_watchdog_fence`. The baseline transaction survived, none of the uncertain writes appeared in either store, and the post-recovery write survived a further SIGKILL/restart. The oracle also accepts fully committed uncertain writes, but never partial KV/SQL transactions. Both disks were retained. The full five-scenario suite also passed (`tck-7d20e9d9-54c2-4fdd-b884-8e074fed0830`), including the final check that previously observed state remains unchanged after a further crash.

## Multi-node bucket durability

All four stages passed (`tck-7bc94af3-ab2e-4887-a77d-498596e4179e`): cross-node routing, owner failover, old-owner rejoin, and storage-network partition with lease-watchdog fencing. Ownership records confirmed a new owner and higher epoch after each takeover. [MULTINODE.md](MULTINODE.md) describes the topology, recorded histories, and limits; fleet-durable follower-log recovery is covered by the separate fleet suite below.

## Fleet durability

All five fleet-mode stages passed (`tck-8df7ada7-55e5-455f-afa3-1a8d50addfde`), including an acknowledged transaction with its owner disconnected from MinIO, a fleet-proof log for the tested cell, nonempty follower-log recovery after killing the owner, and exact SQL/KV state through both nodes after rejoin. The bucket-mode regression suite also passed (`tck-04567198-2c77-4d07-97ea-b129328c5147`). See [FLEET.md](FLEET.md).

Two development runs exposed harness issues: the log parser did not initially accept generation-qualified dead-node IDs (`tck-c214f9ea-93ad-427a-ab0d-e1ff3842698f`), and the state oracle incorrectly expected numeric ordering for KV keys above 9 (`tck-d99988a6-2876-4b95-ba19-d3f0cafddfe9`). Both are fixed with regression coverage; neither was waived as a celld bug.

## Three-node resilience

All ten stages passed (`tck-6905c62a-9c6f-40dd-ae5e-770f398a9be3`), including paused-owner fencing, interrupted transactions, simultaneous restart, and loss of one follower followed by loss of the leader. The interrupted writes all lost their responses and were absent after recovery; earlier acknowledged history remained exact. The driver verified a transaction had entered its response delay before killing the owner.

The all-replica-disk-loss scenario observed **declared data loss**, not full recovery: acknowledged transaction 151 disappeared after every node disk was destroyed, while all 19 earlier acknowledged transactions remained intact in SQL and KV. celld published a loss record matching the owner generation and log epoch. This exceeds fleet RPO=0's surviving-copy assumption. The test passed because the loss was explicitly declared and surviving transactions were atomic; it does not claim that acknowledged data survived this fault. See [RESILIENCE.md](RESILIENCE.md) and the run's `replica-disk-loss.json`.

The updated single-node recovery suite also passed all five scenarios (`tck-cc3dd0e4-3074-4da5-852d-26f96c2ea785`), and the reference API corpus passed all 63 (`tck-0e01dfdd-48c7-4fd5-9c0a-04ebb732989e`).

## Three-node harness development

Two runs exposed scenario setup assumptions: `tck-ec3da2e5-73ac-4d9e-a188-cab70c37b178` waited for an existing healthy one-follower ensemble to expand, which v0.5.0 does not do; `tck-18002bd4-362e-4166-9588-78a1f93bf721` waited for a replacement ensemble to become active without issuing a new transaction to activate it. Setup now establishes a fresh two-follower ensemble with both peers ready and performs distinct warm-up writes before requiring an active lease. Failed runs are retained, not waived. The interrupted-write stage also requires a fixture log showing that a transaction entered its response delay before the owner is killed.

## Scope

See [coverage.json](coverage.json) for exact coverage and exclusions. The corpus exercises real network HTTP/WebSocket calls, actual alarms, Queue delivery/retry, Workflow execution/retry/events, and uploaded fixture modules. Miniflare's KV/D1/R2/Queues/Workflows implementations remain local service emulators. Passing these cases does not establish edge caching, production service latency, host-failure durability or correctness under all multi-node failure schedules. Actual hibernation and a bounded set of fault/load schedules are now covered by the separate [qualification suite](QUALIFICATION.md).

No upstream fixes, commits, pushes, or cloud deployments are part of this change.
