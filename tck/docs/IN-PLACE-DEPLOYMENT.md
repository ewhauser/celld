# In-place deployment

Six local qualification cases exercise the [celld v0.5.0 deployment contract](https://github.com/denoland/celld/blob/v0.5.0/docs/README.md). They run in the existing faults CI job and results matrix.

- `faults.reload-adoption`: publish a valid second revision, explicitly reload each node, and require the new Worker, Durable Object, and named-service revision through every public endpoint.
- `faults.reload-invalid`: publish a replacement with a deterministic top-level initialization error. Each reload must reject with that error, while the original Worker, object, and service continue serving.
- `faults.reload-in-flight`: adopt while a request, an alarm, and a durability barrier are open on the resident object.
- `faults.reload-socket`: adopt at a safe point with an open hibernatable WebSocket and durable state.
- `faults.reload-forced`: hold a safe point open past the configured adoption deadline with a regular WebSocket and require the documented forced transition.
- `faults.reload-module-bytes`: replace a published module's bytes in the bucket, keeping its length, and require every node to refuse the reload.

The first two scenarios seed an independently recorded SQL/KV acknowledgment history, verify it through all three nodes after reload, append more acknowledged writes, and verify again. Docker container IDs, process IDs, process start times, and restart counts must remain identical. Unavailability, lost acknowledged writes, stale code after successful adoption, and adoption of invalid code fail the scenario. The four lifecycle scenarios keep the same ledger, process-identity, and revision evidence and add the observations described below.

The scenario-specific Compose overlay sets deployment polling to 3,600 seconds, beyond the scenario's eight-minute bound. After publication and before explicit reload, all three Workers must still report the old revision. This prevents background polling from substituting for the reload trigger. Invalid code is accepted by the deployment tool but fails during runtime initialization; a deployment-tool error cannot pass this case.

## v0.5.0 inventory

Confirmed on the pinned binary from the release documentation and by live probing of a running three-node fleet before the cases were written. Every item below is both documented and observable.

| Contract               | v0.5.0 statement                                                                                                                                                  | Observed                                                                                                                                                                 |
| ---------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| Safe point             | A resident object moves when no request runs in it, no alarm handler runs in it, no output waits for durability, and no regular WebSocket is open                 | `/state` reports `deployment.generation`, `deployment.swapping`, `deployment.draining`, and `deployment.cells[<cell>]`, the generation each resident object runs         |
| Node-level adoption    | "it switches new requests to the new deployment in one step"; "a request that started on the previous deployment finishes on it"                                  | The Worker route reports the new revision within ~150 ms of `POST /reload`, while a request already in the object returns the old revision after its full hold           |
| Requests during a move | "A request that arrives while the object moves waits for the new code"                                                                                            | Object requests issued after the trigger do not return until the move completes, which bounds how long a scenario may block a safe point                                 |
| Adoption deadline      | `CELLD_DEPLOY_MAX_AGE_S`, default 60 seconds; 0 forces every resident object at the adoption                                                                      | With the overlay set to 20, a blocked object stayed on the old generation for ~20.8 s and then moved                                                                     |
| Forced close           | "celld cancels its running work and closes its regular WebSockets with code 1012"                                                                                 | The client sees close code `1012` with reason `service restart`; the hibernatable socket on the same object stays open                                                   |
| Preservation           | "The move keeps the object's storage, its epoch, and its hibernatable WebSockets"                                                                                 | The same socket reports a new activation token and the new revision with its serialized attachment counter continuing; SQL/KV history is unchanged                       |
| Module verification    | "A new deployment manifest records the full SHA-256 digest of each JavaScript or WebAssembly module. A node verifies each module before it builds the deployment" | `<prefix>/manifest.json` carries `modules[].name`, `.bytes`, and `.sha256`; a same-length replacement is refused with `deployment module digest mismatch for "index.js"` |

Nothing in roadmap section 2 was found untestable on this release.

## Lifecycle scenarios

`faults.reload-in-flight` arms an alarm whose handler holds for 1.5 seconds, starts a six-second request in the object, and leaves an explicit `storage.sync()` barrier outstanding while the storage proxy injects latency on this cell's object path. It then reloads all three nodes and issues nine more acknowledged writes. The oracle requires all three Workers to report the new revision, the owner's `/state` to still place the resident object on the previous generation, the held request, the alarm, and the durability barrier to complete on the previous revision, the object to reach the new revision after the safe point, and every write acknowledged after the trigger to survive. The case uses bucket durability so the injected latency is on the acknowledgment path.

Two timings are load-bearing and were measured, not assumed. The injected latency is scoped to the cell's storage path with `pathContains`, because a fleet-wide mode also slows the module download that `POST /reload` performs and pushes the trigger past the held request. The hold is six seconds because a request that arrives while the object moves waits for the new code, and the driver's transport bound is ten seconds.

`faults.reload-socket` keeps a hibernatable WebSocket connected across an unobstructed adoption. Nothing blocks a safe point, so the object moves on its own. The same socket must remain open with no close frame, report the previous revision before and the adopted revision after, continue its serialized attachment counter, and show a changed activation token. Durable rows must survive and accept new writes.

`faults.reload-forced` adds `infra/adoption-deadline.yaml`, which sets `CELLD_DEPLOY_MAX_AGE_S` to 20 seconds on every node, and opens both a regular and a hibernatable WebSocket. The regular socket prevents a safe point. The oracle requires close code 1012, a close no earlier than the configured deadline and bounded after it, an untouched hibernatable socket that then reports the new revision, and a reconnecting client that reaches the new revision on a fresh regular socket.

`faults.reload-module-bytes` publishes a valid revision, reads the digest the manifest declares for `index.js`, and rewrites the stored object in place with a marker substituted at a fixed offset so the published length is unchanged. Only a content digest can reject the result. All three nodes must answer `422` with `ok:false`, `outcome:"failed"`, and an error naming the module and a mismatch that is not a length difference, while the Worker, object, and named service keep serving the previous revision.

Run individually:

```sh
pnpm tck --profile local --suite faults --case faults.reload-adoption
pnpm tck --profile local --suite faults --case faults.reload-invalid
pnpm tck --profile local --suite faults --case faults.reload-in-flight
pnpm tck --profile local --suite faults --case faults.reload-socket
pnpm tck --profile local --suite faults --case faults.reload-forced
pnpm tck --profile local --suite faults --case faults.reload-module-bytes
```

Evidence includes source digests, deployment output, the published manifest, each reload response, `/state` snapshots, Worker/object/service revision observations, socket exchanges and close frames, process identities, and the existing external ledger and SQL/KV history artifacts. Negative oracle tests reject stale revisions, process restarts, accidental invalid-code adoption, unrelated errors, lost or corrupted acknowledged history, a request attributed to the wrong revision, a dropped alarm, a rejected durability barrier, a hibernatable socket closed by the move, a forced close with the wrong code or before the deadline, a failed reconnection, and an accepted or length-only rejection of modified module bytes.

Fixture routes remain observation-only and identical on every node: `/hold`, `/alarm/arm`, `/alarm/state`, `/socket/regular`, and `/durability/pending-sync` report what happened and never assert.

Local validation uses three celld v0.5.0 nodes and MinIO. This does not qualify AWS or binary upgrades. Cross-deployment calls during the drain window, rebalance interaction with an in-progress adoption, and `CELLD_DEPLOY_MAX_AGE_S=0` remain untested; see the [roadmap](ROADMAP.md).
