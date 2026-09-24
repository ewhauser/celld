# Operator-backed previews

Requires the `CelldPreview` operator API from
[celld-operator PR #52](https://github.com/ewhauser/celld-operator/pull/52), `kubectl`
on PATH, and Kubernetes authentication. The CLI uses an explicit context; it does
not change the current context. Configure preview storage/routing on an existing
`CelldFleet`. Developers create only `CelldPreview` resources.

## Developer workflow

```sh
celld preview pr-42 --context development --namespace previews \
  --fleet development --config ./wrangler.jsonc --revision abc123

celld preview reproduce-cart --context development --namespace previews \
  --fleet development --seed-from production \
  --object Cart:canonical-cart-id --object Customer:canonical-customer-id

celld preview status pr-42 --context development --namespace previews --json
celld preview delete pr-42 --context development --namespace previews
```

The command builds the project, creates or updates its preview, waits for any
requested initialization, publishes the build to the child fleet's bound storage,
then waits for infrastructure readiness and the operator's observations of that
exact application version/prefix. It prints the stable preview URL once the route
responds without a server error. Application-specific assertions remain the
application's responsibility. `Ready` alone never proves a code deployment.

Run the first command again to update code while retaining the same URL and state.
`--revision` is informational; the built content determines the deployment version.
Omitted seed/TTL options preserve existing settings. Seeding is one-time and TTL
is measured from creation; neither can be changed or reset. The default TTL is
24 hours. `--timeout-seconds` defaults to 600; a timeout retains the resource so
`status` and a later deploy can continue. `delete` requests normal operator cleanup
with UID/resourceVersion preconditions; it does not delete fleet objects directly.

`--dry-run` prints the proposed resource as JSON (also valid YAML) without a build,
cluster mutation or storage access:

```yaml
apiVersion: celld.eric.dev/v1alpha1
kind: CelldPreview
metadata:
  name: reproduce-cart
  namespace: previews
spec:
  fleetRef:
    name: development
  source: reproduce-cart
  ttlSeconds: 86400
  seed:
    source: production
    alarms: Clear
    objects:
      - class: Cart
        id: canonical-cart-id
      - class: Customer
        id: canonical-customer-id
```

IDs are canonical runtime IDs, not input names for `idFromName`. Retain the source
Worker name and class/binding mapping when deploying a clone. Snapshots contain
persisted per-object SQLite/KV checkpoints, not a simultaneous live cut of all
selected objects. See [snapshot semantics](preview-runtime.md). `--alarms Preserve`
retains scheduled work; the default clears alarms. Application data may contain
sensitive values, so the approved source alias is a data-access grant.

## Platform executor

Seeded previews need the updated runtime image **and** a trusted executor. Enable
`spec.previews.seeding.executor: celld-snapshot-v1` and authorize source fleet UIDs
on the parent. Run one supervised platform process per authorized namespace:

```sh
celld preview seed --watch --context development --namespace previews
```

This polls reservations every ten seconds, claims eligible work with Kubernetes
resourceVersion concurrency control, and processes it serially. Multiple processes
can coexist because only one can claim each reservation. A single operation can
also be dispatched explicitly:

```sh
celld preview seed s3-scope-RESERVATION_HASH --context development
```

Use the operator's executor RBAC example: reservation get/list/watch and status
get/patch, plus fleet/preview get in the authorized target and source namespaces.
Developers need preview get/create/update/delete and read access to the parent and
child fleets, but no reservation-status or child-fleet write permission.

Supply storage credentials through the standard AWS credential chain with source
read and destination read/create permissions. The executor and developer CLI need
network access to the configured endpoints. They do not extract Kubernetes Secrets.
The developer needs write access only to authorized preview prefixes. Storage
bucket, prefix, region and endpoint come from the bound operator resources; ambient
`CELLD_BUCKET`, `S3_ENDPOINT` and AWS endpoint overrides cannot redirect deployment.

The executor rechecks preview, child, parent, source and reservation identities,
startup intent and storage authority before work. It captures every selected object
and durably pins the complete snapshot manifest before importing any of them.
Cancellation/deadline checks occur between fully awaited object transfers; it never
abandons a storage PUT and then attests that no writer remains.

A process crash or uncertain storage/API error can leave `Running` work blocked.
Automatic restarts never steal or reset that claim. An administrator must establish
that the former writer and any uncertain requests are fenced before resolving it;
there is no automatic recovery/force-resume command. Snapshot data and reservations
remain retained, with no garbage collector in this change.

## Validation boundaries

Focused Rust tests cover selection, immutable updates, storage/ownership binding,
loaded-version readiness, claim exclusion and the snapshot/bootstrap path. Local
integration also exercised real CRD admission and status patches in Kind, two
source objects on MinIO, import into a separate prefix, native runtime restore,
both alarm policies, source isolation, background dispatch, and CLI
create/update/status/delete. A redeploy check withheld the new observed version
and verified the CLI waited; it then returned the native runtime URL after the
matching observation, despite misleading ambient bucket/endpoint settings. Kubernetes
objects in that integration were test fixtures; public Ingress/Gateway, DNS/TLS,
cloud IAM and a fully deployed operator/CLI rollout still need qualification.
