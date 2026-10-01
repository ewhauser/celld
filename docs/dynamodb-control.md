# DynamoDB coordination

A celld fleet coordinates through a handful of small records: who owns each
cell, which nodes are alive, and which deployment is current. By default they
live in the fleet bucket next to the cell data. An `s3://` fleet can keep
them in one Amazon DynamoDB table instead. The cell data, the deployments,
the node-log bundles and everything else stay in the bucket.

This guide covers when the table helps, how to set a fleet up with one, what
it needs from AWS, and what it does not do yet. The
[design](design/dynamodb-control-plane.md) explains why the guarantees still
hold with two stores.

## Status

DynamoDB coordination is on `main` and is not in a fork release yet. What
works today:

- Keeping the coordination records of a **new** fleet in a table:
  `celld control init`, then nodes started with `CELLD_CONTROL`.
- Every node and every operator command (`celld deploy`, `celld cell`,
  `celld queue`, `celld diagnose`, previews) reaches the records wherever
  the fleet keeps them, with no new flag.
- Startup checks that refuse a table that could break the guarantees, a
  table claimed by another fleet, and a node configured for the other store.

Still to come:

- **Moving an existing fleet.** There is no `celld control migrate`. A fleet
  chooses its store before it holds any state, so start a table fleet in an
  empty bucket or prefix.
- **Repairing a table restored from a backup** (see [Recovery](#recovery)).

## When to use it

The table makes coordination faster and, for many cells, cheaper:

- **Lease renewals.** A node renews its lease every few seconds and stops
  itself if a renewal cannot land in time. A DynamoDB conditional write
  takes a few milliseconds where an S3 conditional write takes tens, and
  its slow tail is far shorter, so a slow store is much less likely to take
  a healthy node out of the fleet.
- **Cold activations.** Taking ownership of a cell is a read and a
  conditional write of its ownership record. On the table that part of an
  activation takes milliseconds.
- **Request cost.** Each activation and release writes the ownership record.
  With millions of cells a day, those writes cost several times less on the
  table than as S3 PUTs.

The cost is a second dependency: the fleet stops if either the bucket or the
table is unavailable. It is also AWS-only. Keep the default bucket
coordination if you run on R2, Google Cloud Storage, Azure or Tigris, or if
one store is worth more to you than the latency.

Warm requests and writes acknowledged by follower nodes do not touch the
coordination records, so they do not change.

## What moves to the table

| Record | Bucket key |
| --- | --- |
| Cell ownership | `cells/<cell>/own.json` |
| Node lease, with the node's log record | `nodes/<node>.json` |
| Drain token | `drain/token.json` |
| Waker role | `wake/waker.json` |
| Fleet deployment pointer | `deploy/current.json` |
| Named deployment pointer | `deploy/<script>/current.json` |
| Queue consumer attachment | `deploy/queues/<queue>/consumer.json` |

Everything else stays in the bucket: the SQLite replicas under `cells/`,
the node logs under `log/`, the deployments, the alarm index under `wake/`,
the peer-authentication secret, and a new file, `fleet/control.json`, that
records which store the fleet chose.

## Set up a fleet

You need an `s3://` fleet bucket, or a prefix of one, that holds no fleet
yet, and AWS credentials that can reach both the bucket and DynamoDB. The
table uses the same credential chain as the bucket.

1. Create the table and record the choice, before the first node starts:

   ```sh
   export CELLD_BUCKET=s3://my-bucket/prod
   celld control init --table celld-prod
   ```

   The command creates the table if it does not exist, with on-demand
   capacity, deletion protection and point-in-time recovery. It checks the
   table's shape, claims the table for this fleet, tests its conditional
   writes, and writes `fleet/control.json` to the bucket. It is safe to run
   again. Pass `--no-create` to adopt a table you created yourself, and
   `--table-region` when the table is not in the bucket's region. For a
   fleet of more than a couple of hundred nodes, also pass
   `--lease-shards` (see [Large fleets](#large-fleets)).

2. Deploy as usual. `celld deploy` reads `fleet/control.json` and writes the
   deployment pointer to the table:

   ```sh
   celld deploy .
   ```

3. Start every node with the same table:

   ```sh
   export CELLD_CONTROL=dynamodb://celld-prod
   celld --bucket "$CELLD_BUCKET" --listen 0.0.0.0:8080 ...
   ```

   The node logs where its coordination records live when it starts, and
   its listener banner reads `ownership=dynamodb`.

A node or command without `CELLD_CONTROL` follows `fleet/control.json`, so
the variable is a guard rather than a requirement: a node whose
`CELLD_CONTROL` names a different store refuses to start. Set it on every
node so that a fleet whose marker changed does not start silently.

### Create the table yourself

If you manage tables with your own tooling, create one with:

- partition key `pk` and sort key `sk`, both strings
- no global or local secondary indexes
- no replicas (not a global table)
- time-to-live disabled

Then run `celld control init --table NAME --no-create`. On-demand capacity
suits most fleets; with provisioned capacity, leave headroom for the lease
renewals, because a throttled renewal that cannot land in time stops a node.

## What celld refuses

These checks run when a node starts, and `celld control init` runs them
too. Each one fails with a message that names the cause.

- **A bucket that already holds a fleet.** Any object under `cells/`,
  `nodes/` or `log/`, or any coordination record in the bucket, refuses a
  table, even when every node has stopped. Its existing records would not
  move, so its cells would come back empty.
- **A table another fleet claimed.** The table records the fleet and bucket
  that claimed it. A second fleet pointed at the same table is refused, and
  no marker is written, so correcting `CELLD_CONTROL` recovers.
- **A table that lost its claim.** A table emptied or replaced after the
  fleet chose it has lost the ownership records, and it is refused rather
  than claimed again.
- **A table that can serve stale records.** Global tables, secondary
  indexes and time-to-live are refused, because each can hand a node a
  record that another node already replaced, or delete one.
- **A mixed configuration.** A node whose `CELLD_CONTROL` disagrees with
  `fleet/control.json` refuses to start.
- **An operator command without the marker.** A command run with
  `CELLD_CONTROL=dynamodb://…` against a bucket that has no
  `fleet/control.json` refuses and asks for `celld control init`.

## Permissions

Scope the table credential to the one table. A node needs:

```text
dynamodb:GetItem
dynamodb:PutItem
dynamodb:DeleteItem
dynamodb:Query
dynamodb:DescribeTable
dynamodb:DescribeTimeToLive
```

`celld control init` also needs `dynamodb:CreateTable`,
`dynamodb:UpdateContinuousBackups` and `dynamodb:DescribeContinuousBackups`,
and `celld control show` needs `dynamodb:DescribeContinuousBackups`.

The bucket stays sensitive: it holds the data, the deployments, the
peer-authentication secret and `fleet/control.json`. Whoever can write
either the bucket or the table can disrupt the fleet.

## Operate the fleet

`celld control show` prints the fleet's choice and the table's health:

```sh
celld control show --bucket "$CELLD_BUCKET"
```

```json
{
  "backend": "dynamodb://celld-prod",
  "marker": {
    "backend": "dynamodb",
    "fleet": "…",
    "format": 1,
    "region": "us-east-1",
    "table": "celld-prod"
  },
  "lease_shards": 1,
  "node_leases": 3,
  "point_in_time_recovery": true,
  "shape": "ok",
  "table_fleet": "…"
}
```

`celld diagnose` adds a `control` row that names the store.

Watch the table's `ThrottledRequests` and `SystemErrors` metrics. A
throttled lease renewal is retried, but sustained throttling stops nodes.
An on-demand table throttles traffic that more than doubles its previous
peak, so pre-warm it before a large rollout or load test.

### Large fleets

Every node reads every lease every few seconds to find dead nodes and
recover their logs. On a table it reads them all with one query per lease
partition and shares that read across everything that needs it, but the
reads still grow with the square of the fleet: about 19,000 read units a
second for 500 nodes, against a limit of 3,000 per partition.

Spread the leases when you set the fleet up:

```sh
celld control init --table celld-prod --lease-shards 8
```

Each shard is a partition of its own, so eight shards serve about eight
times the reads and renewals. Choose up to 64; each one costs every node
one more request per read. The count is fixed when the fleet claims the
table, and `init` refuses a different count later. `celld control show`
prints it.

`CELLD_FLEET_VIEW_MS` sets how old a node's shared read may be, five
seconds by default. Raising it cuts the reads in proportion and lets a
node notice a dead peer that much later.

### Recovery

Point-in-time recovery restores the ownership records to an earlier moment,
when some cells may have moved to higher epochs since. celld does not
overwrite data in that case, but those cells cannot activate until their
epochs are repaired, and that repair command does not exist yet. Treat a
table restore as a fleet-wide recovery event.

## Test locally

Use [DynamoDB Local](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/DynamoDBLocal.html)
and an S3-compatible store, and point celld at the local endpoint:

```sh
docker run -d -p 8000:8000 amazon/dynamodb-local -jar DynamoDBLocal.jar -inMemory
export CELLD_CONTROL=dynamodb://celld-dev
export CELLD_CONTROL_ENDPOINT=http://127.0.0.1:8000
celld control init --bucket s3://dev-bucket --endpoint http://127.0.0.1:9000
```

DynamoDB Local does not support point-in-time recovery, so `init` warns
that it could not enable it. `celld dev` keeps using its local store and
does not use a table.

## Settings

| Variable | Default | Meaning |
| --- | --- | --- |
| `CELLD_CONTROL` | follow `fleet/control.json`; `bucket` for a new fleet | `bucket`, or `dynamodb://TABLE` |
| `CELLD_CONTROL_REGION` | the bucket's region | The table's region, when it differs |
| `CELLD_CONTROL_ENDPOINT` | none | A DynamoDB endpoint override, for DynamoDB Local |
| `CELLD_CONTROL_LEASE_SHARDS` | 1 | Lease partitions of a new table, 1 to 64, when a node rather than `init` claims it |
| `CELLD_FLEET_VIEW_MS` | 5000 | How old a node's shared read of the leases may be |

```text
celld control init --table NAME [--table-region REGION] [--no-create] [--lease-shards N] --bucket s3://NAME[/PREFIX]
celld control show --bucket s3://NAME[/PREFIX] [--json]
```

## Limitations

- The table needs an `s3://` fleet bucket, because it signs with the
  bucket's AWS credentials.
- A fleet cannot move between the bucket and a table in either direction.
- `celld cell list` reads the cell prefixes in the bucket. A cell that has
  an ownership record but has never written data has no prefix, so the
  listing leaves it out. Such a cell holds no data.
- The shared fleet capacity sample, `fleet/capacity-v1.json`, stays in the
  bucket.
- Releases that predate this feature do not read `fleet/control.json`.
  Never run one against a table fleet.
