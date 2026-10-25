# Fork builds

The operator still requires this fork for process identity, recovery safeguards,
idle-follower failure handling, and preview support.

## Retired operator APIs (removed in 0.6.0-ewhauser.2)

Strict disk-removal shutdown and `/state.node_log` reporting have been removed.
The current operator uses ordinary Kubernetes workload lifecycle operations and
consumes neither API. Requests with a shutdown `mode` parameter return HTTP 400
without stopping the process. Ordinary and preserve shutdown remain supported.

The `/state.shutdown` identity envelope retains `schema_version: 1` and
`runtime_generation` for existing operator readers and advertises
`strict_disk_removal: false`. It no longer reports an operation or control-only
phase. Native `bucket_complete` publication and recovery readers remain: ordinary
ensemble maintenance also uses this proof. Witness handling, member/disk binding,
actor transaction rollback, previews, and fork release tooling are unchanged.

The old disk-removal API documentation, demo, and qualification fixtures have
been removed with the implementation. The release history below describes older
artifacts; it does not imply those retired APIs exist in current source.

## Building fork artifacts

The release workflow builds native Linux x86_64, Linux aarch64 and macOS aarch64
binaries. Each workflow artifact contains the compressed executable and a JSON
record of its source repository, commit, version, target and SHA-256 digest.
After all native builds and the container smoke build pass, the workflow creates
a draft prerelease with the same artifacts, `SHA256SUMS`, and build attestations.
A release tag that already identifies a different commit is rejected.

### Cutting a release

release-please keeps one release PR open against `main`. The PR bumps the
version in `crates/celld/Cargo.toml` and `Cargo.lock` and adds a
`CHANGELOG.md` entry. It is built from the Conventional Commit subjects merged
since the last release. Merge feature PRs with squash, so the PR title becomes
the commit subject. The `PR title` check requires the `type: summary` or
`type(scope): summary` form.

- `fix:`, `feat:`, `perf:`, and `revert:` start a release.
- `docs:`, `test:`, `build:`, `ci:`, `chore:`, `refactor:`, and `style:` are
  left out of the changelog.
- Every release advances only the `-ewhauser.N` counter. The check rejects a
  breaking-change marker (`!`), because release-please would leave the
  `0.6.3` base. Describe a break in the PR body. A `BREAKING CHANGE:` footer
  in a commit body has the same effect, so do not use one.
- After a rebase onto a new upstream release, set the new base version, such
  as `0.6.4-ewhauser.0`, in `.release-please-manifest.json`,
  `crates/celld/Cargo.toml`, and `Cargo.lock`. The rebase rewrites the
  commits release-please last saw, so also point `bootstrap-sha` and
  `last-release-sha` in `release-please-config.json` at the rebased commit of
  the last release, and give the rebase commit a `Release-As:
  0.6.4-ewhauser.1` footer. The last release PR must be `autorelease: tagged`
  first.

Merging the release PR pushes its commit to the `candidate` branch and starts
the release workflow. The workflow builds the draft prerelease, and its notes
come from that version's `CHANGELOG.md` section. After you verify and publish
the draft, the workflow marks the release PR `autorelease: tagged`, and
release-please can then open the next one. Until you publish, no new release PR
opens. If a candidate fails, delete the draft and merge the fix. Then push
`main` to `candidate` and run the release workflow on `candidate` again.

To run a candidate build by hand from an exact reviewed branch or tag:

```sh
gh workflow run release.yml --repo ewhauser/celld --ref <reviewed-ref>
```

Download the workflow artifacts with `gh run download`, or the draft assets
with `gh release download v0.5.1-ewhauser.1 --repo ewhauser/celld`. Verify the
checksums and source commit before using them. The macOS binary is a command-line
build without Apple signing or notarization.

Publishing the verified draft starts the existing container phase. It builds
and tests Linux amd64 and arm64 images at the release tag and publishes them to
`ghcr.io/ewhauser/celld`, including a multi-architecture manifest and provenance.
Fork prereleases never update `latest`. The operator must pin the verified
manifest digest; per-platform digests can be used for platform-specific tests.
Do not substitute an upstream image or infer a digest from a tag name.

All members and potential recovery processes must use a compatible fork build:
recovery must understand the `bucket_complete` proof. Artifact publication alone
does not qualify EKS/EBS behavior. Validate the operator and runtime together
against the exact binary/image being used.

## 0.5.1-ewhauser.3: unavailable recovery witnesses

Fleet recovery no longer treats an unreachable follower as conclusively lost
because its lease expired more than three lease lifetimes ago. This could seal
a predecessor log and declare permanent loss while acknowledged writes still
existed on a retained disk whose process was starting later than its peer.

A missing address or failed seal request now keeps that member undecided.
Without another complete witness or an existing `bucket_complete` proof,
recovery refuses to seal, write a loss record, or replace the predecessor lease.
Startup keeps serving authenticated follower seal/tail requests during its
existing bounded retry ladder. A witness that returns within that ladder can
complete recovery; a permanently unreachable witness prevents startup instead
of turning an unknown disk state into data loss. Retry counts and deadlines
are unchanged.

The existing explicit-loss policy for reachable members reporting missing or
incomplete fragments is unchanged. This fix does not repair a predecessor
already sealed with a loss record by an older build. All potential recovering
members must run the corrected build before relying on the new behavior.

## 0.5.1-ewhauser.5

### Node-log state in `/state`

This release added a `node_log` object to the internal `GET /state` response.
That reporting API has since been removed, as described above. It reports this node's
durability posture, its log session and folded log, whether its shipper is
healthy, and the result of the last dead-leader sweep. The sweep result lists
every unsealed log whose lease has expired, and, for each ensemble member, the
leader sessions whose current epoch still needs that member's fragment. An
operator can gate a voluntary disruption on this fleet state without a bucket
client of its own.

The sweep already listed and read every node record; it now keeps its last
result in memory. The change adds no bucket requests, and `/state` makes none.
The bucket posture runs no sweep, so it reports `fleet: null`. A pass that
cannot list or read a record reports `complete: false`.

### Member and disk binding on recovery seal and tail

Before this build, a process that answered at a recovered member's address
with an empty follower store reported fragment epoch 0. Recovery treated that
answer as conclusive. With no complete witness and every member conclusive, it
wrote `log/<session>.e<epoch>.loss.json` and sealed the log. A replacement
machine that reused the node's name, and so its stable address, with a fresh
disk could therefore declare loss on the word of a disk that never held the
fragment, while the disk that did hold it was still retained.

Each follower store now keeps a random disk incarnation in
`<data>/peerlog/incarnation`, created and fsynced (file and directories) the
first time a process needs it, and read back after a restart. Each node
publishes it in its lease as the optional `disk_incarnation` field. Recovery
sends the member name and that incarnation with every seal and tail request
(`member` and `incarnation`, both optional). A follower with another name or
another incarnation refuses the request before it writes a seal mark, and
recovery counts the refusal as an undecided member, like an unreachable one.
The same binding applies to the tail reads that fold a quietly stranded cell.

0.5.1-ewhauser.7 narrows this refusal to another member name: the member's own
name on another incarnation answers from its replacement disk. See that section
for the deadlock the refusal caused.

The check protects only while the member's lease names a disk other than the
one answering, for example while a replacement is still recovering its own
predecessor. Once the replacement installs its lease, its disk is the member's
disk of record and the explicit-loss policy applies to its answer as before.
This is intentional. A disk that no longer exists cannot be recovered, so a
session whose only complete copy was on it gets a bounded loss record instead of
blocking recovery forever. Incarnations are therefore not recorded at
recruitment.
A startup whose incarnation file cannot be read fails instead of creating a
new identity.

Rollout: every field is additive and optional. Older requests and older lease
records deserialize with the fields absent and keep the unchecked behavior. The
check protects a recovery only when the recovering node, the answering
follower and the member lease that names the disk all come from this build.
Run it on every node that can recover or answer for this fleet before relying
on it.

## 0.5.1-ewhauser.6

### An idle leader moves off a departed follower

A leader that received no writes never left a follower that had gone away,
for example a pod deleted after a scale-in. The idle probe, an empty append
sent to each quiet member every 2 seconds, ignored a transport failure. It also
recorded the failed attempt as a completed append, so the gray-follower ledger
read a refused connection as a fast, healthy sample. Nothing degraded the
shipper. Maintenance therefore never drained the epoch to `bucket_complete` or
opened a new one without the member. The leader's current epoch kept naming the
departed member, and `/state.node_log.fleet.obligations` kept it as an
obligation, so a disk-removal gate never released that member's disk. Under
write load the first failed append degraded the shipper and the obligation
cleared, which is why only idle fleets showed the bug.

A failed probe now records a failure, not a latency sample. Three failed probes
in a row, with no answer between them, degrade the shipper the same way a failed
write does: acknowledgements wait for bucket proofs, and maintenance drains the
epoch to `bucket_complete` and opens a new one from the members whose leases are
live. An answer resets the count. The count tolerates a brief network fault
without opening a new epoch on a quiet fleet. A probe carries no
acknowledgement, so the tolerance never delays a degrade a write needs: a write
still degrades on its own first failure. The departed member leaves the
obligations about four seconds after its first failed probe, or after its lease
expires and a maintenance pass runs if the ensemble recruits it again before
then.

Safety does not change. Degrading only moves acknowledgements to the bucket
proof and starts the existing reconfiguration path.

## 0.5.1-ewhauser.7

### A replacement disk under the member's name answers recovery

0.5.1-ewhauser.5 made a follower refuse any recovery seal or tail whose
`incarnation` differed from its own disk, and recovery counted the refusal as
an undecided member. That deadlocked a fleet whose members lost their disks at
the same time. In a two-member fleet that lost both disks, each replacement
must recover its predecessor session before it installs its own lease. The
only ensemble member of that session is the other replacement, and that
member's lease still names its lost disk, because the other replacement is
blocked in the same step. Each follower refused the other's request with
`400 Bad Request`, each recovery failed with `no complete true witness ... 1
member(s) undecided`, and startup exited with `refusing to install a lease
over an unrecovered predecessor log`. No lease was replaced and no loss was
recorded, so the fleet never recovered.

A node name identifies exactly one disk at a time, and a new incarnation under
that name exists only because the named disk was replaced. The follower now
applies this rule:

- Another `member` name is refused, as before, and recovery counts the member
  as undecided.
- The same `member` name with another `incarnation` answers from the
  follower's own store and logs a warning that names the superseded
  incarnation. For an empty replacement disk the answer is a conclusive "no
  fragment". With no complete copy and every member conclusive, recovery
  writes `log/<session>.e<epoch>.loss.json` and seals, as it did before
  0.5.1-ewhauser.5.
- An `incarnation` without a `member` is still refused on a mismatch, and a
  request without an `incarnation` keeps the unchecked answer.

Tail reads, including the fold of a quietly stranded cell, follow the same
rule. The strict disk-removal path is unchanged. A three-member fleet that
loses two disks recovers each lost session from the survivor when it holds a
complete copy, and records the loss when it does not.

This reverses part of the 0.5.1-ewhauser.5 protection: a node name that runs on
a new disk now declares its old disk lost, even if that disk still exists
elsewhere. Reuse a node name on another disk only when the previous disk is
gone for good. `disk_incarnation` stays in the lease so that a follower can
report which disk it supersedes.

Rollout: no wire or record changes. A follower on an older build still refuses
a replacement's answer, so upgrade every node before relying on the fix.

## 0.6.0-ewhauser.1

Based on upstream v0.6.0. It keeps every fork change through
0.5.1-ewhauser.7.

### Upgrading from 0.5.1-ewhauser builds needs a full stop

Upstream's rule for v0.5.1 to v0.6.0 applies. A fleet with `fleet` durability
must not roll from any 0.5.1-ewhauser build to this one. Stop every member,
then start every member on this build. A 0.6.0 node recovers its previous log
session only from a follower that returns the ranged tail format, and a 0.5.1
follower returns only the entries-only format, so a restarted member waits
for a witness the old members cannot give. A fleet with `bucket` durability
has no followers and can roll.

### How the fork changes sit on upstream v0.6.0

- An unreachable recovery witness stays undecided however long its lease has
  been expired (0.5.1-ewhauser.3). Upstream v0.6.0 still counts a member that
  is unreachable with a long-expired lease as conclusive; this build does not.
  Upstream's own rules also apply: an HTTP error, a legacy entries-only tail
  or a tail without complete range evidence leaves the member undecided.
- Seal and tail requests still carry the member name and disk incarnation
  (0.5.1-ewhauser.5 and .7). The ranged tail endpoint applies the same
  addressee check as the seal.
- Folding a quietly stranded cell before it is activated again now follows
  upstream: an epoch that may have fleet acknowledgements needs a complete
  follower tail. A log record the bucket already covers (`bucket_complete`)
  needs none, as before. Each tail request names the member's disk.
- Disk-removal coverage (`bucket_complete`) now waits for upstream's
  `all_fragment_rows_tiered`. It follows each stopped cell's retained tail
  until its uploads land, and replaces the fork's flag that stayed set once
  any cell had ended with an untiered tail.

## 0.6.0-ewhauser.2

Based on upstream v0.6.0. It keeps every fork change through
0.6.0-ewhauser.1.

### Retired operator APIs removed

Strict disk-removal shutdown and `/state.node_log` reporting are gone. A
shutdown request with a `mode` parameter returns HTTP 400 without stopping
the process. See "Retired operator APIs" above for what `/state.shutdown`
still reports.

### OTLP metrics

With `CELLD_OTEL` set to a collector URL, celld now exports a metrics signal
to `/v1/metrics` beside traces and logs:

- node load gauges, read from the same snapshot `/state` serves;
- `celld.cell.cpu_time`, the thread CPU of each cell that ran JavaScript in
  the interval;
- `celld.cell.heap_bytes`, each isolate's V8 heap divided among the cells
  that share it.

Metrics are on by default with a collector and export every 60 seconds.
`OTEL_METRICS_EXPORTER=none` turns them off. `OTEL_RESOURCE_ATTRIBUTES` adds
resource attributes, such as a fleet name, to every OTLP signal. While
metrics are on, celld reads the thread CPU clock around every cell turn, even
without a CPU limit. `docs/telemetry.md` lists every metric and the limits of
the heap average.

Rollout: no record or storage format changes. Follower append responses no
longer carry `quiesced`, which only strict disk removal set; older nodes read
its absence as false. Nodes can roll from 0.6.0-ewhauser.1.

## 0.6.1-ewhauser.1

Based on upstream v0.6.1. It keeps every fork change through
0.6.0-ewhauser.2. Upstream's v0.6.1 release notes list the new behavior:
Python Workers, epoch GC (`CELLD_LTX_RETENTION_SECS`), `celld cell gc
--dry-run`, configurable asset and Dynamic Worker size limits, log severity,
and the WebSocket ordering fixes.

### Change export

A node can export every committed row change of the classes it is told to
export, as a convergent mirror of each cell's tables. It is off by default;
`CELLD_EXPORT_*` turns it on. `docs/export.md` is the user guide and
`docs/design/change-export.md` the design.

- The release build carries the bucket sink, which writes Parquet record
  files beside the cell's data, and the `celld export` commands: `repair`,
  `backfill`, `inspect`, `reconcile`, `verify` and `erase`.
- The blob-stream sink (`export-blob-stream`), the Kafka sink
  (`export-kafka`) and the Snowflake audit (`export-snowflake`,
  `celld export ... --consumer snowflake`) are Cargo features that the
  release binaries and default image do not enable. Each release also
  publishes a `kafka` variant built with `export-kafka,export-snowflake`:
  binaries named `celld-kafka-<target>.gz` beside the default
  `celld-<target>.gz`, and an image tagged
  `ghcr.io/ewhauser/celld:<tag>-kafka`. For any other feature,
  build celld with it
  (the Dockerfile takes `--build-arg CELLD_FEATURES=...`). The Kafka variant
  can run `celld export reconcile --consumer snowflake` with the loader's
  `SNOWFLAKE_*` settings. A node configured for a sink it was not built with
  refuses to start.
- `celld-export-loader` loads a blob-stream or Kafka topic into Snowflake
  through Snowpipe Streaming. It is built from `crates/export-snowflake` and
  is not a release asset.
- Facet databases export as their own streams, with ordered incarnations and
  `deleted` records when a facet goes away.

Known gaps: nothing loads the bucket sink into Snowflake continuously, a
node runs one sink at a time, and the Snowflake path has been tested against
an emulator, not a real Snowflake account.

### Other changes

- Cell runtimes start off the core thread, and a lone dev node activates
  fresh cells faster.
- A Durable Object alarm set years ahead no longer aborts the node.
- `celld dev` nodes stop when their supervisor dies on macOS.
- `celld-perf` runs performance and network-fault tests; nightly results,
  the TCK dashboard, and the fork documentation are published on the docs
  site.

### How the fork changes sit on upstream v0.6.1

- Upstream now runs ordinary Actor effects on the host runtime, off the core
  thread that owns the node lease timer. The fork already did this for
  node-log recovery and isolate startup; those effects now use upstream's
  path.
- Actor timers keep the fork's deadline-ordered queue instead of tokio-util's
  `DelayQueue`, so a Durable Object alarm years ahead still cannot panic the
  core.
- The log Parquet files gain upstream's `severity_number` and
  `severity_text` columns.

Rollout: upstream's rule for v0.6.0 to v0.6.1 applies. Nodes can roll from
0.6.0-ewhauser.2. Set `CELLD_LTX_RETENTION_SECS`, deploy a Python Worker, or
raise `CELLD_MAX_ASSET_FILE_BYTES` above 25 MiB only after every node runs
this build. With epoch GC on, change export repair and backfill can no longer
restore positions in a deleted epoch; they restore at the chain's base
instead.

## 0.6.2-ewhauser.2

Based on upstream v0.6.2 and including the fork's change export,
recovery safeguards, and default and Kafka artifact variants.
The new `--consumer-topic T` option lets the Snowflake audit commands inspect
and repair a topic-keyed loader whose tables are shared by several fleets.
It scopes reads, findings, tombstones, and landed rows to that topic. In topic
mode it starts the loader's `EXPORT_ROUTE` and `EXPORT_ERASE` tasks; the
commands report that the tasks started, not that they finished.

The option is available in the `-kafka` binaries and image, which include
`export-kafka,export-snowflake`. The standard artifacts omit the Snowflake
audit feature. The separate Snowflake loader still requires a source build.
The six native artifacts passed the release build and checksum verification;
the Kafka macOS binary reported this version and exposed `--consumer-topic`.
CI, TCK, and both container smoke builds passed. The Snowflake path has been
tested against an emulator, not a real Snowflake account.

## 0.6.2-ewhauser.3

`storage.transaction()` now gives back its place in the object's transaction
queue when its block is retired before the callback runs. A retired block
results from a spent handler budget, a shutdown cancel, or a terminated
isolate. The transaction rejects with
`the cell event ended before its transaction could start`. Previously the
leaked place made every later transaction on the object wait, holding the input
gate, until the 30-second block limit reset the object. Plain reads waited too.

Rollout: nodes can roll from 0.6.2-ewhauser.2. There is no storage-format or
protocol change.

## 0.6.2-ewhauser.4

A peer call no longer hangs on a pooled tunnel whose network path died without
a FIN or RST. A stateful firewall, NAT, or proxy can expire an idle flow this
way. Previously such a call waited about 15 minutes for the kernel to abandon
the socket, and each hung call left another dead tunnel in the pool.

- A pooled peer tunnel idle for `CELLD_PEER_TUNNEL_IDLE_MS` (default 60000) is
  closed instead of reused. Keep the value below the idle timeout of any
  middlebox between nodes.
- Each tunneled call waits for its reply at most the admission wait, the
  handler budget, and `CELLD_OPERATION_DEADLINE_MS` added together. On expiry
  celld closes the tunnel and fails the call as ambiguous. The call is not
  replayed, because the owner may already have run it.

Rollout: nodes can roll from 0.6.2-ewhauser.2 or .3. There is no storage-format
or protocol change. The six native artifacts passed the release build and
checksum verification, and the macOS binary reported this version.

## 0.6.3-ewhauser.1

Based on upstream v0.6.3. It keeps every fork change through
0.6.2-ewhauser.5. Upstream's v0.6.3 changes:

- Outbound TLS no longer trusts the emSign ECC Root CA - G3 certificate or the
  eight eMudhra intermediate CA certificates whose private keys are public
  (webpki-roots 1.0.10, rustls-webpki 0.103.16).
- rustls 0.23.46 fixes GHSA-2mjx-qc3c-rqvc, where a TLS 1.3 handshake message
  was accepted across an encryption level boundary.
- A node refuses to acquire a cell whose ownership record
  (`cells/<cell>/own.json`) is missing while its LTX is still in the bucket,
  instead of starting the cell at epoch 1 with an empty database. Requests
  that need the record fail, and the node logs `celld ownership record lost`
  at most once every 10 seconds per cell. `docs/guarantees.md` describes how
  to restore the record.

Rollout: nodes can roll from any 0.6.2-ewhauser build, and a rollback can roll
too. Upgrade each node promptly for the TLS fixes. Until every node runs this
build, an older node can still acquire a cell whose ownership record was lost
and start it with an empty database. There is no storage-format or protocol
change.
