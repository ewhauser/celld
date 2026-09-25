# Fork builds

The operator's strict disk-removal contract requires this fork. Stock celld
v0.5.1 does not provide it. The first fork build identifies itself as
`0.5.1-ewhauser.1`, including in `celld --version`.

The release workflow builds native Linux x86_64, Linux aarch64 and macOS aarch64
binaries. Each workflow artifact contains the compressed executable and a JSON
record of its source repository, commit, version, target and SHA-256 digest.
After all native builds and the container smoke build pass, the workflow creates
a draft prerelease with the same artifacts, `SHA256SUMS`, and build attestations.
A release tag that already identifies a different commit is rejected.

Run the candidate build from an exact reviewed branch or tag:

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

All members and potential recovery processes must use a compatible fork build
before removing a last follower: recovery must understand the `bucket_complete`
proof. Artifact publication alone does not qualify EKS/EBS removal. Validate the
strict API and launcher handshake against the exact binary/image being used.

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

The internal `GET /state` response gains a `node_log` object, documented in
the [README](README.md#shut-down-and-roll-out-a-node). It reports this node's
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
