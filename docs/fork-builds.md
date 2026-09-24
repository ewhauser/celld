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
