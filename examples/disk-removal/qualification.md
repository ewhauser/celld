# Local qualification, 2026-09-20

This run built the strict-shutdown patch from fork `main` at v0.5.1
(`42269c121c989c65c0638ab01f368baf18a5f0df`). It does not reuse the older
retirement prototype's binaries or results. The tested debug binary's SHA-256,
operation/generation results and ledger counts are in [results.json](results.json).

Environment: native macOS ARM64, Rust 1.97.1, esbuild 0.28.2, Python via `uv`,
and the digest-pinned Docker MinIO image in `demo.py`.

| Scenario | Result |
|---|---|
| Shrink, grow on a fresh disk, shrink again, 2-to-1, survivor crash/restart | 33/33 acknowledged writes recovered; removed disks deleted; no loss markers |
| Leader killed after a peer-only acknowledgment while MinIO was frozen; MinIO killed before restart to discard buffered PUTs | 17/17 acknowledged writes recovered after follower disk deletion |
| One-millisecond strict shutdown deadline | Immutable `failed` result; duplicate request did not clear failure; disk retained |
| S3 outage | Existing lease watchdog self-fenced with exit 3; completion unavailable; disk retained |
| Preserve, ordinary shutdown, SIGTERM and restart | Each exited 0; 12/12 acknowledged writes verified after each restart |

The normal and failed-leader scenarios check wrong-generation rejection,
duplicate acceptance, conflicting-operation rejection, acceptance distinct from
completion, post-actor `/state` polling and exact-generation completion before
process termination and disk deletion. The normal scenario also verifies that a
new incarnation rejects the previous incarnation's operation.

Reproduce each scenario using the desired binary:

```sh
uv run examples/disk-removal/demo.py --binary target/debug/celld --esbuild /path/to/esbuild
uv run examples/disk-removal/demo.py --binary target/debug/celld --esbuild /path/to/esbuild --failed-leader
uv run examples/disk-removal/demo.py --binary target/debug/celld --esbuild /path/to/esbuild --deadline
uv run examples/disk-removal/demo.py --binary target/debug/celld --esbuild /path/to/esbuild --outage
uv run examples/disk-removal/demo.py --binary target/debug/celld --esbuild /path/to/esbuild --ordinary
```

Seven focused core/adapter tests, `cargo fmt --all -- --check`, and
`cargo clippy -p celld -p celld-logic --all-targets -- -D warnings` passed.
Earlier live attempts correctly remained blocked, exposing a continuing actor
lease and a quiet follower's refusal not triggering leader degradation. The
checked-in schedules now cover both fixes.

This is local runtime evidence. No AWS/Kubernetes/launcher qualification, hosted
CI result, mixed-version recovery qualification or upstream private simulation /
TLA+ execution is claimed. The caller must enforce the homogeneous recovery
binary requirement in [the contract](../../docs/disk-removal.md).
