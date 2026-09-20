# Strict shutdown without a live successor

The released `0.5.1-ewhauser.1` binary fails a populated final-member removal:
all runtimes stop and their ownership records release, but the drain still waits
for a successor to adopt those cells. With no successor, the 12.5-second
no-progress window expires inside the unchanged 20-second process budget. The
operation correctly becomes `failed` and retains the disk.

`0.5.1-ewhauser.2` uses the same durability, runtime-stop and ownership-release
pipeline for strict removal, then permits recovery on later demand. Ordinary
handoff still requires successor adoption. Strict completion still requires the
independent own/follower bucket proof and joined actor and durability tasks.
An adoption already in flight is retired only after its runtime and ownership
were released; late adoption replies cannot reopen its core operation.

The checked-in [results](full-stop-results.json) record the released failing
binary, the patched debug binary, and these native macOS ARM64 runs:

| Schedule | Result after deleting every original disk |
|---|---|
| Bucket, 3→2→1→0 with new writes on the populated last member | 28/28 acknowledged KV and SQL writes verified through each of three fresh-disk nodes |
| Bucket, all three nodes accept strict shutdown before any process exits | 20/20 acknowledged writes verified through each fresh-disk node |
| Fleet, all three nodes accept strict shutdown before any process exits | 21/21 acknowledged writes, including a peer-only ACK while MinIO was paused, verified through each fresh-disk node |

Every deletion followed the exact-generation `data_safe` result and process
exit 0. No native loss markers appeared. The tests kept the object store alive;
its bounded temporary filesystem avoids unrelated Docker disk pressure. They do
not qualify object-store restart durability, Kubernetes CSI deletion, or AWS.

Reproduce against a chosen binary, including the released `.1` baseline:

```sh
uv run examples/disk-removal/demo.py --binary /path/to/celld --esbuild /path/to/esbuild --full-stop sequential --durability bucket
uv run examples/disk-removal/demo.py --binary /path/to/celld --esbuild /path/to/esbuild --full-stop concurrent --durability bucket
uv run examples/disk-removal/demo.py --binary /path/to/celld --esbuild /path/to/esbuild --full-stop concurrent --durability fleet
```

Five deterministic drain tests cover multiple bounded cohorts, unchanged
ordinary adoption, failed durability, incomplete runtime stop/failed ownership
release, and a pre-existing rebalance adoption with a late completion. All 12
public core/adapter tests, all-target Clippy with warnings denied, and formatting
checks pass. The fork does not contain upstream's private simulation suite.
