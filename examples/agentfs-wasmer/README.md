# AgentFS / native Wasmer feasibility probe

This runs a real celld Durable Object with `AgentFS.create(ctx.storage)` and a
native Wasmer WASI guest. Guest filesystem operations call back into the same
cell. There is no second filesystem database, mount-copy step or workspace
export/import.

The architectural recommendation and source evidence are in
[exploration.md](../../exploration.md).

## Run

Tested on macOS ARM64, Node 26.5.0, Rust 1.97.x. Requires a native Rust toolchain,
`rustup`, Node/npm and `pgrep`. Ports 19876 and 19877 must be free. The first Rust
build needs several GB of disk space. All state belongs to this example.

From the repository root:

```sh
cargo build -p celld
npm ci --prefix examples/agentfs-wasmer --ignore-scripts
rustup target add wasm32-wasip1 --toolchain stable
rustup run stable rustc --target wasm32-wasip1 -O \
  examples/agentfs-wasmer/guest.rs -o examples/agentfs-wasmer/guest.wasm
cargo build --locked --manifest-path examples/agentfs-wasmer/native/Cargo.toml
node examples/agentfs-wasmer/run.mjs
```

`CELLD_BIN` can select another celld binary. `CELLD_ESBUILD` can select a native
esbuild executable; the runner otherwise selects the installed platform package.
It binds only loopback and uses a synthetic cell named `synthetic-agent`.

The runner performs three happy-path commands, a retained failing truncate-growth
probe, a helper timeout, replay/conflicting command IDs, and stale-token checks.
It kills **its own** celld serving child and supervisor, moves the runtime directory
under `.celld/` aside, restarts against the existing dev object store, and checks
workspace recovery. It repeats this during a running command and verifies
`interrupted` status and rejection of the former execution token. It stops its
processes on completion. The two moved runtime directories remain under `.celld/`
for inspection; reruns create new names.

Exit 0 means these stated expectations passed, **including the expected failure**:
AgentFS 0.6.4 truncate growth reports size 32 but returns only 25 readable bytes.
This is not a complete filesystem conformance test. If that defect is fixed,
update the edge expectation after verifying zero-filled reads.

Generated output: `results-local.json` and `.celld-probe.log` (ignored).
[results.json](results.json) is the retained observation, with old commands from
previous debugging runs excluded. No runtime binaries or cell data are tracked.

## Package probes

With an installed Wasmer CLI (tested release: 7.4.2):

```sh
WASMER_BIN=/path/to/wasmer python3 examples/agentfs-wasmer/package-probes.py
```

This downloads pinned packages into a temporary cache and executes a Bash pipeline,
Python stdlib imports and coreutils `echo`. Results go to
`package-results-local.json`; [package-results.json](package-results.json) records
the observed run. These tests use the stock Wasmer CLI filesystem, **not** the
AgentFS adapter. They establish candidate package execution only. The Python
package version and reported interpreter version differ; both are recorded.

## Scope

- `worker.ts`: sole cell storage authority, short AgentFS operations and a tiny
  command journal. The helper token lives in the trusted adapter, not guest memory.
- `native/src/main.rs`: real Wasmer `FileSystem`, `FileOpener` and `VirtualFile`
  implementation; fresh native Store and module compilation for each command.
- `guest.rs`: ordinary WASI reads, writes, seek/overwrite, append, rename/unlink,
  metadata and 256 KiB I/O. Edge and interruption modes exercise failures.
- `run.mjs`: local supervisor/helper HTTP service and assertions. Actual guest
  egress is disabled; captured output returns via the DO.

This is synthetic-data research code, not a public sandbox service. The HTTP
control routes are unauthenticated loopback routes; do not expose them. The native
adapter deliberately blocks inside async polls on a dedicated helper process;
metadata times, error propagation and handle cleanup are incomplete. There are no
production CPU/memory quotas, no concurrent filesystem writers, no symlinks, no
background-process/session support and no general shell/Python runner integration.

Every filesystem callback uses celld's ordinary response gates, and explicit sync
uses `ctx.storage.sync()`. Recovery from the local object store exercises celld's
LTX path. It does not establish multi-node/follower failover, network-partition
fencing or survival of the local machine's physical disk loss. See the report for
the precise remaining tests and proposed production protocol.
