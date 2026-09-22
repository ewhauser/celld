# Durable Wasmer workspaces

Run configured WASI/WASIX tools against the same AgentFS tables used by a
TypeScript Durable Object. celld owns the database, placement and replication.
The executor is a separate, supervised Linux service. It never opens a cell
database or receives bucket credentials.

This is a constrained backend: Bash/coreutils, Python's standard library and
custom WASI modules; one active command per workspace; buffered UTF-8 stdio;
network access disabled. It is not a Docker replacement or an implementation of
the unmodified `@cloudflare/sandbox` SDK. See [qualification.md](qualification.md)
for retained evidence and deployment boundaries.

## Build and configure

Use the celld binary built from this checkout. Older releases lack the
`ctx.assertCanAwaitCallback()` deadlock guard and are rejected by the SDK.
The executor requires Linux, Node 24.12+ and the pinned Rust dependencies.
macOS is supported only for development tests.

```sh
# From the repository root:
cargo build --release --locked -p celld
cd packages/wasmer-sandbox
npm ci --ignore-scripts
docker build -t celld-wasmer-sandbox:local .

# Use a trusted Wasmer CLI; 7.4.2 was qualified. Downloads happen at setup,
# never during guest execution. Every artifact is checked against tools.lock.json.
WASMER_BIN=/path/to/wasmer node service/fetch-tools.mjs tools
cp service/config.example.json service/config.json
```

Set `callbackOrigin` in `service/config.json` to the HTTPS origin of your celld
Worker router. The executor uses only that configured origin; command requests
cannot choose callback URLs. Configure these Worker bindings through your normal
celld deployment configuration/secret-management process:

| Binding | Value |
| --- | --- |
| `SANDBOX_API_TOKEN` | A random service API token, at least 32 characters |
| `SANDBOX_CALLBACK_TOKEN` | A distinct random executor-to-Worker token |
| `SANDBOX_SUPERVISOR_TOKEN` | A distinct random Worker-to-executor token |
| `SANDBOX_SUPERVISOR_URL` | HTTPS executor origin, or `http://127.0.0.1:19877` for a local executor |
| `WORKSPACES` | Durable Object namespace for `SandboxWorkspace` |

[example/worker.ts](example/worker.ts) and
[example/wrangler.json](example/wrangler.json) provide the Worker and namespace.
Supply the string bindings in the deployment config's `vars`, using a protected
generated config outside version control. celld stores Worker configuration in
the fleet bucket; access to that bucket is administrator access. No credentials
are embedded in this repository. Generate tokens with `openssl rand -hex 32`.

Start the executor with the matching supervisor/callback tokens:

```sh
export CELLD_SANDBOX_TOKEN='YOUR_SUPERVISOR_TOKEN'
export CELLD_SANDBOX_CALLBACK_TOKEN='YOUR_CALLBACK_TOKEN'
docker compose -f service/compose.yaml up -d --build
```

The Compose service runs as UID 10001 with a read-only root filesystem, no Linux
capabilities, no privilege escalation, 2 GiB memory, two CPUs and 256 PIDs.
Tool files are mounted read-only. Keep the executor separate from celld's memory
budget. For remote executors put an authenticated TLS reverse proxy in front of
the loopback listener. Only the celld Worker should possess the supervisor token.
Restrict executor egress to the callback router using your network policy; the
guest's virtual networking implementation independently denies network access.

Deploy the Worker with your ordinary `celld deploy` workflow. This repository
does not publish images, install cloud infrastructure or alter an existing fleet.
Run the supplied qualification against the target environment before admitting
untrusted production traffic. Pin your built image by digest when deploying it.

## Use the HTTP API

Paths use `/v1/workspaces/<workspace>/<action>`. A workspace is an agent name
matching `[A-Za-z0-9_-]{1,128}`, or an actual 64-character lowercase hexadecimal
Durable Object ID. Names resolve through `idFromName`; hexadecimal IDs use
`idFromString`. Reserve hexadecimal names for IDs. All actions use POST and
`Authorization: Bearer <SANDBOX_API_TOKEN>`. This is an internal service API;
apply tenant/user authorization before forwarding requests from your product.

```sh
curl "$WORKER/v1/workspaces/agent-42/write" \
  -H "Authorization: Bearer $SANDBOX_API_TOKEN" -H 'Content-Type: application/json' \
  -d '{"path":"/workspace/input.txt","data":"aGVsbG8K"}'

curl "$WORKER/v1/workspaces/agent-42/exec" \
  -H "Authorization: Bearer $SANDBOX_API_TOKEN" -H 'Content-Type: application/json' \
  -d '{"id":"job-001","tool":"bash","args":["-c","cat input.txt | wc -c"],"cwd":"/workspace","timeoutMs":30000}'

curl "$WORKER/v1/workspaces/agent-42/status" \
  -H "Authorization: Bearer $SANDBOX_API_TOKEN" -H 'Content-Type: application/json' \
  -d '{"id":"job-001"}'
```

`exec` accepts `id`, configured `tool`, `args`, `cwd`, `env`, `stdin` and
`timeoutMs`. No shell interpolation is performed: use the configured Bash tool
and `-c` explicitly when desired. The result contains `status`, `startedAt`,
`finishedAt` and `result: {reason, exitCode, stdout, stderr}`. `status` returns
null for an unknown ID. `cancel` accepts `{id}` and closes filesystem admission
immediately; poll `status` for the terminal record. Cancellation preserves
already committed file changes.

Other actions: `read`/`write` (`{path,data}` with base64 data), `stat`, `list`,
`mkdir`, `unlink`, `rmdir` (`{path}`), and `rename` (`{path,to}`). Read returns
`{data}` in base64. Read/write convenience operations are limited to 1 MiB;
guest file descriptors support larger files in bounded chunks. `/fs` is a
private protocol using the callback token plus an activation-scoped execution
capability. Never forward end-user requests to it as trusted callback traffic.

## Use TypeScript inside the agent's Durable Object

Use a local workspace dependency on this package (currently private/unpublished),
or import the source paths directly when bundling your Worker:

```ts
import { SandboxWorkspace, routeWorkspace } from "@celld/wasmer-sandbox/worker";

export class Agent extends SandboxWorkspace {
  async runAnalysis(commandId: string) {
    this.sandbox.fs.writeFile("/workspace/input.json", new TextEncoder().encode('{"n":42}'));
    this.ctx.storage.sql.exec("CREATE TABLE IF NOT EXISTS results(id TEXT PRIMARY KEY, status TEXT)");
    const result = await this.sandbox.exec({
      id: commandId,
      tool: "python",
      args: ["-c", "import json; print(json.load(open('/workspace/input.json'))['n'])"],
    });
    this.ctx.storage.sql.exec("INSERT OR REPLACE INTO results VALUES (?,?)", commandId, result.status);
    return result;
  }
}
export default { fetch: routeWorkspace };
```

Bind `WORKSPACES` to your `Agent` class. Its inherited fetch handler serves
callbacks to the same cell ID. `runAnalysis` is an application method; expose it
through your own authorized application handler/RPC boundary. `WasmerSandbox`
is also available as a composition API for existing DO classes, provided their
callback router resolves its `workspace` option to that **same** DO.

`fs` implements synchronous `readFile`, `writeFile`, `stat`, `list`, `mkdir`,
`rename`, `unlink`, `rmdir`, `open`, `read`, `write`, `truncate`, `fstat` and
`close`. It uses the AgentFS 0.4 format with a corrected managed-storage
implementation. Do not mix it with the unpatched AgentFS 0.6.4 Cloudflare
reader: that reader mishandles sparse/extended files. Application SQL tables
coexist with `fs_*` and `celld_sandbox_commands`; querying them is supported.
Direct writes to those reserved tables bypass SDK invariants and are not part
of the supported contract.

## Execution and durability contract

- One command writes a workspace at a time. DO reads can observe completed
  filesystem operations while `exec` is awaiting. SDK application mutations
  return `EBUSY` during execution. Handles are execution/activation scoped.
- Each write, append, truncate, rename, unlink and bounded file replacement
  commits in a short `transactionSync`. Append resolves EOF in that transaction.
  Growth is sparse and reads synthesize zeroes. A multi-call write can leave a
  committed prefix after a trap, cancellation or machine failure.
- Explicit guest sync invokes celld's real durability barrier. Every callback
  response and terminal result follows celld's output gates. Healthy fleets may
  acknowledge from follower fsync; object-store upload is not required per chunk.
- Never hold `transactionSync`, an async storage transaction,
  `blockConcurrencyWhile`, or an application mutex needed by callbacks across
  `exec`. The celld guard rejects the first three before launching a helper.
- Same command ID and canonical payload returns the journaled result/status.
  A different payload is a conflict. An interrupted command is never retried
  automatically. Inspect its files and decide whether a **new** command is safe.
  No exactly-once guarantee is made for arbitrary commands.
- Eviction/reload/ownership takeover constructs a new SDK instance, marks
  `running` records `interrupted` and rejects previous capabilities. The helper
  heartbeat terminates an execution whose owner no longer admits it. CPU-bound
  guests are also bounded by the supervisor's absolute wall-clock deadline.
  There is no stack/memory continuation.
- Results are buffered until completion. Streaming, guest networking, interactive
  sessions, persistent background jobs and preview ports are not exposed.
- Runtime/package files are immutable to the guest. `/workspace` is durable;
  `/tmp` is temporary. Cross-mount rename, symlink/hard-link creation and deletion
  or replacement of an open durable inode are rejected. Close the inode first.

Default limits: 64 MiB logical workspace, 16 MiB/file, 4,096 inodes, 128 handles,
64 KiB per filesystem transfer, 16 MiB of temporary file buffer capacity, eight guest tasks,
64 KiB per stdout/stderr, 64 KiB stdin, 128 arguments, 64 environment entries,
30 seconds per command (120 seconds maximum), two executions per supervisor.
Each Wasm memory is capped at 512 MiB and each table at 1,000,000 elements.
Linux also applies a 128 GiB virtual-address ceiling (reserved address space, not
physical memory), CPU-time limit, 256 file
descriptors and disabled core dumps/no-new-privileges. The service container's
memory/PID limits cover runtime overhead and temporary filesystem metadata.
Limits are rejection boundaries, not quotas silently truncated on success.
Temporary buffer capacity is released when the underlying file is deleted and
its handles are closed; truncation may retain allocated capacity. The pinned
[filesystem patch](runner/vendor/README.md) reserves quota before allocation and
keeps rejected growth from modifying existing files.

The journal stops admitting commands after 10,000 IDs by default. Archive it
under your retention policy while the workspace is idle. Deleting a journal row
ends deduplication for that ID: preserve an external tombstone or never reuse
expired IDs. The SDK deliberately does not silently expire successful IDs.

## Operations and checks

`GET /healthz` reports readiness, active jobs and capacity. Admission returns 503
while the supervisor is full or shutting down. SIGTERM stops admission and kills
active execution groups; the DO records interruption/cancellation. A supervisor
crash can interrupt all its active commands, so choose its capacity and cgroup
budget together. Runner diagnostics are bounded and available through the
`runnerDiagnostic` event and emitted as JSON on the service stderr; guest
stdout/stderr remain separate from those logs.
Keep tokens and callback bodies out of access logs. Rotate by draining commands,
updating both ends, and restarting the executor.

```sh
npm test
npm run check
npm run format:check
cargo test --locked --manifest-path runner/Cargo.toml
cargo clippy --locked --all-targets --manifest-path runner/Cargo.toml -- -D warnings

# From this package directory, after building celld at the repository root:
rustup target add wasm32-wasip1 --toolchain stable
rustup run stable rustc --target wasm32-wasip1 -O test/guest.rs -o test/guest.wasm
cargo build --locked --manifest-path runner/Cargo.toml
SANDBOX_TEST_TOOLS="$PWD/tools/tools.json" npm run test:integration
npm run test:fleet  # needs Docker; creates/removes its own MinIO container
```

Tests use synthetic data and retain logs/results under ignored `test/artifacts`.
The fleet test kills only its own nodes, removes their synthetic working state,
and pauses only its isolated MinIO container. Ports 19876–19877 and 19970–19985
must be free (overridable with `SANDBOX_TEST_PORT`/`SANDBOX_FLEET_PORT`). See the
Linux test Dockerfile and CI workflow for container qualification.

## Local IPC experiment

An opt-in [native stat experiment](ipc-experiment.md) uses a persistent binary
Unix socket into celld’s managed storage turn. It includes a paired HTTP/native
benchmark and capability/failure tests. It currently accelerates path metadata
only; the default service continues to use HTTP for all filesystem operations.
