# Qualification record

Verified locally on September 22, 2026, against celld baseline
`c91ca5436db5974e17b9a8abb3d216fe35737831` plus this worktree's callback guard and
Wasmer package. This is an implemented service with executable integration and
fault tests. It is not a deployed fleet, published image/package, or hosted CI
result. Source hashes and local image IDs are in [evidence/build.json](evidence/build.json).

## Results

| Check | Result | Evidence |
| --- | --- | --- |
| Managed AgentFS, journal, protocol, cancellation and supervisor unit tests | 13 passed on macOS and Linux | [macOS](evidence/unit-tests.txt), [Linux](evidence/linux-unit-tests.txt) |
| Native output, path policy, memory and table bounds | 4 passed | [runner-tests.txt](evidence/runner-tests.txt) |
| TypeScript strict checking, Prettier, Rust fmt and Clippy with warnings denied | Passed | Commands below; CI repeats them |
| Linux arm64, non-root/read-only/capability-free container, 2 GiB, 2 CPUs, 256 PIDs | 10 integration checks passed; exit 0; no OOM kill | [linux-arm64.json](evidence/linux-arm64.json) |
| macOS arm64 development runner | 10 integration checks passed | [macos-development.json](evidence/macos-development.json) |
| Three native celld nodes and isolated MinIO | All three fault checks passed | [three-node-faults.json](evidence/three-node-faults.json) |
| Compose configuration | Validated with Docker Compose | [service/compose.yaml](service/compose.yaml) |

The Linux integration uses the release-built native runner and celld's `lab`
profile in a Colima Linux VM. It runs the same supervisor, SDK and Worker router
as the service, along with test-only celld and esbuild dependencies. The separate
Linux unit run permits a writable/executable temporary directory for its fake
runner fixture; the real integration uses the restricted container above. macOS is a
development target and does not exercise Linux resource limits. Its retained run
preceded the final replay/input-validation fixes, which have separate unit
regressions and are included in the final Linux run.

Integration checks exercise real managed SQLite callbacks, sparse byte offsets,
truncate shrink/regrowth, append, rename, stdin/environment/stdio, replay and ID
conflicts, and the three real celld deadlock guards. They also check failed exits,
traps, CPU deadlines, overflowing output, cancellation, denied host reads and
network access, immutable runtime files, temporary-file quotas, cross-mount
rename rejection, and denied Wasm memory growth above the configured maximum.

The configured Bash WebC executes a pipeline using coreutils and writes a file
that TypeScript reads back. Python imports standard-library modules and writes
JSON through the same managed filesystem. Restarts discard the previous local
runtime directory and recover from celld's persisted LTX state. Mid-command
owner loss preserves the committed prefix, marks the command interrupted, and
rejects the old callback capability.

The separate fleet test establishes a healthy follower, pauses only its own
MinIO object store, and requires a write response before unpausing it. It then
kills the owner, deletes that node's synthetic working directory, and reads the
acknowledged data through a new owner. A second fault suspends an owner during a
CPU-bound execution, waits for takeover, verifies callback fencing and the
committed prefix, then resumes the former owner and checks self-fencing. The
retained final run acknowledged the paused-store write on attempt one. The
harness permits up to three fresh attempts because transient follower selection
can safely block availability; it records every attempt and never counts an
unacknowledged write as a durability success.

During qualification an initial 8 GiB process address-space limit prevented
Wasmer from reserving memory on Linux. The final runner uses a 128 GiB **virtual
address** ceiling, caps each Wasm memory at 512 MiB and each table at one million
elements, and relies on the service's 2 GiB cgroup for aggregate physical memory.
Those distinct limits are intentional. Allocation minima and subsequent growth
have native regression tests; the guest integration tests the effective limit.

## Reproduce

Follow the [build instructions](README.md#build-and-configure), including the
hash-pinned tool catalog. From the package directory:

```sh
npm ci --ignore-scripts
npm run check
npm run format:check
npm test
cargo fmt --manifest-path runner/Cargo.toml --check
cargo clippy --locked --all-targets --manifest-path runner/Cargo.toml -- -D warnings
cargo test --locked --manifest-path runner/Cargo.toml
cargo build --locked --manifest-path runner/Cargo.toml
rustup target add wasm32-wasip1 --toolchain stable
rustup run stable rustc --target wasm32-wasip1 -O test/guest.rs -o test/guest.wasm
SANDBOX_TEST_TOOLS="$PWD/tools/tools.json" npm run test:integration
npm run test:fleet
```

For the restricted Linux container, build these local images from the repository
root. The test image includes the compiled guest from the preceding commands:

```sh
docker build --target build --build-arg CELLD_PROFILE=lab -t celld-wasmer-runtime:qualification .
docker build -t celld-wasmer-sandbox:qualification packages/wasmer-sandbox
docker build -f packages/wasmer-sandbox/test/Linux.Dockerfile \
  -t celld-wasmer-linux-tests:qualification packages/wasmer-sandbox
```

Generate the container-path tool catalog and run the tests from the package
directory. Use a fresh evidence volume so previous generated Worker paths cannot
interfere. Only the three public WebC files and their catalog are mounted here.

```sh
node --input-type=module -e '
import {readFile,writeFile} from "node:fs/promises";
const catalog=JSON.parse(await readFile("service/tools.lock.json","utf8"));
for(const [name,tool] of Object.entries(catalog)) tool.path="/tools/"+name+".webc";
await writeFile("tools/tools-linux.json",JSON.stringify(catalog));'
docker volume create celld-wasmer-linux-evidence
docker run --name celld-wasmer-linux-qualification --read-only \
  --mount type=volume,source=celld-wasmer-linux-evidence,target=/app/test/artifacts \
  --tmpfs /tmp:rw,size=64m,mode=1777 --memory 2g --cpus 2 --pids-limit 256 \
  --cap-drop ALL --security-opt no-new-privileges \
  -v "$PWD/tools:/tools:ro" -e SANDBOX_TEST_TOOLS=/tools/tools-linux.json \
  celld-wasmer-linux-tests:qualification
# Copy the printed results.json path out before removing your test container/volume.
```

The [CI workflow](../../.github/workflows/wasmer-sandbox.yml) repeats native
Linux unit, integration and three-node checks with pinned tool downloads. It has
been added but has not run on GitHub in this task. Raw local test directories may
contain synthetic credentials; only selected non-secret results are retained.

## Deployment boundary

This evidence covers the constrained API in the README and the pinned tool
catalog. It does not establish arbitrary native Linux/ELF compatibility, Python
native extensions, an interactive terminal, a persistent process session,
streaming output, network-enabled guests, preview servers, or Cloudflare Sandbox
SDK compatibility. Cancellation and failure preserve committed filesystem
operations; arbitrary commands are not atomic or exactly once.

No production credentials, cloud account, external fleet, registry, or public
endpoint was changed. Linux x86-64, target-provider object-store behavior,
multi-machine network partitions, workload capacity/latency objectives, and an
independent adversarial sandbox assessment remain deployment qualification work.
Use the required cgroup, privilege and egress restrictions from the README;
Wasmer resource limits alone do not bound all runtime/compiler memory. The
executor's shared service token is an internal trust boundary, so applications
must authorize tenant access before forwarding requests.
