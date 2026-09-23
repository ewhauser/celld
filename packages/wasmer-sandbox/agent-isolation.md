# Agent execution isolation

The security boundary combines per-agent API authorization, execution-scoped
native filesystem grants, a separate Wasmer helper process per command, private
execution state, and kernel resource limits. celld remains the sole owner of
managed SQLite. A helper never mounts an agent database or a host workspace.

This design trusts Wasmer, its native bindings, the supervisor, celld Worker
code, the credential issuer and host administrators. It does not protect against
a Wasmer/native-runtime escape. cgroups constrain resource consumption; they are
not memory-access or host-filesystem security boundaries. There is no microVM,
Firecracker or in-process Wasmer embedding in this profile.

## Private state and caches

Each execution receives a new virtual `/tmp`, `/tmp/home` and private XDG cache,
configuration and data directories. Temporary data is bounded by the existing
16 MiB buffer quota and discarded when the helper exits. The separate host
scratch directory has a random name, mode 0700 and a private host environment.
It is removed after exit, cancellation or deadline, before returning the result
or releasing the workspace's execution slot. Linux kills the native helper if
its supervisor dies; startup additionally kills orphan command cgroups before
removing their scratch and admitting requests. A SQLite OS lock prevents two
supervisors from using the same runtime directory, including after SIGKILL.

The configured global tool catalog accepts only entries marked `public: true`,
verified against their SHA-256 digest. All configured WebC dependencies are
visible to all commands: never register private agent packages in this catalog.
Mount these artifacts read-only and make them administrator-owned. Compiled
module caches are in-memory and local to a fresh runtime. Guest-created caches
stay in private `/tmp` or the agent's authorized durable `/workspace`.

## Secrets and output

Each tool has an `envAllowlist`; the default is empty. Requests containing other
keys fail admission. Supervisor/control credentials, loader options and private
HOME/TMPDIR/XDG paths cannot be allowlisted. Guest environment values come only
from explicitly allowed request values plus fixed private directory variables;
the helper's host environment is separately constructed without inheriting the
supervisor's environment. A guest can deliberately print any secret explicitly
supplied to it, so treat its authorized stdout/stderr as private data too.

Native compiler/trap diagnostics are discarded. Shared logs receive only an
opaque workspace ID, random execution ID, byte count and event type. Command
arguments, environment, stdin, grant tokens and raw diagnostics are not logged.
Results publish only known output/counter fields. Retry keys include the
workspace and execution grant; retained request metadata consists of hashes.
The retry cache holds at most 128 entries for five minutes, with a 16 MiB output
budget. Payload eviction leaves a tombstone and returns 410 rather than rerunning
a command. The owning workspace journal is the durable authority for results.
Application log sinks and journal retention still need their normal access policy.

## Production setup

Requires Linux cgroup v2 with CPU, memory and PID controllers and `cgroup.kill`,
Node 24.12+, the pinned runner build and the owning node's private filesystem IPC
socket. Run celld and the executor with the same dedicated UID to access the
0700 socket directory and 0600 socket. The executor never needs root.

The example [systemd unit](service/celld-sandbox.service) delegates controllers
to that UID. Its [launcher](service/start-delegated.sh) moves the supervisor into
a leaf and creates an empty commands parent, enabling controllers before launch.
The service is a deployment template; install paths and limits for your host:

1. Install this package and production dependencies at `/opt/celld-sandbox`, the
   runner at `/usr/local/bin/celld-wasmer-runner`, Node 24.12+ at `/usr/bin/node`,
   and public hash-pinned tools under `/tools`. Keep code/tools root-owned.
2. Install [config.example.json](service/config.example.json) as
   `/etc/celld-sandbox/config.json`. Keep its loopback listener for a colocated
   owner. Configure the filesystem socket and tool paths. The launcher supplies
   the actual `cgroupParent` through `CELLD_SANDBOX_CGROUP_PARENT`.
3. Put the Worker-matching `CELLD_SANDBOX_TOKEN` in a root-readable
   `/etc/celld-sandbox/secrets.env` (mode 0600). Do not put it in guest environments.
4. Install the unit, reload systemd and start it. `/healthz` must report
   `ready: true` and `perCommandResources: true` before admitting traffic.

For another service manager, precreate an empty writable cgroup parent with
`cpu memory pids` in `cgroup.subtree_control`, and a stable 0700 runtime directory
owned by the supervisor. **Both paths must belong exclusively to one supervisor;
never reuse a cgroup parent with a different runtime directory.** Parent migration
permissions must also permit moving the child from the supervisor's leaf to a
command subgroup. The supervisor fails startup on missing delegation, a shared
lease or unsafe runtime-directory permissions. Teardown failure stops admission;
restart after fixing the underlying filesystem/controller problem.

The default per-command cgroup limits cover the whole native process tree:

| Setting | Default | Meaning |
| --- | --- | --- |
| `resources.memoryBytes` | 1 GiB | Resident/runtime memory limit; swap disabled; group OOM kill |
| `resources.cpuMillis` | 1000 | One CPU worth of time per second; enforced with a 100 ms period |
| `resources.pids` | 64 | All native processes and threads in the execution tree |

The supervisor attaches the waiting helper to its cgroup before sending any
configuration. Existing wall-clock deadlines, guest-task, Wasm-memory, filesystem
and output limits still apply. The example service adds an aggregate 3 GiB,
two-CPU, 256-task budget for two concurrent commands and supervisor overhead.
Tune the service and per-command budgets together. Shared-host contention and
failure of the whole supervisor can still affect multiple agents.

`development: true` explicitly permits operation without command cgroups. The
[development config](service/config.development.example.json) and Compose profile
use this mode; Compose has aggregate container limits only. Do not use it to
claim per-agent kernel resource isolation in production. Existing production
configs must add the stable runtime directory, delegation and `public: true`
for every shared tool; add only the environment keys each tool actually needs.

## Validation

`npm test` includes concurrent synthetic helpers with identical command IDs and
execution tokens, distinct private results/environments/scratch, cross-agent
cancellation, diagnostics redaction, cleanup, cache pressure and lease exclusion.
Rust tests check fresh temporary filesystems and home/cache directories.

The real runtime integration starts simultaneous Wasm commands using identical
paths and IDs under different agent credentials. It verifies their private file,
temporary, cache and output canaries; cancellation of one command leaves the
other intact. Restart and three-node owner takeover tests preserve distinct
private durable files and fence stale access.

On a Linux host, test real kernel memory exhaustion, fork limits, CPU throttling,
whole-tree cleanup and SIGKILL recovery as an unprivileged user:

```sh
sudo sh test/resources-linux.sh "$(command -v node)" "$PWD" "$(id -u)" "$(id -g)"
```

The setup shell needs root only to create/delegate its own disposable test cgroup.
Every assertion and allocation runs under the supplied non-root UID. For a full
integration using an already-delegated exclusive subtree, set
`SANDBOX_TEST_CGROUP_PARENT` and `SANDBOX_TEST_TOOLS`, then run
`SANDBOX_NATIVE_FILESYSTEM=1 npm run test:integration`. The harness uses production
resource admission whenever that parent is set. Keep generated test configs and
raw logs private; selected non-secret results are in the qualification record.
