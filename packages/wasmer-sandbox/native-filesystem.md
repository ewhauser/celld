# Native AgentFS over local IPC

![Native filesystem architecture](native-filesystem.png)

The SDK defaults to the native filesystem. All Wasmer filesystem operations
(`stat`, `list`, `mkdir`, `open`, `close`, `fstat`, `read`, `write`, `truncate`,
`rename`, `unlink`, `rmdir`, `sync`, and `heartbeat`) use a persistent Unix socket.
There is no JavaScript filesystem callback on this path and no HTTP fallback.

TypeScript calls the same native implementation through `ctx.agentFsOperation`.
The SDK initializes the AgentFS 0.4 schema through managed SQL, then configures
the native backend's limits. The owning cell's existing SQLite connection is the
only database authority; neither the supervisor nor the helper opens its files.
`ctx.agentFsCapability` grants/revokes one process-local command capability.
These celld-specific methods are not Cloudflare platform APIs. IPC capabilities
currently support standalone cells; embedded facets are rejected.

## Execution boundary

The shell authenticates a resident owner's capability, admits the local request,
and enters a serialized cell turn under the worker/isolate lock. The native
backend executes outside JavaScript, on that cell's managed SQLite connection.
The answer carries its written or observed position through the ordinary celld
output gate. Reads, errors, sync and heartbeat cannot expose an unreplicated
write. Healthy follower fsync or object-store durability satisfies the existing
fleet policy; no separate database or durability policy was added.

Each mutating operation uses celld's normal transaction controls and SQL critical
error handling. In-memory handle changes publish only after successful commit.
Pathname operations can join an application's outer transaction using a
savepoint. Handle lifecycle operations (`open`, `close`, `closeAll`, configure)
return `EBUSY` inside an outer SQL transaction: process-local handles cannot be
rolled back with SQL. `exec` still rejects SQL transactions and closed JS input
gates before launch. Guest requests fail closed with `EBUSY` while an application
input gate or SQL transaction is active.

## Files and handles

Native and TypeScript operations share inode lookup, quotas, sparse I/O, append
positioning, directory rules and open-inode protection. Handles are tagged by
application versus command ownership; a guest cannot guess an application
handle. Rename preserves source handles. Removing or replacing any open inode
returns `EBUSY`. Revocation drops the command's handles while retaining application
handles; SDK command cleanup also closes its application handles as before.

Default limits are 64 MiB total logical bytes, 16 MiB per file, 4,096 inodes and
128 handles. Native configurations allow at most 4,096 inodes and 4,096 handles.
Read/write requests carry at most 64 KiB; TypeScript `readFile`/`writeFile` are
limited to 1 MiB and replacement is atomic. Growing files remain sparse.
Paths stay under `/workspace`, with at most 4,096 UTF-8 bytes, 64 components and
255 bytes per component. Symlinks and special files are rejected. Directory
listings are bounded to 4,096 entries and the response frame limit.

## Transport and fencing

Set `CELLD_AGENTFS_SOCKET` on celld and `filesystemSocket` on the colocated
supervisor. The socket parent must be owned by celld's UID with mode 0700; the
socket is 0600. The helper needs the same UID and access to that directory.
The server refuses existing sockets at startup; it never unlinks a possibly
live owner's socket. It does not wake dormant cells or forward to remote owners.

Protocol version 2 uses a u32 little-endian outer frame length, a version byte,
a u32 metadata length, UTF-8 JSON metadata, then raw file bytes. Request metadata
is bounded to 16 KiB and frames to 2 MiB; payloads are bounded to 1 MiB. File bytes
are never represented as JSON integer arrays on IPC or the V8/native boundary.
Metadata contains scope, token, sequence and an operation; responses contain
`value` or an errno `code`, followed by read bytes when applicable.

Each command has a random activation token, absolute deadline and monotonic
sequence, capped at 100,000 requests. Authentication precedes input-gate/state
inspection. Ordinary filesystem errors consume the sequence. A closed input gate
returns `EBUSY` without consuming it, but the production helper terminates the
command on this condition. At most 32 socket connections are active. Partial
frames and active requests have five-second deadlines; idle connections have
310 seconds. The supervisor separately enforces command deadlines.

Transport loss, malformed replies, stale capabilities, busy gates or native I/O
failure poison the helper. There is no reconnect, replay of an ambiguous
mutation, or switch to HTTP. A committed operation can survive even if its reply
is lost; the command becomes interrupted and must not be automatically rerun.
Capabilities and handles are never persisted or restored after owner loss.

The HTTP implementation is retained only as an explicit reference/remote backend
(`nativeFilesystem: false` / `SANDBOX_NATIVE_FILESYSTEM=0`). Its configuration and
filesystem callback token are separate. Native helpers receive neither a callback
URL nor a callback token. Execute/cancel and command results still use the
supervisor's authenticated HTTP control API.

See [qualification.md](qualification.md) for validation. The older
[stat experiment](ipc-experiment.md) and its benchmarks are historical evidence,
not performance claims for full file I/O.
