# Architecture image generation

Generated with the built-in imagegen tool from the user-provided Wasmer architecture image. Output: `native-filesystem.png`.

## Initial edit prompt

Use case: infographic-diagram edit.
Update the supplied architecture infographic to reflect the implemented full local IPC architecture. Preserve its polished pale-blue rounded panels, navy typography, teal V8 region, orange filesystem accents, simple icons, numbered lifecycle at right, landscape 3:2 composition and excellent readable type. Recompose panels as needed for correct containment and less crowding. High-resolution image.

Title, verbatim: "Inside celld: durable workspaces over local IPC"
Subtitle: "One SQLite owner. Shared native AgentFS. Wasmer runs outside V8."

Show ONE large boundary around the left two columns labeled "Owning Linux node • same host / same UID". The right lifecycle column is outside this boundary. There are two separate processes/services INSIDE this same-host boundary:
LEFT: "celld process • Rust". Within it a smaller teal "V8 isolate • JavaScript" containing a "Durable Object • Class + ID" with "Agent code + WasmerSandbox", a "Command journal" tile ("ID • payload hash • status") and a "WorkspaceFS API" tile. Authenticated API badge and arrow enter this Durable Object.
BELOW the V8 box, but still INSIDE the Rust celld process, an orange "Native AgentFS backend • Rust" panel: "Serialized cell turns • shared handles • quotas". Draw a vertical arrow from WorkspaceFS API to this backend labeled "Native host calls".
Below native backend show cylinder "Managed SQLite • per cell" with "AgentFS tables + app state + command journal". Native backend connects to SQLite with label "Short managed transactions". Command journal also persists in this same database.
Below SQLite show "celld replication + output gates" with subtitle "Gate filesystem replies and command results". From that show two durability destinations "Follower fsync" and "Object storage • LTX". These destinations can be at bottom as fleet durability, outside host boundary if needed. Do NOT label object storage as file blobs.

MIDDLE/RIGHT within same-host boundary: "Local executor service". At top "Supervisor • Node.js" with "Authenticate • admit • cancel • deadline". Below supervisor arrow "spawn / reap" to a "Per-command helper • Rust" box. Inside helper show "Wasmer runtime", "Pinned WASI / WASIX tools", "Bash / Python / custom modules", then "Virtual filesystem adapter". Inside adapter two tiles: "/workspace" labeled "Durable via IPC" and "/tmp" labeled "Local, ephemeral". Under helper: "Read-only runtime files • guest network disabled". Small badge "One active command per workspace".

Connect Durable Object to supervisor with blue arrows labeled "Execute / cancel • HTTP" and "Exit + buffered stdio". These are CONTROL ONLY.
Connect virtual filesystem adapter to the Native AgentFS backend (NOT to V8 or a JavaScript callback) with a prominent orange bidirectional arrow across the gap, labeled "Private Unix socket • all filesystem operations". Smaller label "Raw file bytes • capability + sequence". Add a nearby readable note "Replies pass celld durability gates". No shared SQLite mount, no helper database access, no filesystem HTTP arrow anywhere.

RIGHT column heading "Command lifecycle" with five blue numbered steps:
1 "Route to the owner" — "Resolve the workspace to its Durable Object."
2 "Journal and grant capability" — "Persist the command; bind a token to this activation."
3 "Start a bounded helper" — "The local supervisor launches Wasmer."
4 "Run native filesystem operations" — "IPC enters serialized cell turns; replies wait for durability."
5 "Finish or interrupt" — "Save the result; revoke capability and command handles."
Right bottom callout: "Committed files survive failure." and "Interrupted commands are not automatically rerun."

Bottom banner verbatim: "One durable filesystem. All guest file operations over local IPC."
Second line: "TypeScript and Wasmer share celld-managed SQLite through native AgentFS."
Small footer "celld • AgentFS • Wasmer"

Accuracy constraints: The native filesystem backend is outside the V8 isolate but inside the Rust celld process. Both executor and celld are on the same host. Wasmer runs in a separate helper process. Filesystem requests terminate at the native backend, not the Durable Object. Every filesystem operation including metadata, read/write, directory operations, handle operations, sync and heartbeat now uses IPC. The managed SQLite database is opened only by celld. Retain the visual style, but replace all obsolete remote executor or authenticated filesystem HTTP RPC labels. Avoid overlapping arrows and text.

## Final correction summary

Preserve the diagram and correct three details: replace the host subtitle with "celld and its executor share this host and user account."; label object storage "Durable snapshots and logs"; move the fleet durability destinations outside the owning-node boundary in a white cutout with a stepped lower border and label the group "Fleet durability • remote services". Preserve the executor inside the same-host boundary, the native backend outside V8, the Unix-socket arrow, and all other labels. No new components or database mounts.
