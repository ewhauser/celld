# `websocket_standard_binary_type` is not applied; `binaryType` stays `arraybuffer`

## Version and reproduction

Confirmed on celld **v0.5.0**, compatibility date 2026-07-30, no explicit compatibility flags, with bucket durability on MinIO. Reference: workerd 1.20260730.1 through Miniflare 4.20260730.0. Evidence run `tck-fe61abc7-a30e-41b9-96d7-ca3a68a09757`.

A Worker opens an outbound WebSocket to its own Durable Object, accepts it, and reads the default `binaryType` before sending anything:

```ts
const response = await stub.fetch("https://fixture.test/websocket/echo", {
  headers: { Upgrade: "websocket" },
});
const socket = response.webSocket!;
socket.accept();
socket.binaryType; // "blob" on workerd, "arraybuffer" on celld
```

| Compatibility flags                 | workerd         | celld           |
| ----------------------------------- | --------------- | --------------- |
| none (defaults at 2026-07-30)       | `"blob"`        | `"arraybuffer"` |
| `no_websocket_standard_binary_type` | `"arraybuffer"` | `"arraybuffer"` |

celld therefore reports the legacy value in both configurations: the enable path is not applied, and the disable path matches only because it already produces the legacy value.

Reproduce with `pnpm tck --profile local --suite flags`. The sibling case `flags.disabled-counterparts` passes, and the `delete_all_deletes_alarm` / `delete_all_preserves_alarm` pair exercised by the same request is honored correctly in both variants, so this is specific to the WebSocket switch rather than to compatibility-flag handling as a whole.

## Contract

[celld's compatibility page](https://celld.dev/docs/cloudflare-compat/) states that celld honors `websocket_standard_binary_type` among its compatibility switches. Cloudflare's [compatibility flags reference](https://developers.cloudflare.com/workers/configuration/compatibility-flags/) documents that flag as default-on from 2026-03-17 and describes it as setting the `binaryType` default to `"blob"` to match the WebSocket specification, with `no_websocket_standard_binary_type` as the disable counterpart. The pinned compatibility date 2026-07-30 is after that default date, so the standard value is expected without naming the flag.

## Impact and repair boundary

Code that relies on the specification default receives `ArrayBuffer` message payloads where it expects `Blob`, so `instanceof` checks and `await data.arrayBuffer()` calls on binary frames fail or take the wrong branch. Because the value is silently legacy rather than an error, the mismatch surfaces only when a binary frame arrives.

This report covers the default value of `binaryType` on an accepted outbound socket. It does not describe whether assigning `binaryType` is honored, nor the delivered payload type for binary frames, neither of which this case observes.
