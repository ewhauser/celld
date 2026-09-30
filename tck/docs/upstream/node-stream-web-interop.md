# `node:stream` exposes Readable.toWeb/fromWeb but both throw TypeError

## Version and reproduction

Confirmed on celld **v0.5.0**, compatibility date 2026-07-30, compatibility flag `nodejs_compat`, with bucket durability on MinIO. Reference: workerd 1.20260730.1 through Miniflare 4.20260730.0. Evidence run `tck-3168c226-4b59-4847-b179-739fd47fa9f7`.

```ts
import { Readable } from "node:stream";

Readable.toWeb(Readable.from(["a", "λ"]));
Readable.fromWeb(new Response("aλ").body);
```

Both are exported functions on celld, and both throw synchronously:

```
TypeError: lazyWebStreams(...).newReadableStreamFromStreamReadable is not a function
    at Readable.toWeb (<anonymous>:3418:31)
```

| Observation                               | workerd         | celld       |
| ----------------------------------------- | --------------- | ----------- |
| `Readable.toWeb` over object-mode chunks  | `["a", "λ"]`    | `TypeError` |
| `Readable.toWeb` over `Uint8Array` chunks | `[0, 128, 255]` | `TypeError` |
| `Readable.fromWeb` over a `Response` body | `"aλ"`          | `TypeError` |

`node:timers/promises` in the same case conforms: the shorter `setTimeout` wins the race, `setImmediate` resolves with its value, and an already-aborted signal rejects with `AbortError`.

Run `pnpm tck --profile local --suite node --case node.stream-timers` to reproduce.

## Contract

[celld's compatibility page](https://celld.dev/docs/cloudflare-compat/) lists `node:stream` among the implemented Node.js modules and documents no exclusion for the Web Streams adapters; [limitations](https://celld.dev/docs/limitations/) does not mention them either. Cloudflare's `nodejs_compat` implementation of [`node:stream`](https://nodejs.org/api/stream.html) provides `Readable.toWeb` and `Readable.fromWeb`, and the reference run exercises both.

## Impact and repair boundary

Libraries that bridge Node streams and Web streams — body handling, compression and parsing pipelines, and most `node:stream`-based SDKs ported to Workers — fail at the conversion boundary. The failure is a `TypeError` about a missing internal function rather than a "not implemented" rejection, so it does not read as an unsupported feature at the call site.

Either implement the `lazyWebStreams` bridge or remove the exports so the missing capability is detectable. This observation covers `Readable.toWeb` and `Readable.fromWeb` only; it does not describe `Writable`/`Duplex` conversions, `pipeline`, or `compose`.
