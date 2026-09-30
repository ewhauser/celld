# DO storage list rejects a negative limit with Error instead of TypeError

## Version and reproduction

Confirmed on celld v0.5.0 at upstream commit `12d5b6333fe52717325addcfe1e99e9fd4f77bcd`, using SQLite DO storage, compatibility date 2026-07-30, and bucket durability on MinIO. Reference: workerd 1.20260730.1 through Miniflare 4.20260730.0.

Inside a Durable Object, capture the error from `storage.list({ limit: -1 })`. Also evaluate `[...storage.kv.list({ limit: -1 })]` inside a synchronous error boundary.

```ts
const asyncResult = Effect.tryPromise({
  try: () => this.ctx.storage.list({ limit: -1 }),
  catch: (error) => error,
});
const syncResult = Effect.try({
  try: () => [...this.ctx.storage.kv.list({ limit: -1 })],
  catch: (error) => error,
});
```

Both operations reject on both engines. Workerd throws TypeError; celld throws a generic Error. Neither result is a DOMException. A valid `limit: 1` call succeeds on both engines.

Run `pnpm tck --profile local --suite repros --case repro.storage-errors` using [the reproduction fixture and guide](README.md). The checked-in observations show the asynchronous and synchronous results independently.

## Contract and source investigation

The [Cloudflare storage API](https://developers.cloudflare.com/durable-objects/api/sqlite-storage-api/) supplies the API contract. The exact TypeError classification in this report is established by the pinned workerd execution; it is not presented as an explicit promise in the prose documentation for every invalid options value.

- [`StorageListOptions.limit`](https://github.com/denoland/celld/blob/12d5b6333fe52717325addcfe1e99e9fd4f77bcd/crates/celld/js/storage_ops.rs#L784) is `Option<usize>`.
- [Asynchronous options decoding](https://github.com/denoland/celld/blob/12d5b6333fe52717325addcfe1e99e9fd4f77bcd/crates/celld/js/storage_ops.rs#L793) and [synchronous options decoding](https://github.com/denoland/celld/blob/12d5b6333fe52717325addcfe1e99e9fd4f77bcd/crates/celld/js/storage_ops.rs#L831) send deserialization errors to `throw_storage_error`.
- [`throw_storage_error`](https://github.com/denoland/celld/blob/12d5b6333fe52717325addcfe1e99e9fd4f77bcd/crates/celld/js/storage_ops.rs#L1228) creates `v8::Exception::error`, explaining the generic Error class.

## Impact and repair boundary

Code that distinguishes invalid arguments using `instanceof TypeError` behaves differently. The invalid call is rejected; no incorrect list or data corruption was observed.

Separate argument-validation errors from backend/storage failures. Do not globally change `throw_storage_error` to TypeError, since database, I/O, and other operational failures also use it. Validate option coercion against workerd before broadening the fix beyond this negative-limit case; the observation here does not define behavior for fractional limits, strings, NaN, or infinity.
