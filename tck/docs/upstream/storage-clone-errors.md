# DO storage put throws TypeError for uncloneable values instead of DataCloneError

## Version and reproduction

Confirmed on celld v0.5.0 at upstream commit `12d5b6333fe52717325addcfe1e99e9fd4f77bcd`, compatibility date 2026-07-30, SQLite DO storage, and bucket durability on MinIO. Reference: workerd 1.20260730.1 through Miniflare 4.20260730.0.

Inside a Durable Object, try to store a function through each public path:

```ts
const single = Effect.tryPromise({
  try: () => this.ctx.storage.put("fn", () => 1),
  catch: (error) => error,
});
const synchronous = Effect.try({
  try: () => this.ctx.storage.kv.put("sync-fn", () => 1),
  catch: (error) => error,
});
const batch = Effect.tryPromise({
  try: () =>
    this.ctx.storage.put({ first: "must-not-commit", second: () => 1 }),
  catch: (error) => error,
});
```

All three reject. Workerd returns a DOMException whose name is `DataCloneError`; celld returns a TypeError that is not a DOMException. Inspecting `fn`, `sync-fn`, `first`, and `second` after the rejected operations finds no stored keys on either engine. In particular, the batch's valid first entry does not partially commit.

Run `pnpm tck --profile local --suite repros --case repro.storage-errors` using [the reproduction fixture and guide](README.md). The observations include the exception name, DOMException identity, and a post-rejection state check.

## Contract and source investigation

[Cloudflare storage](https://developers.cloudflare.com/durable-objects/api/sqlite-storage-api/) uses structured-clone-compatible values. The [structured serialization algorithm](https://html.spec.whatwg.org/multipage/structured-data.html#structuredserializeinternal) rejects callable values with DataCloneError; the exact public storage exception behavior was also verified against workerd.

- [`StorageValueDelegate::throw_data_clone_error`](https://github.com/denoland/celld/blob/12d5b6333fe52717325addcfe1e99e9fd4f77bcd/crates/celld/js/storage_ops.rs#L887) explicitly constructs `v8::Exception::type_error`.
- [`serialize_storage_value`](https://github.com/denoland/celld/blob/12d5b6333fe52717325addcfe1e99e9fd4f77bcd/crates/celld/js/storage_ops.rs#L1010) installs that delegate for persisted values.
- [The storage fallback](https://github.com/denoland/celld/blob/12d5b6333fe52717325addcfe1e99e9fd4f77bcd/crates/celld/js/harness.js#L4783) rethrows the original error when there are no liftable stored stubs, so the TypeError reaches the application.
- [RPC already has a DataCloneError wrapper](https://github.com/denoland/celld/blob/12d5b6333fe52717325addcfe1e99e9fd4f77bcd/crates/celld/js/harness.js#L3584), but that does not normalize the storage path.

## Impact and repair boundary

Error-name and `instanceof DOMException` checks differ from workerd. This reproduction shows correct rejection and no partial writes; it does not demonstrate data loss.

Repair the classification at the serialization boundary while preserving original errors from user getters and operational storage failures. Do not blanket-wrap every `put()` failure as DataCloneError. Preserve synchronous, asynchronous, batch, and stored-stub behavior. The transient serializer also constructs TypeError but serves other call paths; changing it requires separate coverage rather than assuming every caller has the same contract.
