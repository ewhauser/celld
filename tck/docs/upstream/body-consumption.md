# Request and Response body readers allow repeated consumption

## Version

celld v0.5.0, upstream [`12d5b6333fe52717325addcfe1e99e9fd4f77bcd`](https://github.com/denoland/celld/commit/12d5b6333fe52717325addcfe1e99e9fd4f77bcd), compatibility date 2026-07-30 with no Node flags. Reference: Miniflare 4.20260730.0 / workerd 1.20260730.1. Confirmed against the actual published celld image, using MinIO and bucket durability.

## Minimal operation

Construct a Request with a string body, consume it with `text()`, then call `text()` again. The first call succeeds and sets `bodyUsed` to true. The second call also succeeds on celld; workerd rejects with TypeError.

This Effect-based fragment is the essential operation in the attached fixture's fetch handler:

```ts
const body = new Request("https://example.test/", {
  method: "POST",
  body: "hello",
});
const program = Effect.gen(function* () {
  yield* Effect.promise(() => body.text());
  return yield* Effect.tryPromise({
    try: () => body.text(),
    catch: (error) => error,
  }).pipe(
    Effect.as("accepted"),
    Effect.catch((error) =>
      Effect.succeed(error instanceof Error ? error.name : "Unknown"),
    ),
  );
});
// Effect.runPromise(program): workerd -> "TypeError"; celld -> "accepted".
```

The full runnable fixture, configuration, commands, and captured observations are linked in [the reproduction guide](README.md).

## Expected and actual

| Operation                                                                                        | workerd                                              | celld v0.5.0                                        |
| ------------------------------------------------------------------------------------------------ | ---------------------------------------------------- | --------------------------------------------------- |
| Second `text()`, `json()`, `arrayBuffer()`, `blob()`, or `formData()` on a non-null Request body | TypeError                                            | Accepted                                            |
| Same five methods on a non-null Response body                                                    | TypeError                                            | Accepted                                            |
| Two `text()` calls on a null-body Request or Response                                            | Both resolve to empty string; bodyUsed remains false | Both resolve to empty string; bodyUsed becomes true |

All ten non-null combinations and both null-body controls were independently observed. The five readers use valid input for their respective format; JSON parsing or form parsing failures do not explain the difference.

The [Fetch Body mixin](https://fetch.spec.whatwg.org/#body-mixin) defines unusable bodies in terms of a non-null body's disturbed or locked stream. The [consume-body algorithm](https://fetch.spec.whatwg.org/#concept-body-consume-body) rejects unusable bodies with TypeError. A null body has no stream to disturb, so a blanket single-use flag also produces the wrong result.

## Source investigation

- [Response `_consume()` and readers](https://github.com/denoland/celld/blob/12d5b6333fe52717325addcfe1e99e9fd4f77bcd/crates/celld/js/harness.js#L322): `_consume()` returns cached `_bodyBytes`. The readers set `bodyUsed = true` without first rejecting a used body. The buffered Response path checks cancellation, but not prior consumption.
- [Request `_consume()` and readers](https://github.com/denoland/celld/blob/12d5b6333fe52717325addcfe1e99e9fd4f77bcd/crates/celld/js/harness.js#L493): the Request path similarly returns cached bytes, and `text()` memoizes the decoded string.
- [`__drainBody`](https://github.com/denoland/celld/blob/12d5b6333fe52717325addcfe1e99e9fd4f77bcd/crates/celld/js/harness.js#L139) also materializes bytes. Streaming-body behavior needs regression coverage before changing this shared path.

These source observations explain the reproduced buffered-body behavior; they do not establish the complete behavior of network streams or partially consumed readers.

## Impact and repair boundary

Workers that use rejection or `bodyUsed` to detect prior consumption behave differently. This report does not claim a security exploit or data loss.

A repair should centralize public Body consumption semantics, while distinguishing internal byte materialization from a public read. It should account for null, locked, disturbed, cancelled, and streaming bodies. Simply checking `bodyUsed` after a reader sets it would break every first read; simply forbidding every second read would break null-body reads. Add cross-method and direct-stream-reader regressions as well as the repeated-reader cases here.
