import { expect, it } from "@effect/vitest";
import { Effect, Fiber, Ref } from "effect";
import { cleanupAll, owned } from "../src/Resources.js";

it.effect("cleans partially acquired resources after setup fails", () =>
  Effect.gen(function* () {
    const cleaned = yield* Ref.make(false);
    yield* Effect.exit(
      Effect.scoped(
        owned(
          Effect.fail("partial setup"),
          Ref.set(cleaned, true),
          () => Effect.void,
        ),
      ),
    );
    expect(yield* Ref.get(cleaned)).toBe(true);
  }),
);
it.effect("cleans resources when the owner is interrupted", () =>
  Effect.gen(function* () {
    const cleaned = yield* Ref.make(false);
    const acquired = yield* Ref.make(false);
    const fiber = yield* Effect.scoped(
      owned(
        Ref.set(acquired, true).pipe(Effect.andThen(Effect.never)),
        Ref.set(cleaned, true),
        () => Effect.void,
      ),
    ).pipe(Effect.forkChild);
    yield* Effect.yieldNow;
    expect(yield* Ref.get(acquired)).toBe(true);
    yield* Fiber.interrupt(fiber);
    expect(yield* Ref.get(cleaned)).toBe(true);
  }),
);
it.effect("reports cleanup failure instead of silently succeeding", () =>
  Effect.gen(function* () {
    const errors = yield* Ref.make<ReadonlyArray<string>>([]);
    yield* Effect.scoped(
      owned(Effect.void, Effect.fail("cleanup broke"), (error) =>
        Ref.update(errors, (all) => [...all, error]),
      ),
    );
    expect((yield* Ref.get(errors)).join(" ")).toContain("cleanup broke");
  }),
);
it.effect("attempts teardown even when diagnostic collection fails", () =>
  Effect.gen(function* () {
    const cleaned = yield* Ref.make(false);
    const result = yield* Effect.exit(
      cleanupAll([Effect.fail("logs unavailable"), Ref.set(cleaned, true)]),
    );
    expect(result._tag).toBe("Failure");
    expect(yield* Ref.get(cleaned)).toBe(true);
  }),
);
