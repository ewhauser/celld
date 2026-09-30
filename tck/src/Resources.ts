import { Cause, Effect, Exit } from "effect";
import { TckError } from "./Domain.js";

export const cleanupAll = <E, R>(
  actions: ReadonlyArray<Effect.Effect<unknown, E, R>>,
) =>
  Effect.gen(function* () {
    const failures: string[] = [];
    for (const action of actions) {
      // Bound each step separately so a hung diagnostic does not prevent teardown.
      const exit = yield* Effect.exit(
        Effect.interruptible(action).pipe(Effect.timeout("15 seconds")),
      );
      if (Exit.isFailure(exit)) failures.push(Cause.pretty(exit.cause));
    }
    if (failures.length)
      return yield* Effect.fail(
        new TckError({ phase: "cleanup", message: failures.join("\n") }),
      );
  });

// Reserve ownership before acquisition starts: failed partial setup is still cleaned.
export const owned = <A, E, R, E2, R2>(
  acquire: Effect.Effect<A, E, R>,
  release: Effect.Effect<void, E2, R2>,
  onCleanupFailure: (detail: string) => Effect.Effect<void>,
) =>
  Effect.gen(function* () {
    yield* Effect.addFinalizer(() =>
      release.pipe(
        Effect.timeout("45 seconds"),
        Effect.catchCause((cause) => onCleanupFailure(Cause.pretty(cause))),
      ),
    );
    return yield* acquire;
  });
