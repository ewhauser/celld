import { Effect } from "effect";
export { platform } from "../shared/Platform.js";
export const operation = <A>(body: () => A) =>
  Effect.try({ try: body, catch: (cause) => cause });
export const rejection = (effect: Effect.Effect<unknown, unknown>) =>
  effect.pipe(
    Effect.as("accepted"),
    Effect.catch((cause) =>
      Effect.succeed(cause instanceof Error ? cause.name : "Unknown"),
    ),
  );
// Observations that may legitimately be a value on one runtime and a rejection on
// another keep the error name in place of the value instead of failing the request.
export const outcome = <A>(effect: Effect.Effect<A, unknown>) =>
  effect.pipe(
    Effect.catch((cause) =>
      Effect.succeed(
        (cause instanceof Error ? cause.name : "Unknown") as A | string,
      ),
    ),
  );
