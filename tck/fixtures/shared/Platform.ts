import { Effect } from "effect";

// Adapts a Promise-returning platform API into an Effect once, at the boundary.
// The rejection value is surfaced unchanged because fixtures echo it back in responses.
export const platform = <A>(operation: () => PromiseLike<A>) =>
  Effect.tryPromise({
    try: () => Promise.resolve(operation()),
    catch: (cause) => cause,
  });

// Readiness probe shared by the Durable Object fixtures.
export const ready = () => Response.json({ ready: true });
