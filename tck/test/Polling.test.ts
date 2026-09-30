import { expect, it } from "@effect/vitest";
import { Deferred, Effect, Fiber } from "effect";
import { TestClock } from "effect/testing";
import { TckError } from "../src/Domain.js";
import {
  pollUntil,
  retryRead,
  pendingPhase,
  waitForReady,
} from "../src/Polling.js";
import { equal } from "../src/Oracle.js";

const options = {
  interval: "1 second",
  attempts: 3,
  timeout: "10 seconds",
} as const;
it.effect("polls pending observations within the attempt bound", () =>
  Effect.gen(function* () {
    let calls = 0;
    const fiber = yield* pollUntil(
      Effect.sync(() => ++calls),
      (value) => value === 3,
      { ...options, message: "waiting" },
    ).pipe(Effect.forkChild);
    yield* TestClock.adjust("3 seconds");
    expect(yield* Fiber.join(fiber)).toBe(3);
    expect(calls).toBe(3);
  }),
);
it.effect(
  "does not retry probe errors, including a coincident pending phase",
  () =>
    Effect.gen(function* () {
      for (const phase of ["assertion", "pending", "http"]) {
        let calls = 0;
        const error = new TckError({ phase, message: "bad observation" });
        const outcome = yield* pollUntil(
          Effect.suspend(() => {
            calls++;
            return Effect.fail(error);
          }),
          () => true,
          { ...options, message: "waiting" },
        ).pipe(Effect.exit);
        expect(outcome._tag).toBe("Failure");
        expect(calls).toBe(1);
      }
    }),
);
it.effect(
  "bounds perpetual pending and retries only explicitly transient failures",
  () =>
    Effect.gen(function* () {
      let calls = 0;
      const fiber = yield* retryRead(
        Effect.suspend(() => {
          calls++;
          return Effect.fail(
            new TckError({ phase: "transient", message: "not ready" }),
          );
        }),
        { ...options, retryable: pendingPhase("transient") },
      ).pipe(Effect.exit, Effect.forkChild);
      yield* TestClock.adjust("4 seconds");
      expect((yield* Fiber.join(fiber))._tag).toBe("Failure");
      expect(calls).toBe(3);
    }),
);
it.effect(
  "bounds a hung read and propagates cancellation through its scope",
  () =>
    Effect.gen(function* () {
      for (const interrupt of [false, true]) {
        const entered = yield* Deferred.make<void>();
        let cleaned = false;
        const probe = Effect.scoped(
          Effect.gen(function* () {
            yield* Effect.addFinalizer(() =>
              Effect.sync(() => {
                cleaned = true;
              }),
            );
            yield* Deferred.succeed(entered, undefined);
            return yield* Effect.never;
          }),
        );
        const fiber = yield* pollUntil(probe, () => false, {
          ...options,
          message: "waiting",
        }).pipe(Effect.exit, Effect.forkChild);
        yield* Deferred.await(entered);
        if (interrupt) yield* Fiber.interrupt(fiber);
        else {
          yield* TestClock.adjust("11 seconds");
          expect((yield* Fiber.join(fiber))._tag).toBe("Failure");
        }
        expect(cleaned).toBe(true);
      }
    }),
);
it.effect(
  "readiness retries transport/service unavailability but checks successful bodies once",
  () =>
    Effect.gen(function* () {
      let calls = 0;
      const probe = Effect.suspend(() => {
        calls++;
        if (calls === 1)
          return Effect.fail(
            new TckError({ phase: "http", message: "connection refused" }),
          );
        return Effect.succeed({
          status: calls === 2 ? 503 : 200,
          headers: {},
          body: "wrong",
        });
      });
      const fiber = yield* waitForReady(probe, {
        ...options,
        attempts: 5,
      }).pipe(
        Effect.flatMap((response) => equal(response.body, "ready")),
        Effect.exit,
        Effect.forkChild,
      );
      yield* TestClock.adjust("4 seconds");
      expect((yield* Fiber.join(fiber))._tag).toBe("Failure");
      expect(calls).toBe(3);
      let invalidCalls = 0;
      const outcome = yield* waitForReady(
        Effect.sync(() => {
          invalidCalls++;
          return { status: 401, headers: {}, body: "denied" };
        }),
        options,
      ).pipe(Effect.exit);
      expect(outcome._tag).toBe("Failure");
      expect(invalidCalls).toBe(1);
    }),
);
