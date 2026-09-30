import { expect, it } from "@effect/vitest";
import { Cause, Deferred, Effect, Exit, Fiber } from "effect";
import { fleetFaults } from "../src/FleetFaults.js";
import { Artifacts } from "../src/Artifacts.js";
import { Processes } from "../src/Processes.js";
import { TckError } from "../src/Domain.js";

const harness = () => {
  const state = {
    Id: "owned",
    HostConfig: { Memory: 256, MemorySwap: 768 },
    State: { OOMKilled: false },
    NetworkSettings: {
      Networks: {
        tck_default: { Aliases: ["peer-celld", "custom"] },
        tck_store: { Aliases: null },
      } as Record<string, { Aliases: string[] | null }>,
    },
  };
  const calls: string[][] = [];
  const evidence: string[] = [];
  const run = (_file: string, args: readonly string[]) =>
    Effect.sync(() => {
      calls.push([...args]);
      if (args[0] === "update")
        state.HostConfig = {
          Memory: Number(args[2]),
          MemorySwap: Number(args[4]),
        };
      if (args[1] === "disconnect")
        delete state.NetworkSettings.Networks.tck_default;
      if (args[1] === "connect")
        state.NetworkSettings.Networks.tck_default = {
          Aliases: args.filter((_, i) => args[i - 1] === "--alias"),
        };
      return {
        stdout: args[0] === "inspect" ? JSON.stringify([state]) : "",
        stderr: "",
      };
    });
  const provide = <A, E, R>(
    effect: Effect.Effect<A, E, R | Artifacts | Processes>,
    override:
      | typeof run
      | ((
          file: string,
          args: readonly string[],
        ) => Effect.Effect<{ stdout: string; stderr: string }, TckError>) = run,
  ) =>
    effect.pipe(
      Effect.provideService(Artifacts, {
        directory: "/unused",
        text: (name) =>
          Effect.sync(() => {
            evidence.push(name);
          }),
        json: () => Effect.void,
      }),
      Effect.provideService(Processes, { run: override }),
    );
  const controls = fleetFaults("tck", () => Effect.succeed({ Id: "owned" }));
  return { state, calls, evidence, run, provide, controls };
};
it.effect(
  "restores exact prior memory settings and peer aliases after scenario failure",
  () =>
    Effect.gen(function* () {
      const h = harness();
      const result = yield* h
        .provide(
          Effect.scoped(
            Effect.gen(function* () {
              const controls = yield* h.controls;
              yield* controls.memoryLimit("celld", { bytes: 128 });
              yield* controls.partitionPeers("celld");
              expect(h.state.HostConfig.Memory).toBe(128);
              expect(
                h.state.NetworkSettings.Networks.tck_default,
              ).toBeUndefined();
              return yield* Effect.fail(
                new TckError({
                  phase: "assertion",
                  message: "scenario failed",
                }),
              );
            }),
          ),
        )
        .pipe(Effect.exit);
      expect(result._tag).toBe("Failure");
      expect(h.state.HostConfig).toEqual({ Memory: 256, MemorySwap: 768 });
      expect(h.state.NetworkSettings.Networks.tck_default?.Aliases).toEqual([
        "peer-celld",
        "custom",
      ]);
      expect(h.evidence.length).toBeGreaterThan(0);
    }),
);
it.effect("restores a memory mutation interrupted before its response", () =>
  Effect.gen(function* () {
    const h = harness();
    const entered = yield* Deferred.make<void>();
    const run = (file: string, args: readonly string[]) =>
      h
        .run(file, args)
        .pipe(
          Effect.flatMap((value) =>
            args[0] === "update" && args[2] === "128"
              ? Deferred.succeed(entered, undefined).pipe(
                  Effect.andThen(Effect.never),
                )
              : Effect.succeed(value),
          ),
        );
    const fiber = yield* h
      .provide(
        Effect.scoped(
          Effect.gen(function* () {
            const controls = yield* h.controls;
            yield* controls.memoryLimit("celld", { bytes: 128 });
          }),
        ),
        run,
      )
      .pipe(Effect.forkChild);
    yield* Deferred.await(entered);
    yield* Fiber.interrupt(fiber);
    expect(h.state.HostConfig).toEqual({ Memory: 256, MemorySwap: 768 });
  }),
);
it.effect(
  "reconnects after a disconnect takes effect but reports failure",
  () =>
    Effect.gen(function* () {
      const h = harness();
      const run = (file: string, args: readonly string[]) =>
        h.run(file, args).pipe(
          Effect.flatMap((value) =>
            args[1] === "disconnect"
              ? Effect.fail(
                  new TckError({
                    phase: "process",
                    message: "lost response",
                  }),
                )
              : Effect.succeed(value),
          ),
        );
      const result = yield* h
        .provide(
          Effect.scoped(
            Effect.gen(function* () {
              const controls = yield* h.controls;
              yield* controls.partitionPeers("celld");
            }),
          ),
          run,
        )
        .pipe(Effect.exit);
      expect(result._tag).toBe("Failure");
      expect(h.state.NetworkSettings.Networks.tck_default?.Aliases).toEqual([
        "peer-celld",
        "custom",
      ]);
    }),
);
it.effect(
  "attempts all restorations and retains scenario and cleanup failures",
  () =>
    Effect.gen(function* () {
      const h = harness();
      const run = (file: string, args: readonly string[]) =>
        h.run(file, args).pipe(
          Effect.flatMap((value) =>
            args[1] === "connect"
              ? Effect.fail(
                  new TckError({
                    phase: "process",
                    message: "reconnect failed",
                  }),
                )
              : Effect.succeed(value),
          ),
        );
      const result = yield* h
        .provide(
          Effect.scoped(
            Effect.gen(function* () {
              const controls = yield* h.controls;
              yield* controls.memoryLimit("celld", { bytes: 128 });
              yield* controls.partitionPeers("celld");
              return yield* Effect.fail(
                new TckError({
                  phase: "assertion",
                  message: "scenario failed",
                }),
              );
            }),
          ),
          run,
        )
        .pipe(Effect.exit);
      expect(Exit.isFailure(result)).toBe(true);
      if (Exit.isFailure(result)) {
        expect(Cause.pretty(result.cause)).toContain("scenario failed");
        expect(Cause.pretty(result.cause)).toContain("reconnect failed");
      }
      expect(h.state.HostConfig).toEqual({ Memory: 256, MemorySwap: 768 });
    }),
);
it.effect("rejects evidence for a different container before any fault", () =>
  Effect.gen(function* () {
    const h = harness();
    h.state.Id = "unowned";
    const result = yield* h
      .provide(
        Effect.scoped(
          Effect.gen(function* () {
            const controls = yield* h.controls;
            yield* controls.memoryLimit("celld", { bytes: 128 });
          }),
        ),
      )
      .pipe(Effect.exit);
    expect(result._tag).toBe("Failure");
    expect(h.calls.some((args) => args[0] === "update")).toBe(false);
  }),
);

it.effect("peer restoration does not depend on diagnostic writes", () =>
  Effect.gen(function* () {
    const h = harness();
    let failEvidence = false;
    const program = Effect.scoped(
      Effect.gen(function* () {
        const controls = yield* h.controls;
        yield* controls.partitionPeers("celld");
        failEvidence = true;
        yield* controls.resources("celld");
      }),
    ).pipe(
      Effect.provideService(Artifacts, {
        directory: "/unused",
        text: () =>
          Effect.suspend(() =>
            failEvidence
              ? Effect.fail(
                  new TckError({ phase: "artifact", message: "disk full" }),
                )
              : Effect.void,
          ),
        json: () => Effect.void,
      }),
    );
    const result = yield* h.provide(program).pipe(Effect.exit);
    expect(result._tag).toBe("Failure");
    expect(h.state.NetworkSettings.Networks.tck_default?.Aliases).toEqual([
      "peer-celld",
      "custom",
    ]);
  }),
);

it.effect(
  "unlimited baselines require a declared recovery budget before mutation",
  () =>
    Effect.gen(function* () {
      const h = harness();
      h.state.HostConfig = { Memory: 0, MemorySwap: 0 };
      const result = yield* h
        .provide(
          Effect.scoped(
            Effect.gen(function* () {
              const controls = yield* h.controls;
              yield* controls.memoryLimit("celld", { bytes: 128 });
            }),
          ),
        )
        .pipe(Effect.exit);
      expect(result._tag).toBe("Failure");
      expect(h.calls.some((args) => args[0] === "update")).toBe(false);
      const recovery = { Memory: 512, MemorySwap: 1024 };
      yield* h.provide(
        Effect.scoped(
          Effect.gen(function* () {
            const controls = yield* h.controls;
            expect(
              yield* controls.memoryLimit("celld", {
                bytes: 128,
                unlimitedRecovery: recovery,
              }),
            ).toEqual(recovery);
            expect(h.state.HostConfig).toEqual({
              Memory: 128,
              MemorySwap: 128,
            });
          }),
        ),
      );
      expect(h.state.HostConfig).toEqual(recovery);
      expect(
        h.calls
          .filter((args) => args[0] === "update")
          .every((args) => args[2] !== "0" && args[4] !== "0"),
      ).toBe(true);
    }),
);
