import { Effect, Exit } from "effect";
import { equal } from "./Oracle.js";
import {
  type QualificationContext,
  acknowledgedBatch,
  traffic,
} from "./QualificationContext.js";
import type { Node } from "./FleetControls.js";
import { runQueue } from "./QualificationDependencies.js";
import { runSocketOrStream } from "./QualificationStreams.js";
import { leaseLapse } from "./Domain.js";
const largeRestore = (ctx: QualificationContext) =>
  Effect.scoped(
    Effect.gen(function* () {
      const { fleet } = ctx;

      const count = 256; // 16 MiB of application state, checked in bounded chunks.
      for (let i = 0; i < count; i++)
        yield* equal(yield* ctx.json(`/blob/write?id=${i}`, "celld", "POST"), {
          id: i,
          bytes: 65536,
        });
      const prior = (yield* ctx.owner()).node;
      yield* fleet.kill(prior);
      yield* leaseLapse;
      yield* fleet.discard(prior);
      yield* ctx.start(prior);
      const started = Date.now();
      yield* equal(yield* ctx.json(`/blob/check?count=${count}`, prior), {
        count,
        bytes: count * 65536,
      });
      return {
        bytes: count * 65536,
        restoreAndCheckMs: Date.now() - started,
      };
    }),
  );
const memoryPressure = (
  id: "capacity.memory-pressure" | "capacity.insufficient-spare",
  ctx: QualificationContext,
) =>
  Effect.gen(function* () {
    const { fleet, nodes, artifacts } = ctx;
    const constrained =
      id === "capacity.memory-pressure"
        ? nodes
        : nodes.filter((node) => node !== "celld");
    const recoveryLimits = new Map<
      Node,
      { Memory: number; MemorySwap: number }
    >();
    const result = yield* Effect.scoped(
      Effect.gen(function* () {
        yield* acknowledgedBatch(ctx, 24);

        for (const node of constrained)
          recoveryLimits.set(
            node,
            yield* fleet.memoryLimit(node, {
              bytes:
                (id === "capacity.insufficient-spare" ? 8 : 128) * 1024 * 1024,
              // Preserve the scenarios' existing 512 MiB recovery budget when
              // Compose started the nodes without a memory limit.
              unlimitedRecovery: {
                Memory: 512 * 1024 * 1024,
                MemorySwap: 512 * 1024 * 1024,
              },
            }),
          );
        const limits = yield* Effect.forEach(constrained, (node) =>
          Effect.gen(function* () {
            const inspected = yield* fleet.resources(node);
            yield* artifacts.json(`${id}-${node}-limits.json`, inspected);
            yield* equal(
              inspected.HostConfig.Memory,
              (id === "capacity.insufficient-spare" ? 8 : 128) * 1024 * 1024,
            );
            return inspected.HostConfig;
          }),
        );
        const pressures = yield* Effect.forEach(
          constrained,
          (node) => ctx.request("/pressure?mb=96", node).pipe(Effect.exit),
          { concurrency: "unbounded" },
        );
        yield* artifacts.json(`${id}-pressure.json`, pressures);
        const oom = yield* Effect.forEach(constrained, (node) =>
          Effect.gen(function* () {
            const inspected = yield* fleet.resources(node);
            yield* artifacts.json(
              `${id}-${node}-after-pressure.json`,
              inspected,
            );
            return inspected.State.OOMKilled;
          }),
        );
        let allocationCompleted = false;
        for (const pressure of pressures)
          if (Exit.isSuccess(pressure) && pressure.value.status === 200) {
            yield* equal(pressure.value.body, { mb: 96, checksum: 4560 });
            allocationCompleted = true;
          }
        const pressureStates = yield* Effect.forEach(
          constrained,
          fleet.inspect,
        );
        const terminatedUnderLimit = pressureStates.some(
          (state) => !state.State.Running && state.State.ExitCode === 137,
        );
        yield* equal(
          allocationCompleted || oom.some(Boolean) || terminatedUnderLimit,
          true,
        );
        if (id === "capacity.insufficient-spare") {
          yield* fleet.kill("celld");
          // Both replacement nodes are below the observed runtime footprint.
          yield* equal(
            (yield* Effect.forEach(constrained, (node) =>
              fleet.inspect(node),
            )).every((state) => !state.State.Running),
            true,
          );
        }
        yield* traffic(ctx, 24);
        for (const node of nodes) {
          const state = yield* fleet.inspect(node);
          yield* artifacts.json(`${id}-${node}.json`, state);
          if (state.State.Running) yield* fleet.kill(node);
        }
        // Limits are restored before recovery by the scoped finalizers below.
        return {
          limits,
          oom,
          allocationCompleted,
          terminatedUnderLimit,
          pressures: pressures.map((exit) => exit._tag),
          acknowledged: (yield* ctx.ledger.read()).filter(
            (event) => event.kind === "ack",
          ).length,
        };
      }),
    );
    yield* leaseLapse;
    yield* ctx.startAll();
    for (const node of constrained) {
      const restored = yield* fleet.resources(node);
      yield* artifacts.json(`${id}-${node}-restored-capacity.json`, restored);
      yield* equal(restored.HostConfig, recoveryLimits.get(node));
    }
    for (const node of nodes) {
      ctx.targets[node] = yield* fleet.target(node);
      yield* ctx.verify(node);
    }
    return result;
  });
export const capacityCases = [
  { id: "capacity.large-restore" as const, run: largeRestore },
  {
    id: "capacity.memory-pressure" as const,
    run: (ctx: QualificationContext) =>
      memoryPressure("capacity.memory-pressure", ctx),
  },
  {
    id: "capacity.slow-consumer" as const,
    run: (ctx: QualificationContext) =>
      runSocketOrStream("capacity.slow-consumer", ctx),
  },
  {
    id: "capacity.queue-load" as const,
    run: (ctx: QualificationContext) => runQueue("capacity.queue-load", ctx),
  },
  {
    id: "capacity.insufficient-spare" as const,
    run: (ctx: QualificationContext) =>
      memoryPressure("capacity.insufficient-spare", ctx),
  },
];
