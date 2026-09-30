import { Effect } from "effect";
import { equal } from "./Oracle.js";
import { type QualificationContext, events } from "./QualificationContext.js";
import { checkWorkflowResult } from "./QualificationOracles.js";
import { runSocketOrStream } from "./QualificationStreams.js";
export const runQueue = (
  id:
    | "dependencies.queue-acceptance"
    | "dependencies.queue-redelivery"
    | "capacity.queue-load",
  ctx: QualificationContext,
) =>
  Effect.scoped(
    Effect.gen(function* () {
      const { fleet, nodes, artifacts } = ctx;

      const count = id === "capacity.queue-load" ? 500 : 4;
      const retry = id === "dependencies.queue-redelivery";
      const started = Date.now();
      for (let offset = 0; offset < count; offset += 100) {
        const batch = Math.min(100, count - offset);
        yield* equal(
          yield* ctx.json(
            `/queue/send?count=${batch}&offset=${offset}&delay=${retry || count > 4 ? 0 : 15}&retry=${retry ? 1 : 0}`,
            "celld",
            "POST",
          ),
          { accepted: batch },
        );
        if (count > 4) yield* Effect.sleep("1 second");
      }
      const acceptedAt = started;
      if (retry) {
        const firstDelivery = yield* ctx.poll(
          events(ctx),
          (items) => items.length === count,
        );
        yield* equal(
          firstDelivery.every(([, attempts]) => attempts === 1),
          true,
        );
      } else if (count === 4) yield* equal(yield* events(ctx), []);
      if (id !== "capacity.queue-load") {
        for (const node of nodes) yield* fleet.kill(node);
        yield* equal(Date.now() - acceptedAt < 15000, true);
        yield* artifacts.json(`${id}-outage.json`, {
          acceptedAt,
          stoppedAt: Date.now(),
          delaySeconds: 15,
        });
        yield* Effect.sleep("16 seconds");
        yield* ctx.startAll();
      }
      const observed = yield* ctx.poll(
        events(ctx),
        (items) =>
          items.length === count &&
          items.every(([, attempts]) => attempts >= (retry ? 2 : 1)),
      );
      yield* equal(
        observed.map(([key]) => key).sort(),
        Array.from({ length: count }, (_, i) => `event:queue:${i}`).sort(),
      );
      return { accepted: count, observed, elapsedMs: Date.now() - started };
    }),
  );
const runWorkflow = (ctx: QualificationContext) =>
  Effect.scoped(
    Effect.gen(function* () {
      yield* ctx.json("/flow/start", "celld", "POST");
      yield* ctx.poll(events(ctx), (items) =>
        items.some(([key]) => key === "event:flow-first"),
      );
      yield* ctx.poll(
        ctx.json("/flow/status"),
        (value) =>
          typeof value === "object" &&
          value !== null &&
          "status" in value &&
          value.status === "waiting",
      );
      yield* ctx.restartAll();
      yield* equal(yield* events(ctx), [["event:flow-first", 1]]);
      yield* ctx.json("/flow/continue", "celld", "POST");
      const status = yield* ctx.poll(
        ctx.json("/flow/status"),
        (value) =>
          typeof value === "object" &&
          value !== null &&
          "status" in value &&
          value.status === "complete",
      );
      yield* checkWorkflowResult(status);
      yield* equal(yield* events(ctx), [
        ["event:flow-first", 1],
        ["event:flow-last", 1],
      ]);
      return status;
    }),
  );
export const dependencyCases = [
  ...(
    ["dependencies.queue-acceptance", "dependencies.queue-redelivery"] as const
  ).map((id) => ({
    id,
    run: (ctx: QualificationContext) => runQueue(id, ctx),
  })),
  { id: "dependencies.workflow-recovery" as const, run: runWorkflow },
  ...(
    [
      "dependencies.stream-reconnect",
      "dependencies.hibernation",
      "dependencies.socket-failover",
    ] as const
  ).map((id) => ({
    id,
    run: (ctx: QualificationContext) => runSocketOrStream(id, ctx),
  })),
];
