import { Effect, Fiber } from "effect";
import { equal } from "./Oracle.js";
import {
  type QualificationContext,
  acknowledgedBatch,
  traffic,
} from "./QualificationContext.js";
import { checkFencedReceipts } from "./History.js";
import { auditLedger } from "./Audit.js";
import { leaseLapse } from "./Domain.js";
const runTraffic = (
  id: "traffic.crash-ledger" | "traffic.paused-owner" | "traffic.restart-races",
  ctx: QualificationContext,
) =>
  Effect.scoped(
    Effect.gen(function* () {
      const { fleet, nodes, artifacts } = ctx;

      yield* acknowledgedBatch(ctx, 24);
      if (id === "traffic.paused-owner") {
        const prior = yield* ctx.owner();
        const oldActivation = (yield* ctx.ledger.read())
          .filter((event) => event.kind === "ack")
          .at(-1)!.activation!;
        const successor = nodes.find((node) => node !== prior.node)!;
        yield* fleet.pause(prior.node);
        yield* leaseLapse;
        yield* ctx.writeAcknowledged(successor);
        const after = yield* ctx.owner();
        yield* equal(
          after.node !== prior.node && after.epoch > prior.epoch,
          true,
        );
        const takeoverAt = Date.now();
        yield* artifacts.json("takeover.json", {
          prior,
          after,
          oldActivation,
          takeoverAt,
        });
        const pending = yield* traffic(ctx, 96).pipe(Effect.forkScoped);
        yield* fleet.unpause(prior.node);
        yield* Fiber.join(pending);
        yield* checkFencedReceipts(
          yield* ctx.ledger.read(),
          oldActivation,
          takeoverAt,
        );
        const stopped = yield* fleet.inspect(prior.node);
        yield* equal(stopped.State.Running, false);
        yield* equal(stopped.State.ExitCode, 3);
        yield* ctx.start(prior.node);
      } else {
        for (let round = 0; round < 3; round++) {
          const owner = (yield* ctx.owner()).node;
          const fiber = yield* traffic(ctx, 96).pipe(Effect.forkScoped);
          yield* Effect.sleep(
            `${100 + ((ctx.seed + round * 137) % 600)} millis`,
          );
          yield* fleet.kill(owner);
          if (id === "traffic.restart-races") {
            yield* ctx.start(owner);
            yield* fleet.kill(owner);
          }
          yield* leaseLapse;
          yield* ctx.start(owner);
          yield* Fiber.join(fiber);
          for (const node of nodes) yield* ctx.verify(node);
        }
      }
      yield* acknowledgedBatch(ctx, 12);
      return yield* ctx.verify();
    }),
  );
export const trafficCases = (
  [
    "traffic.crash-ledger",
    "traffic.paused-owner",
    "traffic.restart-races",
  ] as const
).map((id) => ({
  id,
  run: (ctx: QualificationContext) =>
    Effect.gen(function* () {
      const result = yield* runTraffic(id, ctx);
      yield* auditLedger({
        ledger: ctx.ledger.path,
        endpoint: ctx.targets.celld.baseUrl,
        name: ctx.name,
      });
      return result;
    }),
}));
