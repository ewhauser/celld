import { expect, it } from "@effect/vitest";
import { Effect } from "effect";
import { checkReplicaLoss } from "../src/Resilience.js";

it.effect(
  "lost replicas require a matching loss declaration and atomic surviving data",
  () =>
    Effect.gen(function* () {
      const history = [
        { id: 1, acknowledged: true },
        { id: 2, acknowledged: true },
      ];
      const retained = {
        kv: [["op:1", "value-1"]] as const,
        sql: [{ id: 1, value: "value-1" }],
      };
      const loss = { leader: "celld/generation", epoch: 4, note: "no witness" };
      expect(
        yield* checkReplicaLoss(retained, history, loss.leader, 4, [loss]),
      ).toBe("declared-data-loss");
      for (const records of [
        [],
        [{ ...loss, epoch: 3 }],
        [{ ...loss, leader: "celld/old-generation" }],
      ])
        expect(
          (yield* Effect.exit(
            checkReplicaLoss(retained, history, loss.leader, 4, records),
          ))._tag,
        ).toBe("Failure");
      for (const corrupt of [
        { ...retained, sql: [] },
        { ...retained, sql: [{ id: 1, value: "corrupt" }] },
        { ...retained, kv: [...retained.kv, ...retained.kv] },
        {
          kv: [["op:3", "value-3"]] as const,
          sql: [{ id: 3, value: "value-3" }],
        },
      ])
        expect(
          (yield* Effect.exit(
            checkReplicaLoss(corrupt, history, loss.leader, 4, [loss]),
          ))._tag,
        ).toBe("Failure");
      expect(
        yield* checkReplicaLoss(retained, [history[0]!], loss.leader, 4, []),
      ).toBe("fully-recovered");
    }),
);
