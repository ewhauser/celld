import { expect, it } from "@effect/vitest";
import { Effect } from "effect";
import {
  checkHandoff,
  hasFleetProof,
  hasLogRecovery,
} from "../src/Multinode.js";
it.effect("failover requires the successor and a strictly newer epoch", () =>
  Effect.gen(function* () {
    const before = { node: "celld" as const, epoch: 3 };
    yield* checkHandoff(before, { node: "celld2", epoch: 4 }, "celld2");
    for (const after of [
      { node: "celld" as const, epoch: 4 },
      { node: "celld2" as const, epoch: 3 },
      { node: "celld2" as const, epoch: 2 },
    ])
      expect(
        (yield* Effect.exit(checkHandoff(before, after, "celld2")))._tag,
      ).toBe("Failure");
  }),
);

it.effect(
  "fleet evidence requires the tested cell and a nonempty recovery for the dead node",
  () =>
    Effect.sync(() => {
      expect(hasFleetProof('durable_wait cell=abc proof="fleet"', "abc")).toBe(
        true,
      );
      expect(hasFleetProof('durable_wait cell=abc proof="bucket"', "abc")).toBe(
        false,
      );
      expect(
        hasFleetProof('durable_wait cell=other proof="fleet"', "abc"),
      ).toBe(false);
      expect(
        hasFleetProof("log ensemble open; fleet acks enabled", "abc"),
      ).toBe(false);
      expect(
        hasLogRecovery(
          `node log recovered and sealed dead="celld/${"a".repeat(64)}" entries=3`,
          "celld",
        ),
      ).toBe(true);
      expect(
        hasLogRecovery(
          `node log recovered and sealed dead="celld2/${"a".repeat(64)}" entries=3`,
          "celld",
        ),
      ).toBe(false);
      expect(
        hasLogRecovery(
          'node log recovered and sealed dead="celld" entries=2',
          "celld",
        ),
      ).toBe(true);
      expect(
        hasLogRecovery(
          'node log recovered and sealed dead="celld2" entries=2',
          "celld",
        ),
      ).toBe(false);
      expect(
        hasLogRecovery(
          'node log recovered and sealed dead="celld" entries=0',
          "celld",
        ),
      ).toBe(false);
    }),
);

import { checkEnsemble } from "../src/Resilience.js";
it.effect(
  "three-node ensemble must contain both distinct remote followers",
  () =>
    Effect.gen(function* () {
      yield* checkEnsemble(
        "celld",
        ["celld", "celld2", "celld3"],
        ["celld3", "celld2"],
      );
      for (const invalid of [
        ["celld2"],
        ["celld", "celld2"],
        ["celld2", "celld2"],
      ] as const)
        expect(
          (yield* Effect.exit(
            checkEnsemble("celld", ["celld", "celld2", "celld3"], invalid),
          ))._tag,
        ).toBe("Failure");
    }),
);
