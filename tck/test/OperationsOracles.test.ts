import { expect, it } from "@effect/vitest";
import { Effect } from "effect";
import {
  checkAcceptedCompleted,
  checkAcknowledgedRetained,
  checkBoundedShutdown,
  checkDrainedAway,
  checkHandoffEvidence,
  checkMixedFleet,
  checkNoAcquisition,
  checkNoMoves,
  checkNodeReleases,
  checkOwnershipPreserved,
  checkPublishedWeights,
  checkReadinessOrder,
  checkSerializedDrains,
  checkWeightedPlacement,
  parseDrainLog,
  type OwnershipMap,
} from "../src/OperationsOracles.js";

const fails = <A, E>(effect: Effect.Effect<A, E>) =>
  Effect.map(Effect.exit(effect), (exit) => exit._tag === "Failure");

const WEIGHTS = { celld: 8, celld2: 1, celld3: 1 };

it.effect(
  "weighted placement rejects a distribution that ignores the node weights",
  () =>
    Effect.gen(function* () {
      // Observed on celld v0.5.0 with 24, 25 and 48 cells at weights 8/1/1.
      yield* checkWeightedPlacement(WEIGHTS, {
        celld: 20,
        celld2: 1,
        celld3: 3,
      });
      yield* checkWeightedPlacement(WEIGHTS, {
        celld: 21,
        celld2: 1,
        celld3: 3,
      });
      yield* checkWeightedPlacement(WEIGHTS, {
        celld: 39,
        celld2: 5,
        celld3: 4,
      });
      for (const owned of [
        { celld: 8, celld2: 8, celld3: 8 }, // an even split
        { celld: 24, celld2: 0, celld3: 0 }, // never balanced at all
        { celld: 0, celld2: 12, celld3: 12 }, // weighted the wrong way round
        { celld: 18, celld2: 5, celld3: 1 }, // one light node two whole cells over
        { celld: 22, celld2: 1, celld3: 2 }, // the heavy node two whole cells over
        { celld: 0, celld2: 0, celld3: 0 }, // an empty fleet is not evidence
      ])
        expect(yield* fails(checkWeightedPlacement(WEIGHTS, owned))).toBe(true);
      // A single node cannot demonstrate a weighting, and neither can a
      // missing or nonsensical weight.
      expect(yield* fails(checkWeightedPlacement(WEIGHTS, { celld: 4 }))).toBe(
        true,
      );
      expect(
        yield* fails(
          checkWeightedPlacement(
            { celld: 8, celld2: 0, celld3: 1 },
            { celld: 20, celld2: 1, celld3: 3 },
          ),
        ),
      ).toBe(true);
    }),
);

it.effect("published placement weights must match the configured ones", () =>
  Effect.gen(function* () {
    yield* checkPublishedWeights(WEIGHTS, { celld: 8, celld2: 1, celld3: 1 });
    for (const published of [
      { celld: 4, celld2: 4, celld3: 4 }, // the CPU-count default, not ours
      { celld: 8, celld2: 1 }, // a node that published nothing
      { celld: 8, celld2: 1, celld3: 2 },
    ])
      expect(yield* fails(checkPublishedWeights(WEIGHTS, published))).toBe(
        true,
      );
    // Identical weights cannot separate weighted placement from even placement.
    expect(
      yield* fails(
        checkPublishedWeights({ celld: 4, celld2: 4 }, { celld: 4, celld2: 4 }),
      ),
    ).toBe(true);
  }),
);

const owners = (entries: Record<string, [string, number]>): OwnershipMap =>
  Object.fromEntries(
    Object.entries(entries).map(([cell, [node, epoch]]) => [
      cell,
      { node, epoch },
    ]),
  );

it.effect("a paused fleet may not move or drop an ownership record", () =>
  Effect.gen(function* () {
    const before = owners({ a: ["celld", 2], b: ["celld2", 1] });
    yield* checkNoMoves("paused", before, before);
    for (const after of [
      owners({ a: ["celld2", 3], b: ["celld2", 1] }), // moved to a peer
      owners({ a: ["celld", 3], b: ["celld2", 1] }), // reacquired at a new epoch
      owners({ a: ["", 2], b: ["celld2", 1] }), // released
      owners({ b: ["celld2", 1] }), // record disappeared
    ])
      expect(yield* fails(checkNoMoves("paused", before, after))).toBe(true);
    expect(yield* fails(checkNoMoves("paused", {}, {}))).toBe(true);
  }),
);

it.effect("a drained node may not keep any of the cells it held", () =>
  Effect.gen(function* () {
    const before = owners({
      a: ["celld2", 1],
      b: ["celld2", 1],
      c: ["celld", 1],
    });
    yield* checkDrainedAway(
      "celld2",
      before,
      owners({ a: ["celld", 2], b: ["celld3", 2], c: ["celld", 1] }),
    );
    // One cell that stayed on the draining node fails the whole drain.
    expect(
      yield* fails(
        checkDrainedAway(
          "celld2",
          before,
          owners({ a: ["celld", 2], b: ["celld2", 1], c: ["celld", 1] }),
        ),
      ),
    ).toBe(true);
    // A node that owned nothing produces no evidence about a handoff.
    expect(yield* fails(checkDrainedAway("celld3", before, before))).toBe(true);
  }),
);

it.effect("a node without a capacity sample may not acquire a cell", () =>
  Effect.gen(function* () {
    const before = owners({ a: ["celld3", 1], b: ["celld", 1] });
    yield* checkNoAcquisition(
      "celld3",
      before,
      owners({ a: ["celld3", 1], b: ["celld2", 2] }),
    );
    expect(
      yield* fails(
        checkNoAcquisition(
          "celld3",
          before,
          owners({ a: ["celld3", 1], b: ["celld3", 2] }),
        ),
      ),
    ).toBe(true);
    expect(yield* fails(checkNoAcquisition("celld3", before, {}))).toBe(true);
  }),
);

it.effect("a same-node preserve keeps every record on the same node", () =>
  Effect.gen(function* () {
    const before = owners({
      a: ["celld3", 2],
      b: ["celld3", 2],
      c: ["celld", 1],
    });
    yield* checkOwnershipPreserved("celld3", before, before);
    for (const after of [
      owners({ a: ["celld", 3], b: ["celld3", 2], c: ["celld", 1] }), // handed off
      owners({ a: ["", 2], b: ["celld3", 2], c: ["celld", 1] }), // released
      owners({ b: ["celld3", 2], c: ["celld", 1] }), // record gone
    ])
      expect(
        yield* fails(checkOwnershipPreserved("celld3", before, after)),
      ).toBe(true);
    expect(
      yield* fails(checkOwnershipPreserved("celld2", before, before)),
    ).toBe(true);
  }),
);

it.effect("readiness must fall once during a drain and never rise again", () =>
  Effect.gen(function* () {
    yield* checkReadinessOrder("celld2", [
      { atMs: 1000, healthy: true },
      { atMs: 1250, healthy: true },
      { atMs: 1500, healthy: false },
      { atMs: 1750, healthy: false },
    ]);
    for (const samples of [
      // A flip in the wrong order: healthy again after the drain started.
      [
        { atMs: 1000, healthy: true },
        { atMs: 1250, healthy: false },
        { atMs: 1500, healthy: true },
      ],
      // Never became unhealthy, so no transition was observed.
      [
        { atMs: 1000, healthy: true },
        { atMs: 1250, healthy: true },
      ],
      // Already unhealthy, so the drain is not what removed readiness.
      [
        { atMs: 1000, healthy: false },
        { atMs: 1250, healthy: false },
      ],
      // Out of time order, so the sequence means nothing.
      [
        { atMs: 1500, healthy: true },
        { atMs: 1000, healthy: false },
      ],
      [{ atMs: 1000, healthy: true }],
    ])
      expect(yield* fails(checkReadinessOrder("celld2", samples))).toBe(true);
  }),
);

it.effect(
  "an accepted request must complete across the drain, not be lost",
  () =>
    Effect.gen(function* () {
      const spanning = {
        acceptedAtMs: 1000,
        drainStartedAtMs: 2000,
        completedAtMs: 7000,
        expectedId: "cell0-2",
      };
      yield* checkAcceptedCompleted({
        ...spanning,
        status: 200,
        receiptId: "cell0-2",
      });
      for (const observed of [
        // Accepted and then lost.
        { ...spanning, error: "socket hang up" },
        // Refused instead of finished.
        { ...spanning, status: 503 },
        // Someone else's receipt.
        { ...spanning, status: 200, receiptId: "cell0-1" },
        // No receipt at all.
        { ...spanning, status: 200 },
        // Did not span the drain: it finished before the drain started.
        {
          ...spanning,
          completedAtMs: 1500,
          status: 200,
          receiptId: "cell0-2",
        },
        // Was not accepted before the drain started.
        {
          ...spanning,
          acceptedAtMs: 2500,
          status: 200,
          receiptId: "cell0-2",
        },
      ])
        expect(yield* fails(checkAcceptedCompleted(observed))).toBe(true);
    }),
);

it.effect("every acknowledged write must survive the operation", () =>
  Effect.gen(function* () {
    yield* checkAcknowledgedRetained("drain", ["a", "b"], ["a", "b", "c"]);
    // A lost acknowledged write fails even when most of the history is there.
    expect(
      yield* fails(checkAcknowledgedRetained("drain", ["a", "b"], ["a"])),
    ).toBe(true);
    expect(yield* fails(checkAcknowledgedRetained("drain", [], ["a"]))).toBe(
      true,
    );
  }),
);

// Verbatim shapes from a celld v0.5.0 drain, with and without the second
// timestamp `docker compose logs --timestamps` prepends.
const DRAIN_LOG = [
  'celld2-1  | 2026-09-19T01:40:31.929663Z  INFO celld: captured the pre-drain restore baseline event="drain_restoration_baseline" nodes=2 maximum=0',
  'celld2-1  | 2026-09-19T01:40:31.933613Z  INFO celld::drain_token: acquired the fleet drain token event="drain_token_acquired" waited_ms=3 expires_ms=1789782151929',
  'celld2-1  | 2026-09-19T01:40:31.956920Z 2026-09-19T01:40:31.956920Z  INFO celld::actor: successor acquired a released cell event="cell_handoff_accepted" cell=Recovery:dccf2aed released_epoch=1 successor=celld successor_epoch=2 attempts=1 elapsed_ms=13',
  'celld2-1  | 2026-09-19T01:40:31.992166Z  INFO celld::drain_token: released the fleet drain token event="drain_token_released"',
].join("\n");

it("parses a drain out of celld's own log", () => {
  const evidence = parseDrainLog(DRAIN_LOG);
  expect(evidence.tokenWaitedMs).toBe(3);
  expect(evidence.tokenAcquiredMs).toBe(Date.parse("2026-09-19T01:40:31.933Z"));
  expect(evidence.tokenReleasedMs).toBe(Date.parse("2026-09-19T01:40:31.992Z"));
  expect(evidence.handoffs).toEqual([
    {
      cell: "Recovery:dccf2aed",
      successor: "celld",
      successorEpoch: 2,
    },
  ]);
  expect(evidence.cleanReload).toBe("none");
  expect(parseDrainLog("").handoffs).toEqual([]);
  expect(
    parseDrainLog(
      'x | 2026-09-19T01:41:49.215400Z  WARN celld: local reload preparation failed; replacement will use normal recovery event="clean_reload_abandoned" error=mismatch',
    ).cleanReload,
  ).toBe("abandoned");
  expect(
    parseDrainLog(
      'x | 2026-09-19T01:43:15.771305Z  INFO celld: prepared local cells event="clean_reload_prepared" stale_live_databases_pruned=0',
    ).cleanReload,
  ).toBe("prepared");
});

it.effect("a drain must log its own handoff for every cell it released", () =>
  Effect.gen(function* () {
    const evidence = parseDrainLog(DRAIN_LOG);
    yield* checkHandoffEvidence("celld2", evidence, ["Recovery:dccf2aed"]);
    // A cell with no logged handoff, and a node that recorded itself as the
    // successor, are both refused.
    expect(
      yield* fails(
        checkHandoffEvidence("celld2", evidence, ["Recovery:other"]),
      ),
    ).toBe(true);
    expect(
      yield* fails(
        checkHandoffEvidence(
          "celld",
          { handoffs: evidence.handoffs, cleanReload: "none" },
          ["Recovery:dccf2aed"],
        ),
      ),
    ).toBe(true);
    expect(yield* fails(checkHandoffEvidence("celld2", evidence, []))).toBe(
      true,
    );
  }),
);

it.effect("concurrent donors must hold the drain token one at a time", () =>
  Effect.gen(function* () {
    yield* checkSerializedDrains([
      {
        node: "celld2",
        evidence: {
          tokenAcquiredMs: 1000,
          tokenReleasedMs: 1500,
          handoffs: [],
          cleanReload: "none",
        },
      },
      {
        node: "celld3",
        evidence: {
          tokenAcquiredMs: 1500,
          tokenReleasedMs: 2000,
          handoffs: [],
          cleanReload: "none",
        },
      },
    ]);
    // Overlapping holds are exactly what the token is supposed to prevent.
    expect(
      yield* fails(
        checkSerializedDrains([
          {
            node: "celld2",
            evidence: {
              tokenAcquiredMs: 1000,
              tokenReleasedMs: 1800,
              handoffs: [],
              cleanReload: "none",
            },
          },
          {
            node: "celld3",
            evidence: {
              tokenAcquiredMs: 1500,
              tokenReleasedMs: 2000,
              handoffs: [],
              cleanReload: "none",
            },
          },
        ]),
      ),
    ).toBe(true);
    // A donor that logged no complete hold is not evidence of serialization.
    expect(
      yield* fails(
        checkSerializedDrains([
          {
            node: "celld2",
            evidence: { handoffs: [], cleanReload: "none" },
          },
          {
            node: "celld3",
            evidence: {
              tokenAcquiredMs: 1500,
              tokenReleasedMs: 2000,
              handoffs: [],
              cleanReload: "none",
            },
          },
        ]),
      ),
    ).toBe(true);
    expect(
      yield* fails(
        checkSerializedDrains([
          {
            node: "celld2",
            evidence: {
              tokenAcquiredMs: 2000,
              tokenReleasedMs: 1000,
              handoffs: [],
              cleanReload: "none",
            },
          },
          {
            node: "celld3",
            evidence: {
              tokenAcquiredMs: 3000,
              tokenReleasedMs: 4000,
              handoffs: [],
              cleanReload: "none",
            },
          },
        ]),
      ),
    ).toBe(true);
  }),
);

it.effect("a bounded shutdown must stay inside its configured bound", () =>
  Effect.gen(function* () {
    yield* checkBoundedShutdown("celld2", 4200, 20000);
    expect(yield* fails(checkBoundedShutdown("celld2", 20001, 20000))).toBe(
      true,
    );
    expect(yield* fails(checkBoundedShutdown("celld2", -1, 20000))).toBe(true);
    expect(yield* fails(checkBoundedShutdown("celld2", 100, 0))).toBe(true);
    expect(
      yield* fails(checkBoundedShutdown("celld2", Number.NaN, 20000)),
    ).toBe(true);
  }),
);

it.effect("binary-upgrade evidence must show the releases it claims", () =>
  Effect.gen(function* () {
    yield* checkNodeReleases(
      { celld: "0.6.0", celld2: "0.6.0", celld3: "0.6.0" },
      { celld: "0.6.0", celld2: "0.6.0", celld3: "0.6.0" },
    );
    expect(
      yield* fails(
        checkNodeReleases(
          { celld: "0.6.0", celld2: "0.6.0", celld3: "0.6.0" },
          { celld: "0.6.0", celld2: "0.5.1", celld3: "0.6.0" },
        ),
      ),
    ).toBe(true);
    yield* checkMixedFleet({
      celld: "0.5.1",
      celld2: "0.6.0",
      celld3: "0.5.1",
    });
    // A uniform fleet is never evidence of mixed-version behaviour.
    expect(
      yield* fails(
        checkMixedFleet({ celld: "0.6.0", celld2: "0.6.0", celld3: "0.6.0" }),
      ),
    ).toBe(true);
  }),
);
