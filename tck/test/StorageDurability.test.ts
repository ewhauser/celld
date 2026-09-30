import { expect, it } from "@effect/vitest";
import { Effect } from "effect";
import {
  abortMarker,
  checkAbortSync,
  checkBarrierPartition,
  classifyAbortSync,
  hasDurabilityProof,
  syncMarker,
  checkSyncedState,
  checkCursorOutput,
  checkSyncRejection,
  checkDeadlineRecovery,
  checkTransactionSync,
  checkCursorSync,
  checkDeadline,
  checkOutageRecovery,
} from "../src/StorageDurability.js";

const verify = (
  check: (value: unknown) => Effect.Effect<unknown, unknown>,
  good: unknown,
  bad: unknown[],
) =>
  Effect.gen(function* () {
    yield* check(good);
    for (const value of bad)
      expect((yield* Effect.exit(check(value)))._tag).toBe("Failure");
  });
it.effect("sync recovery rejects lost SQL or KV writes", () =>
  verify(
    (value) => checkSyncedState(value, true),
    { rows: [{ n: 1 }, { n: 2 }], value: "after" },
    [
      { rows: [{ n: 1 }], value: "after" },
      { rows: [{ n: 1 }, { n: 2 }], value: "before" },
    ],
  ),
);
it.effect("sync in a transaction must reject and rollback", () =>
  verify(checkTransactionSync, { rejected: true, rows: [{ n: 1 }] }, [
    { rejected: false, rows: [{ n: 1 }] },
    { rejected: true, rows: [{ n: 1 }, { n: 99 }] },
  ]),
);
it.effect(
  "an unfinished write cursor must reject sync without losing remaining rows",
  () =>
    verify(
      checkCursorSync,
      { first: { n: 2 }, rejected: true, remaining: [{ n: 3 }] },
      [
        { first: { n: 2 }, rejected: false, remaining: [{ n: 3 }] },
        { first: { n: 2 }, rejected: true, remaining: [] },
      ],
    ),
);
it.effect(
  "transaction and gate deadlines require a timeout, not an arbitrary handler failure",
  () =>
    verify(
      checkDeadline,
      { rejected: true, message: "transaction timed out" },
      [
        { rejected: false, message: "transaction timed out" },
        { rejected: true, message: "unknown route" },
      ],
    ),
);
it.effect(
  "post-timeout recovery rejects an uncommitted transaction write",
  () =>
    verify(
      (value) => checkSyncedState(value, false),
      { rows: [{ n: 1 }], value: "before" },
      [{ rows: [{ n: 1 }, { n: 99 }], value: "before" }],
    ),
);
it.effect(
  "outage recovery allows uncertain writes but never loss or corruption of acknowledged state",
  () =>
    verify(checkOutageRecovery, { rows: [{ n: 1 }], value: "after" }, [
      { rows: [{ n: 1 }, { n: 2 }], value: "after" },
      { rows: [], value: "before" },
      { rows: [{ n: 2 }], value: "after" },
      { rows: [{ n: 1 }, { n: 1 }], value: "after" },
      { rows: [{ n: 1 }], value: "corrupt" },
    ]),
);

it.effect("cursor response and outbound boundaries reject leaked output", () =>
  verify(
    checkCursorOutput,
    {
      status: 500,
      body: { rejected: true, message: "uncommitted write cursor" },
      witness: { count: 1 },
    },
    [
      {
        status: 200,
        body: { rejected: true, message: "uncommitted write cursor" },
        witness: { count: 1 },
      },
      {
        status: 500,
        body: { rejected: true, message: "uncommitted write cursor" },
        witness: { count: 2 },
      },
      {
        status: 500,
        body: { rejected: true, message: "unrelated failure" },
        witness: { count: 1 },
      },
    ],
  ),
);
it.effect(
  "sync outage requires rejection by sync itself, not an output-gate error",
  () =>
    verify(
      checkSyncRejection,
      { rejected: true, message: "tck-sync-rejected: durability timeout" },
      [
        { rejected: true, message: "output gate durability timeout" },
        { rejected: false, message: "tck-sync-rejected: timeout" },
      ],
    ),
);
it.effect(
  "deadlines require the configured wait and a new activation with rolled-back state",
  () => {
    const good = {
      error: { rejected: true, message: "transaction timed out" },
      before: "old",
      after: "new",
      elapsedMs: 30000,
      recovered: { rows: [{ n: 1 }], value: "before" },
    };
    return verify(checkDeadlineRecovery, good, [
      { ...good, after: "old" },
      { ...good, elapsedMs: 10 },
      { ...good, elapsedMs: 46000 },
      { ...good, recovered: { rows: [{ n: 1 }, { n: 99 }], value: "before" } },
    ]);
  },
);

it("classifies a post-abort barrier by its own error text, not by the reset", () => {
  const abort = { status: 500, body: { rejected: true, message: abortMarker } };
  expect(classifyAbortSync(abort)).toBe("abort-error");
  expect(
    classifyAbortSync({
      status: 500,
      body: { rejected: true, message: `${syncMarker} durability timeout` },
    }),
  ).toBe("sync-rejected");
  expect(
    classifyAbortSync({ status: 200, body: { pending: "unsettled" } }),
  ).toBe("sync-unsettled");
  expect(
    classifyAbortSync({ status: 200, body: { pending: "resolved" } }),
  ).toBe("sync-resolved");
  // The reset's own error wrapped in the barrier's marker is still the reset.
  expect(
    classifyAbortSync({
      status: 200,
      body: { pending: `${syncMarker} ${abortMarker}` },
    }),
  ).toBe("abort-error");
  expect(
    classifyAbortSync({
      status: 500,
      body: { rejected: true, message: `${syncMarker} ${abortMarker}` },
    }),
  ).toBe("abort-error");
  expect(classifyAbortSync({ status: 500, body: { message: "boom" } })).toBe(
    "unclassified",
  );
});
it.effect(
  "post-abort sync rejects a resolved barrier and a surviving activation",
  () => {
    const good = {
      armed: { status: 500, body: { rejected: true, message: abortMarker } },
      activation: { before: "old", after: "new" },
      recovered: { rows: [{ n: 1 }], value: "after" },
      resumed: { rows: [{ n: 1 }, { n: 2 }], value: "after" },
    };
    return verify(checkAbortSync, good, [
      // A barrier that resolved after the object was reset.
      { ...good, armed: { status: 200, body: { pending: "resolved" } } },
      // An outcome that names neither source proves nothing.
      {
        ...good,
        armed: { status: 500, body: { rejected: true, message: "boom" } },
      },
      // abort() must reset the object.
      { ...good, activation: { before: "old", after: "old" } },
      // The acknowledged seed row must survive the reset.
      { ...good, recovered: { rows: [], value: "after" } },
      // An uncommitted transaction row must not appear.
      { ...good, recovered: { rows: [{ n: 1 }, { n: 99 }], value: "after" } },
      // The new activation must accept barriers again.
      { ...good, resumed: { rows: [{ n: 1 }], value: "before" } },
    ]);
  },
);
it("durability proof requires a wait logged for this cell", () => {
  const cell = "a".repeat(64);
  expect(
    hasDurabilityProof(
      `timing: durability proof reached event="durable_wait" cell="Recovery:${cell}" proof="bucket"`,
      cell,
    ),
  ).toBe(true);
  expect(
    hasDurabilityProof(`event="durable_wait" cell="Recovery:b"`, cell),
  ).toBe(false);
  expect(
    hasDurabilityProof(`ensemble open cell="Recovery:${cell}"`, cell),
  ).toBe(false);
});
it.effect(
  "an interrupted barrier must not resolve without durability evidence or lose an acknowledged write",
  () => {
    const states = ["celld", "celld2", "celld3"].map((node) => ({
      node,
      rows: [{ n: 1 }],
      value: "barrier-3",
    }));
    const good = {
      partitionProof: true,
      inFlight: true,
      classification: "resolved" as const,
      rounds: [
        { round: 1, outcome: "resolved", elapsedMs: 4 },
        { round: 2, outcome: "resolved", elapsedMs: 6 },
        { round: 3, outcome: "resolved", elapsedMs: 5 },
      ],
      durableProof: true,
      falseFleetProof: false,
      states,
    };
    return verify(checkBarrierPartition, good, [
      // Resolved while every peer was unreachable, with no durability evidence.
      { ...good, durableProof: false },
      // A fleet-replication proof while no peer was reachable is a false claim.
      { ...good, falseFleetProof: true },
      // The barrier was never open across the partition.
      { ...good, inFlight: false },
      // The peers were still reachable.
      { ...good, partitionProof: false },
      // A write acknowledged by round 3 was rolled back to round 2.
      {
        ...good,
        states: states.map((state) => ({ ...state, value: "barrier-2" })),
      },
      // Nodes disagree about durable state after reconnection.
      {
        ...good,
        states: [states[0]!, { ...states[1]!, value: "barrier-2" }, states[2]!],
      },
      // A partial transaction left an extra committed row.
      {
        ...good,
        states: states.map((state) => ({
          ...state,
          rows: [{ n: 1 }, { n: 2 }],
        })),
      },
      // Only two of the three nodes were read.
      { ...good, states: states.slice(0, 2) },
      // An unclassified round outcome is not evidence of anything.
      {
        ...good,
        rounds: [...good.rounds, { round: 4, outcome: "boom", elapsedMs: 1 }],
      },
      // No barrier ran at all.
      { ...good, rounds: [] },
    ]);
  },
);
