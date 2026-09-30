import { expect, it } from "@effect/vitest";
import { Effect } from "effect";
import {
  checkForcedAdoption,
  checkLifecycleAdoption,
  checkModuleMismatch,
  checkSocketFailover,
  checkSocketPreservation,
  checkWorkflowResult,
  hasStorageFaultEvidence,
} from "../src/QualificationOracles.js";

const fails = (work: Effect.Effect<unknown, unknown>) =>
  work.pipe(
    Effect.exit,
    Effect.map((exit) => expect(exit._tag).toBe("Failure")),
  );
const exchange = (revision: string, counter: number, activation: string) => ({
  counter,
  activation,
  revision,
  message: "probe",
});
it.effect(
  "socket failover requires a forced close on every held socket and a fresh activation",
  () =>
    Effect.gen(function* () {
      const socket = {
        received: 1,
        receivedAfterKill: 0,
        closed: true,
        clean: false,
        code: 1006,
      };
      const valid = {
        sockets: [socket, socket, socket],
        beforeActivation: "before",
        afterActivation: "after",
        reconnectCounter: 1,
      };
      yield* checkSocketFailover(valid);
      for (const invalid of [
        // No socket was actually held across the ownership change.
        { ...valid, sockets: [] },
        // A socket that never served a frame proves nothing was connected.
        { ...valid, sockets: [{ ...socket, received: 0 }] },
        // The transport survived the loss of its owner.
        { ...valid, sockets: [{ ...socket, closed: false }] },
        // A clean close would mean the owner handed the socket over.
        { ...valid, sockets: [{ ...socket, clean: true }] },
        // A frame after the kill would mean the killed owner still served.
        { ...valid, sockets: [{ ...socket, receivedAfterKill: 1 }] },
        { ...valid, sockets: [{ ...socket, code: 0 }] },
        // Reconnection landed on the killed activation, not a new one.
        { ...valid, afterActivation: "before" },
        // A resumed counter would mean the old socket's state leaked in.
        { ...valid, reconnectCounter: 2 },
      ])
        expect((yield* Effect.exit(checkSocketFailover(invalid)))._tag).toBe(
          "Failure",
        );
    }),
);
it.effect(
  "workflow recovery requires the persisted first step's result, not just completion",
  () =>
    Effect.gen(function* () {
      yield* checkWorkflowResult({
        status: "complete",
        output: { first: 1, last: 1 },
      });
      for (const output of [
        null,
        {},
        { first: 2, last: 1 },
        { first: 1, last: 2 },
      ])
        expect(
          (yield* Effect.exit(
            checkWorkflowResult({ status: "complete", output }),
          ))._tag,
        ).toBe("Failure");
    }),
);
it.effect(
  "storage fault evidence must affect this cell's data and prove an actual lost successful response",
  () =>
    Effect.sync(() => {
      const valid = {
        mode: "drop-response",
        method: "PUT",
        path: "/tck/cells/Recovery:cell/ltx/object",
        upstreamStatus: 200,
        dropped: true,
      };
      expect(hasStorageFaultEvidence([valid], "drop-response", "cell")).toBe(
        true,
      );
      for (const invalid of [
        { ...valid, path: "/tck/nodes/cell.json" },
        { ...valid, path: "/tck/cells/Recovery:other/ltx/object" },
        { ...valid, path: "/tck/cells/Recovery:cell-extra/ltx/object" },
        { ...valid, upstreamStatus: 503 },
        { ...valid, dropped: false },
        { ...valid, method: "GET" },
        { ...valid, mode: "normal" },
      ])
        expect(
          hasStorageFaultEvidence([invalid], "drop-response", "cell"),
        ).toBe(false);
    }),
);

// --- In-place deployment lifecycle ---------------------------------------

const lifecycle = {
  workerBefore: ["qualification-v1", "qualification-v1", "qualification-v1"],
  workerAfter: ["qualification-v2", "qualification-v2", "qualification-v2"],
  objectGenerationDuring: 1,
  deploymentGeneration: 2,
  objectAfter: "qualification-v2",
  inFlight: { revision: "qualification-v1", requestedMs: 12000, heldMs: 12004 },
  alarm: { count: 1, revision: "qualification-v1", pending: null },
  pendingSync: {
    revision: "qualification-v1",
    rejected: false,
    value: "pending",
  },
  acknowledgedDuringAdoption: 9,
};
it.effect(
  "adoption with in-flight work rejects a request served by the wrong revision",
  () =>
    Effect.gen(function* () {
      yield* checkLifecycleAdoption(lifecycle);
      // The held request started on the previous deployment; it must not be
      // attributed to the adopted one, nor may the object stay behind.
      yield* fails(
        checkLifecycleAdoption({
          ...lifecycle,
          inFlight: { ...lifecycle.inFlight, revision: "qualification-v2" },
        }),
      );
      yield* fails(
        checkLifecycleAdoption({
          ...lifecycle,
          objectAfter: "qualification-v1",
        }),
      );
      // A node that never left the previous deployment is not an adoption.
      yield* fails(
        checkLifecycleAdoption({
          ...lifecycle,
          workerAfter: [
            "qualification-v2",
            "qualification-v1",
            "qualification-v2",
          ],
        }),
      );
      // The object may not be reported as already moved while work is open.
      yield* fails(
        checkLifecycleAdoption({ ...lifecycle, objectGenerationDuring: 2 }),
      );
      // A request that returned early was never in flight across the adoption.
      yield* fails(
        checkLifecycleAdoption({
          ...lifecycle,
          inFlight: { ...lifecycle.inFlight, heldMs: 11999 },
        }),
      );
    }),
);
it.effect(
  "adoption with in-flight work rejects a dropped alarm or a lost durability barrier",
  () =>
    Effect.gen(function* () {
      for (const alarm of [
        { count: 0, revision: null, pending: null },
        { count: 2, revision: "qualification-v1", pending: null },
        { count: 1, revision: "qualification-v2", pending: null },
        { count: 1, revision: "qualification-v1", pending: 1789781605602 },
      ])
        yield* fails(checkLifecycleAdoption({ ...lifecycle, alarm }));
      for (const pendingSync of [
        { revision: "qualification-v1", rejected: true, value: "pending" },
        { revision: "qualification-v1", rejected: false, value: "before" },
        { revision: "qualification-v2", rejected: false, value: "pending" },
      ])
        yield* fails(checkLifecycleAdoption({ ...lifecycle, pendingSync }));
      // No acknowledged write crossed the adoption, so nothing was proven.
      yield* fails(
        checkLifecycleAdoption({ ...lifecycle, acknowledgedDuringAdoption: 0 }),
      );
    }),
);

const preservation = {
  before: exchange("qualification-v1", 1, "activation-a"),
  after: exchange("qualification-v2", 2, "activation-b"),
  closed: null,
  open: true,
  objectAfter: "qualification-v2",
  rowsBefore: 12,
  rowsAfter: 18,
};
it.effect(
  "safe-point preservation rejects a socket that was closed when it should have survived",
  () =>
    Effect.gen(function* () {
      yield* checkSocketPreservation(preservation);
      // A hibernatable socket closed by the move fails, including with the
      // forced-adoption close code, which belongs to regular sockets only.
      yield* fails(
        checkSocketPreservation({
          ...preservation,
          open: false,
          closed: { code: 1012, reason: "service restart", afterMs: 20500 },
        }),
      );
      yield* fails(
        checkSocketPreservation({
          ...preservation,
          closed: { code: 1001, reason: "going away", afterMs: 900 },
        }),
      );
      yield* fails(checkSocketPreservation({ ...preservation, open: false }));
    }),
);
it.effect(
  "safe-point preservation rejects lost attachment state or an unmoved object",
  () =>
    Effect.gen(function* () {
      // A reconnected socket would restart its counter instead of continuing.
      yield* fails(
        checkSocketPreservation({
          ...preservation,
          after: exchange("qualification-v2", 1, "activation-b"),
        }),
      );
      yield* fails(
        checkSocketPreservation({
          ...preservation,
          after: exchange("qualification-v1", 2, "activation-b"),
        }),
      );
      // The same instance across the move means no new code was loaded.
      yield* fails(
        checkSocketPreservation({
          ...preservation,
          after: exchange("qualification-v2", 2, "activation-a"),
        }),
      );
      yield* fails(
        checkSocketPreservation({
          ...preservation,
          objectAfter: "qualification-v1",
        }),
      );
      // Durable rows must survive the move and accept new writes after it.
      yield* fails(checkSocketPreservation({ ...preservation, rowsAfter: 12 }));
      yield* fails(
        checkSocketPreservation({
          ...preservation,
          rowsBefore: 0,
          rowsAfter: 6,
        }),
      );
    }),
);

const forced = {
  deadlineMs: 20000,
  regularBefore: exchange("qualification-v1", 1, "activation-a"),
  regularClose: { code: 1012, reason: "service restart", afterMs: 20813 },
  hibernatableBefore: exchange("qualification-v1", 1, "activation-a"),
  hibernatableAfter: exchange("qualification-v2", 2, "activation-b"),
  hibernatableClosed: null,
  reconnect: exchange("qualification-v2", 1, "activation-b"),
  workerAfter: ["qualification-v2", "qualification-v2", "qualification-v2"],
  objectAfter: "qualification-v2",
};
it.effect("forced adoption rejects a close with the wrong code or timing", () =>
  Effect.gen(function* () {
    yield* checkForcedAdoption(forced);
    // A normal or abnormal close is not the documented forced transition.
    for (const code of [1000, 1001, 1006, 1011, 1013])
      yield* fails(
        checkForcedAdoption({
          ...forced,
          regularClose: { ...forced.regularClose, code },
        }),
      );
    // Closing before the configured deadline is not a deadline-driven force.
    yield* fails(
      checkForcedAdoption({
        ...forced,
        regularClose: { ...forced.regularClose, afterMs: 19999 },
      }),
    );
    // An unbounded wait after the deadline is also a failure.
    yield* fails(
      checkForcedAdoption({
        ...forced,
        regularClose: { ...forced.regularClose, afterMs: 80000 },
      }),
    );
  }),
);
it.effect(
  "forced adoption rejects a collateral hibernatable close or a failed reconnection",
  () =>
    Effect.gen(function* () {
      // Forcing may close regular sockets only.
      yield* fails(
        checkForcedAdoption({
          ...forced,
          hibernatableClosed: {
            code: 1012,
            reason: "service restart",
            afterMs: 20813,
          },
        }),
      );
      yield* fails(
        checkForcedAdoption({
          ...forced,
          hibernatableAfter: exchange("qualification-v1", 2, "activation-b"),
        }),
      );
      // The reconnected client must reach the deployment that forced the close.
      yield* fails(
        checkForcedAdoption({
          ...forced,
          reconnect: exchange("qualification-v1", 1, "activation-b"),
        }),
      );
      yield* fails(
        checkForcedAdoption({ ...forced, objectAfter: "qualification-v1" }),
      );
    }),
);

const rejected = (error: string) =>
  JSON.stringify({ ok: false, outcome: "failed", error });
const mismatch = {
  module: "index.js",
  manifestDigest: "aa".repeat(32),
  bytesBefore: 669196,
  digestBefore: "aa".repeat(32),
  bytesAfter: 669196,
  digestAfter: "bb".repeat(32),
  reloads: (["celld", "celld2", "celld3"] as const).map((node) => ({
    node,
    status: 422,
    body: rejected(
      'deployment module digest mismatch for "index.js": expected aa, got bb',
    ),
  })),
  worker: ["qualification-v1", "qualification-v1", "qualification-v1"],
  object: ["qualification-v1", "qualification-v1", "qualification-v1"],
  service: ["qualification-v1", "qualification-v1", "qualification-v1"],
};
it.effect("modified module bytes must not be accepted by any node", () =>
  Effect.gen(function* () {
    yield* checkModuleMismatch(mismatch);
    // An adopted reload is the failure this case exists to catch.
    for (const accepted of [
      { status: 200, body: JSON.stringify({ ok: true, outcome: "adopted" }) },
      {
        status: 422,
        body: JSON.stringify({ ok: true, outcome: "adopted", error: "none" }),
      },
    ])
      yield* fails(
        checkModuleMismatch({
          ...mismatch,
          reloads: [
            { node: "celld", ...accepted },
            ...mismatch.reloads.slice(1),
          ],
        }),
      );
    // A node that adopted the tampered bytes shows up as a changed revision.
    for (const boundary of ["worker", "object", "service"] as const)
      yield* fails(
        checkModuleMismatch({
          ...mismatch,
          [boundary]: [
            "qualification-v1",
            "qualification-v2",
            "qualification-v1",
          ],
        }),
      );
    // Only two of three nodes rejected.
    yield* fails(
      checkModuleMismatch({ ...mismatch, reloads: mismatch.reloads.slice(1) }),
    );
  }),
);
it.effect(
  "module rejection must rest on the digest, not on a length change",
  () =>
    Effect.gen(function* () {
      // A length difference would be caught without any digest verification.
      yield* fails(
        checkModuleMismatch({
          ...mismatch,
          bytesAfter: 669212,
          reloads: mismatch.reloads.map((reload) => ({
            ...reload,
            body: rejected(
              'deployment module size mismatch for "index.js": expected 669196, got 669212',
            ),
          })),
        }),
      );
      // An unrelated rejection does not prove module verification.
      for (const error of [
        "tck-invalid-deployment",
        'missing module "index.js"',
        'deployment module digest mismatch for "other.wasm": expected aa, got bb',
      ])
        yield* fails(
          checkModuleMismatch({
            ...mismatch,
            reloads: mismatch.reloads.map((reload) => ({
              ...reload,
              body: rejected(error),
            })),
          }),
        );
      // The published bytes must have matched the manifest before tampering.
      yield* fails(
        checkModuleMismatch({ ...mismatch, manifestDigest: "cc".repeat(32) }),
      );
      // A no-op edit proves nothing.
      yield* fails(
        checkModuleMismatch({
          ...mismatch,
          digestAfter: mismatch.digestBefore,
        }),
      );
    }),
);
