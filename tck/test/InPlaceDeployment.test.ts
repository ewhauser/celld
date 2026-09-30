import { checkHistory } from "../src/History.js";
import { expect, it } from "@effect/vitest";
import { Effect } from "effect";
import { checkAdoption, checkReload } from "../src/InPlaceDeployment.js";

const process = {
  Id: "container-1",
  RestartCount: 0,
  State: { Running: true, Pid: 42, StartedAt: "2026-09-18T00:00:00Z" },
};
const observation = (revision: string) => ({
  before: process,
  after: structuredClone(process),
  worker: { revision },
  object: { revision },
  service: { revision },
});
const fails = (work: Effect.Effect<unknown, unknown>) =>
  work.pipe(
    Effect.exit,
    Effect.map((exit) => expect(exit._tag).toBe("Failure")),
  );
it.effect("reload-adoption rejects stale Workers, objects, and services", () =>
  Effect.gen(function* () {
    const good = observation("qualification-v2");
    yield* checkAdoption(good, "qualification-v2");
    for (const boundary of ["worker", "object", "service"])
      yield* fails(
        checkAdoption(
          { ...good, [boundary]: { revision: "qualification-v1" } },
          "qualification-v2",
        ),
      );
  }),
);
it.effect(
  "reload-adoption rejects a container replacement or process restart",
  () =>
    Effect.gen(function* () {
      const good = observation("qualification-v2");
      for (const after of [
        { ...process, Id: "replacement" },
        { ...process, RestartCount: 1 },
        { ...process, State: { ...process.State, Pid: 99 } },
        {
          ...process,
          State: { ...process.State, StartedAt: "2026-09-18T00:01:00Z" },
        },
      ])
        yield* fails(checkAdoption({ ...good, after }, "qualification-v2"));
    }),
);
it.effect(
  "reload-invalid rejects accidental adoption of the invalid revision",
  () =>
    Effect.gen(function* () {
      const good = observation("qualification-v1");
      yield* checkAdoption(good, "qualification-v1");
      for (const boundary of ["worker", "object", "service"])
        yield* fails(
          checkAdoption(
            { ...good, [boundary]: { revision: "qualification-v2" } },
            "qualification-v1",
          ),
        );
    }),
);
it.effect(
  "reload-invalid requires build rejection rather than success or an unrelated failure",
  () =>
    Effect.gen(function* () {
      yield* checkReload(
        {
          status: 422,
          body: JSON.stringify({
            ok: false,
            outcome: "failed",
            error: "Error: tck-invalid-deployment",
          }),
        },
        false,
      );
      const failed = JSON.stringify({
        ok: false,
        outcome: "failed",
        error: "tck-invalid-deployment",
      });
      yield* fails(checkReload({ status: 200, body: failed }, false));
      yield* fails(
        checkReload(
          {
            status: 422,
            body: JSON.stringify({
              ok: true,
              outcome: "adopted",
              error: "tck-invalid-deployment",
            }),
          },
          false,
        ),
      );
      yield* fails(
        checkReload(
          {
            status: 422,
            body: JSON.stringify({
              ok: false,
              outcome: "failed",
              error: "unrelated",
            }),
          },
          false,
        ),
      );
      yield* checkReload(
        { status: 200, body: JSON.stringify({ ok: true, outcome: "adopted" }) },
        true,
      );
      yield* fails(checkReload({ status: 200, body: failed }, true));
      yield* fails(
        checkReload(
          {
            status: 200,
            body: JSON.stringify({ ok: true, outcome: "unchanged" }),
          },
          true,
        ),
      );
    }),
);

it.effect(
  "reload-adoption and reload-invalid reject lost or corrupted acknowledged state",
  () =>
    Effect.gen(function* () {
      const history = [
        {
          kind: "intent" as const,
          id: "a",
          at: 1,
          payload: "stored",
          node: "celld",
        },
        {
          kind: "ack" as const,
          id: "a",
          at: 2,
          payload: "stored",
          node: "celld",
          seq: 1,
          activation: "old",
        },
      ];
      const rows = [{ id: "a", seq: 1, payload: "stored", kv: "stored" }];
      yield* checkHistory(history, rows);
      yield* fails(checkHistory(history, []));
      yield* fails(checkHistory(history, [{ ...rows[0]!, kv: "corrupt" }]));
    }),
);
