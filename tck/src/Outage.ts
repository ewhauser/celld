import { waitForReady } from "./Polling.js";
import { Effect, Schema } from "effect";
import { Artifacts } from "./Artifacts.js";
import {
  attemptRequest,
  leaseLapse,
  Transport,
  type Observation,
  type RuntimeHandle,
  type Target,
  type TckError,
} from "./Domain.js";
import { equal } from "./Oracle.js";

export const OutageState = Schema.Struct({
  kv: Schema.Array(Schema.Tuple([Schema.String, Schema.String])),
  sql: Schema.Array(Schema.Struct({ id: Schema.Int, value: Schema.String })),
});
export interface WriteOutcome {
  readonly id: number;
  readonly acknowledged: boolean;
  readonly observation?: unknown;
  readonly error?: string;
}
// A 2xx must be exactly the fixture's acknowledgment; any other response or a
// transport failure leaves the write unacknowledged and possibly committed.
export const classifyWrite = <R>(
  id: number,
  request: Effect.Effect<Observation, TckError, R>,
): Effect.Effect<WriteOutcome, TckError, R> =>
  Effect.gen(function* () {
    const result = yield* attemptRequest(request);
    if ("error" in result)
      return { id, acknowledged: false, error: result.error };
    const observation = result.response;
    if (observation.status >= 200 && observation.status < 300) {
      yield* equal(observation.status, 200);
      yield* equal(observation.body, { acknowledged: id });
      return { id, acknowledged: true, observation };
    }
    return { id, acknowledged: false, observation };
  });
export const checkOutageState = (
  state: typeof OutageState.Type,
  outcomes: readonly WriteOutcome[],
) =>
  Effect.gen(function* () {
    const expectedKv: Array<readonly [string, string]> = [];
    const expectedSql: Array<{ id: number; value: string }> = [];
    for (const outcome of outcomes) {
      const key = `op:${outcome.id}`;
      const present =
        state.kv.some(([name]) => name === key) ||
        state.sql.some((row) => row.id === outcome.id);
      if (outcome.acknowledged || present) {
        expectedKv.push([key, `value-${outcome.id}`]);
        expectedSql.push({ id: outcome.id, value: `value-${outcome.id}` });
      }
    }
    expectedKv.sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0));
    expectedSql.sort((a, b) => a.id - b.id);
    // Exact arrays reject partial transactions, duplicates, corrupt payloads, and unknown writes.
    yield* equal(state, { kv: expectedKv, sql: expectedSql });
  });

export const runOutage = (
  initial: Target,
  name: string,
  lifecycle: NonNullable<RuntimeHandle["lifecycle"]>,
) =>
  Effect.gen(function* () {
    const transport = yield* Transport;
    const artifacts = yield* Artifacts;
    let target = initial;
    const outcomes: WriteOutcome[] = [];
    const write = (id: number) =>
      Effect.gen(function* () {
        const outcome = yield* classifyWrite(
          id,
          transport.request(target, {
            path: `/outage/write?name=${name}&id=${id}`,
            method: "POST",
          }),
        );
        outcomes.push(outcome);
        yield* artifacts.json("outage-write-outcomes.json", outcomes);
        return outcome;
      });
    const read = () =>
      transport.request(target, { path: `/outage/state?name=${name}` }).pipe(
        Effect.tap((value) => equal(value.status, 200)),
        Effect.flatMap((value) =>
          Schema.decodeUnknownEffect(OutageState)(value.body),
        ),
      );
    const ready = () =>
      waitForReady(
        transport.request(target, { path: "/ready?name=outage-readiness" }),
        { interval: "500 millis", attempts: 91, timeout: "60 seconds" },
      ).pipe(
        Effect.flatMap((response) => equal(response.body, { ready: true })),
      );
    yield* equal((yield* write(1)).acknowledged, true);
    const before = yield* read();
    yield* checkOutageState(before, outcomes);
    yield* lifecycle.stopStorage();
    // Never retry writes: a missing response cannot establish that a write did not commit.
    for (const id of [2, 3, 4]) yield* write(id);
    yield* leaseLapse;
    // Act as the supervisor: self-fenced processes require restarting. Cold-read even if still running.
    yield* lifecycle.prepareRestart();
    yield* leaseLapse;
    yield* lifecycle.restoreStorage();
    target = yield* lifecycle.start();
    yield* ready();
    const recovered = yield* read();
    yield* artifacts.json("outage-recovered.json", {
      before,
      outcomes,
      recovered,
    });
    yield* checkOutageState(recovered, outcomes);
    yield* equal((yield* write(5)).acknowledged, true);
    yield* lifecycle.stop(true);
    yield* leaseLapse;
    target = yield* lifecycle.start();
    yield* ready();
    const final = yield* read();
    yield* artifacts.json("outage-final.json", { outcomes, final });
    yield* checkOutageState(final, outcomes);
    // The successful recovered read also establishes durable observations, even for uncertain writes.
    yield* equal(final, {
      kv: [...recovered.kv, ["op:5", "value-5"]],
      sql: [...recovered.sql, { id: 5, value: "value-5" }],
    });
    return { target, observations: { outcomes, recovered, final } };
  });
