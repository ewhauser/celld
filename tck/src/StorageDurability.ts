import { Effect, Fiber, Schema } from "effect";
import { equal } from "./Oracle.js";
import { decodeAs } from "./Artifacts.js";
import { TckError } from "./Domain.js";
import type { Node } from "./FleetControls.js";
import { hasFleetProof } from "./Multinode.js";
import {
  acknowledgedBatch,
  type QualificationContext,
} from "./QualificationContext.js";
import {
  hasStorageFaultEvidence,
  StorageEvent,
} from "./QualificationOracles.js";

/** Attributes a failure to the object reset rather than to the barrier. */
export const abortMarker = "tck-object-abort";
/** Attributes a rejection to the explicit sync() barrier. */
export const syncMarker = "tck-sync-rejected:";

export const checkSyncedState = (value: unknown, after: boolean) =>
  equal(value, {
    rows: after ? [{ n: 1 }, { n: 2 }] : [{ n: 1 }],
    value: after ? "after" : "before",
  });
export const checkTransactionSync = (value: unknown) =>
  equal(value, { rejected: true, rows: [{ n: 1 }] });
export const checkCursorSync = (value: unknown) =>
  equal(value, { first: { n: 2 }, rejected: true, remaining: [{ n: 3 }] });
export const checkDeadline = (value: unknown) =>
  Effect.gen(function* () {
    const result = yield* decodeAs(
      Schema.Struct({ rejected: Schema.Literal(true), message: Schema.String }),
      "assertion",
    )(value);
    yield* equal(
      /(time.?out|timed out|time limit|too long|30.?second)/i.test(
        result.message,
      ),
      true,
    );
  });
export const checkSyncRejection = (value: unknown) =>
  Effect.gen(function* () {
    const error = yield* decodeAs(
      Schema.Struct({ rejected: Schema.Literal(true), message: Schema.String }),
      "assertion",
    )(value);
    yield* equal(error.message.includes(syncMarker), true);
  });
export const checkDeadlineRecovery = (value: unknown) =>
  Effect.gen(function* () {
    const result = yield* decodeAs(
      Schema.Struct({
        error: Schema.Unknown,
        before: Schema.String,
        after: Schema.String,
        elapsedMs: Schema.Number,
        recovered: Schema.Unknown,
      }),
      "assertion",
    )(value);
    yield* checkDeadline(result.error);
    yield* equal(result.before !== result.after, true);
    yield* equal(result.elapsedMs >= 25000 && result.elapsedMs < 45000, true);
    yield* checkSyncedState(result.recovered, false);
  });
const request = (
  ctx: QualificationContext,
  path: string,
  options: { node?: Node; timeoutMs?: number } = {},
) =>
  ctx.transport.request(ctx.targets[options.node ?? "celld"], {
    path: `/durability/${path}${path.includes("?") ? "&" : "?"}name=${ctx.name}`,
    timeoutMs: options.timeoutMs ?? 50_000,
  });
const json = (ctx: QualificationContext, path: string) =>
  request(ctx, path).pipe(
    Effect.tap((result) => equal(result.status, 200)),
    Effect.map((result) => result.body),
  );
const seed = (ctx: QualificationContext) =>
  json(ctx, "seed").pipe(
    Effect.flatMap((value) => checkSyncedState(value, false)),
  );
const recovery = (ctx: QualificationContext) =>
  Effect.gen(function* () {
    yield* seed(ctx);
    const acknowledgment = yield* json(ctx, "sync");
    yield* checkSyncedState(acknowledgment, true);
    yield* ctx.artifacts.json("sync-acknowledgment.json", acknowledgment);
    yield* ctx.restartAll();
    const recovered = yield* json(ctx, "state");
    yield* checkSyncedState(recovered, true);
    return { acknowledgment, recovered };
  });
const outage = (ctx: QualificationContext) =>
  Effect.scoped(
    Effect.gen(function* () {
      yield* seed(ctx);
      yield* Effect.addFinalizer(() =>
        ctx.mode("normal", 0).pipe(Effect.orDie),
      );
      const injection = yield* ctx.transport.request(ctx.proxy, {
        path: `/mode?mode=throttle&ms=14000&pathContains=${encodeURIComponent(ctx.identity.cell)}`,
      });
      yield* equal(injection.status, 200);
      const result = yield* request(ctx, "sync-unconfirmed");
      yield* equal(result.status, 500);
      yield* checkSyncRejection(result.body);
      const stats = yield* ctx.transport.request(ctx.proxy, { path: "/stats" });
      const evidence = yield* decodeAs(
        Schema.Struct({ events: Schema.Array(StorageEvent) }),
        "assertion",
      )(stats.body);
      yield* equal(
        hasStorageFaultEvidence(evidence.events, "throttle", ctx.identity.cell),
        true,
      );
      yield* ctx.artifacts.json("sync-outage.json", {
        result,
        events: evidence.events,
      });
      yield* ctx.mode("normal", 0);
      yield* ctx.restartAll();
      // An unacknowledged write may survive. The pre-fault acknowledged value must not disappear.
      const recovered = yield* json(ctx, "state");
      yield* checkOutageRecovery(recovered);
      return { result, recovered };
    }),
  );
export const checkOutageRecovery = (value: unknown) =>
  Effect.gen(function* () {
    const state = yield* decodeAs(
      Schema.Struct({
        rows: Schema.Array(Schema.Struct({ n: Schema.Int })),
        value: Schema.String,
      }),
      "assertion",
    )(value);
    yield* equal(state.rows, [{ n: 1 }]);
    yield* equal(["before", "after"].includes(state.value), true);
  });
const deadline = (
  path: "transaction-deadline" | "gate-deadline",
  ctx: QualificationContext,
) =>
  Effect.gen(function* () {
    yield* seed(ctx);
    const before = yield* ctx.json("/fleet/id");
    const started = Date.now();
    const result = yield* request(ctx, path);
    const elapsedMs = Date.now() - started;
    yield* equal(result.status, 500);
    yield* checkDeadline(result.body);
    const after = yield* ctx.json("/fleet/id");
    const Activation = Schema.Struct({ activation: Schema.String });
    const old = yield* decodeAs(Activation, "assertion")(before);
    const next = yield* decodeAs(Activation, "assertion")(after);
    yield* equal(old.activation !== next.activation, true);
    const recovered = yield* json(ctx, "state");
    yield* checkSyncedState(recovered, false);
    yield* checkDeadlineRecovery({
      error: result.body,
      before: old.activation,
      after: next.activation,
      elapsedMs,
      recovered,
    });
    return { before, result, after, recovered, elapsedMs };
  });
export const checkCursorOutput = (value: unknown) =>
  Effect.gen(function* () {
    const result = yield* decodeAs(
      Schema.Struct({
        status: Schema.Literal(500),
        body: Schema.Struct({
          rejected: Schema.Literal(true),
          message: Schema.String,
        }),
        witness: Schema.Struct({ count: Schema.Literal(1) }),
      }),
      "assertion",
    )(value);
    yield* equal(
      /(cursor|uncommitted|RETURNING)/i.test(result.body.message),
      true,
    );
  });
const cursorOutput = (
  boundary: "response" | "outbound",
  ctx: QualificationContext,
) =>
  Effect.gen(function* () {
    yield* seed(ctx);
    yield* equal(yield* json(ctx, "consumed-cursor-outbound"), { status: 200 });
    const control = yield* ctx.transport.request(ctx.targets.celld, {
      path: `/durability/witness-read?name=${ctx.name}-witness`,
    });
    yield* equal(control.status, 200);
    yield* equal(control.body, { count: 1 });
    const result = yield* request(ctx, `write-cursor-${boundary}`);
    const witness = yield* ctx.transport.request(ctx.targets.celld, {
      path: `/durability/witness-read?name=${ctx.name}-witness`,
    });
    yield* equal(witness.status, 200);
    const observation = { ...result, witness: witness.body };
    yield* checkCursorOutput(observation);
    return observation;
  });

export type AbortSyncClassification =
  | "abort-error"
  | "sync-rejected"
  | "sync-unsettled"
  | "sync-resolved"
  | "unclassified";
/**
 * Separates the outcomes a barrier armed before `ctx.abort()` can have. A
 * rejection whose text carries the abort marker is the reset's own error, not
 * an independent barrier rejection, so it never counts as `sync-rejected`.
 */
export const classifyAbortSync = (result: {
  readonly status: number;
  readonly body: unknown;
}): AbortSyncClassification => {
  const body =
    typeof result.body === "object" && result.body !== null
      ? (result.body as Record<string, unknown>)
      : undefined;
  const pending = typeof body?.pending === "string" ? body.pending : undefined;
  const message = typeof body?.message === "string" ? body.message : undefined;
  if (result.status === 200 && pending !== undefined) {
    if (pending === "resolved") return "sync-resolved";
    if (pending === "unsettled") return "sync-unsettled";
    if (!pending.startsWith(syncMarker)) return "unclassified";
    return pending.includes(abortMarker) ? "abort-error" : "sync-rejected";
  }
  if (result.status === 500 && message !== undefined) {
    if (message.includes(abortMarker)) return "abort-error";
    if (message.includes(syncMarker)) return "sync-rejected";
  }
  return "unclassified";
};
export const checkAbortSync = (value: unknown) =>
  Effect.gen(function* () {
    const observation = yield* decodeAs(
      Schema.Struct({
        armed: Schema.Struct({ status: Schema.Int, body: Schema.Unknown }),
        activation: Schema.Struct({
          before: Schema.String,
          after: Schema.String,
        }),
        recovered: Schema.Unknown,
        resumed: Schema.Unknown,
      }),
      "assertion",
    )(value);
    const classification = classifyAbortSync(observation.armed);
    // A barrier that resolves after the reset claims a durability the reset did
    // not provide; an unclassified result proves nothing about either source.
    yield* equal(
      ["abort-error", "sync-rejected", "sync-unsettled"].includes(
        classification,
      ),
      true,
    );
    // The documented contract is that abort() immediately resets the object.
    yield* equal(
      observation.activation.before !== observation.activation.after,
      true,
    );
    // The acknowledged seed survives; only the unconfirmed replacement may not.
    yield* checkOutageRecovery(observation.recovered);
    // A new activation must accept barriers again.
    yield* checkSyncedState(observation.resumed, true);
    return classification;
  });
const activation = (ctx: QualificationContext) =>
  ctx.json("/fleet/id").pipe(
    Effect.flatMap(
      decodeAs(Schema.Struct({ activation: Schema.String }), "assertion"),
    ),
    Effect.map((identity) => identity.activation),
  );
const abortSync = (ctx: QualificationContext) =>
  Effect.gen(function* () {
    yield* seed(ctx);
    yield* ctx.writeAcknowledged();
    const before = yield* activation(ctx);
    const armed = yield* request(ctx, "sync-abort");
    const after = yield* activation(ctx);
    const recovered = yield* json(ctx, "state");
    const resumed = yield* json(ctx, "sync");
    const observation = {
      armed: { status: armed.status, body: armed.body },
      activation: { before, after },
      recovered,
      resumed,
    };
    yield* ctx.artifacts.json("sync-abort.json", observation);
    const classification = yield* checkAbortSync(observation);
    // The reset must not cost an acknowledged write from the independent ledger.
    yield* ctx.writeAcknowledged();
    yield* ctx.verify();
    return { ...observation, classification };
  });
const BarrierRound = Schema.Struct({
  round: Schema.Int,
  outcome: Schema.String,
  elapsedMs: Schema.Number,
});
const BarrierState = Schema.Struct({
  node: Schema.String,
  rows: Schema.Array(Schema.Struct({ n: Schema.Int })),
  value: Schema.String,
});
const barrierIndex = (value: string) => {
  const match = /^barrier-(\d+)$/.exec(value);
  return match ? Number(match[1]) : undefined;
};
/** Any durability wait celld logged for this cell, fleet-backed or not. */
export const hasDurabilityProof = (logs: string, cell: string) =>
  logs
    .split("\n")
    .some((line) => line.includes("durable_wait") && line.includes(cell));
export const checkBarrierPartition = (value: unknown) =>
  Effect.gen(function* () {
    const observation = yield* decodeAs(
      Schema.Struct({
        partitionProof: Schema.Boolean,
        inFlight: Schema.Boolean,
        classification: Schema.Literals(["resolved", "rejected", "blocked"]),
        rounds: Schema.Array(BarrierRound),
        durableProof: Schema.Boolean,
        falseFleetProof: Schema.Boolean,
        states: Schema.Array(BarrierState),
      }),
      "assertion",
    )(value);
    // Without an unreachable peer and an open barrier there is no interruption.
    yield* equal(observation.partitionProof, true);
    yield* equal(observation.inFlight, true);
    // No peer was reachable, so a fleet-replication proof would be a false claim
    // whatever the barrier then reported to the caller.
    yield* equal(observation.falseFleetProof, false);
    yield* equal(observation.rounds.length > 0, true);
    for (const round of observation.rounds)
      yield* equal(
        round.outcome === "resolved" || round.outcome.startsWith(syncMarker),
        true,
      );
    yield* equal(observation.states.length, 3);
    // No partial transaction: the seeded row is the only committed SQL state.
    for (const state of observation.states)
      yield* equal(state.rows, [{ n: 1 }]);
    const values = new Set(observation.states.map((state) => state.value));
    yield* equal(values.size, 1);
    const resolved = observation.rounds.filter(
      (round) => round.outcome === "resolved",
    );
    const last = resolved.at(-1);
    if (last) {
      // A resolved barrier acknowledged its write; losing it is a lost write.
      const surviving = barrierIndex([...values][0]!);
      yield* equal(surviving !== undefined && surviving >= last.round, true);
    }
    // Resolving every barrier while the peers were unreachable needs evidence
    // that the node actually waited for durability instead of short-circuiting.
    if (observation.classification === "resolved")
      yield* equal(observation.durableProof, true);
    return observation.classification;
  });
const proveFleetDurability = (ctx: QualificationContext, owner: Node) =>
  Effect.gen(function* () {
    // Distinct warm-up transactions establish a live ensemble; a bucket-only
    // acknowledgment cannot satisfy this and must not silently pass.
    for (let attempt = 1; attempt <= 8; attempt++) {
      const previous = yield* ctx.fleet.logs(owner);
      yield* ctx.writeAcknowledged(owner);
      const current = yield* ctx.fleet.logs(owner);
      yield* equal(current.startsWith(previous), true);
      if (hasFleetProof(current.slice(previous.length), ctx.identity.cell))
        return attempt;
      yield* Effect.sleep("1 second");
    }
    return yield* Effect.fail(
      new TckError({
        phase: "fleet-proof",
        message: "No fleet durability proof before the barrier",
      }),
    );
  });
const peerUnreachable = (ctx: QualificationContext, peers: readonly Node[]) =>
  Effect.gen(function* () {
    const probes: Array<{ peer: Node; stdout: string }> = [];
    for (const peer of peers) {
      const probe = yield* ctx.runtime.controls.compose([
        "exec",
        "-T",
        "proxy",
        "node",
        "--input-type=module",
        "-e",
        `try { await fetch("http://peer-${peer}:8081/health", { signal: AbortSignal.timeout(1000) }); process.exitCode = 1; } catch { console.log("peer-unreachable"); }`,
      ]);
      probes.push({ peer, stdout: probe.stdout.trim() });
    }
    yield* ctx.artifacts.json("barrier-peer-probe.json", probes);
    return probes.every((probe) => probe.stdout === "peer-unreachable");
  });
const barrierPartition = (ctx: QualificationContext) =>
  Effect.gen(function* () {
    yield* seed(ctx);
    yield* acknowledgedBatch(ctx, 6);
    const owner = (yield* ctx.owner()).node;
    // Partition the owner's peers, not the owner: replication stops while the
    // owner keeps the published port this driver observes the barrier through.
    const peers = ctx.nodes.filter((node) => node !== owner);
    const warmup = yield* proveFleetDurability(ctx, owner);
    const beforeLogs = yield* ctx.fleet.logs(owner);
    const fiber = yield* Effect.forkChild(
      request(ctx, "sync-barrier?hold=20000", {
        node: owner,
        timeoutMs: 120_000,
      }),
    );
    // Let ordinary fleet-backed barriers resolve before replication is cut.
    yield* Effect.sleep("2 seconds");
    const interruption = yield* Effect.scoped(
      Effect.gen(function* () {
        for (const peer of peers) yield* ctx.fleet.partitionPeers(peer);
        const partitionProof = yield* peerUnreachable(ctx, peers);
        const inFlight = yield* Effect.sync(
          () => fiber.pollUnsafe() === undefined,
        );
        // The probes above outlast any barrier that started before the cut, so
        // this snapshot opens a window containing only partitioned barriers.
        const opened = yield* ctx.fleet.logs(owner);
        const settled = yield* Fiber.await(fiber).pipe(
          Effect.timeout("40 seconds"),
          Effect.catch(() => Effect.succeed(undefined)),
        );
        const closed = yield* ctx.fleet.logs(owner);
        yield* equal(closed.startsWith(opened), true);
        return {
          partitionProof,
          inFlight,
          settled: settled !== undefined,
          partitioned: closed.slice(opened.length),
        };
      }),
    );
    // Scope exit reconnected the peers; a blocked barrier may finish only now.
    const response = yield* Fiber.join(fiber);
    yield* equal(response.status, 200);
    const barrier = yield* decodeAs(
      Schema.Struct({ rounds: Schema.Array(BarrierRound) }),
      "assertion",
    )(response.body);
    const afterLogs = yield* ctx.fleet.logs(owner);
    yield* equal(afterLogs.startsWith(beforeLogs), true);
    const durableProof = hasDurabilityProof(
      interruption.partitioned,
      ctx.identity.cell,
    );
    const falseFleetProof = hasFleetProof(
      interruption.partitioned,
      ctx.identity.cell,
    );
    yield* ctx.artifacts.text("sync-barrier-owner.log", afterLogs);
    yield* ctx.artifacts.text(
      "sync-barrier-partitioned.log",
      interruption.partitioned,
    );
    const classification = !interruption.settled
      ? ("blocked" as const)
      : barrier.rounds.some((round) => round.outcome !== "resolved")
        ? ("rejected" as const)
        : ("resolved" as const);
    // Peer reconnection does not restore Docker's published ports; restarting
    // the fleet refreshes every endpoint before reading through all nodes.
    yield* ctx.restartAll();
    const states = yield* Effect.forEach(ctx.nodes, (node) =>
      request(ctx, "state", { node }).pipe(
        Effect.tap((result) => equal(result.status, 200)),
        Effect.map((result) => ({
          node,
          ...(result.body as Record<string, unknown>),
        })),
      ),
    );
    const observation = {
      owner,
      peers,
      warmup,
      partitionProof: interruption.partitionProof,
      inFlight: interruption.inFlight,
      classification,
      rounds: barrier.rounds,
      durableProof,
      falseFleetProof,
      states,
    };
    yield* ctx.artifacts.json("sync-barrier-partition.json", observation);
    yield* checkBarrierPartition(observation);
    for (const node of ctx.nodes) yield* ctx.verify(node);
    return observation;
  });
export const storageDurabilityCases = [
  {
    id: "faults.write-cursor-response",
    run: (ctx: QualificationContext) => cursorOutput("response", ctx),
  },
  {
    id: "faults.write-cursor-outbound",
    run: (ctx: QualificationContext) => cursorOutput("outbound", ctx),
  },
  { id: "faults.sync-recovery", durability: "bucket" as const, run: recovery },
  { id: "faults.sync-outage", durability: "bucket" as const, run: outage },
  {
    id: "faults.sync-transaction",
    run: (ctx: QualificationContext) =>
      Effect.gen(function* () {
        yield* seed(ctx);
        const value = yield* json(ctx, "sync-transaction");
        yield* checkTransactionSync(value);
        return value;
      }),
  },
  {
    id: "faults.write-cursor-sync",
    run: (ctx: QualificationContext) =>
      Effect.gen(function* () {
        yield* seed(ctx);
        const value = yield* json(ctx, "write-cursor-sync");
        yield* checkCursorSync(value);
        return value;
      }),
  },
  { id: "faults.sync-abort", durability: "bucket" as const, run: abortSync },
  { id: "faults.sync-barrier-partition", run: barrierPartition },
  {
    id: "faults.transaction-deadline",
    run: (ctx: QualificationContext) => deadline("transaction-deadline", ctx),
  },
  {
    id: "faults.gate-deadline",
    run: (ctx: QualificationContext) => deadline("gate-deadline", ctx),
  },
];
