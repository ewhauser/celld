import { readEventually, retryRead, pendingPhase } from "./Polling.js";
import { Effect, Exit, Fiber, Schema } from "effect";
import { Artifacts } from "./Artifacts.js";
import {
  attemptRequest,
  leaseLapse,
  TckError,
  type Observation,
} from "./Domain.js";
import { type Node, type fleetControls } from "./FleetControls.js";
import { equal } from "./Oracle.js";
import {
  checkOutageState,
  classifyWrite,
  OutageState,
  type WriteOutcome,
} from "./Outage.js";

type Fleet = Effect.Success<ReturnType<typeof fleetControls>>;
export interface ResilienceContext {
  fleet: Fleet;
  nodes: readonly Node[];
  cell: () => string;
  acknowledged: number[];
  write: (node: Node, id: number) => Effect.Effect<void, unknown>;
  check: (node: Node) => Effect.Effect<void, unknown>;
  read: (node: Node) => Effect.Effect<typeof OutageState.Type, unknown>;
  ready: (node: Node) => Effect.Effect<void, unknown>;
  start: (node: Node) => Effect.Effect<void, unknown>;
  request: (
    node: Node,
    path: string,
    method?: "GET" | "POST",
  ) => Effect.Effect<Observation, TckError>;
}
export const checkEnsemble = (
  owner: Node,
  peers: readonly Node[],
  ensemble: readonly Node[],
) => equal([...ensemble].sort(), peers.filter((node) => node !== owner).sort());
export interface LossRecord {
  readonly leader: string;
  readonly epoch: number;
  readonly note: string;
}
export const checkReplicaLoss = (
  state: typeof OutageState.Type,
  history: readonly WriteOutcome[],
  leader: string,
  epoch: number,
  losses: readonly LossRecord[],
) =>
  Effect.gen(function* () {
    const complete = yield* Effect.exit(checkOutageState(state, history));
    if (Exit.isSuccess(complete)) return "fully-recovered" as const;
    // Declared loss permits absent transactions, never partial or corrupt transactions.
    yield* checkOutageState(
      state,
      history.map((entry) => ({ ...entry, acknowledged: false })),
    );
    yield* equal(
      losses.some(
        (record) => record.leader === leader && record.epoch === epoch,
      ),
      true,
    );
    return "declared-data-loss" as const;
  });
export const resilienceStages = (ctx: ResilienceContext) =>
  Effect.gen(function* () {
    const artifacts = yield* Artifacts;
    const { fleet, nodes } = ctx;
    const others = (node: Node) => nodes.filter((peer) => peer !== node);
    const checkAll = () => Effect.forEach(nodes, ctx.check, { discard: true });
    const readable = (node: Node) =>
      readEventually(ctx.read(node), { attempts: 31, timeout: "60 seconds" });
    const ensemble = (
      node: Node,
      peers: readonly Node[],
      requireActive = true,
    ) =>
      Effect.gen(function* () {
        const lease = yield* fleet.lease(node);
        if (
          !lease.log ||
          lease.log.state !== "open" ||
          (requireActive && !lease.log.active) ||
          JSON.stringify([...lease.log.ensemble].sort()) !==
            JSON.stringify([...peers].sort())
        )
          return yield* Effect.fail(
            new TckError({
              phase: "ensemble-pending",
              message: "Waiting for active ensemble",
            }),
          );
        yield* checkEnsemble(node, [node, ...peers], lease.log.ensemble);
        return lease;
      }).pipe((probe) =>
        retryRead(probe, {
          retryable: pendingPhase("ensemble-pending"),
          interval: "1 second",
          attempts: 31,
          timeout: "45 seconds",
        }),
      );
    // A healthy one-follower ensemble does not expand in v0.5.0. Start a fresh
    // owner generation after both peers are ready to establish this scenario's precondition.
    const recruitBoth = (id: number, warmId: number) =>
      Effect.gen(function* () {
        const leader = (yield* fleet.owner(ctx.cell())).node;
        yield* fleet.kill(leader);
        yield* leaseLapse;
        yield* ctx.start(leader);
        yield* ctx.ready(leader);
        yield* ctx.write(leader, id);
        const current = (yield* fleet.owner(ctx.cell())).node;
        yield* ensemble(current, others(current), false);
        yield* ctx.write(current, warmId);
        const lease = yield* ensemble(current, others(current));
        return { leader: current, lease };
      });
    const recordHistory = () =>
      ctx.acknowledged
        .map((id) => ({ id, acknowledged: true }))
        .sort((a, b) => a.id - b.id);
    return [
      {
        id: "resilience.paused-owner",
        run: Effect.gen(function* () {
          const prior = yield* fleet.owner(ctx.cell());
          const successor = others(prior.node)[0]!;
          yield* fleet.pause(prior.node);
          yield* leaseLapse;
          yield* readable(successor);
          yield* ctx.check(successor);
          const next = yield* fleet.owner(ctx.cell());
          yield* equal(next.node === prior.node, false);
          yield* equal(next.epoch > prior.epoch, true);
          yield* ctx.write(successor, 110);
          yield* fleet.unpause(prior.node);
          yield* fleet.fenced(prior.node);
          yield* ctx.start(prior.node);
          yield* ctx.ready(prior.node);
          yield* checkAll();
        }),
      },
      {
        id: "resilience.interrupted-writes",
        run: Effect.scoped(
          Effect.gen(function* () {
            const prior = yield* fleet.owner(ctx.cell());
            const successor = others(prior.node)[0]!;
            const operations = yield* Effect.forEach(
              [120, 121, 122, 123, 124, 125],
              (id) =>
                classifyWrite(
                  id,
                  ctx.request(
                    prior.node,
                    `/outage/write?id=${id}&hold=1`,
                    "POST",
                  ),
                ).pipe(Effect.forkScoped),
            );
            const entered = yield* fleet.logs(prior.node).pipe(
              Effect.flatMap((logs) =>
                /tck-interrupted-write id=12[0-5]\b/.test(logs)
                  ? Effect.succeed(logs)
                  : Effect.fail(
                      new TckError({
                        phase: "write-pending",
                        message:
                          "Waiting for a transaction to enter its response delay",
                      }),
                    ),
              ),
              (probe) =>
                retryRead(probe, {
                  retryable: pendingPhase("write-pending"),
                  interval: "50 millis",
                  attempts: 31,
                  timeout: "3 seconds",
                }),
            );
            yield* artifacts.text("interrupted-write-entry.log", entered);
            yield* fleet.kill(prior.node);
            const outcomes: WriteOutcome[] = yield* Effect.forEach(
              operations,
              Fiber.join,
            );
            yield* artifacts.json("interrupted-write-outcomes.json", outcomes);
            yield* equal(
              outcomes.some((outcome) => !outcome.acknowledged),
              true,
            );
            yield* leaseLapse;
            const recovered = yield* readable(successor);
            yield* artifacts.json("interrupted-write-state.json", recovered);
            yield* checkOutageState(recovered, [
              ...recordHistory(),
              ...outcomes,
            ]);
            for (const outcome of outcomes)
              if (recovered.sql.some((row) => row.id === outcome.id))
                ctx.acknowledged.push(outcome.id);
            yield* ctx.start(prior.node);
            yield* ctx.ready(prior.node);
            yield* checkAll();
          }),
        ),
      },
      {
        id: "resilience.simultaneous-restart",
        run: Effect.gen(function* () {
          yield* ctx.write(nodes[0]!, 130);
          yield* Effect.all(nodes.map(fleet.kill), {
            concurrency: "unbounded",
          });
          yield* leaseLapse;
          yield* Effect.all(nodes.map(ctx.start), { concurrency: "unbounded" });
          for (const node of nodes) yield* ctx.ready(node);
          yield* checkAll();
        }),
      },
      {
        id: "resilience.follower-loss",
        run: Effect.gen(function* () {
          const { leader } = yield* recruitBoth(140, 144);
          const lost = others(leader)[0]!;
          const remaining = others(leader)[1]!;
          yield* fleet.kill(lost);
          yield* leaseLapse;
          yield* ctx.write(leader, 141);
          yield* ensemble(leader, [remaining], false);
          yield* ctx.write(leader, 143);
          yield* ensemble(leader, [remaining]);
          // With bucket access cut, a successful new write requires the remaining follower.
          yield* fleet.partition(leader);
          yield* ctx.write(leader, 142);
          yield* fleet.kill(leader);
          yield* leaseLapse;
          yield* readable(remaining);
          yield* ctx.check(remaining);
          yield* fleet.reconnect(leader);
          yield* ctx.start(leader);
          yield* ctx.ready(leader);
          yield* ctx.start(lost);
          yield* ctx.ready(lost);
          yield* checkAll();
        }),
      },
      {
        id: "resilience.replica-disk-loss",
        run: Effect.gen(function* () {
          const { leader, lease } = yield* recruitBoth(150, 154);
          yield* fleet.partition(leader);
          yield* ctx.write(leader, 151);
          yield* Effect.all(nodes.map(fleet.kill), {
            concurrency: "unbounded",
          });
          yield* leaseLapse;
          for (const node of nodes) yield* fleet.discard(node);
          yield* Effect.all(nodes.map(ctx.start), { concurrency: "unbounded" });
          // Losing all durable copies exceeds RPO=0. Only full recovery or explicit refusal/loss reporting is acceptable.
          yield* Effect.sleep("15 seconds");
          const response = yield* attemptRequest(
            ctx.request(nodes[0]!, "/outage/state"),
          );
          const losses = yield* fleet.losses();
          const relevant = losses.filter(
            (record) =>
              record.leader === `${leader}/${lease.probe_public_key}` &&
              record.epoch === lease.log?.epoch,
          );
          if ("response" in response && response.response.status === 200) {
            const state = yield* Schema.decodeUnknownEffect(OutageState)(
              response.response.body,
            );
            const outcome = yield* checkReplicaLoss(
              state,
              recordHistory(),
              `${leader}/${lease.probe_public_key}`,
              lease.log!.epoch,
              losses,
            );
            yield* artifacts.json("replica-disk-loss.json", {
              outcome,
              state,
              losses,
              acknowledged: ctx.acknowledged,
            });
          } else {
            const logs = (yield* Effect.forEach(nodes, fleet.logs)).join("\n");
            yield* equal(
              relevant.length > 0 ||
                logs.includes(
                  `node-log recovery for ${leader}/${lease.probe_public_key}: no complete true witness`,
                ),
              true,
            );
            yield* artifacts.json("replica-disk-loss.json", {
              outcome: "recovery-refused",
              response,
              losses,
              acknowledged: ctx.acknowledged,
            });
          }
        }),
      },
    ];
  });
