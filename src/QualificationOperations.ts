// Fleet-operations qualification.
//
// Every case drives a documented v0.5.0 operational control — weighted
// ownership placement, balancing pause and resume, a graceful drain, a
// same-node preserve, a rolling binary upgrade — across a three-node fleet
// that holds several cells, and keeps ownership, readiness and
// acknowledged-write evidence across the operation. The controls, where they
// live on the binary, and the limits of this coverage are recorded in
// docs/FLEET-OPERATIONS.md. Orchestration lives here; every classification is
// a pure oracle in OperationsOracles.ts.
import { Effect, Fiber, Schema } from "effect";
import { decodeAs, decodeJson } from "./Artifacts.js";
import { leaseLapse } from "./Domain.js";
import { NodeLoad, type Node } from "./FleetControls.js";
import { pollUntil, waitForReady } from "./Polling.js";
import { equal } from "./Oracle.js";
import {
  makeContext,
  sidecarProbe,
  type QualificationContext,
} from "./QualificationContext.js";
import { checkAllowed, observation } from "./SecurityOracles.js";
import type { Probe } from "./SecurityProbe.js";
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
  overTarget,
  parseDrainLog,
  type OwnershipMap,
  type ReadinessSample,
} from "./OperationsOracles.js";

/** The weights infra/operations.yaml pins. Kept in step with that overlay. */
const WEIGHTS: Record<Node, number> = { celld: 8, celld2: 1, celld3: 1 };
/** The complete process stop bound infra/operations.yaml pins. */
const SHUTDOWN_TOTAL_MS = 20000;
/**
 * Docker start-up, the compose round trip and the harness poll interval all
 * sit between celld's own stop and the moment this process can observe it, so
 * the observed duration is compared against the bound plus this margin.
 */
const STOP_MARGIN_MS = 15000;

/** The release infra/compose.yaml pins and the whole suite otherwise runs. */
export const CURRENT_RELEASE = "0.6.0";
export const CURRENT_IMAGE =
  "ghcr.io/denoland/celld:v0.6.0@sha256:e188a7f2bb0b8cec9fb04ee4c3d1ed7cca0ea0419519ae2a9ba36e5b6fe5161b";
/**
 * v0.6.0 supports a rolling update from v0.5.1 only with bucket durability.
 * Fleet durability requires stopping the whole fleet before upgrading.
 */
export const PREVIOUS_RELEASE = "0.5.1";
export const PREVIOUS_IMAGE =
  "ghcr.io/denoland/celld:v0.5.1@sha256:9df15352bcbb92a8d73dabadc349383ccd3d7851b2ba4ccffe2c956f2f9fc93f";

const NodeState = Schema.Struct({
  owned_cells: Schema.Int,
  occupied: Schema.Int,
  handed_off: Schema.Int,
  rebalanced: Schema.Int,
  rebalance_failed: Schema.Int,
  node_load: NodeLoad,
});
type NodeState = typeof NodeState.Type;
const Paused = Schema.Struct({ rebalance_paused: Schema.Boolean });
const Ok = Schema.Struct({ ok: Schema.Boolean });

/** A separate cell with its own ledger, so several cells carry evidence. */
type Cell = QualificationContext;

// ---------------------------------------------------------------------------
// Operator listener
// ---------------------------------------------------------------------------

let probeSequence = 0;
/**
 * One call on celld's internal listener. The listener is never published
 * outside the Compose network, so every operator request runs through the
 * in-network probe; a raw socket is required because `/shutdown` answers and
 * then takes the process down while a client is still attached.
 */
const operator = (
  ctx: QualificationContext,
  label: string,
  node: Node,
  path: string,
  method: "GET" | "POST" = "GET",
) =>
  Effect.gen(function* () {
    const probe: Probe = {
      kind: "http",
      label,
      url: `http://${node}:8081${path}`,
      method,
      timeoutMs: 30000,
    };
    const results = yield* sidecarProbe(ctx, `${label}-${++probeSequence}`, [
      probe,
    ]);
    const result = yield* observation(results, label);
    yield* checkAllowed(result, { status: 200 });
    return result.body ?? "";
  });

const nodeState = (ctx: QualificationContext, node: Node) =>
  operator(ctx, `state-${node}`, node, "/state").pipe(
    Effect.flatMap((body) => decodeJson(NodeState, body)),
  );

const nodeStates = (ctx: QualificationContext, nodes: readonly Node[]) =>
  Effect.forEach(nodes, (node) =>
    nodeState(ctx, node).pipe(Effect.map((state) => [node, state] as const)),
  ).pipe(
    Effect.map(
      (entries) =>
        Object.fromEntries(entries) as Readonly<Record<Node, NodeState>>,
    ),
  );

const ownedCells = (states: Readonly<Record<string, NodeState>>) =>
  Object.fromEntries(
    Object.entries(states).map(([node, state]) => [node, state.owned_cells]),
  );

/**
 * Waits for balancing to settle. The sample interval is one second and a move
 * is one ownership write plus one signed acquire, so a two-second poll reads
 * the fleet often enough without turning every convergence into hundreds of
 * operator requests.
 */
const settled = (ctx: QualificationContext, nodes: readonly Node[]) =>
  pollUntil(
    nodeStates(ctx, nodes),
    (states) => overTarget(WEIGHTS, ownedCells(states)).length === 0,
    {
      interval: "2 seconds",
      attempts: 75,
      timeout: "150 seconds",
      message: "Waiting for ownership balancing to settle",
    },
  );

// ---------------------------------------------------------------------------
// Cells
// ---------------------------------------------------------------------------

/**
 * Several independent cells, each with its own ledger. The qualification
 * fixture keys objects by `name`, so a distinct name is a distinct cell, and
 * each one records its own acknowledged writes and its own owner.
 */
const cells = (
  ctx: QualificationContext,
  count: number,
  home: (index: number) => Node = () => "celld",
) =>
  Effect.forEach(Array.from({ length: count }), (_, index) =>
    makeContext(
      ctx.runtime,
      `${ctx.name}-cell${index}`,
      ctx.seed + index,
      home(index),
    ),
  );

/**
 * Background cells that only exist to give balancing something to move. They
 * carry no ledger: the acknowledged-write evidence lives on the cells above.
 */
const ballast = (
  ctx: QualificationContext,
  node: Node,
  tag: string,
  count: number,
) =>
  Effect.forEach(
    Array.from({ length: count }),
    (_, index) =>
      Effect.gen(function* () {
        // The tag keeps two rounds of ballast from naming the same cells.
        const name = `${ctx.name}-${tag}${index}`;
        // Ballast is background load, not evidence, so a cold activation that
        // the node refuses under contention is retried rather than failing the
        // scenario. Every acknowledged write under test lives on a ledgered
        // cell, where no retry is applied.
        const accepted = (
          path: string,
          method: "GET" | "POST",
          body?: string,
        ) =>
          pollUntil(
            ctx.transport.request(ctx.targets[node], {
              path,
              method,
              timeoutMs: 30000,
              ...(body === undefined
                ? {}
                : { body, headers: { "content-type": "application/json" } }),
            }),
            (response) => response.status === 200,
            {
              interval: "1 second",
              attempts: 10,
              timeout: "60 seconds",
              message: `Waiting for ${name} to accept a ballast request`,
            },
          );
        yield* accepted(
          `/history/write?name=${name}`,
          "POST",
          JSON.stringify({ id: `${tag}-${index}`, payload: name }),
        );
        const identity = yield* accepted(`/fleet/id?name=${name}`, "GET");
        return yield* decodeAs(
          Schema.Struct({ cell: Schema.String }),
          "operations",
        )(identity.body).pipe(Effect.map((decoded) => decoded.cell));
      }),
    { concurrency: 2 },
  );

/** Every published endpoint a restarted node hands out has to be re-read. */
const refresh = (
  ctx: QualificationContext,
  members: readonly Cell[],
  node: Node,
) =>
  Effect.gen(function* () {
    const target = yield* ctx.fleet.target(node);
    ctx.targets[node] = target;
    for (const member of members) member.targets[node] = target;
  });

/**
 * Brings stopped nodes back and waits for the documented first-readiness gate.
 *
 * Every node is started before any of them is waited on, because the gate is a
 * fleet condition: a replacement holds its first healthy response until the
 * fleet is settled, and a fleet that is still missing a node can keep it
 * closed. The wait covers the whole 60-second gate infra/operations.yaml pins,
 * which is longer than the context's ordinary readiness budget.
 */
const restart = (
  ctx: QualificationContext,
  members: readonly Cell[],
  stopped: readonly Node[],
) =>
  Effect.gen(function* () {
    for (const node of stopped) yield* ctx.fleet.start(node);
    for (const node of stopped) yield* refresh(ctx, members, node);
    yield* Effect.forEach(
      stopped,
      (node) =>
        waitForReady(
          ctx.transport.request(ctx.targets[node], {
            path: "/.well-known/celld/health",
          }),
          { interval: "1 second", attempts: 150, timeout: "150 seconds" },
        ),
      { discard: true },
    );
  });

/** The acknowledged ids of one cell, read back from its persisted ledger. */
const acknowledgedIds = (cell: Cell) =>
  cell.ledger
    .read()
    .pipe(
      Effect.map((events) =>
        events.filter((event) => event.kind === "ack").map((event) => event.id),
      ),
    );

/**
 * Every acknowledged write of every cell must still be readable through every
 * node, and the full ledger check must pass on each of them.
 */
const verifyAll = (members: readonly Cell[], nodes: readonly Node[]) =>
  Effect.forEach(members, (member) =>
    Effect.gen(function* () {
      const acknowledged = yield* acknowledgedIds(member);
      for (const node of nodes) {
        const rows = yield* member.state(node);
        yield* checkAcknowledgedRetained(
          `${member.name} through ${node}`,
          acknowledged,
          rows.map((row) => row.id),
        );
      }
      return yield* member.verify(nodes[0]!);
    }),
  );

/** celld's logs and the bucket keys carry the class-qualified scope. */
const scope = (cell: string) => `Recovery:${cell}`;

const ownersOf = (owners: OwnershipMap, members: readonly Cell[]) =>
  Object.fromEntries(
    members.map((member) => [member.name, owners[member.identity.cell]]),
  );

// ---------------------------------------------------------------------------
// operations.weighted-placement
// ---------------------------------------------------------------------------

const weightedPlacement = (ctx: QualificationContext) =>
  Effect.gen(function* () {
    const { fleet, nodes, artifacts } = ctx;
    // The configured weights must be what the fleet actually publishes,
    // through both surfaces an operator can read.
    const initial = yield* nodeStates(ctx, nodes);
    yield* checkPublishedWeights(
      WEIGHTS,
      Object.fromEntries(
        Object.entries(initial).map(([node, state]) => [
          node,
          state.node_load.placement_weight,
        ]),
      ),
    );
    const leases = yield* Effect.forEach(nodes, (node) =>
      fleet.lease(node).pipe(Effect.map((lease) => [node, lease] as const)),
    );
    yield* checkPublishedWeights(
      WEIGHTS,
      Object.fromEntries(
        leases.map(([node, lease]) => [node, lease.load?.placement_weight]),
      ) as Record<string, number>,
    );

    // Every cell is created through one node, so a converged distribution can
    // only be the result of balancing, not of where the requests arrived.
    const members = yield* cells(ctx, 3);
    yield* Effect.forEach(members, (member) =>
      member.writeAcknowledged("celld"),
    );
    const filler = yield* ballast(ctx, "celld", "fill", 21);
    const before = yield* nodeStates(ctx, nodes);
    yield* equal(overTarget(WEIGHTS, ownedCells(before)).length > 0, true);

    const converged = yield* settled(ctx, nodes);
    yield* checkWeightedPlacement(WEIGHTS, ownedCells(converged));

    const owners = yield* fleet.ownership();
    yield* artifacts.json("weighted-placement.json", {
      weights: WEIGHTS,
      before: ownedCells(before),
      after: ownedCells(converged),
      cellOwners: ownersOf(owners, members),
      ballast: filler.length,
    });
    yield* verifyAll(members, nodes);
    return {
      weights: WEIGHTS,
      placedFrom: ownedCells(before),
      placedTo: ownedCells(converged),
    };
  });

// ---------------------------------------------------------------------------
// operations.rebalance-control
// ---------------------------------------------------------------------------

const rebalanceControl = (ctx: QualificationContext) =>
  Effect.gen(function* () {
    const { fleet, nodes, artifacts } = ctx;
    // The pause is issued on celld2 and the overload is built on celld: one
    // paused lease is documented to stop every move in the fleet, not only the
    // moves of the node that published it.
    const pauseBody = yield* operator(
      ctx,
      "rebalance-pause",
      "celld2",
      "/rebalance/pause",
      "POST",
    );
    yield* equal(yield* decodeJson(Paused, pauseBody), {
      rebalance_paused: true,
    });
    yield* ctx.poll(fleet.lease("celld2"), (lease) =>
      Boolean(lease.load?.rebalance_paused),
    );

    const members = yield* cells(ctx, 3);
    yield* Effect.forEach(members, (member) =>
      member.writeAcknowledged("celld"),
    );
    yield* ballast(ctx, "celld", "fill", 21);
    const during = yield* nodeStates(ctx, nodes);
    // Without an imbalance the fleet has nothing to move and a pause proves
    // nothing, so the overload is required before the window is observed.
    yield* equal(
      overTarget(WEIGHTS, ownedCells(during)).includes("celld"),
      true,
    );
    const before = yield* fleet.ownership();
    // Ten sample intervals, an order of magnitude past the one-second period
    // infra/balancing.yaml pins.
    yield* Effect.sleep("10 seconds");
    const after = yield* fleet.ownership();
    yield* checkNoMoves("rebalance paused", before, after);
    const held = yield* nodeStates(ctx, nodes);
    for (const node of nodes)
      yield* equal(held[node].rebalanced, during[node].rebalanced);

    const resumeBody = yield* operator(
      ctx,
      "rebalance-resume",
      "celld2",
      "/rebalance/resume",
      "POST",
    );
    yield* equal(yield* decodeJson(Paused, resumeBody), {
      rebalance_paused: false,
    });
    const converged = yield* settled(ctx, nodes);
    yield* checkWeightedPlacement(WEIGHTS, ownedCells(converged));

    // A frozen process publishes no new sample and cannot renew its lease, so
    // the fleet sees its capacity sample age past the maximum sample age and
    // then disappear. Nothing may be placed on it while that holds.
    const live: readonly Node[] = ["celld", "celld2"];
    const staleBefore = yield* fleet.ownership();
    yield* fleet.pause("celld3");
    // The sample-staleness window alone can close before the node lease
    // expires on a fast host; the self-fence asserted after unpause needs the
    // lease to have lapsed, so wait it out explicitly.
    yield* leaseLapse;
    yield* ballast(ctx, "celld2", "stale", 9);
    const moved = yield* ctx.poll(nodeStates(ctx, live), (states) =>
      live.some((node) => states[node].rebalanced > converged[node].rebalanced),
    );
    const staleAfter = yield* fleet.ownership();
    yield* checkNoAcquisition("celld3", staleBefore, staleAfter);

    yield* artifacts.json("rebalance-control.json", {
      weights: WEIGHTS,
      pausedDistribution: ownedCells(during),
      heldDistribution: ownedCells(held),
      resumedDistribution: ownedCells(converged),
      liveDistributionWithoutSample: ownedCells(moved),
      rebalanceFailed: Object.fromEntries(
        live.map((node) => [node, moved[node].rebalance_failed]),
      ),
      staleOwned: {
        before: Object.values(staleBefore).filter((o) => o.node === "celld3")
          .length,
        after: Object.values(staleAfter).filter((o) => o.node === "celld3")
          .length,
      },
      cellOwners: ownersOf(staleAfter, members),
    });

    // A node that could not renew its lease fences itself as soon as it runs
    // again; the fleet is put back together before the ledgers are checked.
    yield* fleet.unpause("celld3");
    yield* fleet.fenced("celld3");
    yield* leaseLapse;
    yield* restart(ctx, members, ["celld3"]);
    yield* verifyAll(members, nodes);
    return {
      pausedDistribution: ownedCells(during),
      resumedDistribution: ownedCells(converged),
      liveDistributionWithoutSample: ownedCells(moved),
    };
  });

// ---------------------------------------------------------------------------
// operations.graceful-drain
// ---------------------------------------------------------------------------

/** Samples the public readiness path until the fiber is interrupted. */
const readinessSampler = (
  ctx: QualificationContext,
  node: Node,
  into: ReadinessSample[],
) =>
  Effect.gen(function* () {
    for (;;) {
      const healthy = yield* ctx.transport
        .request(ctx.targets[node], {
          path: "/.well-known/celld/health",
          timeoutMs: 2000,
        })
        .pipe(
          Effect.map((response) => response.status === 200),
          Effect.catch(() => Effect.succeed(false)),
        );
      into.push({ atMs: Date.now(), healthy });
      yield* Effect.sleep("250 millis");
    }
  });

/** Waits for a node's container to stop, and reports how long that took. */
const stopped = (ctx: QualificationContext, node: Node, sinceMs: number) =>
  ctx
    .poll(ctx.fleet.inspect(node), (state) => !state.State.Running)
    .pipe(
      Effect.map((state) => ({
        elapsedMs: Date.now() - sinceMs,
        exitCode: state.State.ExitCode,
      })),
    );

const gracefulDrain = (ctx: QualificationContext) =>
  Effect.scoped(
    Effect.gen(function* () {
      const { fleet, nodes, artifacts } = ctx;
      const donor: Node = "celld2";
      // Activation places a new cell on the node that receives its first
      // request, so every evidence cell starts on the node about to drain.
      const members = yield* cells(ctx, 3, () => donor);
      yield* Effect.forEach(members, (member) =>
        member.writeAcknowledged(donor),
      );
      const before = yield* fleet.ownership();
      const drained = members
        .map((member) => member.identity.cell)
        .filter((cell) => before[cell]?.node === donor);
      yield* equal(drained.length, members.length);

      const samples: ReadinessSample[] = [];
      const sampler = yield* readinessSampler(ctx, donor, samples).pipe(
        Effect.forkScoped,
      );
      // One healthy sample has to exist before the drain, so the transition
      // has a starting point that is not assumed.
      yield* ctx.poll(
        Effect.sync(() => samples),
        (observed) => observed.some((sample) => sample.healthy),
      );

      // A write that is accepted by the owning object and still running when
      // the drain starts: celld documents that it finishes what it accepted.
      const inFlight = yield* members[0]!
        .attemptWrite(donor, 6000)
        .pipe(Effect.forkScoped);
      yield* ctx.poll(members[0]!.json("/events", donor), (observed) =>
        JSON.stringify(observed).includes("event:accepted:"),
      );
      const acceptedAtMs = Date.now();

      const drainBody = yield* operator(
        ctx,
        "graceful-drain",
        donor,
        "/shutdown",
        "POST",
      );
      const drainStartedAtMs = Date.now();
      yield* equal(yield* decodeJson(Ok, drainBody), { ok: true });

      // "New public requests receive a 503 response" while the node drains.
      const refused = yield* ctx.poll(
        ctx.transport
          .request(ctx.targets[donor], { path: "/deployment/revision" })
          .pipe(
            Effect.map((response) => response.status),
            Effect.catch(() => Effect.succeed(0)),
          ),
        (status) => status === 503,
      );

      const completed = yield* Fiber.join(inFlight);
      yield* checkAcceptedCompleted({
        acceptedAtMs,
        completedAtMs: completed.completedAtMs,
        drainStartedAtMs,
        expectedId: completed.id,
        ...("status" in completed ? { status: completed.status } : {}),
        ...("receiptId" in completed ? { receiptId: completed.receiptId } : {}),
        ...("error" in completed ? { error: completed.error } : {}),
      });

      const stop = yield* stopped(ctx, donor, drainStartedAtMs);
      yield* Fiber.interrupt(sampler);
      yield* equal(stop.exitCode, 0);
      yield* checkBoundedShutdown(
        donor,
        stop.elapsedMs,
        SHUTDOWN_TOTAL_MS + STOP_MARGIN_MS,
      );
      yield* checkReadinessOrder(donor, samples);

      const after = yield* fleet.ownership();
      const evidence = parseDrainLog(yield* fleet.logs(donor));
      // Written before the oracles run, so a failing drain still leaves its
      // readiness, ownership and handoff evidence behind.
      yield* artifacts.json("graceful-drain.json", {
        donor,
        refusedWith: refused,
        stop,
        readiness: samples,
        accepted: completed,
        ownersBefore: ownersOf(before, members),
        ownersAfter: ownersOf(after, members),
        evidence,
      });
      yield* checkDrainedAway(donor, before, after);
      yield* checkHandoffEvidence(donor, evidence, drained.map(scope));

      yield* restart(ctx, members, [donor]);
      yield* verifyAll(members, nodes);
      return {
        donor,
        stopMs: stop.elapsedMs,
        handoffs: evidence.handoffs.length,
        readinessSamples: samples.length,
      };
    }),
  );

// ---------------------------------------------------------------------------
// operations.concurrent-drain
// ---------------------------------------------------------------------------

const concurrentDrain = (ctx: QualificationContext) =>
  Effect.gen(function* () {
    const { fleet, nodes, artifacts } = ctx;
    const donors: readonly Node[] = ["celld2", "celld3"];
    const survivor: Node = "celld";
    // Two cells start on each donor, so both drains have something to hand off.
    const members = yield* cells(
      ctx,
      4,
      (index) => donors[index % donors.length]!,
    );
    for (const [index, member] of members.entries())
      yield* member.writeAcknowledged(donors[index % donors.length]!);
    const before = yield* fleet.ownership();
    const held = members
      .map((member) => member.identity.cell)
      .filter((cell) => donors.includes((before[cell]?.node ?? "") as Node));
    yield* equal(held.length, members.length);

    const result = yield* Effect.scoped(
      Effect.gen(function* () {
        // The survivor is held below the fleet's ordinary runtime budget, so
        // the two drains hand their cells to a node without spare capacity.
        const recovery = yield* fleet.memoryLimit(survivor, {
          bytes: 128 * 1024 * 1024,
          unlimitedRecovery: {
            Memory: 512 * 1024 * 1024,
            MemorySwap: 512 * 1024 * 1024,
          },
        });
        const constrained = yield* fleet.resources(survivor);
        yield* equal(constrained.HostConfig.Memory, 128 * 1024 * 1024);

        const startedAtMs = Date.now();
        yield* Effect.forEach(
          donors,
          (node) =>
            operator(ctx, `concurrent-drain-${node}`, node, "/shutdown", "POST")
              .pipe(Effect.flatMap((body) => decodeJson(Ok, body)))
              .pipe(Effect.flatMap((body) => equal(body, { ok: true }))),
          { concurrency: "unbounded", discard: true },
        );
        const stops = yield* Effect.forEach(donors, (node) =>
          stopped(ctx, node, startedAtMs).pipe(
            Effect.map((stop) => [node, stop] as const),
          ),
        );
        for (const [node, stop] of stops) {
          yield* equal(stop.exitCode, 0);
          yield* checkBoundedShutdown(
            node,
            stop.elapsedMs,
            SHUTDOWN_TOTAL_MS + STOP_MARGIN_MS,
          );
        }
        const evidence = yield* Effect.forEach(donors, (node) =>
          fleet
            .logs(node)
            .pipe(
              Effect.map((log) => ({ node, evidence: parseDrainLog(log) })),
            ),
        );
        yield* checkSerializedDrains(evidence);
        for (const donor of evidence)
          yield* checkHandoffEvidence(
            donor.node,
            donor.evidence,
            held.filter((cell) => before[cell]!.node === donor.node).map(scope),
          );
        const after = yield* fleet.ownership();
        for (const node of donors) yield* checkDrainedAway(node, before, after);
        return { recovery, stops, evidence, after };
      }),
    );

    // Limits are restored by the scoped finalizer above; the survivors get
    // their ordinary budget back before recovery is required.
    yield* restart(ctx, members, donors);
    yield* artifacts.json("concurrent-drain.json", {
      donors,
      survivor,
      recovery: result.recovery,
      stops: result.stops,
      evidence: result.evidence,
      ownersBefore: ownersOf(before, members),
      ownersAfter: ownersOf(result.after, members),
    });
    yield* verifyAll(members, nodes);
    return {
      donors,
      stopMs: Object.fromEntries(
        result.stops.map(([node, stop]) => [node, stop.elapsedMs]),
      ),
    };
  });

// ---------------------------------------------------------------------------
// operations.preserve-reload
// ---------------------------------------------------------------------------

const preserveReload = (ctx: QualificationContext) =>
  Effect.gen(function* () {
    const { fleet, nodes, artifacts } = ctx;
    const node: Node = "celld3";
    const members = yield* cells(ctx, 3, () => node);
    yield* Effect.forEach(members, (member) => member.writeAcknowledged(node));
    const before = yield* fleet.ownership();
    const preserved = members
      .map((member) => member.identity.cell)
      .filter((cell) => before[cell]?.node === node);
    yield* equal(preserved.length, members.length);

    const body = yield* operator(
      ctx,
      "preserve-reload",
      node,
      "/shutdown?handoff=preserve",
      "POST",
    );
    const startedAtMs = Date.now();
    yield* equal(yield* decodeJson(Ok, body), { ok: true });
    const stop = yield* stopped(ctx, node, startedAtMs);
    yield* equal(stop.exitCode, 0);
    yield* checkBoundedShutdown(
      node,
      stop.elapsedMs,
      SHUTDOWN_TOTAL_MS + STOP_MARGIN_MS,
    );

    const after = yield* fleet.ownership();
    yield* checkOwnershipPreserved(node, before, after);
    // A preserve is not a handoff: the records must be byte-identical, not
    // merely back on the same node under a new epoch.
    yield* checkNoMoves(
      "same-node preserve",
      Object.fromEntries(preserved.map((cell) => [cell, before[cell]!])),
      after,
    );
    const evidence = parseDrainLog(yield* fleet.logs(node));
    yield* equal(evidence.handoffs.length, 0);
    yield* equal(evidence.cleanReload, "prepared");

    yield* restart(ctx, members, [node]);
    const restarted = yield* fleet.ownership();
    yield* checkOwnershipPreserved(node, before, restarted);
    yield* artifacts.json("preserve-reload.json", {
      node,
      stop,
      evidence,
      ownersBefore: ownersOf(before, members),
      ownersAfterStop: ownersOf(after, members),
      ownersAfterRestart: ownersOf(restarted, members),
    });
    yield* verifyAll(members, nodes);
    return { node, stopMs: stop.elapsedMs, cleanReload: evidence.cleanReload };
  });

// ---------------------------------------------------------------------------
// operations.binary-upgrade
// ---------------------------------------------------------------------------

const release = (ctx: QualificationContext, node: Node) =>
  ctx.runtime.controls
    .compose(["exec", "-T", node, "celld", "--version"])
    .pipe(
      Effect.map((output) => output.stdout.trim().replace(/^celld\s+/, "")),
    );

const releases = (ctx: QualificationContext, nodes: readonly Node[]) =>
  Effect.forEach(nodes, (node) =>
    release(ctx, node).pipe(Effect.map((value) => [node, value] as const)),
  ).pipe(Effect.map((entries) => Object.fromEntries(entries)));

const binaryUpgrade = (ctx: QualificationContext) =>
  Effect.gen(function* () {
    const { fleet, nodes, artifacts } = ctx;
    // The fleet started on the explicitly pinned older release.
    const started = yield* releases(ctx, nodes);
    yield* checkNodeReleases(
      Object.fromEntries(nodes.map((node) => [node, PREVIOUS_RELEASE])),
      started,
    );

    // One cell per node, so the upgrade moves an owned cell at every step.
    const members = yield* cells(ctx, 3, (index) => nodes[index % 3]!);
    for (const [index, member] of members.entries())
      yield* member.writeAcknowledged(nodes[index % nodes.length]!);
    const before = yield* fleet.ownership();

    const steps: unknown[] = [];
    let mixedObserved = false;
    for (const node of nodes) {
      yield* ctx.runtime.controls.setImage(node, CURRENT_IMAGE);
      const startedAtMs = Date.now();
      const body = yield* operator(
        ctx,
        `upgrade-drain-${node}`,
        node,
        "/shutdown",
        "POST",
      );
      yield* equal(yield* decodeJson(Ok, body), { ok: true });
      const stop = yield* stopped(ctx, node, startedAtMs);
      yield* equal(stop.exitCode, 0);

      // The replacement adopts the re-pinned image and has to report healthy
      // before the rollout moves on, which is the documented rolling update.
      yield* fleet.recreate(node);
      yield* refresh(ctx, members, node);
      yield* waitForReady(
        ctx.transport.request(ctx.targets[node], {
          path: "/.well-known/celld/health",
        }),
        { interval: "1 second", attempts: 150, timeout: "150 seconds" },
      );
      const image = (yield* fleet.inspect(node)).Config.Image;
      yield* equal(image, CURRENT_IMAGE);

      const observed = yield* releases(ctx, nodes);
      if (new Set(Object.values(observed)).size > 1) {
        yield* checkMixedFleet(observed);
        mixedObserved = true;
        // A mixed fleet must still accept and keep writes on every node.
        for (const member of members)
          for (const live of nodes) yield* member.writeAcknowledged(live);
      }
      steps.push({ node, stop, observed });
    }

    const finished = yield* releases(ctx, nodes);
    yield* checkNodeReleases(
      Object.fromEntries(nodes.map((node) => [node, CURRENT_RELEASE])),
      finished,
    );
    // A rolling upgrade that never ran two releases at once proves nothing
    // about mixed-version compatibility.
    yield* equal(mixedObserved, true);

    const after = yield* fleet.ownership();
    yield* artifacts.json("binary-upgrade.json", {
      from: { release: PREVIOUS_RELEASE, image: PREVIOUS_IMAGE },
      to: { release: CURRENT_RELEASE, image: CURRENT_IMAGE },
      started,
      steps,
      finished,
      ownersBefore: ownersOf(before, members),
      ownersAfter: ownersOf(after, members),
    });
    yield* verifyAll(members, nodes);
    return { from: PREVIOUS_RELEASE, to: CURRENT_RELEASE, steps: steps.length };
  });

export const operationsCases = [
  {
    id: "operations.weighted-placement" as const,
    operations: true,
    balancing: true,
    run: weightedPlacement,
  },
  {
    id: "operations.rebalance-control" as const,
    operations: true,
    balancing: true,
    run: rebalanceControl,
  },
  {
    id: "operations.graceful-drain" as const,
    operations: true,
    run: gracefulDrain,
  },
  {
    id: "operations.concurrent-drain" as const,
    operations: true,
    run: concurrentDrain,
  },
  {
    id: "operations.preserve-reload" as const,
    operations: true,
    run: preserveReload,
  },
  {
    id: "operations.binary-upgrade" as const,
    durability: "bucket" as const,
    operations: true,
    upgrade: true,
    images: {
      TCK_IMAGE_CELLD: PREVIOUS_IMAGE,
      TCK_IMAGE_CELLD2: PREVIOUS_IMAGE,
      TCK_IMAGE_CELLD3: PREVIOUS_IMAGE,
    },
    run: binaryUpgrade,
  },
];
