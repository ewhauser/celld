// Pure oracles for the fleet-operations suite.
//
// Every observation these classify is produced by the orchestration in
// QualificationOperations.ts: ownership records read from the fleet bucket,
// node state read from celld's operator listener through the sidecar probe,
// readiness sampled on the public listener, and celld's own drain log.
// Keeping the classification here is what makes a deliberately wrong
// observation — a cell that stayed on a draining node, a request accepted and
// then lost, a readiness flip in the wrong order, a placement that ignores the
// node weights, a lost acknowledged write — testable without a fleet.
//
// The contracts are the ones celld v0.5.0 publishes; docs/FLEET-OPERATIONS.md
// records the inventory they rest on.
import { Effect, Schema } from "effect";
import { TckError } from "./Domain.js";

const refuse = (message: string, detail?: string) =>
  Effect.fail(
    new TckError({
      phase: "operations",
      message,
      ...(detail === undefined ? {} : { detail }),
    }),
  );

/** An ownership record as the bucket carries it; a released cell has no node. */
export interface Ownership {
  readonly node: string;
  readonly epoch: number;
}
/** Cell identity to its ownership record. */
export type OwnershipMap = Readonly<Record<string, Ownership>>;

const describe = (owners: OwnershipMap) =>
  JSON.stringify(
    Object.fromEntries(
      Object.entries(owners).map(([cell, owner]) => [
        cell.slice(0, 12),
        `${owner.node || "(released)"}@${owner.epoch}`,
      ]),
    ),
  );

// ---------------------------------------------------------------------------
// Weighted ownership placement
// ---------------------------------------------------------------------------

/**
 * "Each node has an ownership target: the fleet's owned cells, divided in
 * proportion to the node weights."
 */
export const placementTarget = (
  weights: Readonly<Record<string, number>>,
  node: string,
  ownedTotal: number,
) =>
  (ownedTotal * weights[node]!) /
  Object.values(weights).reduce((a, b) => a + b, 0);

/**
 * The settled distribution, expressed as an upper bound per node.
 *
 * A donor hands over the whole cells by which it exceeds its target, and hands
 * over no more than the receivers have room for, where a receiver fills only
 * to 2% below its own target. Both roundings are downwards and both are for
 * whole cells, so a settled fleet can leave a node holding close to two whole
 * cells above its target and no more. Observed on celld v0.5.0: 20/1/3 at 24
 * cells and 21/1/3 at 25 cells, against targets of 19.2/2.4/2.4 and 20/2.5/2.5.
 *
 * With a fixed fleet-wide total this one bound pins the distribution: a lightly
 * weighted node cannot absorb the difference, so an even split and a one-node
 * pile-up both fail.
 */
const TOLERANCE = 2;
export const overTarget = (
  weights: Readonly<Record<string, number>>,
  owned: Readonly<Record<string, number>>,
): readonly string[] => {
  const total = Object.values(owned).reduce((a, b) => a + b, 0);
  return Object.keys(owned).filter(
    (node) => owned[node]! >= placementTarget(weights, node, total) + TOLERANCE,
  );
};

export const checkWeightedPlacement = (
  weights: Readonly<Record<string, number>>,
  owned: Readonly<Record<string, number>>,
): Effect.Effect<void, TckError> =>
  Effect.gen(function* () {
    const nodes = Object.keys(owned);
    if (nodes.length < 2)
      return yield* refuse(
        "A weighted-placement oracle needs at least two nodes",
        JSON.stringify(owned),
      );
    for (const node of nodes) {
      const weight = weights[node];
      if (weight === undefined || !Number.isInteger(weight) || weight <= 0)
        return yield* refuse(
          `No usable placement weight for ${node}`,
          JSON.stringify(weights),
        );
      if (!Number.isInteger(owned[node]!) || owned[node]! < 0)
        return yield* refuse(
          `Unusable owned-cell count for ${node}`,
          JSON.stringify(owned),
        );
    }
    const total = Object.values(owned).reduce((a, b) => a + b, 0);
    // An empty fleet satisfies every weighting, so it is never evidence.
    if (total === 0)
      return yield* refuse("No owned cells to place", JSON.stringify(owned));
    const over = overTarget(weights, owned);
    if (over.length > 0)
      return yield* refuse(
        `Ownership ignores the node weights: ${over.join(", ")} above target`,
        `weights ${JSON.stringify(weights)} owned ${JSON.stringify(owned)}`,
      );
  });

/** Each node must publish the weight it was configured with. */
export const checkPublishedWeights = (
  configured: Readonly<Record<string, number>>,
  published: Readonly<Record<string, number>>,
): Effect.Effect<void, TckError> =>
  Effect.gen(function* () {
    const expected = Object.keys(configured).sort();
    const observed = Object.keys(published).sort();
    if (JSON.stringify(expected) !== JSON.stringify(observed))
      return yield* refuse(
        "Published weights do not cover the configured nodes",
        `${JSON.stringify(observed)} vs ${JSON.stringify(expected)}`,
      );
    for (const node of expected)
      if (published[node] !== configured[node])
        return yield* refuse(
          `${node} publishes placement weight ${published[node]}, configured ${configured[node]}`,
        );
    // Identical weights everywhere cannot distinguish weighted from even.
    if (new Set(Object.values(configured)).size < 2)
      return yield* refuse(
        "A weighted-placement control requires at least two distinct weights",
        JSON.stringify(configured),
      );
  });

// ---------------------------------------------------------------------------
// Ownership movement
// ---------------------------------------------------------------------------

/** While balancing is paused, no ownership record may change at all. */
export const checkNoMoves = (
  label: string,
  before: OwnershipMap,
  after: OwnershipMap,
): Effect.Effect<void, TckError> =>
  Effect.gen(function* () {
    const cells = Object.keys(before);
    if (cells.length === 0)
      return yield* refuse(`No ownership records to compare: ${label}`);
    for (const cell of cells) {
      const was = before[cell]!;
      const now = after[cell];
      if (now === undefined)
        return yield* refuse(
          `Ownership record disappeared while balancing was paused: ${label}`,
          cell,
        );
      if (now.node !== was.node || now.epoch !== was.epoch)
        return yield* refuse(
          `Ownership moved while balancing was paused: ${label}`,
          `${cell} ${was.node}@${was.epoch} -> ${now.node}@${now.epoch}`,
        );
    }
  });

/** A drained node must not still own any cell it was asked to hand off. */
export const checkDrainedAway = (
  node: string,
  before: OwnershipMap,
  after: OwnershipMap,
): Effect.Effect<void, TckError> =>
  Effect.gen(function* () {
    const drained = Object.keys(before).filter(
      (cell) => before[cell]!.node === node,
    );
    // Without a cell on the node before the drain there is nothing to observe.
    if (drained.length === 0)
      return yield* refuse(
        `${node} owned no cell before the drain`,
        describe(before),
      );
    const stayed = drained.filter((cell) => after[cell]?.node === node);
    if (stayed.length > 0)
      return yield* refuse(
        `${stayed.length} cell(s) stayed on the draining node ${node}`,
        describe(
          Object.fromEntries(stayed.map((cell) => [cell, after[cell]!])),
        ),
      );
  });

/**
 * A node whose capacity sample is missing must acquire nothing: the set of
 * cells naming it can only shrink while it is not publishing a sample.
 */
export const checkNoAcquisition = (
  node: string,
  before: OwnershipMap,
  after: OwnershipMap,
): Effect.Effect<void, TckError> =>
  Effect.gen(function* () {
    const cells = Object.keys(after);
    if (cells.length === 0)
      return yield* refuse(`No ownership records to compare for ${node}`);
    const acquired = cells.filter(
      (cell) => after[cell]!.node === node && before[cell]?.node !== node,
    );
    if (acquired.length > 0)
      return yield* refuse(
        `${node} acquired ${acquired.length} cell(s) while its capacity sample was missing`,
        acquired.join(", "),
      );
  });

/**
 * A same-node preserve keeps the ownership records. Every cell the node held
 * must still name that node — neither handed to a peer nor released.
 */
export const checkOwnershipPreserved = (
  node: string,
  before: OwnershipMap,
  after: OwnershipMap,
): Effect.Effect<void, TckError> =>
  Effect.gen(function* () {
    const held = Object.keys(before).filter(
      (cell) => before[cell]!.node === node,
    );
    if (held.length === 0)
      return yield* refuse(
        `${node} owned no cell before the preserve`,
        describe(before),
      );
    const lost = held.filter((cell) => after[cell]?.node !== node);
    if (lost.length > 0)
      return yield* refuse(
        `A same-node preserve released ${lost.length} of ${node}'s ${held.length} cell(s)`,
        describe(
          Object.fromEntries(
            lost.map((cell) => [cell, after[cell] ?? { node: "", epoch: -1 }]),
          ),
        ),
      );
  });

// ---------------------------------------------------------------------------
// Readiness and accepted requests
// ---------------------------------------------------------------------------

export const ReadinessSample = Schema.Struct({
  atMs: Schema.Int,
  healthy: Schema.Boolean,
});
export type ReadinessSample = typeof ReadinessSample.Type;

/**
 * "The `/.well-known/celld/health` path reports the node as unhealthy, so a
 * load balancer stops public routing to it." A drained node therefore goes
 * healthy, then unhealthy, and never back: after the first healthy response
 * readiness is not removed again by fleet state, and this node is leaving.
 */
export const checkReadinessOrder = (
  node: string,
  samples: readonly ReadinessSample[],
): Effect.Effect<void, TckError> =>
  Effect.gen(function* () {
    if (samples.length < 2)
      return yield* refuse(`Too few readiness samples for ${node}`);
    for (let i = 1; i < samples.length; i++)
      if (samples[i]!.atMs < samples[i - 1]!.atMs)
        return yield* refuse(
          `Readiness samples for ${node} are not in time order`,
          JSON.stringify(samples),
        );
    const firstUnhealthy = samples.findIndex((sample) => !sample.healthy);
    if (firstUnhealthy === -1)
      return yield* refuse(
        `${node} never reported unhealthy during its drain`,
        JSON.stringify(samples),
      );
    if (firstUnhealthy === 0)
      return yield* refuse(
        `${node} was already unhealthy before its drain started`,
        JSON.stringify(samples),
      );
    const flipped = samples.slice(firstUnhealthy).filter((s) => s.healthy);
    if (flipped.length > 0)
      return yield* refuse(
        `${node} reported healthy again after it started draining`,
        JSON.stringify(samples),
      );
  });

export interface AcceptedRequest {
  readonly acceptedAtMs: number;
  readonly completedAtMs: number;
  readonly drainStartedAtMs: number;
  readonly status?: number;
  readonly receiptId?: string;
  readonly expectedId: string;
  readonly error?: string;
}

/**
 * "The node finishes the public HTTP requests that it accepted before
 * shutdown." The request has to have been accepted first, still be running
 * when the drain starts, and then complete with its own receipt.
 */
export const checkAcceptedCompleted = (
  observed: AcceptedRequest,
): Effect.Effect<void, TckError> =>
  Effect.gen(function* () {
    if (observed.acceptedAtMs >= observed.drainStartedAtMs)
      return yield* refuse(
        "The request was not accepted before the drain started",
        JSON.stringify(observed),
      );
    if (observed.completedAtMs <= observed.drainStartedAtMs)
      return yield* refuse(
        "The request completed before the drain started, so it did not span it",
        JSON.stringify(observed),
      );
    if (observed.error !== undefined)
      return yield* refuse(
        "An accepted request was lost during the drain",
        observed.error,
      );
    if (observed.status !== 200)
      return yield* refuse(
        `An accepted request was answered ${observed.status} during the drain`,
        JSON.stringify(observed),
      );
    if (observed.receiptId !== observed.expectedId)
      return yield* refuse(
        "The completed request returned another operation's receipt",
        `${observed.receiptId} vs ${observed.expectedId}`,
      );
  });

/** Every acknowledged write must still be readable after the operation. */
export const checkAcknowledgedRetained = (
  label: string,
  acknowledged: readonly string[],
  observed: readonly string[],
): Effect.Effect<void, TckError> =>
  Effect.gen(function* () {
    if (acknowledged.length === 0)
      return yield* refuse(`No acknowledged writes to retain: ${label}`);
    const present = new Set(observed);
    const missing = acknowledged.filter((id) => !present.has(id));
    if (missing.length > 0)
      return yield* refuse(
        `${missing.length} acknowledged write(s) lost: ${label}`,
        missing.slice(0, 16).join(", "),
      );
  });

// ---------------------------------------------------------------------------
// Drain log evidence
// ---------------------------------------------------------------------------

export interface Handoff {
  readonly cell: string;
  readonly successor: string;
  readonly successorEpoch: number;
}
export interface DrainEvidence {
  readonly tokenAcquiredMs?: number;
  readonly tokenWaitedMs?: number;
  readonly tokenReleasedMs?: number;
  readonly handoffs: readonly Handoff[];
  readonly cleanReload: "prepared" | "abandoned" | "none";
}

const ISO = /\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?Z/g;
/**
 * celld's own timestamp, which is the last one before the structured fields:
 * `docker compose logs --timestamps` prepends a second one of its own.
 */
const lineTimeMs = (line: string): number | undefined => {
  const head = line.slice(0, line.indexOf('event="'));
  const found = head.match(ISO);
  const last = found?.at(-1);
  const parsed = last === undefined ? Number.NaN : Date.parse(last);
  return Number.isFinite(parsed) ? parsed : undefined;
};
/** One `key=value` or `key="value"` field of a celld structured log line. */
const field = (line: string, name: string): string | undefined => {
  const match = new RegExp(`\\b${name}=(?:"([^"]*)"|(\\S+))`).exec(line);
  return match ? (match[1] ?? match[2]) : undefined;
};

/** Parses a node's drain out of its own log; asserts nothing. */
export const parseDrainLog = (log: string): DrainEvidence => {
  let tokenAcquiredMs: number | undefined;
  let tokenWaitedMs: number | undefined;
  let tokenReleasedMs: number | undefined;
  let cleanReload: DrainEvidence["cleanReload"] = "none";
  const handoffs: Handoff[] = [];
  for (const line of log.split("\n")) {
    const event = field(line, "event");
    if (event === "drain_token_acquired") {
      tokenAcquiredMs = lineTimeMs(line);
      const waited = Number(field(line, "waited_ms"));
      if (Number.isFinite(waited)) tokenWaitedMs = waited;
    } else if (event === "drain_token_released")
      tokenReleasedMs = lineTimeMs(line);
    else if (event === "cell_handoff_accepted") {
      const cell = field(line, "cell");
      const successor = field(line, "successor");
      const successorEpoch = Number(field(line, "successor_epoch"));
      if (cell && successor && Number.isInteger(successorEpoch))
        handoffs.push({ cell, successor, successorEpoch });
    } else if (event === "clean_reload_prepared") cleanReload = "prepared";
    else if (event === "clean_reload_abandoned") cleanReload = "abandoned";
  }
  return {
    ...(tokenAcquiredMs === undefined ? {} : { tokenAcquiredMs }),
    ...(tokenWaitedMs === undefined ? {} : { tokenWaitedMs }),
    ...(tokenReleasedMs === undefined ? {} : { tokenReleasedMs }),
    handoffs,
    cleanReload,
  };
};

/** A drain must show its own handoffs, to a peer, for the cells it released. */
export const checkHandoffEvidence = (
  node: string,
  evidence: DrainEvidence,
  cells: readonly string[],
): Effect.Effect<void, TckError> =>
  Effect.gen(function* () {
    if (cells.length === 0)
      return yield* refuse(`No cells to hand off from ${node}`);
    const byCell = new Map(
      evidence.handoffs.map((handoff) => [handoff.cell, handoff]),
    );
    for (const cell of cells) {
      const handoff = byCell.get(cell);
      if (handoff === undefined)
        return yield* refuse(
          `${node} logged no handoff for ${cell}`,
          JSON.stringify(evidence.handoffs.slice(0, 8)),
        );
      if (handoff.successor === node)
        return yield* refuse(
          `${node} recorded itself as the successor of ${cell}`,
        );
    }
  });

/**
 * "A draining node claims a fleet drain token before it releases cells, so
 * concurrent donors hand off one node at a time." Two donors therefore hold
 * the token over disjoint intervals.
 */
export const checkSerializedDrains = (
  donors: readonly {
    readonly node: string;
    readonly evidence: DrainEvidence;
  }[],
): Effect.Effect<void, TckError> =>
  Effect.gen(function* () {
    if (donors.length < 2)
      return yield* refuse("Serialization needs at least two donors");
    const holds: { node: string; from: number; to: number }[] = [];
    for (const donor of donors) {
      const { tokenAcquiredMs, tokenReleasedMs } = donor.evidence;
      if (tokenAcquiredMs === undefined || tokenReleasedMs === undefined)
        return yield* refuse(
          `${donor.node} logged no complete drain-token hold`,
          JSON.stringify(donor.evidence),
        );
      if (tokenReleasedMs < tokenAcquiredMs)
        return yield* refuse(
          `${donor.node} released the drain token before it acquired it`,
        );
      holds.push({
        node: donor.node,
        from: tokenAcquiredMs,
        to: tokenReleasedMs,
      });
    }
    holds.sort((a, b) => a.from - b.from);
    for (let i = 1; i < holds.length; i++)
      if (holds[i]!.from < holds[i - 1]!.to)
        return yield* refuse(
          `Concurrent donors held the drain token at the same time: ${holds[i - 1]!.node} and ${holds[i]!.node}`,
          JSON.stringify(holds),
        );
  });

/** The complete process stop must stay inside the configured total bound. */
export const checkBoundedShutdown = (
  node: string,
  elapsedMs: number,
  boundMs: number,
): Effect.Effect<void, TckError> =>
  Effect.gen(function* () {
    if (!Number.isFinite(boundMs) || boundMs <= 0)
      return yield* refuse(
        "A shutdown bound must be positive",
        String(boundMs),
      );
    if (!Number.isFinite(elapsedMs) || elapsedMs < 0)
      return yield* refuse(
        `Unusable shutdown duration for ${node}`,
        String(elapsedMs),
      );
    if (elapsedMs > boundMs)
      return yield* refuse(
        `${node} took ${elapsedMs} ms to stop, past its ${boundMs} ms bound`,
      );
  });

// ---------------------------------------------------------------------------
// Binary versions
// ---------------------------------------------------------------------------

/** Each node must report the release its pinned image carries. */
export const checkNodeReleases = (
  expected: Readonly<Record<string, string>>,
  observed: Readonly<Record<string, string>>,
): Effect.Effect<void, TckError> =>
  Effect.gen(function* () {
    for (const node of Object.keys(expected))
      if (observed[node] !== expected[node])
        return yield* refuse(
          `${node} reports celld ${observed[node]}, expected ${expected[node]}`,
          JSON.stringify(observed),
        );
  });

/** A mixed-version observation that shows one release proves nothing. */
export const checkMixedFleet = (
  observed: Readonly<Record<string, string>>,
): Effect.Effect<void, TckError> =>
  new Set(Object.values(observed)).size < 2
    ? refuse(
        "A mixed-version fleet must run at least two distinct releases",
        JSON.stringify(observed),
      )
    : Effect.void;
