import { Effect, Schema } from "effect";
import { decodeAs, decodeJson } from "./Artifacts.js";
import { TckError, toTckError } from "./Domain.js";
import { equal } from "./Oracle.js";
export const StorageEvent = Schema.Struct({
  mode: Schema.String,
  path: Schema.String,
  method: Schema.String,
  upstreamStatus: Schema.optionalKey(Schema.Int),
  dropped: Schema.optionalKey(Schema.Boolean),
});
export const hasStorageFaultEvidence = (
  events: readonly (typeof StorageEvent.Type)[],
  mode: string,
  cell: string,
) =>
  events.some(
    (event) =>
      event.mode === mode &&
      (event.path.includes(`/cells/Recovery:${cell}/ltx/`) ||
        event.path.includes(`/cells/Recovery%3A${cell}/ltx/`)) &&
      (mode !== "drop-response" ||
        (event.method === "PUT" &&
          event.upstreamStatus === 200 &&
          event.dropped === true)),
  );
export const SocketOutcome = Schema.Struct({
  /** Frames observed before the owner was killed. */
  received: Schema.Int,
  /** Frames observed after the kill; a lost owner cannot keep serving. */
  receivedAfterKill: Schema.Int,
  closed: Schema.Boolean,
  clean: Schema.Boolean,
  code: Schema.Int,
});
/**
 * celld documents that a WebSocket transport cannot move to a new cell owner
 * and that the client must reconnect. Every held socket must therefore end,
 * without a client-initiated close and without serving another frame, and the
 * reconnection must land on a new activation of the same cell.
 */
export const checkSocketFailover = (value: {
  readonly sockets: readonly (typeof SocketOutcome.Type)[];
  readonly beforeActivation: string;
  readonly afterActivation: string;
  readonly reconnectCounter: number;
}) =>
  Effect.gen(function* () {
    if (value.sockets.length === 0)
      return yield* fail("No sockets were held across the ownership change");
    for (const socket of value.sockets) {
      if (socket.received < 1)
        return yield* fail("A held socket never served a frame");
      if (!socket.closed)
        return yield* fail("A held socket survived the loss of its owner");
      if (socket.clean)
        return yield* fail(
          "A held socket reported a clean close after its owner was killed",
        );
      if (socket.receivedAfterKill !== 0)
        return yield* fail("A held socket served a frame after the kill");
      if (socket.code < 1000 || socket.code > 4999)
        return yield* fail(`Invalid close code ${socket.code}`);
    }
    if (value.beforeActivation === value.afterActivation)
      return yield* fail("Reconnection reused the killed activation");
    if (value.reconnectCounter !== 1)
      return yield* fail("Reconnected socket did not start a fresh exchange");
  });
const fail = (message: string) =>
  Effect.fail(
    new TckError({ phase: "qualification", message }),
  ) as Effect.Effect<never, TckError>;
export const checkWorkflowResult = (value: unknown) =>
  Schema.decodeUnknownEffect(
    Schema.Struct({
      status: Schema.Literal("complete"),
      output: Schema.Struct({
        first: Schema.Literal(1),
        last: Schema.Literal(1),
      }),
    }),
  )(value).pipe(Effect.asVoid);

// --- In-place deployment lifecycle ---------------------------------------
//
// Pure oracles for the celld v0.5.0 safe-point contract. The scenarios in
// InPlaceDeployment.ts only collect observations; every semantic decision about
// which deployment served what is made here.
export const previousRevision = "qualification-v1";
export const adoptedRevision = "qualification-v2";
const SocketExchange = Schema.Struct({
  counter: Schema.Int,
  activation: Schema.String,
  revision: Schema.String,
  message: Schema.String,
});
const SocketClose = Schema.Struct({
  code: Schema.Int,
  reason: Schema.String,
  afterMs: Schema.Int,
});
const uniform = (values: readonly string[], expected: string) =>
  values.length > 0 && values.every((value) => value === expected);

export const LifecycleAdoption = Schema.Struct({
  workerBefore: Schema.Array(Schema.String),
  workerAfter: Schema.Array(Schema.String),
  objectGenerationDuring: Schema.Int,
  deploymentGeneration: Schema.Int,
  objectAfter: Schema.String,
  inFlight: Schema.Struct({
    revision: Schema.String,
    requestedMs: Schema.Int,
    heldMs: Schema.Int,
  }),
  alarm: Schema.Struct({
    count: Schema.Int,
    revision: Schema.NullOr(Schema.String),
    pending: Schema.NullOr(Schema.Number),
  }),
  pendingSync: Schema.Struct({
    revision: Schema.String,
    rejected: Schema.Boolean,
    value: Schema.String,
  }),
  acknowledgedDuringAdoption: Schema.Int,
});
/**
 * Adoption with in-flight work: every node serves the new deployment at once,
 * the resident object keeps the previous one until it reaches a safe point, and
 * the request, alarm, and durability barrier that started on the previous
 * deployment all finish there without losing acknowledged writes.
 */
export const checkLifecycleAdoption = (value: unknown) =>
  Effect.gen(function* () {
    const observation = yield* decodeAs(LifecycleAdoption, "assertion")(value);
    yield* equal(observation.workerBefore.length, 3);
    yield* equal(observation.workerAfter.length, 3);
    yield* equal(uniform(observation.workerBefore, previousRevision), true);
    yield* equal(uniform(observation.workerAfter, adoptedRevision), true);
    // The node advanced a generation while the resident object stayed behind
    // it, because a request, an alarm, and a durability barrier were open.
    yield* equal(observation.deploymentGeneration >= 2, true);
    yield* equal(
      observation.objectGenerationDuring,
      observation.deploymentGeneration - 1,
    );
    yield* equal(observation.objectAfter, adoptedRevision);
    // "A request that started on the previous deployment finishes on it."
    yield* equal(observation.inFlight.revision, previousRevision);
    yield* equal(
      observation.inFlight.heldMs >= observation.inFlight.requestedMs,
      true,
    );
    yield* equal(observation.alarm.count, 1);
    yield* equal(observation.alarm.revision, previousRevision);
    yield* equal(observation.alarm.pending, null);
    yield* equal(observation.pendingSync.rejected, false);
    yield* equal(observation.pendingSync.revision, previousRevision);
    yield* equal(observation.pendingSync.value, "pending");
    yield* equal(observation.acknowledgedDuringAdoption > 0, true);
  });

export const SocketPreservation = Schema.Struct({
  before: SocketExchange,
  after: SocketExchange,
  closed: Schema.NullOr(SocketClose),
  open: Schema.Boolean,
  objectAfter: Schema.String,
  rowsBefore: Schema.Int,
  rowsAfter: Schema.Int,
});
/**
 * Safe-point transition: "The move keeps the object's storage, its epoch, and
 * its hibernatable WebSockets." The same socket must report the previous
 * revision before and the adopted revision after, with its attachment intact.
 */
export const checkSocketPreservation = (value: unknown) =>
  Effect.gen(function* () {
    const observation = yield* decodeAs(SocketPreservation, "assertion")(value);
    yield* equal(observation.closed, null);
    yield* equal(observation.open, true);
    yield* equal(observation.before.revision, previousRevision);
    yield* equal(observation.after.revision, adoptedRevision);
    yield* equal(observation.objectAfter, adoptedRevision);
    // Serialized attachment survives the move; a fresh socket would reset it.
    yield* equal(observation.after.counter, observation.before.counter + 1);
    // New code means a new instance, so the activation token must change.
    yield* equal(
      observation.before.activation !== observation.after.activation,
      true,
    );
    yield* equal(observation.rowsBefore > 0, true);
    yield* equal(observation.rowsAfter > observation.rowsBefore, true);
  });

export const ForcedAdoption = Schema.Struct({
  deadlineMs: Schema.Int,
  regularBefore: SocketExchange,
  regularClose: SocketClose,
  hibernatableBefore: SocketExchange,
  hibernatableAfter: SocketExchange,
  hibernatableClosed: Schema.NullOr(SocketClose),
  reconnect: SocketExchange,
  workerAfter: Schema.Array(Schema.String),
  objectAfter: Schema.String,
});
/**
 * Forced adoption: "An object that reaches no safe point in
 * CELLD_DEPLOY_MAX_AGE_S seconds is forced: celld cancels its running work and
 * closes its regular WebSockets with code 1012." A hibernatable socket is not a
 * regular socket and must survive the same transition.
 */
export const checkForcedAdoption = (value: unknown) =>
  Effect.gen(function* () {
    const observation = yield* decodeAs(ForcedAdoption, "assertion")(value);
    yield* equal(observation.regularBefore.revision, previousRevision);
    yield* equal(observation.regularClose.code, 1012);
    // Forced only after the configured deadline, and bounded after it.
    yield* equal(
      observation.regularClose.afterMs >= observation.deadlineMs,
      true,
    );
    yield* equal(
      observation.regularClose.afterMs < observation.deadlineMs + 60_000,
      true,
    );
    yield* equal(observation.hibernatableClosed, null);
    yield* equal(observation.hibernatableBefore.revision, previousRevision);
    yield* equal(observation.hibernatableAfter.revision, adoptedRevision);
    yield* equal(
      observation.hibernatableAfter.counter,
      observation.hibernatableBefore.counter + 1,
    );
    // The client reconnects to the deployment that forced the close.
    yield* equal(observation.reconnect.revision, adoptedRevision);
    yield* equal(observation.reconnect.counter, 1);
    yield* equal(observation.workerAfter.length, 3);
    yield* equal(uniform(observation.workerAfter, adoptedRevision), true);
    yield* equal(observation.objectAfter, adoptedRevision);
  });

const ReloadResponse = Schema.Struct({
  node: Schema.String,
  status: Schema.Int,
  body: Schema.String,
});
export const ModuleMismatch = Schema.Struct({
  module: Schema.String,
  manifestDigest: Schema.String,
  bytesBefore: Schema.Int,
  digestBefore: Schema.String,
  bytesAfter: Schema.Int,
  digestAfter: Schema.String,
  reloads: Schema.Array(ReloadResponse),
  worker: Schema.Array(Schema.String),
  object: Schema.Array(Schema.String),
  service: Schema.Array(Schema.String),
});
/**
 * "A node verifies each module before it builds the deployment, so changed
 * module bytes cannot become active." The tampered upload keeps the published
 * length, so only a content digest can reject it, and the previous deployment
 * must keep serving on every node.
 */
export const checkModuleMismatch = (value: unknown) =>
  Effect.gen(function* () {
    const observation = yield* decodeAs(ModuleMismatch, "assertion")(value);
    // The published bytes matched the manifest, and the replacement keeps that
    // length with a different digest.
    yield* equal(observation.digestBefore, observation.manifestDigest);
    yield* equal(observation.bytesAfter, observation.bytesBefore);
    yield* equal(observation.digestAfter !== observation.digestBefore, true);
    yield* equal(observation.reloads.length, 3);
    for (const response of observation.reloads) {
      yield* equal(response.status, 422);
      const body = yield* decodeJson(
        Schema.Struct({
          ok: Schema.Boolean,
          outcome: Schema.String,
          error: Schema.String,
        }),
        response.body,
      ).pipe(Effect.mapError(toTckError("assertion")));
      yield* equal(body.ok, false);
      yield* equal(body.outcome, "failed");
      yield* equal(body.error.includes(observation.module), true);
      yield* equal(/mismatch/i.test(body.error), true);
      // A same-length replacement cannot be rejected as a length difference.
      yield* equal(/size|length|bytes/i.test(body.error), false);
    }
    for (const revisions of [
      observation.worker,
      observation.object,
      observation.service,
    ]) {
      yield* equal(revisions.length, 3);
      yield* equal(uniform(revisions, previousRevision), true);
    }
  });
