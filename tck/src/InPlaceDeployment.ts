import { TckError, toTckError } from "./Domain.js";
import { Effect, Fiber, FileSystem, Schema } from "effect";
import { decodeAs, decodeJson } from "./Artifacts.js";
import { sha256 } from "./Build.js";
import { inspectService, mcCat } from "./Compose.js";
import { Processes } from "./Processes.js";
import { equal } from "./Oracle.js";
import {
  adoptedRevision,
  checkForcedAdoption,
  checkLifecycleAdoption,
  checkModuleMismatch,
  checkSocketPreservation,
} from "./QualificationOracles.js";
import {
  acknowledgedBatch,
  type QualificationContext,
} from "./QualificationContext.js";
import type { Node } from "./FleetControls.js";

const ProcessIdentity = Schema.Struct({
  Id: Schema.String,
  RestartCount: Schema.Int,
  State: Schema.Struct({
    Running: Schema.Literal(true),
    Pid: Schema.Int,
    StartedAt: Schema.String,
  }),
});
const Revision = Schema.Struct({ revision: Schema.String });
const Reload = Schema.Struct({ status: Schema.Int, body: Schema.String });
const DeploymentIdentity = Schema.Struct({
  worker: Schema.String,
  version: Schema.String,
});
const NodeState = Schema.Struct({
  deployment: Schema.Struct({
    generation: Schema.Int,
    version: Schema.String,
    swapping: Schema.Int,
    cells: Schema.Record(Schema.String, Schema.Int),
    draining: Schema.Array(Schema.Unknown),
  }),
});
const Manifest = Schema.Struct({
  modules: Schema.Array(
    Schema.Struct({
      name: Schema.String,
      bytes: Schema.Int,
      sha256: Schema.String,
    }),
  ),
});
export const checkReload = (value: unknown, valid: boolean) =>
  Effect.gen(function* () {
    const result = yield* decodeAs(Reload, "assertion")(value);
    const body = yield* decodeJson(
      Schema.Struct({
        ok: Schema.Boolean,
        outcome: Schema.String,
        error: Schema.optional(Schema.String),
      }),
      result.body,
    ).pipe(Effect.mapError(toTckError("assertion")));
    yield* equal(result.status, valid ? 200 : 422);
    yield* equal(body.ok, valid);
    yield* equal(body.outcome, valid ? "adopted" : "failed");
    if (!valid)
      yield* equal(body.error?.includes("tck-invalid-deployment"), true);
  });
export const checkAdoption = (value: unknown, revision: string) =>
  Effect.gen(function* () {
    const result = yield* decodeAs(
      Schema.Struct({
        before: ProcessIdentity,
        after: ProcessIdentity,
        worker: Revision,
        object: Revision,
        service: Revision,
      }),
      "assertion",
    )(value);
    yield* equal(result.before, result.after);
    yield* equal(result.worker.revision, revision);
    yield* equal(result.object.revision, revision);
    yield* equal(result.service.revision, revision);
  });
const identity = (ctx: QualificationContext, node: Node) =>
  Effect.gen(function* () {
    const processes = yield* Processes;
    const raw = yield* inspectService(
      ctx.runtime.controls.compose,
      processes,
      node,
    );
    const records = yield* decodeJson(Schema.Array(ProcessIdentity), raw);
    yield* equal(records.length, 1);
    return records[0]!;
  });
const operator = (
  ctx: QualificationContext,
  node: Node,
  path: string,
  method: "GET" | "POST",
) =>
  Effect.gen(function* () {
    // Node's fetch is confined to this sidecar boundary, which reaches the
    // private listener. The operator API sends a complete buffered response
    // before closing, so nothing here depends on an early-close observation.
    const output = yield* ctx.runtime.controls.compose([
      "exec",
      "-T",
      "proxy",
      "node",
      "--input-type=module",
      "-e",
      'const r = await fetch("http://" + process.argv[1] + ":8081" + process.argv[2], { method: process.argv[3], signal: AbortSignal.timeout(60000) }); console.log(JSON.stringify({ status: r.status, body: await r.text() }));',
      node,
      path,
      method,
    ]);
    return yield* decodeJson(Reload, output.stdout);
  });
const reload = (ctx: QualificationContext, node: Node) =>
  operator(ctx, node, "/reload", "POST");
const nodeState = (ctx: QualificationContext, node: Node) =>
  Effect.gen(function* () {
    const response = yield* operator(ctx, node, "/state", "GET");
    yield* equal(response.status, 200);
    yield* ctx.artifacts.text(`reload-state-${node}.json`, response.body);
    return yield* decodeJson(NodeState, response.body);
  });
const publish = (ctx: QualificationContext, invalid: boolean) =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const path = `${ctx.artifacts.directory}/fixture/worker.js`;
    const source = yield* fs.readFileString(path);
    yield* equal(source.includes("qualification-v1"), true);
    const replacement =
      (invalid ? 'throw new Error("tck-invalid-deployment");\n' : "") +
      source.replaceAll("qualification-v1", "qualification-v2");
    yield* ctx.artifacts.json("reload-manifest.json", {
      initialSha256: sha256(source),
      replacementSha256: sha256(replacement),
      invalid,
    });
    return yield* Effect.acquireUseRelease(
      fs.writeFileString(path, replacement),
      () =>
        ctx.runtime.controls.deploy().pipe(
          Effect.tap((result) =>
            ctx.artifacts.text("reload-deployment.json", result.stdout),
          ),
          Effect.flatMap((result) =>
            decodeJson(DeploymentIdentity, result.stdout),
          ),
        ),
      () => fs.writeFileString(path, source).pipe(Effect.orDie),
    );
  });
const run = (ctx: QualificationContext, invalid: boolean) =>
  Effect.gen(function* () {
    yield* acknowledgedBatch(ctx, 12);
    const before = yield* Effect.forEach(ctx.nodes, (node) =>
      identity(ctx, node),
    );
    for (const node of ctx.nodes) {
      yield* equal(yield* ctx.json("/deployment/revision", node), {
        revision: "qualification-v1",
      });
      const object = yield* decodeAs(
        Revision,
        "assertion",
      )(yield* ctx.json("/fleet/id", node));
      yield* equal(object.revision, "qualification-v1");
    }
    yield* publish(ctx, invalid);
    // The pointer is published, but no node may adopt before the explicit trigger.
    for (const node of ctx.nodes)
      yield* equal(yield* ctx.json("/deployment/revision", node), {
        revision: "qualification-v1",
      });
    const reloads = [];
    for (const node of ctx.nodes) {
      const response = yield* reload(ctx, node);
      reloads.push({ node, ...response });
      yield* ctx.artifacts.json("reload-responses.json", reloads);
      yield* checkReload(response, !invalid);
    }
    const revision = invalid ? "qualification-v1" : "qualification-v2";
    const observations = [];
    for (const [index, node] of ctx.nodes.entries()) {
      const observation = {
        node,
        before: before[index],
        after: yield* identity(ctx, node),
        worker: yield* ctx.json("/deployment/revision", node),
        object: yield* ctx.json("/fleet/id", node),
        service: yield* ctx.json("/service", node),
      };
      observations.push(observation);
      yield* ctx.artifacts.json("reload-adoption.json", observations);
      yield* checkAdoption(observation, revision);
      yield* ctx.verify(node);
    }
    // Rejection must retain a writable application, not just a cached response.
    yield* acknowledgedBatch(ctx, 6);
    for (const node of ctx.nodes) yield* ctx.verify(node);
    return { reloads, observations, history: yield* ctx.verify() };
  });
// --- Lifecycle transitions -------------------------------------------------
//
// These scenarios only collect observations. Every semantic decision lives in
// the pure oracles in QualificationOracles.ts.

/** A request whose bound exceeds the transport's default ten-second timeout. */
const slowJson = (
  ctx: QualificationContext,
  path: string,
  timeoutMs: number,
  node: Node = "celld",
) =>
  ctx.transport
    .request(ctx.targets[node], {
      path: `${path}${path.includes("?") ? "&" : "?"}name=${ctx.name}`,
      timeoutMs,
    })
    .pipe(
      Effect.tap((response) => equal(response.status, 200)),
      Effect.map((response) => response.body),
    );
const revisionOf = (
  ctx: QualificationContext,
  path: string,
  node: Node = "celld",
) =>
  ctx.json(path, node).pipe(
    Effect.flatMap(decodeAs(Revision, "assertion")),
    Effect.map((value) => value.revision),
  );
const everyNode = (ctx: QualificationContext, path: string) =>
  Effect.forEach(ctx.nodes, (node) => revisionOf(ctx, path, node));
const reloadAll = (ctx: QualificationContext, valid: boolean) =>
  Effect.gen(function* () {
    const responses: Array<{ node: Node; status: number; body: string }> = [];
    for (const node of ctx.nodes) {
      const response = yield* reload(ctx, node);
      responses.push({ node, ...response });
      yield* ctx.artifacts.json("reload-responses.json", responses);
      yield* checkReload(response, valid);
    }
    return responses;
  });

interface Exchange {
  readonly counter: number;
  readonly activation: string;
  readonly revision: string;
  readonly message: string;
}
interface Closed {
  readonly code: number;
  readonly reason: string;
  readonly at: number;
}
const ExchangeSchema = Schema.Struct({
  counter: Schema.Int,
  activation: Schema.String,
  revision: Schema.String,
  message: Schema.String,
});
/** A scoped client socket that records its own close frame. */
const connect = (ctx: QualificationContext, path: string) =>
  Effect.gen(function* () {
    const socket = yield* Effect.acquireRelease(
      Effect.sync(
        () =>
          new WebSocket(
            `${ctx.targets.celld.baseUrl.replace("http:", "ws:")}${path}?name=${ctx.name}`,
          ),
      ),
      (socket) => Effect.sync(() => socket.close()),
    );
    let closed: Closed | null = null;
    socket.addEventListener("close", (event) => {
      closed ??= {
        code: event.code,
        reason: event.reason,
        at: Date.now(),
      };
    });
    yield* Effect.callback<void, TckError>((resume) => {
      if (socket.readyState === WebSocket.OPEN) return resume(Effect.void);
      socket.onopen = () => resume(Effect.void);
      socket.onerror = () =>
        resume(
          Effect.fail(
            new TckError({ phase: "websocket", message: "Connection failed" }),
          ),
        );
    }).pipe(Effect.timeout("20 seconds"));
    const exchange = (message: string) =>
      Effect.callback<unknown, TckError>((resume) => {
        socket.onmessage = (event) =>
          resume(
            decodeAs(
              Schema.fromJsonString(Schema.Unknown),
              "decode",
            )(String(event.data)),
          );
        socket.onerror = () =>
          resume(
            Effect.fail(
              new TckError({ phase: "websocket", message: "Socket failed" }),
            ),
          );
        socket.onclose = () =>
          resume(
            Effect.fail(
              new TckError({
                phase: "websocket",
                message: "Socket closed during exchange",
              }),
            ),
          );
        socket.send(message);
        return Effect.sync(() => {
          socket.onmessage = null;
          socket.onclose = null;
          socket.onerror = null;
        });
      }).pipe(
        Effect.timeout("25 seconds"),
        Effect.flatMap(decodeAs(ExchangeSchema, "assertion")),
      );
    return {
      exchange: exchange as (
        message: string,
      ) => Effect.Effect<Exchange, TckError>,
      closed: () => closed,
      open: () => socket.readyState === WebSocket.OPEN,
      awaitClose: () =>
        ctx
          .poll(
            Effect.sync(() => closed),
            (value) => value !== null,
          )
          .pipe(Effect.map((value) => value!)),
    };
  });
const relative = (close: Closed, startedAt: number) => ({
  code: close.code,
  reason: close.reason,
  afterMs: close.at - startedAt,
});

/** Roadmap 2.3: adoption with in-flight requests, alarms, and pending durability. */
const lifecycleAdoption = (ctx: QualificationContext) =>
  Effect.scoped(
    Effect.gen(function* () {
      // A request that arrives while the object moves waits for the new code,
      // so the hold must stay inside the driver's ten-second request bound.
      const holdMs = 6_000;
      yield* acknowledgedBatch(ctx, 12);
      const before = yield* Effect.forEach(ctx.nodes, (node) =>
        identity(ctx, node),
      );
      const workerBefore = yield* everyNode(ctx, "/deployment/revision");
      // Resolve the owner before the trigger: rebalancing is disabled for
      // qualification, and the lease read must not delay the state sample.
      const owner = (yield* ctx.owner()).node;
      const cell = `Recovery:${ctx.identity.cell}`;
      yield* publish(ctx, false);
      // Arm an alarm, hold one request open, and leave a durability barrier
      // outstanding, so the resident object cannot reach a safe point.
      yield* ctx.json("/alarm/arm?ms=2000&hold=1500", "celld");
      yield* Effect.addFinalizer(() =>
        ctx.mode("normal", 0).pipe(Effect.orDie),
      );
      // Scope the injected latency to this cell's storage path. A fleet-wide
      // mode would also slow the module download that `/reload` performs, and
      // the trigger must land while the held request is still open.
      const injection = yield* ctx.transport.request(ctx.proxy, {
        path: `/mode?mode=latency&ms=15000&pathContains=${encodeURIComponent(ctx.identity.cell)}`,
      });
      yield* equal(injection.status, 200);
      const held = yield* slowJson(ctx, `/hold?ms=${holdMs}`, 60_000).pipe(
        Effect.forkScoped,
      );
      const pending = yield* slowJson(
        ctx,
        "/durability/pending-sync",
        60_000,
      ).pipe(Effect.forkScoped);
      yield* Effect.sleep("500 millis");
      const reloadedAt = Date.now();
      const reloads = yield* reloadAll(ctx, true);
      // Sample the owner first: the object must still run the previous
      // generation while the request, alarm, and barrier are open.
      const during = yield* nodeState(ctx, owner);
      const duringAtMs = Date.now() - reloadedAt;
      // Acknowledged writes issued after the trigger wait for the move and
      // must still be durable once it completes.
      const writes = yield* acknowledgedBatch(ctx, 9).pipe(Effect.forkScoped);
      const workerAfter = yield* everyNode(ctx, "/deployment/revision");
      const moved = yield* Effect.forEach([0, 1], (index) =>
        Effect.sleep("2 seconds").pipe(
          Effect.andThen(nodeState(ctx, owner)),
          Effect.map((state) => ({
            index,
            atMs: Date.now() - reloadedAt,
            generation: state.deployment.generation,
            swapping: state.deployment.swapping,
            objectGeneration: state.deployment.cells[cell] ?? -1,
          })),
        ),
      );
      const inFlight = yield* Fiber.join(held);
      const pendingSync = yield* Fiber.join(pending);
      yield* Fiber.join(writes);
      yield* ctx.mode("normal", 0);
      const objectAfter = yield* ctx.poll(
        revisionOf(ctx, "/fleet/id"),
        (value) => value === adoptedRevision,
      );
      const alarm = yield* ctx.json("/alarm/state");
      const acknowledged = (yield* ctx.ledger.read()).filter(
        (event) => event.kind === "ack" && event.at >= reloadedAt,
      ).length;
      const observation = {
        workerBefore,
        workerAfter,
        objectGenerationDuring: during.deployment.cells[cell] ?? -1,
        deploymentGeneration: during.deployment.generation,
        objectAfter,
        inFlight,
        alarm,
        pendingSync,
        acknowledgedDuringAdoption: acknowledged,
      };
      yield* ctx.artifacts.json("reload-in-flight.json", {
        ...observation,
        duringAtMs,
        moved,
        reloads,
        state: during,
      });
      yield* checkLifecycleAdoption(observation);
      for (const [index, node] of ctx.nodes.entries()) {
        yield* checkAdoption(
          {
            before: before[index],
            after: yield* identity(ctx, node),
            worker: { revision: workerAfter[index]! },
            object: { revision: objectAfter },
            service: yield* ctx.json("/service", node),
          },
          adoptedRevision,
        );
        yield* ctx.verify(node);
      }
      yield* acknowledgedBatch(ctx, 6);
      for (const node of ctx.nodes) yield* ctx.verify(node);
      return { ...observation, history: yield* ctx.verify() };
    }),
  );

/** Roadmap 2.4: storage and hibernatable WebSocket preservation at a safe point. */
const socketPreservation = (ctx: QualificationContext) =>
  Effect.scoped(
    Effect.gen(function* () {
      yield* acknowledgedBatch(ctx, 12);
      const socket = yield* connect(ctx, "/socket");
      const before = yield* socket.exchange("before-adoption");
      const rowsBefore = (yield* ctx.state()).length;
      const identities = yield* Effect.forEach(ctx.nodes, (node) =>
        identity(ctx, node),
      );
      yield* publish(ctx, false);
      const reloads = yield* reloadAll(ctx, true);
      // Nothing blocks a safe point, so the object moves on its own.
      const objectAfter = yield* ctx.poll(
        revisionOf(ctx, "/fleet/id"),
        (value) => value === adoptedRevision,
      );
      const after = yield* socket.exchange("after-adoption");
      yield* acknowledgedBatch(ctx, 6);
      const rowsAfter = (yield* ctx.state()).length;
      const observation = {
        before,
        after,
        closed: socket.closed(),
        open: socket.open(),
        objectAfter,
        rowsBefore,
        rowsAfter,
      };
      yield* ctx.artifacts.json("reload-socket.json", {
        ...observation,
        reloads,
      });
      yield* checkSocketPreservation(observation);
      for (const [index, node] of ctx.nodes.entries()) {
        yield* equal(yield* identity(ctx, node), identities[index]);
        yield* ctx.verify(node);
      }
      return { ...observation, history: yield* ctx.verify() };
    }),
  );

/** Roadmap 2.5: forced adoption after the configured deadline. */
const forcedAdoption = (ctx: QualificationContext) =>
  Effect.scoped(
    Effect.gen(function* () {
      // Matches infra/adoption-deadline.yaml.
      const deadlineMs = 20_000;
      yield* acknowledgedBatch(ctx, 12);
      const hibernatable = yield* connect(ctx, "/socket");
      const hibernatableBefore = yield* hibernatable.exchange("before-forced");
      // A regular WebSocket is the documented safe-point blocker.
      const regular = yield* connect(ctx, "/socket/regular");
      const regularBefore = yield* regular.exchange("before-forced");
      const identities = yield* Effect.forEach(ctx.nodes, (node) =>
        identity(ctx, node),
      );
      yield* publish(ctx, false);
      const startedAt = Date.now();
      const reloads = yield* reloadAll(ctx, true);
      const workerAfter = yield* everyNode(ctx, "/deployment/revision");
      const regularClose = relative(yield* regular.awaitClose(), startedAt);
      const objectAfter = yield* ctx.poll(
        revisionOf(ctx, "/fleet/id"),
        (value) => value === adoptedRevision,
      );
      const hibernatableAfter = yield* hibernatable.exchange("after-forced");
      const reconnected = yield* connect(ctx, "/socket/regular");
      const reconnect = yield* reconnected.exchange("after-reconnect");
      const observation = {
        deadlineMs,
        regularBefore,
        regularClose,
        hibernatableBefore,
        hibernatableAfter,
        hibernatableClosed: hibernatable.closed(),
        reconnect,
        workerAfter,
        objectAfter,
      };
      yield* ctx.artifacts.json("reload-forced.json", {
        ...observation,
        reloads,
      });
      yield* checkForcedAdoption(observation);
      yield* acknowledgedBatch(ctx, 6);
      for (const [index, node] of ctx.nodes.entries()) {
        yield* equal(yield* identity(ctx, node), identities[index]);
        yield* ctx.verify(node);
      }
      return { ...observation, history: yield* ctx.verify() };
    }),
  );

/** Roadmap 2.6: reject module bytes that do not match the deployment manifest. */
const moduleMismatch = (ctx: QualificationContext) =>
  Effect.scoped(
    Effect.gen(function* () {
      const module = "index.js";
      yield* acknowledgedBatch(ctx, 12);
      const identities = yield* Effect.forEach(ctx.nodes, (node) =>
        identity(ctx, node),
      );
      const deployment = yield* publish(ctx, false);
      const prefix = `local/tck/deploy/${deployment.worker}/${deployment.version}`;
      const manifest = yield* decodeJson(
        Manifest,
        (yield* mcCat(ctx.runtime.controls.compose, `${prefix}/manifest.json`))
          .stdout,
      );
      const declared = manifest.modules.find((entry) => entry.name === module);
      if (!declared)
        return yield* Effect.fail(
          new TckError({
            phase: "deployment",
            message: `Manifest does not declare ${module}`,
          }),
        );
      // Replace bytes in place so the object keeps its published length: only a
      // content digest can reject the result.
      const tampered = yield* ctx.runtime.controls.compose([
        "run",
        "--rm",
        "-T",
        "--entrypoint",
        "/bin/sh",
        "storage",
        "-ec",
        [
          'mc cat "$1" > /tmp/original',
          "printf '/*tck-tampered*/' > /tmp/marker",
          "SIZE=$(wc -c < /tmp/original); MARK=$(wc -c < /tmp/marker)",
          "head -c 1024 /tmp/original > /tmp/replacement",
          "cat /tmp/marker >> /tmp/replacement",
          "tail -c $((SIZE - 1024 - MARK)) /tmp/original >> /tmp/replacement",
          'mc pipe "$1" < /tmp/replacement > /dev/null 2>&1',
          'mc cat "$1" > /tmp/stored',
          'printf \'%s %s %s %s\\n\' "$SIZE" "$(sha256sum < /tmp/original | cut -d\' \' -f1)" "$(wc -c < /tmp/stored)" "$(sha256sum < /tmp/stored | cut -d\' \' -f1)"',
        ].join("\n"),
        "tamper",
        `${prefix}/${module}`,
      ]);
      yield* ctx.artifacts.text("reload-tamper.txt", tampered.stdout);
      const fields = tampered.stdout.trim().split("\n").at(-1)!.split(/\s+/);
      if (fields.length !== 4)
        return yield* Effect.fail(
          new TckError({
            phase: "deployment",
            message: `Unexpected tamper report: ${tampered.stdout}`,
          }),
        );
      const reloads = [];
      for (const node of ctx.nodes) {
        const response = yield* reload(ctx, node);
        reloads.push({ node, ...response });
        yield* ctx.artifacts.json("reload-responses.json", reloads);
      }
      const observation = {
        module,
        manifestDigest: declared.sha256,
        bytesBefore: Number(fields[0]),
        digestBefore: fields[1]!,
        bytesAfter: Number(fields[2]),
        digestAfter: fields[3]!,
        reloads,
        worker: yield* everyNode(ctx, "/deployment/revision"),
        object: yield* everyNode(ctx, "/fleet/id"),
        service: yield* everyNode(ctx, "/service"),
      };
      yield* ctx.artifacts.json("reload-module-bytes.json", observation);
      yield* checkModuleMismatch(observation);
      // Rejection must retain a writable application, not just a cached response.
      yield* acknowledgedBatch(ctx, 6);
      for (const [index, node] of ctx.nodes.entries()) {
        yield* equal(yield* identity(ctx, node), identities[index]);
        yield* ctx.verify(node);
      }
      return { ...observation, history: yield* ctx.verify() };
    }),
  );

export const inPlaceDeploymentCases = [
  {
    id: "faults.reload-adoption",
    manualReload: true,
    run: (ctx: QualificationContext) => run(ctx, false),
  },
  {
    id: "faults.reload-invalid",
    manualReload: true,
    run: (ctx: QualificationContext) => run(ctx, true),
  },
  {
    id: "faults.reload-in-flight",
    manualReload: true,
    // Bucket durability puts the injected storage latency on the barrier path.
    durability: "bucket" as const,
    run: lifecycleAdoption,
  },
  {
    id: "faults.reload-socket",
    manualReload: true,
    run: socketPreservation,
  },
  {
    id: "faults.reload-forced",
    manualReload: true,
    adoptionDeadline: true,
    run: forcedAdoption,
  },
  {
    id: "faults.reload-module-bytes",
    manualReload: true,
    run: moduleMismatch,
  },
];
