import { inPlaceDeploymentCases } from "./InPlaceDeployment.js";
import { storageDurabilityCases } from "./StorageDurability.js";
import { Effect, Fiber, FileSystem, Schema } from "effect";
import { equal } from "./Oracle.js";
import {
  type QualificationContext,
  acknowledgedBatch,
  traffic,
} from "./QualificationContext.js";
import { sha256 } from "./Build.js";
import {
  hasStorageFaultEvidence,
  StorageEvent,
} from "./QualificationOracles.js";
import { leaseLapse } from "./Domain.js";
const runStorageFault = (id: string, ctx: QualificationContext) =>
  Effect.scoped(
    Effect.gen(function* () {
      const { artifacts } = ctx;

      yield* acknowledgedBatch(ctx, 12);
      const mode =
        id.replace("faults.storage-", "") === "ambiguous"
          ? "drop-response"
          : id.replace("faults.storage-", "");
      yield* ctx.mode(mode, mode === "timeout" ? 12000 : 5000);
      yield* Effect.addFinalizer(() =>
        ctx.mode("normal", 0).pipe(Effect.orDie),
      );
      yield* traffic(ctx, 24);
      yield* ctx.mode("normal", 0);
      const stats = yield* ctx.transport.request(ctx.proxy, {
        path: "/stats",
      });
      yield* artifacts.json(`${id}-proxy.json`, stats.body);
      const observed = yield* Schema.decodeUnknownEffect(
        Schema.Struct({
          events: Schema.Array(StorageEvent),
        }),
      )(stats.body);
      yield* equal(
        hasStorageFaultEvidence(observed.events, mode, ctx.identity.cell),
        true,
      );
      yield* ctx.restartAll();
      yield* acknowledgedBatch(ctx, 4);
      return yield* ctx.verify();
    }),
  );
const partition = (ctx: QualificationContext) =>
  Effect.scoped(
    Effect.gen(function* () {
      const { fleet, artifacts } = ctx;

      yield* acknowledgedBatch(ctx, 24);
      const owner = (yield* ctx.owner()).node;
      const container = yield* fleet.inspect(owner);
      const project = container.Config.Labels["com.docker.compose.project"]!;
      yield* fleet.partitionPeers(owner);
      yield* equal(
        Object.keys((yield* fleet.inspect(owner)).NetworkSettings.Networks),
        [`${project}_store`],
      );
      const probe = yield* ctx.runtime.controls.compose([
        "exec",
        "-T",
        "proxy",
        "node",
        "--input-type=module",
        "-e",
        `try { await fetch("http://peer-${owner}:8081/health", { signal: AbortSignal.timeout(1000) }); process.exitCode = 1; } catch { console.log("peer-unreachable"); }`,
      ]);
      yield* equal(probe.stdout.trim(), "peer-unreachable");
      yield* artifacts.text("peer-partition-proof.txt", probe.stdout);
      yield* traffic(ctx, 40);
      // Restore before reading through all public endpoints. Finalizer owns reconnect.
      return {
        partitioned: owner,
        attempted: (yield* ctx.ledger.read()).filter(
          (event) => event.kind === "intent",
        ).length,
      };
    }),
  );
const rollingDeployment = (ctx: QualificationContext) =>
  Effect.scoped(
    Effect.gen(function* () {
      const { nodes, artifacts } = ctx;

      yield* acknowledgedBatch(ctx, 24);
      const fs = yield* FileSystem.FileSystem;
      const path = `${artifacts.directory}/fixture/worker.js`;
      const source = yield* fs.readFileString(path);
      yield* equal(source.includes("qualification-v1"), true);
      const replacement = source.replaceAll(
        "qualification-v1",
        "qualification-v2",
      );
      yield* artifacts.text("rolling-worker-v2.js", replacement);
      yield* artifacts.json("rolling-manifest.json", {
        initialSha256: sha256(source),
        replacementSha256: sha256(replacement),
      });
      yield* fs.writeFileString(path, replacement);
      const deployed = yield* ctx.runtime.controls
        .deploy()
        .pipe(
          Effect.ensuring(fs.writeFileString(path, source).pipe(Effect.orDie)),
        );
      yield* artifacts.text("rolling-deployment.json", deployed.stdout);
      for (const node of nodes) {
        const fiber = yield* traffic(ctx, 24).pipe(Effect.forkScoped);
        yield* ctx.runtime.controls.compose(["stop", "--timeout", "20", node]);
        yield* ctx.start(node);
        yield* Fiber.join(fiber);
        yield* equal(yield* ctx.json("/service", node), {
          value: ctx.name,
          revision: "qualification-v2",
        });
        yield* ctx.verify(node);
      }
      return yield* ctx.verify();
    }),
  );
const peerPartition = (ctx: QualificationContext) =>
  Effect.gen(function* () {
    const result = yield* partition(ctx);
    const other = ctx.nodes.find((node) => node !== result.partitioned)!;
    yield* ctx.verify(other);
    // Reconnection restores the network; restart restores Docker's public port mapping.
    if ((yield* ctx.fleet.inspect(result.partitioned)).State.Running)
      yield* ctx.fleet.kill(result.partitioned);
    yield* leaseLapse;
    yield* ctx.start(result.partitioned);
    for (const node of ctx.nodes) {
      ctx.targets[node] = yield* ctx.fleet.target(node);
      yield* ctx.verify(node);
    }
    return result;
  });
export const faultCases = [
  ...storageDurabilityCases,
  ...inPlaceDeploymentCases,
  ...(
    [
      "faults.storage-latency",
      "faults.storage-throttle",
      "faults.storage-timeout",
      "faults.storage-ambiguous",
    ] as const
  ).map((id) => ({
    id,
    // Bucket proofs put storage faults on the acknowledgment path.
    durability: "bucket" as const,
    run: (ctx: QualificationContext) => runStorageFault(id, ctx),
  })),
  { id: "faults.peer-partition" as const, run: peerPartition },
  { id: "faults.rolling-deployment" as const, run: rollingDeployment },
];
