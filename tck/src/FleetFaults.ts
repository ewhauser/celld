import { Effect, Schema } from "effect";
import { Artifacts, decodeJson } from "./Artifacts.js";
import { TckError } from "./Domain.js";
import { Processes } from "./Processes.js";
import { cleanupAll } from "./Resources.js";
import type { Node } from "./FleetControls.js";

const Resources = Schema.Struct({
  Id: Schema.String,
  HostConfig: Schema.Struct({
    Memory: Schema.Number,
    MemorySwap: Schema.Number,
  }),
  State: Schema.Struct({ OOMKilled: Schema.Boolean }),
  NetworkSettings: Schema.Struct({
    Networks: Schema.Record(
      Schema.String,
      Schema.Struct({
        Aliases: Schema.NullOr(Schema.Array(Schema.String)),
      }),
    ),
  }),
});

export const fleetFaults = <E>(
  project: string,
  ownedContainer: (node: Node) => Effect.Effect<{ Id: string }, E>,
) =>
  Effect.gen(function* () {
    const processes = yield* Processes;
    const artifacts = yield* Artifacts;
    let sequence = 0;
    const resources = (node: Node) =>
      Effect.gen(function* () {
        const container = yield* ownedContainer(node);
        const raw = yield* processes.run("docker", ["inspect", container.Id]);
        yield* artifacts.text(
          `resources-${++sequence}-${node}.json`,
          raw.stdout,
        );
        const entries = yield* decodeJson(Schema.Array(Resources), raw.stdout);
        if (entries.length !== 1 || entries[0]!.Id !== container.Id)
          return yield* Effect.fail(
            new TckError({
              phase: "fleet",
              message: "Expected the owned node container",
            }),
          );
        return entries[0]!;
      });
    const memoryLimit = (
      node: Node,
      options: {
        bytes: number;
        unlimitedRecovery?: { Memory: number; MemorySwap: number };
      },
    ) =>
      Effect.gen(function* () {
        const before = yield* resources(node);
        // Docker update treats zero as "unchanged", not as restoration to unlimited.
        // Scenarios with an unbounded baseline must name their finite recovery budget.
        const restore =
          before.HostConfig.Memory === 0 || before.HostConfig.MemorySwap === 0
            ? options.unlimitedRecovery
            : before.HostConfig;
        if (!restore || restore.Memory <= 0 || restore.MemorySwap === 0)
          return yield* Effect.fail(
            new TckError({
              phase: "fleet",
              message:
                "Unlimited memory settings require an explicit finite recovery budget",
            }),
          );
        yield* artifacts.json(`memory-recovery-${++sequence}-${node}.json`, {
          before: before.HostConfig,
          restore,
        });
        const update = (memory: number, swap: number) =>
          processes.run("docker", [
            "update",
            "--memory",
            String(memory),
            "--memory-swap",
            String(swap),
            before.Id,
          ]);
        // Install restoration before mutation, including interrupted or partially failed updates.
        yield* Effect.addFinalizer(() =>
          cleanupAll([update(restore.Memory, restore.MemorySwap)]).pipe(
            Effect.orDie,
          ),
        );
        yield* update(options.bytes, options.bytes);
        return restore;
      });
    const partitionPeers = (node: Node) =>
      Effect.gen(function* () {
        const before = yield* resources(node);
        const network = `${project}_default`;
        const connection = before.NetworkSettings.Networks[network];
        if (!connection)
          return yield* Effect.fail(
            new TckError({
              phase: "fleet",
              message: "Node is not connected to the peer network",
            }),
          );
        const restore = Effect.gen(function* () {
          // The captured container ID is already ownership-checked. Restoration must
          // not depend on writing more diagnostics after an artifact failure.
          const raw = yield* processes.run("docker", ["inspect", before.Id]);
          const entries = yield* decodeJson(
            Schema.Array(Resources),
            raw.stdout,
          );
          if (entries.length !== 1 || entries[0]!.Id !== before.Id)
            return yield* Effect.fail(
              new TckError({
                phase: "fleet",
                message: "Cannot restore the owned node container",
              }),
            );
          if (!entries[0]!.NetworkSettings.Networks[network])
            yield* processes.run("docker", [
              "network",
              "connect",
              ...(connection.Aliases ?? []).flatMap((alias) => [
                "--alias",
                alias,
              ]),
              network,
              before.Id,
            ]);
        });
        yield* Effect.addFinalizer(() =>
          cleanupAll([restore]).pipe(Effect.orDie),
        );
        yield* processes.run("docker", [
          "network",
          "disconnect",
          network,
          before.Id,
        ]);
      });
    return { resources, memoryLimit, partitionPeers };
  });
