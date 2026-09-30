import { fleetFaults } from "./FleetFaults.js";
import { Effect, Schema } from "effect";
import { pendingPhase, retryRead } from "./Polling.js";
import { Artifacts, decodeJson } from "./Artifacts.js";
import { Processes } from "./Processes.js";
import { TckError, type Target } from "./Domain.js";
import { DiskContainer, DiskVolume, ownedStateVolume } from "./DiskLoss.js";
import { equal } from "./Oracle.js";
import { cleanupAll } from "./Resources.js";
import { inspectService, mcCat, publishedPort } from "./Compose.js";
export const Node = Schema.Literals(["celld", "celld2", "celld3"]);
export type Node = typeof Node.Type;
export const Owner = Schema.Struct({ node: Node, epoch: Schema.Int });
/**
 * An ownership record as it can actually be observed mid-operation: a released
 * cell keeps its record with an empty node until a successor acquires it, so a
 * drain or a rebalance can be sampled without a decode failure.
 */
export const OwnerRecord = Schema.Struct({
  node: Schema.String,
  epoch: Schema.Int,
});
export type OwnerRecord = typeof OwnerRecord.Type;
/**
 * The load block a node publishes in its lease and repeats in `/state`, as far
 * as the fleet-operations cases read it. celld publishes more fields; decoding
 * only these keeps the schema from breaking on an unrelated addition.
 */
export const NodeLoad = Schema.Struct({
  sampled_ms: Schema.Int,
  owned_cells: Schema.Int,
  placement_weight: Schema.Int,
  rebalance_paused: Schema.Boolean,
  draining: Schema.Boolean,
});
export type NodeLoad = typeof NodeLoad.Type;
export const fleetControls = (
  compose: (
    args: readonly string[],
  ) => Effect.Effect<{ stdout: string; stderr: string }, TckError>,
  project: string,
  base: Target,
) =>
  Effect.gen(function* () {
    const processes = yield* Processes;
    const artifacts = yield* Artifacts;
    let sequence = 0;
    const paused = new Set<Node>();
    const inspect = (node: Node) =>
      Effect.gen(function* () {
        const raw = yield* inspectService(compose, processes, node);
        yield* artifacts.text(`fleet-${++sequence}-${node}.json`, raw);
        const entries = yield* decodeJson(
          Schema.Array(
            Schema.Struct({
              Id: Schema.String,
              // The resolved image ID and the configured reference: a binary
              // upgrade has to prove which image a node is actually running.
              Image: Schema.String,
              Config: Schema.Struct({
                Image: Schema.String,
                Labels: Schema.Record(Schema.String, Schema.String),
              }),
              State: Schema.Struct({
                Running: Schema.Boolean,
                Paused: Schema.Boolean,
                ExitCode: Schema.Int,
              }),
              NetworkSettings: Schema.Struct({
                Networks: Schema.Record(Schema.String, Schema.Unknown),
              }),
            }),
          ),
          raw,
        );
        if (entries.length !== 1)
          return yield* Effect.fail(
            new TckError({
              phase: "fleet",
              message: "Expected one node container",
            }),
          );
        const state = entries[0]!;
        yield* equal(
          state.Config.Labels["com.docker.compose.project"],
          project,
        );
        yield* equal(state.Config.Labels["com.docker.compose.service"], node);
        return state;
      });
    // A node cut off from its lease must exit 3 on its own; wait for that proof.
    const fenced = (node: Node) =>
      Effect.gen(function* () {
        const current = yield* inspect(node);
        if (current.State.Running)
          return yield* Effect.fail(
            new TckError({
              phase: "fence-pending",
              message: "Waiting for lease fence",
            }),
          );
        yield* equal(current.State.ExitCode, 3);
      }).pipe((probe) =>
        retryRead(probe, {
          retryable: pendingPhase("fence-pending"),
          interval: "500 millis",
          attempts: 81,
          timeout: "40 seconds",
        }),
      );
    const target = (node: Node) =>
      Effect.gen(function* () {
        const address = yield* publishedPort(
          compose,
          node,
          "8080",
          "fleet",
          "Invalid public endpoint",
        );
        return { ...base, name: node, baseUrl: `http://${address}` };
      });
    yield* Effect.addFinalizer(() =>
      cleanupAll([...paused].map((node) => compose(["unpause", node]))).pipe(
        Effect.orDie,
      ),
    );
    const faults = yield* fleetFaults(project, inspect);
    return {
      ...faults,
      pause: (node: Node) =>
        Effect.gen(function* () {
          yield* equal((yield* inspect(node)).State.Running, true);
          paused.add(node);
          yield* compose(["pause", node]);
          yield* equal((yield* inspect(node)).State.Paused, true);
        }),
      unpause: (node: Node) =>
        Effect.gen(function* () {
          yield* compose(["unpause", node]);
          paused.delete(node);
          yield* equal((yield* inspect(node)).State.Paused, false);
        }),
      lease: (node: Node) =>
        Effect.gen(function* () {
          const raw = (yield* mcCat(compose, `local/tck/nodes/${node}.json`))
            .stdout;
          yield* artifacts.text(`lease-${++sequence}-${node}.json`, raw);
          return yield* decodeJson(
            Schema.Struct({
              probe_public_key: Schema.String,
              load: Schema.optionalKey(NodeLoad),
              log: Schema.optionalKey(
                Schema.Struct({
                  state: Schema.String,
                  epoch: Schema.Int,
                  active: Schema.Boolean,
                  ensemble: Schema.Array(Node),
                }),
              ),
            }),
            raw,
          );
        }),
      discard: (node: Node) =>
        Effect.gen(function* () {
          const current = yield* inspect(node);
          const raw = (yield* processes.run("docker", ["inspect", current.Id]))
            .stdout;
          const container = (yield* decodeJson(
            Schema.Array(DiskContainer),
            raw,
          ))[0]!;
          const volumesRaw = (yield* processes.run("docker", [
            "volume",
            "inspect",
            `${project}_${node}-state`,
          ])).stdout;
          const volumes = yield* decodeJson(
            Schema.Array(DiskVolume),
            volumesRaw,
          );
          if (volumes.length !== 1)
            return yield* Effect.fail(
              new TckError({
                phase: "lifecycle",
                message: "Expected one state volume",
              }),
            );
          const volume = volumes[0]!;
          const name = yield* ownedStateVolume(
            project,
            container,
            volume,
            node,
          );
          const logs = yield* compose([
            "logs",
            "--no-color",
            "--timestamps",
            node,
          ]);
          yield* artifacts.text(
            `before-discard-${node}.log`,
            logs.stdout + logs.stderr,
          );
          yield* artifacts.text(`discard-${node}-container.json`, raw);
          yield* artifacts.text(`discard-${node}-volume.json`, volumesRaw);
          yield* compose(["rm", "--force", node]);
          yield* processes.run("docker", ["volume", "rm", name]);
          yield* compose(["create", node]);
          const replacement = yield* inspect(node);
          if (replacement.Id === current.Id)
            return yield* Effect.fail(
              new TckError({
                phase: "lifecycle",
                message: "Container was not replaced",
              }),
            );
          // Verify the recreated disk is empty before any node process can use it.
          yield* compose([
            "run",
            "--no-deps",
            "--rm",
            "-T",
            "--volume",
            `${name}:/empty:ro`,
            "--entrypoint",
            "/bin/sh",
            "storage",
            "-ec",
            'entries=$(ls -A /empty); test -z "$entries"',
          ]);
          yield* artifacts.json(`discard-${node}.json`, {
            volume: name,
            emptyBeforeStart: true,
          });
          return {
            volume: name,
            oldContainer: current.Id,
            newContainer: replacement.Id,
          };
        }),
      losses: () =>
        Effect.gen(function* () {
          const found = (yield* compose([
            "run",
            "--rm",
            "-T",
            "--entrypoint",
            "mc",
            "storage",
            "find",
            "local/tck/log",
            "--name",
            "*.loss.json",
          ])).stdout
            .trim()
            .split("\n")
            .filter(Boolean);
          const records: Array<{
            leader: string;
            epoch: number;
            note: string;
          }> = [];
          for (const path of found) {
            if (!/^local\/tck\/log\/[a-zA-Z0-9/_.-]+\.loss\.json$/.test(path))
              return yield* Effect.fail(
                new TckError({
                  phase: "fleet",
                  message: "Invalid loss-record path",
                }),
              );
            const raw = (yield* mcCat(compose, path)).stdout;
            yield* artifacts.text(`loss-record-${++sequence}.json`, raw);
            records.push(
              yield* decodeJson(
                Schema.Struct({
                  leader: Schema.String,
                  epoch: Schema.Int,
                  note: Schema.String,
                }),
                raw,
              ),
            );
          }
          return records;
        }),
      inspect,
      fenced,
      target,
      /**
       * Every ordinary-object ownership record in the fleet bucket, keyed by
       * cell identity. One throwaway container reads the whole set: a fleet
       * operation has to snapshot ownership repeatedly, and one `mc cat` per
       * cell would cost more than the operation under test.
       */
      ownership: () =>
        Effect.gen(function* () {
          const output = yield* compose([
            "run",
            "--rm",
            "-T",
            "--entrypoint",
            "/bin/sh",
            "storage",
            "-ec",
            'mc find local/tck/cells --name own.json | while read -r path; do printf "%s\t" "$path"; mc cat "$path"; printf "\n"; done',
          ]);
          yield* artifacts.text(`ownership-${++sequence}.tsv`, output.stdout);
          const records: Record<string, OwnerRecord> = {};
          for (const line of output.stdout.split("\n")) {
            if (!line.trim()) continue;
            const separator = line.indexOf("\t");
            const path = line.slice(0, separator);
            const cell =
              /^local\/tck\/cells\/Recovery:([a-f0-9]{64})\/own\.json$/.exec(
                path,
              )?.[1];
            // A reserved runtime class also has an ownership record; only the
            // ordinary-object cells this harness creates are addressable here.
            if (!cell) continue;
            records[cell] = yield* decodeJson(
              OwnerRecord,
              line.slice(separator + 1),
            );
          }
          return records as Readonly<Record<string, OwnerRecord>>;
        }),
      owner: (cell: string) =>
        Effect.gen(function* () {
          if (!/^[a-f0-9]{64}$/.test(cell))
            return yield* Effect.fail(
              new TckError({
                phase: "fleet",
                message: "Invalid cell identity",
              }),
            );
          const raw = (yield* mcCat(
            compose,
            `local/tck/cells/Recovery:${cell}/own.json`,
          )).stdout;
          yield* artifacts.text(`owner-${++sequence}.json`, raw);
          return yield* decodeJson(Owner, raw);
        }),
      /**
       * The same record, tolerating the empty node a released cell carries
       * between its donor's release and its successor's acquire.
       */
      ownerRecord: (cell: string) =>
        Effect.gen(function* () {
          if (!/^[a-f0-9]{64}$/.test(cell))
            return yield* Effect.fail(
              new TckError({
                phase: "fleet",
                message: "Invalid cell identity",
              }),
            );
          const raw = (yield* mcCat(
            compose,
            `local/tck/cells/Recovery:${cell}/own.json`,
          )).stdout;
          yield* artifacts.text(`owner-record-${++sequence}.json`, raw);
          return yield* decodeJson(OwnerRecord, raw);
        }),
      /**
       * Starts or replaces one node's container without touching its peers, so
       * a re-pinned image is adopted the way a rolling update adopts it.
       */
      recreate: (node: Node) =>
        Effect.gen(function* () {
          yield* compose(["up", "-d", "--no-deps", node]);
          return yield* target(node);
        }),
      kill: (node: Node) =>
        Effect.gen(function* () {
          yield* equal((yield* inspect(node)).State.Running, true);
          yield* compose(["kill", "--signal", "SIGKILL", node]);
          yield* equal((yield* inspect(node)).State, {
            Running: false,
            Paused: false,
            ExitCode: 137,
          });
        }),
      start: (node: Node) =>
        Effect.gen(function* () {
          yield* compose(["start", node]);
          return yield* target(node);
        }),
      partition: (node: Node) =>
        Effect.gen(function* () {
          const before = yield* inspect(node);
          yield* equal(before.State.Running, true);
          yield* equal(
            Object.keys(before.NetworkSettings.Networks).sort(),
            [`${project}_default`, `${project}_store`].sort(),
          );
          yield* processes.run("docker", [
            "network",
            "disconnect",
            `${project}_store`,
            before.Id,
          ]);
          yield* equal(
            Object.keys((yield* inspect(node)).NetworkSettings.Networks),
            [`${project}_default`],
          );
        }),
      reconnect: (node: Node) =>
        Effect.gen(function* () {
          const current = yield* inspect(node);
          yield* processes.run("docker", [
            "network",
            "connect",
            `${project}_store`,
            current.Id,
          ]);
        }),
      logs: (node: Node) =>
        Effect.gen(function* () {
          const logs = yield* compose([
            "logs",
            "--no-color",
            "--timestamps",
            node,
          ]);
          return logs.stdout + logs.stderr;
        }),
    };
  });
