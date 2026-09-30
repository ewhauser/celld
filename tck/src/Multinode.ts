import { readEventually, waitForReady } from "./Polling.js";
import { Effect, Schema } from "effect";
import { provenance } from "./Provenance.js";
import { Artifacts } from "./Artifacts.js";
import { buildFixtureFor } from "./Build.js";
import {
  attemptRequest,
  leaseLapse,
  Transport,
  TckError,
  type Profile,
} from "./Domain.js";
import { acquireLocal } from "./Local.js";
import { equal } from "./Oracle.js";
import { OutageState, checkOutageState } from "./Outage.js";
import { type Node, type Owner } from "./FleetControls.js";
import { resilienceStages } from "./Resilience.js";
import { makeSuiteExecutor } from "./SuiteExecutor.js";
export const hasFleetProof = (logs: string, cell: string) =>
  logs
    .split("\n")
    .some(
      (line) =>
        line.includes("durable_wait") &&
        line.includes(cell) &&
        /proof="?fleet"?(?:\s|$)/.test(line),
    );
export const hasLogRecovery = (logs: string, node: Node) =>
  logs
    .split("\n")
    .some(
      (line) =>
        line.includes("node log recovered and sealed") &&
        line.match(/\bdead="?([a-z0-9]+)(?:\/[a-f0-9]{64})?"?(?:\s|$)/)?.[1] ===
          node &&
        /entries=[1-9][0-9]*(?:\s|$)/.test(line),
    );
export const checkHandoff = (
  before: typeof Owner.Type,
  after: typeof Owner.Type,
  successor: Node,
) =>
  Effect.gen(function* () {
    yield* equal(after.node, successor);
    if (after.node === before.node || after.epoch <= before.epoch)
      return yield* Effect.fail(
        new TckError({
          phase: "assertion",
          message: "Ownership did not advance to a new node and epoch",
        }),
      );
  });
export const multinodeSuite = (
  durability: "bucket" | "fleet",
  resilience = false,
) =>
  resilience ? "resilience" : durability === "fleet" ? "fleet" : "multinode";
export const multinodeIds = (
  durability: "bucket" | "fleet",
  resilience = false,
) => {
  const prefix = `${multinodeSuite(durability, resilience)}.`;
  return [
    "routing",
    "owner-failover",
    "owner-rejoin",
    "storage-partition",
    ...(durability === "fleet" ? ["follower-recovery"] : []),
    ...(resilience
      ? [
          "paused-owner",
          "interrupted-writes",
          "simultaneous-restart",
          "follower-loss",
          "replica-disk-loss",
        ]
      : []),
  ].map((suffix) => `${prefix}${suffix}`);
};
export const runMultinode = (options: {
  runId: string;
  profile: Profile;
  seed: number;
  caseId: string;
  durability?: "bucket" | "fleet";
  resilience?: boolean;
}) =>
  Effect.gen(function* () {
    if (options.profile !== "local" || options.caseId)
      return yield* Effect.fail(
        new TckError({
          phase: "arguments",
          message:
            "Multinode requires local profile and the complete scenario; omit --case",
        }),
      );
    const durability = options.durability ?? "bucket";
    const nodeCount = options.resilience ? 3 : 2;
    const suite = multinodeSuite(durability, options.resilience ?? false);
    const ids = multinodeIds(durability, options.resilience ?? false);
    const artifacts = yield* Artifacts;
    const transport = yield* Transport;
    const environment: Record<string, unknown> = {
      suite,
      durability,
    };
    const executor = yield* makeSuiteExecutor({
      ...options,
      profile: "local",
      ids,
      environment,
      timeout: options.resilience ? "15 minutes" : "8 minutes",
    });
    const work = Effect.gen(function* () {
      yield* artifacts.json("run.json", {
        ...options,
        suite,
        selected: ids,
      });
      Object.assign(environment, yield* provenance);
      const bundle = yield* buildFixtureFor("recovery");
      const runtime = yield* acquireLocal({
        runId: `${options.runId}-multinode`,
        bundle,
        cleanupError: executor.cleanupError,
        topology: "cluster",
        durability,
        nodeCount,
      });
      environment.candidate = runtime.metadata;
      environment.fixtureSha256 = bundle.sha256;
      const fleet = runtime.fleet;
      const nodes: Node[] = options.resilience
        ? ["celld", "celld2", "celld3"]
        : ["celld", "celld2"];
      const targets = {
        celld: yield* fleet.target("celld"),
        celld2: yield* fleet.target("celld2"),
        celld3: options.resilience
          ? yield* fleet.target("celld3")
          : runtime.target,
      };
      const name = `${options.runId}-shared`;
      const acknowledged: number[] = [];
      const request = (
        node: Node,
        path: string,
        method: "GET" | "POST" = "GET",
      ) =>
        transport.request(targets[node], {
          path: `${path}${path.includes("?") ? "&" : "?"}name=${name}`,
          method,
        });
      const ready = (node: Node) =>
        waitForReady(
          transport.request(targets[node], {
            path: "/.well-known/celld/health",
          }),
          { interval: "500 millis", attempts: 91, timeout: "60 seconds" },
        ).pipe(Effect.asVoid);
      const read = (node: Node) =>
        request(node, "/outage/state").pipe(
          Effect.tap((value) => equal(value.status, 200)),
          Effect.flatMap((value) =>
            Schema.decodeUnknownEffect(OutageState)(value.body),
          ),
        );
      const check = (node: Node) =>
        Effect.gen(function* () {
          const state = yield* read(node);
          yield* artifacts.json(
            `state-${node}-${acknowledged.length}.json`,
            state,
          );
          yield* checkOutageState(
            state,
            [...acknowledged]
              .sort((a, b) => a - b)
              .map((id) => ({ id, acknowledged: true })),
          );
        });
      const write = (node: Node, id: number) =>
        Effect.gen(function* () {
          const result = yield* request(node, `/outage/write?id=${id}`, "POST");
          yield* artifacts.json(`write-${id}.json`, { node, result });
          yield* equal(result.status, 200);
          yield* equal(result.body, { acknowledged: id });
          acknowledged.push(id);
        });
      const stage = (suffix: string, work: Effect.Effect<void, unknown>) =>
        executor.runCase(`${suite}.${suffix}`, () => work, {
          timeout: "150 seconds",
          onFailure: "stop",
        });
      yield* ready("celld");
      yield* ready("celld2");
      if (options.resilience) yield* ready("celld3");
      let cell = "";
      let owner: Node = "celld";
      let survivor: Node = "celld2";
      let original: typeof Owner.Type = { node: owner, epoch: 0 };
      yield* stage(
        "routing",
        Effect.gen(function* () {
          yield* Effect.all([write("celld", 1), write("celld2", 2)], {
            concurrency: 2,
          });
          if (options.resilience) yield* write("celld3", 100);
          for (const node of nodes) yield* check(node);
          const identity = yield* request("celld", "/fleet/id");
          yield* equal(identity.status, 200);
          cell = (yield* Schema.decodeUnknownEffect(
            Schema.Struct({ cell: Schema.String }),
          )(identity.body)).cell;
          original = yield* fleet.owner(cell);
          owner = original.node;
          survivor = owner === "celld" ? "celld2" : "celld";
        }),
      );
      yield* stage(
        "owner-failover",
        Effect.gen(function* () {
          yield* fleet.kill(owner);
          yield* leaseLapse;
          // Cold activation can take time; retry read transport/setup only, then assert data once.
          yield* readEventually(read(survivor));
          yield* check(survivor);
          yield* checkHandoff(original, yield* fleet.owner(cell), survivor);
          yield* write(survivor, 3);
        }),
      );
      yield* stage(
        "owner-rejoin",
        Effect.gen(function* () {
          targets[owner] = yield* fleet.start(owner);
          yield* ready(owner);
          yield* check(owner);
          yield* check(survivor);
          yield* write(owner, 4);
          yield* check(survivor);
        }),
      );
      yield* stage(
        "storage-partition",
        Effect.gen(function* () {
          const prior = yield* fleet.owner(cell);
          const isolated = prior.node;
          const healthy = isolated === "celld" ? "celld2" : "celld";
          yield* fleet.partition(isolated);
          yield* fleet.fenced(isolated);
          const logs = yield* fleet.logs(isolated);
          yield* artifacts.text("partition-node.log", logs);
          yield* equal(logs.includes("node_lease_watchdog_fence"), true);
          const refused = yield* attemptRequest(
            request(isolated, "/outage/write?id=5", "POST"),
          );
          yield* artifacts.json("partition-refused-write.json", refused);
          if ("response" in refused)
            yield* equal(
              refused.response.status >= 200 && refused.response.status < 300,
              false,
            );
          yield* readEventually(read(healthy));
          yield* check(healthy);
          yield* checkHandoff(prior, yield* fleet.owner(cell), healthy);
          yield* write(healthy, 6);
          yield* fleet.reconnect(isolated);
          targets[isolated] = yield* fleet.start(isolated);
          yield* ready(isolated);
          yield* check(isolated);
          yield* write(isolated, 7);
          yield* check(healthy);
          yield* check(isolated);
        }),
      );
      if (durability === "fleet")
        yield* stage(
          "follower-recovery",
          Effect.gen(function* () {
            const prior = yield* fleet.owner(cell);
            const leader = prior.node;
            const follower = leader === "celld" ? "celld2" : "celld";
            // Distinct warm-up transactions establish a live ensemble; bucket-only execution cannot pass.
            let nextId = 8;
            let proved = false;
            for (; nextId <= 15; nextId++) {
              const previous = yield* fleet.logs(leader);
              yield* write(leader, nextId);
              const current = yield* fleet.logs(leader);
              if (!current.startsWith(previous))
                return yield* Effect.fail(
                  new TckError({
                    phase: "fleet-proof",
                    message: "Runtime log prefix changed",
                  }),
                );
              if (hasFleetProof(current.slice(previous.length), cell)) {
                proved = true;
                nextId++;
                break;
              }
              yield* Effect.sleep("1 second");
            }
            yield* equal(proved, true);
            yield* equal((yield* fleet.owner(cell)).node, leader);
            const beforeProof = yield* fleet.logs(leader);
            const recoveryNodes = nodes.filter((node) => node !== leader);
            const beforeRecovery = yield* Effect.forEach(
              recoveryNodes,
              fleet.logs,
            );
            yield* fleet.partition(leader);
            yield* write(leader, nextId);
            const proofLog = yield* fleet.logs(leader);
            yield* artifacts.text("fleet-offline-proof.log", proofLog);
            yield* equal(proofLog.startsWith(beforeProof), true);
            yield* equal(
              hasFleetProof(proofLog.slice(beforeProof.length), cell),
              true,
            );
            yield* fleet.kill(leader);
            yield* leaseLapse;
            yield* readEventually(read(follower), { timeout: "60 seconds" });
            yield* check(follower);
            yield* checkHandoff(prior, yield* fleet.owner(cell), follower);
            const recoveredLogs = yield* Effect.forEach(
              recoveryNodes,
              fleet.logs,
            );
            yield* artifacts.text(
              "fleet-follower-recovery.log",
              recoveredLogs.join("\n"),
            );
            for (const [index, logs] of recoveredLogs.entries())
              yield* equal(logs.startsWith(beforeRecovery[index]!), true);
            yield* equal(
              recoveredLogs.some((logs, index) =>
                hasLogRecovery(
                  logs.slice(beforeRecovery[index]!.length),
                  leader,
                ),
              ),
              true,
            );
            yield* write(follower, nextId + 1);
            yield* fleet.reconnect(leader);
            targets[leader] = yield* fleet.start(leader);
            yield* ready(leader);
            yield* check(leader);
            yield* check(follower);
          }),
        );
      if (options.resilience) {
        const advanced = yield* resilienceStages({
          fleet,
          nodes,
          cell: () => cell,
          acknowledged,
          write,
          check,
          read,
          ready,
          request,
          start: (node) =>
            fleet.start(node).pipe(
              Effect.map((target) => {
                targets[node] = target;
              }),
            ),
        });
        for (const step of advanced)
          yield* executor.runCase(step.id, () => step.run, {
            timeout: "150 seconds",
            onFailure: "stop",
          });
      }
    });
    yield* executor.execute(work);
  });
