import { NodeRuntime, NodeServices } from "@effect/platform-node";
import { Effect, Layer } from "effect";
import { Command, Flag } from "effect/unstable/cli";
import { FetchHttpClient } from "effect/unstable/http";
import { randomUUID } from "node:crypto";
import { resolve } from "node:path";
import { artifactsLayer } from "./Artifacts.js";
import { TckError } from "./Domain.js";
import { processesLayer } from "./Processes.js";
import { auditLedger } from "./Audit.js";
import { runQualification } from "./Qualification.js";
import { runMultinode } from "./Multinode.js";
import { runRecovery } from "./Recovery.js";
import { runSuite, selectCases } from "./Runner.js";
import { transportLayer } from "./Transport.js";
import { suiteEntry, suiteNames } from "./Suites.js";

const cli = Command.make(
  "celld-tck",
  {
    profile: Flag.Literals("profile", ["local", "reference"]).pipe(
      Flag.withDefault("local"),
    ),
    suite: Flag.Literals("suite", suiteNames).pipe(Flag.withDefault("all")),
    knownBugs: Flag.Literals("known-bugs", ["allow", "error"]).pipe(
      Flag.withDefault("allow"),
    ),
    seed: Flag.Int("seed").pipe(Flag.withDefault(42)),
    caseId: Flag.String("case").pipe(Flag.withDefault("")),
    output: Flag.String("output").pipe(Flag.withDefault("artifacts")),
    ledger: Flag.String("ledger").pipe(Flag.withDefault("")),
    endpoint: Flag.String("endpoint").pipe(Flag.withDefault("")),
    name: Flag.String("name").pipe(Flag.withDefault("")),
  },
  (options) =>
    Effect.gen(function* () {
      if (options.seed < 0 || options.seed > 0xffffffff)
        return yield* Effect.fail(
          new TckError({
            phase: "arguments",
            message: "Seed must be an unsigned 32-bit integer",
          }),
        );
      const suite = suiteEntry(options.suite);
      if (suite.kind === "api") yield* selectCases(options.caseId, suite.name);
      const runId = `tck-${yield* Effect.sync(() => randomUUID())}`;
      const services = Layer.mergeAll(processesLayer, transportLayer).pipe(
        Layer.provideMerge(artifactsLayer(resolve(options.output, runId))),
      );
      const run =
        suite.kind === "audit"
          ? auditLedger(options)
          : suite.kind === "qualification"
            ? runQualification({ ...options, runId, suite: suite.name })
            : suite.kind === "multinode"
              ? runMultinode({
                  ...options,
                  runId,
                  durability: suite.durability,
                  resilience: suite.resilience,
                })
              : suite.kind === "recovery"
                ? runRecovery({ ...options, runId })
                : runSuite({ ...options, runId, suite: suite.name });
      yield* run.pipe(Effect.provide(services));
    }),
);

Command.run(cli, { version: "0.0.0" }).pipe(
  Effect.provide(Layer.mergeAll(NodeServices.layer, FetchHttpClient.layer)),
  NodeRuntime.runMain,
);
