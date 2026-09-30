import { waitForReady } from "./Polling.js";
import { Console, Effect, FileSystem, Schema } from "effect";
import { resolve } from "node:path";
import { Artifacts, artifactsLayer, decodeJson } from "./Artifacts.js";
import { buildFixtureFor } from "./Build.js";
import { cases, suites, type Suite } from "./Catalog.js";
import { Coverage, validateCoverage } from "./Coverage.js";
import {
  ApiEnvironment,
  Transport,
  TckError,
  CaseResult,
  type Profile,
  type Target,
} from "./Domain.js";
import { deploymentIds as allDeploymentIds } from "./DeploymentChecks.js";
import { acquireLocal } from "./Local.js";
import { provenance } from "./Provenance.js";
import { equal, evaluate } from "./Oracle.js";
import { acquireReference } from "./Reference.js";
import { BugRegistry, validateBugRegistry } from "./KnownBugs.js";
import { makeSuiteExecutor } from "./SuiteExecutor.js";

export interface RunOptions {
  readonly runId: string;
  readonly profile: Profile;
  readonly seed: number;
  readonly caseId: string;
  readonly suite?: Suite;
  readonly knownBugs?: "allow" | "error";
}
export const selectCases = (caseId: string, suite: Suite = "all") =>
  Effect.gen(function* () {
    const selected = caseId
      ? suites[suite].filter((test) => test.id === caseId)
      : suites[suite];
    if (!selected.length)
      return yield* Effect.fail(
        new TckError({
          phase: "arguments",
          message: `Unknown case: ${caseId}`,
        }),
      );
    return selected;
  });
const ready = (target: Target, runId: string) =>
  Effect.gen(function* () {
    const transport = yield* Transport;
    const observation = yield* waitForReady(
      transport.request(target, { path: `/ready?name=${runId}-readiness` }),
      { interval: "250 millis", attempts: 31, timeout: "45 seconds" },
    );
    yield* equal(observation, {
      status: 200,
      headers: { "content-type": "application/json" },
      body: { value: "ok" },
    });
  });

export const runSuite = (options: RunOptions) =>
  Effect.gen(function* () {
    const selected = yield* selectCases(options.caseId, options.suite);
    const artifacts = yield* Artifacts;
    const fs = yield* FileSystem.FileSystem;
    const coverage = yield* decodeJson(
      Coverage,
      yield* fs.readFileString(
        new URL("../docs/coverage.json", import.meta.url).pathname,
      ),
    );
    yield* validateCoverage(cases, coverage);
    const bugs = yield* decodeJson(
      BugRegistry,
      yield* fs.readFileString(
        new URL("../docs/bugs.json", import.meta.url).pathname,
      ),
    );
    yield* validateBugRegistry(bugs, cases);
    yield* artifacts.json("bugs.json", {
      ...bugs,
      policy: options.knownBugs ?? "allow",
    });
    yield* artifacts.json("coverage.json", {
      ...coverage,
      selected: selected.map((test) => test.id),
      notSelected: cases
        .filter((test) => !selected.includes(test))
        .map((test) => test.id),
      divergences: selected
        .filter((test) => test.divergence)
        .map((test) => ({ id: test.id, ...test.divergence, check: undefined })),
    });
    const deploymentIds =
      options.profile === "local" &&
      selected.some((test) => (test.fixture ?? "core") === "core")
        ? allDeploymentIds
        : [];
    const environment: Record<string, unknown> = {
      fixtures: {},
      referenceOnly: options.profile === "reference",
    };
    const executor = yield* makeSuiteExecutor({
      ...options,
      ids: [...selected.map((test) => test.id), ...deploymentIds],
      environment,
      policy: "compatibility",
      timeout: "10 minutes",
    });
    const work = Effect.gen(function* () {
      Object.assign(environment, yield* provenance);
      yield* artifacts.json("run.json", {
        ...options,
        selected: selected.map((test) => ({
          id: test.id,
          contract: test.contract,
        })),
      });
      yield* Console.log(
        `Building Effect fixtures; profile=${options.profile}, seed=${options.seed}`,
      );
      for (const fixture of [
        "core",
        "node",
        "flags",
        "extensions",
        "repro",
      ] as const) {
        if (!selected.some((test) => (test.fixture ?? "core") === fixture))
          continue;
        yield* Effect.scoped(
          Effect.gen(function* () {
            const bundle = yield* buildFixtureFor(fixture);
            const groupEnvironment: Record<string, unknown> = {
              fixtureSha256: bundle.sha256,
              compatibilityDate: bundle.compatibilityDate,
              compatibilityFlags: bundle.config.compatibility_flags,
            };
            (environment.fixtures as Record<string, unknown>)[fixture] =
              groupEnvironment;
            const reference = yield* acquireReference(
              "reference",
              bundle,
              executor.cleanupError,
            );
            groupEnvironment.reference = reference.metadata;
            yield* ready(reference.target, options.runId);
            yield* Console.log(
              `Reference ready. Starting ${options.profile === "local" ? "celld + MinIO" : "second isolated workerd"}...`,
            );
            const candidate = yield* options.profile === "local"
              ? acquireLocal({
                  runId: `${options.runId}-${fixture}`,
                  bundle,
                  cleanupError: executor.cleanupError,
                  topology: "single",
                  deploymentChecks: fixture === "core",
                })
              : acquireReference("candidate", bundle, executor.cleanupError);
            groupEnvironment.candidate = candidate.metadata;
            if (options.profile === "local" && fixture === "core") {
              const deploymentResults = yield* Schema.decodeUnknownEffect(
                Schema.Array(CaseResult),
              )(candidate.metadata.deploymentChecks);
              yield* equal(
                deploymentResults.map((result) => result.id),
                deploymentIds,
              );
              for (const result of deploymentResults)
                yield* executor.record(result);
            }
            yield* ready(candidate.target, options.runId);
            for (const test of selected) {
              if ((test.fixture ?? "core") !== fixture) continue;
              const result = yield* evaluate(
                test,
                reference.target,
                candidate.target,
                {
                  namespace: `${options.runId}-${test.id.replaceAll(".", "-")}`,
                  seed: options.seed,
                  compatibilityDate: bundle.compatibilityDate,
                  compatibilityFlags: bundle.config.compatibility_flags,
                },
                bugs.expectations.find((entry) => entry.caseId === test.id),
                options.knownBugs,
              );
              yield* executor.record(result);
            }
          }),
        ).pipe(
          Effect.provide(artifactsLayer(resolve(artifacts.directory, fixture))),
        );
      }
      yield* Schema.decodeUnknownEffect(ApiEnvironment)(environment);
    });
    yield* executor.execute(work);
  });
