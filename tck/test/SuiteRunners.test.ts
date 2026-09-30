import { NodeServices } from "@effect/platform-node";
import { expect, it } from "@effect/vitest";
import {
  Context,
  Deferred,
  Effect,
  Fiber,
  FileSystem,
  Ref,
  Schema,
} from "effect";
import { vi } from "vitest";
import { artifactsLayer } from "../src/Artifacts.js";
import { Report, Transport } from "../src/Domain.js";
import { Processes } from "../src/Processes.js";
import { runSuite } from "../src/Runner.js";
import { qualificationIds, runQualification } from "../src/Qualification.js";
import { suites } from "../src/Catalog.js";
import { deploymentIds } from "../src/DeploymentChecks.js";
import { recoveryIds, runRecovery } from "../src/Recovery.js";
import { multinodeIds, runMultinode } from "../src/Multinode.js";

class SetupProbe extends Context.Service<
  SetupProbe,
  {
    readonly entered: Deferred.Deferred<void>;
    readonly cleaned: Ref.Ref<boolean>;
  }
>()("test/SetupProbe") {}
const setup = Effect.gen(function* () {
  const probe = yield* SetupProbe;
  yield* Effect.addFinalizer(() => Ref.set(probe.cleaned, true));
  yield* Deferred.succeed(probe.entered, undefined);
  return yield* Effect.never;
});
vi.mock("../src/Local.js", () => ({ acquireLocal: () => setup }));
vi.mock("../src/Reference.js", () => ({ acquireReference: () => setup }));

const options = {
  runId: "tck-cancellation",
  profile: "local" as const,
  seed: 42,
  caseId: "",
};
// Every selected case must still be reported, so the expected counts come from
// the same registries the runners select from.
const runners = [
  {
    name: "api",
    work: runSuite(options),
    count: suites.all.length + deploymentIds.length,
  },
  { name: "recovery", work: runRecovery(options), count: recoveryIds.length },
  {
    name: "multinode",
    work: runMultinode(options),
    count: multinodeIds("bucket").length,
  },
  {
    name: "qualification",
    work: runQualification({ ...options, suite: "traffic" }),
    count: qualificationIds.filter((id) => id.startsWith("traffic.")).length,
  },
];
for (const runner of runners)
  it.effect(
    `${runner.name} writes every selected result after interrupted setup cleanup`,
    () =>
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const directory = yield* fs.makeTempDirectoryScoped();
        const entered = yield* Deferred.make<void>();
        const cleaned = yield* Ref.make(false);
        const fiber = yield* runner.work.pipe(
          Effect.provide(artifactsLayer(directory)),
          Effect.provideService(SetupProbe, { entered, cleaned }),
          Effect.provideService(Processes, {
            run: () => Effect.succeed({ stdout: "test", stderr: "" }),
          }),
          Effect.provideService(Transport, {
            request: () => Effect.die("No HTTP before setup finishes"),
            websocket: () => Effect.die("No sockets before setup finishes"),
          }),
          Effect.forkChild,
        );
        yield* Deferred.await(entered);
        yield* Fiber.interrupt(fiber);
        expect(yield* Ref.get(cleaned)).toBe(true);
        const report = yield* Schema.decodeUnknownEffect(
          Schema.fromJsonString(Report),
        )(yield* fs.readFileString(`${directory}/report.json`));
        expect(report.success).toBe(false);
        expect(report.cases).toHaveLength(runner.count);
        expect(
          report.cases.every(
            (result) => result.status === "infrastructure-error",
          ),
        ).toBe(true);
        expect(report.counts?.["infrastructure-error"]).toBe(runner.count);
        expect(report.errors.join(" ")).toContain("interrupt");
        expect(yield* fs.readFileString(`${directory}/junit.xml`)).toContain(
          "testsuite",
        );
      }).pipe(Effect.provide(NodeServices.layer)),
  );
