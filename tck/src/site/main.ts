import { NodeRuntime, NodeServices } from "@effect/platform-node";
import { Console, Effect, FileSystem } from "effect";
import { Command, Flag } from "effect/unstable/cli";
import { decodeJson } from "../Artifacts.js";
import { buildSite } from "./Build.js";
import { Run } from "./Model.js";
const cli = Command.make(
  "build-results-site",
  {
    input: Flag.String("input").pipe(Flag.withDefault(".cache/site-input")),
    output: Flag.String("output").pipe(Flag.withDefault(".cache/site")),
    context: Flag.String("context").pipe(Flag.withDefault("")),
  },
  (options) =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const run = options.context
        ? yield* decodeJson(Run, yield* fs.readFileString(options.context))
        : {
            repository: process.env.GITHUB_REPOSITORY ?? "ewhauser/celld-tck",
            sha: process.env.GITHUB_SHA ?? "local",
            runId: process.env.GITHUB_RUN_ID ?? "local",
            attempt: process.env.GITHUB_RUN_ATTEMPT ?? "1",
            number: process.env.GITHUB_RUN_NUMBER ?? "local",
            branch: process.env.GITHUB_REF_NAME ?? "local preview",
            conclusion: process.env.CI_RESULT ?? "unknown",
          };
      const model = yield* buildSite({ ...options, run });
      yield* Console.log(
        `Built ${options.output}/index.html: ${model.rows.length} cases, ${model.complete ? "complete" : "needs attention"}`,
      );
    }),
);
Command.run(cli, { version: "0.0.0" }).pipe(
  Effect.provide(NodeServices.layer),
  NodeRuntime.runMain,
);
