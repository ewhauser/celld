import { Effect, FileSystem, Schema } from "effect";
import { build } from "esbuild";
import { decodeJson } from "../Artifacts.js";
import { Report, TckError } from "../Domain.js";
import { Coverage } from "../Coverage.js";
import { buildModel, definitions, Job, Run, type SuiteInput } from "./Model.js";
import { renderSite } from "./Render.js";

export const buildSite = (options: {
  input: string;
  output: string;
  run: Run;
  generatedAt?: string;
}) =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const run = yield* Schema.decodeUnknownEffect(Run)(options.run);
    const inputs: Record<string, SuiteInput> = {};
    const reportsUnder = (
      directory: string,
      depth = 0,
    ): Effect.Effect<string[], unknown> =>
      Effect.gen(function* () {
        if (depth > 5) return [];
        const paths: string[] = [];
        for (const name of yield* fs.readDirectory(directory)) {
          const path = `${directory}/${name}`;
          const info = yield* fs.stat(path);
          if (info.type === "Directory")
            paths.push(...(yield* reportsUnder(path, depth + 1)));
          else if (name === "report.json") paths.push(path);
        }
        return paths;
      });
    for (const definition of definitions) {
      const input: SuiteInput = { problems: [] };
      inputs[definition.key] = input;
      const directory = `${options.input}/compatibility-summary-${definition.key}`;
      if (!(yield* fs.exists(directory))) continue;
      yield* Effect.gen(function* () {
        input.job = yield* decodeJson(
          Job,
          yield* fs.readFileString(`${directory}/job.json`),
        );
        const reports = yield* reportsUnder(directory);
        if (reports.length > 1) {
          input.problems.push(
            "Multiple reports found for this suite; refusing to choose one.",
          );
        } else if (reports[0]) {
          input.report = yield* decodeJson(
            Report,
            yield* fs.readFileString(reports[0]),
          );
        }
      }).pipe(
        Effect.catch((error) =>
          Effect.sync(() => {
            input.problems.push(`Cannot read suite evidence: ${String(error)}`);
          }),
        ),
      );
    }
    const coverage = yield* decodeJson(
      Coverage,
      yield* fs.readFileString(
        new URL("../../docs/coverage.json", import.meta.url).pathname,
      ),
    );
    const model = buildModel(
      run,
      inputs,
      coverage,
      options.generatedAt ?? new Date().toISOString(),
    );
    yield* fs.makeDirectory(`${options.output}/reports`, { recursive: true });
    yield* fs.writeFileString(
      `${options.output}/index.html`,
      renderSite(model),
    );
    yield* fs.writeFileString(
      `${options.output}/results.json`,
      JSON.stringify(model, null, 2) + "\n",
    );
    yield* fs.writeFileString(`${options.output}/.nojekyll`, "");
    for (const suite of model.suites)
      if (suite.report)
        yield* fs.writeFileString(
          `${options.output}/reports/${suite.key}.json`,
          JSON.stringify(suite.report, null, 2) + "\n",
        );
      else
        yield* fs.remove(`${options.output}/reports/${suite.key}.json`, {
          force: true,
        });
    yield* fs.copyFile(
      new URL("../../web/style.css", import.meta.url).pathname,
      `${options.output}/style.css`,
    );
    yield* Effect.tryPromise({
      try: () =>
        build({
          entryPoints: [new URL("./Client.ts", import.meta.url).pathname],
          outfile: `${options.output}/client.js`,
          bundle: true,
          minify: true,
          platform: "browser",
          target: "es2023",
        }),
      catch: (error) =>
        new TckError({ phase: "site-build", message: String(error) }),
    });
    return model;
  });
