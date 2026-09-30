import { NodeServices } from "@effect/platform-node";
import { expect, it } from "@effect/vitest";
import { Effect, FileSystem } from "effect";
import { buildSite } from "../src/site/Build.js";
import {
  buildModel,
  definitions,
  type Run,
  type SuiteInput,
} from "../src/site/Model.js";
import { renderSite } from "../src/site/Render.js";

const run: Run = {
  repository: "owner/repo",
  sha: "abc123",
  runId: "42",
  attempt: "1",
  number: "7",
  branch: "main",
  conclusion: "success",
};
const coverage = { scope: "Local validation", excluded: [] };
const inputs = (): Record<string, SuiteInput> =>
  Object.fromEntries(
    definitions.map((suite) => [
      suite.key,
      {
        job: {
          suite: suite.key,
          conclusion: "success",
          sha: run.sha,
          runId: run.runId,
          attempt: run.attempt,
        },
        problems: [],
        ...(suite.ids.length
          ? {
              report: {
                schemaVersion: 1,
                runId: `tck-${suite.key}`,
                profile: suite.key === "reference" ? "reference" : "local",
                seed: 0,
                startedAt: "now",
                completedAt: "now",
                environment: { sourceRevision: run.sha },
                cases: suite.ids.map((id) => ({
                  id,
                  status: "pass",
                  durationMs: 1,
                })),
                errors: [],
                success: true,
              },
            }
          : {}),
      },
    ]),
  );
const model = (data = inputs()) =>
  buildModel(run, data, coverage, "2026-09-18T12:00:00.000Z");

it.effect(
  "requires all scheduled evidence while keeping repro cases explicitly unscheduled",
  () =>
    Effect.sync(() => {
      const result = model();
      expect(result.complete).toBe(true);
      expect(result.suites).toHaveLength(definitions.length);
      expect(result.counts["not-scheduled"]).toBe(4);
      expect(result.counts.missing).toBeUndefined();
      const missing = model({});
      expect(missing.complete).toBe(false);
      expect(missing.counts.pass).toBeUndefined();
      expect(missing.counts.missing).toBeGreaterThan(0);
    }),
);

it.effect(
  "retains accepted exceptions and derives counts from individual observations",
  () =>
    Effect.sync(() => {
      const data = inputs();
      const report = data.local!.report!;
      data.local!.report = {
        ...report,
        cases: report.cases.map((value, i) => ({
          ...value,
          status: i === 0 ? "known-bug" : i === 1 ? "divergence" : value.status,
        })),
      };
      const result = model(data);
      expect(result.complete).toBe(true);
      expect(result.counts["known-bug"]).toBe(1);
      expect(result.counts.divergence).toBe(1);
      expect(result.counts.pass).toBe(model().counts.pass! - 2);
    }),
);

for (const mutation of [
  "missing case",
  "duplicate case",
  "unexpected case",
  "cleanup error",
  "failed observation",
  "failed job",
  "failed checks",
  "wrong commit",
  "wrong attempt",
  "wrong profile",
] as const) {
  it.effect(`never reports complete with ${mutation}`, () =>
    Effect.sync(() => {
      const data = inputs();
      const entry = data.local!;
      const report = entry.report!;
      switch (mutation) {
        case "missing case":
          entry.report = { ...report, cases: report.cases.slice(1) };
          break;
        case "duplicate case":
          entry.report = {
            ...report,
            cases: [...report.cases, report.cases[0]!],
          };
          break;
        case "unexpected case":
          entry.report = {
            ...report,
            cases: [
              ...report.cases,
              { id: "invented", status: "pass", durationMs: 1 },
            ],
          };
          break;
        case "cleanup error":
          entry.report = { ...report, errors: ["cleanup failed"] };
          break;
        case "failed observation":
          entry.report = {
            ...report,
            cases: report.cases.map((value, i) =>
              i === 0 ? { ...value, status: "fail" } : value,
            ),
          };
          break;
        case "failed job":
          entry.job = { ...entry.job!, conclusion: "failure" };
          break;
        case "failed checks":
          data.checks!.job = { ...data.checks!.job!, conclusion: "failure" };
          break;
        case "wrong commit":
          entry.report = { ...report, environment: { sourceRevision: "old" } };
          break;
        case "wrong attempt":
          entry.job = { ...entry.job!, attempt: "2" };
          break;
        case "wrong profile":
          entry.report = { ...report, profile: "reference" };
          break;
      }
      const result = model(data);
      expect(result.complete).toBe(false);
      if (
        ["wrong commit", "wrong attempt", "wrong profile"].includes(mutation)
      ) {
        expect(
          result.suites.find((suite) => suite.key === "local")!.report,
        ).toBeUndefined();
        expect(result.counts.missing).toBe(report.cases.length);
      }
    }),
  );
}

it.effect("escapes untrusted diagnostics in HTML and embedded JSON", () =>
  Effect.sync(() => {
    const attack = '</script><img src=x onerror="alert(1)">';
    const data = inputs();
    data.local!.problems.push(attack);
    const result = model(data);
    result.rows[0]!.contract = "javascript:alert(1)";
    const html = renderSite(result);
    expect(html).not.toContain(attack);
    expect(html).not.toContain('href="javascript:');
    expect(html).toContain("&lt;/script&gt;");
    const embedded = html.match(
      /<script id="site-data" type="application\/json">(.*?)<\/script>/s,
    )![1]!;
    expect(
      JSON.parse(embedded).suites.find(
        (suite: { key: string }) => suite.key === "local",
      ).problems,
    ).toContain(attack);
  }),
);

it.effect(
  "builds real assets, surfaces malformed evidence, and removes stale report downloads",
  () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const directory = yield* fs.makeTempDirectoryScoped();
      const input = `${directory}/input`;
      const output = `${directory}/output`;
      const suiteDir = `${input}/compatibility-summary-local`;
      yield* fs.makeDirectory(suiteDir, { recursive: true });
      const entry = inputs().local!;
      yield* fs.writeFileString(
        `${suiteDir}/job.json`,
        JSON.stringify(entry.job),
      );
      yield* fs.writeFileString(
        `${suiteDir}/report.json`,
        JSON.stringify(entry.report),
      );
      yield* buildSite({ input, output, run });
      expect(yield* fs.exists(`${output}/reports/local.json`)).toBe(true);
      expect(
        (yield* fs.readFileString(`${output}/client.js`)).length,
      ).toBeGreaterThan(100);
      yield* fs.writeFileString(
        `${suiteDir}/report.json`,
        '{"cases":[{"status":"invented"}]}',
      );
      const result = yield* buildSite({ input, output, run });
      expect(result.complete).toBe(false);
      expect(
        result.suites.find((suite) => suite.key === "local")!.problems.join(),
      ).toContain("Cannot read suite evidence");
      expect(yield* fs.exists(`${output}/reports/local.json`)).toBe(false);
    }).pipe(Effect.provide(NodeServices.layer)),
);
