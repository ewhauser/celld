import { Schema } from "effect";
import { cases, suites } from "../Catalog.js";
import { deploymentIds } from "../DeploymentChecks.js";
import { recoveryIds } from "../Recovery.js";
import { multinodeIds } from "../Multinode.js";
import { qualificationIds } from "../Qualification.js";
import { qualificationGroups } from "../Suites.js";
import type { CaseResult, Report } from "../Domain.js";

export const Job = Schema.Struct({
  suite: Schema.String,
  conclusion: Schema.Literals(["success", "failure", "cancelled", "skipped"]),
  sha: Schema.String,
  runId: Schema.String,
  attempt: Schema.String,
});
export type Job = typeof Job.Type;
export const Run = Schema.Struct({
  repository: Schema.String,
  sha: Schema.String,
  runId: Schema.String,
  attempt: Schema.String,
  number: Schema.String,
  branch: Schema.String,
  conclusion: Schema.String,
});
export type Run = typeof Run.Type;
const apiIds = suites.all.map((test) => test.id);
export const definitions: readonly {
  key: string;
  label: string;
  group: string;
  ids: readonly string[];
}[] = [
  {
    key: "checks",
    label: "Static & unit",
    group: "checks",
    ids: [] as string[],
  },
  { key: "reference", label: "Reference", group: "api", ids: apiIds },
  {
    key: "local",
    label: "celld",
    group: "api",
    ids: [...apiIds, ...deploymentIds],
  },
  { key: "recovery", label: "Recovery", group: "recovery", ids: recoveryIds },
  {
    key: "multinode",
    label: "Multinode",
    group: "recovery",
    ids: multinodeIds("bucket"),
  },
  {
    key: "fleet",
    label: "Fleet",
    group: "recovery",
    ids: multinodeIds("fleet"),
  },
  {
    key: "resilience",
    label: "Resilience",
    group: "recovery",
    ids: multinodeIds("fleet", true),
  },
  ...qualificationGroups.map((key) => ({
    key,
    label: key[0]!.toUpperCase() + key.slice(1),
    group: "qualification",
    ids: qualificationIds.filter((id) => id.startsWith(key + ".")),
  })),
];
export type Status =
  | CaseResult["status"]
  | "missing"
  | "not-scheduled"
  | "not-applicable";
export interface Cell {
  status: Status;
  suite?: string;
  result?: CaseResult;
}
export interface MatrixRow {
  id: string;
  family: string;
  group: string;
  contract?: string;
  reference: Cell;
  candidate: Cell;
}
export interface SuiteInput {
  job?: Job;
  report?: Report;
  problems: string[];
}
export interface SuiteSummary {
  key: string;
  label: string;
  group: string;
  expected: number;
  status: "success" | "failure" | "missing";
  conclusion: string;
  problems: string[];
  report?: Report;
}
export interface SiteModel {
  run: Run;
  generatedAt: string;
  suites: SuiteSummary[];
  rows: MatrixRow[];
  counts: Record<string, number>;
  complete: boolean;
  scope: string;
  excluded: readonly { family: string; reason: string }[];
}
export const isProblem = (status: Status) =>
  ["fail", "reference-error", "infrastructure-error", "missing"].includes(
    status,
  );
export const buildModel = (
  run: Run,
  inputs: Readonly<Record<string, SuiteInput>>,
  coverage: {
    scope: string;
    excluded: readonly { family: string; reason: string }[];
  },
  generatedAt: string,
): SiteModel => {
  const summaries: SuiteSummary[] = definitions.map((definition) => {
    const input = inputs[definition.key];
    const problems = [...(input?.problems ?? [])];
    let report = input?.report;
    const job = input?.job;
    if (!job) problems.push("No CI job status was received.");
    else if (
      job.suite !== definition.key ||
      job.sha !== run.sha ||
      job.runId !== run.runId ||
      job.attempt !== run.attempt
    ) {
      report = undefined;
      problems.push(
        "Job status belongs to a different suite, commit, or run attempt.",
      );
    }
    if (
      report &&
      (report.environment.sourceRevision !== run.sha ||
        report.profile !==
          (definition.key === "reference" ? "reference" : "local"))
    ) {
      problems.push("Report provenance does not match this commit or profile.");
      report = undefined;
    }
    if (definition.ids.length && !report)
      problems.push(
        "No valid report was received. The suite may have failed before reporting.",
      );
    if (report) {
      const actual = report.cases.map((result) => result.id);
      if (new Set(actual).size !== actual.length)
        problems.push("Report contains duplicate case IDs.");
      const absent = definition.ids.filter((id) => !actual.includes(id));
      const unexpected = actual.filter((id) => !definition.ids.includes(id));
      if (absent.length)
        problems.push(
          `${absent.length} scheduled cases are missing from the report.`,
        );
      if (unexpected.length)
        problems.push(`Unexpected case IDs: ${unexpected.join(", ")}`);
      problems.push(...report.errors);
      if (!report.success)
        problems.push("The suite reported an unsuccessful run.");
    }
    const failed =
      job?.conclusion !== "success" ||
      problems.length > 0 ||
      report?.cases.some((result) => isProblem(result.status));
    return {
      key: definition.key,
      label: definition.label,
      group: definition.group,
      expected: definition.ids.length,
      status: !job && !report ? "missing" : failed ? "failure" : "success",
      conclusion: job?.conclusion ?? "missing",
      problems,
      ...(report ? { report } : {}),
    };
  });
  const cell = (suite: string, id: string): Cell => {
    const report = summaries.find((item) => item.key === suite)?.report;
    const result = report?.cases.find((item) => item.id === id);
    return result
      ? { suite, status: result.status, result }
      : { suite, status: "missing" };
  };
  const rows: MatrixRow[] = cases.map((test) => ({
    id: test.id,
    family: test.id.split(".")[0]!,
    group: "api",
    contract: test.contract,
    reference:
      test.fixture === "repro"
        ? { status: "not-scheduled" }
        : cell("reference", test.id),
    candidate:
      test.fixture === "repro"
        ? { status: "not-scheduled" }
        : cell("local", test.id),
  }));
  rows.push(
    ...deploymentIds.map((id) => ({
      id,
      family: "deployment",
      group: "api",
      reference: { status: "not-applicable" as const },
      candidate: cell("local", id),
    })),
  );
  for (const definition of definitions.filter(
    (item) => !["checks", "reference", "local"].includes(item.key),
  ))
    rows.push(
      ...definition.ids.map((id) => ({
        id,
        family: definition.key,
        group: definition.group,
        reference: { status: "not-applicable" as const },
        candidate: cell(definition.key, id),
      })),
    );
  rows.sort(
    (left, right) =>
      left.group.localeCompare(right.group, "en") ||
      left.id.localeCompare(right.id, "en"),
  );
  summaries.sort((left, right) => left.label.localeCompare(right.label, "en"));
  const counts: Record<string, number> = {};
  for (const row of rows)
    for (const value of [row.reference, row.candidate])
      counts[value.status] = (counts[value.status] ?? 0) + 1;
  return {
    run,
    generatedAt,
    suites: summaries,
    rows,
    counts,
    complete:
      run.conclusion === "success" &&
      summaries.every((suite) => suite.status === "success"),
    scope: coverage.scope,
    excluded: coverage.excluded,
  };
};
