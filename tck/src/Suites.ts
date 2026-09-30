/**
 * Single source of truth for the suites the CLI can run.
 *
 * The CLI flag literals, the dispatch in main.ts, the API-suite subtype in
 * Catalog.ts, and the qualification keys in the site model are all derived from
 * this registry so a new suite only has to be declared once.
 */
export type ApiSuite =
  | "all"
  | "core"
  | "bindings"
  | "node"
  | "flags"
  | "extensions"
  | "repros";
export type MultinodeSuite = "multinode" | "fleet" | "resilience";
export type QualificationSuite =
  | "qualification"
  | "traffic"
  | "dependencies"
  | "faults"
  | "capacity"
  | "security"
  | "telemetry"
  | "operations";

export type SuiteEntry =
  | { readonly name: ApiSuite; readonly kind: "api" }
  | { readonly name: "recovery"; readonly kind: "recovery" }
  | {
      readonly name: MultinodeSuite;
      readonly kind: "multinode";
      readonly durability: "bucket" | "fleet";
      readonly resilience: boolean;
    }
  | {
      readonly name: QualificationSuite;
      readonly kind: "qualification";
      // The aggregate key selects every qualification case rather than one group.
      readonly aggregate: boolean;
    }
  | { readonly name: "audit"; readonly kind: "audit" };

export type SuiteName = SuiteEntry["name"];

// Order is load-bearing: it is the order the CLI lists --suite values in.
export const suiteRegistry: readonly SuiteEntry[] = [
  { name: "all", kind: "api" },
  { name: "core", kind: "api" },
  { name: "bindings", kind: "api" },
  { name: "node", kind: "api" },
  { name: "flags", kind: "api" },
  { name: "extensions", kind: "api" },
  { name: "repros", kind: "api" },
  { name: "recovery", kind: "recovery" },
  {
    name: "multinode",
    kind: "multinode",
    durability: "bucket",
    resilience: false,
  },
  { name: "fleet", kind: "multinode", durability: "fleet", resilience: false },
  {
    name: "resilience",
    kind: "multinode",
    durability: "fleet",
    resilience: true,
  },
  { name: "qualification", kind: "qualification", aggregate: true },
  { name: "audit", kind: "audit" },
  { name: "traffic", kind: "qualification", aggregate: false },
  { name: "dependencies", kind: "qualification", aggregate: false },
  { name: "faults", kind: "qualification", aggregate: false },
  { name: "capacity", kind: "qualification", aggregate: false },
  { name: "security", kind: "qualification", aggregate: false },
  { name: "telemetry", kind: "qualification", aggregate: false },
  { name: "operations", kind: "qualification", aggregate: false },
];

export const suiteNames: readonly SuiteName[] = suiteRegistry.map(
  (suite) => suite.name,
);

export const suiteEntry = (name: SuiteName): SuiteEntry =>
  suiteRegistry.find((suite) => suite.name === name)!;

export const isQualification = (name: string): name is QualificationSuite =>
  suiteRegistry.some(
    (suite) => suite.kind === "qualification" && suite.name === name,
  );

/** Qualification case groups, excluding the aggregate key. */
export const qualificationGroups: readonly QualificationSuite[] = suiteRegistry
  .filter((suite) => suite.kind === "qualification" && !suite.aggregate)
  .map((suite) => suite.name as QualificationSuite);
