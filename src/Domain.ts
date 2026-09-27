import { Context, Data, Effect, Schema } from "effect";

export class TckError extends Data.TaggedError("TckError")<{
  readonly phase: string;
  readonly message: string;
  readonly detail?: string;
}> {}

/** Preserve an already-typed failure; wrap anything else at the given phase. */
export const toTckError = (phase: string) => (error: unknown) =>
  error instanceof TckError
    ? error
    : new TckError({ phase, message: String(error) });

// Default celld lease lifetime is 10 seconds. Wait beyond it before restart.
export const leaseLapse = Effect.sleep("11 seconds");

export const Target = Schema.Struct({
  name: Schema.String,
  baseUrl: Schema.String,
  engine: Schema.optionalKey(Schema.Literals(["celld", "workerd"])),
  version: Schema.optionalKey(Schema.String),
});
export type Target = typeof Target.Type;
export const Observation = Schema.Struct({
  status: Schema.Int,
  headers: Schema.Record(Schema.String, Schema.String),
  body: Schema.Unknown,
});
export type Observation = typeof Observation.Type;
export interface RequestSpec {
  readonly path: string;
  readonly timeoutMs?: number;
  readonly method?: "GET" | "POST";
  readonly body?: string;
  readonly headers?: Readonly<Record<string, string>>;
}
export class Transport extends Context.Service<
  Transport,
  {
    readonly websocket: (
      target: Target,
      path: string,
      mode?: "stream",
    ) => Effect.Effect<unknown, TckError>;
    readonly request: (
      target: Target,
      spec: RequestSpec,
    ) => Effect.Effect<Observation, TckError>;
  }
>()("tck/Transport") {}

// Requests whose outcome must be classified rather than failed: a transport
// failure is an uncertain result, every other TckError still propagates.
export const attemptRequest = <A, R>(request: Effect.Effect<A, TckError, R>) =>
  request.pipe(
    Effect.map((response): { response: A } | { error: string } => ({
      response,
    })),
    Effect.catch((error) =>
      error.phase === "http"
        ? Effect.succeed<{ response: A } | { error: string }>({
            error: error.message,
          })
        : Effect.fail(error),
    ),
  );

export interface CaseInput {
  readonly compatibilityDate?: string;
  readonly compatibilityFlags?: readonly string[];
  readonly namespace: string;
  readonly seed: number;
}
export interface TestCase {
  readonly divergence?: {
    readonly celldVersion: string;
    readonly compatibilityDate: string;
    readonly compatibilityFlags: readonly string[];
    readonly source: string;
    readonly reason: string;
    readonly reviewDate: string;
    readonly owner: string;
    readonly check: (value: unknown) => Effect.Effect<void, TckError>;
  };
  readonly fixture?: "core" | "node" | "flags" | "extensions" | "repro";
  readonly id: string;
  readonly contract: string;
  readonly run: (
    target: Target,
    input: CaseInput,
  ) => Effect.Effect<unknown, TckError, Transport>;
  readonly check: (
    value: unknown,
    input: CaseInput,
  ) => Effect.Effect<void, TckError>;
  readonly compare: (
    reference: unknown,
    candidate: unknown,
  ) => Effect.Effect<void, TckError>;
}
export const CaseResult = Schema.Struct({
  id: Schema.String,
  status: Schema.Literals([
    "pass",
    "divergence",
    "known-bug",
    "fail",
    "reference-error",
    "infrastructure-error",
  ]),
  durationMs: Schema.Number,
  divergence: Schema.optionalKey(Schema.String),
  knownBugs: Schema.optionalKey(Schema.Array(Schema.String)),
  reference: Schema.optionalKey(Schema.Unknown),
  candidate: Schema.optionalKey(Schema.Unknown),
  error: Schema.optionalKey(Schema.String),
});
export type CaseResult = typeof CaseResult.Type;

export interface Bundle {
  readonly modules: Readonly<Record<string, string>>;
  readonly config: Schema.Schema.Type<
    typeof import("./FixtureConfig.js").FixtureConfig
  >;
  readonly directory: string;
  readonly source: string;
  readonly sha256: string;
  readonly compatibilityDate: string;
  readonly binding: { readonly name: string; readonly className: string };
}
export interface RuntimeHandle {
  readonly lifecycle?: {
    readonly stopStorage: () => Effect.Effect<void, TckError>;
    readonly restoreStorage: () => Effect.Effect<void, TckError>;
    readonly prepareRestart: () => Effect.Effect<void, TckError>;
    readonly discardDisk: () => Effect.Effect<void, TckError>;
    readonly stop: (crash: boolean) => Effect.Effect<void, TckError>;
    readonly start: () => Effect.Effect<Target, TckError>;
  };
  readonly target: Target;
  readonly metadata: Readonly<Record<string, unknown>>;
}
export type Profile = "local" | "reference";
// API-suite provenance is separate from lifecycle suite environment metadata.
export const ApiEnvironment = Schema.Struct({
  hostNode: Schema.String,
  platform: Schema.String,
  architecture: Schema.String,
  effect: Schema.String,
  platformNode: Schema.String,
  esbuild: Schema.String,
  referenceOnly: Schema.Boolean,
  sourceRevision: Schema.optionalKey(Schema.String),
  dirty: Schema.optionalKey(Schema.NullOr(Schema.Boolean)),
  lockfileSha256: Schema.optionalKey(Schema.String),
  fixtures: Schema.Record(
    Schema.String,
    Schema.Struct({
      fixtureSha256: Schema.String,
      compatibilityDate: Schema.String,
      compatibilityFlags: Schema.Array(Schema.String),
      reference: Schema.optionalKey(Schema.Unknown),
      candidate: Schema.optionalKey(Schema.Unknown),
    }),
  ),
});
export const Report = Schema.Struct({
  schemaVersion: Schema.Literal(1),
  runId: Schema.String,
  profile: Schema.Literals(["local", "reference"]),
  seed: Schema.Int,
  startedAt: Schema.String,
  completedAt: Schema.String,
  environment: Schema.Record(Schema.String, Schema.Unknown),
  cases: Schema.Array(CaseResult),
  errors: Schema.Array(Schema.String),
  counts: Schema.optionalKey(Schema.Record(Schema.String, Schema.Int)),
  success: Schema.Boolean,
});
export type Report = typeof Report.Type;
