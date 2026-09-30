import {
  Cause,
  Clock,
  Console,
  Effect,
  Exit,
  Schema,
  type Duration,
  type Scope,
} from "effect";
import { Artifacts } from "./Artifacts.js";
import { CaseResult, Report, TckError, type Profile } from "./Domain.js";
import { junit } from "./Report.js";

interface SuiteOptions {
  readonly runId: string;
  readonly profile: Profile;
  readonly seed: number;
  readonly ids: readonly string[];
  readonly environment: Record<string, unknown>;
  readonly policy?: "strict" | "compatibility";
  readonly timeout?: Duration.Input;
}
interface CaseOptions {
  readonly timeout?: Duration.Input;
  readonly includesSetup?: boolean;
  readonly onFailure?: "continue" | "stop";
}

// One executor per run. Runners own selection, setup and scenario sequencing;
// this module owns terminal results, deadlines, cleanup diagnostics and reporting.
export const makeSuiteExecutor = (options: SuiteOptions) =>
  Effect.gen(function* () {
    const artifacts = yield* Artifacts;
    const startedAt = new Date().toISOString();
    if (!options.ids.length || new Set(options.ids).size !== options.ids.length)
      return yield* Effect.fail(
        new TckError({
          phase: "suite",
          message: "Selected case IDs must be nonempty and unique",
        }),
      );
    const results: CaseResult[] = options.ids.map((id) => ({
      id,
      status: "infrastructure-error",
      durationMs: 0,
      error: "Case not reached",
    }));
    const recorded = new Set<string>();
    const errors: string[] = [];
    const accepted = (result: CaseResult) =>
      result.status === "pass" ||
      (options.policy === "compatibility" &&
        (result.status === "divergence" || result.status === "known-bug"));
    const cleanupError = (detail: string) =>
      Effect.sync(() => {
        errors.push(`Cleanup: ${detail}`);
      });
    // Resolve Artifacts at the call site: API fixtures retain their own case files.
    // Aggregate reports always use the root service captured above.
    const record = (value: CaseResult) =>
      Effect.gen(function* () {
        const result = yield* Schema.decodeUnknownEffect(CaseResult)(value);
        const index = options.ids.indexOf(result.id);
        if (index < 0 || recorded.has(result.id))
          return yield* Effect.fail(
            new TckError({
              phase: "suite",
              message: `Unknown or duplicate case result: ${result.id}`,
            }),
          );
        recorded.add(result.id);
        results[index] = result;
        yield* (yield* Artifacts).json(`case-${result.id}.json`, result);
        yield* Console.log(`${result.status.toUpperCase()} ${result.id}`);
      });
    const runCase = <A, E, R>(
      id: string,
      work: (test: {
        readonly ready: Effect.Effect<void>;
      }) => Effect.Effect<A, E, R>,
      settings: CaseOptions = {},
    ) =>
      Effect.uninterruptibleMask((restore) =>
        Effect.gen(function* () {
          if (!options.ids.includes(id) || recorded.has(id))
            return yield* Effect.fail(
              new TckError({
                phase: "suite",
                message: `Unknown or duplicate case execution: ${id}`,
              }),
            );
          const start = yield* Clock.currentTimeMillis;
          let setupDone = !settings.includesSetup;
          const scoped = Effect.scoped(
            Effect.suspend(() =>
              work({
                ready: Effect.sync(() => {
                  setupDone = true;
                }),
              }),
            ),
          );
          const exit = yield* Effect.exit(
            restore(
              settings.timeout === undefined
                ? scoped
                : scoped.pipe(Effect.timeout(settings.timeout)),
            ),
          );
          const result: CaseResult = {
            id,
            status: Exit.isSuccess(exit)
              ? "pass"
              : setupDone
                ? "fail"
                : "infrastructure-error",
            durationMs: (yield* Clock.currentTimeMillis) - start,
            ...(Exit.isSuccess(exit)
              ? exit.value !== undefined
                ? { candidate: exit.value }
                : {}
              : { error: Cause.pretty(exit.cause) }),
          };
          const recordedExit = yield* Effect.exit(record(result));
          if (Exit.isFailure(recordedExit))
            return yield* Effect.failCause(
              Exit.isFailure(exit)
                ? Cause.combine(exit.cause, recordedExit.cause)
                : recordedExit.cause,
            );
          if (
            Exit.isFailure(exit) &&
            (Cause.hasInterrupts(exit.cause) || settings.onFailure === "stop")
          )
            return yield* Effect.failCause(exit.cause);
          return result;
        }),
      );
    const execute = <A, E, R>(work: Effect.Effect<A, E, R | Scope.Scope>) => {
      const scoped = Effect.scoped(work);
      return (
        options.timeout === undefined
          ? scoped
          : scoped.pipe(Effect.timeout(options.timeout))
      ).pipe(
        // Outside the work scope: resource finalizers run before the report.
        Effect.onExit((exit) =>
          Effect.gen(function* () {
            if (Exit.isFailure(exit)) errors.push(Cause.pretty(exit.cause));
            const report = yield* Schema.decodeUnknownEffect(Report)({
              schemaVersion: 1,
              runId: options.runId,
              profile: options.profile,
              seed: options.seed,
              startedAt,
              completedAt: new Date().toISOString(),
              environment: options.environment,
              cases: results,
              counts: Object.fromEntries(
                [
                  "pass",
                  "fail",
                  "divergence",
                  "known-bug",
                  "reference-error",
                  "infrastructure-error",
                ].map((status) => [
                  status,
                  results.filter((r) => r.status === status).length,
                ]),
              ),
              errors,
              success:
                Exit.isSuccess(exit) &&
                !errors.length &&
                results.every(accepted),
            });
            // Attempt both formats, even if one write fails. onExit preserves the
            // original cause alongside a reporting defect.
            yield* artifacts
              .json("report.json", report)
              .pipe(
                Effect.ensuring(
                  artifacts.text("junit.xml", junit(report)).pipe(Effect.orDie),
                ),
              );
            yield* Console.log(`Evidence: ${artifacts.directory}/report.json`);
          }).pipe(Effect.orDie),
        ),
        Effect.tap(() =>
          errors.length || !results.every(accepted)
            ? Effect.fail(
                new TckError({
                  phase: "suite",
                  message: "Suite failed; inspect the evidence bundle.",
                }),
              )
            : Effect.void,
        ),
      );
    };
    return { execute, record, runCase, cleanupError };
  });
