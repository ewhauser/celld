import { Context, Effect, Layer, Stream } from "effect";
import { ChildProcess, ChildProcessSpawner } from "effect/unstable/process";
import { Artifacts } from "./Artifacts.js";
import { TckError, toTckError } from "./Domain.js";

export class Processes extends Context.Service<
  Processes,
  {
    readonly run: (
      file: string,
      args: ReadonlyArray<string>,
      env?: Record<string, string>,
    ) => Effect.Effect<{ stdout: string; stderr: string }, TckError>;
  }
>()("tck/Processes") {}

export const processesLayer = Layer.effect(
  Processes,
  Effect.gen(function* () {
    const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
    const artifacts = yield* Artifacts;
    return {
      run: (file, args, env) =>
        Effect.scoped(
          Effect.gen(function* () {
            const handle = yield* spawner.spawn(
              ChildProcess.make(file, args, {
                ...(env ? { env, extendEnv: true } : {}),
                stdin: "ignore",
                stdout: "pipe",
                stderr: "pipe",
                forceKillAfter: "2 seconds",
              }),
            );
            const collect = (stream: typeof handle.stdout) =>
              stream.pipe(
                Stream.decodeText(),
                Stream.runFoldEffect(
                  () => "",
                  (previous, chunk) =>
                    previous.length + chunk.length > 8 * 1024 * 1024
                      ? Effect.fail(
                          new TckError({
                            phase: "process",
                            message: "Process output exceeded 8 MiB",
                          }),
                        )
                      : Effect.succeed(previous + chunk),
                ),
              );
            const output = yield* Effect.all(
              {
                stdout: collect(handle.stdout),
                stderr: collect(handle.stderr),
                code: handle.exitCode,
              },
              { concurrency: "unbounded" },
            );
            yield* artifacts.text(
              "commands.jsonl",
              JSON.stringify({ file, args, ...output }) + "\n",
              true,
            );
            if (output.code !== 0)
              return yield* Effect.fail(
                new TckError({
                  phase: "process",
                  message: `${file} ${args.join(" ")} exited ${output.code}`,
                  detail: output.stderr,
                }),
              );
            return { stdout: output.stdout, stderr: output.stderr };
          }),
        ).pipe(
          Effect.timeout("120 seconds"),
          Effect.mapError(toTckError("process")),
        ),
    };
  }),
);
