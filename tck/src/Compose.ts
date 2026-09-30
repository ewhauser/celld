import { Effect } from "effect";
import { Processes } from "./Processes.js";
import { TckError } from "./Domain.js";

export type Compose = (
  args: readonly string[],
) => Effect.Effect<{ stdout: string; stderr: string }, TckError>;

/** `mc cat <key>` through the storage service's throwaway container. */
export const mcCat = (compose: Compose, key: string) =>
  compose(["run", "--rm", "-T", "--entrypoint", "mc", "storage", "cat", key]);

/** `tool deploy <path> [--dry-run] --json`. */
export const toolDeploy = (
  compose: Compose,
  path: string,
  options: { readonly dryRun: boolean },
) =>
  compose([
    "run",
    "--rm",
    "-T",
    "tool",
    "deploy",
    path,
    ...(options.dryRun ? ["--dry-run"] : []),
    "--json",
  ]);

/** Published loopback endpoint for a service port, validated before use. */
export const publishedPort = (
  compose: Compose,
  service: string,
  port: string,
  phase: string,
  message: string | ((address: string) => string),
) =>
  Effect.gen(function* () {
    const address = (yield* compose(["port", service, port])).stdout.trim();
    if (!/^127\.0\.0\.1:\d+$/.test(address))
      return yield* Effect.fail(
        new TckError({
          phase,
          message: typeof message === "string" ? message : message(address),
        }),
      );
    return address;
  });

/** Raw `docker inspect` output for the single container of a compose service. */
export const inspectService = (
  compose: Compose,
  processes: typeof Processes.Service,
  service: string,
) =>
  Effect.gen(function* () {
    const id = (yield* compose(["ps", "--all", "-q", service])).stdout.trim();
    return (yield* processes.run("docker", ["inspect", id])).stdout;
  });
