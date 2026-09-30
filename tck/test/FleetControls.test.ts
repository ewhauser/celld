import { expect, it } from "@effect/vitest";
import { Effect } from "effect";
import { fleetControls } from "../src/FleetControls.js";
import { Artifacts } from "../src/Artifacts.js";
import { Processes } from "../src/Processes.js";
import { TckError } from "../src/Domain.js";

it.effect(
  "scope teardown attempts every frozen node even when one unpause fails",
  () =>
    Effect.gen(function* () {
      const paused = new Set<string>();
      const releases: string[] = [];
      const compose = (args: readonly string[]) =>
        Effect.gen(function* () {
          const node = args.at(-1)!;
          if (args[0] === "pause") paused.add(node);
          if (args[0] === "unpause") {
            releases.push(node);
            if (node === "celld")
              return yield* Effect.fail(
                new TckError({ phase: "process", message: "unpause failed" }),
              );
            paused.delete(node);
          }
          return { stdout: args[0] === "ps" ? node : "", stderr: "" };
        });
      const outcome = yield* Effect.exit(
        Effect.scoped(
          Effect.gen(function* () {
            const controls = yield* fleetControls(compose, "tck-owned", {
              name: "test",
              baseUrl: "http://127.0.0.1",
            });
            yield* controls.pause("celld");
            yield* controls.pause("celld2");
          }),
        ).pipe(
          Effect.provideService(Artifacts, {
            directory: "/unused",
            text: () => Effect.void,
            json: () => Effect.void,
          }),
          Effect.provideService(Processes, {
            run: (_file, args) =>
              Effect.sync(() => {
                const node = args[1]!;
                return {
                  stdout: JSON.stringify([
                    {
                      Id: node,
                      Image: `sha256:${node}`,
                      Config: {
                        Image: "ghcr.io/denoland/celld:v0.5.0",
                        Labels: {
                          "com.docker.compose.project": "tck-owned",
                          "com.docker.compose.service": node,
                        },
                      },
                      State: {
                        Running: true,
                        Paused: paused.has(node),
                        ExitCode: 0,
                      },
                      NetworkSettings: { Networks: {} },
                    },
                  ]),
                  stderr: "",
                };
              }),
          }),
        ),
      );
      expect(outcome._tag).toBe("Failure");
      expect(releases).toEqual(["celld", "celld2"]);
      expect(paused.has("celld2")).toBe(false);
    }),
);
