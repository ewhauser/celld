import { Effect, Exit, FileSystem, Schema, Schedule } from "effect";
import { build } from "esbuild";
import { resolve } from "node:path";
import { fleetControls } from "./FleetControls.js";
import { DiskContainer } from "./DiskLoss.js";
import { equal } from "./Oracle.js";
import { checkDeployment } from "./DeploymentChecks.js";
import { Artifacts, decodeJson } from "./Artifacts.js";
import { sha256 } from "./Build.js";
import {
  TckError,
  toTckError,
  type Bundle,
  type RuntimeHandle,
} from "./Domain.js";
import { Processes } from "./Processes.js";
import { cleanupAll, owned } from "./Resources.js";
import { inspectService, mcCat, publishedPort, toolDeploy } from "./Compose.js";

const Deployment = Schema.Struct({
  worker: Schema.String,
  version: Schema.String,
  location: Schema.String,
  dry_run: Schema.Boolean,
});
const Container = Schema.Struct({
  Name: Schema.String,
  Image: Schema.String,
  Platform: Schema.String,
  Config: Schema.Struct({ Image: Schema.String }),
});

export type LocalOptions = {
  manualReload?: boolean;
  /** Lowers `CELLD_DEPLOY_MAX_AGE_S` so forced adoption fits the scenario bound. */
  adoptionDeadline?: boolean;
  runId: string;
  bundle: Bundle;
  cleanupError: (detail: string) => Effect.Effect<void>;
} & (
  | {
      topology: "single";
      durability?: "bucket";
      qualification?: false;
      deploymentChecks?: boolean;
    }
  | {
      topology: "cluster";
      nodeCount: 2 | 3;
      durability: "bucket" | "fleet";
      qualification?: false;
    }
  | {
      topology: "cluster";
      nodeCount: 3;
      durability: "bucket" | "fleet";
      qualification: true;
      /** Adds the security-boundary overlay and the in-network probe binary. */
      security?: boolean;
      /**
       * Selects a telemetry overlay: `otlp` adds the recording collector
       * sidecar and points every node at it, `bucket` selects the Parquet sink,
       * and `cli` adds the `celld dev` service without enabling telemetry.
       */
      telemetry?: TelemetryMode | undefined;
      /** Adds the fleet-operations overlay and the in-network probe binary. */
      operations?: boolean;
      /** Adds the ownership-balancing overlay. Requires `operations`. */
      balancing?: boolean;
      /**
       * Adds the per-node image overlay so a run can pin a second celld
       * release. Requires `operations`; `images` seeds the starting pins.
       */
      upgrade?: boolean;
      images?: Readonly<Record<string, string>>;
    }
);

export type TelemetryMode = "otlp" | "bucket" | "cli";

export const acquireLocal = (options: LocalOptions) =>
  Effect.gen(function* () {
    const { runId, bundle, cleanupError } = options;
    const multiNode = options.topology === "cluster";
    const nodeCount = options.topology === "cluster" ? options.nodeCount : 2;
    const durability = options.durability ?? "bucket";
    const qualification = options.qualification ?? false;
    const qualified =
      options.topology === "cluster" && options.qualification === true
        ? options
        : undefined;
    const security = qualified?.security ?? false;
    const telemetry: TelemetryMode | undefined = qualified?.telemetry;
    const operations = qualified?.operations ?? false;
    const balancing = operations && (qualified?.balancing ?? false);
    const upgrade = operations && (qualified?.upgrade ?? false);
    // Mutable so a rolling binary upgrade can re-pin one node at a time; the
    // values are read on every compose invocation, never cached by Compose.
    const images: Record<string, string> = { ...qualified?.images };
    const wantsDeploymentChecks =
      options.topology === "single" && (options.deploymentChecks ?? false);
    const processes = yield* Processes;
    const artifacts = yield* Artifacts;
    const fs = yield* FileSystem.FileSystem;
    const alternatives = process.env.TCK_COMPOSE_BIN
      ? [{ file: process.env.TCK_COMPOSE_BIN, prefix: [] as string[] }]
      : [
          { file: "docker", prefix: ["compose"] },
          { file: "docker-compose", prefix: [] },
          { file: resolve(".cache/tools/docker-compose"), prefix: [] },
        ];
    let selected: (typeof alternatives)[number] | undefined;
    for (const alternative of alternatives) {
      const check = yield* Effect.exit(
        processes.run(alternative.file, [...alternative.prefix, "version"]),
      );
      if (Exit.isSuccess(check)) {
        selected = alternative;
        break;
      }
    }
    if (!selected)
      return yield* Effect.fail(
        new TckError({
          phase: "setup",
          message:
            "Install Docker Compose or set TCK_COMPOSE_BIN to its standalone executable.",
        }),
      );
    const composeCommand = selected;
    // Compose merges later files over earlier ones, so this order is load-bearing.
    // Every selected overlay is also copied into the evidence bundle.
    const overlays = (
      [
        ["compose.yaml", true],
        ["storage-proxy.yaml", true],
        ["multinode.yaml", multiNode],
        ["fleet.yaml", durability === "fleet"],
        ["three-node.yaml", multiNode && nodeCount === 3],
        ["qualification.yaml", qualification],
        ["security.yaml", security],
        ["telemetry.yaml", telemetry === "otlp"],
        ["telemetry-bucket.yaml", telemetry === "bucket"],
        ["telemetry-cli.yaml", telemetry === "cli"],
        ["operations.yaml", operations],
        ["balancing.yaml", balancing],
        ["upgrade.yaml", upgrade],
        ["manual-reload.yaml", options.manualReload ?? false],
        ["adoption-deadline.yaml", options.adoptionDeadline ?? false],
      ] as const
    )
      .filter(([, enabled]) => enabled)
      .map(([name]) => ({
        name,
        path: new URL(`../infra/${name}`, import.meta.url).pathname,
      }));
    const compose = (args: ReadonlyArray<string>) =>
      processes.run(
        composeCommand.file,
        [
          ...composeCommand.prefix,
          "--project-name",
          runId,
          ...overlays.flatMap((overlay) => ["--file", overlay.path]),
          ...args,
        ],
        {
          TCK_FIXTURE_DIR: bundle.directory,
          TCK_DURABILITY: durability,
          ...images,
        },
      );
    for (const overlay of overlays)
      yield* artifacts.text(
        overlay.name,
        yield* fs.readFileString(overlay.path),
      );
    // Sidecar programs live beside the fixture so the sidecar's read-only bind
    // mount can reach them; the in-network probe is built only when needed.
    // Both the security and the fleet-operations suites drive it: celld's
    // operator listener is never published outside the Compose network.
    const sidecars = [
      { entry: "./StorageProxy.ts", out: "proxy.mjs" },
      ...(security || operations
        ? [{ entry: "./SecurityProbe.ts", out: "security-probe.mjs" }]
        : []),
      ...(telemetry === "otlp"
        ? [
            {
              entry: "./TelemetryCollector.ts",
              out: "telemetry-collector.mjs",
            },
          ]
        : []),
    ];
    for (const sidecar of sidecars)
      yield* Effect.tryPromise({
        try: () =>
          build({
            entryPoints: [new URL(sidecar.entry, import.meta.url).pathname],
            bundle: true,
            platform: "node",
            format: "esm",
            outfile: resolve(bundle.directory, sidecar.out),
          }),
        catch: (error) =>
          new TckError({ phase: "build", message: String(error) }),
      });
    return yield* owned(
      Effect.gen(function* () {
        yield* compose(["up", "-d", "minio"]);
        yield* compose(["run", "--rm", "-T", "storage"]);
        yield* compose(["up", "-d", "proxy"]);
        yield* compose([
          "exec",
          "-T",
          "proxy",
          "node",
          "--input-type=module",
          "-e",
          'const response = await fetch("http://127.0.0.1:9091/stats"); if (!response.ok) process.exitCode = 1;',
        ]).pipe(
          Effect.retry({
            schedule: Schedule.spaced("200 millis"),
            times: 20,
          }),
        );
        const diagnosis = yield* compose([
          "run",
          "--rm",
          "-T",
          "--env",
          "S3_ENDPOINT=http://proxy:8082",
          "tool",
          "diagnose",
          "--json",
        ]);
        yield* artifacts.text("diagnose.jsonl", diagnosis.stdout);
        const deploymentChecks = wantsDeploymentChecks
          ? yield* checkDeployment(bundle, compose)
          : [];
        const deployed = yield* toolDeploy(compose, "/fixture", {
          dryRun: false,
        });
        yield* artifacts.text("deployment.json", deployed.stdout);
        const deployment = yield* decodeJson(Deployment, deployed.stdout);
        if (
          !/^[a-z0-9-]+$/.test(deployment.worker) ||
          !/^[a-zA-Z0-9_-]+$/.test(deployment.version) ||
          deployment.dry_run
        ) {
          return yield* Effect.fail(
            new TckError({
              phase: "deployment",
              message: "Invalid deployment identity",
            }),
          );
        }
        const key = `local/tck/deploy/${deployment.worker}/${deployment.version}/index.js`;
        const uploaded = yield* mcCat(compose, key);
        const uploadedHash = sha256(uploaded.stdout);
        if (uploadedHash !== bundle.sha256)
          return yield* Effect.fail(
            new TckError({
              phase: "deployment",
              message: "Uploaded fixture bytes differ from reference artifact",
            }),
          );
        for (const [name, hash] of Object.entries(bundle.modules)) {
          if (!name.endsWith(".wasm")) continue;
          const wasmKey = `local/tck/deploy/${deployment.worker}/${deployment.version}/${name}`;
          const result = yield* compose([
            "run",
            "--rm",
            "-T",
            "--entrypoint",
            "/bin/sh",
            "storage",
            "-ec",
            'mc cat "$1" | sha256sum',
            "verify-module",
            wasmKey,
          ]);
          if (result.stdout.trim().split(/\s+/)[0] !== hash)
            return yield* Effect.fail(
              new TckError({
                phase: "deployment",
                message: `Uploaded module differs: ${name}`,
              }),
            );
        }
        const version = (yield* compose([
          "run",
          "--rm",
          "-T",
          "tool",
          "--version",
        ])).stdout.trim();
        // The nodes open their exporter at startup, so the collector has to be
        // answering before any of them boots.
        if (telemetry === "otlp") {
          yield* compose(["up", "-d", "collector"]);
          yield* compose([
            "exec",
            "-T",
            "collector",
            "node",
            "--input-type=module",
            "-e",
            'const response = await fetch("http://127.0.0.1:9092/"); if (!response.ok) process.exitCode = 1;',
          ]).pipe(
            Effect.retry({
              schedule: Schedule.spaced("200 millis"),
              times: 25,
            }),
          );
        }
        yield* compose([
          "up",
          "-d",
          "celld",
          ...(multiNode
            ? nodeCount === 3
              ? ["celld2", "celld3"]
              : ["celld2"]
            : []),
        ]);
        const address = yield* publishedPort(
          compose,
          "celld",
          "8080",
          "setup",
          (value) => `Unexpected public address: ${value}`,
        );
        const ids = (yield* compose(["ps", "-q", "--all"])).stdout
          .trim()
          .split(/\s+/)
          .filter(Boolean);
        const inspected = yield* decodeJson(
          Schema.Array(Container),
          (yield* processes.run("docker", ["inspect", ...ids])).stdout,
        );
        const containers = inspected.map((entry) => ({
          name: entry.Name,
          image: entry.Config.Image,
          imageId: entry.Image,
          platform: entry.Platform,
        }));
        const target = {
          name: "candidate",
          baseUrl: `http://${address}`,
          engine: "celld" as const,
          version: version.replace(/^celld\s+/, ""),
        };
        const fleet = yield* fleetControls(compose, runId, target);
        let sequence = 0;
        const recordState = (label: string) =>
          Effect.gen(function* () {
            const raw = yield* inspectService(compose, processes, "celld");
            yield* artifacts.text(`lifecycle-${++sequence}-${label}.json`, raw);
            return yield* decodeJson(
              Schema.Array(
                Schema.Struct({
                  State: Schema.Struct({
                    Running: Schema.Boolean,
                    ExitCode: Schema.Number,
                  }),
                }),
              ),
              raw,
            );
          });
        return {
          fleet,
          lifecycle: {
            stopStorage: () =>
              Effect.gen(function* () {
                const before = (yield* recordState("before-outage"))[0]?.State;
                if (!before?.Running)
                  return yield* Effect.fail(
                    new TckError({
                      phase: "lifecycle",
                      message: "celld must be running before the outage",
                    }),
                  );
                yield* compose(["stop", "--timeout", "5", "minio"]);
                const raw = yield* inspectService(compose, processes, "minio");
                yield* artifacts.text("outage-minio-stopped.json", raw);
                const state = (yield* decodeJson(
                  Schema.Array(DiskContainer),
                  raw,
                ))[0];
                if (!state || state.State.Running)
                  return yield* Effect.fail(
                    new TckError({
                      phase: "lifecycle",
                      message: "MinIO did not stop",
                    }),
                  );
              }),
            restoreStorage: () =>
              Effect.gen(function* () {
                yield* compose(["start", "minio"]);
                yield* compose([
                  "run",
                  "--no-deps",
                  "--rm",
                  "-T",
                  "--entrypoint",
                  "mc",
                  "storage",
                  "stat",
                  "local/tck",
                ]).pipe(
                  Effect.retry({
                    schedule: Schedule.spaced("500 millis"),
                    times: 10,
                  }),
                  Effect.timeout("30 seconds"),
                  Effect.mapError(toTckError("lifecycle")),
                );
              }),
            prepareRestart: () =>
              Effect.gen(function* () {
                const state = (yield* recordState("after-outage"))[0]?.State;
                if (!state)
                  return yield* Effect.fail(
                    new TckError({
                      phase: "lifecycle",
                      message: "Missing celld state after outage",
                    }),
                  );
                if (state.Running) {
                  yield* compose(["kill", "--signal", "SIGKILL", "celld"]);
                  const stopped = (yield* recordState(
                    "outage-restart-stopped",
                  ))[0]?.State;
                  if (!stopped || stopped.Running || stopped.ExitCode !== 137)
                    return yield* Effect.fail(
                      new TckError({
                        phase: "lifecycle",
                        message: "Could not stop celld for recovery",
                      }),
                    );
                } else if (state.ExitCode !== 3)
                  return yield* Effect.fail(
                    new TckError({
                      phase: "lifecycle",
                      message: `Unexpected celld exit after outage: ${state.ExitCode}`,
                    }),
                  );
              }),
            discardDisk: () =>
              Effect.gen(function* () {
                // MinIO is inspected around the delegated fleet discard so the
                // storage container is proven untouched by the disk loss.
                const minioState = () =>
                  Effect.gen(function* () {
                    const raw = yield* inspectService(
                      compose,
                      processes,
                      "minio",
                    );
                    yield* artifacts.text(
                      `disk-loss-${++sequence}-minio.json`,
                      raw,
                    );
                    const entries = yield* decodeJson(
                      Schema.Array(DiskContainer),
                      raw,
                    );
                    if (entries.length !== 1)
                      return yield* Effect.fail(
                        new TckError({
                          phase: "lifecycle",
                          message: "Expected one owned container",
                        }),
                      );
                    return entries[0]!;
                  });
                const minio = yield* minioState();
                const discarded = yield* fleet.discard("celld");
                yield* equal(yield* minioState(), minio);
                yield* artifacts.json("disk-loss.json", {
                  removedVolume: discarded.volume,
                  oldContainer: discarded.oldContainer,
                  newContainer: discarded.newContainer,
                  emptyBeforeStart: true,
                  minioUnchanged: true,
                });
              }),
            stop: (crash: boolean) =>
              Effect.gen(function* () {
                const before = (yield* recordState("before-stop"))[0]?.State;
                if (!before?.Running)
                  return yield* Effect.fail(
                    new TckError({
                      phase: "lifecycle",
                      message:
                        "Expected a running container before fault injection",
                    }),
                  );
                yield* compose(
                  crash
                    ? ["kill", "--signal", "SIGKILL", "celld"]
                    : ["stop", "--timeout", "20", "celld"],
                );
                const state = (yield* recordState("stopped"))[0]?.State;
                if (
                  !state ||
                  state.Running ||
                  (crash ? state.ExitCode !== 137 : state.ExitCode !== 0)
                )
                  return yield* Effect.fail(
                    new TckError({
                      phase: "lifecycle",
                      message:
                        "Container did not stop with the expected exit status",
                    }),
                  );
              }),
            start: () =>
              Effect.gen(function* () {
                yield* compose(["start", "celld"]);
                const started = (yield* recordState("started"))[0]?.State;
                if (!started?.Running)
                  return yield* Effect.fail(
                    new TckError({
                      phase: "lifecycle",
                      message: "Container did not start",
                    }),
                  );
                const endpoint = yield* publishedPort(
                  compose,
                  "celld",
                  "8080",
                  "lifecycle",
                  "Invalid restarted endpoint",
                );
                return { ...target, baseUrl: `http://${endpoint}` };
              }),
          },
          target,
          controls: {
            compose,
            proxy: () =>
              compose(["port", "proxy", "9091"]).pipe(
                Effect.map((result) => ({
                  name: "storage-proxy",
                  baseUrl: `http://${result.stdout.trim()}`,
                })),
              ),
            /** Loopback control endpoint of the recording OTLP collector. */
            collector: () =>
              publishedPort(
                compose,
                "collector",
                "9092",
                "telemetry",
                (value) => `Unexpected collector address: ${value}`,
              ).pipe(
                Effect.map((address) => ({
                  name: "telemetry-collector",
                  baseUrl: `http://${address}`,
                })),
              ),
            /** The project directory `celld dev` owns, as the dev service sees it. */
            fixtureDirectory: bundle.directory,
            deploy: () => toolDeploy(compose, "/fixture", { dryRun: false }),
            /**
             * Re-pins one node's celld image. Only infra/upgrade.yaml reads
             * these variables, so this is inert unless that overlay is
             * selected; the node picks the new image up at its next
             * `compose up`, which is what a rolling binary upgrade does.
             */
            setImage: (node: string, image: string) =>
              Effect.sync(() => {
                images[`TCK_IMAGE_${node.toUpperCase()}`] = image;
              }),
            evict: (node: string, cell: string) =>
              compose([
                "exec",
                "-T",
                "proxy",
                "node",
                "--input-type=module",
                "-e",
                `const r = await fetch(${JSON.stringify("http://")} + process.argv[1] + ":8081/evict/Recovery:" + process.argv[2]); console.log(await r.text()); if (!r.ok) process.exitCode=1;`,
                node,
                cell,
              ]),
          },
          metadata: {
            deploymentChecks,
            engine: "celld",
            version,
            project: runId,
            storage: "minio",
            durability,
            nodes: multiNode ? nodeCount : 1,
            fixtureSha256: uploadedHash,
            containers,
          },
        } satisfies RuntimeHandle & { fleet: typeof fleet; controls: unknown };
      }),
      cleanupAll([
        compose(["logs", "--no-color", "--timestamps"]).pipe(
          Effect.flatMap((output) =>
            artifacts.text("celld.log", output.stdout + output.stderr),
          ),
        ),
        compose(["down", "--volumes", "--remove-orphans", "--timeout", "10"]),
      ]),
      cleanupError,
    );
  });
