import { Effect, Schema } from "effect";
import { TckError } from "./Domain.js";

export const DiskContainer = Schema.Struct({
  Id: Schema.String,
  Config: Schema.Struct({
    Labels: Schema.Record(Schema.String, Schema.String),
  }),
  State: Schema.Struct({ Running: Schema.Boolean, StartedAt: Schema.String }),
  Mounts: Schema.Array(
    Schema.Struct({
      Type: Schema.String,
      Name: Schema.optionalKey(Schema.String),
      Destination: Schema.String,
    }),
  ),
});
export const DiskVolume = Schema.Struct({
  Name: Schema.String,
  Labels: Schema.Record(Schema.String, Schema.String),
});
export const ownedStateVolume = (
  project: string,
  container: typeof DiskContainer.Type,
  volume: typeof DiskVolume.Type,
  service: "celld" | "celld2" | "celld3" = "celld",
) =>
  Effect.gen(function* () {
    const mounts = container.Mounts.filter(
      (mount) => mount.Destination === "/state",
    );
    const expected = `${project}_${service}-state`;
    if (
      !project.startsWith("tck-") ||
      container.State.Running ||
      container.Config.Labels["com.docker.compose.project"] !== project ||
      container.Config.Labels["com.docker.compose.service"] !== service ||
      mounts.length !== 1 ||
      mounts[0]?.Type !== "volume" ||
      mounts[0]?.Name !== expected ||
      volume.Name !== expected ||
      volume.Labels["com.docker.compose.project"] !== project ||
      volume.Labels["com.docker.compose.volume"] !== `${service}-state`
    )
      return yield* Effect.fail(
        new TckError({
          phase: "lifecycle",
          message:
            "Refusing disk removal: stopped container and run-owned celld volume must match",
        }),
      );
    return expected;
  });
