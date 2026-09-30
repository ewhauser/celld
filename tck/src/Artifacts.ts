import { Context, Effect, FileSystem, Layer, Schema } from "effect";
import { resolve } from "node:path";
import { TckError } from "./Domain.js";

export class Artifacts extends Context.Service<
  Artifacts,
  {
    readonly directory: string;
    readonly text: (
      name: string,
      value: string,
      append?: boolean,
    ) => Effect.Effect<void, TckError>;
    readonly json: (
      name: string,
      value: unknown,
    ) => Effect.Effect<void, TckError>;
  }
>()("tck/Artifacts") {}

export const artifactsLayer = (directory: string) =>
  Layer.effect(
    Artifacts,
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      yield* fs.makeDirectory(directory, { recursive: true });
      const text = (name: string, value: string, append = false) =>
        fs
          .writeFileString(resolve(directory, name), value, {
            flag: append ? "a" : "w",
          })
          .pipe(
            Effect.mapError(
              (error) =>
                new TckError({
                  phase: "artifact",
                  message: `Cannot write ${name}`,
                  detail: String(error),
                }),
            ),
          );
      return {
        directory,
        text,
        json: (name, value) =>
          text(name, JSON.stringify(value, null, 2) + "\n"),
      };
    }),
  );

export const decodeAs =
  <S extends Schema.Constraint>(schema: S, phase: string) =>
  (input: unknown) =>
    Schema.decodeUnknownEffect(schema)(input).pipe(
      Effect.mapError(
        (error) => new TckError({ phase, message: String(error) }),
      ),
    );

export const decodeJson = <S extends Schema.Constraint>(
  schema: S,
  input: string,
) => decodeAs(Schema.fromJsonString(schema), "decode")(input);
