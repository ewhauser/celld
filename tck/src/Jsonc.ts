import { Effect, Schema } from "effect";
import { parse, printParseErrorCode, type ParseError } from "jsonc-parser";
import { TckError } from "./Domain.js";

export const decodeJsonc = <S extends Schema.Constraint>(
  schema: S,
  input: string,
) =>
  Effect.gen(function* () {
    const errors: ParseError[] = [];
    const value: unknown = yield* Effect.try({
      try: () => parse(input, errors, { allowTrailingComma: true }),
      catch: (error) =>
        new TckError({ phase: "config", message: String(error) }),
    });
    if (errors.length)
      return yield* Effect.fail(
        new TckError({
          phase: "config",
          message: errors
            .map(
              (error) =>
                `${printParseErrorCode(error.error)} at ${error.offset}`,
            )
            .join(", "),
        }),
      );
    return yield* Schema.decodeUnknownEffect(schema)(value).pipe(
      Effect.mapError(
        (error) => new TckError({ phase: "config", message: String(error) }),
      ),
    );
  });
